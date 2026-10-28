//! Sonda ativa periódica de upstreams + relatório detalhado em `/health`.
use crate::loadbalancer::RoundRobin;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

pub struct HealthChecker {
    lbs: Vec<Arc<RoundRobin>>,
    interval: Duration,
    timeout: Duration,
    path: String,
}

impl HealthChecker {
    pub fn new(lbs: Vec<Arc<RoundRobin>>, interval: Duration) -> Self {
        Self {
            lbs,
            interval,
            timeout: Duration::from_millis(500),
            path: "/__gw_health".into(),
        }
    }

    pub fn with_params(
        lbs: Vec<Arc<RoundRobin>>,
        interval: Duration,
        timeout: Duration,
        path: String,
    ) -> Self {
        Self {
            lbs,
            interval,
            timeout,
            path,
        }
    }

    pub async fn run(self) {
        let client =
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .build_http::<http_body_util::Full<bytes::Bytes>>();
        let clean_path = if self.path.starts_with('/') {
            self.path.clone()
        } else {
            format!("/{}", self.path)
        };

        loop {
            tokio::time::sleep(self.interval).await;
            for lb in &self.lbs {
                let snap = lb.snapshot();
                for up in snap {
                    let url = format!("{}{}", up.url.trim_end_matches('/'), clean_path);
                    let Ok(uri) = url.parse::<hyper::Uri>() else {
                        warn!(upstream = %up.url, "URI inválida no health checker");
                        continue;
                    };
                    let Ok(req) = hyper::Request::builder()
                        .method(hyper::Method::GET)
                        .uri(uri)
                        .body(http_body_util::Full::new(bytes::Bytes::new()))
                    else {
                        continue;
                    };
                    let result = tokio::time::timeout(self.timeout, client.request(req)).await;
                    let healthy = match result {
                        Ok(Ok(resp)) => resp.status().is_success(),
                        Ok(Err(e)) => {
                            debug!(upstream = %up.url, error = %e, "health fail");
                            false
                        }
                        Err(_) => {
                            debug!(upstream = %up.url, "health timeout");
                            false
                        }
                    };
                    if healthy != up.healthy {
                        debug!(upstream = %up.url, healthy, "health stub atualizou estado");
                    }
                    lb.set_healthy(&up.url, healthy);
                }
            }
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct HealthReport {
    pub status: &'static str,
    pub routes: Vec<RouteHealth>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct RouteHealth {
    pub name: String,
    pub upstreams: Vec<UpstreamHealth>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct UpstreamHealth {
    pub url: String,
    pub healthy: bool,
    pub active_requests: usize,
    pub ewma_latency_us: u64,
}

pub fn build_report(names: &[String], lbs: &[Arc<RoundRobin>]) -> HealthReport {
    let mut routes = Vec::new();
    for (name, lb) in names.iter().zip(lbs.iter()) {
        let upstreams = lb
            .snapshot()
            .into_iter()
            .map(|u| UpstreamHealth {
                url: u.url,
                healthy: u.healthy,
                active_requests: u.active_requests,
                ewma_latency_us: u.ewma_latency_us,
            })
            .collect();
        routes.push(RouteHealth {
            name: name.clone(),
            upstreams,
        });
    }
    let any_up = routes.iter().any(|r| r.upstreams.iter().any(|u| u.healthy));
    HealthReport {
        status: if any_up { "ok" } else { "degraded" },
        routes,
    }
}
