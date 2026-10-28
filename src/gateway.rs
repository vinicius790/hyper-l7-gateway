//! Datapath: listener -> filtros -> LB (RR / P2C-EWMA / LeastConn) -> retry/streaming -> upstream.
use crate::config::{
    AuthConfig, CircuitBreakerConfig, CorsConfig, GatewayConfig, RateLimitConfig, RouteConfig,
};
use crate::error::{GatewayError, Result};
use crate::health::{self, HealthChecker};
use crate::loadbalancer::RoundRobin;
use crate::metrics::Metrics;
use crate::middleware::auth::AuthGate;
use crate::middleware::circuit_breaker::CircuitBreaker;
use crate::middleware::concurrency::AdaptiveConcurrency;
use crate::middleware::rate_limit::RateLimiterMap;
use crate::middleware::tracing_context::{
    TraceContext, HEADER_REQUEST_ID, HEADER_TRACEPARENT, HEADER_TRACESTATE,
};
use crate::retry::jittered_backoff;
use crate::router::{match_route_full, rewrite_path};
use crate::timeout::timed_out;

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::{Body, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{HeaderMap, Method, Request, Response, StatusCode};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioIo;
use parking_lot::RwLock;

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tracing::{info, warn};

pub type GatewayErrorBox = Box<dyn std::error::Error + Send + Sync>;
pub type BoxedGatewayBody = BoxBody<Bytes, GatewayErrorBox>;
type HttpClient = Client<HttpConnector, BoxedGatewayBody>;

pub fn full_body(bytes: Bytes) -> BoxedGatewayBody {
    Full::new(bytes)
        .map_err(|never: Infallible| Box::new(never) as GatewayErrorBox)
        .boxed()
}

pub struct RouteRuntime {
    pub name: String,
    pub config: RouteConfig,
    pub lb: Arc<RoundRobin>,
    pub rate_limit: Option<RateLimiterMap>,
    pub auth: Option<AuthGate>,
    pub breaker: Option<CircuitBreaker>,
    pub concurrency: Option<AdaptiveConcurrency>,
}

pub struct RuntimeState {
    pub cfg: GatewayConfig,
    pub routes: Vec<RouteRuntime>,
    pub global_rl: Option<RateLimiterMap>,
}

pub struct Gateway {
    pub config_path: String,
    state: RwLock<Arc<RuntimeState>>,
    metrics: Arc<Metrics>,
    client: HttpClient,
}

impl Gateway {
    pub fn from_config(cfg: GatewayConfig) -> Arc<Self> {
        Self::new(cfg, "config/gateway.yaml".into())
    }

    pub fn new(cfg: GatewayConfig, config_path: String) -> Arc<Self> {
        let runtime = Self::build_state(cfg);

        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        connector.set_connect_timeout(Some(Duration::from_secs(3)));

        let client = Client::builder(hyper_util::rt::TokioExecutor::new())
            .http1_writev(true)
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(32)
            .build(connector);

        Arc::new(Self {
            config_path,
            state: RwLock::new(Arc::new(runtime)),
            metrics: Arc::new(Metrics::new()),
            client,
        })
    }

    fn build_state(cfg: GatewayConfig) -> RuntimeState {
        let mut routes = Vec::new();
        for r in &cfg.routes {
            let lb = Arc::new(RoundRobin::with_strategy(r.upstreams.clone(), r.algorithm));
            let rate_limit = r
                .rate_limit
                .as_ref()
                .map(|rl| RateLimiterMap::new(rl.capacity, rl.refill_per_sec));
            let auth = r.auth.clone().map(AuthGate::new);
            let breaker = r.circuit_breaker.as_ref().map(|cb| {
                CircuitBreaker::new(cb.failure_threshold, cb.open_ms, cb.half_open_success)
            });
            let concurrency = r.adaptive_concurrency.as_ref().map(|ac| {
                AdaptiveConcurrency::new(ac.target_latency_ms, ac.min_limit, ac.max_limit)
            });

            routes.push(RouteRuntime {
                name: r.name.clone(),
                config: r.clone(),
                lb,
                rate_limit,
                auth,
                breaker,
                concurrency,
            });
        }

        let global_rl = cfg
            .global_rate_limit
            .as_ref()
            .map(|rl| RateLimiterMap::new(rl.capacity, rl.refill_per_sec));

        RuntimeState {
            cfg,
            routes,
            global_rl,
        }
    }

    /// Recarrega atomicamente a configuração a partir do arquivo YAML sem derrubar conexões.
    pub fn reload(&self) -> Result<()> {
        let new_cfg = GatewayConfig::load(&self.config_path)?;
        self.reload_from_config(new_cfg);
        info!(path = %self.config_path, "Hot-reload de configuração concluído");
        Ok(())
    }

    pub fn reload_from_config(&self, new_cfg: GatewayConfig) {
        let new_state = Arc::new(Self::build_state(new_cfg));
        *self.state.write() = new_state;
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    pub fn route_lb(&self, name: &str) -> Option<Arc<RoundRobin>> {
        let st = self.state.read().clone();
        st.routes.iter().find(|r| r.name == name).map(|r| r.lb.clone())
    }

    pub fn route_breaker(&self, name: &str) -> Option<CircuitBreaker> {
        let st = self.state.read().clone();
        st.routes
            .iter()
            .find(|r| r.name == name)
            .and_then(|r| r.breaker.clone())
    }

    pub async fn serve(self: Arc<Self>) -> Result<()> {
        let (addr, admin_opt, hc_cfg, lbs, drain_ms) = {
            let st = self.state.read();
            let addr: SocketAddr = st
                .cfg
                .listen
                .parse()
                .map_err(|e| GatewayError::Config(format!("listen inválido: {e}")))?;
            let admin_opt: Option<SocketAddr> = st
                .cfg
                .admin_listen
                .as_ref()
                .map(|a| {
                    a.parse()
                        .map_err(|e| GatewayError::Config(format!("admin_listen inválido: {e}")))
                })
                .transpose()?;
            let lbs: Vec<_> = st.routes.iter().map(|r| r.lb.clone()).collect();
            (
                addr,
                admin_opt,
                st.cfg.health_check.clone(),
                lbs,
                st.cfg.drain_timeout_ms,
            )
        };

        tokio::spawn(
            HealthChecker::with_params(
                lbs,
                Duration::from_millis(hc_cfg.interval_ms),
                Duration::from_millis(hc_cfg.timeout_ms),
                hc_cfg.path,
            )
            .run(),
        );

        if let Some(admin_addr) = admin_opt {
            let gw = self.clone();
            tokio::spawn(async move {
                if let Err(e) = serve_admin(gw, admin_addr).await {
                    warn!(error = %e, "admin server encerrou");
                }
            });
            info!(%admin_addr, "admin /health /metrics ativo");
        }

        let listener = TcpListener::bind(addr).await?;
        let local = listener.local_addr()?;
        info!(%local, "gateway escutando");

        loop {
            tokio::select! {
                accept_res = listener.accept() => {
                    let (stream, peer) = accept_res?;
                    let gw = self.clone();
                    tokio::spawn(async move {
                        gw.metrics.active_connections.fetch_add(1, Ordering::Relaxed);
                        let io = TokioIo::new(stream);
                        let gw_svc = gw.clone();
                        let svc = service_fn(move |req| {
                            let gw = gw_svc.clone();
                            async move { Ok::<_, Infallible>(gw.handle(req, peer).await) }
                        });
                        if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
                            warn!(error = %e, %peer, "conexão encerrada com erro");
                        }
                        gw.metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
                    });
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("Sinal de encerramento recebido. Drenando conexões ativas...");
                    let deadline = Instant::now() + Duration::from_millis(drain_ms);
                    while self.metrics.active_connections.load(Ordering::Relaxed) > 0
                        && Instant::now() < deadline
                    {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    info!("Encerramento gracioso concluído.");
                    return Ok(());
                }
            }
        }
    }

    /// Bind em porta efêmera (usado em testes de integração).
    pub async fn serve_with_listener(
        self: Arc<Self>,
        listener: TcpListener,
    ) -> Result<SocketAddr> {
        let local = listener.local_addr()?;
        let gw_accept = self.clone();
        tokio::spawn(async move {
            loop {
                let (stream, peer) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let gw = gw_accept.clone();
                tokio::spawn(async move {
                    gw.metrics.active_connections.fetch_add(1, Ordering::Relaxed);
                    let io = TokioIo::new(stream);
                    let gw_svc = gw.clone();
                    let svc = service_fn(move |req| {
                        let gw = gw_svc.clone();
                        async move { Ok::<_, Infallible>(gw.handle(req, peer).await) }
                    });
                    let _ = http1::Builder::new().serve_connection(io, svc).await;
                    gw.metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
                });
            }
        });
        Ok(local)
    }

    async fn handle(
        self: Arc<Self>,
        mut req: Request<Incoming>,
        peer: SocketAddr,
    ) -> Response<BoxedGatewayBody> {
        let started_at = Instant::now();
        self.metrics.requests_total.fetch_add(1, Ordering::Relaxed);

        let trace = TraceContext::extract_or_generate(req.headers());
        trace.inject(req.headers_mut());
        let req_id = trace.request_id.clone();

        let state = self.state.read().clone();

        // Admin na mesma porta se admin_listen ausente.
        if state.cfg.admin_listen.is_none() {
            let path = req.uri().path();
            if path == "/health" && req.method() == Method::GET {
                return self.admin_health();
            }
            if path == "/metrics" && req.method() == Method::GET {
                return self.admin_metrics();
            }
        }

        // Rate limit global.
        if let Some(ref rl) = state.global_rl {
            let key = resolve_rl_key(
                state
                    .cfg
                    .global_rate_limit
                    .as_ref()
                    .map(|c| c.key.as_str())
                    .unwrap_or("ip"),
                peer,
                req.headers(),
            );
            let decision = rl.check(&key);
            if !decision.allowed {
                self.metrics.rate_limited.fetch_add(1, Ordering::Relaxed);
                self.metrics.observe_status(429);
                return rate_limit_error(&req_id, decision);
            }
        }

        let host = req
            .headers()
            .get(http::header::HOST)
            .and_then(|v| v.to_str().ok());
        let path = req.uri().path().to_string();
        let method_str = req.method().as_str().to_string();

        let route_cfgs: Vec<_> = state.routes.iter().map(|r| r.config.clone()).collect();
        let matched = match_route_full(
            &route_cfgs,
            host,
            &path,
            Some(&method_str),
            Some(req.headers()),
        );

        // Tratamento automático de CORS Preflight (OPTIONS) mesmo quando método é filtrado
        if req.method() == Method::OPTIONS {
            if let Some(m_any) = match_route_full(&route_cfgs, host, &path, None, None) {
                if let Some(ref cors) = state.routes[m_any.index].config.cors {
                    return build_cors_preflight_response(cors, &req_id, &trace.traceparent);
                }
            }
        }

        let Some(m) = matched else {
            self.metrics.observe_status(404);
            return json_error(StatusCode::NOT_FOUND, "no_route", &req_id);
        };

        let rt = &state.routes[m.index];
        self.metrics.inc_route(&rt.name);

        // Filtro de IP (Allowlist / Denylist).
        if let Some(ref ip_filter) = rt.config.ip_filter {
            let client_ip = peer.ip().to_string();
            if ip_filter.deny.iter().any(|ip| ip == &client_ip)
                || (!ip_filter.allow.is_empty()
                    && !ip_filter.allow.iter().any(|ip| ip == &client_ip))
            {
                self.metrics.observe_status(403);
                return json_error(StatusCode::FORBIDDEN, "ip_forbidden", &req_id);
            }
        }

        // Autenticação (API Key / Basic / JWT HS256).
        let mut auth_subject: Option<String> = None;
        if let Some(ref auth) = rt.auth {
            match auth.authenticate(req.headers(), req.uri().query()) {
                Ok(identity) => {
                    auth_subject = identity.subject;
                }
                Err(_) => {
                    self.metrics.auth_denied.fetch_add(1, Ordering::Relaxed);
                    self.metrics.observe_status(401);
                    return json_error(StatusCode::UNAUTHORIZED, "unauthorized", &req_id);
                }
            }
        }

        // Rate limit da rota.
        let mut rl_decision = None;
        if let Some(ref rl) = rt.rate_limit {
            let key_spec = rt
                .config
                .rate_limit
                .as_ref()
                .map(|c| c.key.as_str())
                .unwrap_or("ip");
            let key = resolve_rl_key(key_spec, peer, req.headers());
            let decision = rl.check(&key);
            if !decision.allowed {
                self.metrics.rate_limited.fetch_add(1, Ordering::Relaxed);
                self.metrics.observe_status(429);
                return rate_limit_error(&req_id, decision);
            }
            rl_decision = Some(decision);
        }

        // Verificação de tamanho máximo de payload via Content-Length.
        if let Some(max_bytes) = rt.config.max_body_bytes {
            if let Some(exact) = req.body().size_hint().exact() {
                if exact as usize > max_bytes {
                    self.metrics.observe_status(413);
                    return json_error(StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large", &req_id);
                }
            }
        }

        // Adaptive Concurrency (Load Shedding).
        if let Some(ref conc) = rt.concurrency {
            if !conc.try_acquire() {
                self.metrics.load_shed.fetch_add(1, Ordering::Relaxed);
                self.metrics.observe_status(503);
                return json_error(StatusCode::SERVICE_UNAVAILABLE, "load_shedding", &req_id);
            }
        }

        // Circuit breaker.
        if let Some(ref cb) = rt.breaker {
            if !cb.allow() {
                if let Some(ref conc) = rt.concurrency {
                    conc.release(started_at.elapsed().as_micros() as u64);
                }
                self.metrics.circuit_open.fetch_add(1, Ordering::Relaxed);
                self.metrics.observe_status(503);
                return json_error(StatusCode::SERVICE_UNAVAILABLE, "circuit_open", &req_id);
            }
        }

        let new_path = rewrite_path(&rt.config, &path);
        let query = req
            .uri()
            .query()
            .map(|q| format!("?{q}"))
            .unwrap_or_default();

        let method = req.method().clone();
        let headers = req.headers().clone();

        let max_retries = rt
            .config
            .retry
            .as_ref()
            .filter(|_| is_idempotent(&method))
            .map(|r| r.max_retries)
            .unwrap_or(0);

        let response = if max_retries == 0 && rt.config.max_body_bytes.is_none() {
            // Caminho Zero-Copy Streaming puro
            self.forward_streaming_once(
                rt,
                &state,
                req.into_body(),
                method,
                &headers,
                &new_path,
                &query,
                peer,
                &trace,
                auth_subject.as_deref(),
            )
            .await
        } else {
            // Coleta controlada para suportar retentativas idempotentes e validação estrita de max_body_bytes
            match req.collect().await {
                Ok(collected) => {
                    let body_bytes = collected.to_bytes();
                    if let Some(max_bytes) = rt.config.max_body_bytes {
                        if body_bytes.len() > max_bytes {
                            if let Some(ref conc) = rt.concurrency {
                                conc.release(started_at.elapsed().as_micros() as u64);
                            }
                            self.metrics.observe_status(413);
                            return json_error(
                                StatusCode::PAYLOAD_TOO_LARGE,
                                "payload_too_large",
                                &req_id,
                            );
                        }
                    }
                    self.forward_with_retries(
                        rt,
                        &state,
                        body_bytes,
                        method,
                        &headers,
                        &new_path,
                        &query,
                        peer,
                        &trace,
                        auth_subject.as_deref(),
                        max_retries,
                    )
                    .await
                }
                Err(e) => {
                    warn!(error = %e, "erro ao ler body");
                    if let Some(ref conc) = rt.concurrency {
                        conc.release(started_at.elapsed().as_micros() as u64);
                    }
                    self.metrics.proxy_errors.fetch_add(1, Ordering::Relaxed);
                    self.metrics.observe_status(400);
                    return json_error(StatusCode::BAD_REQUEST, "bad_body", &req_id);
                }
            }
        };

        let elapsed_us = started_at.elapsed().as_micros() as u64;
        self.metrics.observe_latency_us(elapsed_us);
        if let Some(ref conc) = rt.concurrency {
            conc.release(elapsed_us);
        }

        finalize_response(response, rt, rl_decision)
    }

    #[allow(clippy::too_many_arguments)]
    async fn forward_streaming_once(
        &self,
        rt: &RouteRuntime,
        state: &RuntimeState,
        incoming_body: Incoming,
        method: Method,
        headers: &HeaderMap,
        new_path: &str,
        query: &str,
        peer: SocketAddr,
        trace: &TraceContext,
        auth_subject: Option<&str>,
    ) -> Response<BoxedGatewayBody> {
        let Some((upstream, _guard)) = rt.lb.pick() else {
            self.metrics.proxy_errors.fetch_add(1, Ordering::Relaxed);
            self.metrics.observe_status(503);
            return json_error(StatusCode::SERVICE_UNAVAILABLE, "no_upstream", &trace.request_id);
        };

        let target = format!("{}{}{}", upstream.trim_end_matches('/'), new_path, query);
        let Ok(uri) = target.parse::<hyper::Uri>() else {
            return json_error(StatusCode::BAD_GATEWAY, "bad_upstream_uri", &trace.request_id);
        };

        let builder = prepare_upstream_request(
            method,
            uri,
            headers,
            &upstream,
            peer,
            trace,
            auth_subject,
            rt.config.headers.as_ref(),
        );

        let streaming_body: BoxedGatewayBody = incoming_body
            .map_err(|e| Box::new(e) as GatewayErrorBox)
            .boxed();

        let Ok(out_req) = builder.body(streaming_body) else {
            return json_error(StatusCode::INTERNAL_SERVER_ERROR, "build_req", &trace.request_id);
        };

        let timeout = Duration::from_millis(rt.config.timeout_ms);
        let result = tokio::time::timeout(timeout, self.client.request(out_req)).await;
        self.process_upstream_result(
            result,
            rt,
            &upstream,
            trace,
            state.cfg.health_check.passive_failure_threshold,
        )
    }

    #[allow(clippy::too_many_arguments)]
    async fn forward_with_retries(
        &self,
        rt: &RouteRuntime,
        state: &RuntimeState,
        body_bytes: Bytes,
        method: Method,
        headers: &HeaderMap,
        new_path: &str,
        query: &str,
        peer: SocketAddr,
        trace: &TraceContext,
        auth_subject: Option<&str>,
        max_retries: u32,
    ) -> Response<BoxedGatewayBody> {
        let retry_cfg = rt.config.retry.as_ref();
        let base_ms = retry_cfg.map(|r| r.base_backoff_ms).unwrap_or(20);
        let cap_ms = retry_cfg.map(|r| r.max_backoff_ms).unwrap_or(250);
        let start_ms = 0u64;
        let call_start = Instant::now();

        for attempt in 0..=max_retries {
            if attempt > 0 {
                let elapsed_ms = call_start.elapsed().as_millis() as u64;
                if timed_out(start_ms, elapsed_ms, rt.config.timeout_ms) {
                    break;
                }
                self.metrics.retries_total.fetch_add(1, Ordering::Relaxed);
                let sleep_ms = jittered_backoff(base_ms, attempt - 1, cap_ms, attempt as u64 + 17);
                tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
            }

            let Some((upstream, _guard)) = rt.lb.pick() else {
                self.metrics.proxy_errors.fetch_add(1, Ordering::Relaxed);
                self.metrics.observe_status(503);
                return json_error(StatusCode::SERVICE_UNAVAILABLE, "no_upstream", &trace.request_id);
            };

            let target = format!("{}{}{}", upstream.trim_end_matches('/'), new_path, query);
            let Ok(uri) = target.parse::<hyper::Uri>() else {
                return json_error(StatusCode::BAD_GATEWAY, "bad_upstream_uri", &trace.request_id);
            };

            let builder = prepare_upstream_request(
                method.clone(),
                uri,
                headers,
                &upstream,
                peer,
                trace,
                auth_subject,
                rt.config.headers.as_ref(),
            );

            let Ok(out_req) = builder.body(full_body(body_bytes.clone())) else {
                return json_error(StatusCode::INTERNAL_SERVER_ERROR, "build_req", &trace.request_id);
            };

            let timeout = Duration::from_millis(rt.config.timeout_ms);
            let result = tokio::time::timeout(timeout, self.client.request(out_req)).await;

            let should_retry = match &result {
                Ok(Ok(resp)) => resp.status().is_server_error() && attempt < max_retries,
                Ok(Err(_)) | Err(_) => attempt < max_retries,
            };

            if should_retry {
                if let Ok(Err(_)) = &result {
                    rt.lb.record_result(
                        &upstream,
                        false,
                        state.cfg.health_check.passive_failure_threshold,
                    );
                }
                continue;
            }

            return self.process_upstream_result(
                result,
                rt,
                &upstream,
                trace,
                state.cfg.health_check.passive_failure_threshold,
            );
        }

        self.metrics.proxy_errors.fetch_add(1, Ordering::Relaxed);
        self.metrics.observe_status(504);
        json_error(StatusCode::GATEWAY_TIMEOUT, "upstream_timeout", &trace.request_id)
    }

    fn process_upstream_result(
        &self,
        result: std::result::Result<
            std::result::Result<Response<Incoming>, hyper_util::client::legacy::Error>,
            tokio::time::error::Elapsed,
        >,
        rt: &RouteRuntime,
        upstream: &str,
        trace: &TraceContext,
        passive_threshold: u32,
    ) -> Response<BoxedGatewayBody> {
        match result {
            Ok(Ok(resp)) => {
                let status = resp.status();
                if status.is_server_error() {
                    if let Some(ref cb) = rt.breaker {
                        cb.on_failure();
                    }
                } else {
                    if let Some(ref cb) = rt.breaker {
                        cb.on_success();
                    }
                    rt.lb.record_result(upstream, true, passive_threshold);
                }
                self.metrics.observe_status(status.as_u16());

                let (parts, body) = resp.into_parts();
                let mut out = Response::builder().status(parts.status);
                for (k, v) in parts.headers.iter() {
                    if is_hop_by_hop(k.as_str()) {
                        continue;
                    }
                    if let Some(ref hp) = rt.config.headers {
                        if hp
                            .response_remove
                            .iter()
                            .any(|rm| rm.eq_ignore_ascii_case(k.as_str()))
                        {
                            continue;
                        }
                    }
                    out = out.header(k, v);
                }
                out = out.header(HEADER_REQUEST_ID, &trace.request_id);
                out = out.header(HEADER_TRACEPARENT, &trace.traceparent);

                let streamed_resp: BoxedGatewayBody = body
                    .map_err(|e| Box::new(e) as GatewayErrorBox)
                    .boxed();

                out.body(streamed_resp).unwrap_or_else(|_| {
                    json_error(StatusCode::INTERNAL_SERVER_ERROR, "resp", &trace.request_id)
                })
            }
            Ok(Err(e)) => {
                warn!(error = %e, upstream = %upstream, "proxy error");
                if let Some(ref cb) = rt.breaker {
                    cb.on_failure();
                }
                rt.lb.record_result(upstream, false, passive_threshold);
                self.metrics.proxy_errors.fetch_add(1, Ordering::Relaxed);
                self.metrics.observe_status(502);
                json_error(StatusCode::BAD_GATEWAY, "upstream_error", &trace.request_id)
            }
            Err(_) => {
                if let Some(ref cb) = rt.breaker {
                    cb.on_failure();
                }
                self.metrics.proxy_errors.fetch_add(1, Ordering::Relaxed);
                self.metrics.observe_status(504);
                json_error(StatusCode::GATEWAY_TIMEOUT, "upstream_timeout", &trace.request_id)
            }
        }
    }

    fn admin_health(&self) -> Response<BoxedGatewayBody> {
        let st = self.state.read();
        let names: Vec<_> = st.routes.iter().map(|r| r.name.clone()).collect();
        let lbs: Vec<_> = st.routes.iter().map(|r| r.lb.clone()).collect();
        let report = health::build_report(&names, &lbs);
        let body = serde_json::to_vec(&report).unwrap_or_default();
        Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(full_body(Bytes::from(body)))
            .unwrap()
    }

    fn admin_metrics(&self) -> Response<BoxedGatewayBody> {
        let text = self.metrics.render_prometheus();
        Response::builder()
            .status(StatusCode::OK)
            .header(
                http::header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )
            .body(full_body(Bytes::from(text)))
            .unwrap()
    }
}

#[allow(clippy::too_many_arguments)]
fn prepare_upstream_request(
    method: Method,
    uri: hyper::Uri,
    headers: &HeaderMap,
    upstream: &str,
    peer: SocketAddr,
    trace: &TraceContext,
    auth_subject: Option<&str>,
    header_policy: Option<&crate::config::HeaderPolicyConfig>,
) -> http::request::Builder {
    let mut builder = Request::builder().method(method).uri(uri);
    for (k, v) in headers.iter() {
        if is_hop_by_hop(k.as_str()) {
            continue;
        }
        if let Some(hp) = header_policy {
            if hp
                .request_remove
                .iter()
                .any(|rm| rm.eq_ignore_ascii_case(k.as_str()))
            {
                continue;
            }
        }
        builder = builder.header(k, v);
    }

    builder = builder.header(HEADER_REQUEST_ID, &trace.request_id);
    builder = builder.header(HEADER_TRACEPARENT, &trace.traceparent);
    if let Some(ref ts) = trace.tracestate {
        builder = builder.header(HEADER_TRACESTATE, ts);
    }

    // X-Forwarded-For / X-Forwarded-Proto
    let xff = match headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
        Some(existing) => format!("{existing}, {}", peer.ip()),
        None => peer.ip().to_string(),
    };
    builder = builder.header("x-forwarded-for", xff);
    builder = builder.header("x-forwarded-proto", "http");

    if let Some(sub) = auth_subject {
        builder = builder.header("x-authenticated-sub", sub);
    }

    if let Some(hp) = header_policy {
        for (k, v) in &hp.request_set {
            builder = builder.header(k.as_str(), v.as_str());
        }
    }

    if let Some(host_hdr) = upstream_host(upstream) {
        builder = builder.header(http::header::HOST, host_hdr);
    }
    builder
}

fn finalize_response(
    mut resp: Response<BoxedGatewayBody>,
    rt: &RouteRuntime,
    rl_decision: Option<crate::middleware::RateLimitDecision>,
) -> Response<BoxedGatewayBody> {
    let headers = resp.headers_mut();

    if let Some(d) = rl_decision {
        if let Ok(v) = http::HeaderValue::from_str(&d.limit.to_string()) {
            headers.insert("x-ratelimit-limit", v);
        }
        if let Ok(v) = http::HeaderValue::from_str(&d.remaining.to_string()) {
            headers.insert("x-ratelimit-remaining", v);
        }
    }

    if let Some(ref cors) = rt.config.cors {
        apply_cors_headers(headers, cors);
    }

    if let Some(ref hp) = rt.config.headers {
        for (k, v) in &hp.response_set {
            if let (Ok(name), Ok(val)) = (
                http::HeaderName::from_bytes(k.as_bytes()),
                http::HeaderValue::from_str(v),
            ) {
                headers.insert(name, val);
            }
        }
    }

    resp
}

fn apply_cors_headers(headers: &mut HeaderMap, cors: &CorsConfig) {
    let origins = cors.allow_origins.join(", ");
    let methods = cors.allow_methods.join(", ");
    let allow_hdrs = cors.allow_headers.join(", ");
    if let Ok(v) = http::HeaderValue::from_str(&origins) {
        headers.insert(http::header::ACCESS_CONTROL_ALLOW_ORIGIN, v);
    }
    if let Ok(v) = http::HeaderValue::from_str(&methods) {
        headers.insert(http::header::ACCESS_CONTROL_ALLOW_METHODS, v);
    }
    if let Ok(v) = http::HeaderValue::from_str(&allow_hdrs) {
        headers.insert(http::header::ACCESS_CONTROL_ALLOW_HEADERS, v);
    }
    if let Ok(v) = http::HeaderValue::from_str(&cors.max_age_secs.to_string()) {
        headers.insert(http::header::ACCESS_CONTROL_MAX_AGE, v);
    }
}

fn build_cors_preflight_response(
    cors: &CorsConfig,
    req_id: &str,
    traceparent: &str,
) -> Response<BoxedGatewayBody> {
    let mut resp = Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header(HEADER_REQUEST_ID, req_id)
        .header(HEADER_TRACEPARENT, traceparent)
        .body(full_body(Bytes::new()))
        .unwrap();
    apply_cors_headers(resp.headers_mut(), cors);
    resp
}

fn resolve_rl_key(spec: &str, peer: SocketAddr, headers: &HeaderMap) -> String {
    if spec == "global" {
        "global".to_string()
    } else if let Some(hdr_name) = spec.strip_prefix("header:") {
        headers
            .get(hdr_name.to_ascii_lowercase().as_str())
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .unwrap_or_else(|| peer.ip().to_string())
    } else {
        peer.ip().to_string()
    }
}

fn is_idempotent(method: &Method) -> bool {
    matches!(
        *method,
        Method::GET | Method::HEAD | Method::OPTIONS | Method::PUT | Method::DELETE
    )
}

async fn serve_admin(gw: Arc<Gateway>, addr: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let gw = gw.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = service_fn(move |req: Request<Incoming>| {
                let gw = gw.clone();
                async move {
                    let path = req.uri().path();
                    let resp = if path == "/health" {
                        gw.admin_health()
                    } else if path == "/metrics" {
                        gw.admin_metrics()
                    } else {
                        json_error(StatusCode::NOT_FOUND, "admin_only", "-")
                    };
                    Ok::<_, Infallible>(resp)
                }
            });
            let _ = http1::Builder::new().serve_connection(io, svc).await;
        });
    }
}

fn rate_limit_error(
    req_id: &str,
    decision: crate::middleware::RateLimitDecision,
) -> Response<BoxedGatewayBody> {
    let body = format!("{{\"error\":\"rate_limited\",\"request_id\":\"{req_id}\"}}");
    Response::builder()
        .status(StatusCode::TOO_MANY_REQUESTS)
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(HEADER_REQUEST_ID, req_id)
        .header("x-ratelimit-limit", decision.limit.to_string())
        .header("x-ratelimit-remaining", decision.remaining.to_string())
        .header(http::header::RETRY_AFTER, decision.retry_after_secs.to_string())
        .body(full_body(Bytes::from(body)))
        .unwrap()
}

pub fn json_error(status: StatusCode, code: &str, req_id: &str) -> Response<BoxedGatewayBody> {
    let body = format!("{{\"error\":\"{code}\",\"request_id\":\"{req_id}\"}}");
    Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(HEADER_REQUEST_ID, req_id)
        .body(full_body(Bytes::from(body)))
        .unwrap()
}

fn is_hop_by_hop(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailers"
            | "transfer-encoding"
            | "upgrade"
            | "host"
    )
}

fn upstream_host(url: &str) -> Option<String> {
    let u: hyper::Uri = url.parse().ok()?;
    let host = u.host()?;
    match u.port_u16() {
        Some(p) => Some(format!("{host}:{p}")),
        None => Some(host.to_string()),
    }
}

#[allow(dead_code)]
pub fn demo_rate_limit(capacity: u64, refill: f64) -> RateLimitConfig {
    RateLimitConfig {
        capacity,
        refill_per_sec: refill,
        key: "ip".into(),
    }
}

#[allow(dead_code)]
pub fn demo_auth_api_key(key: &str) -> AuthConfig {
    AuthConfig::ApiKey {
        header: "X-API-Key".into(),
        query_param: None,
        keys: vec![key.into()],
    }
}

#[allow(dead_code)]
pub fn demo_breaker(threshold: u32, open_ms: u64) -> CircuitBreakerConfig {
    CircuitBreakerConfig {
        failure_threshold: threshold,
        open_ms,
        half_open_success: 1,
    }
}
