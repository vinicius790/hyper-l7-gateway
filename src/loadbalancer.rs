//! Balanceador de carga multi-algoritmo: Round-Robin, P2C + Peak-EWMA e Least-Connections.
use crate::config::LoadBalanceStrategy;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct Upstream {
    pub url: String,
    pub healthy: bool,
    pub active_requests: usize,
    pub ewma_latency_us: u64,
    pub consecutive_failures: u32,
}

#[derive(Debug)]
struct NodeMetrics {
    active_requests: AtomicUsize,
    ewma_latency_us: AtomicU64,
    consecutive_failures: AtomicUsize,
}

impl NodeMetrics {
    fn new() -> Self {
        Self {
            active_requests: AtomicUsize::new(0),
            ewma_latency_us: AtomicU64::new(5_000), // Estimativa inicial: 5ms
            consecutive_failures: AtomicUsize::new(0),
        }
    }

    fn score(&self) -> u64 {
        let lat = self.ewma_latency_us.load(Ordering::Relaxed).max(1);
        let act = self.active_requests.load(Ordering::Relaxed) as u64;
        lat.saturating_mul(act + 1)
    }

    fn record_latency(&self, sample_us: u64) {
        let prev = self.ewma_latency_us.load(Ordering::Relaxed);
        // Peak-EWMA: reage rápido a subidas de latência (peso 0.5) e decai suavemente (peso 0.2)
        let next = if sample_us > prev {
            (sample_us + prev) / 2
        } else {
            ((sample_us * 2) + (prev * 8)) / 10
        };
        self.ewma_latency_us.store(next.max(1), Ordering::Relaxed);
    }
}

pub struct ActiveRequestGuard {
    metrics: Arc<NodeMetrics>,
    start: Instant,
}

impl Drop for ActiveRequestGuard {
    fn drop(&mut self) {
        self.metrics.active_requests.fetch_sub(1, Ordering::Relaxed);
        let elapsed_us = self.start.elapsed().as_micros() as u64;
        self.metrics.record_latency(elapsed_us);
    }
}

/// Load balancer thread-safe suportando Round-Robin, P2C-EWMA e Least-Connections.
#[derive(Debug)]
pub struct RoundRobin {
    upstreams: Mutex<Vec<(String, bool, Arc<NodeMetrics>)>>,
    cursor: AtomicUsize,
    rng_state: AtomicU64,
    strategy: LoadBalanceStrategy,
}

pub type LoadBalancer = RoundRobin;

impl RoundRobin {
    pub fn new(urls: impl IntoIterator<Item = String>) -> Self {
        Self::with_strategy(urls, LoadBalanceStrategy::RoundRobin)
    }

    pub fn with_strategy(
        urls: impl IntoIterator<Item = String>,
        strategy: LoadBalanceStrategy,
    ) -> Self {
        let upstreams = urls
            .into_iter()
            .map(|url| (url, true, Arc::new(NodeMetrics::new())))
            .collect();
        Self {
            upstreams: Mutex::new(upstreams),
            cursor: AtomicUsize::new(0),
            rng_state: AtomicU64::new(0x853c49e6748fea9b),
            strategy,
        }
    }

    pub fn len(&self) -> usize {
        self.upstreams.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Compatibilidade direta: retorna a URL do próximo upstream saudável.
    pub fn next(&self) -> Option<String> {
        self.pick().map(|(url, _)| url)
    }

    /// Seleciona um upstream saudável aplicando o algoritmo configurado e retorna um guard RAII.
    pub fn pick(&self) -> Option<(String, ActiveRequestGuard)> {
        let ups = self.upstreams.lock();
        let n = ups.len();
        if n == 0 {
            return None;
        }

        let chosen_idx = match self.strategy {
            LoadBalanceStrategy::RoundRobin => {
                let start = self.cursor.fetch_add(1, Ordering::Relaxed);
                let mut found = None;
                for i in 0..n {
                    let idx = (start + i) % n;
                    if ups[idx].1 {
                        found = Some(idx);
                        break;
                    }
                }
                found
            }
            LoadBalanceStrategy::LeastConn => {
                let start = self.cursor.fetch_add(1, Ordering::Relaxed);
                let mut best: Option<(usize, usize, u64)> = None;
                for i in 0..n {
                    let idx = (start + i) % n;
                    if !ups[idx].1 {
                        continue;
                    }
                    let act = ups[idx].2.active_requests.load(Ordering::Relaxed);
                    let lat = ups[idx].2.ewma_latency_us.load(Ordering::Relaxed);
                    match best {
                        None => best = Some((idx, act, lat)),
                        Some((_, b_act, b_lat)) if (act, lat) < (b_act, b_lat) => {
                            best = Some((idx, act, lat));
                        }
                        _ => {}
                    }
                }
                best.map(|(idx, _, _)| idx)
            }
            LoadBalanceStrategy::P2cEwma => {
                let healthy_indices: Vec<usize> = (0..n).filter(|&i| ups[i].1).collect();
                if healthy_indices.is_empty() {
                    None
                } else if healthy_indices.len() == 1 {
                    Some(healthy_indices[0])
                } else {
                    let r1 = self.next_u64() as usize % healthy_indices.len();
                    let mut r2 = self.next_u64() as usize % healthy_indices.len();
                    if r1 == r2 {
                        r2 = (r1 + 1) % healthy_indices.len();
                    }
                    let i1 = healthy_indices[r1];
                    let i2 = healthy_indices[r2];
                    if ups[i1].2.score() <= ups[i2].2.score() {
                        Some(i1)
                    } else {
                        Some(i2)
                    }
                }
            }
        }?;

        let (ref url, _, ref metrics) = ups[chosen_idx];
        metrics.active_requests.fetch_add(1, Ordering::Relaxed);
        Some((
            url.clone(),
            ActiveRequestGuard {
                metrics: metrics.clone(),
                start: Instant::now(),
            },
        ))
    }

    fn next_u64(&self) -> u64 {
        let mut x = self
            .rng_state
            .fetch_add(0x9e3779b97f4a7c15, Ordering::Relaxed);
        x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
        x ^ (x >> 31)
    }

    pub fn set_healthy(&self, url: &str, healthy: bool) {
        let mut ups = self.upstreams.lock();
        if let Some(u) = ups.iter_mut().find(|u| u.0 == url) {
            u.1 = healthy;
            if healthy {
                u.2.consecutive_failures.store(0, Ordering::Relaxed);
            }
        }
    }

    pub fn record_result(&self, url: &str, success: bool, failure_threshold: u32) {
        let mut ups = self.upstreams.lock();
        if let Some(u) = ups.iter_mut().find(|u| u.0 == url) {
            if success {
                u.2.consecutive_failures.store(0, Ordering::Relaxed);
            } else {
                let fails = u.2.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
                if fails as u32 >= failure_threshold.max(1) {
                    u.1 = false;
                }
            }
        }
    }

    pub fn snapshot(&self) -> Vec<Upstream> {
        self.upstreams
            .lock()
            .iter()
            .map(|(url, healthy, m)| Upstream {
                url: url.clone(),
                healthy: *healthy,
                active_requests: m.active_requests.load(Ordering::Relaxed),
                ewma_latency_us: m.ewma_latency_us.load(Ordering::Relaxed),
                consecutive_failures: m.consecutive_failures.load(Ordering::Relaxed) as u32,
            })
            .collect()
    }

    pub fn mark_all_healthy(&self) {
        for u in self.upstreams.lock().iter_mut() {
            u.1 = true;
            u.2.consecutive_failures.store(0, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_robin_order() {
        let rr = RoundRobin::new(vec![
            "http://a".into(),
            "http://b".into(),
            "http://c".into(),
        ]);
        assert_eq!(rr.next().unwrap(), "http://a");
        assert_eq!(rr.next().unwrap(), "http://b");
        assert_eq!(rr.next().unwrap(), "http://c");
        assert_eq!(rr.next().unwrap(), "http://a");
    }

    #[test]
    fn skips_unhealthy() {
        let rr = RoundRobin::new(vec!["http://a".into(), "http://b".into()]);
        rr.set_healthy("http://a", false);
        assert_eq!(rr.next().unwrap(), "http://b");
        assert_eq!(rr.next().unwrap(), "http://b");
        rr.set_healthy("http://b", false);
        assert!(rr.next().is_none());
    }

    #[test]
    fn least_connections_prefers_idle_node() {
        let lb = RoundRobin::with_strategy(
            vec!["http://a".into(), "http://b".into()],
            LoadBalanceStrategy::LeastConn,
        );
        let (u1, _guard1) = lb.pick().unwrap();
        assert_eq!(u1, "http://a");
        // Enquanto _guard1 está ativo em "http://a", o próximo deve ser "http://b"
        let (u2, _guard2) = lb.pick().unwrap();
        assert_eq!(u2, "http://b");
    }
}
