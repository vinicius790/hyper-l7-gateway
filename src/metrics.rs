//! Métricas Prometheus Text 0.0.4 (Counters, Gauges e Histogramas de Latência).
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

const LATENCY_BUCKETS_US: [u64; 9] = [
    1_000,     // 1ms
    5_000,     // 5ms
    10_000,    // 10ms
    25_000,    // 25ms
    50_000,    // 50ms
    100_000,   // 100ms
    250_000,   // 250ms
    500_000,   // 500ms
    1_000_000, // 1s
];

const LATENCY_BUCKET_LABELS: [&str; 9] = [
    "0.001", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1.0",
];

#[derive(Debug)]
pub struct Metrics {
    pub requests_total: AtomicU64,
    pub active_connections: AtomicU64,
    pub responses_2xx: AtomicU64,
    pub responses_4xx: AtomicU64,
    pub responses_5xx: AtomicU64,
    pub rate_limited: AtomicU64,
    pub auth_denied: AtomicU64,
    pub circuit_open: AtomicU64,
    pub load_shed: AtomicU64,
    pub retries_total: AtomicU64,
    pub proxy_errors: AtomicU64,
    latency_buckets: [AtomicU64; 9],
    latency_inf: AtomicU64,
    latency_sum_us: AtomicU64,
    per_route: Mutex<HashMap<String, AtomicU64>>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            requests_total: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            responses_2xx: AtomicU64::new(0),
            responses_4xx: AtomicU64::new(0),
            responses_5xx: AtomicU64::new(0),
            rate_limited: AtomicU64::new(0),
            auth_denied: AtomicU64::new(0),
            circuit_open: AtomicU64::new(0),
            load_shed: AtomicU64::new(0),
            retries_total: AtomicU64::new(0),
            proxy_errors: AtomicU64::new(0),
            latency_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            latency_inf: AtomicU64::new(0),
            latency_sum_us: AtomicU64::new(0),
            per_route: Mutex::new(HashMap::new()),
        }
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc_route(&self, name: &str) {
        let mut map = self.per_route.lock();
        map.entry(name.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_latency_us(&self, duration_us: u64) {
        self.latency_sum_us.fetch_add(duration_us, Ordering::Relaxed);
        self.latency_inf.fetch_add(1, Ordering::Relaxed);
        for (i, &bound) in LATENCY_BUCKETS_US.iter().enumerate() {
            if duration_us <= bound {
                self.latency_buckets[i].fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn observe_status(&self, status: u16) {
        match status {
            200..=299 => {
                self.responses_2xx.fetch_add(1, Ordering::Relaxed);
            }
            400..=499 => {
                self.responses_4xx.fetch_add(1, Ordering::Relaxed);
            }
            500..=599 => {
                self.responses_5xx.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    pub fn render_prometheus(&self) -> String {
        let mut out = String::with_capacity(2048);
        macro_rules! counter {
            ($name:expr, $help:expr, $val:expr) => {{
                out.push_str(&format!("# HELP {} {}\n", $name, $help));
                out.push_str(&format!("# TYPE {} counter\n", $name));
                out.push_str(&format!("{} {}\n", $name, $val));
            }};
        }
        macro_rules! gauge {
            ($name:expr, $help:expr, $val:expr) => {{
                out.push_str(&format!("# HELP {} {}\n", $name, $help));
                out.push_str(&format!("# TYPE {} gauge\n", $name));
                out.push_str(&format!("{} {}\n", $name, $val));
            }};
        }

        counter!(
            "gw_requests_total",
            "Total de requests recebidos",
            self.requests_total.load(Ordering::Relaxed)
        );
        gauge!(
            "gw_active_connections",
            "Conexoes HTTP em processamento no momento",
            self.active_connections.load(Ordering::Relaxed)
        );
        counter!(
            "gw_responses_2xx_total",
            "Respostas 2xx",
            self.responses_2xx.load(Ordering::Relaxed)
        );
        counter!(
            "gw_responses_4xx_total",
            "Respostas 4xx",
            self.responses_4xx.load(Ordering::Relaxed)
        );
        counter!(
            "gw_responses_5xx_total",
            "Respostas 5xx",
            self.responses_5xx.load(Ordering::Relaxed)
        );
        counter!(
            "gw_rate_limited_total",
            "Rejeitados por rate limit",
            self.rate_limited.load(Ordering::Relaxed)
        );
        counter!(
            "gw_auth_denied_total",
            "Rejeitados por auth",
            self.auth_denied.load(Ordering::Relaxed)
        );
        counter!(
            "gw_circuit_open_total",
            "Rejeitados com circuit open",
            self.circuit_open.load(Ordering::Relaxed)
        );
        counter!(
            "gw_load_shed_total",
            "Rejeitados por adaptive concurrency load shedding",
            self.load_shed.load(Ordering::Relaxed)
        );
        counter!(
            "gw_retries_total",
            "Total de retentativas automaticas",
            self.retries_total.load(Ordering::Relaxed)
        );
        counter!(
            "gw_proxy_errors_total",
            "Erros de proxy/upstream",
            self.proxy_errors.load(Ordering::Relaxed)
        );

        out.push_str("# HELP gw_request_duration_seconds Histograma de latencia das requisicoes\n");
        out.push_str("# TYPE gw_request_duration_seconds histogram\n");
        for (i, label) in LATENCY_BUCKET_LABELS.iter().enumerate() {
            out.push_str(&format!(
                "gw_request_duration_seconds_bucket{{le=\"{}\"}} {}\n",
                label,
                self.latency_buckets[i].load(Ordering::Relaxed)
            ));
        }
        let count = self.latency_inf.load(Ordering::Relaxed);
        let sum_secs = (self.latency_sum_us.load(Ordering::Relaxed) as f64) / 1_000_000.0;
        out.push_str(&format!("gw_request_duration_seconds_bucket{{le=\"+Inf\"}} {count}\n"));
        out.push_str(&format!("gw_request_duration_seconds_sum {sum_secs:.6}\n"));
        out.push_str(&format!("gw_request_duration_seconds_count {count}\n"));

        out.push_str("# HELP gw_route_requests_total Requests por rota\n");
        out.push_str("# TYPE gw_route_requests_total counter\n");
        let map = self.per_route.lock();
        for (name, c) in map.iter() {
            out.push_str(&format!(
                "gw_route_requests_total{{route=\"{}\"}} {}\n",
                sanitize_label(name),
                c.load(Ordering::Relaxed)
            ));
        }
        out
    }
}

fn sanitize_label(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}
