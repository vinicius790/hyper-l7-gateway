//! Configuração YAML e validação estrita (fail-fast) do gateway.
use crate::error::{GatewayError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayConfig {
    /// Endereço de escuta do proxy (ex.: `0.0.0.0:8080`).
    #[serde(default = "default_listen")]
    pub listen: String,
    /// Endereço do admin (`/health`, `/metrics`). Se omitido, usa a mesma porta.
    pub admin_listen: Option<String>,
    /// Rotas em ordem de prioridade.
    #[serde(default)]
    pub routes: Vec<RouteConfig>,
    /// Rate limit global opcional (por IP, antes das rotas).
    pub global_rate_limit: Option<RateLimitConfig>,
    /// Tempo máximo de drenagem no encerramento gracioso (ms).
    #[serde(default = "default_drain_timeout_ms")]
    pub drain_timeout_ms: u64,
    /// Configuração do verificador ativo de saúde dos upstreams.
    #[serde(default)]
    pub health_check: HealthCheckConfig,
}

fn default_listen() -> String {
    "127.0.0.1:8080".into()
}

fn default_drain_timeout_ms() -> u64 {
    5_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    #[serde(default = "default_hc_interval_ms")]
    pub interval_ms: u64,
    #[serde(default = "default_hc_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_hc_path")]
    pub path: String,
    #[serde(default = "default_hc_unhealthy_threshold")]
    pub passive_failure_threshold: u32,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            interval_ms: default_hc_interval_ms(),
            timeout_ms: default_hc_timeout_ms(),
            path: default_hc_path(),
            passive_failure_threshold: default_hc_unhealthy_threshold(),
        }
    }
}

fn default_hc_interval_ms() -> u64 {
    5_000
}
fn default_hc_timeout_ms() -> u64 {
    500
}
fn default_hc_path() -> String {
    "/__gw_health".into()
}
fn default_hc_unhealthy_threshold() -> u32 {
    1
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoadBalanceStrategy {
    RoundRobin,
    P2cEwma,
    LeastConn,
}

impl Default for LoadBalanceStrategy {
    fn default() -> Self {
        Self::RoundRobin
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteConfig {
    pub name: String,
    /// Match por Host (opcional, suporta exato ou wildcard `*.exemplo.local`).
    pub host: Option<String>,
    /// Prefixo de path (obrigatório, ex.: `/api`).
    pub path_prefix: String,
    /// Remove o prefixo antes de encaminhar ao upstream.
    #[serde(default)]
    pub strip_prefix: bool,
    /// Lista de métodos HTTP permitidos (ex.: `["GET", "POST"]`). Se vazio, aceita todos.
    #[serde(default)]
    pub methods: Vec<String>,
    /// Cabeçalhos obrigatórios para casar a rota.
    #[serde(default)]
    pub match_headers: HashMap<String, String>,
    /// Peso para Canary Deployment entre rotas de mesmo host e path_prefix (1..=100).
    #[serde(default = "default_canary_weight")]
    pub canary_weight: u8,
    /// Lista de upstreams `http://host:port`.
    pub upstreams: Vec<String>,
    /// Estratégia de balanceamento (`round_robin`, `p2c_ewma`, `least_conn`).
    #[serde(default)]
    pub algorithm: LoadBalanceStrategy,
    pub rate_limit: Option<RateLimitConfig>,
    pub auth: Option<AuthConfig>,
    pub circuit_breaker: Option<CircuitBreakerConfig>,
    pub adaptive_concurrency: Option<AdaptiveConcurrencyConfig>,
    pub retry: Option<RetryConfig>,
    pub cors: Option<CorsConfig>,
    pub headers: Option<HeaderPolicyConfig>,
    pub ip_filter: Option<IpFilterConfig>,
    /// Limite máximo do tamanho do corpo em bytes (opcional).
    pub max_body_bytes: Option<usize>,
    /// Timeout de proxy em ms (default 10_000).
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

fn default_canary_weight() -> u8 {
    100
}

fn default_timeout_ms() -> u64 {
    10_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Capacidade do token bucket.
    pub capacity: u64,
    /// Tokens adicionados por segundo.
    pub refill_per_sec: f64,
    /// Chave: `ip` (default), `global` ou `header:<nome>`.
    #[serde(default = "default_rl_key")]
    pub key: String,
}

fn default_rl_key() -> String {
    "ip".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthConfig {
    ApiKey {
        /// Header a ler (default `X-API-Key`).
        #[serde(default = "default_api_key_header")]
        header: String,
        /// Parâmetro de query opcional (ex.: `api_key`).
        pub query_param: Option<String>,
        keys: Vec<String>,
    },
    Basic {
        /// Pares `user:password`.
        users: Vec<String>,
    },
    Jwt {
        /// Segredo HMAC-SHA256 (`HS256`).
        secret: String,
        /// Emissor esperado (`iss`), opcional.
        pub issuer: Option<String>,
        /// Audiência esperada (`aud`), opcional.
        pub audience: Option<String>,
        /// Escopos obrigatórios no claim `scope` ou `scopes`.
        #[serde(default)]
        pub required_scopes: Vec<String>,
    },
}

fn default_api_key_header() -> String {
    "X-API-Key".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Falhas consecutivas para abrir.
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: u32,
    /// Tempo em ms no estado Open antes de HalfOpen.
    #[serde(default = "default_open_ms")]
    pub open_ms: u64,
    /// Sucessos em HalfOpen para fechar.
    #[serde(default = "default_half_open_success")]
    pub half_open_success: u32,
}

fn default_failure_threshold() -> u32 {
    5
}
fn default_open_ms() -> u64 {
    5_000
}
fn default_half_open_success() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptiveConcurrencyConfig {
    #[serde(default = "default_target_latency_ms")]
    pub target_latency_ms: u64,
    #[serde(default = "default_min_concurrency")]
    pub min_limit: usize,
    #[serde(default = "default_max_concurrency")]
    pub max_limit: usize,
}

fn default_target_latency_ms() -> u64 {
    50
}
fn default_min_concurrency() -> usize {
    5
}
fn default_max_concurrency() -> usize {
    500
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Número máximo de retentativas adicionais em caso de falha em métodos idempotentes.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Backoff base em milissegundos.
    #[serde(default = "default_base_backoff_ms")]
    pub base_backoff_ms: u64,
    /// Teto de backoff em milissegundos.
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,
}

fn default_max_retries() -> u32 {
    2
}
fn default_base_backoff_ms() -> u64 {
    20
}
fn default_max_backoff_ms() -> u64 {
    250
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorsConfig {
    #[serde(default = "default_cors_origins")]
    pub allow_origins: Vec<String>,
    #[serde(default = "default_cors_methods")]
    pub allow_methods: Vec<String>,
    #[serde(default = "default_cors_headers")]
    pub allow_headers: Vec<String>,
    #[serde(default = "default_cors_max_age")]
    pub max_age_secs: u64,
}

fn default_cors_origins() -> Vec<String> {
    vec!["*".into()]
}
fn default_cors_methods() -> Vec<String> {
    vec!["GET".into(), "POST".into(), "PUT".into(), "DELETE".into(), "OPTIONS".into()]
}
fn default_cors_headers() -> Vec<String> {
    vec!["Content-Type".into(), "Authorization".into(), "X-API-Key".into(), "X-Request-Id".into()]
}
fn default_cors_max_age() -> u64 {
    86_400
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HeaderPolicyConfig {
    #[serde(default)]
    pub request_set: HashMap<String, String>,
    #[serde(default)]
    pub request_remove: Vec<String>,
    #[serde(default)]
    pub response_set: HashMap<String, String>,
    #[serde(default)]
    pub response_remove: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IpFilterConfig {
    /// Lista de IPs permitidos. Se não-vazia, apenas IPs listados têm acesso.
    #[serde(default)]
    pub allow: Vec<String>,
    /// Lista de IPs bloqueados.
    #[serde(default)]
    pub deny: Vec<String>,
}

impl GatewayConfig {
    pub fn from_yaml_str(s: &str) -> Result<Self> {
        let cfg: Self = serde_yaml::from_str(s)?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let raw = fs::read_to_string(path.as_ref())?;
        Self::from_yaml_str(&raw)
    }

    pub fn validate(&self) -> Result<()> {
        if self.routes.is_empty() {
            return Err(GatewayError::Config(
                "pelo menos uma rota é necessária".into(),
            ));
        }
        if let Some(rl) = &self.global_rate_limit {
            if rl.capacity == 0 || rl.refill_per_sec <= 0.0 {
                return Err(GatewayError::Config("global_rate_limit inválido".into()));
            }
        }
        for r in &self.routes {
            if r.name.trim().is_empty() {
                return Err(GatewayError::Config("nome da rota não pode ser vazio".into()));
            }
            if r.path_prefix.is_empty() || !r.path_prefix.starts_with('/') {
                return Err(GatewayError::Config(format!(
                    "rota '{}': path_prefix deve começar com /",
                    r.name
                )));
            }
            if r.upstreams.is_empty() {
                return Err(GatewayError::Config(format!(
                    "rota '{}': precisa de >= 1 upstream",
                    r.name
                )));
            }
            for u in &r.upstreams {
                if !(u.starts_with("http://") || u.starts_with("https://")) {
                    return Err(GatewayError::Config(format!(
                        "rota '{}': upstream inválido '{}'",
                        r.name, u
                    )));
                }
            }
            if let Some(rl) = &r.rate_limit {
                if rl.capacity == 0 || rl.refill_per_sec <= 0.0 {
                    return Err(GatewayError::Config(format!(
                        "rota '{}': rate_limit inválido",
                        r.name
                    )));
                }
            }
            if let Some(AuthConfig::Jwt { secret, .. }) = &r.auth {
                if secret.is_empty() {
                    return Err(GatewayError::Config(format!(
                        "rota '{}': segredo JWT não pode ser vazio",
                        r.name
                    )));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal() {
        let yaml = r#"
listen: "127.0.0.1:0"
routes:
  - name: echo
    path_prefix: /echo
    upstreams: ["http://127.0.0.1:9999"]
"#;
        let c = GatewayConfig::from_yaml_str(yaml).unwrap();
        assert_eq!(c.routes.len(), 1);
        assert_eq!(c.routes[0].name, "echo");
        assert_eq!(c.routes[0].canary_weight, 100);
    }

    #[test]
    fn reject_empty_routes() {
        let yaml = r#"
listen: "127.0.0.1:0"
routes: []
"#;
        assert!(GatewayConfig::from_yaml_str(yaml).is_err());
    }
}
