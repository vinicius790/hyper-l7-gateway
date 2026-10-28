//! Erros estruturados do API Gateway.
use thiserror::Error;

#[derive(Debug, Error)]
pub enum GatewayError {
    #[error("config: {0}")]
    Config(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("http: {0}")]
    Http(String),
    #[error("yaml: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("rota não encontrada")]
    NoRoute,
    #[error("método não permitido")]
    MethodNotAllowed,
    #[error("ip bloqueado pela política de acesso")]
    IpForbidden,
    #[error("payload excede o limite máximo permitido")]
    PayloadTooLarge,
    #[error("nenhum upstream saudável")]
    NoHealthyUpstream,
    #[error("rate limit excedido")]
    RateLimited,
    #[error("não autorizado")]
    Unauthorized,
    #[error("circuit breaker aberto")]
    CircuitOpen,
    #[error("limite de concorrência adaptativa atingido (load shedding)")]
    LoadShed,
    #[error("upstream: {0}")]
    Upstream(String),
}

pub type Result<T> = std::result::Result<T, GatewayError>;
