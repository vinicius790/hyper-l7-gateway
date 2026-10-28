//! API Gateway L7 de Alta Performance (HTTP/1.1 & HTTP/2).
//! Recursos: Roteamento Radix/Prefix + Host Wildcard + Canary Weight,
//! Load Balancing (Round-Robin, P2C + Peak-EWMA, Least-Connections),
//! Token Bucket Rate Limiting, Auth (API Key, Basic, JWT HS256),
//! Circuit Breaker, Adaptive Concurrency Load Shedding, Retries com Full Jitter,
//! W3C Trace Context (`traceparent`), Hot-Reload e Prometheus Metrics (Counters + Histograms).

pub mod config;
pub mod error;
pub mod gateway;
pub mod health;
pub mod loadbalancer;
pub mod metrics;
pub mod middleware;
pub mod retry;
pub mod router;
pub mod timeout;
pub mod watcher;

pub use config::GatewayConfig;
pub use error::{GatewayError, Result};
pub use gateway::Gateway;
pub use watcher::spawn_config_watcher;
