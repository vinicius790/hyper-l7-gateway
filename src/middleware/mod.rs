//! Cadeia de filtros do datapath.
pub mod auth;
pub mod circuit_breaker;
pub mod concurrency;
pub mod rate_limit;
pub mod request_id;
pub mod tracing_context;

pub use auth::{AuthGate, AuthIdentity, JwtClaims, sign_jwt_hs256, verify_jwt_hs256};
pub use circuit_breaker::{CircuitBreaker, CircuitState};
pub use concurrency::AdaptiveConcurrency;
pub use rate_limit::{RateLimitDecision, RateLimiterMap, TokenBucket};
pub use request_id::{ensure_request_id, inject_request_id, REQUEST_ID_HEADER};
pub use tracing_context::{TraceContext, HEADER_REQUEST_ID, HEADER_TRACEPARENT};
