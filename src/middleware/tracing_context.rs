//! Propagação W3C Trace Context (`traceparent` / `tracestate`) e `x-request-id`.
use crate::middleware::request_id::{ensure_request_id, REQUEST_ID_HEADER};
use http::{HeaderMap, HeaderName, HeaderValue};
use uuid::Uuid;

pub const HEADER_TRACEPARENT: &str = "traceparent";
pub const HEADER_TRACESTATE: &str = "tracestate";
pub const HEADER_REQUEST_ID: &str = REQUEST_ID_HEADER;

#[derive(Debug, Clone)]
pub struct TraceContext {
    pub trace_id: String,
    pub span_id: String,
    pub trace_flags: String,
    pub traceparent: String,
    pub tracestate: Option<String>,
    pub request_id: String,
}

impl TraceContext {
    pub fn extract_or_generate(headers: &HeaderMap) -> Self {
        let request_id = ensure_request_id(headers);
        let tracestate = headers
            .get(HEADER_TRACESTATE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);

        let new_span_id = random_hex_16();

        if let Some(tp) = headers.get(HEADER_TRACEPARENT).and_then(|v| v.to_str().ok()) {
            if let Some((trace_id, flags)) = parse_traceparent(tp) {
                let traceparent = format!("00-{trace_id}-{new_span_id}-{flags}");
                return Self {
                    trace_id,
                    span_id: new_span_id,
                    trace_flags: flags,
                    traceparent,
                    tracestate,
                    request_id,
                };
            }
        }

        let trace_id = Uuid::new_v4().simple().to_string();
        let flags = "01".to_string();
        let traceparent = format!("00-{trace_id}-{new_span_id}-{flags}");

        Self {
            trace_id,
            span_id: new_span_id,
            trace_flags: flags,
            traceparent,
            tracestate,
            request_id,
        }
    }

    pub fn inject(&self, headers: &mut HeaderMap) {
        if let Ok(v) = HeaderValue::from_str(&self.traceparent) {
            headers.insert(HeaderName::from_static(HEADER_TRACEPARENT), v);
        }
        if let Ok(v) = HeaderValue::from_str(&self.request_id) {
            headers.insert(HeaderName::from_static(HEADER_REQUEST_ID), v);
        }
    }
}

fn parse_traceparent(tp: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = tp.trim().split('-').collect();
    if parts.len() != 4 {
        return None;
    }
    let version = parts[0];
    let trace_id = parts[1];
    let parent_span = parts[2];
    let flags = parts[3];

    if version != "00" || trace_id.len() != 32 || parent_span.len() != 16 || flags.len() != 2 {
        return None;
    }
    if !trace_id.chars().all(|c| c.is_ascii_hexdigit())
        || trace_id.chars().all(|c| c == '0')
        || !parent_span.chars().all(|c| c.is_ascii_hexdigit())
        || parent_span.chars().all(|c| c == '0')
        || !flags.chars().all(|c| c.is_ascii_hexdigit())
    {
        return None;
    }

    Some((trace_id.to_ascii_lowercase(), flags.to_ascii_lowercase()))
}

fn random_hex_16() -> String {
    let u = Uuid::new_v4().as_u128() as u64;
    format!("{:016x}", if u == 0 { 1 } else { u })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_trace_id_and_rotates_span_id() {
        let mut h = HeaderMap::new();
        h.insert(
            HeaderName::from_static("traceparent"),
            HeaderValue::from_static("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        );
        let ctx = TraceContext::extract_or_generate(&h);
        assert_eq!(ctx.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_ne!(ctx.span_id, "00f067aa0ba902b7");
        assert_eq!(ctx.span_id.len(), 16);
    }
}
