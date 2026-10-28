//! Injeta / propaga `X-Request-Id`.
use http::{HeaderMap, HeaderName, HeaderValue, Request};
use uuid::Uuid;

pub const REQUEST_ID_HEADER: &str = "x-request-id";

/// Retorna o request-id existente ou gera um novo UUID v4.
pub fn ensure_request_id(headers: &HeaderMap) -> String {
    if let Some(v) = headers.get(REQUEST_ID_HEADER) {
        if let Ok(s) = v.to_str() {
            if !s.is_empty() {
                return s.to_string();
            }
        }
    }
    Uuid::new_v4().to_string()
}

/// Garante header no request mutável.
pub fn inject_request_id<B>(req: &mut Request<B>) -> String {
    let id = ensure_request_id(req.headers());
    let name = HeaderName::from_static(REQUEST_ID_HEADER);
    if let Ok(val) = HeaderValue::from_str(&id) {
        req.headers_mut().insert(name, val);
    }
    id
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[test]
    fn generates_when_missing() {
        let h = HeaderMap::new();
        let a = ensure_request_id(&h);
        let b = ensure_request_id(&h);
        assert_ne!(a, b);
        assert_eq!(a.len(), 36);
    }

    #[test]
    fn preserves_existing() {
        let mut h = HeaderMap::new();
        h.insert(
            HeaderName::from_static(REQUEST_ID_HEADER),
            HeaderValue::from_static("fixed-id-123"),
        );
        assert_eq!(ensure_request_id(&h), "fixed-id-123");
    }
}
