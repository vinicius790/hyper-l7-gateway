// tests/enterprise_features.rs
mod common;
use api_gateway::middleware::{sign_jwt_hs256, JwtClaims};
use api_gateway::GatewayConfig;
use common::*;
use hyper::StatusCode;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn jwt_hs256_end_to_end() {
    let up = spawn_echo_upstream().await;
    let secret = "chave-secreta-hmac-sha256-super-segura";
    let yaml = format!(
        r#"
listen: "127.0.0.1:0"
routes:
  - name: jwt_protected
    path_prefix: /jwt
    strip_prefix: true
    upstreams: ["http://{}"]
    auth:
      type: jwt
      secret: "{}"
      issuer: "gw-issuer"
      required_scopes: ["orders:read"]
"#,
        up.addr, secret
    );
    let (gw, _) = start_gateway(&yaml).await;

    let (st_no_token, _, _) = http_get(gw, "/jwt/items", &[]).await;
    assert_eq!(st_no_token, StatusCode::UNAUTHORIZED);

    let claims = JwtClaims {
        sub: Some("user-42".into()),
        iss: Some("gw-issuer".into()),
        aud: None,
        exp: Some(4_102_444_800),
        nbf: None,
        scope: Some("orders:read profile:read".into()),
        scopes: None,
    };
    let token = sign_jwt_hs256(&claims, secret.as_bytes());
    let (st_ok, body, hdrs) = http_get(
        gw,
        "/jwt/items",
        &[("authorization", &format!("Bearer {token}"))],
    )
    .await;
    assert_eq!(st_ok, StatusCode::OK);
    assert!(body.contains("/items"));
    assert!(hdrs.get("traceparent").is_some());
}

#[tokio::test]
async fn idempotent_retry_recovers_from_failed_upstream() {
    let dead_addr = spawn_fail_upstream().await;
    let healthy_up = spawn_echo_upstream().await;
    let yaml = format!(
        r#"
listen: "127.0.0.1:0"
routes:
  - name: resilient
    path_prefix: /res
    strip_prefix: true
    upstreams: ["http://{}", "http://{}"]
    retry:
      max_retries: 2
      base_backoff_ms: 5
      max_backoff_ms: 20
"#,
        dead_addr, healthy_up.addr
    );
    let (gw, gateway) = start_gateway(&yaml).await;
    let (st, body, _) = http_get(gw, "/res/data", &[]).await;
    assert_eq!(st, StatusCode::OK);
    assert!(body.contains("/data"));
    assert!(gateway.metrics().retries_total.load(Ordering::Relaxed) >= 1);
}

#[tokio::test]
async fn canary_routing_and_hot_reload() {
    let up_v1 = spawn_echo_upstream().await;
    let up_v2 = spawn_echo_upstream().await;
    let yaml = format!(
        r#"
listen: "127.0.0.1:0"
routes:
  - name: v1
    path_prefix: /app
    strip_prefix: true
    canary_weight: 50
    upstreams: ["http://{}"]
  - name: v2
    path_prefix: /app
    strip_prefix: true
    canary_weight: 50
    upstreams: ["http://{}"]
"#,
        up_v1.addr, up_v2.addr
    );
    let (gw, gateway) = start_gateway(&yaml).await;
    for _ in 0..6 {
        let (st, _, _) = http_get(gw, "/app/check", &[]).await;
        assert_eq!(st, StatusCode::OK);
    }
    assert_eq!(up_v1.hits.load(Ordering::Relaxed), 3);
    assert_eq!(up_v2.hits.load(Ordering::Relaxed), 3);

    // Hot-reload redirecionando 100% para v2 em memória sem reiniciar o socket
    let new_yaml = format!(
        r#"
listen: "127.0.0.1:0"
routes:
  - name: v2_only
    path_prefix: /app
    strip_prefix: true
    upstreams: ["http://{}"]
"#,
        up_v2.addr
    );
    let new_cfg = GatewayConfig::from_yaml_str(&new_yaml).unwrap();
    gateway.reload_from_config(new_cfg);

    for _ in 0..4 {
        let (st, _, _) = http_get(gw, "/app/check", &[]).await;
        assert_eq!(st, StatusCode::OK);
    }
    assert_eq!(up_v1.hits.load(Ordering::Relaxed), 3);
    assert_eq!(up_v2.hits.load(Ordering::Relaxed), 7);
}

#[tokio::test]
async fn cors_ip_filter_and_payload_limit() {
    let up = spawn_echo_upstream().await;
    let yaml = format!(
        r#"
listen: "127.0.0.1:0"
routes:
  - name: guarded
    path_prefix: /guarded
    strip_prefix: true
    upstreams: ["http://{}"]
    max_body_bytes: 10
    cors:
      allow_origins: ["https://console.local"]
      allow_methods: ["GET", "POST", "OPTIONS"]
      allow_headers: ["Content-Type"]
      max_age_secs: 3600
"#,
        up.addr
    );
    let (gw, _) = start_gateway(&yaml).await;

    // 1. Preflight OPTIONS
    let (st_opt, _, hdrs_opt) = http_req(gw, "OPTIONS", "/guarded/resource", &[], b"").await;
    assert_eq!(st_opt, StatusCode::NO_CONTENT);
    assert_eq!(
        hdrs_opt.get("access-control-allow-origin").unwrap().to_str().unwrap(),
        "https://console.local"
    );

    // 2. Payload acima do limite (10 bytes) -> 413 Payload Too Large
    let (st_large, body_large, _) = http_req(
        gw,
        "POST",
        "/guarded/upload",
        &[],
        b"payload-muito-maior-que-dez-bytes",
    )
    .await;
    assert_eq!(st_large, StatusCode::PAYLOAD_TOO_LARGE);
    assert!(body_large.contains("payload_too_large"));
}
