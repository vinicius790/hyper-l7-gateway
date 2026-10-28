// tests/routing.rs
mod common;
use common::*;
use hyper::StatusCode;

#[tokio::test]
async fn routes_by_path_prefix() {
    let up = spawn_echo_upstream().await;
    let yaml = format!(
        r#"
listen: "127.0.0.1:0"
routes:
  - name: echo
    path_prefix: /echo
    strip_prefix: true
    upstreams: ["http://{}"]
"#,
        up.addr
    );
    let (gw, _) = start_gateway(&yaml).await;
    let (st, body, _) = http_get(gw, "/echo/hello", &[]).await;
    assert_eq!(st, StatusCode::OK);
    assert!(body.contains("\"path\":\"/hello\""), "{body}");
}

#[tokio::test]
async fn routes_by_host() {
    let up_a = spawn_echo_upstream().await;
    let up_b = spawn_echo_upstream().await;
    let yaml = format!(
        r#"
listen: "127.0.0.1:0"
routes:
  - name: a
    host: api.local
    path_prefix: /
    strip_prefix: false
    upstreams: ["http://{}"]
  - name: b
    host: www.local
    path_prefix: /
    strip_prefix: false
    upstreams: ["http://{}"]
"#,
        up_a.addr, up_b.addr
    );
    let (gw, _) = start_gateway(&yaml).await;
    let (st, body, _) = http_get(gw, "/x", &[("host", "api.local")]).await;
    assert_eq!(st, StatusCode::OK);
    assert!(body.contains("\"path\":\"/x\""));
    assert_eq!(up_a.hits.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(up_b.hits.load(std::sync::atomic::Ordering::Relaxed), 0);

    let (st2, _, _) = http_get(gw, "/x", &[("host", "www.local")]).await;
    assert_eq!(st2, StatusCode::OK);
    assert_eq!(up_b.hits.load(std::sync::atomic::Ordering::Relaxed), 1);
}

#[tokio::test]
async fn no_route_returns_404() {
    let up = spawn_echo_upstream().await;
    let yaml = format!(
        r#"
listen: "127.0.0.1:0"
routes:
  - name: only
    path_prefix: /only
    upstreams: ["http://{}"]
"#,
        up.addr
    );
    let (gw, _) = start_gateway(&yaml).await;
    let (st, body, _) = http_get(gw, "/missing", &[]).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    assert!(body.contains("no_route"));
}

#[tokio::test]
async fn round_robin_across_upstreams() {
    let a = spawn_echo_upstream().await;
    let b = spawn_echo_upstream().await;
    let yaml = format!(
        r#"
listen: "127.0.0.1:0"
routes:
  - name: rr
    path_prefix: /rr
    strip_prefix: true
    upstreams: ["http://{}", "http://{}"]
"#,
        a.addr, b.addr
    );
    let (gw, _) = start_gateway(&yaml).await;
    for _ in 0..4 {
        let (st, _, _) = http_get(gw, "/rr", &[]).await;
        assert_eq!(st, StatusCode::OK);
    }
    let ha = a.hits.load(std::sync::atomic::Ordering::Relaxed);
    let hb = b.hits.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(ha, 2, "a={ha}");
    assert_eq!(hb, 2, "b={hb}");
}

#[tokio::test]
async fn preserves_and_injects_request_id() {
    let up = spawn_echo_upstream().await;
    let yaml = format!(
        r#"
listen: "127.0.0.1:0"
routes:
  - name: echo
    path_prefix: /echo
    strip_prefix: true
    upstreams: ["http://{}"]
"#,
        up.addr
    );
    let (gw, _) = start_gateway(&yaml).await;
    let (st, body, hdrs) = http_get(gw, "/echo", &[("x-request-id", "client-fixed-id")]).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        hdrs.get("x-request-id").unwrap().to_str().unwrap(),
        "client-fixed-id"
    );
    assert!(body.contains("client-fixed-id"));
}

#[tokio::test]
async fn admin_health_and_metrics() {
    let up = spawn_echo_upstream().await;
    let yaml = format!(
        r#"
listen: "127.0.0.1:0"
routes:
  - name: echo
    path_prefix: /echo
    upstreams: ["http://{}"]
"#,
        up.addr
    );
    let (gw, _) = start_gateway(&yaml).await;
    let (st, body, _) = http_get(gw, "/health", &[]).await;
    assert_eq!(st, StatusCode::OK);
    assert!(body.contains("\"status\""));
    assert!(body.contains("echo"));

    let _ = http_get(gw, "/echo/z", &[]).await;
    let (st2, metrics, _) = http_get(gw, "/metrics", &[]).await;
    assert_eq!(st2, StatusCode::OK);
    assert!(metrics.contains("gw_requests_total"));
    assert!(metrics.contains("gw_route_requests_total"));
    assert!(metrics.contains("gw_request_duration_seconds_bucket"));
}
