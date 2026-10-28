mod common;

use common::*;
use hyper::StatusCode;
use std::time::Duration;

#[tokio::test]
async fn opens_after_upstream_failures() {
    let fail = spawn_fail_upstream().await;
    let yaml = format!(
        r#"
listen: "127.0.0.1:0"
routes:
  - name: brittle
    path_prefix: /b
    strip_prefix: true
    upstreams: ["http://{}"]
    timeout_ms: 2000
    circuit_breaker:
      failure_threshold: 2
      open_ms: 200
      half_open_success: 1
"#,
        fail
    );
    let (gw, gateway) = start_gateway(&yaml).await;

    let (s1, _, _) = http_get(gw, "/b", &[]).await;
    assert_eq!(s1, StatusCode::INTERNAL_SERVER_ERROR);
    let (s2, _, _) = http_get(gw, "/b", &[]).await;
    assert_eq!(s2, StatusCode::INTERNAL_SERVER_ERROR);

    // Terceira deve ser circuit_open (503) sem bater no upstream.
    let (s3, body, _) = http_get(gw, "/b", &[]).await;
    assert_eq!(s3, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("circuit_open"));

    use api_gateway::middleware::CircuitState;
    assert_eq!(
        gateway.route_breaker("brittle").unwrap().state(),
        CircuitState::Open
    );

    tokio::time::sleep(Duration::from_millis(250)).await;
    // Half-open: permite mais uma tentativa (ainda 500 do upstream).
    let (s4, _, _) = http_get(gw, "/b", &[]).await;
    assert!(
        s4 == StatusCode::INTERNAL_SERVER_ERROR || s4 == StatusCode::SERVICE_UNAVAILABLE,
        "unexpected {s4}"
    );
}
