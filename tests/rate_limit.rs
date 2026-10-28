mod common;

use common::*;
use hyper::StatusCode;

#[tokio::test]
async fn token_bucket_returns_429() {
    let up = spawn_echo_upstream().await;
    let yaml = format!(
        r#"
listen: "127.0.0.1:0"
routes:
  - name: limited
    path_prefix: /lim
    strip_prefix: true
    upstreams: ["http://{}"]
    rate_limit:
      capacity: 2
      refill_per_sec: 0.01
      key: ip
"#,
        up.addr
    );
    let (gw, _) = start_gateway(&yaml).await;
    let (s1, _, _) = http_get(gw, "/lim", &[]).await;
    let (s2, _, _) = http_get(gw, "/lim", &[]).await;
    let (s3, body, _) = http_get(gw, "/lim", &[]).await;
    assert_eq!(s1, StatusCode::OK);
    assert_eq!(s2, StatusCode::OK);
    assert_eq!(s3, StatusCode::TOO_MANY_REQUESTS);
    assert!(body.contains("rate_limited"));
}

#[tokio::test]
async fn global_rate_limit() {
    let up = spawn_echo_upstream().await;
    let yaml = format!(
        r#"
listen: "127.0.0.1:0"
global_rate_limit:
  capacity: 1
  refill_per_sec: 0.01
  key: ip
routes:
  - name: echo
    path_prefix: /echo
    upstreams: ["http://{}"]
"#,
        up.addr
    );
    let (gw, _) = start_gateway(&yaml).await;
    let (s1, _, _) = http_get(gw, "/echo", &[]).await;
    let (s2, _, _) = http_get(gw, "/echo", &[]).await;
    assert_eq!(s1, StatusCode::OK);
    assert_eq!(s2, StatusCode::TOO_MANY_REQUESTS);
}
