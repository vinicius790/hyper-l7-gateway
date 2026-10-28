mod common;

use base64::Engine;
use common::*;
use hyper::StatusCode;

#[tokio::test]
async fn api_key_required() {
    let up = spawn_echo_upstream().await;
    let yaml = format!(
        r#"
listen: "127.0.0.1:0"
routes:
  - name: secure
    path_prefix: /secure
    strip_prefix: true
    upstreams: ["http://{}"]
    auth:
      type: api_key
      header: X-API-Key
      keys: ["s3cret"]
"#,
        up.addr
    );
    let (gw, _) = start_gateway(&yaml).await;
    let (st, body, _) = http_get(gw, "/secure", &[]).await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);
    assert!(body.contains("unauthorized"));

    let (st2, body2, _) = http_get(gw, "/secure", &[("x-api-key", "s3cret")]).await;
    assert_eq!(st2, StatusCode::OK);
    assert!(body2.contains("s3cret"));
}

#[tokio::test]
async fn basic_auth() {
    let up = spawn_echo_upstream().await;
    let yaml = format!(
        r#"
listen: "127.0.0.1:0"
routes:
  - name: basic
    path_prefix: /basic
    strip_prefix: true
    upstreams: ["http://{}"]
    auth:
      type: basic
      users: ["bob:hunter2"]
"#,
        up.addr
    );
    let (gw, _) = start_gateway(&yaml).await;
    let (st, _, _) = http_get(gw, "/basic", &[]).await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);

    let token = base64::engine::general_purpose::STANDARD.encode("bob:hunter2");
    let (st2, _, _) = http_get(
        gw,
        "/basic",
        &[("authorization", &format!("Basic {token}"))],
    )
    .await;
    assert_eq!(st2, StatusCode::OK);
}

#[tokio::test]
async fn wrong_api_key_denied() {
    let up = spawn_echo_upstream().await;
    let yaml = format!(
        r#"
listen: "127.0.0.1:0"
routes:
  - name: secure
    path_prefix: /s
    upstreams: ["http://{}"]
    auth:
      type: api_key
      keys: ["good"]
"#,
        up.addr
    );
    let (gw, _) = start_gateway(&yaml).await;
    let (st, _, _) = http_get(gw, "/s", &[("x-api-key", "bad")]).await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);
}
