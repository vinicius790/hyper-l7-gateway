#![allow(dead_code)]
//! Helpers compartilhados pelos testes de integração.
use api_gateway::{Gateway, GatewayConfig};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;

pub struct MockUpstream {
    pub addr: SocketAddr,
    pub hits: Arc<AtomicU32>,
}

/// Upstream que ecoa método, path e headers selecionados.
pub async fn spawn_echo_upstream() -> MockUpstream {
    let hits = Arc::new(AtomicU32::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits_c = hits.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let hits = hits_c.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(move |req: Request<Incoming>| {
                    let hits = hits.clone();
                    async move {
                        hits.fetch_add(1, Ordering::Relaxed);
                        let path = req.uri().path().to_string();
                        let method = req.method().clone();
                        let api_key = req
                            .headers()
                            .get("x-api-key")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("-")
                            .to_string();
                        let req_id = req
                            .headers()
                            .get("x-request-id")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("-")
                            .to_string();
                        let _ = req.collect().await;
                        let body = format!(
                            "{{\"method\":\"{method}\",\"path\":\"{path}\",\"api_key\":\"{api_key}\",\"request_id\":\"{req_id}\"}}"
                        );
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header("content-type", "application/json")
                                .body(Full::new(Bytes::from(body)))
                                .unwrap(),
                        )
                    }
                });
                let _ = http1::Builder::new().serve_connection(io, svc).await;
            });
        }
    });
    MockUpstream { addr, hits }
}

/// Upstream que sempre responde 500.
pub async fn spawn_fail_upstream() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(|_req: Request<Incoming>| async {
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body(Full::new(Bytes::from_static(b"fail")))
                            .unwrap(),
                    )
                });
                let _ = http1::Builder::new().serve_connection(io, svc).await;
            });
        }
    });
    addr
}

pub async fn start_gateway(yaml: &str) -> (SocketAddr, Arc<Gateway>) {
    let cfg = GatewayConfig::from_yaml_str(yaml).expect("yaml");
    let gw = Gateway::from_config(cfg);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = gw.clone().serve_with_listener(listener).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    (addr, gw)
}

pub async fn http_get(
    addr: SocketAddr,
    path: &str,
    headers: &[(&str, &str)],
) -> (StatusCode, String, hyper::HeaderMap) {
    http_req(addr, "GET", path, headers, b"").await
}

pub async fn http_req(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> (StatusCode, String, hyper::HeaderMap) {
    let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
        .build_http::<Full<Bytes>>();
    let uri = format!("http://{}{}", addr, path);
    let mut builder = Request::builder().method(method).uri(uri);
    for (k, v) in headers {
        builder = builder.header(*k, *v);
    }
    let req = builder
        .body(Full::new(Bytes::copy_from_slice(body)))
        .unwrap();
    let resp = client.request(req).await.expect("client");
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = resp.collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).to_string(), headers)
}
