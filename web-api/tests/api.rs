use cache_server::server::{run, Config};
use reqwest::StatusCode;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::net::TcpListener;
use web_api::{app, client::CacheClient};

async fn spawn_stack() -> String {
    let cache = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cache_addr = cache.local_addr().unwrap().to_string();
    tokio::spawn(run(cache, Config::default(), std::future::pending()));
    let api = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", api.local_addr().unwrap());
    let router = app(Arc::new(CacheClient::new(cache_addr)));
    tokio::spawn(async move { axum::serve(api, router).await.unwrap() });
    base
}

#[tokio::test]
async fn crud_ttl_incr_through_api() {
    let base = spawn_stack().await;
    let http = reqwest::Client::new();

    assert_eq!(
        http.get(format!("{base}/healthz"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        http.get(format!("{base}/v1/keys/a"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );

    let r = http
        .put(format!("{base}/v1/keys/a"))
        .json(&json!({"value": "hello world", "ttl_secs": 60}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NO_CONTENT);
    let body: Value = http
        .get(format!("{base}/v1/keys/a"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["value"], "hello world");
    assert!(body["ttl_secs"].as_i64().unwrap() > 0);

    let r = http
        .post(format!("{base}/v1/keys/a/expire"))
        .json(&json!({"ttl_secs": 100}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NO_CONTENT);

    let n: Value = http
        .post(format!("{base}/v1/keys/counter/incr"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(n["value"], 1);
    let r = http
        .post(format!("{base}/v1/keys/a/incr"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let r = http
        .put(format!("{base}/v1/keys/b"))
        .json(&json!({"value": "line\nbreak"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);

    assert_eq!(
        http.delete(format!("{base}/v1/keys/a"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        http.delete(format!("{base}/v1/keys/a"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn reports_bad_gateway_when_cache_down() {
    let api = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", api.local_addr().unwrap());
    let router = app(Arc::new(CacheClient::new("127.0.0.1:1")));
    tokio::spawn(async move { axum::serve(api, router).await.unwrap() });
    let http = reqwest::Client::new();
    assert_eq!(
        http.get(format!("{base}/v1/keys/a"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_GATEWAY
    );
    assert_eq!(
        http.get(format!("{base}/healthz"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn serves_openapi_and_swagger_ui() {
    let base = spawn_stack().await;
    let http = reqwest::Client::new();
    let doc: Value = http
        .get(format!("{base}/openapi.json"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(doc["openapi"].as_str().unwrap().starts_with("3."));
    for path in [
        "/healthz",
        "/v1/keys/{key}",
        "/v1/keys/{key}/expire",
        "/v1/keys/{key}/incr",
    ] {
        assert!(doc["paths"].get(path).is_some(), "missing {path}");
    }
    let r = http.get(format!("{base}/docs/")).send().await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);
}
