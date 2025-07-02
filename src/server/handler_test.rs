use async_once_cell::OnceCell;
use crate::server::handlers::app;
use crate::server::service::init_server;
use axum::body::{Body};
use axum::http::Request;
use axum::{body, Router};
use tokio::sync::Mutex;
use tower::ServiceExt;

pub async fn get_router() -> &'static Mutex<Router> {
    static REDIS: OnceCell<Mutex<Router>> = OnceCell::new();
    REDIS.get_or_init(async {
        let server = init_server("127.0.0.1:9042".to_string(), "redis://127.0.0.1:6380/".to_string()).await;
        Mutex::new(app(server))
    }).await
}

#[tokio::test]
pub async fn test_handle_get_tile() {
    let mut _router = get_router().await.lock().await;
    
    let request = Request::builder()
        .method("GET")
        .uri("/tile?x=1&y=2")
        .body(Body::empty())
        .unwrap();

    let resp = _router.clone().oneshot(request).await.unwrap();
    let body_bytes = body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let body: &str = std::str::from_utf8(&body_bytes).unwrap();

    assert_eq!(body, "hello world");
}

#[tokio::test]
pub async fn test_handle_post_tile() {
    let mut _router = get_router().await.lock().await;
    
    let request = Request::builder()
        .method("POST")
        .uri("/tile")
        .body(Body::from("{ x: 1, y: 2, rgb: [1, 2, 3] }"))
        .unwrap();

    let resp = _router.clone().oneshot(request).await.unwrap();

    let body_bytes = body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let body: &str = std::str::from_utf8(&body_bytes).unwrap();

    assert_eq!(body, "hello world");
}

#[tokio::test]
pub async fn test_handle_get_group() {
    let mut _router = get_router().await.lock().await;
    
    let request = Request::builder()
        .method("GET")
        .uri("/group?x=0&y=0")
        .body(Body::empty())
        .unwrap();

    let resp = _router.clone().oneshot(request).await.unwrap();

    let body_bytes = body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let body: &str = std::str::from_utf8(&body_bytes).unwrap();

    assert_eq!(body, "hello world");
}

#[tokio::test]
pub async fn test_handle_get_placements() {
    let mut _router = get_router().await.lock().await;
    
    let request = Request::builder()
        .method("GET")
        .uri("/placements?days_ago=1&timestamp_after=2025-07-01T05%3A21%3A30Z")
        .body(Body::empty())
        .unwrap();

    let resp = _router.clone().oneshot(request).await.unwrap();

    let body_bytes = body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let body: &str = std::str::from_utf8(&body_bytes).unwrap();

    assert_eq!(body, "hello world");
}