use crate::server::handlers::app;
use crate::server::service::init_server;
use axum::body::Body;
use axum::http::Request;
use axum::Router;
use tower::ServiceExt;
use tracing::log::info;

const SCYLLA_URI: &str = "127.0.0.1:9042";
const REDIS_URL: &str = "redis://127.0.0.1:6380/";

pub async fn test_handlers() {
    let server = init_server(SCYLLA_URI.to_string(), REDIS_URL.to_string()).await;
    let router = &mut app(server);

    info!("begin test for handle_get_tile");
    test_handle_get_tile(router).await;

    info!("begin test for handle_post_tile");
    test_handle_post_tile(router).await;

    info!("begin test for handle_get_group");
    test_handle_get_group(router).await;

    info!("begin test for handle_get_placements");
    test_handle_get_placements(router).await;
}

pub async fn test_handle_get_tile(router: &mut Router) {
    let request = Request::builder()
        .method("GET")
        .uri("/tile")
        .body(Body::from("{ x: 1, y: 2 }"))
        .unwrap();

    let resp = router.oneshot(request).await.unwrap();
}

pub async fn test_handle_post_tile(router: &mut Router) {
    let request = Request::builder()
        .method("POST")
        .uri("/tile")
        .body(Body::from("{ x: 1, y: 2, rgb: [1, 2, 3] }"))
        .unwrap();

    let resp = router.oneshot(request).await.unwrap();
}

pub async fn test_handle_get_group(router: &mut Router) {
    let request = Request::builder()
        .method("GET")
        .uri("/group")
        .body(Body::from("{ x: 0, y: 0 }"))
        .unwrap();

    let resp = router.oneshot(request).await.unwrap();
}

pub async fn test_handle_get_placements(router: &mut Router) {
    let request = Request::builder()
        .method("GET")
        .uri("/placements")
        .body(Body::from("{ days_ago: 1, timestamp_after: \"2025-07-01T05:21:30Z\" }"))
        .unwrap();

    let resp = router.oneshot(request).await.unwrap();
}