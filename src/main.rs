use crate::backend::broadcast::create_message_subscriber;
use crate::backend::handlers::{handle_get_group, handle_get_tile, handle_post_tile, handle_sse};
use axum::routing::{get, post};
use axum::Router;
use backend::query::QueryStore;
use bb8_redis::bb8::Pool;
use bb8_redis::{bb8, redis, RedisConnectionManager};
use scylla::{Session, SessionBuilder};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tower_http::services::{ServeDir, ServeFile};
use tracing::log::info;
use crate::backend::handlers::ServerState;

mod backend;

async fn init_server(scylla_uri: String, redis_url: String) -> ServerState {
    let session: Session = SessionBuilder::new()
        .known_node(scylla_uri)
        .build()
        .await
        .unwrap();
    let query = Arc::new(QueryStore::init_queries(session).await.unwrap());
    let redis = Pool::builder().build(RedisConnectionManager::new(redis_url).unwrap()).await.unwrap();
    let (tx, _) = broadcast::channel(1000);

    ServerState { broadcast_tx: tx, query, redis }
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().unwrap();
    tracing_subscriber::fmt()
        .init();
    
    let scylla_uri = std::env::var("SCYLLA_URI").expect("Missing SCYLLA_URI env variable");
    let redis_url = std::env::var("REDIS_URL").expect("Missing REDIS_URL env variable");

    let server = init_server(scylla_uri, redis_url.clone()).await;

    let client = redis::Client::open(redis_url).unwrap();
    let subscriber_handle = create_message_subscriber(client, server.broadcast_tx.clone());

    let serve_resources = ServeDir::new("./resources").fallback(ServeFile::new("./resources/index.html"));
    let router = Router::new()
        .route("/canvas/sse", get(handle_sse))
        .route("/tile", get(handle_get_tile))
        .route("/tile", post(handle_post_tile))
        .route("/group", get(handle_get_group))
        .with_state(server)
        .fallback_service(serve_resources);

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    info!("Server running at {}", addr);

    let listener = TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, router.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap();

    info!("Stopped the backend. Shutting down the subscriber task.");
    subscriber_handle.abort();
}