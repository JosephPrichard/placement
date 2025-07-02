use crate::server::broadcast::create_channel_subscriber;
use crate::server::handlers::app;
use crate::server::service::{init_server, ServerState};
use axum::routing::{get, post};
use axum::Router;
use deadpool_redis::{redis, Config, Runtime};
use scylla::{Session, SessionBuilder};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::log::info;

mod server;

#[tokio::main]
async fn main() {
    dotenv::dotenv().unwrap();
    tracing_subscriber::fmt()
        // .with_max_level(tracing::Level::DEBUG)
        .init();
    
    let scylla_uri = std::env::var("SCYLLA_URI").expect("Missing SCYLLA_URI env variable");
    let redis_url = std::env::var("REDIS_URL").expect("Missing REDIS_URL env variable");

    let server = init_server(scylla_uri, redis_url.clone()).await;

    let client = redis::Client::open(redis_url).unwrap();
    let subscriber_handle = create_channel_subscriber(client, server.broadcast_tx.clone());

    let router = app(server);

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    info!("Server running at {}", addr);

    let listener = TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, router.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap();

    info!("Stopped the server. Shutting down the subscriber task.");
    subscriber_handle.abort();
}