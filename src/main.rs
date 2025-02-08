use std::net::SocketAddr;
use std::sync::Arc;
use rust_embed::RustEmbed;
use axum::Router;
use axum::routing::get;
use axum_embed::ServeEmbed;
use server::query::QueryStore;
use scylla::{Session, SessionBuilder};
use tokio::sync::broadcast;
use tracing::log::info;
use crate::server::ws::{ws_handler, Server};

mod server;

#[derive(RustEmbed, Clone)]
#[folder = "assets"]
struct Assets;

#[tokio::main]
async fn main() {
    dotenv::dotenv().unwrap();
    tracing_subscriber::fmt().init();
    
    let scylla_uri = std::env::var("SCYLLA_URI").expect("Missing SCYLLA_URI env variable");
    let redis_url = std::env::var("REDIS_URL").expect("Missing REDIS_URL env variable");

    let session: Session = SessionBuilder::new()
        .known_node(scylla_uri)
        .build()
        .await
        .unwrap();
    let query = QueryStore::init_queries(session).await.unwrap();
    let client = redis::Client::open(redis_url).unwrap();
    let (tx, _) = broadcast::channel(1000);

    let server = Server{ query: Arc::new(query), client: Arc::new(client), tx: Arc::new(tx), };

    let serve_assets = ServeEmbed::<Assets>::new();

    let router = Router::new()
        .route("/canvas", get(ws_handler))
        .fallback_service(serve_assets)
        .with_state(server);

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    info!("Server running at {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, router).await.unwrap();
}