use std::net::SocketAddr;
use std::sync::Arc;
use axum::{Router};
use axum::routing::get;
use backend::query::QueryStore;
use scylla::{Session, SessionBuilder};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tower_http::services::{ServeDir, ServeFile};
use tracing::log::info;
use crate::backend::broadcast::create_message_subscriber;
use crate::backend::server::{ws_handler, Server};

mod backend;

#[tokio::main]
async fn main() {
    dotenv::dotenv().unwrap();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
    
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

    let subscriber_handle = create_message_subscriber(server.client.clone(), server.tx.clone());

    let serve_resources = ServeDir::new("./resources").fallback(ServeFile::new("./resources/index.html"));

    let router = Router::new()
        .route("/canvas", get(ws_handler))
        .with_state(server)
        .fallback_service(serve_resources);

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    info!("Server running at {}", addr);

    let listener = TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, router).await.unwrap();

    info!("Stopped the backend. Shutting down the subscriber task.");
    subscriber_handle.abort();
}