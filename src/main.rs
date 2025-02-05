mod services;
mod web;

use services::query::QueryStore;
use scylla::{Session, SessionBuilder};

#[tokio::main]
async fn main() {
    let uri = std::env::var("SCYLLA_URI")
        .unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .build()
        .await
        .unwrap();
    
    let query = QueryStore::init_queries(session).await;
}
