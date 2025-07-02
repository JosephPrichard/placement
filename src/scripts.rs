use std::env;
use crate::server::query::create_schema;
use scylla::SessionBuilder;
use tracing::log::info;

mod server;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    
    if args.len() <= 1 {
        panic!("Expected at least one argument (name) when running the script")
    }
    
    if args[1] == "schema" {
        dotenv::dotenv().unwrap();
        tracing_subscriber::fmt()
            .init();

        let scylla_uri = env::var("SCYLLA_URI").expect("Missing SCYLLA_URI env variable");

        let session = SessionBuilder::new()
            .known_node(scylla_uri)
            .build()
            .await
            .unwrap();
        info!("Started the scylla session");

        session.query_unpaged("DROP KEYSPACE IF EXISTS pks;", ()).await.unwrap();
        create_schema(&session).await.unwrap();
        info!("Finished creating the scylla schema");
    }
}