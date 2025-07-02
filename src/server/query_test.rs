mod test {
    use std::future::Future;
    use crate::server::models::{GroupKey, Placement, ServiceError, Tile, TileGroup, GROUP_DIM_I32};
    use crate::server::query::{create_schema, QueryStore};
    use crate::server::utils::epoch_to_day;
    use chrono::{TimeZone, Utc};
    use scylla::SessionBuilder;
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::{LazyLock, OnceLock};
    use std::time::SystemTime;
    use async_once_cell::OnceCell;
    use deadpool_redis::{Config, Pool, Runtime};
    use tracing::log::info;

    async fn get_queries() -> &'static QueryStore {
        static REDIS: OnceCell<QueryStore> = OnceCell::new();
        REDIS.get_or_init(async {
            let port = 9042;
            let scylla_url = format!("127.0.0.1:{}", port);
            let session = SessionBuilder::new()
                .known_node(scylla_url.as_str())
                .build()
                .await
                .unwrap();
            info!("Started the scylla session");

            session.query_unpaged("DROP KEYSPACE IF EXISTS pks;", ()).await.unwrap();
            create_schema(&session).await.unwrap();
            info!("Created the scylla schema");
            
            let queries = QueryStore::init_queries(session).await.unwrap();

            info!("Initialized queries at url={}", scylla_url);
            queries
        }).await
    }
    
    async fn clear_tables(query: &QueryStore) {
        query.session.query_unpaged("TRUNCATE pks.tiles;", ()).await.unwrap();
        query.session.query_unpaged("TRUNCATE pks.placements;", ()).await.unwrap();
    }

    #[tokio::test]
    async fn test_tiles() {
        let query = get_queries().await;
        
        clear_tables(query).await;

        let tile1 = query.get_one_tile(1, 1).await;

        let time = SystemTime::from(Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap());

        query.batch_upsert_tile(5, 4, (2, 2, 2), IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), time).await.unwrap();
        let tile2 = query.get_one_tile(5, 4).await;

        assert_eq!(tile1, Err(ServiceError::NotFound(String::from("tile not found at given location: (1, 1)"))));
        assert_eq!(tile2, Ok(Tile { x: 5, y: 4, rgb: (2, 2, 2), date: "2025-01-01T00:00:00+00:00".to_string() }));

        clear_tables(query).await
    }

    #[tokio::test]
    async fn test_tile_group() {
        let query = get_queries().await;

        clear_tables(query).await;

        query.batch_upsert_tile(0, 0, (1, 1, 1), IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), SystemTime::now()).await.unwrap();
        query.batch_upsert_tile(2, 0, (5, 5, 5), IpAddr::V4(Ipv4Addr::new(127, 0, 1, 1)), SystemTime::now()).await.unwrap();
        query.batch_upsert_tile(GROUP_DIM_I32 + 1, GROUP_DIM_I32 + 1, (6, 6, 6), IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), SystemTime::now()).await.unwrap();
        let tiles = query.get_tile_group(GroupKey(0, 0)).await.unwrap();

        let mut expected = TileGroup::empty();
        expected.set(0, 0, (1, 1, 1));
        expected.set(2, 0, (5, 5, 5));

        assert_eq!(tiles, expected);

        clear_tables(query).await
    }

    #[tokio::test]
    async fn test_placements() {
        let query = get_queries().await;
        
        let time_now = SystemTime::from(Utc.with_ymd_and_hms(2020, 1, 1, 0, 30, 0).unwrap());

        clear_tables(query).await;

        let time1 = SystemTime::from(Utc.with_ymd_and_hms(2015, 1, 1, 0, 0, 0).unwrap());
        let time2 = SystemTime::from(Utc.with_ymd_and_hms(2020, 1, 1, 0, 15, 0).unwrap());
        let time3 = SystemTime::from(Utc.with_ymd_and_hms(2020, 1, 1, 0, 30, 0).unwrap());

        query.batch_upsert_tile(1, 0, (3, 3, 3), IpAddr::V4(Ipv4Addr::new(127, 2, 2, 2)), time1).await.unwrap();
        query.batch_upsert_tile(0, 0, (1, 1, 1), IpAddr::V4(Ipv4Addr::new(127, 0, 0, 0)), time2).await.unwrap();
        query.batch_upsert_tile(2, 0, (2, 2, 2), IpAddr::V4(Ipv4Addr::new(127, 1, 1, 1)), time3).await.unwrap();

        let duration_now = time_now.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        let day_now = epoch_to_day(duration_now);

        let placements = query.get_placements(day_now, duration_now).await.unwrap();

        let expected = vec![Placement {
            x: 0,
            y: 0,
            rgb: (1, 1, 1),
            placement_date: "2020-01-01T00:15:00+00:00".to_string(),
        }, Placement {
            x: 2,
            y: 0,
            rgb: (2, 2, 2),
            placement_date: "2020-01-01T00:30:00+00:00".to_string(),
        }];
        assert_eq!(placements, expected);

        clear_tables(query).await
    }
}