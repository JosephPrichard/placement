use crate::services::models::{GroupKey, Tile, ServiceError, TileGroup, GROUP_LEN};
use futures_util::StreamExt;
use scylla::{frame::value::CqlTimestamp, prepared_statement::PreparedStatement, transport::errors::QueryError, Session};
use std::{net::IpAddr, time::SystemTime};

async fn create_keyspace(session: &Session) -> Result<PreparedStatement, QueryError> {
    let query = r#"
    CREATE KEYSPACE IF NOT EXISTS pks
    WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
    "#;
    let prepared: PreparedStatement = session.prepare(query).await?;
    Ok(prepared)
}

async fn create_tiles(session: &Session) -> Result<PreparedStatement, QueryError> {
    let query = r#"
    CREATE TABLE IF NOT EXISTS pks.tiles (
        group_x int,
        group_y int,
        x int,
        y int,
        color tinyint,
        last_updated_ipaddress inet,
        last_updated_time timestamp,
        PRIMARY KEY ((group_x, group_y), x, y));"#;
    let prepared: PreparedStatement = session.prepare(query).await?;
    Ok(prepared)
}

async fn create_stats(session: &Session) -> Result<PreparedStatement, QueryError> {
    let query = r#"
    CREATE TABLE IF NOT EXISTS pks.stats (
        country varchar,
        ipaddress inet,
        times_placed counter,
        PRIMARY KEY (country, ipaddress));"#;
    let prepared: PreparedStatement = session.prepare(query).await?;
    Ok(prepared)
}

async fn create_placement(session: &Session) -> Result<PreparedStatement, QueryError> {
    let query = r#"
    CREATE TABLE IF NOT EXISTS pks.placements (
        hour int,
        x int,
        y int,
        color tinyint,
        ipaddress inet,
        placement_time timestamp,
        PRIMARY KEY (hour, ipaddress, placement_time));"#;
    let prepared: PreparedStatement = session.prepare(query).await?;
    Ok(prepared)
}

async fn increment_times_stat(session: &Session) -> Result<PreparedStatement, QueryError> {
    let query = "UPDATE pks.stats SET times_placed = times_placed + 1 WHERE country = ? AND ipaddress = ?;";
    let prepared: PreparedStatement = session.prepare(query).await?;
    Ok(prepared)
}

async fn insert_placement(session: &Session) -> Result<PreparedStatement, QueryError> {
    let query = "INSERT INTO pks.placements (hour, x, y, color, ipaddress, placement_time) VALUES (?, ?, ?, ?, ?, ?);";
    let prepared: PreparedStatement = session.prepare(query).await?;
    Ok(prepared)
}

async fn update_tile(session: &Session) -> Result<PreparedStatement, QueryError> {
    let query = r#"
    INSERT INTO pks.tiles
        (group_x, group_y, x, y, color, last_updated_ipaddress, last_updated_time)
    VALUES
        (?, ?, ?, ?, ?, ?, ?);"#;
    let prepared: PreparedStatement = session.prepare(query).await?;
    Ok(prepared)
}

async fn get_one_tile(session: &Session) -> Result<PreparedStatement, QueryError> {
    let query = r#"
    SELECT x, y, color, last_updated_time
    FROM pks.tiles
    WHERE group_x = ?
        AND group_y = ?
        AND x = ?
        AND y = ?
    LIMIT 1;"#;
    let prepared: PreparedStatement = session.prepare(query).await?;
    Ok(prepared)
}

async fn get_tile_group(session: &Session) -> Result<PreparedStatement, QueryError> {
    let query = r#"
    SELECT x, y, color
    FROM pks.tiles
    WHERE group_x = ? AND group_y = ?;"#;
    let prepared: PreparedStatement = session.prepare(query).await?;
    Ok(prepared)
}

pub struct QueryStore {
    session: Session,
    insert_placement: PreparedStatement,
    increment_times_stat: PreparedStatement,
    get_one_tile: PreparedStatement,
    get_tile_group: PreparedStatement,
    update_tile: PreparedStatement,
}

impl QueryStore {
    pub async fn init_queries(session: Session) -> Result<QueryStore, QueryError> {
        let query = QueryStore {
            insert_placement: insert_placement(&session).await?,
            increment_times_stat: increment_times_stat(&session).await?,
            get_one_tile: get_one_tile(&session).await?,
            get_tile_group: get_tile_group(&session).await?,
            update_tile: update_tile(&session).await?,
            session
        };
        Ok(query)
    }

    pub async fn get_tile_group(&self, x: i32, y: i32) -> Result<TileGroup, ServiceError> {
        let grp_key = GroupKey::from_point(x, y);

        let mut rows_stream = self.session
            .execute_iter(self.get_tile_group.clone(), (grp_key.0, grp_key.1))
            .await
            .map_err(|e| ServiceError::handle_fatal(e, "when selecting tile group"))?
            .rows_stream::<(i32, i32, i8,)>()
            .map_err(|e| ServiceError::handle_fatal(e, "while extracting rows stream from select tile group result"))?;

        let mut group = [[0i8; GROUP_LEN]; GROUP_LEN];
        while let Some(row) = rows_stream.next().await {
            let (x, y, color, ) = row.map_err(|e| ServiceError::handle_fatal(e, "while streaming rows"))?;
            group[x as usize][y as usize] = color;
        }
        Ok(group)
    }

    pub async fn get_one_tile(&self, x: i32, y: i32) -> Result<Tile, ServiceError> {
        let grp_key = GroupKey::from_point(x, y);

        let mut rows_stream = self.session
            .execute_iter(self.get_one_tile.clone(), (grp_key.0, grp_key.1, x, y))
            .await
            .map_err(|e| ServiceError::handle_fatal(e, "when selecting one tile"))?
            .rows_stream::<(i32, i32, i8, CqlTimestamp, )>()
            .map_err(|e| ServiceError::handle_fatal(e, "while extracting rows stream from select tile result"))?;

        match rows_stream.next().await {
            Some(row) => {
                let (x, y, color, last_updated_time ) = row.map_err(|e| ServiceError::handle_fatal(e, "while streaming rows"))?;
                Ok(Tile { x, y, color, last_updated_time })
            },
            None => Err(ServiceError::NotFoundError(String::from("tile not found at given location")))
        }
    }

    pub async fn update_tile(&self, x: i32, y: i32, color: i8, placer_ipaddress: IpAddr) -> Result<(), ServiceError> {
        let grp_key = GroupKey::from_point(x, y);
        let time_now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
        let placement_time = CqlTimestamp(time_now.as_millis() as i64);

        self.session.execute_unpaged(&self.update_tile, (grp_key.0, grp_key.1, x, y, color, placer_ipaddress, placement_time))
            .await
            .map_err(|e| ServiceError::handle_fatal(e, "when updating tile"))?;
        Ok(())
    }

    pub async fn increment_times_stat(&self, country: &str, ipaddress: IpAddr) -> Result<(), ServiceError> {
        self.session.execute_unpaged(&self.increment_times_stat, (country, ipaddress))
            .await
            .map_err(|e| ServiceError::handle_fatal(e, "when incrementing times placed stat"))?;
        Ok(())
    }

    pub async fn insert_placement_log(&self, x: i32, y: i32, color: i8, placer_ipaddress: IpAddr) -> Result<(), ServiceError> {
        let time_now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
        let hour = (time_now.as_secs() / (60 * 60)) as i64;
        let placement_time = CqlTimestamp(time_now.as_millis() as i64);

        self.session.execute_unpaged(&self.insert_placement, (hour, x, y, color, placer_ipaddress, placement_time))
            .await
            .map_err(|e| ServiceError::handle_fatal(e, "when inserting placement"))?;
        Ok(())
    }
}

pub async fn create_schema(session: &Session) -> Result<(), QueryError> {
    session.execute_unpaged(&create_keyspace(session).await?, ()).await?;
    session.execute_unpaged(&create_tiles(session).await?, ()).await?;
    session.execute_unpaged(&create_stats(session).await?, ()).await?;
    session.execute_unpaged(&create_placement(session).await?, ()).await?;
    Ok(())
}

mod test {
    use crate::services::models::{Tile, ServiceError, TileGroup, GROUP_LEN};
    use crate::services::query::{create_schema, QueryStore};
    use scylla::SessionBuilder;
    use std::net::{IpAddr, Ipv4Addr};
    use scylla::frame::value::CqlTimestamp;

    async fn init_queries() -> QueryStore {
        let session = SessionBuilder::new()
            .known_node("127.0.0.1:9042")
            .build()
            .await
            .unwrap();
        session.query_unpaged("DROP KEYSPACE IF EXISTS pks;", ()).await.unwrap();
        create_schema(&session).await.unwrap();
        QueryStore::init_queries(session).await.unwrap()
    }

    async fn test_get_then_update_tile(query: &QueryStore) {
        let result = query.get_one_tile(1, 1).await;
        
        query.update_tile(5, 4, 2, IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))).await.unwrap();
        let tile = query.get_one_tile(5, 4).await.unwrap();

        assert_eq!(result, Err(ServiceError::NotFoundError(String::from("tile not found at given location"))));
        assert_eq!(tile, Tile { x: 5, y: 4, color: 2, last_updated_time: CqlTimestamp(0) });
    }

    async fn test_get_tile_group(query: &QueryStore) {
        query.update_tile(0, 0, 1, IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))).await.unwrap();
        query.update_tile(2, 0, 5, IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))).await.unwrap();
        query.update_tile((GROUP_LEN + 1) as i32, (GROUP_LEN + 1) as i32, 5, IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))).await.unwrap();
        let tiles = query.get_tile_group(0, 0).await.unwrap();

        let mut expected = [[0i8; GROUP_LEN]; GROUP_LEN];
        expected[0][0] = 1;
        expected[2][0] = 5;

        assert_eq!(tiles, expected)
    }

    #[tokio::test]
    async fn test_store_integration() {
        // let query = init_queries().await;
        // test_get_then_update_tile(&query).await;

        let query = init_queries().await;
        test_get_tile_group(&query).await;
    }
}