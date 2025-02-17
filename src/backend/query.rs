use crate::backend::models::{GroupKey, Placement, ServiceError, Tile, TileGroup};
use chrono::{DateTime, TimeZone, Utc};
use futures_util::StreamExt;
use scylla::frame::value::Counter;
use scylla::{frame::value::CqlTimestamp, prepared_statement::PreparedStatement, transport::errors::QueryError, Session};
use std::{net::IpAddr, time::SystemTime};
use tracing::log::info;

pub async fn create_schema(session: &Session) -> Result<(), QueryError> {
    session.query_unpaged("\
        CREATE KEYSPACE IF NOT EXISTS pks
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };", ()).await?;
    session.query_unpaged("\
        CREATE TABLE IF NOT EXISTS pks.tiles (
            group_x int,
            group_y int,
            x int,
            y int,
            rgb tuple<tinyint, tinyint, tinyint>,
            last_updated_ipaddress inet,
            last_updated_time timestamp,
            PRIMARY KEY ((group_x, group_y), x, y));", ()).await?;
    session.query_unpaged("\
        CREATE TABLE IF NOT EXISTS pks.stats (
            ipaddress inet,
            times_placed counter,
            PRIMARY KEY (ipaddress));", ()).await?;
    session.query_unpaged("\
        CREATE TABLE IF NOT EXISTS pks.placements (
            hour bigint,
            x int,
            y int,
            rgb tuple<tinyint, tinyint, tinyint>,
            ipaddress inet,
            placement_time timestamp,
            PRIMARY KEY (hour, placement_time, ipaddress));", ()).await?;
    Ok(())
}

async fn increment_times_stat(session: &Session) -> Result<PreparedStatement, QueryError> {
    let query = "UPDATE pks.stats SET times_placed = times_placed + 1 WHERE ipaddress = ?;";
    let prepared: PreparedStatement = session.prepare(query).await?;
    Ok(prepared)
}

async fn insert_placement(session: &Session) -> Result<PreparedStatement, QueryError> {
    let query = "INSERT INTO pks.placements (hour, x, y, rgb, ipaddress, placement_time) VALUES (?, ?, ?, ?, ?, ?);";
    let prepared: PreparedStatement = session.prepare(query).await?;
    Ok(prepared)
}

async fn update_tile(session: &Session) -> Result<PreparedStatement, QueryError> {
    let query = r#"
        INSERT INTO pks.tiles (group_x, group_y, x, y, rgb, last_updated_ipaddress, last_updated_time)
        VALUES (?, ?, ?, ?, ?, ?, ?);"#;
    let prepared: PreparedStatement = session.prepare(query).await?;
    Ok(prepared)
}

async fn get_one_tile(session: &Session) -> Result<PreparedStatement, QueryError> {
    let query = r#"
        SELECT x, y, rgb, last_updated_time
        FROM pks.tiles
        WHERE group_x = ?
            AND group_y = ?
            AND x = ?
            AND y = ?
        LIMIT 1;"#;
    let prepared: PreparedStatement = session.prepare(query).await?;
    Ok(prepared)
}

async fn get_placements(session: &Session) -> Result<PreparedStatement, QueryError> {
    let query = r#"
        SELECT x, y, rgb, ipaddress, placement_time
        FROM pks.placements
        WHERE hour = ? AND placement_time < ?;"#;
    let prepared: PreparedStatement = session.prepare(query).await?;
    Ok(prepared)
}

async fn get_tile_group(session: &Session) -> Result<PreparedStatement, QueryError> {
    let query = r#"
        SELECT x, y, rgb
        FROM pks.tiles
        WHERE group_x = ? AND group_y = ?;"#;
    let prepared: PreparedStatement = session.prepare(query).await?;
    Ok(prepared)
}

async fn get_times_placed(session: &Session) -> Result<PreparedStatement, QueryError> {
    let query = r#"
        SELECT times_placed
        FROM pks.stats
        WHERE ipaddress = ?;"#;
    let prepared: PreparedStatement = session.prepare(query).await?;
    Ok(prepared)
}

pub struct QueryStore {
    pub session: Session,
    insert_placement: PreparedStatement,
    increment_times_stat: PreparedStatement,
    get_one_tile: PreparedStatement,
    get_tile_group: PreparedStatement,
    get_placements: PreparedStatement,
    update_tile: PreparedStatement,
    get_stats: PreparedStatement,
}

impl QueryStore {
    pub async fn init_queries(session: Session) -> Result<QueryStore, ServiceError> {
        let insert_placement = insert_placement(&session)
            .await
            .map_err(|e| ServiceError::handle_fatal(e, "while creating insert_placement query"))?;
        let increment_times_stat = increment_times_stat(&session)
            .await
            .map_err(|e| ServiceError::handle_fatal(e, "while creating increment_times_stat query"))?;
        let get_one_tile = get_one_tile(&session)
            .await
            .map_err(|e| ServiceError::handle_fatal(e, "while creating get_one_tile query"))?;
        let get_tile_group = get_tile_group(&session)
            .await
            .map_err(|e| ServiceError::handle_fatal(e, "while creating get_tile_group query"))?;
        let get_placements = get_placements(&session)
            .await
            .map_err(|e| ServiceError::handle_fatal(e, "while creating get_placements query"))?;
        let update_tile = update_tile(&session)
            .await
            .map_err(|e| ServiceError::handle_fatal(e, "while creating update_tile query"))?;
        let get_stats = get_times_placed(&session)
            .await
            .map_err(|e| ServiceError::handle_fatal(e, "while creating get_times_placed query"))?;
        
        Ok(QueryStore { insert_placement, increment_times_stat, get_one_tile, get_tile_group, get_placements, update_tile, get_stats, session })
    }

    pub async fn get_tile_group(&self, key: GroupKey) -> Result<TileGroup, ServiceError> {
        let mut rows_stream = self.session
            .execute_iter(self.get_tile_group.clone(), (key.0, key.1))
            .await
            .map_err(|e| ServiceError::handle_fatal(e, "when selecting tile group"))?
            .rows_stream::<(i32, i32, (i8, i8, i8))>()
            .map_err(|e| ServiceError::handle_fatal(e, "while extracting rows stream from select tile group result"))?;
        
        let mut group = TileGroup::empty(key.0, key.1);
        while let Some(row) = rows_stream.next().await {
            let (x, y, rgb) = row.map_err(|e| ServiceError::handle_fatal(e, "while streaming tile rows"))?;
            group.set((x - key.0) as usize, (y - key.1) as usize, rgb);
        }

        info!("Selected tile group with x={}, y={}", key.0, key.1);
        Ok(group)
    }

    pub async fn get_one_tile(&self, x: i32, y: i32) -> Result<Tile, ServiceError> {
        let grp_key = GroupKey::from_point(x, y);

        let mut rows_stream = self.session
            .execute_iter(self.get_one_tile.clone(), (grp_key.0, grp_key.1, x, y))
            .await
            .map_err(|e| ServiceError::handle_fatal(e, "when selecting one tile"))?
            .rows_stream::<(i32, i32, (i8, i8, i8), CqlTimestamp)>()
            .map_err(|e| ServiceError::handle_fatal(e, "while extracting rows stream from select tile result"))?;

        match rows_stream.next().await {
            Some(row) => {
                let (x, y, rgb, last_updated_time) = row.map_err(|e| ServiceError::handle_fatal(e, "while streaming tile rows"))?;
                let date: String = format!("{}", Utc.timestamp_millis_opt(last_updated_time.0).unwrap().format("%Y/%m/%d %H:%M:%S"));
                let tile = Tile { x, y, rgb, date };
                info!("Selected one tile={:?}", tile);
                Ok(tile)
            },
            None => Err(ServiceError::NotFoundError(format!("tile not found at given location: {:?}", (x, y))))
        }
    }

    pub async fn get_placements(&self, after_time: DateTime<Utc>) -> Result<Vec<Placement>, ServiceError> {
        let after_millis = after_time.timestamp_millis();
        let hour = after_millis / (1000 * 60 * 60);
        
        info!("Getting placements with hour={}, after_time={}", hour, after_time);

        let mut rows_stream = self.session
            .execute_iter(self.get_placements.clone(), (hour, CqlTimestamp(after_millis)))
            .await
            .map_err(|e| ServiceError::handle_fatal(e, "when selecting placements"))?
            .rows_stream::<(i32, i32, (i8, i8, i8), IpAddr, CqlTimestamp)>()
            .map_err(|e| ServiceError::handle_fatal(e, "while extracting rows stream from select placement"))?;

        let mut placements = vec![];
        while let Some(row) = rows_stream.next().await {
            let (x, y, rgb, ipaddress, placement_time) = 
                row.map_err(|e| ServiceError::handle_fatal(e, "while streaming placement rows"))?;
            
            let placement_date: String = format!("{}", Utc.timestamp_millis_opt(placement_time.0).unwrap().format("%Y/%m/%d %H:%M:%S"));
            placements.push(Placement { x, y, rgb, ipaddress, placement_date })
        }
        Ok(placements)
    }

    pub async fn get_times_placed(&self, ipaddress: IpAddr) -> Result<i64, ServiceError> {
        let mut rows_stream = self.session
            .execute_iter(self.get_stats.clone(),  (ipaddress, ))
            .await
            .map_err(|e| ServiceError::handle_fatal(e, "when selecting stats"))?
            .rows_stream::<(Counter, )>()
            .map_err(|e| ServiceError::handle_fatal(e, "while extracting rows stream from select stats result"))?;

        match rows_stream.next().await {
            Some(row) => {
                let (times_placed, ) = row.map_err(|e| ServiceError::handle_fatal(e, "while streaming stats rows"))?;
                info!("Selected times_placed={} for ip={}", times_placed.0, ipaddress);
                Ok(times_placed.0)
            },
            None => Ok(0)
        }
    }

    pub async fn update_tile(&self, x: i32, y: i32, rgb: (i8, i8, i8), placer_ipaddress: IpAddr, time: SystemTime) -> Result<(), ServiceError> {
        let grp_key = GroupKey::from_point(x, y);
        let time_now = time.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        let placement_time = CqlTimestamp(time_now.as_millis() as i64);
        
        let values = (grp_key.0, grp_key.1, x, y, rgb, placer_ipaddress, placement_time);
        info!("Updating tile record=(grp_key={:?}, x={}, y={}, rgb={:?}, placer_ipaddress={:?}, placement_time={:?})", 
            grp_key, x, y, rgb, placer_ipaddress, placement_time);

        self.session.execute_unpaged(&self.update_tile, values)
            .await
            .map_err(|e| ServiceError::handle_fatal(e, "when updating tile"))?;
        Ok(())
    }

    pub async fn update_tile_now(&self, x: i32, y: i32, rgb: (i8, i8, i8), placer_ipaddress: IpAddr) -> Result<(), ServiceError> {
        self.update_tile(x, y, rgb, placer_ipaddress, SystemTime::now()).await
    }

    pub async fn increment_times_placed(&self, ipaddress: IpAddr) -> Result<(), ServiceError> {
        self.session.execute_unpaged(&self.increment_times_stat, (ipaddress, ))
            .await
            .map_err(|e| ServiceError::handle_fatal(e, "when incrementing times placed stat"))?;
        Ok(())
    }

    pub async fn insert_placement(&self, x: i32, y: i32, rgb: (i8, i8, i8), placer_ipaddress: IpAddr, time: SystemTime) -> Result<(), ServiceError> {
        let time_now = time.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        let hour = (time_now.as_secs() / (60 * 60)) as i64;
        let placement_time = CqlTimestamp(time_now.as_millis() as i64);

        let values = (hour, x, y, rgb, placer_ipaddress, placement_time);
        info!("Inserting a placement record=(hour={}, x={}, y={}, rgb={:?}, placer_ipaddress={:?}, placement_time={:?})", 
            hour, x, y, rgb, placer_ipaddress, placement_time);
        
        self.session.execute_unpaged(&self.insert_placement, values)
            .await
            .map_err(|e| ServiceError::handle_fatal(e, "when inserting placement"))?;
        Ok(())
    }
    
    pub async fn insert_placement_now(&self, x: i32, y: i32, rgb: (i8, i8, i8), placer_ipaddress: IpAddr) -> Result<(), ServiceError> {
        self.insert_placement(x, y, rgb, placer_ipaddress, SystemTime::now()).await
    }
}