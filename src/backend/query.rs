use crate::backend::models::{GroupKey, Placement, ServiceError, Tile, TileGroup};
use futures_util::StreamExt;
use scylla::{frame::value::CqlTimestamp, prepared_statement::PreparedStatement, transport::errors::QueryError, Session};
use std::{net::IpAddr, time::SystemTime};
use std::time::Duration;
use scylla::batch::Batch;
use tracing::log::info;
use crate::backend::utils::{epoch_to_iso_string, epoch_to_day};

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
        CREATE TABLE IF NOT EXISTS pks.placements (
            day bigint,
            x int,
            y int,
            rgb tuple<tinyint, tinyint, tinyint>,
            ipaddress inet,
            placement_time timestamp,
            PRIMARY KEY (day, placement_time, ipaddress));", ()).await?;
    session.query_unpaged("\
        CREATE TABLE IF NOT EXISTS pks.users (
            ipaddress inet,
            placed_time timestamp,
            PRIMARY KEY (ipaddress));", ()).await?;
    Ok(())
}

async fn batch_upsert_tile(session: &Session) -> Result<Batch, QueryError> {
    let upsert_tile_query = r#"
        INSERT INTO pks.tiles (group_x, group_y, x, y, rgb, last_updated_ipaddress, last_updated_time)
        VALUES (?, ?, ?, ?, ?, ?, ?);"#;
    let insert_placement_query = "INSERT INTO pks.placements (day, x, y, rgb, ipaddress, placement_time) VALUES (?, ?, ?, ?, ?, ?);";
    let update_users_query = "UPDATE pks.users SET placed_time = ? WHERE ipaddress = ? IF placed_time < ?;";

    let mut batch: Batch = Default::default();
    batch.append_statement(upsert_tile_query);
    batch.append_statement(insert_placement_query);

    let batch: Batch = session.prepare_batch(&batch).await?;
    Ok(batch)
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
        SELECT x, y, rgb, placement_time
        FROM pks.placements
        WHERE day = ?
        ORDER BY placement_time DESC;"#;
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
    pub get_one_tile: PreparedStatement,
    pub get_tile_group: PreparedStatement,
    pub get_placements: PreparedStatement,
    pub batch_update_tile: Batch,
}

impl QueryStore {
    pub async fn init_queries(session: Session) -> Result<QueryStore, ServiceError> {
        let mut get_one_tile = get_one_tile(&session)
            .await
            .map_err(|e| ServiceError::map_fatal(e, "while creating get_one_tile query"))?;
        let mut get_tile_group = get_tile_group(&session)
            .await
            .map_err(|e| ServiceError::map_fatal(e, "while creating get_tile_group query"))?;
        let mut get_placements = get_placements(&session)
            .await
            .map_err(|e| ServiceError::map_fatal(e, "while creating get_placements query"))?;
        let mut batch_update_tile = batch_upsert_tile(&session)
            .await
            .map_err(|e| ServiceError::map_fatal(e, "while creating batched update_tile query"))?;

        get_one_tile.set_is_idempotent(true);
        get_tile_group.set_is_idempotent(true);
        get_placements.set_is_idempotent(true);
        batch_update_tile.set_is_idempotent(true);
        
        Ok(QueryStore { get_one_tile, get_tile_group, get_placements, batch_update_tile, session })
    }

    pub async fn get_tile_group(&self, key: GroupKey) -> Result<TileGroup, ServiceError> {
        let mut rows_stream = self.session
            .execute_iter(self.get_tile_group.clone(), (key.0, key.1))
            .await
            .map_err(|e| ServiceError::map_fatal(e, "when selecting tile group"))?
            .rows_stream::<(i32, i32, (i8, i8, i8))>()
            .map_err(|e| ServiceError::map_fatal(e, "while extracting rows stream from select tile group result"))?;
        
        let mut group = TileGroup::empty();
        while let Some(row) = rows_stream.next().await {
            let (x, y, rgb) = row.map_err(|e| ServiceError::map_fatal(e, "while streaming tile rows"))?;

            let rgb = (rgb.0 as u8, rgb.1 as u8, rgb.2 as u8); // cast to from signed integers because scylla can only stored signed integers

            let x_offset = (x - key.0).abs() as usize;
            let y_offset = (y - key.1).abs() as usize;
            group.set(x_offset, y_offset, rgb);
        }

        info!("Selected tile group with x={}, y={}", key.0, key.1);
        Ok(group)
    }

    pub async fn get_one_tile(&self, x: i32, y: i32) -> Result<Tile, ServiceError> {
        let grp_key = GroupKey::from_point(x, y);

        let mut rows_stream = self.session
            .execute_iter(self.get_one_tile.clone(), (grp_key.0, grp_key.1, x, y))
            .await
            .map_err(|e| ServiceError::map_fatal(e, "when selecting one tile"))?
            .rows_stream::<(i32, i32, (i8, i8, i8), CqlTimestamp)>()
            .map_err(|e| ServiceError::map_fatal(e, "while extracting rows stream from select tile result"))?;

        match rows_stream.next().await {
            Some(row) => {
                let (x, y, rgb, last_updated_time) = row.map_err(|e| ServiceError::map_fatal(e, "while streaming tile rows"))?;

                let date = epoch_to_iso_string(last_updated_time.0)?;
                let rgb = (rgb.0 as u8, rgb.1 as u8, rgb.2 as u8); // cast to from signed integers because scylla can only stored signed integers
                let tile = Tile { x, y, rgb, date };
                
                info!("Selected one tile={:?}", tile);
                Ok(tile)
            },
            None => Err(ServiceError::NotFound(format!("tile not found at given location: {:?}", (x, y))))
        }
    }

    pub async fn get_placements_in_day(&self, day: i64) -> Result<Vec<Placement>, ServiceError> {
        info!("Getting placements with day={}", day);

        let mut rows_stream = self.session
            .execute_iter(self.get_placements.clone(), (day, ))
            .await
            .map_err(|e| ServiceError::map_fatal(e, "when selecting placements"))?
            .rows_stream::<(i32, i32, (i8, i8, i8), CqlTimestamp)>()
            .map_err(|e| ServiceError::map_fatal(e, "while extracting rows stream from select placement"))?;

        let mut placements = vec![];
        while let Some(row) = rows_stream.next().await {
            let (x, y, rgb, placement_time) = 
                row.map_err(|e| ServiceError::map_fatal(e, "while streaming placement rows"))?;

            let placement_date = epoch_to_iso_string(placement_time.0)?;
            let rgb = (rgb.0 as u8, rgb.1 as u8, rgb.2 as u8); // cast to from signed integers because scylla can only stored signed integers
            placements.push(Placement { x, y, rgb, placement_date })
        }
        Ok(placements)
    }

    pub async fn batch_upsert_tile(&self, x: i32, y: i32, rgb: (u8, u8, u8), ipaddress: IpAddr, time_now: SystemTime) -> Result<(), ServiceError> {
        let grp_key = GroupKey::from_point(x, y);

        let duration_now = time_now.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        
        let day_now = epoch_to_day(duration_now);
        let placement_time = CqlTimestamp(duration_now.as_millis() as i64);

        let rgb = (rgb.0 as i8, rgb.1 as i8, rgb.2 as i8); // cast to signed integers because scylla can only stored signed integers

        info!("Updating tile record=(grp_key={:?}, x={}, y={}, rgb={:?}, placer_ipaddress={:?}, placement_time={:?})",
            grp_key, x, y, rgb, ipaddress, placement_time);
        info!("Inserting a placement record=(day={}, x={}, y={}, rgb={:?}, placer_ipaddress={:?}, placement_time={:?})",
            day_now, x, y, rgb, ipaddress, placement_time);

        let upsert_tile_values = (grp_key.0, grp_key.1, x, y, rgb, ipaddress, placement_time);
        let insert_placement_values = (day_now, x, y, rgb, ipaddress, placement_time);

        self.session.batch(&self.batch_update_tile, (upsert_tile_values, insert_placement_values))
            .await
            .map_err(|e| ServiceError::map_fatal(e, "when executing batch update_tile"))?;
        Ok(())
    }
}

