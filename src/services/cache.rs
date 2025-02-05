use redis::{Commands, Connection};
use crate::services::models::{GroupKey, ServiceError, TileGroup, GROUP_LEN};

pub fn set_tile_group(conn: &mut Connection, grp_key: GroupKey, group: TileGroup) -> Result<(), ServiceError> {
    let mut buffer = vec![];
    for row in group {
        for tile in row {
            buffer.push(tile);
        }
    }

    let grp_key_bytes = bincode::serialize(&grp_key)
        .map_err(|e| ServiceError::handle_fatal(e, "when serializing grp_key"))?;

    conn.set(grp_key_bytes, buffer)
        .map_err(|e| ServiceError::handle_fatal(e, "while setting tile group in cache using grp_key"))?;
    Ok(())
}

pub fn get_tile_group(conn: &mut Connection, grp_key: GroupKey) -> Result<TileGroup, ServiceError> {
    let grp_key_bytes = bincode::serialize(&grp_key)
        .map_err(|e| ServiceError::handle_fatal(e, "when serializing grp_key"))?;

    let buffer: Vec<u8> = conn.get(grp_key_bytes)
        .map_err(|e| ServiceError::handle_fatal(e, "while getting tile group in cache using grp_key"))?;

    let mut group = [[0i8; GROUP_LEN]; GROUP_LEN];
    let mut row = 0;
    let mut col = 0;
    for tile in buffer {
        if row >= GROUP_LEN {
            col += 1;
            row = 0;
        }
        group[row][col] = tile as i8;
    }
    assert_eq!(row, GROUP_LEN);
    assert_eq!(col, GROUP_LEN);
    
    Ok(group)
}

mod test {
    
}