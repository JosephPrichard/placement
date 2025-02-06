use std::fmt::Debug;
use std::net::IpAddr;
use scylla::{DeserializeValue, SerializeValue};
use scylla::frame::value::CqlTimestamp;
use serde::{Deserialize, Serialize};
use tracing::log::error;

pub const GROUP_LEN: usize = 100;

#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct GroupKey(pub i32, pub i32);

impl GroupKey {
    pub fn from_point(x: i32, y: i32) -> GroupKey {
        let tiles_per_group = GROUP_LEN as i32;
        let group_from_x = (x / tiles_per_group) * tiles_per_group;
        let group_from_y = (y / tiles_per_group) * tiles_per_group;
        GroupKey(group_from_x, group_from_y)
    }
}

#[derive(Debug)]
pub struct Placement {
    pub x: i32,
    pub y: i32,
    pub color: i8,
    pub ipaddress: IpAddr,
    pub placement_time: CqlTimestamp,
}

impl PartialEq for Placement {
    fn eq(&self, other: &Self) -> bool {
        self.x == other.x && self.y == other.y && self.color == other.color && self.ipaddress == other.ipaddress
    }
}

impl Eq for Placement {}

#[derive(Debug)]
pub struct Stats {
    pub ipaddress: IpAddr,
    pub times_placed: i32,
}

#[derive(Debug)]
pub struct Tile {
    pub x: i32,
    pub y: i32,
    pub color: i8,
    pub last_updated_time: CqlTimestamp,
}

impl PartialEq for Tile {
    fn eq(&self, other: &Self) -> bool {
        self.x == other.x && self.y == other.y && self.color == other.color
    }
}

impl Eq for Tile {}

pub type TileGroup = [[i8; GROUP_LEN]; GROUP_LEN];

#[derive(Debug, PartialEq)]
pub enum ServiceError {
    FatalError(String),
    NotFoundError(String),
}

impl ServiceError {
    pub fn handle_fatal(e: impl Debug, m: &str) -> ServiceError {
        let m = format!("Fatal error has occurred {}: {:?}", m, e);
        error!("{}", m);
        ServiceError::FatalError(m)
    }
}

mod test {

}