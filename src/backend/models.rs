use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::net::IpAddr;
use tracing::log::error;

pub const GROUP_DIM: usize = 100;
pub const GROUP_DIM_I32: i32 = GROUP_DIM as i32;
pub const GROUP_LEN: usize = GROUP_DIM * GROUP_DIM * 3;

#[derive(Debug, PartialEq)]
pub enum ServiceError {
    NotFound(String), // indicates a resource couldn't be found, and the only way to handle that is to tell the client
    Fatal, // indicates something failed that wasn't supposed to fail
}

impl ServiceError {
    pub fn map_fatal(target: impl Debug, msg: &str) -> ServiceError {
        error!("Fatal error has occurred {}: {:?}", msg, target);
        ServiceError::Fatal
    }
}

#[derive(Debug, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct GroupKey(pub i32, pub i32);

impl GroupKey {
    pub fn from_point(x: i32, y: i32) -> GroupKey {
        let tiles_per_group = GROUP_DIM_I32;
        let group_from_x = (x / tiles_per_group) * tiles_per_group;
        let group_from_y = (y / tiles_per_group) * tiles_per_group;
        GroupKey(group_from_x, group_from_y)
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Placement {
    pub x: i32,
    pub y: i32,
    pub rgb: (u8, u8, u8),
    pub ipaddress: IpAddr,
    pub placement_date: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Tile {
    pub x: i32,
    pub y: i32,
    pub rgb: (u8, u8, u8),
    pub date: String,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct TileGroup(pub Vec<u8>);

impl TileGroup {
    pub fn empty() -> TileGroup {
        TileGroup(vec![])
    }
    
    pub fn get_offset(x: usize, y: usize) -> usize {
        (y * 3 * GROUP_DIM) + (x * 3)
    }

    pub fn set(&mut self, x: usize, y: usize, rgb: (u8, u8, u8)) {
        if self.0.is_empty() {
            self.0 = vec![0u8; GROUP_LEN];
        }
        let location = Self::get_offset(x, y);
        self.0[location] = rgb.0;
        self.0[location + 1] = rgb.1;
        self.0[location + 2] = rgb.2;
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Copy, Clone, Serialize, Deserialize)]
pub struct DrawEvent {
    pub x: i32,
    pub y: i32,
    pub rgb: (u8, u8, u8),
}
