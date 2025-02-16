use std::fmt::Debug;
use std::net::IpAddr;
use serde::{Deserialize, Serialize};

pub const GROUP_DIM: usize = 100;
pub const GROUP_DIM_I32: i32 = GROUP_DIM as i32;
pub const GROUP_LEN: usize = GROUP_DIM * GROUP_DIM * 3;

#[derive(Debug, PartialEq)]
pub enum ServiceError {
    FatalError(String),
    NotFoundError(String),
}

impl ServiceError {
    pub fn handle_fatal(e: impl Debug, m: &str) -> ServiceError {
        let m = format!("Fatal error has occurred {}: {:?}", m, e);
        ServiceError::FatalError(m)
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
    pub rgb: (i8, i8, i8),
    pub ipaddress: IpAddr,
    pub placement_date: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Tile {
    pub x: i32,
    pub y: i32,
    pub rgb: (i8, i8, i8),
    pub date: String,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct TileGroup(pub i32, pub i32, pub Vec<u8>);

impl TileGroup {
    pub fn empty(x: i32, y: i32) -> TileGroup {
        TileGroup(x, y, vec![])
    }

    pub fn set(&mut self, x: usize, y: usize, rgb: (i8, i8, i8)) {
        if self.2.is_empty() {
            self.2 = vec![0u8; GROUP_LEN];
        }
        let location = (y * 3 * GROUP_DIM) + (x * 3);
        self.2[location] = rgb.0 as u8;
        self.2[location + 1] = rgb.1 as u8;
        self.2[location + 2] = rgb.2 as u8;
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Copy, Clone, Serialize, Deserialize)]
pub struct DrawMsg {
    pub x: i32,
    pub y: i32,
    pub rgb: (i8, i8, i8),
}

#[derive(Debug, PartialEq, Deserialize)]
pub enum Input {
    DrawTile(DrawMsg),
    GetGroup(GroupKey),
    GetTileInfo((i32, i32)),
}

#[derive(Debug, PartialEq, Serialize)]
pub enum TextOutput {
    DrawEvent(DrawMsg),
    TileInfo(Tile),
    Err(String),
}

#[derive(Debug, PartialEq, Serialize)]
pub enum BinaryOutput {
    Group(TileGroup),
}