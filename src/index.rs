use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs,
    io::{Seek, SeekFrom},
};

use anyhow::Result;

pub type Offset = u64;

#[derive(Clone, Default, Serialize, Deserialize, Debug)]
pub struct Meta {
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub name: Vec<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub comment: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize, Debug, Hash, PartialEq, Eq)]
pub enum HashIdentifier {
    Sha3_256,
}

#[derive(Clone, Default, Serialize, Deserialize, Debug)]
pub struct Harddrive {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub serial: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,
}

#[derive(Clone, Default, Serialize, Deserialize, Debug)]
pub struct GPTPartitionTable {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guid: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drive: Option<Harddrive>,
}

#[derive(Clone, Default, Serialize, Deserialize, Debug)]
pub struct GPTPartition {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guid: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub no: Option<u32>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<GPTPartitionTable>,
}

#[derive(Clone, Default, Serialize, Deserialize, Debug)]
pub struct MBRPartitionTable {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_signature: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drive: Option<Harddrive>,
}

#[derive(Clone, Default, Serialize, Deserialize, Debug)]
pub struct MBRPartition {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guid: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<MBRPartitionTable>,
}

#[derive(Clone, Default, Serialize, Deserialize, Debug)]
pub struct Zpool {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guid: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_guid: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device: Option<Device>,
}

#[derive(Clone, Default, Serialize, Deserialize, Debug)]
pub struct Zvol {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guid: Option<String>,
}

#[derive(Clone, Default, Serialize, Deserialize, Debug)]
pub struct Tape {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub barcode: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub serial: Option<String>,
}

#[derive(Clone, Default, Serialize, Deserialize, Debug)]
pub struct TapeFile {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub no: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tape: Option<Tape>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub enum Device {
    Harddrive(Harddrive),
    MBRPartition(MBRPartition),
    GPTPartition(GPTPartition),
    Zvol(Zvol),
    Tape(Tape),
    TapeFile(TapeFile),
}

#[derive(Clone, Default, Serialize, Deserialize, Debug)]
pub struct File {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device: Option<Device>,
    #[serde(default)]
    #[serde(skip_serializing_if = "String::is_empty")]
    pub path: String,
}

impl File {
    pub fn as_location_data(self) -> LocationData {
        LocationData::File(self)
    }

    pub fn as_location(self) -> Location {
        self.as_location_data().as_location()
    }
}

#[derive(Clone, Default, Serialize, Deserialize, Debug)]
pub struct URI {
    pub uri: String,
}

#[derive(Clone, Default, Serialize, Deserialize, Debug)]
pub struct ThisBuffer;

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub enum LocationData {
    ThisBuffer(ThisBuffer),
    Device(Device),
    File(File),
    URI(URI),
}

impl LocationData {
    pub fn as_location(self) -> Location {
        Location {
            data: self,
            slice: None,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Location {
    #[serde(flatten)]
    pub data: LocationData,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slice: Option<Slice>,
}

#[derive(Clone, Default, Serialize, Deserialize, Debug)]
pub struct Slice {
    #[serde(default)]
    pub start: Offset,
    #[serde(default)]
    pub end: Offset,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Fragment {
    #[serde(flatten)]
    pub meta: Meta,
    #[serde(flatten)]
    pub location: Location,
    #[serde(default)]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub hashes: HashMap<HashIdentifier, String>,
    #[serde(flatten)]
    pub geometry: Slice,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub holes: Vec<Slice>,
}

#[derive(Clone, Default, Serialize, Deserialize, Debug)]
pub struct Index {
    #[serde(flatten)]
    pub meta: Meta,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub fragments: Vec<Fragment>,
}
