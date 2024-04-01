use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::data_type::CoreDataType;

pub type Configuration = Value;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Extension {
    pub name: String,
    pub configuration: Configuration,
}

#[derive(Serialize_repr, Deserialize_repr, PartialEq, Debug, Clone)]
#[repr(u8)]
pub enum ZarrFormat {
    V1 = 1,
    V2 = 2,
    V3 = 3,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum NodeType {
    Group,
    Array,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(untagged)]
pub enum DataType {
    Core(CoreDataType),
    Extension(Extension),
}
