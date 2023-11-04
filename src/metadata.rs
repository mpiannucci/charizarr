use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_repr::{Deserialize_repr, Serialize_repr};

#[derive(Serialize_repr, Deserialize_repr, PartialEq, Debug)]
#[repr(u8)]
pub enum ZarrFormat {
    V1 = 1,
    V2 = 2,
    V3 = 3,
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum DataType {
    Defined(String),
}

#[derive(Serialize, Deserialize)]
pub struct ArrayMetadata {
    zarr_format: ZarrFormat,
    node_type: String,
    shape: Vec<usize>,
    // TODO: with extensions
    data_type: String,
    // TODO: with extensions and configuration
    chunk_grid: String,
    // TODO: more complex type
    chunk_key_encoding: String,
    // TODO: Actual fill values
    fill_value: bool,
    // TODO: List of objects
    codecs: Vec<String>,
    atttributes: Option<HashMap<String, Value>>,
    // TODO: Very complex type
    storage_transformers: Option<Vec<String>>,
    dimension_names: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize)]
pub struct GroupMetadata {
    zarr_format: ZarrFormat,
    node_type: String,
    atttributes: Option<HashMap<String, Value>>,
}
