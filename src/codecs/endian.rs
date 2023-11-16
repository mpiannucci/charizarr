use serde::{Serialize, Deserialize};
use serde_json::Value;

use crate::{codec::{NamedCodec, ByteToArrayCodec}, chunk::Chunk, data_type::CoreDataType};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Endian {
    Little,
    Big,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndianCodecConfig {
    endian: Endian
}

/// Adapted from https://zarr-specs.readthedocs.io/en/latest/v3/codecs/endian/v1.0.html
pub struct EndianCodec {}

impl EndianCodec {
    fn new() -> Self {
        Self {}
    }

    fn parse_config(&self, config: Value) -> Result<EndianCodecConfig, String> {
        serde_json::from_value::<EndianCodecConfig>(config).map_err(|e| e.to_string())
    }
}

impl NamedCodec for EndianCodec {
    fn resolve_name(&self) -> String {
        "endian".to_string()
    }
}

impl ByteToArrayCodec for EndianCodec {
    fn encode(&self, data_type: &CoreDataType, config: Value, data: &Chunk) -> Result<Vec<u8>, String> {
        let config = self.parse_config(config)?;

        // TODO: Encode ndarrays to bytes

        Err("Not implemented".to_string())
    }

    fn decode(&self, data_type: &CoreDataType, config: Value, data: &[u8]) -> Result<Chunk, String> {
        let config = self.parse_config(config)?;

        // TODO: Decode bytes to ndarrays

        Err("Not implemented".to_string())
    }
}