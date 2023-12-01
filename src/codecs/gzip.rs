use std::io::{Read, Write};

use serde::{Serialize, Deserialize};
use serde_json::Value;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression as GzCompression;

use crate::{codec::{ByteToByteCodec, NamedCodec}, data_type::CoreDataType};


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GZipCodecConfig {
    level: i8,
}

impl From<GZipCodecConfig> for GzCompression {
    fn from(config: GZipCodecConfig) -> Self {
        let level = if config.level < 0 || config.level > 9 {
            6
        } else {
            config.level
        };

        GzCompression::new(level as u32)
    }
}

/// Adapted from https://zarr-specs.readthedocs.io/en/latest/v3/codecs/gzip/v1.0.html
pub struct GZipCodec {}

impl GZipCodec {
    pub fn new() -> Self {
        Self {}
    }

    fn parse_config(&self, config: Value) -> Result<GZipCodecConfig, String> {
        serde_json::from_value::<GZipCodecConfig>(config).map_err(|e| e.to_string())
    }
}

impl NamedCodec for GZipCodec {
    fn resolve_name(&self) -> String {
        "gzip".to_string()
    }
}

impl ByteToByteCodec for GZipCodec {
    fn encode(&self, _data_type: &CoreDataType, config: Value, data: &[u8]) -> Result<Vec<u8>, String> {
        let level = self.parse_config(config)?.into();
        let mut encoder = GzEncoder::new(Vec::new(), level);
        encoder.write_all(data).map_err(|e| e.to_string())?;
        let out = encoder.finish().map_err(|e| e.to_string())?;
        Ok(out)
    }

    fn decode(&self, _data_type: &CoreDataType, _config: Value, data: &[u8]) -> Result<Vec<u8>, String> {
        let mut out = Vec::new();
        let _ = GzDecoder::new(data).read_to_end(&mut out).map_err(|e| e.to_string())?;
        Ok(out)
    }
}