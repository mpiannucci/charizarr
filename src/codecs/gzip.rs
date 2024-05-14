use std::io::{Read, Write};

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression as GzCompression;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    codec::{ByteToByteCodec, NamedCodec},
    error::CharizarrError,
    metadata::DataType,
};

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
#[derive(Clone, Debug)]
pub struct GZipCodec {}

impl GZipCodec {
    pub fn new() -> Self {
        Self {}
    }

    fn parse_config(&self, config: &Value) -> Result<GZipCodecConfig, CharizarrError> {
        serde_json::from_value::<GZipCodecConfig>(config.clone())
            .map_err(|e| CharizarrError::CodecError(e.to_string()))
    }
}

impl NamedCodec for GZipCodec {
    fn resolve_name(&self) -> String {
        "gzip".to_string()
    }
}

impl ByteToByteCodec for GZipCodec {
    fn encode(
        &self,
        _data_type: &DataType,
        config: &Value,
        data: &[u8],
    ) -> Result<Vec<u8>, CharizarrError> {
        let level = self.parse_config(config)?.into();
        let mut encoder = GzEncoder::new(Vec::new(), level);
        encoder
            .write_all(data)
            .map_err(|e| CharizarrError::CodecError(e.to_string()))?;
        let out = encoder
            .finish()
            .map_err(|e| CharizarrError::CodecError(e.to_string()))?;
        Ok(out)
    }

    fn decode(
        &self,
        _data_type: &DataType,
        _config: &Value,
        data: &[u8],
    ) -> Result<Vec<u8>, CharizarrError> {
        let mut out = Vec::new();
        let _ = GzDecoder::new(data)
            .read_to_end(&mut out)
            .map_err(|e| CharizarrError::CodecError(e.to_string()))?;
        Ok(out)
    }
}
