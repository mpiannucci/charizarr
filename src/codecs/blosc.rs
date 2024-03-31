use blosc::{decompress_bytes, Clevel, Compressor, Context, ShuffleMode};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    codec::{ByteToByteCodec, NamedCodec},
    metadata::DataType,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum BloscShuffle {
    NoShuffle,
    Shuffle,
    BitShuffle,
}

impl From<BloscShuffle> for ShuffleMode {
    fn from(value: BloscShuffle) -> Self {
        match value {
            BloscShuffle::NoShuffle => ShuffleMode::None,
            BloscShuffle::Shuffle => ShuffleMode::Byte,
            BloscShuffle::BitShuffle => ShuffleMode::Bit,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BloscCname {
    LZ4,
    LZ4HC,
    BloscLz,
    ZStd,
    Snappy,
    Zlib,
}

impl From<BloscCname> for Compressor {
    fn from(value: BloscCname) -> Self {
        match value {
            BloscCname::LZ4 => Compressor::LZ4,
            BloscCname::LZ4HC => Compressor::LZ4HC,
            BloscCname::BloscLz => Compressor::BloscLZ,
            BloscCname::ZStd => Compressor::Zstd,
            BloscCname::Snappy => Compressor::Snappy,
            BloscCname::Zlib => Compressor::Zlib,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloscCodecConfig {
    typesize: usize,
    cname: BloscCname,
    clevel: u8,
    shuffle: BloscShuffle,
    blocksize: usize,
}

impl BloscCodecConfig {
    pub fn normalized_blocksize(&self) -> Option<usize> {
        if self.blocksize == 0 {
            None
        } else {
            Some(self.blocksize)
        }
    }

    pub fn normalized_clevel(&self) -> Clevel {
        match self.clevel {
            0 => Clevel::None,
            1 => Clevel::L1,
            2 => Clevel::L2,
            3 => Clevel::L3,
            4 => Clevel::L4,
            5 => Clevel::L5,
            6 => Clevel::L6,
            7 => Clevel::L7,
            8 => Clevel::L8,
            9 => Clevel::L9,
            _ => Clevel::None,
        }
    }

    pub fn normalized_typesize(&self) -> Option<usize> {
        if self.shuffle == BloscShuffle::NoShuffle {
            None
        } else {
            Some(self.typesize)
        }
    }
}

impl From<BloscCodecConfig> for Context {
    fn from(config: BloscCodecConfig) -> Self {
        Context::new()
            .blocksize(config.normalized_blocksize())
            .compressor(config.cname.clone().into())
            .unwrap()
            .clevel(config.normalized_clevel())
            .shuffle(config.shuffle.clone().into())
            .typesize(config.normalized_typesize())
    }
}

/// Adapted from https://zarr-specs.readthedocs.io/en/latest/v3/codecs/blosc/v1.0.html
#[derive(Clone, Debug)]
pub struct BloscCodec {}

impl BloscCodec {
    pub fn new() -> Self {
        Self {}
    }

    fn parse_config(&self, config: &Value) -> Result<BloscCodecConfig, String> {
        serde_json::from_value::<BloscCodecConfig>(config.clone()).map_err(|e| e.to_string())
    }
}

impl NamedCodec for BloscCodec {
    fn resolve_name(&self) -> String {
        "blosc".to_string()
    }
}

impl ByteToByteCodec for BloscCodec {
    fn encode(
        &self,
        _data_type: &DataType,
        config: &Value,
        data: &[u8],
    ) -> Result<Vec<u8>, String> {
        let config = self.parse_config(config)?;
        let context = Context::from(config);

        let compressed = context.compress(data);
        Ok(compressed.into())
    }

    fn decode(
        &self,
        _data_type: &DataType,
        _config: &Value,
        data: &[u8],
    ) -> Result<Vec<u8>, String> {
        unsafe { decompress_bytes(data).map_err(|_| String::from("Failed to decompress data")) }
    }
}
