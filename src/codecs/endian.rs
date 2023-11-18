use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    chunk::Chunk,
    codec::{ByteToArrayCodec, NamedCodec},
    data_type::CoreDataType,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Endian {
    Little,
    Big,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndianCodecConfig {
    endian: Endian,
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

macro_rules! decode_endian_chunk {
    ($m_name:expr, $d_name:expr, $w:expr, $d_type:ident) => {
        match $m_name {
            Endian::Little => $d_name
                .chunks($w)
                .map(|x| {
                    $d_type::from_le_bytes(x.try_into().expect("Failed to convert bytes to array"))
                })
                .collect::<Vec<$d_type>>()
                .into(),
            Endian::Big => $d_name
                .chunks($w)
                .map(|x| {
                    $d_type::from_be_bytes(x.try_into().expect("Failed to convert bytes to array"))
                })
                .collect::<Vec<$d_type>>()
                .into(),
        }
    };
}

macro_rules! encode_endian_chunk {
    ($m_name:expr, $d_name:expr, $d_type:ident) => {
        match $m_name {
            Endian::Little => $d_name
                .iter()
                .flat_map(|x| x.to_le_bytes().into_iter().collect::<Vec<u8>>())
                .collect::<Vec<u8>>(),
            Endian::Big => $d_name
                .iter()
                .flat_map(|x| x.to_be_bytes().into_iter().collect::<Vec<u8>>())
                .collect::<Vec<u8>>(),
        }
    };
}

impl ByteToArrayCodec for EndianCodec {
    fn encode(
        &self,
        data_type: &CoreDataType,
        config: Value,
        data: &Chunk,
    ) -> Result<Vec<u8>, String> {
        let config = self.parse_config(config)?;

        // TODO: Encode ndarrays to bytes
        let encoded = match data {
            Chunk::Bool(arr) => arr
                .iter()
                .flat_map(|x| (*x as u8).to_be_bytes())
                .collect::<Vec<u8>>(),
            Chunk::Int8(arr) => encode_endian_chunk!(config.endian, arr, i8),
            Chunk::Int16(arr) => encode_endian_chunk!(config.endian, arr, i16),
            Chunk::Int32(arr) => encode_endian_chunk!(config.endian, arr, i32),
            Chunk::Int64(arr) => encode_endian_chunk!(config.endian, arr, i64),
            Chunk::UInt8(arr) => encode_endian_chunk!(config.endian, arr, u8),
            Chunk::UInt16(arr) => encode_endian_chunk!(config.endian, arr, u16),
            Chunk::UInt32(arr) => encode_endian_chunk!(config.endian, arr, u32),
            Chunk::UInt64(arr) => encode_endian_chunk!(config.endian, arr, u64),
            Chunk::Float32(arr) => encode_endian_chunk!(config.endian, arr, f32),
            Chunk::Float64(arr) => encode_endian_chunk!(config.endian, arr, f64),
            Chunk::Complex64(_) => todo!("This is ignored for now"),
            Chunk::Complex128(_) => todo!("This is ignored for now"),
            Chunk::Raw8(_) => todo!("This is ignored for now"),
            Chunk::Raw16(_) => todo!("This is ignored for now"),
        };

        Ok(encoded)
    }

    fn decode(
        &self,
        data_type: &CoreDataType,
        config: Value,
        data: &[u8],
    ) -> Result<Chunk, String> {
        let config = self.parse_config(config)?;

        let decoded = match data_type {
            CoreDataType::Int8 => decode_endian_chunk!(config.endian, data, 1, i8),
            CoreDataType::Bool => data
                .iter()
                .step_by(1)
                .map(|x| u8::from_be_bytes([*x]) > 0)
                .collect::<Vec<bool>>()
                .into(),
            CoreDataType::Int16 => decode_endian_chunk!(config.endian, data, 2, i16),
            CoreDataType::Int32 => decode_endian_chunk!(config.endian, data, 4, i32),
            CoreDataType::Int64 => decode_endian_chunk!(config.endian, data, 8, i64),
            CoreDataType::UInt8 => decode_endian_chunk!(config.endian, data, 1, u8),
            CoreDataType::UInt16 => decode_endian_chunk!(config.endian, data, 2, u16),
            CoreDataType::UInt32 => decode_endian_chunk!(config.endian, data, 4, u32),
            CoreDataType::UInt64 => decode_endian_chunk!(config.endian, data, 8, u64),
            CoreDataType::Float32 => decode_endian_chunk!(config.endian, data, 4, f32),
            CoreDataType::Float64 => decode_endian_chunk!(config.endian, data, 8, f64),
            CoreDataType::Complex64 => todo!("This is ignored for now"),
            CoreDataType::Complex128 => todo!("This is ignored for now"),
            CoreDataType::Raw8 => todo!("This is ignored for now"),
            CoreDataType::Raw16 => todo!("This is ignored for now"),
        };

        Ok(decoded)
    }
}

#[cfg(test)]
mod tests {
    use ndarray::prelude::*;

    use super::*;

    #[test]
    fn test_endian_codec() {
        let codec = EndianCodec::new();

        let config = serde_json::json!({
            "endian": "little"
        });

        let i_array = Array::from_vec(vec![1, 2, 3, 4]).into_dyn();
        let data = Chunk::Int32(i_array.clone());

        let encoded = codec.encode(&CoreDataType::Int32, config.clone(), &data).unwrap();
        let decoded = codec.decode(&CoreDataType::Int32, config, &encoded).unwrap();
        let o_array = match decoded {
            Chunk::Int32(arr) => Some(arr),
            _ => None,
        }.unwrap();

        assert_eq!(i_array, o_array);
    }
}