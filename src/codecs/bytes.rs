use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    zarray::ZArray,
    codec::{ByteToArrayCodec, NamedCodec},
    data_type::CoreDataType,
    error::CharizarrError,
    metadata::DataType,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Endian {
    Little,
    Big,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BytesCodecConfig {
    endian: Endian,
}

/// Adapted from https://zarr-specs.readthedocs.io/en/latest/v3/codecs/endian/v1.0.html
#[derive(Clone, Debug)]
pub struct BytesCodec {}

impl BytesCodec {
    pub fn new() -> Self {
        Self {}
    }

    fn parse_config(&self, config: &Value) -> Result<BytesCodecConfig, CharizarrError> {
        serde_json::from_value::<BytesCodecConfig>(config.clone())
            .map_err(|e| CharizarrError::CodecError(e.to_string()))
    }
}

impl NamedCodec for BytesCodec {
    fn resolve_name(&self) -> String {
        "bytes".to_string()
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

impl ByteToArrayCodec for BytesCodec {
    fn encode(
        &self,
        _data_type: &DataType,
        config: &Value,
        data: &ZArray,
    ) -> Result<Vec<u8>, CharizarrError> {
        let config = self.parse_config(config)?;

        match data {
            ZArray::Bool(arr) => Ok(arr
                .iter()
                .flat_map(|x| (*x as u8).to_be_bytes())
                .collect::<Vec<u8>>()),
            ZArray::Int8(arr) => Ok(encode_endian_chunk!(config.endian, arr, i8)),
            ZArray::Int16(arr) => Ok(encode_endian_chunk!(config.endian, arr, i16)),
            ZArray::Int32(arr) => Ok(encode_endian_chunk!(config.endian, arr, i32)),
            ZArray::Int64(arr) => Ok(encode_endian_chunk!(config.endian, arr, i64)),
            ZArray::UInt8(arr) => Ok(encode_endian_chunk!(config.endian, arr, u8)),
            ZArray::UInt16(arr) => Ok(encode_endian_chunk!(config.endian, arr, u16)),
            ZArray::UInt32(arr) => Ok(encode_endian_chunk!(config.endian, arr, u32)),
            ZArray::UInt64(arr) => Ok(encode_endian_chunk!(config.endian, arr, u64)),
            ZArray::Float32(arr) => Ok(encode_endian_chunk!(config.endian, arr, f32)),
            ZArray::Float64(arr) => Ok(encode_endian_chunk!(config.endian, arr, f64)),
            ZArray::Complex64(_) => Err(CharizarrError::UnimplementedError(
                "This is ignored for now",
            )),
            ZArray::Complex128(_) => Err(CharizarrError::UnimplementedError(
                "This is ignored for now",
            )),
            ZArray::Raw8(_) => Err(CharizarrError::UnimplementedError(
                "This is ignored for now",
            )),
            ZArray::Raw16(_) => Err(CharizarrError::UnimplementedError(
                "This is ignored for now",
            )),
        }
    }

    fn decode(
        &self,
        data_type: &DataType,
        config: &Value,
        data: &[u8],
    ) -> Result<ZArray, CharizarrError> {
        let config = self.parse_config(config)?;
        let DataType::Core(data_type) = data_type else {
            return Err(CharizarrError::CodecError("Invalid data type".to_string()));
        };

        match data_type {
            CoreDataType::Int8 => Ok(decode_endian_chunk!(config.endian, data, 1, i8)),
            CoreDataType::Bool => Ok(data
                .iter()
                .step_by(1)
                .map(|x| u8::from_be_bytes([*x]) > 0)
                .collect::<Vec<bool>>()
                .into()),
            CoreDataType::Int16 => Ok(decode_endian_chunk!(config.endian, data, 2, i16)),
            CoreDataType::Int32 => Ok(decode_endian_chunk!(config.endian, data, 4, i32)),
            CoreDataType::Int64 => Ok(decode_endian_chunk!(config.endian, data, 8, i64)),
            CoreDataType::UInt8 => Ok(decode_endian_chunk!(config.endian, data, 1, u8)),
            CoreDataType::UInt16 => Ok(decode_endian_chunk!(config.endian, data, 2, u16)),
            CoreDataType::UInt32 => Ok(decode_endian_chunk!(config.endian, data, 4, u32)),
            CoreDataType::UInt64 => Ok(decode_endian_chunk!(config.endian, data, 8, u64)),
            CoreDataType::Float32 => Ok(decode_endian_chunk!(config.endian, data, 4, f32)),
            CoreDataType::Float64 => Ok(decode_endian_chunk!(config.endian, data, 8, f64)),
            CoreDataType::Complex64 => Err(CharizarrError::UnimplementedError(
                "This is ignored for now",
            )),
            CoreDataType::Complex128 => Err(CharizarrError::UnimplementedError(
                "This is ignored for now",
            )),
            CoreDataType::Raw8 => Err(CharizarrError::UnimplementedError(
                "This is ignored for now",
            )),
            CoreDataType::Raw16 => Err(CharizarrError::UnimplementedError(
                "This is ignored for now",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use ndarray::prelude::*;

    use super::*;

    #[test]
    fn test_bytes_codec() {
        let codec = BytesCodec::new();

        let config = serde_json::json!({
            "endian": "little"
        });

        let data_type = DataType::Core(CoreDataType::Int32);
        let i_array = Array::from_vec(vec![1, 2, 3, 4]).into_dyn();
        let data = ZArray::Int32(i_array.clone());

        let encoded = codec.encode(&data_type, &config, &data).unwrap();
        let decoded = codec.decode(&data_type, &config, &encoded).unwrap();
        let o_array = match decoded {
            ZArray::Int32(arr) => Some(arr),
            _ => None,
        }
        .unwrap();

        assert_eq!(i_array, o_array);
    }
}
