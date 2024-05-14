use std::sync::Arc;

use serde_json::Value;

use crate::{error::CharizarrError, metadata::DataType, zarray::ZArray};

#[derive(Clone)]
pub enum Codec {
    ByteToArray(Arc<dyn ByteToArrayCodec>),
    ArrayToArray(Arc<dyn ArrayToArrayCodec>),
    ByteToByte(Arc<dyn ByteToByteCodec>),
}

impl Codec {
    pub fn name(&self) -> String {
        match self {
            Codec::ByteToArray(codec) => codec.resolve_name(),
            Codec::ArrayToArray(codec) => codec.resolve_name(),
            Codec::ByteToByte(codec) => codec.resolve_name(),
        }
    }

    pub fn matches(&self, name: &str) -> bool {
        self.name() == name
    }
}

pub trait NamedCodec {
    fn resolve_name(&self) -> String;
}

pub trait ByteToArrayCodec: NamedCodec {
    fn encode(
        &self,
        data_type: &DataType,
        config: &Value,
        data: &ZArray,
    ) -> Result<Vec<u8>, CharizarrError>;
    fn decode(
        &self,
        data_type: &DataType,
        config: &Value,
        data: &[u8],
    ) -> Result<ZArray, CharizarrError>;
}

pub trait ArrayToArrayCodec: NamedCodec {
    fn encode(
        &self,
        data_type: &DataType,
        config: &Value,
        data: &ZArray,
    ) -> Result<ZArray, CharizarrError>;
    fn decode(
        &self,
        data_type: &DataType,
        config: &Value,
        data: &ZArray,
    ) -> Result<ZArray, CharizarrError>;
}

pub trait ByteToByteCodec: NamedCodec {
    fn encode(
        &self,
        data_type: &DataType,
        config: &Value,
        data: &[u8],
    ) -> Result<Vec<u8>, CharizarrError>;
    fn decode(
        &self,
        data_type: &DataType,
        config: &Value,
        data: &[u8],
    ) -> Result<Vec<u8>, CharizarrError>;
}
