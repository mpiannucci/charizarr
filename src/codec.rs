use serde_json::Value;

use crate::{data_type::CoreDataType, chunk::Chunk};

pub enum Codec {
    ByteToArray(Box<dyn ByteToArrayCodec>),
    ArrayToArray(Box<dyn ArrayToArrayCodec>),
    ByteToByte(Box<dyn ByteToByteCodec>),
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
    fn encode(&self, data_type: &CoreDataType, config: Value, data: &Chunk) -> Result<Vec<u8>, String>;
    fn decode(&self, data_type: &CoreDataType, config: Value, data: &[u8]) -> Result<Chunk, String>;
}

pub trait ArrayToArrayCodec: NamedCodec {
    fn encode(&self, data_type: &CoreDataType, config: Value, data: &Chunk) -> Result<Chunk, String>;
    fn decode(&self, data_type: &CoreDataType, config: Value, data: &Chunk) -> Result<Chunk, String>;
}

pub trait ByteToByteCodec: NamedCodec {
    fn encode(&self, data_type: &CoreDataType, config: Value, data: &[u8]) -> Result<Vec<u8>, String>;
    fn decode(&self, data_type: &CoreDataType, config: Value, data: &[u8]) -> Result<Vec<u8>, String>;
}