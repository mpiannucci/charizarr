use std::{collections::HashMap, sync::Arc};

use crate::{codec::Codec, codecs::bytes::BytesCodec};

#[derive(Clone)]
pub struct CodecRegistry {
    codecs: HashMap<String, Codec>,
}

impl Default for CodecRegistry {
    fn default() -> Self {
        let bytes_codec = Codec::ByteToArray(Arc::new(BytesCodec::new()));

        let mut codecs = HashMap::new();
        codecs.insert(bytes_codec.name(), bytes_codec);

        Self {
            codecs,
        }
    }
}

impl CodecRegistry {
    pub fn register(mut self, codec: Codec) -> Self {
        self.codecs.insert(codec.name(), codec);
        self
    }

    pub fn get(&self, name: &str) -> Option<&Codec> {
        self.codecs.get(name)
    }
}
