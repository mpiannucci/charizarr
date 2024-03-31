use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    chunk::Chunk,
    codec::Codec,
    codec_registry::CodecRegistry,
    codecs::{self, bytes::BytesCodec},
    data_type::CoreDataType,
    metadata::{DataType, Extension, ZarrFormat},
    store::{ListableStore, ReadableStore, WriteableStore},
};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ArrayMetadata {
    pub zarr_format: ZarrFormat,
    pub node_type: String,
    pub shape: Vec<usize>,
    pub data_type: DataType,
    pub chunk_grid: Extension,
    pub chunk_key_encoding: Extension,
    // TODO: Actual fill values
    pub fill_value: Value,
    pub codecs: Vec<Extension>,
    pub attributes: Option<HashMap<String, Value>>,
    pub storage_transformers: Option<Vec<Extension>>,
    pub dimension_names: Option<Vec<String>>,
}

pub struct Array<'a, T>
where
    T: ReadableStore + ListableStore + WriteableStore,
{
    store: &'a T,
    codecs: CodecRegistry,
    pub meta: ArrayMetadata,
    pub path: String,
}

impl<'a, T> Array<'a, T>
where
    T: ReadableStore + ListableStore + WriteableStore,
{
    pub async fn open(
        store: &'a T,
        path: Option<String>,
        codecs: Option<CodecRegistry>,
    ) -> Result<Self, String> {
        let path = path.map_or_else(|| "".to_string(), |p| format!("{p}/"));
        let metadata_path = format!("{path}zarr.json");
        let raw_metadata = store.get(&metadata_path).await?;
        let meta = serde_json::from_slice::<ArrayMetadata>(&raw_metadata)
            .map_err(|e| format!("Failed to parse group metadata: {e}"))?;

        let codecs = codecs.unwrap_or_else(|| CodecRegistry::default());

        Ok(Self {
            store,
            codecs,
            meta,
            path,
        })
    }

    pub async fn get_raw_chunk(&self, key: &str) -> Result<Vec<u8>, String> {
        let chunk_path = format!("{path}{key}", path = self.path);
        self.store.get(&chunk_path).await
    }

    pub async fn get_chunk(&self, key: &str) -> Result<Chunk, String> {
        let bytes = self.get_raw_chunk(key).await?;
        let data_type = self.dtype();

        let mut btb_codecs = vec![];
        let mut bta_codecs = vec![];
        let mut ata_codecs = vec![];

        self.meta.codecs.iter().rev().for_each(|codec| {
            let config = codec.configuration.clone();
            let Some(codec) = self.codecs.get(&codec.name) else {
                println!("Codec not found: {}", codec.name);
                return;
            };
            match codec {
                Codec::ByteToByte(codec) => btb_codecs.push((codec, config)),
                Codec::ByteToArray(codec) => bta_codecs.push((codec, config)),
                Codec::ArrayToArray(codec) => ata_codecs.push((codec, config)),
            }
        });

        // byte to byte
        let bytes = btb_codecs.iter().fold(bytes, |bytes, codec| {
            let (codec, config) = codec;
            println!("Decoding with codec: {}", codec.resolve_name());
            codec.decode(data_type, config, &bytes).unwrap()
        });

        // byte to array
        let (bta_codec, bta_config) = bta_codecs.first().unwrap();
        let arr = bta_codec.decode(data_type, bta_config, &bytes).unwrap();

        // array to array
        let arr = ata_codecs.iter().fold(arr, |arr, (codec, config)| {
            codec.decode(data_type, config, &arr).unwrap()
        });

        Ok(arr)
    }

    pub async fn set_raw_chunk(&self, key: &str, data: &[u8]) -> Result<(), String> {
        let chunk_path = format!("{path}{key}", path = self.path);
        self.store.set(&chunk_path, data).await
    }

    /// The data type of the array
    pub fn dtype(&self) -> &DataType {
        &self.meta.data_type
    }

    /// Get the shape of the entire array
    pub fn shape(&self) -> Vec<usize> {
        self.meta.shape.to_vec()
    }

    /// Get the shape of a single chunk
    /// TODO: Should this take into account grid type?
    pub fn chunk_shape(&self) -> Vec<usize> {
        self.meta
            .chunk_grid
            .configuration
            .get("chunk_shape")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap() as usize)
            .collect::<Vec<usize>>()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{
        data_type::CoreDataType,
        metadata::{DataType, ZarrFormat},
    };

    use super::*;

    #[test]
    fn parse_array_metadata() {
        let metadata = r#"
            {
                "zarr_format": 3,
                "node_type": "array",
                "shape": [10000, 1000],
                "dimension_names": ["rows", "columns"],
                "data_type": "float64",
                "chunk_grid": {
                    "name": "regular",
                    "configuration": {
                        "chunk_shape": [1000, 100]
                    }
                },
                "chunk_key_encoding": {
                    "name": "default",
                    "configuration": {
                        "separator": "/"
                    }
                },
                "codecs": [{
                    "name": "gzip",
                    "configuration": {
                        "level": 1
                    }
                }],
                "fill_value": "NaN",
                "attributes": {
                    "foo": 42,
                    "bar": "apples",
                    "baz": [1, 2, 3, 4]
                }
            }
            "#;

        let array_metadata = serde_json::from_str::<ArrayMetadata>(metadata);
        assert!(array_metadata.is_ok());
        let array_metadata = array_metadata.unwrap();
        assert_eq!(&array_metadata.node_type, "array");
        assert_eq!(array_metadata.zarr_format, ZarrFormat::V3);
        assert_eq!(array_metadata.shape, vec![10000, 1000]);

        let data_type = match array_metadata.data_type {
            DataType::Core(ref dtype) => dtype,
            _ => panic!("Expected defined data type"),
        };
        assert_eq!(*data_type, CoreDataType::Float64);

        let chunk_grid = array_metadata.chunk_grid;

        assert_eq!(&chunk_grid.name, "regular");
        assert_eq!(chunk_grid.configuration["chunk_shape"], json!([1000, 100]));
    }
}
