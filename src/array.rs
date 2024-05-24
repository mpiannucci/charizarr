use std::{collections::HashMap, ops::Range};

use futures::future::try_join_all;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    chunk::{decode_chunk, encode_chunk},
    codec_registry::CodecRegistry,
    error::CharizarrError,
    index::BasicIndexIterator,
    metadata::{DataType, Extension, NodeType, ZarrFormat},
    store::{ListableStore, ReadableStore, WriteableStore},
    zarray::ZArray,
};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ArrayMetadata {
    pub zarr_format: ZarrFormat,
    pub node_type: NodeType,
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
    codec_registry: CodecRegistry,
    pub metadata: ArrayMetadata,
    pub path: String,
}

impl<'a, T> Array<'a, T>
where
    T: ReadableStore + ListableStore + WriteableStore,
{
    /// Open an existing array from a store. If the zarr.json metadata file is not found,
    /// an error is returned.
    pub async fn open(
        store: &'a T,
        path: Option<String>,
        codec_registry: Option<CodecRegistry>,
    ) -> Result<Self, CharizarrError> {
        let path = path.map_or_else(|| "".to_string(), |p| format!("{p}/"));
        let metadata_path = format!("{path}zarr.json");
        let raw_metadata = store.get(&metadata_path).await?;
        let meta = serde_json::from_slice::<ArrayMetadata>(&raw_metadata)
            .map_err(|e| CharizarrError::ArrayError(format!("Failed to parse metadata: {e}")))?;

        let codec_registry = codec_registry.unwrap_or_else(|| CodecRegistry::default());

        Ok(Self {
            store,
            codec_registry,
            metadata: meta,
            path,
        })
    }

    pub async fn create(
        store: &'a T,
        path: Option<String>,
        codec_registry: Option<CodecRegistry>,
        shape: Vec<usize>,
        chunk_shape: Vec<usize>,
        chunk_key_encoding: Option<Extension>,
        data_type: DataType,
        fill_value: Value,
        codecs: Vec<Extension>,
        dimension_names: Option<Vec<String>>,
        attributes: Option<HashMap<String, Value>>,
    ) -> Result<Self, CharizarrError> {
        let path = path.map_or_else(|| "".to_string(), |p| format!("{p}/"));
        let metadata_path = format!("{path}zarr.json");

        let chunk_key_encoding = chunk_key_encoding.unwrap_or_else(|| Extension {
            name: "default".to_string(),
            configuration: serde_json::json!({ "separator": "/" }),
        });

        // Only regular grids are supported for now
        let chunk_grid = Extension {
            name: "regular".to_string(),
            configuration: serde_json::json!({ "chunk_shape": chunk_shape }),
        };

        let metadata = ArrayMetadata {
            zarr_format: ZarrFormat::V3,
            node_type: NodeType::Array,
            shape,
            data_type,
            chunk_grid,
            chunk_key_encoding,
            fill_value,
            codecs,
            attributes,
            storage_transformers: None,
            dimension_names,
        };

        let raw_metadata = serde_json::to_vec(&metadata).map_err(|e| {
            CharizarrError::ArrayError(format!("Failed to serialize metadata: {e}"))
        })?;
        store.set(&metadata_path, &raw_metadata).await?;

        let codec_registry = codec_registry.unwrap_or_else(|| CodecRegistry::default());

        Ok(Self {
            store,
            codec_registry,
            metadata,
            path,
        })
    }

    pub fn chunk_key_separator(&self) -> &str {
        self.metadata.chunk_key_encoding.configuration["separator"]
            .as_str()
            .unwrap_or("/")
    }

    /// Format the chunk key for a given chunk id, given the array's chunk key encoding
    pub fn get_chunk_key(&self, id: &[usize]) -> String {
        let key = id
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<String>>()
            .join(self.chunk_key_separator());
        format!(
            "{path}c{sep}{key}",
            path = self.path,
            sep = self.chunk_key_separator(),
            key = key
        )
    }

    /// Get a raw chunk from the store, without decoding it
    pub async fn get_raw_chunk(&self, id: &[usize]) -> Result<Vec<u8>, CharizarrError> {
        let chunk_path = self.get_chunk_key(id);
        self.store.get(&chunk_path).await
    }

    /// Get a chunk from the store, decoding it according to the array's metadata
    /// and the codecs provided to the array's registry
    pub async fn get_chunk(&self, id: &[usize]) -> Result<ZArray, CharizarrError> {
        let bytes = self.get_raw_chunk(id).await?;
        let data_type = self.dtype();
        let chunk = decode_chunk(
            &self.codec_registry,
            &self.metadata.codecs,
            data_type,
            bytes,
        )?
        .reshape(&self.chunk_shape());
        Ok(chunk)
    }

    /// Set a raw chunk in the store, without encoding it
    pub async fn set_raw_chunk(&self, id: &[usize], data: &[u8]) -> Result<(), CharizarrError> {
        let chunk_path = self.get_chunk_key(id);
        self.store.set(&chunk_path, data).await
    }

    /// Set a chunk in the store, encoding it according to the array's metadata
    pub async fn set_chunk(&self, key: &[usize], chunk: &ZArray) -> Result<(), CharizarrError> {
        let data_type = self.dtype();
        let data = encode_chunk(
            &self.codec_registry,
            &self.metadata.codecs,
            data_type,
            chunk,
        )?;
        self.set_raw_chunk(&key, &data).await
    }

    /// The data type of the array
    pub fn dtype(&self) -> &DataType {
        &self.metadata.data_type
    }

    /// Get the shape of the entire array
    pub fn shape(&self) -> Vec<usize> {
        self.metadata.shape.to_vec()
    }

    /// Get the shape of a single chunk
    pub fn chunk_shape(&self) -> Vec<usize> {
        self.metadata
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

    /// Slice the array according to the given index ranges, asynchronously
    /// reading slices from the store and decoding them, then concatenating
    /// into the correct indices
    ///
    /// This should use Index but async assosciated types are not yet stable
    ///
    /// Also maybe should use ndarray slice or sliceinfo as primitive
    pub async fn get(&self, index: Option<Vec<Range<usize>>>) -> Result<ZArray, CharizarrError> {
        let array_shape = self.shape();

        let index = index.unwrap_or_else(|| {
            array_shape
                .iter()
                .map(|&s| 0..s)
                .collect::<Vec<Range<usize>>>()
        });

        let out_shape = index
            .iter()
            .map(|r| r.end - r.start)
            .collect::<Vec<usize>>();
        let mut out_array = ZArray::zeros(self.dtype(), &out_shape)?;

        // Gather all of the chunks, create futures for fetching chunk data
        let chunks = BasicIndexIterator::new(array_shape, self.chunk_shape(), index).map(
            |chunk_info| async {
                self.get_chunk(&chunk_info.chunk_coords)
                    .await
                    .map(|chunk| (chunk_info, chunk))
            },
        );

        // Trigger the fetch on all of the chunks
        let chunks = try_join_all(chunks).await?;

        // Insert the chunks into the correct place in the output array
        chunks.iter().for_each(|(chunk_info, chunk)| {
            // TODO: Handle error
            let _ = out_array.set(&chunk_info, chunk);
        });

        Ok(out_array)
    }

    /// Slice the array according to the given index ranges, asynchronously
    /// reading slices from the store and decoding them, then concatenating
    /// into the correct indices
    ///
    /// This should use Index but async assosciated types are not yet stable
    ///
    /// Also maybe should use ndarray slice or sliceinfo as primitive
    pub async fn set(
        &self,
        index: Option<Vec<Range<usize>>>,
        value: &ZArray,
    ) -> Result<(), CharizarrError> {
        let array_shape = self.shape();
        let chunk_shape = self.chunk_shape();

        let index = index.unwrap_or_else(|| {
            array_shape
                .iter()
                .map(|&s| 0..s)
                .collect::<Vec<Range<usize>>>()
        });

        let chunks =
            BasicIndexIterator::new(array_shape, chunk_shape, index).map(|chunk_info| async {
                self.get_chunk(&chunk_info.chunk_coords)
                    .await
                    .map(|chunk| (chunk_info, chunk))
            });

        // Trigger the fetch on all of the chunks, then overwrite the values
        // in the chunks with the new values
        let mut existing_chunks = try_join_all(chunks).await?;
        let new_chunks = existing_chunks
            .iter_mut()
            .map(|(chunk_info, chunk)| async move {
                chunk.set(&chunk_info, &value)?;
                self.set_chunk(&chunk_info.chunk_coords, chunk).await
            });

        // Trigger futures, wait for all to complete
        try_join_all(new_chunks).await.map(|_| ())
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
        assert_eq!(&array_metadata.node_type, &NodeType::Array);
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
