use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_repr::{Deserialize_repr, Serialize_repr};

pub type Configuration = HashMap<String, Value>;

#[derive(Serialize, Deserialize)]
pub struct Extension {
    name: String,
    configuration: Option<Configuration>,
}

#[derive(Serialize_repr, Deserialize_repr, PartialEq, Debug)]
#[repr(u8)]
pub enum ZarrFormat {
    V1 = 1,
    V2 = 2,
    V3 = 3,
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum DataType {
    Defined(String),
    Extension(Extension),
}

#[derive(Serialize, Deserialize)]
pub struct ArrayMetadata {
    zarr_format: ZarrFormat,
    node_type: String,
    shape: Vec<usize>,
    data_type: DataType,
    chunk_grid: Extension,
    chunk_key_encoding: Extension,
    // TODO: Actual fill values
    fill_value: Value,
    codecs: Vec<Extension>,
    atttributes: Option<HashMap<String, Value>>,
    storage_transformers: Option<Vec<Extension>>,
    dimension_names: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize)]
pub struct GroupMetadata {
    zarr_format: ZarrFormat,
    node_type: String,
    atttributes: Option<HashMap<String, Value>>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

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
            DataType::Defined(ref dtype) => dtype,
            _ => panic!("Expected defined data type"),
        };
        assert_eq!(data_type, "float64");

        let chunk_grid = match array_metadata.chunk_grid {
            Extension {
                name,
                configuration: Some(config),
            } => (name, config),
            _ => panic!("Expected chunk grid extension"),
        };

        assert_eq!(chunk_grid.0, "regular");
        assert_eq!(chunk_grid.1["chunk_shape"], json!([1000, 100]));
    }
}
