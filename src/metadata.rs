use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::data_type::CoreDataType;

pub type Configuration = HashMap<String, Value>;

#[derive(Serialize, Deserialize, Clone)]
pub struct Extension {
    pub name: String,
    pub configuration: Option<Configuration>,
}

#[derive(Serialize_repr, Deserialize_repr, PartialEq, Debug, Clone)]
#[repr(u8)]
pub enum ZarrFormat {
    V1 = 1,
    V2 = 2,
    V3 = 3,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum DataType {
    Core(CoreDataType),
    Extension(Extension),
}

#[derive(Serialize, Deserialize, Clone)]
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

#[derive(Serialize, Deserialize, Clone)]
pub struct GroupMetadata {
    pub zarr_format: ZarrFormat,
    pub node_type: String,
    pub attributes: Option<HashMap<String, Value>>,
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
            DataType::Core(ref dtype) => dtype,
            _ => panic!("Expected defined data type"),
        };
        assert_eq!(*data_type, CoreDataType::Float64);

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
