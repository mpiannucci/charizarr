use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    array::{Array, ArrayMetadata}, codec_registry::CodecRegistry, error::CharizarrError, metadata::{DataType, Extension, NodeType, ZarrFormat}, store::{ListableStore, ReadableStore, WriteableStore}
};

#[derive(Serialize, Deserialize, Clone)]
pub struct GroupMetadata {
    pub zarr_format: ZarrFormat,
    pub node_type: NodeType,
    pub attributes: Option<HashMap<String, Value>>,
}

impl Default for GroupMetadata {
    fn default() -> Self {
        Self {
            zarr_format: ZarrFormat::V3,
            node_type: NodeType::Group,
            attributes: None,
        }
    }
}

pub struct Group<'a, T>
where
    T: ReadableStore + ListableStore + WriteableStore,
{
    store: &'a T,
    pub metadata: GroupMetadata,
    pub path: String,
}

impl<'a, T> Group<'a, T>
where
    T: ReadableStore + ListableStore + WriteableStore,
{
    /// Open an existing group from a store. If the zarr.json metadata file is not found,
    pub async fn open(store: &'a T, path: Option<String>) -> Result<Self, CharizarrError> {
        let path = path.map_or_else(|| "".to_string(), |p| format!("{p}/"));

        let metadata_path = format!("{path}zarr.json");
        let raw_metadata = store.get(&metadata_path).await?;
        let metadata = serde_json::from_slice::<GroupMetadata>(&raw_metadata)
            .map_err(|e| CharizarrError::GroupError(format!("Failed to parse group metadata: {e}")))?;

        Ok(Self {
            store,
            metadata,
            path,
        })
    }

    /// Create a new group in a store
    pub async fn create(
        store: &'a T,
        path: Option<String>,
        attributes: Option<HashMap<String, Value>>,
    ) -> Result<Self, CharizarrError> {
        let path = path.map_or_else(|| "".to_string(), |p| format!("{p}/"));

        let attributes = attributes
            .unwrap_or_else(|| HashMap::from([("name".into(), Value::String("group".to_string()))]));

        let metadata = GroupMetadata {
            zarr_format: ZarrFormat::V3,
            node_type: NodeType::Group,
            attributes: Some(attributes),
        };
        let raw_metadata = serde_json::to_vec(&metadata)
            .map_err(|e| CharizarrError::GroupError(format!("Failed to serialize group metadata: {e}")))?;
        let metadata_path = format!("{path}zarr.json");
        store.set(&metadata_path, &raw_metadata).await?;

        Ok(Self {
            store,
            metadata,
            path,
        })
    }

    pub fn attrs(&self) -> &Option<HashMap<String, Value>> {
        &self.metadata.attributes
    }

    /// The name of the group
    pub fn name(&self) -> &str {
        self.metadata
            .attributes
            .as_ref()
            .and_then(|a| a.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
    }

    /// Get an child array from the group
    pub async fn get_array(
        &self,
        name: &str,
        codecs: Option<CodecRegistry>,
    ) -> Result<Array<'a, T>, CharizarrError> {
        let path = format!("{path}{name}", path = self.path);
        Array::open(self.store, Some(path), codecs).await
    }

    /// Get an child group from the group
    pub async fn get_group(&self, name: &str) -> Result<Group<'a, T>, CharizarrError> {
        let path = format!("{path}{name}", path = self.path);
        Group::open(self.store, Some(path)).await
    }

    /// Create a new child group in the group
    pub async fn create_group(&self, name: &str) -> Result<Group<'a, T>, CharizarrError> {
        let path = format!("{path}{name}", path = self.path);
        Group::create(self.store, Some(path), None).await
    }

    /// Create a new child array in the group
    pub async fn create_array(
        &self,
        name: &str,
        codec_registry: Option<CodecRegistry>,
        shape: Vec<usize>,
        chunk_shape: Vec<usize>,
        chunk_key_encoding: Option<Extension>,
        data_type: DataType,
        fill_value: Value,
        codecs: Vec<Extension>,
        dimension_names: Option<Vec<String>>,
        attributes: Option<HashMap<String, Value>>,
    ) -> Result<Array<'a, T>, CharizarrError> {
        let path = format!("{path}{name}", path = self.path);
        Array::create(self.store, Some(path), codec_registry, shape, chunk_shape, chunk_key_encoding, data_type, fill_value, codecs, dimension_names, attributes).await
    }

    /// Add an attribute to the group
    pub async fn add_attr(&mut self, key: String, value: Value) -> Result<(), CharizarrError> {
        let mut attrs = self.metadata.attributes.take().unwrap_or_default();
        attrs.insert(key, value);
        self.metadata.attributes = Some(attrs);
        self.write_metadata().await
    }

    /// Add multiple attributes to the group
    pub async fn add_attrs(&mut self, attrs: HashMap<String, Value>) -> Result<(), CharizarrError> {
        let mut existing_attrs = self.metadata.attributes.take().unwrap_or_default();
        existing_attrs.extend(attrs);
        self.metadata.attributes = Some(existing_attrs);
        self.write_metadata().await
    }

    /// Remove an attribute from the group
    pub async fn remove_attr(&mut self, key: &str) -> Result<(), CharizarrError> {
        let mut attrs = self.metadata.attributes.take().unwrap_or_default();
        attrs.remove(key);
        self.metadata.attributes = Some(attrs);
        self.write_metadata().await
    }

    /// Remove multiple attributes from the group
    pub async fn remove_attrs(&mut self, keys: Vec<&str>) -> Result<(), CharizarrError> {
        let mut attrs = self.metadata.attributes.take().unwrap_or_default();
        for key in keys {
            attrs.remove(key);
        }
        self.metadata.attributes = Some(attrs);
        self.write_metadata().await
    }

    /// Set the attributes of the group
    /// This will overwrite any existing attributes
    pub async fn set_attrs(&mut self, attrs: Option<HashMap<String, Value>>) -> Result<(), CharizarrError> {
        self.metadata.attributes = attrs;
        self.write_metadata().await
    }

    async fn write_metadata(&self) -> Result<(), CharizarrError> {
        let raw_metadata = serde_json::to_vec(&self.metadata).map_err(|e| CharizarrError::GroupError(e.to_string()))?;
        let metadata_path = format!("{path}zarr.json", path = self.path);
        self.store.set(&metadata_path, &raw_metadata).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_group_metadata() {
        let metadata = r#"
            {
              "attributes": {
                "name": "data.zarr"
              },
              "zarr_format": 3,
              "node_type": "group"
            }
            "#;

        let group_metadata = serde_json::from_str::<GroupMetadata>(metadata);
        assert!(group_metadata.is_ok());
        let group_metadata = group_metadata.unwrap();
        assert_eq!(&group_metadata.node_type, &NodeType::Group);
        assert_eq!(group_metadata.zarr_format, ZarrFormat::V3);

        let group_name = group_metadata
            .attributes
            .as_ref()
            .and_then(|a| a.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        assert_eq!(group_name, "data.zarr");
    }
}
