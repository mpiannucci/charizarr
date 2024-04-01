use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::fs;

use crate::{
    array::{Array, ArrayMetadata},
    codec_registry::CodecRegistry,
    metadata::ZarrFormat,
    store::{ListableStore, ReadableStore, WriteableStore},
};

#[derive(Serialize, Deserialize, Clone)]
pub struct GroupMetadata {
    pub zarr_format: ZarrFormat,
    pub node_type: String,
    pub attributes: Option<HashMap<String, Value>>,
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
    pub async fn open(store: &'a T, path: Option<String>) -> Result<Self, String> {
        let path = path.map_or_else(|| "".to_string(), |p| format!("{p}/"));

        let metadata_path = format!("{path}zarr.json");
        let raw_metadata = store.get(&metadata_path).await?;
        let metadata = serde_json::from_slice::<GroupMetadata>(&raw_metadata)
            .map_err(|e| format!("Failed to parse group metadata: {e}"))?;

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
    ) -> Result<Self, String> {
        let path = path.map_or_else(|| "".to_string(), |p| format!("{p}/"));

        let attributes = attributes
            .unwrap_or_else(|| HashMap::from([("name".into(), Value::String(store.name()))]));

        let metadata = GroupMetadata {
            zarr_format: ZarrFormat::V3,
            node_type: "group".to_string(),
            attributes: Some(attributes),
        };
        let raw_metadata = serde_json::to_vec(&metadata)
            .map_err(|e| format!("Failed to serialize group metadata: {e}"))?;
        let metadata_path = format!("{path}zarr.json");
        store.set(&metadata_path, &raw_metadata).await?;

        Ok(Self {
            store,
            metadata,
            path,
        })
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
    ) -> Result<Array<'a, T>, String> {
        let path = format!("{path}{name}", path = self.path);
        Array::open(self.store, Some(path), codecs).await
    }

    /// Get an child group from the group
    pub async fn get_group(&self, name: &str) -> Result<Group<'a, T>, String> {
        let path = format!("{path}{name}", path = self.path);
        Group::open(self.store, Some(path)).await
    }

    /// Create a new child group in the group
    pub async fn create_group(&self, name: &str) -> Result<Group<'a, T>, String> {
        let path = format!("{path}{name}", path = self.path);
        Group::create(self.store, Some(path), None).await
    }

    /// Create a new child array in the group
    pub async fn create_array(
        &self,
        name: &str,
        metadata: ArrayMetadata,
        codecs: Option<CodecRegistry>,
    ) -> Result<Array<'a, T>, String> {
        let path = format!("{path}{name}", path = self.path);
        let array = Array::create(self.store, Some(path), metadata, codecs).await?;
        Ok(array)
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
        assert_eq!(&group_metadata.node_type, "group");
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
