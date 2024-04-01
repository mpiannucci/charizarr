use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    array::Array,
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
    pub meta: GroupMetadata,
    pub path: String,
}

impl<'a, T> Group<'a, T>
where
    T: ReadableStore + ListableStore + WriteableStore,
{
    pub async fn open(store: &'a T, path: Option<String>) -> Result<Self, String> {
        let path = path.map_or_else(|| "".to_string(), |p| format!("{p}/"));
        let metadata_path = format!("{path}zarr.json");
        let raw_metadata = store.get(&metadata_path).await?;
        let meta = serde_json::from_slice::<GroupMetadata>(&raw_metadata)
            .map_err(|e| format!("Failed to parse group metadata: {e}"))?;

        Ok(Self { store, meta, path })
    }

    pub fn name(&self) -> &str {
        self.meta
            .attributes
            .as_ref()
            .and_then(|a| a.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
    }

    pub async fn get_array(
        &self,
        name: &str,
        codecs: Option<CodecRegistry>,
    ) -> Result<Array<'a, T>, String> {
        let path = format!("{path}{name}", path = self.path);
        Array::open(self.store, Some(path), codecs).await
    }

    pub async fn get_group(&self, name: &str) -> Result<Group<'a, T>, String> {
        let path = format!("{path}{name}", path = self.path);
        Group::open(self.store, Some(path)).await
    }

    pub async fn create_group(&self, name: &str) -> Result<Group<'a, T>, String> {
        let path = format!("{path}{name}", path = self.path);
        let metadata = GroupMetadata {
            zarr_format: self.meta.zarr_format.clone(),
            node_type: "group".to_string(),
            attributes: None,
        };
        let raw_metadata = serde_json::to_vec(&metadata)
            .map_err(|e| format!("Failed to serialize group metadata: {e}"))?;
        let metadata_path = format!("{path}zarr.json");
        self.store.set(&metadata_path, &raw_metadata).await?;

        Ok(Group::open(self.store, Some(path)).await?)
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

        let group_name = group_metadata.attributes
            .as_ref()
            .and_then(|a| a.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        assert_eq!(group_name, "data.zarr");
    }
}
