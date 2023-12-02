use crate::{
    metadata::{ArrayMetadata, GroupMetadata},
    store::{ListableStore, ReadableStore, WriteableStore},
};

pub struct Array<'a, T>
where
    T: ReadableStore + ListableStore + WriteableStore,
{
    store: &'a T,
    pub meta: ArrayMetadata,
    pub path: String,
}

impl<'a, T> Array<'a, T>
where
    T: ReadableStore + ListableStore + WriteableStore,
{
    pub async fn open(store: &'a T, path: Option<String>) -> Result<Self, String> {
        let path = path.map_or_else(|| "".to_string(), |p| format!("{p}/"));
        let metadata_path = format!("{path}zarr.json");
        let raw_metadata = store.get(&metadata_path).await?;
        let meta = serde_json::from_slice::<ArrayMetadata>(&raw_metadata)
            .map_err(|e| format!("Failed to parse group metadata: {e}"))?;

        Ok(Self { store, meta, path })
    }

    pub async fn get_raw_chunk(&self, key: &str) -> Result<Vec<u8>, String> {
        let chunk_path = format!("{path}{key}", path = self.path);
        self.store.get(&chunk_path).await
    }

    /// Get the shape of the entire array
    pub fn shape(&self) -> Vec<usize> {
        self.meta.shape.to_vec()
    }

    /// Get the shape of a single chunk
    /// TODO: Should this take into account grid type?
    pub fn chunk(&self) -> Vec<usize> {
        self
            .meta
            .chunk_grid
            .configuration
            .as_ref()
            .unwrap()
            .get("chunk_shape")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap() as usize)
            .collect::<Vec<usize>>()
    }
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

    pub async fn get_array(&self, name: &str) -> Result<Array<'a, T>, String> {
        let path = format!("{path}{name}", path = self.path);
        Array::open(self.store, Some(path)).await
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
