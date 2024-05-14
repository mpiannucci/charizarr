use futures::{StreamExt, TryFutureExt};
use object_store::{path::Path, ObjectMeta, ObjectStore, PutPayload};

use crate::{
    error::CharizarrError,
    store::{ListableStore, ReadableStore, WriteableStore},
};

pub struct ZarrObjectStore {
    store: Box<dyn ObjectStore>,
    root: Path,
}

impl ZarrObjectStore {
    pub fn create(store: Box<dyn ObjectStore>, root: Path) -> Self {
        Self { store, root }
    }

    fn path_for_key(&self, key: &str) -> Path {
        key.split("/")
            .into_iter()
            .fold(self.root.clone(), |path, part| path.child(part))
    }

    async fn list_meta(&self, prefix: Option<&str>) -> Result<Vec<ObjectMeta>, CharizarrError> {
        let path = if let Some(prefix) = prefix {
            let child = self.path_for_key(prefix);
            Some(child)
        } else {
            None
        };
        self.store
            .list(path.as_ref())
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|meta| {
                meta.map_err(|e| CharizarrError::StoreError(format!("Failed to list objects: {e}")))
            })
            .collect::<Result<Vec<_>, _>>()
    }
}

impl ReadableStore for ZarrObjectStore {
    async fn get(&self, key: &str) -> Result<Vec<u8>, CharizarrError> {
        let path = self.path_for_key(key);
        let result = self
            .store
            .get(&path)
            .map_err(|e| CharizarrError::StoreError(format!("Failed to read object: {e}")))
            .await?
            .bytes()
            .await
            .map_err(|e| {
                CharizarrError::StoreError(format!("Failed to read data from object: {e}"))
            })?;

        let data = result.to_vec();
        Ok(data)
    }
}

impl ListableStore for ZarrObjectStore {
    async fn list(&self) -> Result<Vec<String>, CharizarrError> {
        let meta = self
            .list_meta(None)
            .await?
            .into_iter()
            .map(|meta| meta.location.to_string())
            .collect();

        Ok(meta)
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, CharizarrError> {
        let meta = self
            .list_meta(Some(prefix))
            .await?
            .into_iter()
            .map(|meta| meta.location.to_string())
            .collect();

        Ok(meta)
    }

    async fn list_dir(&self, prefix: Option<&str>) -> Result<Vec<String>, CharizarrError> {
        Err(CharizarrError::UnimplementedError("list_dir"))
    }
}

impl WriteableStore for ZarrObjectStore {
    async fn set(&self, key: &str, value: &[u8]) -> Result<(), CharizarrError> {
        let bytes = value.to_vec();
        let payload = PutPayload::from_iter(bytes);
        let path = self.path_for_key(key);
        let _response = self
            .store
            .put(&path, payload)
            .map_err(|e| CharizarrError::StoreError(format!("Failed to write object: {e}")))
            .await?;

        Ok(())
    }

    async fn erase(&self, key: &str) -> Result<(), CharizarrError> {
        let path = self.path_for_key(key);
        self.store
            .delete(&path)
            .map_err(|e| CharizarrError::StoreError(format!("Failed to delete object: {e}")))
            .await?;

        Ok(())
    }

    async fn erase_values(&self, keys: &[&str]) -> Result<(), CharizarrError> {
        for key in keys {
            self.erase(key).await?;
        }

        Ok(())
    }

    async fn erase_prefix(&self, prefix: &str) -> Result<(), CharizarrError> {
        self.erase(prefix).await
    }
}
