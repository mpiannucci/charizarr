use std::path::PathBuf;

use tokio::fs;

use crate::{
    error::CharizarrError,
    store::{ListableStore, ReadableStore, WriteableStore},
};

/// Async store backed by the filesystem
/// Adapted from https://zarr-specs.readthedocs.io/en/latest/v3/stores/filesystem/v1.0.html
pub struct FileSystemStore {
    root: PathBuf,
}

impl FileSystemStore {
    /// Open an existing filesystem store
    pub async fn open(root: PathBuf) -> Result<Self, CharizarrError> {
        let exists = fs::try_exists(&root).await.map_err(|e| {
            CharizarrError::StoreError(format!("Failed to check if root exists: {e}"))
        })?;
        if !exists {
            return Err(CharizarrError::StoreError(
                "Root directory does not exist".to_string(),
            ));
        }
        Ok(Self { root })
    }

    /// Create a new filesystem store, this will create the root directory if it does not exist
    pub async fn create(root: PathBuf) -> Result<Self, String> {
        fs::create_dir_all(&root)
            .await
            .map_err(|e| format!("Failed to create directory: {e}"))?;
        Ok(Self { root })
    }
}

impl ReadableStore for FileSystemStore {
    fn name(&self) -> String {
        self.root.file_name().unwrap().to_str().unwrap().to_string()
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, CharizarrError> {
        let path = self.root.join(key);
        fs::read(path)
            .await
            .map_err(|e| CharizarrError::StoreError(format!("Failed to read file: {e}")))
    }
}

impl ListableStore for FileSystemStore {
    async fn list(&self) -> Result<Vec<String>, CharizarrError> {
        let mut children = fs::read_dir(&self.root)
            .await
            .map_err(|e| CharizarrError::StoreError(format!("Failed to read directory: {e}")))?;

        let mut dirs = Vec::new();
        while let Some(child) = children.next_entry().await.map_err(|e| {
            CharizarrError::StoreError(format!("Failed to read directory entry: {e}"))
        })? {
            if child.path().is_dir() {
                dirs.push(child.file_name().to_str().unwrap().to_string());
            }
        }

        Ok(dirs)
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, CharizarrError> {
        let filtered = self
            .list()
            .await?
            .into_iter()
            .filter(|key| key.starts_with(prefix))
            .collect();

        Ok(filtered)
    }

    async fn list_dir(&self, prefix: Option<&str>) -> Result<Vec<String>, CharizarrError> {
        let path = if let Some(key) = prefix {
            self.root.join(key)
        } else {
            self.root.clone()
        };
        let mut children = fs::read_dir(&path)
            .await
            .map_err(|e| CharizarrError::StoreError(format!("Failed to read directory: {e}")))?;

        let mut dirs = Vec::new();
        while let Some(child) = children.next_entry().await.map_err(|e| {
            CharizarrError::StoreError(format!("Failed to read directory entry: {e}"))
        })? {
            if child.path().is_dir() {
                dirs.push(child.file_name().to_str().unwrap().to_string());
            }
        }

        Ok(dirs)
    }
}
impl WriteableStore for FileSystemStore {
    async fn set(&self, key: &str, value: &[u8]) -> Result<(), CharizarrError> {
        let path = self.root.join(key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                CharizarrError::StoreError(format!(
                    "Failed to create parent directory {d}: {e}",
                    d = parent.to_str().unwrap()
                ))
            })?;
        }
        path.parent().map(|parent| fs::create_dir_all(parent));
        fs::write(&path, value).await.map_err(|e| {
            CharizarrError::StoreError(format!(
                "Failed to write file {file}: {e}",
                file = path.to_str().unwrap()
            ))
        })
    }

    async fn erase(&self, key: &str) -> Result<(), CharizarrError> {
        let path = self.root.join(key);
        fs::remove_file(&path).await.map_err(|e| {
            CharizarrError::StoreError(format!(
                "Failed to remove file {file}: {e}",
                file = path.to_str().unwrap()
            ))
        })
    }

    async fn erase_values(&self, keys: &[&str]) -> Result<(), CharizarrError> {
        let futures = keys.iter().map(|key| {
            let path = self.root.join(key);
            fs::remove_file(path)
        });

        let _ = futures::future::try_join_all(futures)
            .await
            .map_err(|e| format!("Failed to remove files: {e}"));

        Ok(())
    }

    async fn erase_prefix(&self, prefix: &str) -> Result<(), CharizarrError> {
        let path = self.root.join(prefix);
        fs::remove_dir_all(path)
            .await
            .map_err(|e| CharizarrError::StoreError(format!("Failed to remove directory: {e}")))
    }
}
