use std::{path::PathBuf, process::Child};

use tokio::fs;

use crate::store::{ListableStore, ReadableStore, WriteableStore};

/// Async store backed by the filesystem
/// Adapted from https://zarr-specs.readthedocs.io/en/latest/v3/stores/filesystem/v1.0.html
pub struct FileSystemStore {
    root: PathBuf,
}

impl FileSystemStore {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }
}

impl ReadableStore for FileSystemStore {
    async fn get(&self, key: &str) -> Result<Vec<u8>, String> {
        let path = self.root.join(key);
        fs::read(path)
            .await
            .map_err(|e| format!("Failed to read file: {e}"))
    }
}

impl ListableStore for FileSystemStore {
    async fn list(&self) -> Result<Vec<String>, String> {
        let mut children = fs::read_dir(&self.root)
            .await
            .map_err(|e| format!("Failed to read directory: {e}"))?;

        let mut dirs = Vec::new();
        while let Some(child) = children
            .next_entry()
            .await
            .map_err(|e| format!("Failed to read directory entry: {e}"))?
        {
            if child.path().is_dir() {
                dirs.push(child.file_name().to_str().unwrap().to_string());
            }
        }

        Ok(dirs)
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, String> {
        let filtered = self
            .list()
            .await?
            .into_iter()
            .filter(|key| key.starts_with(prefix))
            .collect();

        Ok(filtered)
    }

    async fn list_dir(&self, prefix: Option<&str>) -> Result<Vec<String>, String> {
        let path = if let Some(key) = prefix {
            self.root.join(key)
        } else {
            self.root.clone()
        };
        let mut children = fs::read_dir(&path)
            .await
            .map_err(|e| format!("Failed to read directory: {e}"))?;

        let mut dirs = Vec::new();
        while let Some(child) = children
            .next_entry()
            .await
            .map_err(|e| format!("Failed to read directory entry: {e}"))?
        {
            if child.path().is_dir() {
                dirs.push(child.file_name().to_str().unwrap().to_string());
            }
        }

        Ok(dirs)
    }
}
impl WriteableStore for FileSystemStore {
    async fn set(&self, key: &str, value: &[u8]) -> Result<(), String> {
        let path = self.root.join(key);
        fs::write(path, value)
            .await
            .map_err(|e| format!("Failed to write file: {e}"))
    }

    async fn erase(&self, key: &str) -> Result<(), String> {
        let path = self.root.join(key);
        fs::remove_file(path)
            .await
            .map_err(|e| format!("Failed to remove file: {e}"))
    }

    async fn erase_values(&self, keys: &[&str]) -> Result<(), String> {
        let futures = keys.iter().map(|key| {
            let path = self.root.join(key);
            fs::remove_file(path)
        });

        let _ = futures::future::try_join_all(futures)
            .await
            .map_err(|e| format!("Failed to remove files: {e}"));

        Ok(())
    }

    async fn erase_prefix(&self, prefix: &str) -> Result<(), String> {
        let path = self.root.join(prefix);
        fs::remove_dir_all(path)
            .await
            .map_err(|e| format!("Failed to remove directory: {e}"))
    }
}
