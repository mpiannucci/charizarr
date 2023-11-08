use std::path::PathBuf;

use crate::store::{ListableStore, ReadableStore, WriteableStore};

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
        std::fs::read(path).map_err(|e| format!("Failed to read file: {e}"))
    }

    async fn get_partial_values(&self, keys: &[crate::store::KeyRange]) -> Result<Vec<u8>, String> {
        todo!()
    }
}

impl ListableStore for FileSystemStore {
    async fn list(&self) -> Result<Vec<String>, String> {
        todo!()
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, String> {
        todo!()
    }

    async fn list_dir(&self, prefix: Option<&str>) -> Result<Vec<String>, String> {
        todo!()
    }
}
impl WriteableStore for FileSystemStore {
    async fn set(&self, key: &str, value: &[u8]) -> Result<(), String> {
        todo!()
    }

    async fn set_partial_values(
        &self,
        key_start_values: &[crate::store::KeyRangeValues],
    ) -> Result<(), String> {
        todo!()
    }

    async fn erase(&self, key: &str) -> Result<(), String> {
        todo!()
    }

    async fn erase_values(&self, keys: &[&str]) -> Result<(), String> {
        todo!()
    }

    async fn erase_prefix(&self, prefix: &str) -> Result<(), String> {
        todo!()
    }
}
