use std::ops::Range;

use crate::error::CharizarrError;

pub type KeyRange = (String, Range<usize>);
pub type KeyRangeValues = (String, Range<usize>, Vec<u8>);

/// Read only store interface
pub trait ReadableStore {
    /// Retrieve the value associated with a given key
    async fn get(&self, key: &str) -> Result<Vec<u8>, CharizarrError>;

    /// Retrieve possibly partial values from given key_ranges.
    ///
    /// The key_ranges are a list of (key, range) tuples, where range is a
    /// byte range within the value associated with the key.
    ///
    /// Returns a list of values, in the order of the key_ranges,
    /// may contain null/none for missing keys
    ///
    /// By default this is not implemented, and it is optional for stores to
    /// implement.
    async fn get_partial_values(&self, _keys: &[KeyRange]) -> Result<Vec<u8>, CharizarrError> {
        Err(CharizarrError::UnimplementedError("get_partial_values"))
    }
}

/// Listable store interface
pub trait ListableStore {
    /// Retrieve all keys in the store.
    async fn list(&self) -> Result<Vec<String>, CharizarrError>;

    /// Retrieve all keys with a given prefix.
    ///
    /// For example, if a store contains the keys “a/b”, “a/c/d” and “e/f/g”,
    /// then list_prefix("a/") would return “a/b” and “a/c/d”.
    ///
    /// Note: the behaviour of list_prefix is undefined if prefix does
    /// not end with a trailing slash / and the store can assume there
    /// is at least one key that starts with prefix.
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, CharizarrError>;

    /// Retrieve all keys and prefixes with a given prefix and which do not
    /// contain the character “/” after the given prefix.
    async fn list_dir(&self, prefix: Option<&str>) -> Result<Vec<String>, CharizarrError>;
}

/// Writable store interface
pub trait WriteableStore {
    /// Store a (key, value) pair.
    async fn set(&self, key: &str, value: &[u8]) -> Result<(), CharizarrError>;

    /// Store values at a given key, starting at byte range_start.
    ///
    /// must not specify overlapping ranges for the same key
    ///
    /// By default this is not implemented, and it is optional for stores to
    /// implement.
    async fn set_partial_values(&self, _key_start_values: &[KeyRangeValues]) -> Result<(), CharizarrError> {
        Err(CharizarrError::UnimplementedError("set_partial_values"))
    }

    /// Erase the given key/value pair from the store.
    async fn erase(&self, key: &str) -> Result<(), CharizarrError>;

    /// Erase the given key/value pairs from the store.
    async fn erase_values(&self, keys: &[&str]) -> Result<(), CharizarrError>;

    /// Erase all keys with the given prefix from the store:
    async fn erase_prefix(&self, prefix: &str) -> Result<(), CharizarrError>;
}
