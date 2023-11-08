use crate::{
    metadata::{ArrayMetadata, GroupMetadata},
    store::{ListableStore, ReadableStore, WriteableStore},
};

pub struct Array<'a, T>
where
    T: ReadableStore + ListableStore + WriteableStore,
{
    store: &'a T,
    metadata: ArrayMetadata,
    root_path: String,
}

impl<'a, T> Array<'a, T>
where
    T: ReadableStore + ListableStore + WriteableStore,
{
    pub fn new(store: &'a T, metadata: ArrayMetadata, root_path: Option<String>) -> Self {
        Self {
            store,
            metadata,
            root_path: root_path.unwrap_or_else(|| "".to_string()),
        }
    }
}

pub struct Group<'a, T>
where
    T: ReadableStore + ListableStore + WriteableStore,
{
    store: &'a T,
    // metadata: GroupMetadata,
    root_path: String,
}

impl<'a, T> Group<'a, T>
where
    T: ReadableStore + ListableStore + WriteableStore,
{
    pub async fn open(store: &'a T, root_path: Option<String>) -> Result<Self, String> {
        Ok(Self {
            store,
            // metadata,
            root_path: root_path.unwrap_or_else(|| "".to_string()),
        })
    }
}
