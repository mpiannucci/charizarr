use crate::store::{ListableStore, ReadableStore, WriteableStore};

pub struct Hierarchy<'a, T>
where
    T: ReadableStore + ListableStore + WriteableStore,
{
    store: &'a T,
}

impl<'a, T> Hierarchy<'a, T>
where
    T: ReadableStore + ListableStore + WriteableStore,
{
    pub fn new(store: &'a T) -> Self {
        Self { store }
    }
}
