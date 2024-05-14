use object_store::ObjectStore;

use crate::store::ReadableStore;

pub struct ZarrObjectStore {
    store: Box<dyn ObjectStore>,
}

impl ZarrObjectStore {
    pub fn new(store: Box<dyn ObjectStore>) -> Self {
        Self { store }
    }
}
