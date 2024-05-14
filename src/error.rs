use thiserror::Error;

#[derive(Error, Debug)]
pub enum CharizarrError {
    #[error("Zarr Array Error: {0}")]
    ArrayError(String),
    #[error("Error decoding chunk: {0}")]
    CodecError(String),
    #[error("Zarr Group Error: {0}")]
    GroupError(String),
    #[error("Zarr store: {0}")]
    StoreError(String),
    #[error("Chunk is not of type {0}")]
    TypeError(String),
    #[error("Feature not implmented: {0}")]
    UnimplementedError(&'static str),
}
