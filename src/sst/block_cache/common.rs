use crate::storage::blob_store::BlobStoreError;

#[derive(Debug)]
pub(crate) enum CacheError {
    Read(String),
    BlockTooSmall,
    // This should never happen.
    NoBlocks,
}

impl From<BlobStoreError> for CacheError {
    fn from(err: BlobStoreError) -> Self {
        CacheError::Read(format!(
            "CacheError::Read could not write to file: {:?}",
            err
        ))
    }
}
