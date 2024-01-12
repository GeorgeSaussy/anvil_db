use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};

use crate::storage::blob_store::BlobStoreError;

#[derive(Debug)]
pub(crate) enum CacheError {
    Read(String),
    BlockTooSmall,
    // This should never happen.
    NoBlocks,
}

impl Display for CacheError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            CacheError::Read(err) => write!(f, "read error: {}", err),
            CacheError::BlockTooSmall => write!(f, "block too small"),
            CacheError::NoBlocks => write!(f, "no blocks"),
        }
    }
}

impl Error for CacheError {}

impl From<BlobStoreError> for CacheError {
    fn from(err: BlobStoreError) -> Self {
        CacheError::Read(format!(
            "CacheError::Read could not write to file: {:?}",
            err
        ))
    }
}
