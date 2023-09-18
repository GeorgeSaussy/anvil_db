use crate::sst::block_cache::cache::BlockCache;
use crate::storage::blob_store::{BlobStore, FileError};

pub(crate) trait StorageWrapper:
    BlobStore + Send + Sync + Clone + From<(Self::B, Self::C)>
{
    type B: BlobStore;
    type C: BlockCache;

    fn blob_store_ref(&self) -> &Self::B;
    fn block_cache_ref(&self) -> &Self::C;
}

#[derive(Clone, Debug)]
pub(crate) struct SimpleStorageWrapper<B: BlobStore, C: BlockCache> {
    blob_store: B,
    block_cache: C,
}

impl<B: BlobStore, C: BlockCache> From<(B, C)> for SimpleStorageWrapper<B, C> {
    fn from((blob_store, block_cache): (B, C)) -> Self {
        SimpleStorageWrapper {
            blob_store,
            block_cache,
        }
    }
}

impl<B: BlobStore, C: BlockCache> BlobStore for SimpleStorageWrapper<B, C>
where
    C: Send + Sync + 'static,
{
    type BI = B::BI;
    type RC = B::RC;
    type WC = B::WC;

    fn exists(&self, blob_id: &str) -> Result<bool, FileError> {
        self.blob_store.exists(blob_id)
    }

    fn read_cursor(&self, blob_id: &str) -> Result<Self::RC, FileError> {
        self.blob_store.read_cursor(blob_id)
    }

    fn create_blob(&self, blob_id: &str) -> Result<Self::WC, FileError> {
        self.blob_store.create_blob(blob_id)
    }

    fn blob_iter(&self) -> Result<Self::BI, FileError> {
        self.blob_store.blob_iter()
    }

    fn delete(&self, blob_id: &str) -> Result<(), FileError> {
        self.blob_store.delete(blob_id)
    }
}

impl<B: BlobStore, C: BlockCache> StorageWrapper for SimpleStorageWrapper<B, C>
where
    C: Send + Sync + 'static,
{
    type B = B;
    type C = C;

    fn blob_store_ref(&self) -> &Self::B {
        &self.blob_store
    }

    fn block_cache_ref(&self) -> &Self::C {
        &self.block_cache
    }
}
