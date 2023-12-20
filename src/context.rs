use crate::logging::Logger;
use crate::sst::block_cache::BlockCache;
use crate::storage::blob_store::BlobStore;

pub(crate) trait Context:
    Clone + Send + Sync + From<(Self::BlobStore, Self::BlockCache, Self::Logger)>
{
    type BlobStore: BlobStore;
    type BlockCache: BlockCache + Send + Sync;
    type Logger: Logger;

    fn blob_store_ref(&self) -> &Self::BlobStore;
    fn block_cache_ref(&self) -> &Self::BlockCache;
    fn logger(&self) -> &Self::Logger;
}

#[derive(Clone, Debug)]
pub(crate) struct SimpleContext<B: BlobStore, C: BlockCache, L: Logger> {
    blob_store: B,
    block_cache: C,
    logger: L,
}

impl<B: BlobStore, C: BlockCache + Send + Sync + 'static, L: Logger> From<(B, C, L)>
    for SimpleContext<B, C, L>
{
    fn from(value: (B, C, L)) -> Self {
        let (blob_store, block_cache, logger) = value;
        SimpleContext {
            blob_store,
            block_cache,
            logger,
        }
    }
}

impl<B: BlobStore, C: BlockCache + Send + Sync + 'static, L: Logger> Context
    for SimpleContext<B, C, L>
{
    type BlobStore = B;
    type BlockCache = C;
    type Logger = L;

    fn blob_store_ref(&self) -> &Self::BlobStore {
        &self.blob_store
    }

    fn block_cache_ref(&self) -> &Self::BlockCache {
        &self.block_cache
    }

    fn logger(&self) -> &Self::Logger {
        &self.logger
    }
}
