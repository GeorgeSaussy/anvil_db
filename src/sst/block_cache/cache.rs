use std::fmt::Debug;
use std::sync::{Arc, RwLock};

use crate::checksum::crc32;
use crate::helpful_macros::unlock;
use crate::hopscotch::{ConcurrentHopscotchHashMap, Hash};
use crate::sst::block_cache::common::CacheError;
use crate::sst::reader::ThinSstMetadata;
use crate::storage::blob_store::{BlobStore, ReadCursor};

/// Implementations of BlockCache must be tread safe.
/// A clone of the implementation must still refer to the same underlying data.
/// Memory usage is an approximation. It should take into account the size of
/// the cached blocks, and any overhead that is not constant.
pub(crate) trait BlockCache: Clone {
    fn with_capacity(max_blocks_cached: usize) -> Self;
    /// Reads the exact number of bytes into the destination buffer.
    /// It will not work for cross-block reads.
    fn read_exact<B: BlobStore>(
        &self,
        blob_store_ref: &B,
        meta: &ThinSstMetadata,
        blob_id_hash: u32,
        offset: usize,
        dest: &mut [u8],
    ) -> Result<(), CacheError>;
    /// Reads the block containing the offset.
    fn read_block<B: BlobStore>(
        &self,
        blob_store_ref: &B,
        meta: &ThinSstMetadata,
        block_id: u32,
        offset: usize,
    ) -> Result<Vec<u8>, CacheError>;
}

pub(crate) trait CachePolicy: Clone {
    fn with_capacity(max_blocks_cached: usize) -> Self;
    /// Read must be a thread safe operation.
    /// It is called when a block is read. It returns whether the block should
    /// be cached, and a list of blocks to evict if the block should be cached.
    fn read(&self, id: u32) -> (bool, Option<u32>);
}

#[derive(Clone, Default, Debug)]
pub(crate) struct NeverCachePolicy {}

impl CachePolicy for NeverCachePolicy {
    fn with_capacity(_max_blocks_cached: usize) -> Self {
        NeverCachePolicy {}
    }

    fn read(&self, _id: u32) -> (bool, Option<u32>) {
        (false, None)
    }
}

/// TODO(t/1387): This is a very bad cache implementation.
/// This should use the doubly linked list documented
/// in "Lock-Free and Practical Deques and Doubly Linked
/// Lists using Single-Word Compare-And-Swap" by
/// Sundell and Tsigas.
#[derive(Clone, Debug)]
pub(crate) struct LruCache {
    capacity: usize,
    entries: Arc<RwLock<Vec<u32>>>,
}

impl CachePolicy for LruCache {
    fn with_capacity(max_blocks_cached: usize) -> Self {
        LruCache {
            capacity: max_blocks_cached,
            entries: Arc::new(RwLock::new(Vec::with_capacity(max_blocks_cached + 1))),
        }
    }

    fn read(&self, id: u32) -> (bool, Option<u32>) {
        let mut entries = unlock!(self.entries.write());
        let idx: Vec<_> = entries
            .iter()
            .enumerate()
            .filter(|(_, x)| **x == id)
            .map(|(idx, _)| idx)
            .take(1)
            .collect();
        if let Some(idx) = idx.first() {
            entries.remove(*idx);
        }
        entries.insert(0, id);
        if entries.len() > self.capacity {
            return (true, entries.pop());
        }
        (true, None)
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
struct BlockId(u32);

impl Hash for BlockId {
    fn hash1(&self) -> u32 {
        self.0
    }

    fn hash2(&self) -> u32 {
        crc32(1, &self.0.to_be_bytes())
    }
}

type BlockValueMap = ConcurrentHopscotchHashMap<BlockId, Arc<(String, usize, Vec<u8>)>>;
type BlockLockMap = ConcurrentHopscotchHashMap<BlockId, ()>;

#[derive(Clone, Debug)]
pub(crate) struct PolicyHolder<P: CachePolicy> {
    policy: P,
    blocks: Arc<BlockValueMap>,
    block_locks: Arc<BlockLockMap>,
}

impl<P: CachePolicy> PolicyHolder<P> {
    fn lock(&self, block_id: u32) {
        while !self.block_locks.add(BlockId(block_id), ()) {}
    }

    fn unlock(&self, block_id: u32) {
        self.block_locks.remove(&BlockId(block_id));
    }

    /// On success, `read_block` returns the block offset and the value of the
    /// block. The block offset is the offset of the block in the blob.
    fn read_block_from_blob<B: BlobStore>(
        &self,
        blob_store_ref: &B,
        meta: &ThinSstMetadata,
        offset: usize,
    ) -> Result<(usize, Vec<u8>), CacheError> {
        // find the block that contains the offset
        let index_offsets = meta.offsets_ref();
        if index_offsets.is_empty() {
            return Err(CacheError::NoBlocks);
        }
        let mut offset_idx: Option<usize> = None;
        for (idx, (block_start, _)) in index_offsets.iter().enumerate() {
            if offset > *block_start {
                continue;
            }
            if offset == *block_start {
                offset_idx = Some(idx);
                break;
            }
            offset_idx = Some(idx - 1);
            break;
        }
        let block_start = if let Some(offset_idx) = offset_idx {
            index_offsets[offset_idx].0
        } else {
            index_offsets[index_offsets.len() - 1].0
        };
        let blob_size = meta.blob_size();
        let block_end = if let Some(offset_idx) = offset_idx {
            if offset_idx == index_offsets.len() - 1 {
                blob_size
            } else {
                index_offsets[offset_idx + 1].0
            }
        } else {
            blob_size
        };

        let blob_id = meta.blob_id_ref();
        let mut reader = blob_store_ref.read_cursor(blob_id)?;
        reader.skip(block_start)?;
        let block_len = block_end - block_start;
        let mut block_values = vec![0; block_len];
        reader.read_exact(&mut block_values)?;
        Ok((block_start, block_values))
    }

    fn update_cache<S: AsRef<[u8]>>(
        &self,
        meta: &ThinSstMetadata,
        block_id: u32,
        block_start: usize,
        block: S,
    ) {
        self.lock(block_id);
        let (to_insert, eviction) = self.policy.read(block_id);
        if to_insert {
            let block = block.as_ref();
            self.blocks.set(
                BlockId(block_id),
                Arc::new((meta.blob_id_ref().to_string(), block_start, block.to_vec())),
            );
        }
        if let Some(eviction) = eviction {
            self.blocks.remove(&BlockId(eviction));
        }
        self.unlock(block_id);
    }
}

impl<P: CachePolicy + Default> Default for PolicyHolder<P> {
    fn default() -> Self {
        PolicyHolder {
            policy: P::default(),
            blocks: Arc::new(ConcurrentHopscotchHashMap::new()),
            block_locks: Arc::new(ConcurrentHopscotchHashMap::new()),
        }
    }
}

impl<P: CachePolicy> BlockCache for PolicyHolder<P> {
    fn with_capacity(max_blocks_cached: usize) -> Self {
        let policy = P::with_capacity(max_blocks_cached);
        let blocks = Arc::new(ConcurrentHopscotchHashMap::new());
        let block_locks = Arc::new(ConcurrentHopscotchHashMap::new());
        PolicyHolder {
            policy,
            blocks,
            block_locks,
        }
    }

    fn read_exact<B: BlobStore>(
        &self,
        blob_store_ref: &B,
        meta: &ThinSstMetadata,
        block_id: u32,
        offset: usize,
        dest: &mut [u8],
    ) -> Result<(), CacheError> {
        if let Some(arc) = self.blocks.get(&BlockId(block_id)) {
            let (blob_id, block_start, block) = arc.as_ref();
            if blob_id == meta.blob_id_ref() {
                let block_len = block.len();
                if offset + dest.len() > block_len {
                    return Err(CacheError::BlockTooSmall);
                }
                dest.copy_from_slice(&block[offset..offset + dest.len()]);
                self.update_cache(meta, block_id, *block_start, block);
                return Ok(());
            }
        }
        let (block_start, block) = self.read_block_from_blob(blob_store_ref, meta, offset)?;
        let start = offset - block_start;
        let end = start + dest.len();
        if end > block.len() {
            return Err(CacheError::BlockTooSmall);
        }
        dest.copy_from_slice(&block[start..end]);
        self.update_cache(meta, block_id, block_start, block);
        Ok(())
    }

    fn read_block<B: BlobStore>(
        &self,
        blob_store_ref: &B,
        meta: &ThinSstMetadata,
        block_id: u32,
        block_start: usize,
    ) -> Result<Vec<u8>, CacheError> {
        if let Some(arc) = self.blocks.get(&BlockId(block_id)) {
            let (blob_id, block_start, block) = arc.as_ref().clone();
            if blob_id == meta.blob_id_ref() {
                self.update_cache(meta, block_id, block_start, &block);
                return Ok(block);
            }
        }
        let (block_start, block) = self.read_block_from_blob(blob_store_ref, meta, block_start)?;
        self.update_cache(meta, block_id, block_start, &block);
        Ok(block)
    }
}

// TODO(gs): Perhaps NoBlockCache should be used for high fan out scans?
#[allow(dead_code)]
pub(crate) type NoBlockCache = PolicyHolder<NeverCachePolicy>;
pub(crate) type LruBlockCache = PolicyHolder<LruCache>;
