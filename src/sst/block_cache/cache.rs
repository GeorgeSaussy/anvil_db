use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use crate::sst::block_cache::concurrent_hash_map::ConcurrentHashMap;

/// Implementations of BlockCache must be tread safe.
/// A clone of the implementation must still refer to the same underlying data.
/// Memory usage is an approximation. It should take into account the size of
/// the cached blocks, and any overhead that is not constant.
pub(crate) trait BlockCache: Clone {
    fn with_capacity(capacity_bytes: usize) -> Self;
    /// Shed memory until the memory usage by the cache is below the given
    /// threshold. This function may not be able to meet the request. For
    /// example, if the memory overhead of the cache implementation requires
    /// more memory than the threshold, then this function should shed as
    /// much as it can and return.
    fn shed_memory_until_below(&mut self, capacity_bytes: usize);
    fn get(&self, blob_id: &str, offset: u64) -> Option<Arc<Vec<u8>>>;
    fn put(&mut self, blob_id: &str, offset: u64, data: &[u8]);
}

#[derive(Clone, Debug)]
pub(crate) struct NoBlockCache {}

impl BlockCache for NoBlockCache {
    fn with_capacity(_capacity_bytes: usize) -> Self {
        NoBlockCache {}
    }

    fn shed_memory_until_below(&mut self, _capacity_bytes: usize) {}

    fn get(&self, _blob_id: &str, _offset: u64) -> Option<Arc<Vec<u8>>> {
        None
    }

    fn put(&mut self, _blob_id: &str, _offset: u64, _data: &[u8]) {}
}

// GENERIC LRU CACHE IMPLEMENTATION

#[allow(dead_code)]
struct LruCacheEntry<K, V> {
    value: Arc<V>,
    next_key: Option<K>,
    prev_key: Option<K>,
}

// TODO(t/1387): Check that this is thread safe.
// TODO(t/1387): Implement this functionality.
#[allow(dead_code)]
struct LruCache<K: Clone + Debug + Eq + Hash, V> {
    /// The number of entries that are allowed in the cache,
    /// not necessarily the memory usage of the cache.
    entry_capacity: usize,
    /// The current number of entries in the cache.
    entry_count: usize,
    map: ConcurrentHashMap<K, Arc<LruCacheEntry<K, V>>>,
    first_key: Option<K>,
    last_key: Option<K>,
}

impl<K: Clone + Debug + Eq + Hash, V> LruCache<K, V> {
    #[allow(dead_code)]
    fn new(entry_capacity: usize) -> Self {
        LruCache {
            entry_capacity,
            entry_count: 0,
            map: ConcurrentHashMap::new(),
            first_key: None,
            last_key: None,
        }
    }

    #[allow(dead_code)]
    fn get(&mut self, _key: &K) -> Option<Arc<V>> {
        todo!()
    }

    #[allow(dead_code)]
    fn put(&mut self, _key: K, _value: V) {
        todo!()
    }
}

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct LruBlockCache {
    used_bytes: usize,
    capacity_bytes: usize,
}

impl BlockCache for LruBlockCache {
    fn with_capacity(capacity_bytes: usize) -> Self {
        LruBlockCache {
            used_bytes: 0,
            capacity_bytes,
        }
    }

    fn shed_memory_until_below(&mut self, _capacity_bytes: usize) {
        todo!()
    }

    fn get(&self, _blob_id: &str, _offset: u64) -> Option<Arc<Vec<u8>>> {
        todo!()
    }

    fn put(&mut self, _blob_id: &str, _offset: u64, _data: &[u8]) {
        todo!()
    }
}
