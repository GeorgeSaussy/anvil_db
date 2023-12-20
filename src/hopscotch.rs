// TODO(t/1443): Open source the hopscotch hashing implementation as a separate
// crate.

use std::array::from_fn;
use std::mem::replace;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{RwLock, RwLockReadGuard};

use crate::helpful_macros::unlock;

// TODO(t/1443): Check if the [Junction][1] concurrent hash map implementation
// is a better fit for our use case. The main issue is that Junction requires
// atomic which requires `unsafe` in Rust.
//
// [1]: https://preshing.com/20160201/new-concurrent-hash-maps-for-cpp/

const HOP_RANGE: usize = 32;
const NUM_SEGMENTS: usize = 1024;
const INIT_BUCKETS_PER_SEGMENT: usize = 8;
const MAX_TRIES: usize = 2;
const ADD_RANGE: usize = 256;

pub(crate) trait Hash {
    fn hash1(&self) -> u32;
    fn hash2(&self) -> u32;
}

/// The struct `ConcurrentHopscotchHashMap` is based on the data
/// structure described in the paper "Hopscotch Hashing" by
/// Herlihy, Shavit, and Tzafrir.
#[derive(Debug)]
pub(crate) struct ConcurrentHopscotchHashMap<K, V> {
    segments: [RwLock<Segment<K, V>>; NUM_SEGMENTS],
}

#[derive(Debug)]
enum PairWrapper<K, V> {
    None,
    Pair((K, V)),
    Busy,
}

impl<K, V> PairWrapper<K, V> {
    fn is_none(&self) -> bool {
        matches!(self, PairWrapper::None)
    }
}

#[derive(Debug)]
struct Bucket<K, V> {
    hop_info: AtomicU32,
    pair: Box<RwLock<PairWrapper<K, V>>>,
    timestamp: AtomicU32,
    is_locked: AtomicBool,
}

#[derive(Debug)]
struct Segment<K, V> {
    buckets: Vec<Bucket<K, V>>,
}

impl<K: Eq + Hash, V: Clone> Segment<K, V> {
    fn new(init_len: usize) -> Self {
        let buckets = (0..init_len)
            .map(|_| Bucket {
                hop_info: AtomicU32::new(0),
                pair: Box::new(RwLock::new(PairWrapper::None)),
                timestamp: AtomicU32::new(0),
                is_locked: AtomicBool::new(false),
            })
            .collect::<Vec<_>>();
        Segment { buckets }
    }

    fn len(&self) -> usize {
        self.buckets.len()
    }

    fn set_if_contains(&self, key: K, value: V) -> Option<(K, V)> {
        let i_bucket = key.hash2() as usize % self.buckets.len();
        let start_bucket = &self.buckets[i_bucket];
        let mut try_counter = 0;
        let mut timestamp;
        loop {
            timestamp = start_bucket.timestamp.load(Ordering::Relaxed);
            let hop_info = start_bucket.hop_info.load(Ordering::Relaxed);
            for bit_idx in (0..32).rev() {
                if hop_info & (1 << bit_idx) == 0 {
                    continue;
                }
                let check_idx = i_bucket + bit_idx;
                let check_bucket = &self.buckets[check_idx];
                {
                    // fast fail
                    let pair = unlock!(check_bucket.pair.read());
                    let (check_key, _) = if let PairWrapper::Pair((check_key, check_value)) = &*pair
                    {
                        (check_key, check_value)
                    } else {
                        continue;
                    };
                    if key != *check_key {
                        continue;
                    }
                }
                // check again under lock
                let mut pair = unlock!(check_bucket.pair.write());
                let (check_key, _) = if let PairWrapper::Pair((check_key, check_value)) = &*pair {
                    (check_key, check_value)
                } else {
                    continue;
                };
                if key != *check_key {
                    continue;
                }
                *pair = PairWrapper::Pair((key, value));
                return None;
            }

            try_counter += 1;
            if timestamp == start_bucket.timestamp.load(Ordering::Relaxed)
                || try_counter >= MAX_TRIES
            {
                break;
            }
        }
        if timestamp == start_bucket.timestamp.load(Ordering::Relaxed) {
            return Some((key, value));
        }
        for hop_idx in 0..HOP_RANGE {
            let bucket_idx = i_bucket + hop_idx;
            if bucket_idx >= self.len() {
                break;
            }
            let check_bucket = &self.buckets[bucket_idx];
            {
                // fast fail
                let pair = unlock!(check_bucket.pair.read());
                let (check_key, _) = if let PairWrapper::Pair((check_key, check_value)) = &*pair {
                    (check_key, check_value)
                } else {
                    continue;
                };
                if key != *check_key {
                    continue;
                }
            }
            // check again under lock
            let mut pair = unlock!(check_bucket.pair.write());
            let (check_key, _) = if let PairWrapper::Pair((check_key, check_value)) = &*pair {
                (check_key, check_value)
            } else {
                continue;
            };
            if key != *check_key {
                continue;
            }
            *pair = PairWrapper::Pair((key, value));
            return None;
        }
        Some((key, value))
    }

    fn find_closer_free_bucket(
        &self,
        free_bucket_idx: &mut usize,
        free_distance: &mut usize,
    ) -> bool {
        let mut move_bucket_idx = *free_bucket_idx - (HOP_RANGE - 1);
        let move_bucket = &self.buckets[move_bucket_idx];
        for free_dist in (0..HOP_RANGE).rev() {
            let start_hop_info = move_bucket.hop_info.load(Ordering::Relaxed);
            let mut move_free_distance: Option<usize> = None;
            let mut mask = 1;
            for i in 0..free_dist {
                if mask & start_hop_info != 0 {
                    move_free_distance = Some(i);
                    break;
                }
                mask <<= 1;
            }
            if let Some(move_free_distance) = move_free_distance {
                let move_bucket = &self.buckets[move_bucket_idx];
                move_bucket.lock();
                if start_hop_info != move_bucket.hop_info.load(Ordering::Relaxed) {
                    move_bucket.unlock();
                    continue;
                }

                move_bucket
                    .hop_info
                    .store(start_hop_info & !(1 << free_dist), Ordering::Relaxed);

                let new_free_bucket_idx = move_bucket_idx + move_free_distance;
                let new_free_bucket = &self.buckets[new_free_bucket_idx];
                let mut pair = unlock!(new_free_bucket.pair.write());
                let new_raw_pair = replace(&mut *pair, PairWrapper::Busy);

                let free_bucket = &self.buckets[*free_bucket_idx];
                let mut pair = unlock!(free_bucket.pair.write());
                *pair = new_raw_pair;

                move_bucket.timestamp.fetch_add(1, Ordering::Relaxed);

                *free_bucket_idx = new_free_bucket_idx;
                *free_distance -= free_dist;

                move_bucket.unlock();
                return true;
            }
            move_bucket_idx += 1;
        }
        false
    }

    fn set(&self, key: K, value: V) -> Option<(K, V)> {
        let i_bucket = key.hash2() as usize % self.buckets.len();
        let start_bucket = &self.buckets[i_bucket];
        start_bucket.lock();
        let refund = self.set_if_contains(key, value);
        let (key, value) = if let Some((key, value)) = refund {
            (key, value)
        } else {
            start_bucket.unlock();
            return None;
        };
        let mut free_bucket_idx = i_bucket;
        let mut free_distance = 0;
        while free_distance < ADD_RANGE && free_bucket_idx < self.buckets.len() {
            let free_bucket = &self.buckets[free_bucket_idx];
            {
                // fast fail
                let pair = unlock!(free_bucket.pair.read());
                if !pair.is_none() {
                    free_bucket_idx += 1;
                    free_distance += 1;
                    continue;
                }
            }
            // check again under lock
            let mut pair = unlock!(free_bucket.pair.write());
            if pair.is_none() {
                *pair = PairWrapper::Busy;
                break;
            }
            free_bucket_idx += 1;
            free_distance += 1;
        }
        if free_distance < ADD_RANGE && free_bucket_idx < self.buckets.len() {
            loop {
                let free_bucket = &self.buckets[free_bucket_idx];
                if free_distance < HOP_RANGE {
                    let new_hop_info =
                        start_bucket.hop_info.load(Ordering::Relaxed) | (1 << free_distance);
                    start_bucket.hop_info.store(new_hop_info, Ordering::Relaxed);
                    let mut pair = unlock!(free_bucket.pair.write());
                    *pair = PairWrapper::Pair((key, value));
                    start_bucket.unlock();
                    return None;
                }
                if !self.find_closer_free_bucket(&mut free_bucket_idx, &mut free_distance) {
                    break;
                }
            }
        }
        start_bucket.unlock();
        Some((key, value))
    }

    fn get(&self, key: &K) -> Option<V> {
        fn wrap_true_return<K1: Eq, V1: Clone>(
            key: &K1,
            pair: RwLockReadGuard<'_, PairWrapper<K1, V1>>,
        ) -> Option<V1> {
            let (check_key, check_value) =
                if let PairWrapper::Pair((check_key, check_value)) = &*pair {
                    (check_key, check_value)
                } else {
                    return None;
                };
            if *key != *check_key {
                return None;
            }
            Some(check_value.clone())
        }

        let i_bucket = key.hash2() as usize % self.buckets.len();
        let start_bucket = &self.buckets[i_bucket];
        let mut try_counter = 0;
        let mut timestamp;
        loop {
            timestamp = start_bucket.timestamp.load(Ordering::Relaxed);
            let hop_info = start_bucket.hop_info.load(Ordering::Relaxed);
            for bit_idx in (0..32).rev() {
                if hop_info & (1 << bit_idx) == 0 {
                    continue;
                }
                let check_idx = i_bucket + bit_idx;
                let check_bucket = &self.buckets[check_idx];
                let pair = unlock!(check_bucket.pair.read());
                let value = wrap_true_return(key, pair);
                if value.is_some() {
                    return value;
                }
            }

            try_counter += 1;
            if timestamp == start_bucket.timestamp.load(Ordering::Relaxed)
                || try_counter >= MAX_TRIES
            {
                break;
            }
        }
        if timestamp == start_bucket.timestamp.load(Ordering::Relaxed) {
            return None;
        }
        for hop_idx in 0..HOP_RANGE {
            let bucket_idx = i_bucket + hop_idx;
            if bucket_idx >= self.buckets.len() {
                break;
            }
            let check_bucket = &self.buckets[bucket_idx];
            let pair = unlock!(check_bucket.pair.read());
            let value = wrap_true_return(key, pair);
            if value.is_some() {
                return value;
            }
        }
        None
    }

    fn remove(&self, key: &K) {
        let i_bucket = key.hash2() as usize % self.buckets.len();
        let start_bucket = &self.buckets[i_bucket];
        start_bucket.lock();

        let hop_info = start_bucket.hop_info.load(Ordering::Relaxed);
        for bit_idx in (0..32).rev() {
            if hop_info & (1 << bit_idx) == 0 {
                continue;
            }
            let check_idx = i_bucket + bit_idx;
            let check_bucket = &self.buckets[check_idx];
            {
                // fast rejection
                let pair = unlock!(check_bucket.pair.read());
                if let PairWrapper::Pair((check_key, _)) = &*pair {
                    if key != check_key {
                        continue;
                    }
                } else {
                    continue;
                }
            }
            // deleted between iterations
            let mut pair = unlock!(check_bucket.pair.write());
            if let PairWrapper::Pair((check_key, _)) = &*pair {
                if key != check_key {
                    continue;
                }
            } else {
                continue;
            }
            *pair = PairWrapper::None;
            let new_hop_info = hop_info & !(1 << bit_idx);
            start_bucket.hop_info.store(new_hop_info, Ordering::Relaxed);
            start_bucket.unlock();
            return;
        }
        start_bucket.unlock();
    }

    /// Returns true iff the segment contains the key.
    fn check_contains(&self, key: &K) -> bool {
        let i_bucket = key.hash2() as usize % self.buckets.len();
        let start_bucket = &self.buckets[i_bucket];
        let mut try_counter = 0;
        let mut timestamp;
        loop {
            timestamp = start_bucket.timestamp.load(Ordering::Relaxed);
            let hop_info = start_bucket.hop_info.load(Ordering::Relaxed);
            for bit_idx in (0..32).rev() {
                if hop_info & (1 << bit_idx) == 0 {
                    continue;
                }
                let check_idx = i_bucket + bit_idx;
                let check_bucket = &self.buckets[check_idx];
                let pair = unlock!(check_bucket.pair.read());
                let (check_key, _) = if let PairWrapper::Pair((check_key, check_value)) = &*pair {
                    (check_key, check_value)
                } else {
                    continue;
                };
                if key != check_key {
                    continue;
                }
                return true;
            }

            try_counter += 1;
            if timestamp == start_bucket.timestamp.load(Ordering::Relaxed)
                || try_counter >= MAX_TRIES
            {
                break;
            }
        }
        if timestamp == start_bucket.timestamp.load(Ordering::Relaxed) {
            return false;
        }
        for hop_idx in 0..HOP_RANGE {
            let bucket_idx = i_bucket + hop_idx;
            if bucket_idx >= self.len() {
                break;
            }
            let check_bucket = &self.buckets[bucket_idx];
            let pair = unlock!(check_bucket.pair.read());
            let (check_key, _) = if let PairWrapper::Pair((check_key, check_value)) = &*pair {
                (check_key, check_value)
            } else {
                continue;
            };
            if key != check_key {
                continue;
            }
            return true;
        }
        false
    }

    /// Returns (a, b) where a is whether the pair was added or refunds the
    /// pair if a resize is required.
    fn add(&self, key: K, value: V) -> Result<bool, (K, V)> {
        let i_bucket = key.hash2() as usize % self.buckets.len();
        let start_bucket = &self.buckets[i_bucket];
        start_bucket.lock();
        if self.check_contains(&key) {
            start_bucket.unlock();
            return Ok(false);
        }
        let mut free_bucket_idx = i_bucket;
        let mut free_distance = 0;
        while free_distance < ADD_RANGE && free_bucket_idx < self.buckets.len() {
            let free_bucket = &self.buckets[free_bucket_idx];
            {
                // fast fail
                let pair = unlock!(free_bucket.pair.read());
                if !pair.is_none() {
                    free_bucket_idx += 1;
                    free_distance += 1;
                    continue;
                }
            }
            // check again under lock
            let mut pair = unlock!(free_bucket.pair.write());
            if pair.is_none() {
                *pair = PairWrapper::Busy;
                break;
            }
            free_bucket_idx += 1;
            free_distance += 1;
        }
        if free_distance < ADD_RANGE && free_bucket_idx < self.buckets.len() {
            loop {
                let free_bucket = &self.buckets[free_bucket_idx];
                if free_distance < HOP_RANGE {
                    let new_hop_info =
                        start_bucket.hop_info.load(Ordering::Relaxed) | (1 << free_distance);
                    start_bucket.hop_info.store(new_hop_info, Ordering::Relaxed);
                    let mut pair = unlock!(free_bucket.pair.write());
                    *pair = PairWrapper::Pair((key, value));
                    start_bucket.unlock();
                    return Ok(true);
                }
                if !self.find_closer_free_bucket(&mut free_bucket_idx, &mut free_distance) {
                    break;
                }
            }
        }
        start_bucket.unlock();
        Err((key, value))
    }
}

impl<K: Eq + Hash, V: Clone> ConcurrentHopscotchHashMap<K, V> {
    pub(crate) fn new() -> Self {
        let segments: [RwLock<_>; 1024] =
            from_fn(|_| RwLock::new(Segment::new(INIT_BUCKETS_PER_SEGMENT)));
        ConcurrentHopscotchHashMap { segments }
    }

    fn resize(&self, segment_idx: usize) {
        let mut old_segment = unlock!(self.segments[segment_idx].write());
        let new_segment = Segment::new(2 * old_segment.len());
        for bucket in old_segment.buckets.drain(..) {
            let pair = bucket.pair.into_inner().unwrap();
            let (key, value) = if let PairWrapper::Pair((key, value)) = pair {
                (key, value)
            } else {
                continue;
            };
            debug_assert!(new_segment.set(key, value).is_none());
        }
        *old_segment = new_segment;
    }

    pub(crate) fn set(&self, key: K, value: V) {
        let i_segment = key.hash1() as usize % self.segments.len();

        let mut key = key;
        let mut value = value;
        loop {
            {
                let segment = unlock!(self.segments[i_segment].read());
                if let Some((my_key, my_value)) = segment.set(key, value) {
                    key = my_key;
                    value = my_value;
                } else {
                    return;
                }
            }
            self.resize(i_segment);
        }
    }

    pub(crate) fn get(&self, key: &K) -> Option<V> {
        let i_segment = key.hash1() as usize % self.segments.len();

        let segment = unlock!(self.segments[i_segment].read());
        segment.get(key)
    }

    pub(crate) fn remove(&self, key: &K) {
        let i_segment = key.hash1() as usize % self.segments.len();

        let segment = unlock!(self.segments[i_segment].read());
        segment.remove(key);
    }

    pub(crate) fn add(&self, key: K, value: V) -> bool {
        let i_segment = key.hash1() as usize % self.segments.len();

        let mut key = key;
        let mut value = value;
        loop {
            {
                let segment = unlock!(self.segments[i_segment].read());
                match segment.add(key, value) {
                    Ok(b) => return b,
                    Err((k, v)) => {
                        key = k;
                        value = v;
                    }
                }
            }
            self.resize(i_segment);
        }
    }
}

impl<K, V> Bucket<K, V> {
    fn lock(&self) {
        while self
            .is_locked
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {}
    }

    fn unlock(&self) {
        assert!(self
            .is_locked
            .compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::thread::spawn;

    use super::*;
    use crate::checksum::crc32;
    use crate::helpful_macros::unwrap;
    use crate::logging::debug;

    #[derive(Clone, PartialEq, Eq)]
    struct HashU32(u32);

    impl Hash for HashU32 {
        fn hash1(&self) -> u32 {
            self.0
        }

        fn hash2(&self) -> u32 {
            crc32(1, &self.0.to_be_bytes())
        }
    }

    #[test]
    fn test_basic_hash_map() {
        let n_threads = 16;
        let top = 1024;

        let map: Arc<ConcurrentHopscotchHashMap<HashU32, u32>> =
            Arc::new(ConcurrentHopscotchHashMap::new());
        let mut handles = Vec::with_capacity(top);
        for thread_idx in 0..n_threads {
            let map = map.clone();
            let handle = spawn(move || {
                for i in 0..top {
                    let ii = (top * thread_idx + i) as u32;
                    let found = map.get(&HashU32(ii));
                    assert!(found.is_none());
                    map.set(HashU32(ii), ii);
                    let found = map.get(&HashU32(ii));
                    assert_eq!(unwrap!(found), ii);
                    debug!("set done: thread_idx: {}, i: {}", thread_idx, i);
                }
                for i in 0..top {
                    let ii = (top * thread_idx + i) as u32;
                    let found = map.get(&HashU32(ii));
                    assert_eq!(unwrap!(found), ii);
                    map.remove(&HashU32(ii));
                    let found = map.get(&HashU32(ii));
                    assert!(found.is_none());
                    debug!("remove done: thread_idx: {}, i: {}", thread_idx, i);
                }
            });
            handles.push(handle);
        }
        for handle in handles {
            unwrap!(handle.join());
        }
    }
}
