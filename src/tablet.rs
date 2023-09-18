use std::collections::HashSet;
use std::iter::zip;
use std::sync::mpsc::Sender;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::compactor::CompactorMessage;
use crate::kv::{
    JoinedIter, MergedHomogenousIter, TombstonePointReader, TombstoneValue, TryTombstoneScanner,
};
use crate::sst::block_cache::StorageWrapper;
use crate::sst::common::{KeyRangeCmp, RefKeyRange, SstError};
use crate::sst::reader::SstReader;
use crate::sst::reader::SstScanner;
use crate::sst::writer::SstWriteSettings;

pub(crate) enum TaskCategory {
    Any,
    MajorCompaction,
}

pub(crate) enum CompactOrder<BC: StorageWrapper> {
    Minor {
        minor_sst_readers: Vec<SstReader<BC>>,
        l0_sst_readers: Vec<SstReader<BC>>,
        write_settings: SstWriteSettings,
    },
    Regular {
        lo_sst_readers: Vec<SstReader<BC>>,
        hi_sst_readers: Vec<SstReader<BC>>,
        target_level_no: usize,
        write_settings: SstWriteSettings,
    },
    Major {
        minor_sst_readers: Vec<SstReader<BC>>,
        level_sst_readers: Vec<Vec<SstReader<BC>>>,
        target_level_no: usize,
        write_settings: SstWriteSettings,
    },
}

impl<BC: StorageWrapper> CompactOrder<BC> {
    // `sst_readers` should be passed in order from oldest to youngest.
    fn minor<I1: Iterator<Item = SstReader<BC>>, I2: Iterator<Item = SstReader<BC>>>(
        minor_sst_readers: I1,
        l0_sst_readers: I2,
        base_write_settings: SstWriteSettings,
    ) -> Self {
        let minor_sst_readers: Vec<_> = minor_sst_readers.collect();
        let l0_sst_readers: Vec<_> = l0_sst_readers.collect();
        CompactOrder::Minor {
            minor_sst_readers,
            write_settings: base_write_settings,
            l0_sst_readers,
        }
    }

    fn regular<I1: Iterator<Item = SstReader<BC>>, I2: Iterator<Item = SstReader<BC>>>(
        lo_sst_readers: I1,
        hi_sst_readers: I2,
        target_level_no: usize,
        base_write_settings: SstWriteSettings,
    ) -> Self {
        let lo_sst_readers = lo_sst_readers.collect();
        let hi_sst_readers = hi_sst_readers.collect();
        let write_settings =
            base_write_settings.scale_to_level(target_level_no, LEVEL_BLOW_UP_FACTOR);
        CompactOrder::Regular {
            lo_sst_readers,
            hi_sst_readers,
            target_level_no,
            write_settings,
        }
    }

    fn major<
        I1: Iterator<Item = SstReader<BC>>,
        I2: Iterator<Item = SstReader<BC>>,
        I3: Iterator<Item = I2>,
    >(
        minor_sst_readers: I1,
        level_sst_readers: I3,
        target_level_no: usize,
        base_write_settings: SstWriteSettings,
    ) -> Self {
        let minor_sst_readers = minor_sst_readers.collect();
        let level_readers = level_sst_readers.map(|level| level.collect()).collect();
        let write_settings =
            base_write_settings.scale_to_level(target_level_no, LEVEL_BLOW_UP_FACTOR);
        CompactOrder::Major {
            minor_sst_readers,
            level_sst_readers: level_readers,
            target_level_no,
            write_settings,
        }
    }
}

/// An SstStore instance is a thread safe wrapper that allows a user to read
/// and write new SST files. It handles caching as well.
pub(crate) trait TabletSstStore: Clone + TombstonePointReader + TryTombstoneScanner {
    type BC: StorageWrapper;

    fn recover<S: ToString, I: Iterator<Item = S>>(
        storage_wrapper: Self::BC,
        levels: I,
        compactor_tx: Sender<CompactorMessage>,
    ) -> Result<Self, SstError>;
    fn write_settings_ref(&self) -> &SstWriteSettings;
    fn add_minor_sst(&self, sst_reader: SstReader<Self::BC>);
    fn request_task(&self, category: &TaskCategory) -> Option<CompactOrder<Self::BC>>;
    // Result is either a new SST reader or an error.
    fn handle_finished_task<I: Iterator<Item = SstReader<Self::BC>>>(
        &self,
        original_order: &CompactOrder<Self::BC>,
        new_readers: I,
    );
    fn get_sst_reader(&self, blob_id: &str) -> Option<SstReader<Self::BC>>;
}

#[derive(Clone, Debug)]
pub(crate) struct SmartTablet<BC: StorageWrapper> {
    write_settings: SstWriteSettings,
    /// Minor sst files, from oldest to youngest.
    ///
    /// What RocksDB calls "level 0" is stored in this collection.
    minors: Arc<RwLock<Vec<SstReader<BC>>>>,
    /// The levels of the SST files, ordered from youngest to oldest.
    ///
    /// What RocksDB calls "level 1" is stored at index 0, and so on.
    /// The length of levels is preallocated, and the address the `Arc`s point
    /// to is constant for the lifetime of the table. For that reason, it is
    /// safe to copy across threads.
    /// This means that SST in higher levels represent older data.
    ///
    /// Within each level, the SST files are ordered by their key range.
    /// SSTs within the same level do not have overlapping key range.
    /// Therefore, to read a key from a level, simply iterate over the SST
    /// until it is in one of their range, and then call the point read
    /// function on that SST. If it is not found during the iteration, then it
    /// is not present in the table.
    levels: Arc<Vec<RwLock<Vec<SstReader<BC>>>>>,
}

const LEVEL_BLOW_UP_FACTOR: usize = 4;
const MAX_NUM_LEVELS: usize = 8;

impl<BC: StorageWrapper> SmartTablet<BC> {
    pub(crate) fn new<S: ToString, I: Iterator<Item = S>>(
        storage_wrapper: BC,
        levels: I,
        compactor_tx: Sender<CompactorMessage>,
    ) -> Result<Self, SstError> {
        let results: Result<Vec<_>, _> = levels
            .map(|blob_id| {
                SstReader::new(
                    storage_wrapper.clone(),
                    &blob_id.to_string(),
                    compactor_tx.clone(),
                )
            })
            .collect();
        let minors = Arc::new(RwLock::new(results?));
        let levels = (0..MAX_NUM_LEVELS)
            .map(|_| RwLock::new(Vec::new()))
            .collect();
        let levels = Arc::new(levels);

        // TODO(t/1392): The default write settings should be configurable.
        let write_settings = SstWriteSettings::default();

        Ok(SmartTablet {
            write_settings,
            minors,
            levels,
        })
    }

    fn sst_snapshot(&self) -> (Vec<SstReader<BC>>, Vec<Vec<SstReader<BC>>>) {
        (
            self.minors.read().unwrap().clone(),
            (*self.levels)
                .iter()
                .map(|level| level.read().unwrap().clone())
                .collect(),
        )
    }

    fn most_urgent_task(&self) -> Option<CompactOrder<BC>> {
        // TODO(t/1393): This should really look at CPU and IO load
        // and adjust based on compaction time.
        // Right now, it just looks at the number of SST and if it is greater
        // than 10, it flips a coin and compacts either the youngest 5 or
        // the oldest 5. If the number is less than 10, it does a major
        // compaction.

        // It is okay if levels is stale. Since the compactor is single
        // threaded, it only means that some minor SST could have been
        // stacked on top during the compaction.

        let (minors, levels) = self.sst_snapshot();

        // prioritize minor compaction
        if minors.len() >= 2 {
            return if levels.is_empty() {
                Some(CompactOrder::minor(
                    minors.into_iter(),
                    vec![].into_iter(),
                    self.write_settings.clone().keep_tombstones(),
                ))
            } else {
                Some(CompactOrder::minor(
                    minors.into_iter(),
                    levels[0].clone().into_iter(),
                    self.write_settings.clone().keep_tombstones(),
                ))
            };
        }

        // Pick a random non-base level.
        let mut source_level = if let Ok(d) = SystemTime::now().duration_since(UNIX_EPOCH) {
            d.as_nanos() as usize % (MAX_NUM_LEVELS - 1)
        } else {
            0
        };

        // If the level is empty, iterate through levels from low to high to
        // find the first non-empty one.
        if levels[source_level].is_empty() {
            if let Some((i, _)) = levels
                .iter()
                .enumerate()
                .take(MAX_NUM_LEVELS - 1)
                .find(|(_, level)| !level.is_empty())
            {
                source_level = i;
            } else if !minors.is_empty() {
                return Some(CompactOrder::major(
                    minors.into_iter(),
                    levels.into_iter().map(|level| level.into_iter()),
                    MAX_NUM_LEVELS - 1,
                    self.write_settings.clone().discard_tombstones(),
                ));
            } else {
                // If all levels are empty, then there is nothing to compact.
                return None;
            }
        }

        let source_level = source_level;

        // Pick the two largest SSTs in the level
        // or just one if there is only one in the level.
        let lo_sst_readers;
        if levels[source_level].len() == 1 {
            lo_sst_readers = vec![levels[source_level][0].clone()];
        } else {
            let mut max_size = 0;
            let mut biggest_pair: Option<(&SstReader<BC>, &SstReader<BC>)> = None;
            let reader_pairs = zip(
                levels[source_level].iter().take(MAX_NUM_LEVELS - 1),
                levels[source_level].iter().skip(1),
            );
            for (reader_a, reader_b) in reader_pairs {
                let size = reader_a.size() + reader_b.size();
                if size > max_size {
                    max_size = size;
                    biggest_pair = Some((reader_a, reader_b));
                }
            }
            let biggest_pair = biggest_pair.unwrap();
            lo_sst_readers = vec![biggest_pair.0.clone(), biggest_pair.1.clone()];
        }
        let lo_key = lo_sst_readers[0].key_range_ref().start_ref();
        let hi_key = lo_sst_readers[lo_sst_readers.len() - 1]
            .key_range_ref()
            .end_ref();

        let lo_key_range = RefKeyRange::new(lo_key, hi_key);

        let target_level = source_level + 1;
        let mut hi_sst_readers = Vec::with_capacity(levels[target_level].len());
        for sst_reader in &levels[target_level] {
            let cmp = sst_reader.key_range_ref().intersects_range(&lo_key_range);
            match cmp {
                KeyRangeCmp::Less => continue,
                KeyRangeCmp::Greater => break,
                KeyRangeCmp::InRange => {
                    hi_sst_readers.push(sst_reader.clone());
                }
            }
        }

        let mut write_settings = self
            .write_settings
            .clone()
            .scale_to_level(target_level, LEVEL_BLOW_UP_FACTOR);
        if target_level == MAX_NUM_LEVELS - 1 {
            write_settings = write_settings.discard_tombstones();
        }

        Some(CompactOrder::regular(
            lo_sst_readers.into_iter(),
            hi_sst_readers.into_iter(),
            target_level,
            write_settings,
        ))
    }

    fn clear_minors(&self, old_minors: &[SstReader<BC>]) {
        let old_minors: HashSet<_> = old_minors.iter().map(|sst| sst.blob_id_ref()).collect();
        let mut minors = self.minors.write().unwrap();
        let mut minor_idx = 0;
        while minor_idx < minors.len() {
            let minor = &minors[minor_idx];
            if !old_minors.contains(&minor.blob_id_ref()) {
                minor_idx += 1;
                continue;
            }
            minors.remove(minor_idx);
        }
    }

    fn renew_level<I: Iterator<Item = SstReader<BC>>>(
        &self,
        level_no: usize,
        old_sst_readers: &[SstReader<BC>],
        new_sst_readers: I,
    ) {
        let old_sst_blob_ids = old_sst_readers
            .iter()
            .map(|sst| sst.blob_id_ref())
            .collect::<HashSet<_>>();

        let mut level = self.levels[level_no].write().unwrap();
        let mut sst_idx = 0;
        let mut remove_done = false;
        while sst_idx < level.len() {
            let sst = &level[sst_idx];
            if !old_sst_blob_ids.contains(&sst.blob_id_ref()) {
                if remove_done {
                    break;
                }
                sst_idx += 1;
                continue;
            }
            level.remove(sst_idx);
            remove_done = true;
        }
        for sst in new_sst_readers {
            level.insert(sst_idx, sst.clone());
        }
    }
}

impl<BC: StorageWrapper> TombstonePointReader for SmartTablet<BC> {
    type E = SstError;

    fn get(&self, key: &[u8]) -> Result<Option<TombstoneValue>, Self::E> {
        let (minors, levels) = self.sst_snapshot();
        for minor in minors.iter().rev() {
            if let Some(value) = minor.get(key)? {
                return Ok(Some(value));
            }
        }
        for level in levels.iter() {
            for sst in level.iter() {
                match sst.key_range_ref().in_range(key) {
                    KeyRangeCmp::Less => continue,
                    KeyRangeCmp::Greater => break,
                    KeyRangeCmp::InRange => {
                        // fall through
                    }
                }
                if let Some(value) = sst.get(key)? {
                    return Ok(Some(value));
                }
                break;
            }
        }
        Ok(None)
    }
}

impl<BC: StorageWrapper> TryTombstoneScanner for SmartTablet<BC> {
    type E = SstError;
    type I = JoinedIter<
        SstError,
        MergedHomogenousIter<SstScanner<BC>>,
        MergedHomogenousIter<MergedHomogenousIter<SstScanner<BC>>>,
    >;

    fn try_scan(&self) -> Result<Self::I, Self::E> {
        let (minors, levels) = self.sst_snapshot();
        let minor_iters = minors
            .into_iter()
            .map(|sst| sst.try_scan())
            .collect::<Result<Vec<_>, _>>()?;
        let minor_iter = MergedHomogenousIter::new(minor_iters.into_iter());
        let level_scanners: Vec<_> = levels
            .into_iter()
            .map(|level| {
                level
                    .into_iter()
                    .map(|sst| sst.try_scan())
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        let merged_level_scanners = level_scanners
            .into_iter()
            .map(|scanners| MergedHomogenousIter::new(scanners.into_iter()));
        let level_iter = MergedHomogenousIter::new(merged_level_scanners.into_iter());
        let scanner = JoinedIter::new(minor_iter, level_iter);
        Ok(scanner)
    }
}

unsafe impl<BC: StorageWrapper> Send for SmartTablet<BC>
where
    <BC as StorageWrapper>::B: Send,
    <BC as StorageWrapper>::C: Send,
{
}

impl<BC: StorageWrapper> TabletSstStore for SmartTablet<BC> {
    type BC = BC;

    fn recover<S: ToString, I: Iterator<Item = S>>(
        storage_wrapper: Self::BC,
        levels: I,
        compactor_tx: Sender<CompactorMessage>,
    ) -> Result<Self, SstError> {
        SmartTablet::new(storage_wrapper, levels, compactor_tx)
    }

    fn request_task(&self, category: &TaskCategory) -> Option<CompactOrder<Self::BC>> {
        match category {
            TaskCategory::Any => self.most_urgent_task(),
            TaskCategory::MajorCompaction => {
                let (minors, levels) = self.sst_snapshot();
                let level_sst_readers = levels.into_iter().map(|level| level.into_iter());
                Some(CompactOrder::major(
                    minors.into_iter(),
                    level_sst_readers,
                    MAX_NUM_LEVELS - 1,
                    self.write_settings.clone().discard_tombstones(),
                ))
            }
        }
    }

    fn handle_finished_task<I: Iterator<Item = SstReader<BC>>>(
        &self,
        original_order: &CompactOrder<BC>,
        new_readers: I,
    ) {
        // TODO(t/1397): This is not safe if their are multiple compactors.
        // The method `clear_minors` will be fine (if slow) if their are
        // multiple compactor threads, but the `renew_level` method assumes the
        // list of SST in a level is the same as when a compaction was
        // initiated.
        match original_order {
            CompactOrder::Minor {
                minor_sst_readers,
                l0_sst_readers,
                ..
            } => {
                self.clear_minors(minor_sst_readers.as_slice());
                self.renew_level(0, l0_sst_readers.as_slice(), new_readers);
            }
            CompactOrder::Regular {
                lo_sst_readers,
                hi_sst_readers,
                target_level_no,
                ..
            } => {
                self.renew_level(*target_level_no, hi_sst_readers, new_readers);
                self.renew_level(*target_level_no - 1, lo_sst_readers, vec![].into_iter());
            }
            CompactOrder::Major {
                minor_sst_readers,
                level_sst_readers,
                target_level_no,
                ..
            } => {
                debug_assert_eq!(*target_level_no, MAX_NUM_LEVELS - 1);
                debug_assert_eq!(level_sst_readers.len(), MAX_NUM_LEVELS);
                self.renew_level(
                    MAX_NUM_LEVELS - 1,
                    &level_sst_readers[MAX_NUM_LEVELS - 1],
                    new_readers,
                );
                for level_no in (0..MAX_NUM_LEVELS - 1).rev() {
                    self.renew_level(level_no, &level_sst_readers[level_no], vec![].into_iter());
                }
                self.clear_minors(minor_sst_readers.as_slice());
            }
        }
    }

    fn write_settings_ref(&self) -> &SstWriteSettings {
        &self.write_settings
    }

    fn add_minor_sst(&self, sst_reader: SstReader<Self::BC>) {
        self.minors.write().unwrap().push(sst_reader);
    }

    fn get_sst_reader(&self, blob_id: &str) -> Option<SstReader<Self::BC>> {
        let levels = self.minors.read().unwrap().clone();
        levels
            .into_iter()
            .find(|level| level.blob_id_ref() == blob_id)
    }
}

#[cfg(test)]
mod test {

    use std::sync::mpsc::channel;
    use std::thread::spawn;

    use super::*;
    use crate::compactor::Compactor;
    use crate::compactor::CompactorMessage;
    use crate::kv::TombstonePair;
    use crate::kv::TombstonePointReader;
    use crate::logging::debug;
    use crate::sst::block_cache::BlockCache;
    use crate::sst::block_cache::NoBlockCache;
    use crate::sst::block_cache::SimpleStorageWrapper;
    use crate::storage::blob_store::InMemoryBlobStore;

    type SuperSimpleStore = SimpleStorageWrapper<InMemoryBlobStore, NoBlockCache>;
    type SuperSimpleTabletSstStore = SmartTablet<SuperSimpleStore>;

    fn new_test_sst_store(
        compactor_tx: Option<Sender<CompactorMessage>>,
    ) -> (SuperSimpleStore, SuperSimpleTabletSstStore) {
        let blob_store = InMemoryBlobStore::new();
        // TODO(t/1387): This test should be run with the LRU block cache
        // when possible.
        type TestOnlyStorageWrapper = SimpleStorageWrapper<InMemoryBlobStore, NoBlockCache>;
        let storage_wrapper =
            SimpleStorageWrapper::from((blob_store, NoBlockCache::with_capacity(0)));
        type TestOnlyTabletSstStore = SmartTablet<TestOnlyStorageWrapper>;
        let levels: Vec<String> = Vec::new();
        if let Some(compactor_tx) = compactor_tx {
            return (
                storage_wrapper.clone(),
                TestOnlyTabletSstStore::new(storage_wrapper, levels.into_iter(), compactor_tx)
                    .unwrap(),
            );
        }
        let (compactor_tx, compactor_rx) = channel();
        spawn(move || {
            while let Ok(message) = compactor_rx.recv() {
                debug!("message: {:?}", message);
            }
        });
        (
            storage_wrapper.clone(),
            TestOnlyTabletSstStore::new(storage_wrapper, levels.into_iter(), compactor_tx).unwrap(),
        )
    }

    fn run_minor_compaction<
        BC: StorageWrapper,
        T: TabletSstStore<BC = BC>,
        I: Iterator<Item = TombstonePair>,
    >(
        storage_wrapper: BC,
        sst_store: T,
        pairs: I,
    ) {
        let mut compactor = Compactor::one_off(storage_wrapper, sst_store);
        compactor.minor_compact(pairs).unwrap();
    }

    #[test]
    fn test_basic_minor_compact() {
        let top: u64 = 1_024;

        let pairs: Vec<TombstonePair> = (0..top)
            .map(|i| {
                let key_bytes = i.to_be_bytes().to_vec();
                if i % 2 == 0 {
                    TombstonePair::new(key_bytes, (i * i).to_be_bytes().to_vec())
                } else {
                    TombstonePair::deletion_marker(key_bytes)
                }
            })
            .collect();

        let (storage_wrapper, sst_store) = new_test_sst_store(None);
        run_minor_compaction(storage_wrapper, sst_store.clone(), pairs.into_iter());

        for i in 0..top {
            let key_bytes = i.to_be_bytes().to_vec();
            let value = sst_store.get(&key_bytes).unwrap().unwrap();
            if i % 2 == 0 {
                assert_eq!(*value.as_ref().unwrap(), (i * i).to_be_bytes().to_vec());
            } else {
                assert!(value.as_ref().is_none());
            }

            let key_bytes = (top + i).to_be_bytes().to_vec();
            assert!(sst_store.get(&key_bytes).unwrap().is_none());
        }
    }

    fn complex_test_values(top: u64) -> Vec<(Vec<TombstonePair>, Vec<Option<TombstoneValue>>)> {
        let mut ret: Vec<(Vec<TombstonePair>, Vec<Option<TombstoneValue>>)> = Vec::with_capacity(4);

        // BATCH 0: evens are squares, odds are tombstones
        let size_1 = top / 8;
        let wal_values = (0..size_1)
            .map(|i| {
                let key_bytes = i.to_be_bytes().to_vec();
                if i % 2 == 0 {
                    TombstonePair::new(key_bytes, (i * i).to_be_bytes().to_vec())
                } else {
                    TombstonePair::deletion_marker(key_bytes)
                }
            })
            .collect();
        let mut expected_values: Vec<Option<TombstoneValue>> = vec![None; top as usize];
        for i in 0..size_1 {
            expected_values[i as usize] = if i % 2 == 0 {
                Some(TombstoneValue::Value((i * i).to_be_bytes().to_vec()))
            } else {
                Some(TombstoneValue::Tombstone)
            };
        }
        ret.push((wal_values, expected_values.clone()));

        // BATCH 1: every other even is half the square
        let size_2 = top / 4;
        let wal_values = (0..(size_2 / 4))
            .map(|i| {
                let j = 4 * i;
                let key_bytes = j.to_be_bytes().to_vec();
                let value_bytes = (j * j / 2).to_be_bytes().to_vec();
                TombstonePair::new(key_bytes, value_bytes)
            })
            .collect();
        for i in 0..size_2 {
            if i % 4 != 0 {
                continue;
            }
            expected_values[i as usize] =
                Some(TombstoneValue::Value((i * i / 2).to_be_bytes().to_vec()));
        }
        ret.push((wal_values, expected_values.clone()));

        // BATCH 2: every third value is set to 42
        let size_3 = top / 2;
        let wal_values = (0..(size_3 / 3))
            .map(|i| {
                let j = 3 * i;
                let key_bytes = j.to_be_bytes().to_vec();
                let value_bytes = 42u64.to_be_bytes().to_vec();
                TombstonePair::new(key_bytes, value_bytes)
            })
            .collect();
        for i in 0..(3 * (size_3 / 3)) {
            if i % 3 != 0 {
                continue;
            }
            expected_values[i as usize] = Some(TombstoneValue::Value(42u64.to_be_bytes().to_vec()));
        }
        ret.push((wal_values, expected_values.clone()));

        // BATCH 3: every 5th value is deleted
        let size_4 = top;
        let wal_values = (0..(size_4 / 5))
            .map(|i| {
                let j = 5 * i;
                let key_bytes = j.to_be_bytes().to_vec();
                TombstonePair::deletion_marker(key_bytes)
            })
            .collect();
        for i in 0..(5 * (size_4 / 5)) {
            if i % 5 != 0 {
                continue;
            }
            expected_values[i as usize] = Some(TombstoneValue::Tombstone);
        }
        ret.push((wal_values, expected_values));

        ret
    }

    #[test]
    fn test_multiple_minor_compact() {
        let top: u64 = 1_024;

        // TODO(t/1387): This test should be run with the LRU block cache
        // when it is implemented.
        let (storage_wrapper, sst_store) = new_test_sst_store(None);

        fn check_values<T: TabletSstStore>(
            store: &T,
            top: u64,
            prime: u64,
            expected_values: &[Option<TombstoneValue>],
        ) where
            String: From<<T as TombstonePointReader>::E>,
        {
            let mut all_checked = vec![false; top as usize];
            let mut idx = 0_u64;
            for _ in 0..top {
                idx += prime;
                idx %= top;
                all_checked[idx as usize] = true;
                let key_bytes = idx.to_be_bytes().to_vec();
                if expected_values[idx as usize].is_none() {
                    assert!(store.get(&key_bytes).unwrap().is_none());
                    continue;
                }
                let t_value = store.get(&key_bytes).unwrap().unwrap();
                let t_expected = expected_values[idx as usize].as_ref().unwrap().clone();
                if t_expected.as_ref().is_none() {
                    assert!(t_value.as_ref().is_none());
                    continue;
                }
                let expected: Vec<u8> = t_expected.as_ref().unwrap().clone();
                assert_eq!(*t_value.as_ref().unwrap(), expected);
            }
            assert!(all_checked.iter().all(|&b| b));
        }

        let primes = [1009, 1013, 1049, 2539];
        let test_values = complex_test_values(top);

        for (i, (wal_values, expected_values)) in test_values.into_iter().enumerate() {
            run_minor_compaction(
                storage_wrapper.clone(),
                sst_store.clone(),
                wal_values.into_iter(),
            );
            check_values(&sst_store, top, primes[i], &expected_values);
        }
    }

    #[test]
    fn test_multiple_scans() {
        let top = 1024;

        // TODO(t/1387): This test should be run with the LRU block cache
        // when it is implemented.
        let (compactor_tx, _compactor_rx) = channel();

        let (storage_wrapper, sst_store) = new_test_sst_store(Some(compactor_tx));

        let test_values = complex_test_values(top);
        for (wal_values, raw_expected_values) in test_values.into_iter() {
            run_minor_compaction(
                storage_wrapper.clone(),
                sst_store.clone(),
                wal_values.into_iter(),
            );
            let expected_values = raw_expected_values
                .into_iter()
                .enumerate()
                .filter_map(|(i, v)| {
                    v.as_ref()?;
                    let key_bytes = i.to_be_bytes().to_vec();
                    let value = v.unwrap();
                    Some((key_bytes, value))
                })
                .collect::<Vec<(Vec<u8>, TombstoneValue)>>();

            let iter = sst_store.try_scan().unwrap();
            for (idx, result) in iter.enumerate() {
                let found_pair = result.unwrap();
                let (key_bytes, expected_value) = expected_values[idx].clone();
                assert_eq!(found_pair.key_ref(), &key_bytes);
                assert_eq!(found_pair.value_ref().as_ref(), expected_value.as_ref());
            }
        }
    }

    #[test]
    fn test_level_updates_are_atomic() {
        // Time to beat is 8 sec with top = 1024 ** 2 / 8
        let top = 1024 * 1024 / 8;
        let n_partitions = 32;
        let n_scanners = 8;

        // TODO(t/1387): This test should be run with the LRU block cache
        // when it is implemented.
        let (compactor_tx, compactor_rx) = channel();
        let (storage_wrapper, sst_store) = new_test_sst_store(Some(compactor_tx.clone()));

        let compactor = Compactor::new(
            storage_wrapper.clone(),
            sst_store.clone(),
            compactor_tx.clone(),
            compactor_rx,
        );
        spawn(move || compactor.run());

        let test_values = complex_test_values(top);
        for (batch, (wal_values, raw_expected_values)) in
            test_values.into_iter().enumerate().take(3)
        {
            let mut seen_values = HashSet::new();
            for r in 0..n_partitions {
                let pairs = wal_values.iter().skip(r).step_by(n_partitions).cloned();
                run_minor_compaction(storage_wrapper.clone(), sst_store.clone(), pairs.clone());
                for pair in pairs {
                    seen_values.insert(pair.key_ref().to_vec());
                }
            }
            for wal_value in wal_values.iter() {
                assert!(seen_values.contains(wal_value.key_ref()));
            }
            assert_eq!(seen_values.len(), wal_values.len());
            let expected_values = raw_expected_values
                .into_iter()
                .enumerate()
                .filter_map(|(i, v)| {
                    v.as_ref()?;
                    let key_bytes = i.to_be_bytes().to_vec();
                    let value = v.unwrap();
                    Some((key_bytes, value))
                })
                .collect::<Vec<(Vec<u8>, TombstoneValue)>>();

            let mut children = Vec::with_capacity(n_scanners);
            for thread_id in 0..n_scanners {
                let iter = sst_store.try_scan().unwrap();
                let p_expected_values: Vec<_> = expected_values
                    .clone()
                    .into_iter()
                    .filter_map(|(key, value)| value.as_ref().map(|value| (key, value.clone())))
                    .collect();
                children.push(spawn(move || {
                    let real_pairs = iter
                        .map(|result| match result {
                            Ok(ok) => ok,
                            Err(err) => {
                                panic!("Error scanning: {:?}", err);
                            }
                        })
                        .filter_map(|pair| {
                            pair.value_ref()
                                .as_ref()
                                .map(|value| (pair.key_ref().to_vec(), value.clone()))
                        });
                    for (idx, (found_key, found_value)) in real_pairs.enumerate() {
                        let (expected_key, expected_value) = p_expected_values[idx].clone();
                        assert_eq!(
                            found_key, expected_key,
                            "thread_id: {}, idx: {}, batch: {}",
                            thread_id, idx, batch
                        );
                        assert_eq!(
                            found_value, expected_value,
                            "thread_id: {}, idx: {}, batch: {}",
                            thread_id, idx, batch
                        );
                    }
                }));
            }
            for child in children {
                if let Err(err) = child.join() {
                    panic!("Error joining thread: {:?}", err);
                }
            }
        }

        compactor_tx.send(CompactorMessage::Shutdown).unwrap();
    }
}
