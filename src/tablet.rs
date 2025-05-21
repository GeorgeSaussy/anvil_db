use std::collections::HashSet;
use std::iter::zip;
use std::sync::mpsc::Sender;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::compactor::CompactorOrder;
use crate::context::Context;
use crate::helpful_macros::unlock;
use crate::kv::{
    JoinedIter, MergedHomogenousIter, TombstonePointReader, TombstoneValue, TryTombstoneScanner,
};
use crate::logging::{error, info};
use crate::manifest::{
    LoadedLevelUpdateInfo, LoadedManifestUpdate, ManifestOperation, ManifestUpdateType, SstId,
};
use crate::sst::common::{KeyRangeCmp, RefKeyRange, SstError};
use crate::sst::reader::SstReader;
use crate::sst::reader::SstScanner;
use crate::sst::writer::SstWriteSettings;
use crate::storage::blob_store::BlobId;

/// [Tablet] wraps the SSTs for structured reads. It also handles caching.
#[derive(Clone)]
pub(crate) struct Tablet {
    write_settings: SstWriteSettings,
    /// Minor sst files, from oldest to youngest.
    ///
    /// What RocksDB calls "level 0" is stored in this collection.
    minors: Arc<RwLock<Vec<SstReader>>>,
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
    levels: Arc<Vec<RwLock<Vec<SstReader>>>>,
}

impl std::fmt::Debug for Tablet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let binding = self.minors.read().unwrap();
        let minors = binding.iter().count();
        let levels: Vec<_> = self
            .levels
            .iter()
            .map(|level| {
                let binding = level.read().unwrap();
                binding.iter().count()
            })
            .collect();
        f.debug_struct("Tablet")
            .field("minors", &minors)
            .field("levels", &levels.as_slice())
            .finish()
    }
}

const LEVEL_BLOW_UP_FACTOR: usize = 4;
const MAX_NUM_LEVELS: usize = 8;

impl Tablet {
    /// TODO(nit): This should use a [Manifest] struct instead of
    /// `minors` and `levels`.
    pub(crate) fn new<Ctx: Context>(
        ctx: &Ctx,
        minors: &[BlobId],
        levels: &[Vec<BlobId>],
        compactor_tx: Sender<CompactorOrder>,
    ) -> Result<Self, SstError> {
        let blob_store = ctx.blob_store_ref();
        let logger = ctx.logger();

        info!(
            logger,
            "recovering from minors {minors:?} and levels {levels:?}"
        );
        let result: Result<Vec<_>, _> = minors
            .iter()
            .map(|blob_id| SstReader::new(blob_store, blob_id, compactor_tx.clone()))
            .collect();
        let minors = Arc::new(RwLock::new(result?));

        let result: Result<Vec<_>, _> = levels
            .iter()
            .map(|blob_ids| {
                let level = blob_ids
                    .iter()
                    .map(|blob_id| SstReader::new(blob_store, blob_id, compactor_tx.clone()))
                    .collect::<Result<Vec<_>, _>>();
                match level {
                    Ok(level) => Ok(RwLock::new(level)),
                    Err(e) => {
                        error!(logger, "failed to recover level {blob_ids:?}: {e}");
                        Err(e)
                    }
                }
            })
            .collect();
        let mut level_vec = result?;
        level_vec.iter_mut().for_each(|lock| {
            let mut level = lock.write().unwrap();
            level.sort_by(|a, b| {
                a.key_range_ref()
                    .start_ref()
                    .cmp(b.key_range_ref().start_ref())
            });
        });
        while level_vec.len() < MAX_NUM_LEVELS {
            level_vec.push(RwLock::new(Vec::new()));
        }
        let levels = Arc::new(level_vec);

        // TODO(t/1392): The default write settings should be configurable.
        let write_settings = SstWriteSettings::default();

        Ok(Tablet {
            write_settings,
            minors,
            levels,
        })
    }

    fn add_minor_sst(&self, mut sst_readers: Vec<SstReader>) {
        unlock!(self.minors.write()).append(&mut sst_readers);
    }

    fn sst_snapshot(&self) -> (Vec<SstReader>, Vec<Vec<SstReader>>) {
        (
            self.minors.read().unwrap().clone(),
            (*self.levels)
                .iter()
                .map(|level| level.read().unwrap().clone())
                .collect(),
        )
    }

    fn maybe_major_order(
        &self,
        m: Vec<SstReader>,
        l: Vec<Vec<SstReader>>,
    ) -> Option<ManifestOperation> {
        let target_level_no = MAX_NUM_LEVELS - 1;
        let write_settings = self
            .write_settings
            .clone()
            .discard_tombstones()
            .scale_to_level(target_level_no, LEVEL_BLOW_UP_FACTOR);
        let m_count = m.len();
        let non_base_l_count = l
            .iter()
            .take(MAX_NUM_LEVELS - 1)
            .map(|level| level.len())
            .sum::<usize>();
        if m_count <= 1 && non_base_l_count == 0 {
            return None;
        }
        Some(ManifestOperation::major_order(
            m.into_iter(),
            l.into_iter()
                .enumerate()
                .map(|(level_no, readers)| (level_no, readers.into_iter())),
            write_settings,
            None,
        ))
    }

    fn most_urgent_task(&self) -> Option<ManifestOperation> {
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
            return Some(ManifestOperation::minor_merge_order(
                minors.into_iter(),
                levels.first().cloned().unwrap_or(Vec::new()).into_iter(),
                self.write_settings.clone().keep_tombstones(),
            ));
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
                .find(|(_, level)| !level.is_empty())
            {
                source_level = i;
            } else if !minors.is_empty() {
                return self.maybe_major_order(minors, levels);
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
            let mut biggest_pair: Option<(&SstReader, &SstReader)> = None;
            let reader_pairs = zip(
                levels[source_level].iter().take(MAX_NUM_LEVELS - 1),
                levels[source_level].iter().skip(1),
            );
            for (reader_a, reader_b) in reader_pairs {
                let size = reader_a.blob_size() + reader_b.blob_size();
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
        match target_level.cmp(&MAX_NUM_LEVELS) {
            std::cmp::Ordering::Equal => {
                return self.maybe_major_order(minors, levels);
            }
            std::cmp::Ordering::Greater => {
                return None;
            }
            _ => {}
        }
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

        if lo_sst_readers.len() == 1 && hi_sst_readers.is_empty() {
            return self.maybe_major_order(minors, levels);
        }
        let mut write_settings = self
            .write_settings
            .clone()
            .scale_to_level(target_level, LEVEL_BLOW_UP_FACTOR);
        if target_level == MAX_NUM_LEVELS - 1 {
            write_settings = write_settings.discard_tombstones();
        }

        let write_settings = write_settings.scale_to_level(target_level, LEVEL_BLOW_UP_FACTOR);
        let level_sst_readers = vec![
            (source_level, lo_sst_readers),
            (target_level, hi_sst_readers),
        ];
        Some(ManifestOperation::merge_order(
            level_sst_readers,
            write_settings,
        ))
    }

    /// Returns the removed [SstReader]s.
    fn clear_minors(&self, old_minors: &[SstId]) -> Vec<SstReader> {
        // println("clear_minors: {old_minors:?}");
        let old_minors: HashSet<String> = old_minors.iter().cloned().collect();
        let mut minors = unlock!(self.minors.write());
        let mut offset = 0;
        let mut old_readers = Vec::new();
        while offset < minors.len() {
            let minor = &minors[offset];
            if !old_minors.contains(minor.blob_id_ref()) {
                offset += 1;
                continue;
            }
            old_readers.push(minors.remove(offset));
        }
        old_readers
    }

    /// Warning: The levels must be in order from low to high.
    /// The iterator is of ([LevelNo], the old [SstReader], and the new
    /// [SstReader]. The removed [SstReader]s are returned.
    fn renew_levels(&self, level_info: &[LoadedLevelUpdateInfo]) -> Vec<SstReader> {
        #[cfg(test)]
        let mut last_level_no: Option<usize> = None;
        let mut old_readers = Vec::new();
        for info in level_info.iter().rev() {
            let level_no = info.level_no;
            let old_sst_ids = &info.compacted_sst_blob_ids;
            let new_sst_readers = &info.new_sst_readers;
            #[cfg(test)]
            {
                assert!(last_level_no.map(|last| last > level_no).unwrap_or(true));
                last_level_no = Some(level_no);
            }
            let old_sst_blob_ids = old_sst_ids.iter().cloned().collect::<HashSet<_>>();

            let mut level = unlock!(self.levels[level_no].write());
            let mut sst_idx = 0;
            let mut remove_done = false;
            while sst_idx < level.len() {
                let sst = &level[sst_idx];
                if !old_sst_blob_ids.contains(sst.blob_id_ref()) {
                    if remove_done {
                        break;
                    }
                    sst_idx += 1;
                    continue;
                }
                old_readers.push(level.remove(sst_idx));
                remove_done = true;
            }
            for sst in new_sst_readers {
                level.insert(sst_idx, sst.clone());
            }
        }
        old_readers
    }

    fn get<Ctx: Context>(&self, ctx: &Ctx, key: &[u8]) -> Result<Option<TombstoneValue>, SstError> {
        let (minors, levels) = self.sst_snapshot();
        for minor in minors.iter().rev() {
            if let Some(value) = minor.get(ctx, key)? {
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
                if let Some(value) = sst.get(ctx, key)? {
                    return Ok(Some(value));
                }
                break;
            }
        }
        Ok(None)
    }

    pub(crate) fn recover<Ctx: Context>(
        ctx: &Ctx,
        minors: &[BlobId],
        levels: &[Vec<BlobId>],
        compactor_tx: Sender<CompactorOrder>,
    ) -> Result<Self, SstError> {
        Tablet::new(ctx, minors, levels, compactor_tx)
    }

    pub(crate) fn request_task(&self) -> Option<ManifestOperation> {
        self.most_urgent_task()
    }

    /// Returns the removed [SstReader]s.
    pub(crate) fn update_index(&self, update: &LoadedManifestUpdate) -> Vec<SstReader> {
        // TODO(t/1397): This is not safe if their are multiple compactors.
        // The method `clear_minors` will be fine (if slow) if their are
        // multiple compactor threads, but the `renew_level` method assumes the
        // list of SST in a level is the same as when a compaction was
        // initiated.

        if update.update_type == ManifestUpdateType::GarbageCollection {
            return Vec::new();
        }

        let mut old_readers = self.renew_levels(&update.level_sst_updates);
        self.add_minor_sst(update.new_minor_ssts.iter().flatten().cloned().collect());
        let compacted_minors: Vec<_> = update
            .compacted_minor_sst_ids
            .iter()
            .flatten()
            .cloned()
            .collect();
        old_readers.extend(self.clear_minors(compacted_minors.as_slice()));
        old_readers
    }

    pub(crate) fn write_settings_ref(&self) -> &SstWriteSettings {
        &self.write_settings
    }

    pub(crate) fn fill_major_compact_order(
        &self,
        minor_sst_readers: &mut Vec<Vec<SstReader>>,
        level_sst_readers: &mut Vec<(usize, Vec<SstReader>)>,
        target_level_no: &mut usize,
        write_settings: &mut SstWriteSettings,
    ) {
        let (minors, levels) = self.sst_snapshot();
        *minor_sst_readers = vec![minors];
        *level_sst_readers = levels.into_iter().enumerate().collect();
        *target_level_no = MAX_NUM_LEVELS - 1;
        *write_settings = self.write_settings.clone().discard_tombstones();
    }
}

impl TombstonePointReader for Tablet {
    type Error = SstError;

    fn get<Ctx: Context>(
        &self,
        ctx: &Ctx,
        key: &[u8],
    ) -> Result<Option<TombstoneValue>, Self::Error> {
        self.get(ctx, key)
    }
}

pub(crate) type TabletIterator<'a, Ctx> = JoinedIter<
    SstError,
    MergedHomogenousIter<SstScanner<'a, Ctx>>,
    MergedHomogenousIter<MergedHomogenousIter<SstScanner<'a, Ctx>>>,
>;

impl TryTombstoneScanner for Tablet {
    type Error = SstError;
    type Iter<'a, Ctx>
        = TabletIterator<'a, Ctx>
    where
        Ctx: Context + 'a;

    fn try_scan<'a, Ctx: Context>(
        &self,
        storage_wrapper: &'a Ctx,
    ) -> Result<Self::Iter<'a, Ctx>, Self::Error> {
        let (minors, levels) = self.sst_snapshot();
        let minor_iters = minors
            .into_iter()
            .map(|sst| sst.try_scan(storage_wrapper))
            .collect::<Result<Vec<_>, _>>()?;
        let minor_iter = MergedHomogenousIter::new(minor_iters.into_iter());
        let level_scanners: Vec<_> = levels
            .into_iter()
            .rev()
            .map(|level| {
                level
                    .into_iter()
                    .map(|sst| sst.try_scan(storage_wrapper))
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

unsafe impl Send for Tablet {}

#[cfg(test)]
mod test {

    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicU32;
    use std::sync::mpsc::channel;
    use std::time::Duration;

    use super::*;
    use crate::compactor::Compactor;
    use crate::compactor::CompactorSettings;
    use crate::context::SimpleContext;
    use crate::helpful_macros::{clone, spawn, unwrap};
    use crate::kv::TombstonePair;
    use crate::logging::debug;
    use crate::logging::error;
    use crate::logging::info;
    use crate::logging::ArchiveLogger;
    use crate::logging::DefaultLogger;
    use crate::logging::FixPrefixLogger;
    use crate::logging::Logger;
    use crate::mem_table::MemTable;
    use crate::sst::block_cache::cache::LruCache;
    use crate::sst::block_cache::cache::PolicyHolder;
    use crate::sst::block_cache::BlockCache;
    use crate::sst::writer::NameGenerator;
    use crate::storage::blob_store::BlobStore;
    use crate::storage::blob_store::InMemoryBlobStore;
    use crate::wal::wal_wrapper::WalWrapperConfig;

    type SuperSimpleContext<L> = SimpleContext<InMemoryBlobStore, PolicyHolder<LruCache>, L>;

    fn new_test_sst_store(
        compactor_tx: Option<Sender<CompactorOrder>>,
    ) -> (SuperSimpleContext<DefaultLogger>, Tablet) {
        let logger = DefaultLogger::default();
        make_sst_store_from_logger(compactor_tx, logger)
    }
    fn make_sst_store_from_logger<L: Logger>(
        compactor_tx: Option<Sender<CompactorOrder>>,
        logger: L,
    ) -> (SuperSimpleContext<L>, Tablet) {
        let blob_store = InMemoryBlobStore::new();
        let ctx = SimpleContext::from((
            blob_store,
            PolicyHolder::<LruCache>::with_capacity(32),
            logger,
        ));
        let minors = Vec::new();
        let levels = Vec::new();
        if let Some(compactor_tx) = compactor_tx {
            return (
                ctx.clone(),
                Tablet::new(&ctx, &minors, &levels, compactor_tx).unwrap(),
            );
        }
        let (compactor_tx, compactor_rx) = channel();
        spawn!(move || {
            while let Ok(message) = compactor_rx.recv() {
                debug!(&(), "message: {:?}", message);
            }
        });
        (
            ctx.clone(),
            Tablet::new(&ctx, &minors, &levels, compactor_tx).unwrap(),
        )
    }

    fn run_minor_compaction<Ctx: Context + 'static, I: Iterator<Item = TombstonePair>>(
        ctx: &Ctx,
        sst_store: Tablet,
        wal_blob_id: &str,
        pairs: I,
    ) where
        <<Ctx as Context>::BlobStore as BlobStore>::WriteCursor: Send + Sync + 'static,
    {
        let mut gen = NameGenerator::new(2019);
        let write_settings = CompactorSettings::default().apply(
            sst_store
                .write_settings_ref()
                .clone()
                .keep_tombstones()
                .set_writing_minor_sst(true),
        );
        let (compactor_tx, _compactor_rx) = channel();
        let readers = unwrap!(Compactor::<Ctx>::minor_compact(
            ctx,
            "cptr-test",
            &mut gen,
            wal_blob_id,
            pairs,
            write_settings,
            compactor_tx
        ));
        sst_store.add_minor_sst(readers);
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
        let wal_blob_id = "wal_blob_id";

        let (ctx, sst_store) = new_test_sst_store(None);
        run_minor_compaction(&ctx, sst_store.clone(), wal_blob_id, pairs.into_iter());

        for i in 0..top {
            let key_bytes = i.to_be_bytes().to_vec();
            let value = sst_store.get(&ctx, &key_bytes).unwrap().unwrap();
            if i % 2 == 0 {
                assert!(
                    value.vec_ref().is_some(),
                    "could find read value at key {key_bytes:?}; index {i}",
                );
                assert_eq!(*value.vec_ref().unwrap(), (i * i).to_be_bytes().to_vec());
            } else {
                assert!(value.vec_ref().is_none());
            }

            let key_bytes = (top + i).to_be_bytes().to_vec();
            assert!(sst_store.get(&ctx, &key_bytes).unwrap().is_none());
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

        let (ctx, sst_store) = new_test_sst_store(None);

        fn check_values<Ctx: Context>(
            store: &Tablet,
            ctx: &Ctx,
            top: u64,
            prime: u64,
            expected_values: &[Option<TombstoneValue>],
        ) {
            let mut all_checked = vec![false; top as usize];
            let mut idx = 0_u64;
            for _ in 0..top {
                idx += prime;
                idx %= top;
                all_checked[idx as usize] = true;
                let key_bytes = idx.to_be_bytes().to_vec();
                if expected_values[idx as usize].is_none() {
                    assert!(store.get(ctx, &key_bytes).unwrap().is_none());
                    continue;
                }
                let t_value = store.get(ctx, &key_bytes).unwrap().unwrap();
                let t_expected = expected_values[idx as usize].as_ref().unwrap().clone();
                if t_expected.vec_ref().is_none() {
                    assert!(t_value.vec_ref().is_none());
                    continue;
                }
                let expected: Vec<u8> = t_expected.vec_ref().unwrap().clone();
                assert_eq!(*t_value.vec_ref().unwrap(), expected);
            }
            assert!(all_checked.iter().all(|&b| b));
        }

        let primes = [1009, 1013, 1049, 2539];
        let test_values = complex_test_values(top);

        for (i, (wal_values, expected_values)) in test_values.into_iter().enumerate() {
            run_minor_compaction(
                &ctx,
                sst_store.clone(),
                "dummy_wal_blob_id",
                wal_values.into_iter(),
            );
            check_values(&sst_store, &ctx, top, primes[i], &expected_values);
        }
    }

    #[test]
    fn test_multiple_scans() {
        let top = 1024;

        let (compactor_tx, _compactor_rx) = channel();
        let (ctx, sst_store) = new_test_sst_store(Some(compactor_tx));

        let test_values = complex_test_values(top);
        for (wal_values, raw_expected_values) in test_values.into_iter() {
            run_minor_compaction(
                &ctx,
                sst_store.clone(),
                "dummy_wal_blob_id",
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

            let iter = unwrap!(sst_store.try_scan(&ctx));
            for (idx, result) in iter.enumerate() {
                let found_pair = result.unwrap();
                let (key_bytes, expected_value) = expected_values[idx].clone();
                assert_eq!(found_pair.key_ref(), &key_bytes);
                assert_eq!(found_pair.value_ref().vec_ref(), expected_value.vec_ref());
            }
        }
    }

    fn level_updates_are_atomic_inner(thread_id: usize) -> Result<(), ArchiveLogger> {
        // Time to beat is 8 sec with top = 1024 ** 2 / 8
        let logger = ArchiveLogger::new(&format!("lg-{thread_id}"));

        let top = 1024 * 1024 / 8;
        let n_partitions = 32;
        let n_scanners = 8;

        info!(
            &logger,
            "running level_updates_are_atomic_inner thread_id: thread_id = {thread_id} ; top = \
             {top} ; n_partitions = {n_partitions} ; n_scanners = {n_scanners}"
        );

        let (compactor_tx, compactor_rx) = channel();
        let (ctx, sst_store) = new_test_sst_store(Some(compactor_tx.clone()));
        let mem_table = unwrap!(MemTable::new(
            ctx.clone(),
            compactor_tx.clone(),
            WalWrapperConfig::default()
        ));
        let compactor = Compactor::new(
            clone!(ctx),
            clone!(sst_store),
            CompactorSettings::default(),
            clone!(compactor_tx),
            compactor_rx,
            0,
            mem_table,
        );
        {
            let logger = logger.clone();
            spawn!(move || {
                info!(&logger, "starting compactor thread");
                compactor.run()
            });
        }

        info!(&logger, "compactor running in separate thread");

        let test_values = complex_test_values(top);
        for (batch, (wal_values, raw_expected_values)) in
            test_values.into_iter().enumerate().take(3)
        {
            info!(&logger, "setting up batch number: {batch}");
            let mut seen_values = HashSet::new();
            for r in 0..n_partitions {
                info!(
                    &logger,
                    "running minor compaction for partition number {r} in batch number {batch}"
                );
                let pairs: std::iter::Cloned<
                    std::iter::StepBy<std::iter::Skip<std::slice::Iter<'_, TombstonePair>>>,
                > = wal_values.iter().skip(r).step_by(n_partitions).cloned();
                run_minor_compaction(&ctx, sst_store.clone(), "", pairs.clone());
                for pair in pairs {
                    seen_values.insert(pair.key_ref().to_vec());
                }
            }
            for wal_value in wal_values.iter() {
                if !seen_values.contains(wal_value.key_ref()) {
                    error!(
                        &logger,
                        "wal value missing: batch_no = {batch} ; key = {key:?}",
                        key = wal_value.key_ref()
                    );
                    return Err(logger.clone());
                }
            }
            if seen_values.len() != wal_values.len() {
                error!(
                    &logger,
                    "seen values: {seen_values:?} ; wal values: {wal_values:?}"
                );
                return Err(logger.clone());
            }
            let expected_values = raw_expected_values
                .into_iter()
                .enumerate()
                .filter_map(|(i, v)| {
                    let key_bytes = i.to_be_bytes().to_vec();
                    let value = v.as_ref()?.vec_ref()?.clone();
                    Some((key_bytes, value))
                })
                .collect::<Vec<(Vec<u8>, Vec<u8>)>>();
            let logger = logger.clone();
            let num_errors = Arc::new(AtomicU32::new(0));
            std::thread::sleep(Duration::from_millis(5000));
            std::thread::scope(|s| {
                let mut children = Vec::with_capacity(n_scanners);
                for sub_thread_id in 0..n_scanners {
                    let sst_store = sst_store.clone();
                    let ctx = ctx.clone();
                    let p_expected_values = expected_values.clone();
                    let logger = logger.clone();
                    let num_errors = num_errors.clone();
                    children.push(s.spawn(move || {
                        let iter = sst_store.try_scan(&ctx).unwrap();
                        info!(
                            &logger,
                            "starting thread_id: {thread_id}-{sub_thread_id}, batch: {batch}"
                        );
                        let real_pairs: Vec<_> = iter
                            .map(|result| match result {
                                Ok(ok) => ok,
                                Err(err) => {
                                    panic!("Error scanning: {err:?}");
                                }
                            })
                            .filter_map(|pair| {
                                pair.value_ref()
                                    .vec_ref()
                                    .map(|value| (pair.key_ref().to_vec(), value.clone()))
                            })
                            .collect();
                        for (idx, (found_key, found_value)) in real_pairs.iter().enumerate() {
                            let (expected_key, expected_value) = p_expected_values[idx].clone();

                            if *found_key != expected_key {
                                error!(
                                    &logger,
                                    "found_key: {found_key:?}\nexpected_key: {expected_key:?}\n \
                                     thread_id: {thread_id}-{sub_thread_id}, idx: {idx}, batch: \
                                     {batch}",
                                );
                                num_errors.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                return;
                            }

                            if *found_value != expected_value {
                                error!(
                                    &logger,
                                    "found_key: {found_key:?}\nexpected_key: {expected_key:?}\n \
                                     found_value: {found_value:?}\nexpected_value: \
                                     {expected_value:?}\nthread_id: {thread_id}-{sub_thread_id}, \
                                     idx: {idx}, batch: {batch}",
                                );
                                num_errors.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                return;
                            }
                        }
                        info!(
                            &logger,
                            "ending thread_id: {thread_id}-{sub_thread_id}, batch: {batch}"
                        );
                    }));
                }
                for (sub_thread_id, child) in children.into_iter().enumerate() {
                    if child.join().is_err() {
                        error!(
                            &logger,
                            "Error joining thread_id: {thread_id}-{sub_thread_id}, batch: {batch}"
                        );
                        num_errors.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        return;
                    }
                }
            });
            if num_errors.load(std::sync::atomic::Ordering::SeqCst) > 0 {
                error!(
                    &logger,
                    "thread_id: {thread_id}, batch: {batch}, num_errors: {}",
                    num_errors.load(std::sync::atomic::Ordering::SeqCst)
                );
                return Err(logger.clone());
            }
        }

        let (done_tx, done_rx) = channel();
        if let Err(err) = compactor_tx.send(CompactorOrder::Shutdown { done_tx }) {
            error!(
                &logger,
                "Error sending shutdown message to compactor: {err:?}",
            );
            return Err(logger.clone());
        }
        if let Err(err) = done_rx.recv() {
            error!(
                &logger,
                "Error receiving shutdown message from compactor: {err:?}",
            );
            return Err(logger.clone());
        }
        Ok(())
    }

    #[test]
    fn test_level_updates_are_atomic_1() {
        if let Err(logger) = level_updates_are_atomic_inner(0) {
            let inner = FixPrefixLogger::new("[HISTORY]");
            for line in logger.history_ref() {
                debug!(&inner, "{line}");
            }
            panic!();
        }
    }

    #[test]
    fn test_level_updates_are_atomic_2() {
        let top = 4;
        let start = Arc::new(AtomicBool::new(false));
        let mut handles = Vec::with_capacity(top);
        let (logger_tx, logger_rx) = channel();
        for thread_id in 0..top {
            let start = start.clone();
            let logger_tx = logger_tx.clone();
            handles.push(spawn!(move || {
                while !start.load(std::sync::atomic::Ordering::SeqCst) {
                    std::thread::yield_now();
                }
                let _ = logger_tx.send(level_updates_are_atomic_inner(thread_id));
            }));
        }
        start.store(true, std::sync::atomic::Ordering::SeqCst);
        for _ in 0..top {
            let result = unwrap!(logger_rx.recv());
            if let Err(logger) = result {
                let inner = FixPrefixLogger::new("[HISTORY]");
                for line in logger.history_ref() {
                    debug!(&inner, "{line}");
                }
                panic!();
            }
        }
        for handle in handles {
            if let Err(err) = handle.join() {
                panic!("Error joining thread: {err:?}",);
            }
        }
    }
}
