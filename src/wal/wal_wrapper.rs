use std::collections::HashSet;
use std::fmt::Debug;

use super::wal_writer::INIT_WAL_CHECKSUM;
use crate::concurrent_skip_list::ConcurrentSkipList;
use crate::kv::{TombstonePair, TombstoneValueLike};
use crate::storage::blob_store::BlobStore;
use crate::wal::wal_entry::{WalEntry, WalEntryBlock, WalError};
use crate::wal::wal_writer::WalWriter;

// TODO(t/1374): Implement more sophisticated WAL configuration.
#[allow(dead_code)]
pub(crate) enum WalWrapperConfigPreset {
    AutoTune {
        max_flush_wait_millis: u64,
        max_buffered_entry_count: usize,
        max_buffered_byte_length: usize,
    },
    MultiThreadedSyncRequired {
        max_flush_wait_millis: u64,
        max_buffered_entry_count: usize,
        max_buffered_byte_length: usize,
    },
    MultiThreadedLazySyncRequired,
    MultiThreadedNoSyncRequired,
}

impl Default for WalWrapperConfigPreset {
    fn default() -> Self {
        WalWrapperConfigPreset::AutoTune {
            max_flush_wait_millis: 25,
            max_buffered_entry_count: 1024,
            max_buffered_byte_length: 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct WalWrapperConfig {
    next_wal_id: usize,
    max_concurrent_writes: Option<usize>,
    return_ok_before_flush: bool,
    max_wal_size_bytes: usize,
    max_flush_wait_millis: Option<u64>,
}

impl From<WalWrapperConfigPreset> for WalWrapperConfig {
    fn from(config: WalWrapperConfigPreset) -> Self {
        let max_concurrent_writes: Option<usize> = match config {
            WalWrapperConfigPreset::AutoTune {
                max_buffered_entry_count,
                ..
            } => Some(max_buffered_entry_count),
            WalWrapperConfigPreset::MultiThreadedSyncRequired {
                max_buffered_entry_count,
                ..
            } => Some(max_buffered_entry_count),
            WalWrapperConfigPreset::MultiThreadedLazySyncRequired => None,
            WalWrapperConfigPreset::MultiThreadedNoSyncRequired => None,
        };
        let return_ok_before_flush = match config {
            WalWrapperConfigPreset::AutoTune { .. } => true,
            WalWrapperConfigPreset::MultiThreadedSyncRequired { .. } => false,
            WalWrapperConfigPreset::MultiThreadedLazySyncRequired => false,
            WalWrapperConfigPreset::MultiThreadedNoSyncRequired => true,
        };
        let target_wal_size_bytes = 2 * 1024 * 1024 * 1024; // 2 GiB
        let max_flush_wait_millis: Option<u64> = match config {
            WalWrapperConfigPreset::AutoTune { .. } => None,
            WalWrapperConfigPreset::MultiThreadedSyncRequired {
                max_flush_wait_millis,
                ..
            } => Some(max_flush_wait_millis),
            WalWrapperConfigPreset::MultiThreadedLazySyncRequired => None,
            WalWrapperConfigPreset::MultiThreadedNoSyncRequired => None,
        };
        WalWrapperConfig {
            next_wal_id: 0,
            max_concurrent_writes,
            return_ok_before_flush,
            max_wal_size_bytes: target_wal_size_bytes,
            max_flush_wait_millis,
        }
    }
}

impl Default for WalWrapperConfig {
    fn default() -> Self {
        WalWrapperConfig::from(WalWrapperConfigPreset::default())
    }
}

impl WalWrapperConfig {
    pub(crate) fn with_next_wal_id(self, wal_id: usize) -> WalWrapperConfig {
        let mut config = self;
        config.next_wal_id = wal_id;
        config
    }

    pub(crate) fn with_max_concurrent_writers(self, max_concurrent_writers: usize) -> Self {
        let mut config = self;
        config.max_concurrent_writes = Some(max_concurrent_writers);
        config.return_ok_before_flush = false;
        config.max_flush_wait_millis = Some(0);
        config
    }

    pub(crate) fn with_max_wal_bytes(self, max_wal_bytes: usize) -> Self {
        let mut config = self;
        config.max_wal_size_bytes = max_wal_bytes;
        config
    }
}

pub(crate) struct RecoveryData {
    /// tuples of the blob_id for the un-compacted WAL and the updates in that
    /// WAL, listed from oldest to newest
    un_compacted_updates: Vec<(String, Vec<TombstonePair>)>,
    /// levels of the SST store containing the blob_ids in order by level
    /// from oldest to newest
    manifest: Vec<String>,
    /// next WAL id to use
    next_wal_id: usize,
    garbage_wal_blob_ids: HashSet<String>,
    garbage_sst_blob_ids: HashSet<String>,
}

impl RecoveryData {
    pub(crate) fn manifest_ref(&self) -> &[String] {
        &self.manifest
    }

    pub(crate) fn un_compacted_updates_ref(&self) -> &[(String, Vec<TombstonePair>)] {
        &self.un_compacted_updates
    }

    pub(crate) fn next_wal_id(&self) -> usize {
        self.next_wal_id
    }

    pub(crate) fn garbage_blob_ids(&self) -> impl Iterator<Item = &'_ String> {
        self.garbage_wal_blob_ids
            .iter()
            .chain(self.garbage_sst_blob_ids.iter())
    }
}

pub(crate) struct WalWrapper {
    blob_id: String,
    active_wal: WalWriter,
    config: WalWrapperConfig,
}

impl Clone for WalWrapper {
    fn clone(&self) -> Self {
        Self {
            blob_id: self.blob_id.clone(),
            active_wal: self.active_wal.clone(),
            config: self.config.clone(),
        }
    }
}

impl Debug for WalWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalWrapper")
            .field("blob_id", &self.blob_id)
            .field("active_wal", &self.active_wal)
            .field("config", &self.config)
            .finish()
    }
}

struct BlobRecoveryData {
    recovered_values: Vec<TombstonePair>,
    sst_blob_ids: Option<Vec<String>>,
    garbage_wal_blob_ids: HashSet<String>,
    garbage_sst_blob_ids: HashSet<String>,
}

impl WalWrapper {
    pub(crate) fn new<B: BlobStore>(
        blob_store_ref: &B,
        config: WalWrapperConfig,
    ) -> Result<WalWrapper, WalError>
    where
        B::WriteCursor: Send + Sync + 'static,
    {
        let blob_id = format!("wal.{}.log", config.next_wal_id);
        let blob_writer = match blob_store_ref.create_blob(&blob_id) {
            Ok(blob_writer) => blob_writer,
            Err(err) => {
                return Err(WalError::BlobStoreError(format!(
                    "could not create initial wal writer: {:?}",
                    err
                )));
            }
        };
        let active_wal = WalWriter::new(
            blob_writer,
            config.max_concurrent_writes,
            config.max_wal_size_bytes,
            config.return_ok_before_flush,
            config.max_flush_wait_millis,
        );
        Ok(WalWrapper {
            blob_id,
            active_wal,
            config,
        })
    }

    pub(crate) fn blob_id_ref(&self) -> &str {
        &self.blob_id
    }

    pub(crate) fn wal_config_ref(&self) -> &WalWrapperConfig {
        &self.config
    }

    pub(crate) fn recover<B: BlobStore>(
        blob_store: B,
        config: WalWrapperConfig,
    ) -> Result<(WalWrapper, RecoveryData), WalError>
    where
        B::WriteCursor: Send + Sync + 'static,
    {
        // list blobs for log files
        let blob_iter = match blob_store.blob_iter() {
            Ok(blob_iter) => blob_iter,
            Err(err) => {
                return Err(WalError::BlobStoreError(format!(
                    "could not list blobs: {:?}",
                    err
                )));
            }
        };
        let mut wal_blob_names: Vec<String> = blob_iter
            .into_iter()
            .filter(|name| {
                if name.len() < 9 {
                    return false;
                }
                let beginning = ['w', 'a', 'l', '.'];
                let ending = ['.', 'l', 'o', 'g'];
                let end_start = name.len() - 4;
                for (i, c) in name.chars().enumerate() {
                    if i < 4 {
                        if c != beginning[i] {
                            return false;
                        }
                        continue;
                    }
                    if i >= end_start {
                        if c != ending[i - end_start] {
                            return false;
                        }
                        continue;
                    }
                    if !c.is_numeric() {
                        return false;
                    }
                }
                true
            })
            .collect();
        fn name_to_int(s: &str) -> usize {
            let s2 = &s[4..s.len() - 4];
            s2.parse::<usize>().unwrap()
        }
        wal_blob_names.sort_by(|a, b| {
            let a = name_to_int(a);
            let b = name_to_int(b);
            a.cmp(&b)
        });
        let mut un_compacted_updates = Vec::with_capacity(wal_blob_names.len());
        let mut my_wal_id = 0;
        let mut garbage_wal_blob_ids = HashSet::new();
        let mut garbage_sst_blob_ids = HashSet::new();

        let mut sst_blob_ids: Option<Vec<String>> = None;
        for blob_id in &wal_blob_names {
            let wal_id = name_to_int(blob_id);
            if wal_id >= my_wal_id {
                my_wal_id = wal_id + 1;
            }
            let updates = WalWrapper::recover_from_blob(&blob_store, blob_id, sst_blob_ids)?;
            un_compacted_updates.push((blob_id.clone(), updates.recovered_values));
            un_compacted_updates
                .retain(|(my_blob_id, _)| !updates.garbage_wal_blob_ids.contains(my_blob_id));
            sst_blob_ids = updates.sst_blob_ids;
            garbage_wal_blob_ids.extend(updates.garbage_wal_blob_ids);
            garbage_sst_blob_ids.extend(updates.garbage_sst_blob_ids);
        }
        let recovery_data = RecoveryData {
            manifest: sst_blob_ids.unwrap_or_default(),
            un_compacted_updates,
            garbage_wal_blob_ids,
            garbage_sst_blob_ids,
            next_wal_id: my_wal_id + 1,
        };
        let config = config.with_next_wal_id(my_wal_id);
        let wrapper = WalWrapper::new(&blob_store, config)?;
        Ok((wrapper, recovery_data))
    }

    fn recover_from_blob<B: BlobStore>(
        blob_store: &B,
        blob_name: &str,
        start_sst_blob_ids: Option<Vec<String>>,
    ) -> Result<BlobRecoveryData, WalError> {
        let mut reader = blob_store.read_cursor(blob_name)?;
        let mut sst_blob_ids = start_sst_blob_ids;
        let mut garbage_wal_blob_ids = HashSet::new();
        let mut garbage_sst_blob_ids = HashSet::new();
        let skip_list: ConcurrentSkipList<Vec<u8>, TombstonePair> = ConcurrentSkipList::new();
        let mut checksum = INIT_WAL_CHECKSUM;

        fn replace_compacted_sst_blob_ids(
            current_blob_ids: Option<Vec<String>>,
            old_blob_ids: &[String],
            new_blob_id: String,
            compaction_type: &str,
        ) -> Result<Option<Vec<String>>, WalError> {
            let current_blob_ids = current_blob_ids.ok_or_else(|| {
                WalError::RecoveryError(format!(
                    "{} compaction entry found before snapshot could be constructed",
                    compaction_type
                ))
            })?;
            let mut new_blob_ids =
                Vec::with_capacity(current_blob_ids.len() + 1 - old_blob_ids.len());
            let mut pushed_new_sst = false;
            for blob_id in current_blob_ids.iter() {
                if !old_blob_ids.contains(blob_id) {
                    new_blob_ids.push(blob_id.clone());
                    continue;
                }
                if pushed_new_sst {
                    continue;
                }
                new_blob_ids.push(new_blob_id.clone());
                pushed_new_sst = true;
            }
            Ok(Some(new_blob_ids))
        }

        while let Ok((block, next_checksum)) = WalEntryBlock::recover(&mut reader, checksum) {
            for entry in block.entries_ref().iter() {
                match entry {
                    WalEntry::Set { key, value } => {
                        skip_list.set(key.to_vec(), TombstonePair::new(key.clone(), value.clone()));
                    }
                    WalEntry::Remove { key } => {
                        skip_list.set(key.to_vec(), TombstonePair::deletion_marker(key.clone()));
                    }
                    WalEntry::MinorCompaction {
                        wal_blob_id,
                        sst_blob_id,
                    } => {
                        garbage_wal_blob_ids.insert(wal_blob_id.clone());
                        if let Some(current_blob_ids) = sst_blob_ids.as_mut() {
                            current_blob_ids.push(sst_blob_id.clone());
                        } else {
                            sst_blob_ids = Some(vec![sst_blob_id.clone()]);
                        }
                    }
                    WalEntry::MergeCompaction {
                        start_sst_blob_ids,
                        end_sst_blob_id,
                    } => {
                        sst_blob_ids = replace_compacted_sst_blob_ids(
                            sst_blob_ids,
                            start_sst_blob_ids,
                            end_sst_blob_id.clone(),
                            "minor",
                        )?;
                        garbage_wal_blob_ids.extend(start_sst_blob_ids.iter().cloned());
                    }
                    WalEntry::MajorCompaction {
                        start_sst_blob_ids,
                        end_sst_blob_id,
                    } => {
                        sst_blob_ids = replace_compacted_sst_blob_ids(
                            sst_blob_ids,
                            start_sst_blob_ids,
                            end_sst_blob_id.clone(),
                            "major",
                        )?;
                        garbage_sst_blob_ids.extend(start_sst_blob_ids.iter().cloned());
                    }
                    WalEntry::ManifestSnapshot {
                        sst_blob_ids: my_blob_ids,
                    } => {
                        if let Some(current_blob_ids) = sst_blob_ids {
                            if current_blob_ids != *my_blob_ids {
                                return Err(WalError::RecoveryError(format!(
                                    "sst blob ids in manifest snapshot do not match previous \
                                     snapshot: {:?} vs {:?}",
                                    current_blob_ids, my_blob_ids,
                                )));
                            }
                        }
                        sst_blob_ids = Some(my_blob_ids.clone());
                    }
                }
            }
            checksum = next_checksum;
        }

        let pairs = skip_list
            .into_iter()
            .map(|view| {
                let value = view.value_ref();
                value.clone()
            })
            .collect();

        Ok(BlobRecoveryData {
            recovered_values: pairs,
            sst_blob_ids,
            garbage_wal_blob_ids,
            garbage_sst_blob_ids,
        })
    }

    fn write_entry(&self, entry: WalEntry) -> Result<(), WalError> {
        self.active_wal.write(entry)
    }

    async fn async_write_entry(&self, entry: WalEntry) -> Result<(), WalError> {
        self.active_wal.async_write(entry).await
    }

    pub(crate) fn set<T: TombstoneValueLike>(&self, key: &[u8], value: &T) -> Result<(), WalError> {
        if let Some(value) = value.as_ref() {
            return self.write_entry(WalEntry::Set {
                key: key.to_vec(),
                value: value.to_vec(),
            });
        }
        self.write_entry(WalEntry::Remove { key: key.to_vec() })
    }

    pub(crate) async fn async_set<T: TombstoneValueLike>(
        &self,
        key: &[u8],
        value: &T,
    ) -> Result<(), WalError> {
        if let Some(value) = value.as_ref() {
            return self
                .async_write_entry(WalEntry::Set {
                    key: key.to_vec(),
                    value: value.to_vec(),
                })
                .await;
        }
        self.async_write_entry(WalEntry::Remove { key: key.to_vec() })
            .await
    }

    pub(crate) fn mark_minor_compaction(
        &self,
        wal_blob_id: &str,
        sst_blob_id: &str,
    ) -> Result<(), WalError> {
        self.write_entry(WalEntry::MinorCompaction {
            wal_blob_id: wal_blob_id.to_string(),
            sst_blob_id: sst_blob_id.to_string(),
        })
    }

    pub(crate) fn close(self) -> Result<(), WalError> {
        self.active_wal.close()
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::*;
    use crate::kv::TombstoneValue;
    use crate::storage::blob_store::{BlobStore, InMemoryBlobStore, LocalBlobStore, ReadCursor};
    use crate::test_util::{set_up, tear_down};
    use crate::var_int::VarInt64;

    #[test]
    fn test_wal_wrapper_fs() {
        // test with real local storage
        let work_dir = set_up("test_wal_wrapper_real");
        let blob_store = LocalBlobStore::new(&work_dir).unwrap();
        run_test_wal_wrapper(blob_store);
        tear_down(&work_dir);
    }

    #[test]
    fn test_wal_wrapper_mem() {
        // test with in-memory fake storage
        let blob_store = InMemoryBlobStore::new();
        run_test_wal_wrapper(blob_store);
    }

    fn run_test_wal_wrapper<B: BlobStore + Clone + 'static>(blob_store: B)
    where
        B::WriteCursor: Send + Sync + 'static,
    {
        let wal_wrapper = WalWrapper::new(&blob_store, WalWrapperConfig::default()).unwrap();
        let top: u64 = 299;
        for k in 0..top {
            if k % 2 == 0 {
                let key = VarInt64::try_from(k).unwrap();
                let val = VarInt64::try_from(k * k).unwrap();
                assert!(wal_wrapper.set(key.data_ref(), &val.data_ref()).is_ok());
            }
        }
        assert!(wal_wrapper.close().is_ok());

        // Read WAL blob for expected value.
        let expected_wal_bytes: Vec<u8> = vec![
            150, 1, 0, 1, 0, 1, 0, 0, 1, 2, 1, 4, 0, 1, 4, 1, 16, 0, 1, 6, 1, 36, 0, 1, 8, 1,
        ];
        let mut found_bytes = vec![0_u8; expected_wal_bytes.len()];
        let mut reader = blob_store.read_cursor("wal.0.log").unwrap();
        reader.read_exact(&mut found_bytes).unwrap();
        assert_eq!(expected_wal_bytes, found_bytes);

        // Recover the WAL
        let (wal_wrapper, recovery_data) =
            WalWrapper::recover(blob_store.clone(), WalWrapperConfig::default()).unwrap();
        let mut update_map: HashMap<Vec<u8>, TombstoneValue> = HashMap::new();
        for pair in recovery_data.un_compacted_updates[0].1.iter() {
            let key = pair.key_ref();
            let value = pair.value_ref();
            let ki = VarInt64::try_from(key).unwrap().value();
            let vi = VarInt64::try_from(value.as_ref().unwrap().as_ref())
                .unwrap()
                .value();
            assert_eq!(ki % 2, 0);
            assert_eq!(ki * ki, vi);
            assert!(ki < top);
            update_map.insert(key.to_vec(), value.clone());
        }
        for k in 0..top {
            if k % 2 == 0 {
                let key = VarInt64::try_from(k).unwrap();
                let key = key.data_ref().to_vec();
                let value = VarInt64::try_from(k * k).unwrap();
                let value = value.data_ref().to_vec();
                let found_value = update_map.get(&key).unwrap();
                assert_eq!(*found_value, TombstoneValue::Value(value));
                wal_wrapper.set(&key, &None).unwrap();
            }
            if k % 2 == 1 {
                let key = VarInt64::try_from(k).unwrap();
                assert_eq!(
                    update_map.get(key.data_ref()),
                    None,
                    "unexpected value for k={}",
                    k,
                );
                let value = VarInt64::try_from(k + 1).unwrap();
                assert!(wal_wrapper.set(key.data_ref(), &value.data_ref()).is_ok());
            }
        }
        assert!(wal_wrapper.close().is_ok());

        let (wal_wrapper, recovery_data) =
            WalWrapper::recover(blob_store, WalWrapperConfig::default()).unwrap();
        let mut update_map = HashMap::new();
        for un_compacted_updates in &recovery_data.un_compacted_updates {
            for pair in &un_compacted_updates.1 {
                let key: Vec<u8> = pair.key_ref().to_vec();
                let value: Option<Vec<u8>> = pair.value_ref().as_ref().map(|v| v.to_vec());
                update_map.insert(key, value);
            }
        }
        for k in 1..top {
            if k % 2 == 0 {
                let key = VarInt64::try_from(k).unwrap();
                assert!(update_map.get(&key.data_ref().to_vec()).unwrap().is_none());
            }
            if k % 2 == 1 {
                let key = VarInt64::try_from(k).unwrap();
                let key = key.data_ref().to_vec();
                let value = VarInt64::try_from(k + 1).unwrap();
                let value = value.data_ref().to_vec();
                assert_eq!(update_map.get(&key), Some(&Some(value)));
            }
        }
        assert!(wal_wrapper.close().is_ok());
    }
}
