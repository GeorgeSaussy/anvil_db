use std::fmt::Debug;
use std::sync::Arc;

use super::wal_entry::WalRecoveryError;
use super::wal_writer::INIT_WAL_CHECKSUM;
use crate::checksum::crc32;
use crate::kv::{TombstonePair, TombstonePairLike, TombstoneValue, TombstoneValueLike};
use crate::manifest::{Manifest, ManifestUpdate, ManifestUpdateType};
use crate::storage::blob_store::{BlobId, BlobStore, ReadCursor, WriteCursor};
use crate::structs::skip_list::{SkipList, SkipListScanner};
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

#[derive(Default)]
pub(crate) struct WalRecoveryData {
    /// tuples of the blob_id for the un-compacted WAL and the updates in that
    /// WAL, listed from oldest to newest
    un_compacted_updates: Vec<(BlobId, Vec<TombstonePair>)>,
    /// next WAL id to use
    pub(crate) next_wal_idx: usize,
    pub(crate) manifest: Manifest,
}

impl WalRecoveryData {
    pub(crate) fn un_compacted_updates_ref(&self) -> &[(String, Vec<TombstonePair>)] {
        &self.un_compacted_updates
    }

    pub(crate) fn garbage_blob_ids(&self) -> impl Iterator<Item = &'_ String> {
        self.manifest
            .garbage_wal_ids
            .iter()
            .chain(self.manifest.garbage_sst_ids.iter())
    }
}

#[derive(Debug)]
pub(crate) struct WalWrapper<WC> {
    blob_id: String,
    active_wal: WalWriter<WC>,
    config: WalWrapperConfig,
}

impl<WC: WriteCursor> WalWrapper<WC> {
    pub(crate) fn new<B: BlobStore<WriteCursor = WC>>(
        blob_store_ref: &B,
        wal_idx: usize,
        config: WalWrapperConfig,
    ) -> Result<WalWrapper<WC>, WalError> {
        let blob_id = format!("wal.{wal_idx}.log");
        let mut blob_writer = match blob_store_ref.create_blob(&blob_id) {
            Ok(blob_writer) => blob_writer,
            Err(err) => {
                return Err(WalError::BlobStoreError(format!(
                    "could not create initial wal writer: {err:?}",
                )));
            }
        };
        blob_writer.write([0x47, 0x53, 0, 0].as_ref())?;
        let active_wal = WalWriter::new(blob_writer, config.max_wal_size_bytes);
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

    pub(crate) fn recover<B: BlobStore<WriteCursor = WC>>(
        blob_store: B,
        config: WalWrapperConfig,
    ) -> Result<(WalWrapper<WC>, WalRecoveryData), WalError> {
        // list blobs for log files
        let blob_iter = match blob_store.blob_iter() {
            Ok(blob_iter) => blob_iter,
            Err(err) => {
                return Err(WalError::BlobStoreError(format!(
                    "could not list blobs: {err:?}",
                )));
            }
        };
        let mut wal_blob_names: Vec<String> = blob_iter
            .into_iter()
            .filter(|name| {
                let beginning = "wal.";
                let ending = ".log";
                if !name.starts_with(beginning) || !name.ends_with(ending) {
                    return false;
                }
                if name[beginning.len()..name.len() - ending.len()]
                    .chars()
                    .any(|c| !c.is_numeric())
                {
                    return false;
                }
                true
            })
            .collect();

        wal_blob_names.sort_by(|a, b| {
            let a = Self::name_to_int(a);
            let b = Self::name_to_int(b);
            a.cmp(&b)
        });

        let mut recovery_data = WalRecoveryData::default();
        for blob_id in &wal_blob_names {
            WalWrapper::recover_from_wal_id(&blob_store, blob_id, &mut recovery_data)?;
        }
        let wrapper = WalWrapper::new(&blob_store, recovery_data.next_wal_idx, config)?;
        Ok((wrapper, recovery_data))
    }

    fn name_to_int(s: &str) -> usize {
        let s2 = &s[4..s.len() - 4];
        s2.parse::<usize>().unwrap()
    }

    fn recover_from_wal_id<B: BlobStore<WriteCursor = WC>>(
        blob_store: &B,
        blob_id: &str,
        dst: &mut WalRecoveryData,
    ) -> Result<(), WalError> {
        let wal_idx = Self::name_to_int(blob_id);
        dst.next_wal_idx = std::cmp::max(dst.next_wal_idx, wal_idx + 1);
        let mut reader = blob_store.read_cursor(blob_id)?;
        Self::recover_from_reader(&mut reader, blob_id, dst)?;
        Ok(())
    }

    fn recover_from_reader<R: ReadCursor>(
        reader: &mut R,
        blob_id: &str,
        dst: &mut WalRecoveryData,
    ) -> Result<(), WalRecoveryError> {
        let skip_list = SkipList::default();
        let expected_header = [0x47, 0x53, 0, 0];
        let mut checksum = crc32(INIT_WAL_CHECKSUM, &expected_header);

        let mut found_header = [0x00; 4];
        reader
            .read_exact(&mut found_header)
            .map_err(|err| WalRecoveryError::ReadExactFailed {
                blob_id: blob_id.to_string(),
                err,
            })?;
        if found_header != expected_header {
            return Err(WalRecoveryError::IncorrectHeader {
                blob_id: blob_id.to_string(),
                found: found_header,
                expected: expected_header,
            });
        }

        while let Ok((block, next_checksum)) = WalEntryBlock::recover(reader, blob_id, checksum) {
            for entry in block.entries_ref().iter() {
                match entry {
                    WalEntry::Set { key, value } => {
                        skip_list.insert(key, Some(value));
                    }
                    WalEntry::Remove { key } => {
                        skip_list.insert(key, None);
                    }
                    WalEntry::ManifestSnapshot(manifest) => {
                        if dst.manifest != *manifest {
                            return Err(WalRecoveryError::ManifestMismatch {
                                blob_id: blob_id.to_string(),
                                found: Box::new(manifest.clone()),
                                expected: Box::new(dst.manifest.clone()),
                            });
                        }
                    }
                    WalEntry::ManifestUpdate(update) => dst
                        .manifest
                        .apply(update)
                        .map_err(WalRecoveryError::BadManifestUpdate)?,
                }
            }
            checksum = next_checksum;
        }

        let scanner = SkipListScanner::from(Arc::new(skip_list));
        let pairs = scanner
            .map(|view| {
                let key = view.key_ref();
                let value = view.value_ref();

                TombstonePair::from((key.to_vec(), TombstoneValue::from(&value)))
            })
            .collect();
        dst.un_compacted_updates.push((blob_id.to_string(), pairs));

        Ok(())
    }

    fn write_entry(&mut self, entry: WalEntry) -> Result<bool, WalError> {
        self.active_wal.write(entry)
    }

    /// Returns true if the WAL is full and a rotation is required.
    pub(crate) fn set<T: TombstoneValueLike>(
        &mut self,
        key: &[u8],
        value: &T,
    ) -> Result<bool, WalError> {
        self.write_entry(WalEntry::from((key, value)))
    }

    // TODO(t/1581): Implement better async API.
    // TODO(t/1581): Async writes will never trigger WAL rotation.
    async fn async_write_entry(&mut self, entry: WalEntry) -> Result<(), WalError> {
        self.write_entry(entry)?;
        Ok(())
    }

    pub(crate) async fn async_set<T: TombstoneValueLike>(
        &mut self,
        key: &[u8],
        value: &T,
    ) -> Result<(), WalError> {
        self.async_write_entry(WalEntry::from((key, value))).await
    }

    pub(crate) fn record_index_change(&mut self, update: &ManifestUpdate) -> Result<(), WalError> {
        self.write_entry(WalEntry::ManifestUpdate(update.clone()))?;
        Ok(())
    }

    pub(crate) fn recovery_mark_minor_compaction(
        &mut self,
        wal_blob_id: &str,
        sst_blob_ids: &[String],
    ) -> Result<(), WalError> {
        self.write_entry(WalEntry::ManifestUpdate(ManifestUpdate {
            update_type: ManifestUpdateType::Minor,
            garbage_collected_blob_ids: Vec::new(),
            compacted_wal_ids: vec![wal_blob_id.to_string()],
            compacted_minor_sst_ids: Vec::new(),
            new_minor_sst_ids: vec![sst_blob_ids.to_vec()],
            level_sst_updates: Vec::new(),
        }))?;
        Ok(())
    }

    pub(crate) fn flush(&mut self) -> Result<(), WalError> {
        self.active_wal.close()
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::*;
    use crate::kv::TombstoneValue;
    use crate::storage::blob_store::{InMemoryBlobStore, LocalBlobStore, ReadCursor};
    use crate::test_util::{set_up, tear_down};
    use crate::var_int::VarInt64;

    #[test]
    fn test_wal_wrapper_fs() {
        // test with real local storage
        if std::env::var("ANVIL_TEST_QUICK").is_ok() {
            // skip this test if running `make test`
            return;
        }
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
        let mut wal_wrapper = WalWrapper::new(&blob_store, 0, WalWrapperConfig::default()).unwrap();
        let top: u64 = 299;
        for k in 0..top {
            if k % 2 == 0 {
                let key = VarInt64::try_from(k).unwrap();
                let val = VarInt64::try_from(k * k).unwrap();
                assert!(wal_wrapper.set(key.data_ref(), &val.data_ref()).is_ok());
            }
        }
        assert!(wal_wrapper.flush().is_ok());

        // Read WAL blob for expected value.
        let expected_wal_bytes: Vec<u8> = vec![
            0x47, 0x53, // magic number
            0x00, 0x00, // version number
            0x01, // number of entries in the first block, 1 as a VarInt64
            0x00, // the type of the first entry, set
            0x01, // the length of the first key, 1 as a VarInt64
            0x00, // the first key, 0 as a VarInt64
            0x01, // the length of the first value, 1 as a VarInt64
            0x00, // the first value, 0 as a VarInt64
            0x20, 0x60, 0xb9,
            0x60, // crc32 of the file so far, 1622761504 as little-endian u32
        ];
        let mut found_bytes = vec![0_u8; expected_wal_bytes.len()];
        let mut reader = blob_store.read_cursor("wal.0.log").unwrap();
        reader.read_exact(&mut found_bytes).unwrap();
        //assert_eq!(expected_wal_bytes, found_bytes);

        // Recover the WAL
        let (mut wal_wrapper, recovery_data) =
            WalWrapper::recover(blob_store.clone(), WalWrapperConfig::default()).unwrap();
        let mut update_map: HashMap<Vec<u8>, TombstoneValue> = HashMap::new();
        for pair in recovery_data.un_compacted_updates[0].1.iter() {
            let key = pair.key_ref();
            let value = pair.value_ref();
            let ki = VarInt64::try_from(key).unwrap().value();
            let vi = VarInt64::try_from(value.vec_ref().unwrap().as_ref())
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
                    "unexpected value for k={k}",
                );
                let value = VarInt64::try_from(k + 1).unwrap();
                assert!(wal_wrapper.set(key.data_ref(), &value.data_ref()).is_ok());
            }
        }
        assert!(wal_wrapper.flush().is_ok());

        let (mut wal_wrapper, recovery_data) =
            WalWrapper::recover(blob_store, WalWrapperConfig::default()).unwrap();
        let mut update_map = HashMap::new();
        for un_compacted_updates in &recovery_data.un_compacted_updates {
            for pair in &un_compacted_updates.1 {
                let key: Vec<u8> = pair.key_ref().to_vec();
                let value: Option<Vec<u8>> = pair.value_ref().vec_ref().map(|v| v.to_vec());
                update_map.insert(key, value);
            }
        }
        for k in 1..top {
            if k % 2 == 0 {
                let key = VarInt64::try_from(k).unwrap();
                assert!(update_map.get(key.data_ref()).unwrap().is_none());
            }
            if k % 2 == 1 {
                let key = VarInt64::try_from(k).unwrap();
                let key = key.data_ref().to_vec();
                let value = VarInt64::try_from(k + 1).unwrap();
                let value = value.data_ref().to_vec();
                assert_eq!(update_map.get(&key), Some(&Some(value)));
            }
        }
        assert!(wal_wrapper.flush().is_ok());
    }
}
