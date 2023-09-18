use std::sync::mpsc::{channel, Sender};
use std::thread::spawn;

use crate::compactor::{Compactor, CompactorError, CompactorMessage};
use crate::kv::{TombstonePointReader, TombstoneStore, TombstoneValue};
use crate::logging::{DefaultLogger, Logger};
use crate::mem_table::MemTable;
use crate::sst::block_cache::{BlockCache, StorageWrapper};
use crate::storage::blob_store::BlobStore;
use crate::tablet::TabletSstStore;
use crate::wal::wal_wrapper::WalWrapperConfig;

/// The in memory representation of a AnvilDB database.
#[derive(Debug, Clone)]
pub(crate) struct AnvilDb<BC: StorageWrapper, T: TabletSstStore, L: Logger> {
    /// The in-memory part of the LSM tree
    /// The mem_table implementation
    /// is already thread safe, so we do not want to hold
    /// a this lock while manipulating it.
    mem_table: MemTable<BC::B, L>,
    /// the on-disk part of the LSM tree
    sst_store: T,
    /// the compactor object
    compactor_tx: Sender<CompactorMessage>,
}

impl<BC: StorageWrapper, T: TabletSstStore<BC = BC>> AnvilDb<BC, T, DefaultLogger>
where
    BC: Send + 'static,
    T: Send + 'static,
    <<BC as StorageWrapper>::B as BlobStore>::WC: Send + Sync + 'static,
    String: From<<T as TombstonePointReader>::E>,
{
    /// Create a new database instance.
    ///
    /// # Arguments
    ///
    /// - dirname: the directory containing the database files
    ///
    /// # Returns
    ///
    /// A new database instance, or an error String on error.
    pub(crate) fn new(store: BC::B) -> Result<AnvilDb<BC, T, DefaultLogger>, String> {
        let config = AnvilDbConfig::default();
        Self::with_config(store, config)
    }
}

impl<BC: StorageWrapper, T: TabletSstStore<BC = BC>, L: Logger> AnvilDb<BC, T, L>
where
    BC: Send + 'static,
    T: Send + 'static,
    <<BC as StorageWrapper>::B as BlobStore>::WC: Send + Sync + 'static,
    String: From<<T as TombstonePointReader>::E>,
{
    pub(crate) fn with_config<L1: Logger>(
        store: BC::B,
        config: AnvilDbConfig<L1>,
    ) -> Result<AnvilDb<BC, T, L1>, String>
    where
        <<BC as StorageWrapper>::B as BlobStore>::WC: Send + Sync + 'static,
    {
        let block_cache = <BC as StorageWrapper>::C::with_capacity(config.block_cache_bytes);
        let storage_wrapper = BC::from((store.clone(), block_cache));

        let (compactor_tx, compactor_rx) = channel::<CompactorMessage>();
        let mem_table = MemTable::new(
            store,
            compactor_tx.clone(),
            config.wal_config,
            config.logger,
        )?;

        let levels: Vec<String> = Vec::new();
        let sst_store = T::recover(
            storage_wrapper.clone(),
            levels.into_iter(),
            compactor_tx.clone(),
        )?;

        let compactor = Compactor::new(
            storage_wrapper,
            sst_store.clone(),
            compactor_tx.clone(),
            compactor_rx,
        );
        spawn(move || compactor.run());

        Ok(AnvilDb {
            mem_table,
            sst_store,
            compactor_tx,
        })
    }

    /// Get an element from the database.
    /// # Arguments
    /// - key: the key to retrieve
    /// # Returns
    /// A Cord if found, or an error string.
    pub(crate) fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let found = self.mem_table.get(key)?;
        if let Some(value) = found {
            return Ok(value.into());
        }
        let found = self.sst_store.get(key)?;
        if let Some(value) = found {
            return Ok(value.into());
        }
        Ok(None)
    }

    /// Set a key-value pair for the database.
    /// # Arguments
    /// - key: the key to set
    /// - value: the value to set
    /// # Returns
    /// An error String on error.
    pub(crate) fn set(&self, key: &[u8], value: &[u8]) -> Result<(), String> {
        self.mem_table.set(key, &value)?;
        Ok(())
    }

    /// Remove a key from the database.
    ///
    /// # Arguments
    ///
    /// - key: the key to remove
    ///
    /// # Returns
    ///
    /// An error String on error.
    pub(crate) fn remove(&self, key: &[u8]) -> Result<(), String> {
        self.mem_table.set(key, &TombstoneValue::Tombstone)?;
        Ok(())
    }

    pub(crate) fn compact(&self) -> Result<(), String> {
        // TODO(t/1375): Implement compaction.
        self.mem_table.rotate()?;
        let (done_tx, done_rx) = channel::<Result<(), CompactorError>>();
        self.compactor_tx
            .send(CompactorMessage::MajorCompaction { done_tx })
            .unwrap();
        done_rx.recv().unwrap()?;
        Ok(())
    }

    /// Recover the database.
    ///
    /// # Arguments
    ///
    /// - dir_name: the directory containing the database files
    ///
    /// # Returns
    ///
    /// A new database instance, or an error String on error.
    pub(crate) fn recover(
        storage: BC::B,
        config: AnvilDbConfig<L>,
    ) -> Result<AnvilDb<BC, T, L>, String> {
        // TODO(t/1387): Cache size should be configurable when it is
        // implemented.
        let storage_wrapper = BC::from((storage, BC::C::with_capacity(0)));

        let (mem_table, sst_store, compactor, compactor_tx) =
            MemTable::recover(storage_wrapper.clone(), config.wal_config, config.logger)?;
        spawn(move || compactor.run());

        Ok(AnvilDb {
            mem_table,
            sst_store,
            compactor_tx,
        })
    }

    /// Close a database.
    ///
    /// # Returns
    ///
    /// An error String on error.
    pub(crate) fn close(self) -> Result<(), String>
    where
        <<BC as StorageWrapper>::B as BlobStore>::WC: Send + Sync + 'static,
    {
        self.compactor_tx
            .send(CompactorMessage::Shutdown)
            .map_err(|err| format!("failed to send shutdown message: {:?}", err))?;
        self.mem_table.close()?;
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct AnvilDbScanner {}

impl AnvilDbScanner {
    pub(crate) fn from(self, _key: &[u8]) -> Result<Self, String> {
        // TODO(t/1373): Implement scans.
        todo!()
    }

    pub(crate) fn to(self, _key: &[u8]) -> Result<Self, String> {
        // TODO(t/1373): Implement scans.
        todo!()
    }
}

pub(crate) struct AnvilDbConfig<L> {
    block_cache_bytes: usize,
    wal_config: WalWrapperConfig,
    logger: L,
}

impl Default for AnvilDbConfig<DefaultLogger> {
    fn default() -> Self {
        // TODO(t/1387): The default memory size should be based on how
        // much memory the system has to spare.
        let half_gb = 1024 * 1024 * 1024 / 2;
        Self {
            block_cache_bytes: half_gb,
            wal_config: WalWrapperConfig::default(),
            logger: DefaultLogger::default(),
        }
    }
}

impl<L: Logger> AnvilDbConfig<L> {
    pub(crate) fn with_max_concurrent_writers(self, max_concurrent_writers: usize) -> Self {
        let wal_config = self
            .wal_config
            .with_max_concurrent_writers(max_concurrent_writers);
        let block_cache_bytes = self.block_cache_bytes;
        let logger = self.logger;
        AnvilDbConfig {
            block_cache_bytes,
            wal_config,
            logger,
        }
    }

    pub(crate) fn with_max_wal_bytes(self, max_wal_bytes: usize) -> Self {
        let wal_config = self.wal_config.with_max_wal_bytes(max_wal_bytes);
        let block_cache_bytes = self.block_cache_bytes;
        let logger = self.logger;
        AnvilDbConfig {
            block_cache_bytes,
            wal_config,
            logger,
        }
    }
}

#[cfg(test)]
mod test {
    use std::cmp::Ordering;
    use std::thread::sleep;
    use std::time::Duration;
    use std::time::Instant;

    use super::*;
    use crate::logging::debug;
    use crate::sst::block_cache::NoBlockCache;
    use crate::sst::block_cache::SimpleStorageWrapper;
    use crate::storage::blob_store::InMemoryBlobStore;
    use crate::tablet::SmartTablet;
    use crate::var_int::VarInt64;

    #[test]
    fn test_anvil_db_end_to_end() {
        let store = InMemoryBlobStore::new();

        // TODO(t/1387): This should use the real block cache when it is
        // implemented.
        type TestOnlyStorageWrapper = SimpleStorageWrapper<InMemoryBlobStore, NoBlockCache>;
        let jdb: AnvilDb<
            TestOnlyStorageWrapper,
            SmartTablet<TestOnlyStorageWrapper>,
            DefaultLogger,
        > = AnvilDb::<
            TestOnlyStorageWrapper,
            SmartTablet<TestOnlyStorageWrapper>,
            DefaultLogger,
        >::new(store)
        .unwrap();
        // This test is too slow when top is too big.
        let top: u64 = 100;
        for k in 0..top {
            if k % 2 == 0 {
                let key = VarInt64::try_from(k).unwrap();
                let val = VarInt64::try_from(k * k).unwrap();
                let is_none = jdb.get(key.data_ref()).unwrap().is_none();
                assert!(is_none);
                let result = jdb.set(key.data_ref(), val.data_ref());
                assert!(result.is_ok());
                let getter = jdb.get(key.data_ref()).unwrap();
                assert!(getter.is_some());
                let val_in = val.data_ref().to_vec();
                assert!(getter.unwrap().cmp(&val_in) == Ordering::Equal);
            }
        }
        for k in 1..top {
            if k % 2 == 1 {
                let key = VarInt64::try_from(k).unwrap();
                let value = VarInt64::try_from(k + 1).unwrap();
                assert!(jdb.get(key.data_ref()).unwrap().is_none());
                let result = jdb.set(key.data_ref(), value.data_ref());
                assert!(result.is_ok());
                let option = jdb.get(key.data_ref()).unwrap();
                assert!(option.is_some());
                let unwrap = option.unwrap();
                let val_in = value.data_ref().to_vec();
                assert!(unwrap.cmp(&val_in) == Ordering::Equal);
            }
        }
        for k in top / 2..top {
            let key = VarInt64::try_from(k).unwrap();
            let mut value = VarInt64::try_from(k * k).unwrap();
            if k % 2 == 1 {
                value = VarInt64::try_from(k + 1).unwrap();
            }
            let val = jdb.get(key.data_ref()).unwrap();
            let is_some = val.is_some();
            assert!(is_some);
            let val_in = value.data_ref().to_vec();
            assert!(val.unwrap().cmp(&val_in) == Ordering::Equal);
            value = VarInt64::try_from(2 * value.value()).unwrap();
            let result = jdb.set(key.data_ref(), value.data_ref());
            assert!(result.is_ok());
            let val = jdb.get(key.data_ref()).unwrap();
            let is_some = val.is_some();
            assert!(is_some);
            let val_in = value.data_ref().to_vec();
            assert!(val.unwrap().cmp(&val_in) == Ordering::Equal);
        }
        for k in 0..top {
            let key = VarInt64::try_from(k).unwrap();
            let is_some = jdb.get(key.data_ref()).unwrap().is_some();
            assert!(is_some);
            let result = jdb.remove(key.data_ref());
            assert!(result.is_ok());
            let is_none = jdb.get(key.data_ref()).unwrap().is_none();
            assert!(is_none);
        }
        assert!(jdb.close().is_ok());
    }

    fn next_rng(state: &mut u64) -> u64 {
        let mut x = *state;
        if x == 0_u64 {
            x = 1;
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *state = x;
        x
    }

    // TODO(t/1387): This should use the real block cache when it is
    // implemented.
    type TestOnlyStorageWrapper = SimpleStorageWrapper<InMemoryBlobStore, NoBlockCache>;
    type TestOnlyAnvilDb =
        AnvilDb<TestOnlyStorageWrapper, SmartTablet<TestOnlyStorageWrapper>, DefaultLogger>;
    fn db_with_max_wal_bytes(max_wal_bytes: usize) -> (InMemoryBlobStore, TestOnlyAnvilDb) {
        let store = InMemoryBlobStore::new();
        let db: TestOnlyAnvilDb = AnvilDb::<_, _, DefaultLogger>::with_config(
            store.clone(),
            AnvilDbConfig::default()
                .with_max_concurrent_writers(1)
                .with_max_wal_bytes(max_wal_bytes),
        )
        .unwrap();
        (store, db)
    }

    fn write_and_compact() -> (InMemoryBlobStore, TestOnlyAnvilDb) {
        let top = 100_000_usize;
        let max_wal_bytes = 8 * 1024 * 1024;
        let (store, db) = db_with_max_wal_bytes(max_wal_bytes);

        let n_series = 1_009;
        let mut rng = 1337_u64;
        let series: Vec<u32> = (0..n_series)
            .map(|_| next_rng(&mut rng) as u32)
            .collect::<Vec<_>>();

        let mut timestamp = 0;
        let mut sequencers = vec![0u32; n_series];
        let start = Instant::now();
        for _batch in 0..top {
            timestamp += 1;
            let series_id = (next_rng(&mut rng) as usize) % n_series;
            sequencers[series_id] += 1;
            let key: Vec<_> = vec![series[series_id], timestamp, sequencers[series_id]]
                .into_iter()
                .flat_map(|x| x.to_be_bytes())
                .collect();
            let value = (0..8)
                .flat_map(|_| next_rng(&mut rng).to_be_bytes())
                .collect::<Vec<_>>();
            db.set(&key, &value).unwrap();
        }
        let duration = start.elapsed();
        debug!(
            "Writing {} points took {:.2} sec ({:.2} ms per point)",
            top,
            duration.as_secs_f64(),
            duration.as_micros() as f64 / top as f64
        );

        (store, db)
    }

    #[test]
    fn test_user_requested_compactions() {
        let (_, db) = write_and_compact();
        let start = Instant::now();
        db.compact().unwrap();
        let duration = start.elapsed();
        debug!("Took {:.2} sec to compact.", duration.as_secs_f64());
        db.close().unwrap();
    }

    #[test]
    fn test_natural_wal_rotation_1() {
        let (store, db) = write_and_compact();
        let start = Instant::now();
        loop {
            sleep(Duration::from_millis(1000));
            let blob_names: Vec<_> = store.blob_iter().unwrap().collect();
            debug!(
                "{} existing files:\n{}",
                blob_names.len(),
                blob_names.join("\n")
            );
            let target_sst_suffix = ".base.sst";
            let (wal_count, target_sst_count, other_count) =
                blob_names.iter().fold((0, 0, 0), |(wc, bc, oc), name| {
                    if name.starts_with("wal.") {
                        (wc + 1, bc, oc)
                    } else if name.ends_with(target_sst_suffix) {
                        (wc, bc + 1, oc)
                    } else {
                        (wc, bc, oc + 1)
                    }
                });
            debug!(
                "wal_count: {} ;  target_sst_count: {} ; other_count: {}",
                wal_count, target_sst_count, other_count
            );
            assert!(wal_count > 0);
            if wal_count == 1 && target_sst_count == 1 && other_count == 0 {
                break;
            }
            if start.elapsed().as_secs() > 10 {
                panic!("garbage collection took too long");
            }
        }
        db.close().unwrap();
    }

    #[test]
    fn test_natural_wal_rotation_2() {
        let top: u64 = 1024;
        let max_wal_bytes = 1024;

        let (store, db) = db_with_max_wal_bytes(max_wal_bytes);

        fn buf(idx: u64) -> Vec<u8> {
            let mut buf = Vec::with_capacity(16);
            buf.extend_from_slice(&idx.to_be_bytes());
            buf.extend_from_slice(&(idx + 1).to_be_bytes());
            buf
        }

        for idx in 0..top {
            let key = buf(idx);
            let value = buf(idx + 2);
            db.set(&key, &value).unwrap();
            if idx % (top / 8) == 0 {
                sleep(Duration::from_millis(100));
                let found = store.blob_iter().unwrap().fold(false, |found, blob_name| {
                    if blob_name.ends_with(".sst") {
                        return true;
                    }
                    found
                });
                if found {
                    break;
                }
            }
            assert_ne!(idx, top - 1);
        }

        db.close().unwrap();
    }
}
