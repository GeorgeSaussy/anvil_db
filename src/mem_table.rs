use std::fmt::{Debug, Formatter};
use std::sync::mpsc::{channel, Sender};
use std::thread::spawn;

use crate::compactor::{Compactor, CompactorError, CompactorMessage};
use crate::concurrent_skip_list::{ConcurrentSkipListPairView, ConcurrentSkipListScanner};
use crate::kv::{
    MergedHomogenousIter, TombstonePair, TombstonePairLike, TombstonePointReader, TombstoneScanner,
    TombstoneStore, TombstoneValueLike,
};
use crate::logging::{error, info, Logger};
use crate::mem_queue::{MemQueue, MemTableEntryIterator};
use crate::sst::block_cache::StorageWrapper;
use crate::sst::common::SstError;
use crate::tablet::TabletSstStore;
use crate::wal::wal_entry::WalError;
use crate::wal::wal_wrapper::{WalWrapper, WalWrapperConfig};
use crate::{kv::TombstoneValue, storage::blob_store::BlobStore};

#[derive(Debug)]
pub(crate) enum MemTableError {
    Compactor(CompactorError),
    Sst(SstError),
    Wal(WalError),
}

impl From<WalError> for MemTableError {
    fn from(err: WalError) -> Self {
        MemTableError::Wal(err)
    }
}

impl From<SstError> for MemTableError {
    fn from(err: SstError) -> Self {
        MemTableError::Sst(err)
    }
}

impl From<CompactorError> for MemTableError {
    fn from(err: CompactorError) -> Self {
        MemTableError::Compactor(err)
    }
}

impl From<MemTableError> for String {
    fn from(value: MemTableError) -> Self {
        format!("MemTableError: {:?}", value)
    }
}

/// A struct to encapsulate the database's mem_table.
/// The mem_table is thread safe, so references to the
/// mem_table can be Arc<MemTable> instead of
/// Arc<RwLock<MemTable>>
#[derive(Clone, Debug)]
pub(crate) struct MemTable<B: BlobStore, L: Logger> {
    mem_queue: MemQueue<B, L>,
    compactor_tx: Sender<CompactorMessage>,
    logger: L,
}

type RecoveryData<B, BC, T, L> = (
    MemTable<B, L>,
    T,
    Compactor<BC, T>,
    Sender<CompactorMessage>,
);
impl<B: BlobStore, L: Logger> MemTable<B, L>
where
    B::WC: Send + Sync + 'static,
{
    /// Create a new `MemTable` instance.
    ///
    /// # Arguments
    ///
    /// - storage: the backing storage implementation
    /// - dirname: the directory that contains the database data
    ///
    /// # Returns
    ///
    /// A new MemTable, or an error String.
    pub(crate) fn new(
        storage: B,
        compactor_tx: Sender<CompactorMessage>,
        wal_config: WalWrapperConfig,
        logger: L,
    ) -> Result<MemTable<B, L>, MemTableError> {
        Ok(MemTable {
            mem_queue: MemQueue::new(storage, wal_config, logger.clone())?,
            compactor_tx,
            logger,
        })
    }

    /// Recover a MemTable instance.
    /// We do not need to check for disk corruption here, since the
    /// the WalReader implementation will take care of it.
    ///
    /// # Arguments
    ///
    /// - storage: the backing storage client
    /// - dirname: the directory that contains the database data
    ///
    /// # Returns
    ///
    /// A new MemTable, or an error String.
    pub(crate) fn recover<BC: StorageWrapper<B = B>, T: TabletSstStore<BC = BC>>(
        storage_wrapper: BC,
        config: WalWrapperConfig,
        logger: L,
    ) -> Result<RecoveryData<B, BC, T, L>, MemTableError> {
        let recovery = WalWrapper::recover(storage_wrapper.blob_store_ref().clone(), config)?;
        let (wal_wrapper, recovery_data) = recovery;

        let (compactor_tx, compactor_rx) = channel();
        let mem_queue = MemQueue::from_recovery(
            storage_wrapper.blob_store_ref().clone(),
            wal_wrapper,
            recovery_data.next_wal_id(),
            logger.clone(),
        );

        let manifest_levels: Vec<String> = recovery_data
            .manifest_ref()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let sst_store = T::recover(
            storage_wrapper.clone(),
            manifest_levels.iter(),
            compactor_tx.clone(),
        )?;

        let mut compactor = Compactor::new(
            storage_wrapper,
            sst_store.clone(),
            compactor_tx.clone(),
            compactor_rx,
        );
        for x in recovery_data.un_compacted_updates_ref() {
            let sst_blob_id = compactor.minor_compact(x.1.clone().into_iter())?;
            mem_queue.mark_minor_compaction(&x.0, &sst_blob_id)?;
        }

        // TODO(t/1336): Here we send garbage collection to the newly created compactor
        // thread. This would be a good candidate for spawning a new async
        // thread to run garbage collection since it should not really depend on
        // a compactor thread being alive to run, and it also should not block
        // the rest of the recovery process.
        let garbage_blob_ids: Vec<_> = recovery_data
            .garbage_blob_ids()
            .map(|x| x.to_string())
            .collect();
        compactor_tx
            .send(CompactorMessage::GarbageCollect {
                blob_ids: garbage_blob_ids,
                done_tx: None,
            })
            .unwrap();

        let mem_table = MemTable {
            mem_queue,
            compactor_tx: compactor_tx.clone(),
            logger,
        };

        Ok((mem_table, sst_store, compactor, compactor_tx))
    }

    pub(crate) fn rotate(&self) -> Result<(), MemTableError> {
        let (old_wal_blob_id, arc, iter) = self.mem_queue.rotate()?;
        let (done_tx, done_rx) = channel();
        self.compactor_tx
            .send(CompactorMessage::MinorCompact {
                iter: MemTableCompactorIterator { inner: iter },
                done_tx,
            })
            .unwrap();
        match done_rx.recv().unwrap() {
            Ok(new_blob_id) => {
                self.mem_queue
                    .mark_minor_compaction(&old_wal_blob_id, &new_blob_id)?;
            }
            Err(err) => {
                if err != CompactorError::NothingToCompact {
                    error!(&self.logger, "error compacting: {:?}", err);
                    return Err(err.into());
                }
                info!(
                    &self.logger,
                    "cleaning up unused wal blob {}", old_wal_blob_id
                );
            }
        }
        let (done_tx, done_rx) = channel();
        let done_tx = Some(done_tx);
        self.mem_queue.maybe_drop_arc(arc);
        self.compactor_tx
            .send(CompactorMessage::GarbageCollect {
                blob_ids: vec![old_wal_blob_id],
                done_tx,
            })
            .unwrap();
        done_rx.recv().unwrap()?;
        Ok(())
    }

    /// Close the MemTable cleanly.
    ///
    /// # Returns
    ///
    /// An error if the MemTable could not be closed cleanly.
    pub(crate) fn close(self) -> Result<(), MemTableError> {
        self.mem_queue.close()
    }
}

pub(crate) struct MemTableCompactorIteratorPair {
    view: ConcurrentSkipListPairView<Vec<u8>, TombstoneValue>,
}

impl TombstonePairLike for MemTableCompactorIteratorPair {
    fn key_ref(&self) -> &[u8] {
        self.view.key_ref()
    }

    fn value_ref(&self) -> &TombstoneValue {
        self.view.value_ref()
    }
}

pub(crate) struct MemTableCompactorIterator {
    inner: ConcurrentSkipListScanner<Vec<u8>, TombstoneValue>,
}

impl Debug for MemTableCompactorIterator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemTableCompactorIterator").finish()
    }
}

impl Iterator for MemTableCompactorIterator {
    type Item = MemTableCompactorIteratorPair;

    fn next(&mut self) -> Option<Self::Item> {
        let view = self.inner.next()?;
        Some(MemTableCompactorIteratorPair { view })
    }
}

impl<B: BlobStore, L: Logger> TombstonePointReader for MemTable<B, L>
where
    B::WC: Send + Sync + 'static,
{
    type E = MemTableError;

    fn get(&self, key: &[u8]) -> Result<Option<TombstoneValue>, Self::E> {
        Ok(self.mem_queue.get(key))
    }
}

pub(crate) struct MemTableIterator {}

impl Iterator for MemTableIterator {
    type Item = Result<TombstonePair, MemTableError>;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO(t/1373): Implement scans.
        todo!()
    }
}

impl<B: BlobStore, L: Logger> TombstoneScanner for MemTable<B, L>
where
    B::WC: Send + Sync + 'static,
{
    type E = MemTableError;
    type I = MergedHomogenousIter<MemTableEntryIterator>;

    fn scan(&self) -> Self::I {
        self.mem_queue.iter()
    }
}

impl<B: BlobStore, L: Logger> TombstoneStore for MemTable<B, L>
where
    <B as BlobStore>::WC: Send + Sync + 'static,
{
    type E = MemTableError;

    fn set<T: TombstoneValueLike>(
        &self,
        key: &[u8],
        value: &T,
    ) -> Result<(), <Self as TombstoneStore>::E> {
        loop {
            if let Err(err) = self.mem_queue.set(key, value) {
                match err {
                    // the WAL could be rotating, retry
                    WalError::Closed => continue,
                    // the WAL is full, rotate in the background and return ok
                    WalError::RotationRequired => {
                        let my_self = self.clone();
                        // TODO(t/1395): This should be handled by the ECS-like system.
                        spawn(move || {
                            info!(&my_self.logger, "rotating wal in the background");
                            if let Err(err) = my_self.rotate() {
                                error!(&my_self.logger, "error rotating wal: {:?}", err);
                            }
                        });
                        return Ok(());
                    }
                    // otherwise return the error
                    _ => return Err(err.into()),
                }
            }
            break;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::cmp::Ordering;
    use std::sync::mpsc::channel;
    use std::thread::spawn;

    use super::*;
    use crate::compactor::CompactorMessage;
    use crate::kv::TryTombstoneScanner;
    use crate::logging::DefaultLogger;
    use crate::sst::block_cache::{BlockCache, NoBlockCache, SimpleStorageWrapper};
    use crate::storage::blob_store::InMemoryBlobStore;
    use crate::tablet::SmartTablet;
    use crate::var_int::VarInt64;

    #[test]
    fn test_mem_table_new() {
        let storage = InMemoryBlobStore::new();
        let (compactor_tx, _) = channel();
        let logger = DefaultLogger::default();

        // Just check it does not fail.
        MemTable::new(storage, compactor_tx, WalWrapperConfig::default(), logger).unwrap();
    }

    #[test]
    fn test_mem_table_close() {
        let storage = InMemoryBlobStore::new();
        let (compactor_tx, _) = channel();
        let logger = DefaultLogger::default();

        let result = MemTable::new(storage, compactor_tx, WalWrapperConfig::default(), logger);
        let mem_table = result.unwrap();
        assert!(mem_table.close().is_ok());
    }

    #[test]
    fn test_mem_table_end_to_end_1() {
        let top = 100;
        let storage = InMemoryBlobStore::new();
        let (compactor_tx, _) = channel();
        let logger = DefaultLogger::default();

        // OPEN A NEW DB
        let result = MemTable::new(storage, compactor_tx, WalWrapperConfig::default(), logger);
        assert!(result.is_ok());
        let mc = result.ok().unwrap();

        // CREATE SOME DATA
        let mut keys = Vec::new();
        let mut values1 = Vec::new();
        let mut values2 = Vec::new();
        let mut values3 = Vec::new();
        for k in 0..top {
            let key = VarInt64::try_from(k as u64).unwrap();
            keys.push(key);
            let value = VarInt64::try_from((k * k) as u64).unwrap();
            values1.push(value);
            let value = VarInt64::try_from((k + 1) as u64).unwrap();
            values2.push(value);
            let value = VarInt64::try_from((2 * k) as u64).unwrap();
            values3.push(value);
        }

        // WRITE EVEN VALUES FROM VALUES1
        for k in 0..top {
            if k % 2 != 0 {
                continue;
            }
            let key = keys[k as usize].clone();
            let val = values1[k as usize].clone();
            let result = mc.get(key.data_ref());
            assert!(result.is_ok());
            let option = result.ok().unwrap();
            assert!(option.is_none());
            let result = mc.set(key.data_ref(), &val.data_ref());
            assert!(result.is_ok());
            let result = mc.get(key.data_ref());
            assert!(result.is_ok());
            let option = result.ok().unwrap();
            let val_out = option.unwrap().as_ref().unwrap().clone();
            let val_in = val.data_ref().to_vec();
            assert!(val_out.cmp(&val_in) == Ordering::Equal);
        }

        // READ ALL EVEN VALUES AS VALUES1
        for k in 0..top {
            if k % 2 != 0 {
                continue;
            }
            let key = keys[k as usize].clone();
            let val = values1[k as usize].clone();
            let result = mc.get(key.data_ref());
            assert!(result.is_ok());
            let option = result.ok().unwrap();
            assert!(option.is_some());
            let val_out = option.unwrap().as_ref().unwrap().clone();
            let val_in = val.data_ref().to_vec();
            assert!(val_out.cmp(&val_in) == Ordering::Equal);
        }

        // WRITE ODD VALUES AS VALUES2
        for k in 0..top {
            if k % 2 != 1 {
                continue;
            }
            let key = keys[k as usize].clone();
            let val = values2[k as usize].clone();
            let result = mc.get(key.data_ref());
            assert!(result.is_ok());
            let option = result.ok().unwrap();
            assert!(option.is_none());
            let result = mc.set(key.data_ref(), &val.data_ref());
            assert!(result.is_ok());
            let result = mc.get(key.data_ref());
            assert!(result.is_ok());
            let option = result.ok().unwrap();
            let val_out = option.unwrap().as_ref().unwrap().clone();
            let val_in = val.data_ref().to_vec();
            assert!(val_out.cmp(&val_in) == Ordering::Equal);
        }

        // CHECK OVERRIDING SOME VALUES
        for k in top / 2..top {
            let key = keys[k as usize].clone();
            let mut val = values1[k as usize].clone();
            if k % 2 == 1 {
                val = values2[k as usize].clone();
            }
            let result = mc.get(key.data_ref());
            assert!(result.is_ok());
            let option = result.ok().unwrap();
            let val_out = option.unwrap().as_ref().unwrap().clone();
            let val_in = val.data_ref().to_vec();
            assert!(val_out.cmp(&val_in) == Ordering::Equal);
            let val = values3[k as usize].clone();
            let result = mc.set(key.data_ref(), &val.data_ref());
            assert!(result.is_ok());
            let result = mc.get(key.data_ref());
            assert!(result.is_ok());
            let option = result.ok().unwrap();
            let val_out = option.unwrap().as_ref().unwrap().clone();
            let val_in = val.data_ref().to_vec();
            let check = val_out.cmp(&val_in) == Ordering::Equal;
            assert!(check);
        }

        // CHECK THE VALUES CAN BE DELETED
        for k in 0..top {
            let key = keys[k as usize].clone();
            let result = mc.set(key.data_ref(), &TombstoneValue::Tombstone);
            assert!(result.is_ok());
            let result = mc.get(key.data_ref());
            assert!(result.is_ok());
            let option = result.ok().unwrap();
            let val_out = option.unwrap();
            let val_out = val_out.as_ref();
            assert!(val_out.is_none());
        }

        // WRITE EVEN VALUES AS VALUES2
        for k in 0..top {
            if k % 2 != 0 {
                continue;
            }
            let key = keys[k as usize].clone();
            let val = values2[k as usize].clone();
            let result = mc.get(key.data_ref());
            assert!(result.is_ok());
            let option = result.ok().unwrap();
            assert!(option.is_some());
            let val_out = option.unwrap().clone();
            assert!(val_out.as_ref().is_none());
            let result = mc.set(key.data_ref(), &val.data_ref());
            assert!(result.is_ok());
            let result = mc.get(key.data_ref());
            assert!(result.is_ok());
            let option = result.ok().unwrap();
            let val_out = option.unwrap().as_ref().unwrap().clone();
            let val_in = val.data_ref().to_vec();
            assert!(val_out.cmp(&val_in) == Ordering::Equal);
        }
        // CLEAN UP
        let result = mc.close();
        assert!(result.is_ok());
    }

    #[test]
    fn test_mem_table_end_to_end_2() {
        let top: usize = 100;
        let storage = InMemoryBlobStore::new();

        // OPEN A NEW DB

        // TODO(t/1387): This test should be run with the LRU block cache
        // when possible.
        type TestOnlyStorageWrapper = SimpleStorageWrapper<InMemoryBlobStore, NoBlockCache>;
        let storage_wrapper =
            SimpleStorageWrapper::from((storage.clone(), NoBlockCache::with_capacity(0)));
        type TestOnlyTabletSstStore = SmartTablet<TestOnlyStorageWrapper>;
        let levels: Vec<String> = Vec::new();
        let (compactor_tx, compactor_rx) = channel::<CompactorMessage>();
        let sst_store = TestOnlyTabletSstStore::new(
            storage_wrapper.clone(),
            levels.into_iter(),
            compactor_tx.clone(),
        )
        .unwrap();
        let compactor = Compactor::new(
            storage_wrapper.clone(),
            sst_store.clone(),
            compactor_tx.clone(),
            compactor_rx,
        );
        let logger = DefaultLogger::default();

        let mc = MemTable::new(
            storage.clone(),
            compactor_tx.clone(),
            WalWrapperConfig::default(),
            logger,
        )
        .unwrap();

        // CREATE SOME DATA
        let pairs: Vec<_> = (0..top)
            .map(|k| {
                (
                    VarInt64::try_from(k as u64).unwrap().data_ref().to_vec(),
                    VarInt64::try_from((k + 1) as u64)
                        .unwrap()
                        .data_ref()
                        .to_vec(),
                )
            })
            .collect();

        // WRITE VALUES TO MEM_TABLE

        for (key, value) in pairs.iter() {
            assert!(mc.get(key).unwrap().is_none());
            mc.set(key, &value.as_slice()).unwrap();
            assert_eq!(value, mc.get(key).unwrap().unwrap().as_ref().unwrap());
        }

        // SCAN MEM_TABLE

        let scanner = mc.scan();
        let mut count = 0;
        for result in scanner {
            let found = result.unwrap();
            let (key, value) = &pairs[count];
            assert_eq!(key, found.key_ref());
            assert_eq!(value, found.value_ref().as_ref().unwrap());
            count += 1;
        }
        assert_eq!(count, top);

        // RUN MINOR COMPACTION

        spawn(move || compactor.run());
        mc.rotate().unwrap();

        // SCAN SST_STORE

        let scanner = sst_store.try_scan().unwrap();
        let mut count = 0;
        for result in scanner {
            let pair = result.unwrap();
            assert!(top > count);
            let (key, value) = &pairs[count];
            assert_eq!(pair.key_ref(), key);
            assert_eq!(pair.value_ref().as_ref().unwrap(), value);
            count += 1;
        }
        assert_eq!(count, top);

        // CHECK MEM_TABLE IS EMPTY
        let scanner = mc.scan();
        assert_eq!(scanner.count(), 0);

        // CLEAN UP
        mc.close().unwrap();
        compactor_tx.send(CompactorMessage::Shutdown).unwrap();
    }
}
