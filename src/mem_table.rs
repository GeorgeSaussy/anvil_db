use std::error::Error;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::sync::mpsc::{channel, Sender};
use std::thread::spawn;

use crate::anvil_db::AnvilDbConfig;
use crate::compactor::{Compactor, CompactorError, CompactorMessage};
use crate::concurrent_skip_list::{ConcurrentSkipListPairView, ConcurrentSkipListScanner};
use crate::context::Context;
use crate::kv::{
    MergedHomogenousIter, TombstonePairLike, TombstonePointReader, TombstoneScanner,
    TombstoneStore, TombstoneValueLike,
};
use crate::logging::{error, info};
use crate::mem_queue::{MemQueue, MemTableEntryIterator};
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

impl Display for MemTableError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            MemTableError::Compactor(err) => write!(f, "CompactorError: {:?}", err),
            MemTableError::Sst(err) => write!(f, "SstError: {:?}", err),
            MemTableError::Wal(err) => write!(f, "WalError: {:?}", err),
        }
    }
}

impl Error for MemTableError {}

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
pub(crate) struct MemTable<Ctx: Context> {
    ctx: Ctx,
    mem_queue: MemQueue<Ctx>,
    compactor_tx: Sender<CompactorMessage>,
}

type RecoveryData<Ctx, T> = (
    MemTable<Ctx>,
    T,
    Compactor<Ctx, T>,
    Sender<CompactorMessage>,
);

impl<Ctx: Context + Send + 'static> MemTable<Ctx>
where
    <<Ctx as Context>::BlobStore as BlobStore>::WriteCursor: Send + Sync + 'static,
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
        ctx: Ctx,
        compactor_tx: Sender<CompactorMessage>,
        wal_config: WalWrapperConfig,
    ) -> Result<MemTable<Ctx>, MemTableError> {
        Ok(MemTable {
            ctx: ctx.clone(),
            mem_queue: MemQueue::new(ctx.clone(), wal_config)?,
            compactor_tx,
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
    pub(crate) fn recover<T: TabletSstStore>(
        ctx: Ctx,
        config: AnvilDbConfig<<Ctx as Context>::Logger>,
    ) -> Result<RecoveryData<Ctx, T>, MemTableError> {
        let recovery = WalWrapper::recover(ctx.blob_store_ref().clone(), config.wal_config())?;
        let (wal_wrapper, recovery_data) = recovery;

        let (compactor_tx, compactor_rx) = channel();
        let mem_queue =
            MemQueue::from_recovery(ctx.clone(), wal_wrapper, recovery_data.next_wal_id());

        let manifest_levels: Vec<String> = recovery_data
            .manifest_ref()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let sst_store = T::recover(
            ctx.blob_store_ref(),
            manifest_levels.iter(),
            compactor_tx.clone(),
        )?;

        let mut compactor = Compactor::new(
            ctx.clone(),
            sst_store.clone(),
            config.compactor_settings(),
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
            ctx,
            mem_queue,
            compactor_tx: compactor_tx.clone(),
        };

        Ok((mem_table, sst_store, compactor, compactor_tx))
    }

    pub(crate) async fn async_set<T: TombstoneValueLike>(
        &self,
        key: &[u8],
        value: &T,
    ) -> Result<(), <Self as TombstoneStore>::E> {
        if let Err(err) = self.mem_queue.async_set(key, value).await {
            match err {
                // the WAL is full, rotate in the background and return ok
                WalError::RotationRequired => {
                    self.background_rotate();
                    return Ok(());
                }
                // otherwise return the error
                _ => return Err(err.into()),
            }
        }
        Ok(())
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
                if let CompactorError::NothingToCompact = err {
                    error!(self.ctx.logger(), "error compacting: {:?}", err);
                    return Err(err.into());
                }
                info!(
                    self.ctx.logger(),
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

    fn background_rotate(&self) {
        let mem_table = self.clone();
        // TODO(t/1336): This should spawn to an async thread.
        spawn(move || {
            info!(mem_table.ctx.logger(), "rotating wal in the background");
            if let Err(err) = mem_table.rotate() {
                error!(mem_table.ctx.logger(), "error rotating wal: {:?}", err);
            }
        });
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

impl<Ctx: Context> TombstonePointReader for MemTable<Ctx>
where
    <<Ctx as Context>::BlobStore as BlobStore>::WriteCursor: Send + Sync + 'static,
{
    type Error = MemTableError;

    fn get<Ctx2: Context>(
        &self,
        _ctx: &Ctx2,
        key: &[u8],
    ) -> Result<Option<TombstoneValue>, Self::Error> {
        Ok(self.mem_queue.get(key))
    }
}

impl<Ctx: Context> TombstoneScanner for MemTable<Ctx>
where
    <<Ctx as Context>::BlobStore as BlobStore>::WriteCursor: Send + Sync + 'static,
{
    type Error = MemTableError;
    type Iter = MergedHomogenousIter<MemTableEntryIterator>;

    fn scan(&self) -> Self::Iter {
        self.mem_queue.iter()
    }
}

impl<Ctx: Context + Send + 'static> TombstoneStore for MemTable<Ctx>
where
    <<Ctx as Context>::BlobStore as BlobStore>::WriteCursor: Send + Sync + 'static,
{
    type E = MemTableError;

    fn set<T: TombstoneValueLike>(
        &self,
        key: &[u8],
        value: &T,
    ) -> Result<(), <Self as TombstoneStore>::E> {
        if let Err(err) = self.mem_queue.set(key, value) {
            match err {
                // the WAL is full, rotate in the background and return ok
                WalError::RotationRequired => {
                    let my_self = self.clone();
                    // TODO(t/1395): This should be handled by the ECS-like system.
                    spawn(move || {
                        info!(my_self.ctx.logger(), "rotating wal in the background");
                        if let Err(err) = my_self.rotate() {
                            error!(my_self.ctx.logger(), "error rotating wal: {:?}", err);
                        }
                    });
                    return Ok(());
                }
                // otherwise return the error
                _ => return Err(err.into()),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::cmp::Ordering;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::mpsc::channel;
    use std::task::{Context as TaskContext, Poll, Waker};
    use std::thread::spawn;
    use std::time::Instant;

    use super::*;
    use crate::compactor::{CompactorMessage, CompactorSettings};
    use crate::context::{Context, SimpleContext};
    use crate::helpful_macros::{clone, unwrap};
    use crate::kv::TryTombstoneScanner;
    use crate::logging::DefaultLogger;
    use crate::sst::block_cache::cache::LruBlockCache;
    use crate::sst::block_cache::BlockCache;
    use crate::storage::blob_store::InMemoryBlobStore;
    use crate::tablet::SmartTablet;
    use crate::var_int::VarInt64;

    #[test]
    fn test_mem_table_new() {
        let logger = DefaultLogger::default();
        let ctx = SimpleContext::from((
            InMemoryBlobStore::new(),
            LruBlockCache::with_capacity(32),
            logger,
        ));
        let (compactor_tx, _) = channel();

        // Just check it does not fail.
        unwrap!(MemTable::new(
            clone!(ctx),
            compactor_tx,
            WalWrapperConfig::default()
        ));
    }

    #[test]
    fn test_mem_table_close() {
        let storage = InMemoryBlobStore::new();
        let logger = DefaultLogger::default();
        let ctx = SimpleContext::from((storage, LruBlockCache::with_capacity(32), logger));
        let (compactor_tx, _) = channel();

        let result = MemTable::new(ctx, compactor_tx, WalWrapperConfig::default());
        let mem_table = unwrap!(result);
        assert!(mem_table.close().is_ok());
    }

    #[test]
    fn test_mem_table_end_to_end_1() {
        let top = 100;
        let storage = InMemoryBlobStore::new();
        let logger = DefaultLogger::default();
        let ctx = SimpleContext::from((storage, LruBlockCache::with_capacity(32), logger));
        let (compactor_tx, _) = channel();

        // OPEN A NEW DB
        let result = MemTable::new(clone!(ctx), compactor_tx, WalWrapperConfig::default());
        assert!(result.is_ok());
        let mc = unwrap!(result.ok());

        // CREATE SOME DATA
        let mut keys = Vec::new();
        let mut values1 = Vec::new();
        let mut values2 = Vec::new();
        let mut values3 = Vec::new();
        for k in 0..top {
            let key = unwrap!(VarInt64::try_from(k as u64));
            keys.push(key);
            let value = unwrap!(VarInt64::try_from((k * k) as u64));
            values1.push(value);
            let value = unwrap!(VarInt64::try_from((k + 1) as u64));
            values2.push(value);
            let value = unwrap!(VarInt64::try_from((2 * k) as u64));
            values3.push(value);
        }

        // WRITE EVEN VALUES FROM VALUES1
        for k in 0..top {
            if k % 2 != 0 {
                continue;
            }
            let key = keys[k as usize].clone();
            let val = values1[k as usize].clone();
            let result = mc.get(&ctx, key.data_ref());
            assert!(result.is_ok());
            let option = unwrap!(result.ok());
            assert!(option.is_none());
            let result = mc.set(key.data_ref(), &val.data_ref());
            assert!(result.is_ok());
            let result = mc.get(&ctx, key.data_ref());
            assert!(result.is_ok());
            let option = unwrap!(result.ok());
            let val_out = unwrap!(unwrap!(option).as_ref()).clone();
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
            let result = mc.get(&ctx, key.data_ref());
            assert!(result.is_ok());
            let option = unwrap!(result.ok());
            assert!(option.is_some());
            let val_out = unwrap!(unwrap!(option).as_ref()).clone();
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
            let result = mc.get(&ctx, key.data_ref());
            assert!(result.is_ok());
            let option = unwrap!(result.ok());
            assert!(option.is_none());
            let result = mc.set(key.data_ref(), &val.data_ref());
            assert!(result.is_ok());
            let result = mc.get(&ctx, key.data_ref());
            assert!(result.is_ok());
            let option = unwrap!(result.ok());
            let val_out = unwrap!(unwrap!(option).as_ref()).clone();
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
            let result = mc.get(&ctx, key.data_ref());
            assert!(result.is_ok());
            let option = unwrap!(result.ok());
            let val_out = unwrap!(unwrap!(option).as_ref()).clone();
            let val_in = val.data_ref().to_vec();
            assert!(val_out.cmp(&val_in) == Ordering::Equal);
            let val = values3[k as usize].clone();
            let result = mc.set(key.data_ref(), &val.data_ref());
            assert!(result.is_ok());
            let result = mc.get(&ctx, key.data_ref());
            assert!(result.is_ok());
            let option = unwrap!(result.ok());
            let val_out = unwrap!(unwrap!(option).as_ref()).clone();
            let val_in = val.data_ref().to_vec();
            let check = val_out.cmp(&val_in) == Ordering::Equal;
            assert!(check);
        }

        // CHECK THE VALUES CAN BE DELETED
        for k in 0..top {
            let key = keys[k as usize].clone();
            let result = mc.set(key.data_ref(), &TombstoneValue::Tombstone);
            assert!(result.is_ok());
            let result = mc.get(&ctx, key.data_ref());
            assert!(result.is_ok());
            let option = unwrap!(result.ok());
            let val_out = unwrap!(option);
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
            let result = mc.get(&ctx, key.data_ref());
            assert!(result.is_ok());
            let option = unwrap!(result.ok());
            assert!(option.is_some());
            let val_out = unwrap!(option).clone();
            assert!(val_out.as_ref().is_none());
            let result = mc.set(key.data_ref(), &val.data_ref());
            assert!(result.is_ok());
            let result = mc.get(&ctx, key.data_ref());
            assert!(result.is_ok());
            let option = unwrap!(result.ok());
            let val_out = unwrap!(unwrap!(option).as_ref()).clone();
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

        let logger = DefaultLogger::default();
        let ctx = SimpleContext::from((storage, LruBlockCache::with_capacity(32), logger));
        let levels: Vec<String> = Vec::new();
        let (compactor_tx, compactor_rx) = channel::<CompactorMessage>();
        let sst_store = unwrap!(SmartTablet::new(
            ctx.blob_store_ref(),
            levels.into_iter(),
            compactor_tx.clone(),
        ));
        let compactor = Compactor::new(
            clone!(ctx),
            sst_store.clone(),
            CompactorSettings::default(),
            compactor_tx.clone(),
            compactor_rx,
        );

        let mc = unwrap!(MemTable::new(
            clone!(ctx),
            compactor_tx.clone(),
            WalWrapperConfig::default(),
        ));

        // CREATE SOME DATA
        let pairs: Vec<_> = (0..top)
            .map(|k| {
                (
                    unwrap!(VarInt64::try_from(k as u64)).data_ref().to_vec(),
                    unwrap!(VarInt64::try_from((k + 1) as u64))
                        .data_ref()
                        .to_vec(),
                )
            })
            .collect();

        // WRITE VALUES TO MEM_TABLE

        for (key, value) in pairs.iter() {
            assert!(unwrap!(mc.get(&ctx, key)).is_none());
            unwrap!(mc.set(key, &value.as_slice()));
            assert_eq!(value, unwrap!(unwrap!(unwrap!(mc.get(&ctx, key))).as_ref()));
        }

        // SCAN MEM_TABLE

        let scanner = mc.scan();
        let mut count = 0;
        for result in scanner {
            let found = unwrap!(result);
            let (key, value) = &pairs[count];
            assert_eq!(key, found.key_ref());
            assert_eq!(value, unwrap!(found.value_ref().as_ref()));
            count += 1;
        }
        assert_eq!(count, top);

        // RUN MINOR COMPACTION

        spawn(move || compactor.run());
        unwrap!(mc.rotate());

        // SCAN SST_STORE

        let scanner = unwrap!(sst_store.try_scan(&ctx));
        let mut count = 0;
        for result in scanner {
            let pair = unwrap!(result);
            assert!(top > count);
            let (key, value) = &pairs[count];
            assert_eq!(pair.key_ref(), key);
            assert_eq!(unwrap!(pair.value_ref().as_ref()), value);
            count += 1;
        }
        assert_eq!(count, top);

        // CHECK MEM_TABLE IS EMPTY
        let scanner = mc.scan();
        assert_eq!(scanner.count(), 0);

        // CLEAN UP
        unwrap!(mc.close());
        unwrap!(compactor_tx.send(CompactorMessage::Shutdown));
    }

    #[test]
    fn test_mem_table_async_set() {
        let top = 100;
        let ctx = SimpleContext::from((
            InMemoryBlobStore::new(),
            LruBlockCache::with_capacity(32),
            DefaultLogger::default(),
        ));
        let (compactor_tx, _) = channel();

        // OPEN A NEW DB
        let result = MemTable::new(clone!(ctx), compactor_tx, WalWrapperConfig::default());
        assert!(result.is_ok());
        let mc = unwrap!(result.ok());

        // CREATE SOME DATA
        let buffs: Vec<Vec<u8>> = (0..top)
            .map(|k| {
                let key = unwrap!(VarInt64::try_from(k as u64));
                key.data_ref().to_vec()
            })
            .collect();
        for (i, buf_1) in buffs.iter().enumerate() {
            for (j, buf_2) in buffs.iter().enumerate() {
                let found = unwrap!(mc.get(&ctx, buf_2));
                if j < i {
                    let value = unwrap!(found);
                    assert_eq!(unwrap!(value.as_ref()), buf_2);
                    continue;
                }
                assert!(found.is_none());
            }
            let value = TombstoneValue::from(&buf_1.as_slice());
            let mut poller = Box::pin(mc.async_set(buf_1, &value));
            let start = Instant::now();
            loop {
                let poller = Pin::new(&mut poller);
                let waker = Waker::noop();
                let context = &mut TaskContext::from_waker(&waker);
                if let Poll::Ready(result) = poller.poll(context) {
                    assert!(result.is_ok());
                    break;
                }
                if start.elapsed().as_secs() > 1 {
                    panic!("async_set timed out");
                }
            }
            for (j, buf_2) in buffs.iter().enumerate() {
                let found = unwrap!(mc.get(&ctx, buf_2));
                if j <= i {
                    let value = unwrap!(found);
                    assert_eq!(unwrap!(value.as_ref()), buf_2);
                    continue;
                }
                assert!(found.is_none());
            }
        }
    }
}
