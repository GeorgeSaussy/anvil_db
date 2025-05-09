use std::async_iter::AsyncIterator;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::mpsc::{channel, Sender};
use std::task::{Context as TaskContext, Poll};

use crate::compactor::{Compactor, CompactorError, CompactorMessage, CompactorSettings};
use crate::context::Context;
use crate::kv::{
    AsyncTombstoneIterator, JoinedIter, RangeSet, TombstoneIterator, TombstonePair,
    TombstonePointReader, TombstoneScanner, TombstoneStore, TombstoneValue,
    TryAsyncTombstoneScanner, TryTombstoneScanner,
};
use crate::logging::Logger;
use crate::mem_table::MemTable;
use crate::os::{LinuxInterface, OsInterface};
use crate::sst::block_cache::BlockCache;
use crate::storage::blob_store::BlobStore;
use crate::tablet::{SmartTablet, SmartTabletIterator, TabletSstStore};
use crate::wal::wal_wrapper::WalWrapperConfig;

/// The in memory representation of a AnvilDB database.
#[derive(Debug, Clone)]
pub(crate) struct AnvilDb<Ctx: Context> {
    /// Stores library-wide configuration and backend clients.
    ctx: Ctx,
    /// The in-memory part of the LSM tree
    /// The mem_table implementation
    /// is already thread safe, so we do not want to hold
    /// a this lock while manipulating it.
    mem_table: MemTable<Ctx>,
    /// the on-disk part of the LSM tree
    sst_store: SmartTablet,
    /// the compactor object
    compactor_tx: Sender<CompactorMessage>,
}

impl<Ctx: Context> AnvilDb<Ctx>
where
    Ctx: 'static,
    <<Ctx as Context>::BlobStore as BlobStore>::WriteCursor: Send + Sync + 'static,
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
    pub(crate) fn new(blob_store: <Ctx as Context>::BlobStore) -> Result<AnvilDb<Ctx>, String>
    where
        <Ctx as Context>::Logger: Default,
    {
        let config = AnvilDbConfig::default();
        Self::with_config(blob_store, config)
    }

    pub(crate) fn with_config(
        blob_store: <Ctx as Context>::BlobStore,
        config: AnvilDbConfig<<Ctx as Context>::Logger>,
    ) -> Result<AnvilDb<Ctx>, String> {
        let max_cached_blocks = config.block_cache_bytes / config.target_block_size;
        let block_cache = <Ctx as Context>::BlockCache::with_capacity(max_cached_blocks);
        let ctx = Ctx::from((blob_store.clone(), block_cache, config.logger.clone()));

        let (compactor_tx, compactor_rx) = channel::<CompactorMessage>();
        let mem_table = MemTable::new(ctx.clone(), compactor_tx.clone(), config.wal_config())?;

        let levels: Vec<String> = Vec::new();
        let sst_store = SmartTablet::recover(
            ctx.blob_store_ref(),
            levels.into_iter(),
            compactor_tx.clone(),
        )?;

        let compactor = Compactor::new(
            sst_store.clone(),
            config.compactor_settings(),
            compactor_tx.clone(),
            compactor_rx,
            0,
        );

        let bg_ctx = ctx.clone();
        ctx.bg_ref()
            .spawn_compactor_bg(move || compactor.run(&bg_ctx).unwrap());

        Ok(AnvilDb {
            ctx,
            mem_table,
            sst_store,
            compactor_tx,
        })
    }

    /// Get an element from the database.
    ///
    /// # Arguments
    ///
    /// - key: the key to retrieve
    ///
    /// # Returns
    ///
    /// A Cord if found, or an error string.
    pub(crate) fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let found = self.mem_table.get(&self.ctx, key)?;
        if let Some(value) = found {
            return Ok(value.into());
        }
        let found = self.sst_store.get(&self.ctx, key)?;
        if let Some(value) = found {
            return Ok(value.into());
        }
        Ok(None)
    }

    pub(crate) async fn async_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        self.get(key)
    }

    /// Set a key-value pair for the database.
    ///
    /// # Arguments
    ///
    /// - key: the key to set
    /// - value: the value to set
    ///
    /// # Returns
    ///
    /// An error String on error.
    pub(crate) fn set(&self, key: &[u8], value: &[u8]) -> Result<(), String> {
        self.mem_table.set(key, &value)?;
        Ok(())
    }

    pub(crate) async fn async_set(&self, key: &[u8], value: &[u8]) -> Result<(), String> {
        self.mem_table.async_set(key, &value).await?;
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

    pub(crate) async fn async_remove(&self, key: &[u8]) -> Result<(), String> {
        self.remove(key)
    }

    pub(crate) fn try_scan(&self) -> Result<AnvilDbScanner<Ctx>, String> {
        let a = TombstoneScanner::scan(&self.mem_table);
        let b = self.sst_store.try_scan(&self.ctx)?;
        Ok(AnvilDbScanner {
            inner: JoinedIter::new(a, b),
        })
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
        blob_store: <Ctx as Context>::BlobStore,
        config: AnvilDbConfig<<Ctx as Context>::Logger>,
    ) -> Result<AnvilDb<Ctx>, String> {
        let block_cache = <Ctx as Context>::BlockCache::with_capacity(config.block_cache_bytes);
        let ctx = Ctx::from((blob_store.clone(), block_cache, config.logger.clone()));

        let (mem_table, sst_store, compactor, compactor_tx) =
            MemTable::recover(ctx.clone(), config)?;
        let bg_ctx = ctx.clone();
        ctx.bg_ref()
            .spawn_compactor_bg(move || compactor.run(&bg_ctx).unwrap());

        Ok(AnvilDb {
            ctx,
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
    pub(crate) fn close(self) -> Result<(), String> {
        self.compactor_tx
            .send(CompactorMessage::Shutdown)
            .map_err(|err| format!("failed to send shutdown message: {err:?}",))?;
        self.mem_table.close()?;
        Ok(())
    }
}

type BigScanType<'a, Ctx> =
    JoinedIter<String, <MemTable<Ctx> as TombstoneScanner>::Iter, SmartTabletIterator<'a, Ctx>>;

#[derive(Debug)]
pub(crate) struct AnvilDbScanner<'a, Ctx: Context>
where
    <<Ctx as Context>::BlobStore as BlobStore>::WriteCursor: Send + Sync + 'static,
{
    inner: BigScanType<'a, Ctx>,
}

impl<Ctx: Context> Iterator for AnvilDbScanner<'_, Ctx>
where
    <<Ctx as Context>::BlobStore as BlobStore>::WriteCursor: Send + Sync + 'static,
{
    type Item = Result<TombstonePair, String>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<Ctx: Context> RangeSet for AnvilDbScanner<'_, Ctx>
where
    <<Ctx as Context>::BlobStore as BlobStore>::WriteCursor: Send + Sync,
{
    fn from(mut self, key: &[u8]) -> Self {
        self.inner = self.inner.from(key);
        self
    }

    fn to(mut self, key: &[u8]) -> Self {
        self.inner = self.inner.to(key);
        self
    }
}

impl<Ctx: Context> TombstoneIterator for AnvilDbScanner<'_, Ctx>
where
    <<Ctx as Context>::BlobStore as BlobStore>::WriteCursor: Send + Sync,
{
    type Error = String;
}

#[derive(Debug)]
pub(crate) struct AsyncAnvilDbScanner<'a, Ctx: Context>
where
    <<Ctx as Context>::BlobStore as BlobStore>::WriteCursor: Send + Sync,
{
    inner: BigScanType<'a, Ctx>,
}

impl<Ctx: Context> AsyncIterator for AsyncAnvilDbScanner<'_, Ctx>
where
    <<Ctx as Context>::BlobStore as BlobStore>::WriteCursor: Send + Sync,
{
    type Item = Result<TombstonePair, String>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        // TODO(t/1336): This should actually poll the inner iterator.
        Poll::Ready(Some(Err("not implemented".to_string())))
    }
}

impl<Ctx: Context> RangeSet for AsyncAnvilDbScanner<'_, Ctx>
where
    <<Ctx as Context>::BlobStore as BlobStore>::WriteCursor: Send + Sync,
{
    fn from(mut self, key: &[u8]) -> Self {
        self.inner = self.inner.from(key);
        self
    }

    fn to(mut self, key: &[u8]) -> Self {
        self.inner = self.inner.to(key);
        self
    }
}

impl<Ctx: Context> AsyncTombstoneIterator for AsyncAnvilDbScanner<'_, Ctx>
where
    <<Ctx as Context>::BlobStore as BlobStore>::WriteCursor: Send + Sync,
{
    type Error = String;
}

impl<'a, Ctx: Context> TryAsyncTombstoneScanner for &'a AnvilDb<Ctx>
where
    <<Ctx as Context>::BlobStore as BlobStore>::WriteCursor: Send + Sync,
{
    type Error = String;
    type Iter = AsyncAnvilDbScanner<'a, Ctx>;

    fn try_async_scan(&self) -> Result<Self::Iter, Self::Error> {
        let a = TombstoneScanner::scan(&self.mem_table);
        let b = self.sst_store.try_scan(&self.ctx)?;
        Ok(AsyncAnvilDbScanner {
            inner: JoinedIter::new(a, b),
        })
    }
}

pub(crate) struct AnvilDbConfig<L> {
    block_cache_bytes: usize,
    target_block_size: usize,
    wal_config: WalWrapperConfig,
    logger: L,
}

impl<L: Logger + Default> Default for AnvilDbConfig<L> {
    fn default() -> Self {
        // TODO(t/1442): Add support for other OSes.
        let os_client = LinuxInterface;
        let block_cache_bytes = if let Ok(b) = os_client.free_ram() {
            b / 2
        } else {
            // 0.5 GiB
            1024 * 1024 * 1024 / 2
        };

        // TODO(gs): How to set a reasonable block size?
        let target_block_size = 32 * 1024 * 1024; // 32 MiB

        Self {
            block_cache_bytes,
            target_block_size,
            wal_config: WalWrapperConfig::default(),
            logger: L::default(),
        }
    }
}

impl<L: Logger> AnvilDbConfig<L> {
    pub(crate) fn wal_config(&self) -> WalWrapperConfig {
        self.wal_config.clone()
    }

    pub(crate) fn compactor_settings(&self) -> CompactorSettings {
        CompactorSettings::default().with_target_block_size(self.target_block_size)
    }

    pub(crate) fn with_max_concurrent_writers(mut self, max_concurrent_writers: usize) -> Self {
        self.wal_config = self
            .wal_config
            .with_max_concurrent_writers(max_concurrent_writers);
        self
    }

    pub(crate) fn with_max_wal_bytes(mut self, max_wal_bytes: usize) -> Self {
        self.wal_config = self.wal_config.with_max_wal_bytes(max_wal_bytes);
        self
    }

    pub(crate) fn with_cache_size_bytes(mut self, cache_size_bytes: usize) -> Self {
        self.block_cache_bytes = cache_size_bytes;
        self
    }
}

#[cfg(test)]
mod test {
    use std::cmp::Ordering;
    use std::future::Future;
    use std::task::Waker;
    use std::thread::sleep;
    use std::time::Duration;
    use std::time::Instant;

    use super::*;
    use crate::context::SimpleContext;
    use crate::helpful_macros::unwrap;
    use crate::logging::debug;
    use crate::logging::DefaultLogger;
    use crate::sst::block_cache::cache::LruBlockCache;
    use crate::storage::blob_store::InMemoryBlobStore;
    use crate::var_int::VarInt64;

    type TestOnlyContext = SimpleContext<InMemoryBlobStore, LruBlockCache, DefaultLogger>;
    #[test]
    fn test_anvil_db_end_to_end() {
        let store = InMemoryBlobStore::new();
        let jdb: AnvilDb<TestOnlyContext> = unwrap!(AnvilDb::<_>::new(store));
        let top: u64 = 10000;

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

    #[test]
    fn test_anvil_db_async_end_to_end() {
        type TestOnlyContext = SimpleContext<InMemoryBlobStore, LruBlockCache, DefaultLogger>;
        type TestAnvilDb = AnvilDb<TestOnlyContext>;

        let top: u64 = 10000;
        let pairs: Vec<_> = (0..top)
            .map(|k| {
                let key = VarInt64::try_from(k).unwrap();
                let val = VarInt64::try_from(k * k).unwrap();
                (key.data_ref().to_vec(), val.data_ref().to_vec())
            })
            .collect();

        let store = InMemoryBlobStore::new();
        let jdb: TestAnvilDb = AnvilDb::new(store).unwrap();
        let start = Instant::now();
        for pair in pairs.iter() {
            jdb.set(pair.0.as_slice(), pair.1.as_slice()).unwrap();
        }
        let duration = start.elapsed();
        debug!(
            &(),
            "Writing {} points synchronously took {:.2} sec ({:.2} us per point)",
            top,
            duration.as_secs_f64(),
            duration.as_micros() as f64 / top as f64
        );
        for pair in pairs.iter() {
            assert_eq!(
                jdb.get(pair.0.as_slice()).unwrap().unwrap(),
                pair.1.as_slice()
            );
        }
        assert!(jdb.close().is_ok());

        let store = InMemoryBlobStore::new();
        let jdb: TestAnvilDb = AnvilDb::new(store).unwrap();
        let start = Instant::now();
        let mut futures: Vec<_> = pairs
            .clone()
            .into_iter()
            .map(|pair| {
                let j2 = jdb.clone();
                let k = pair.0.clone();
                let v = pair.1.clone();
                Box::pin(async move { j2.async_set(k.as_slice(), v.as_slice()).await })
            })
            .collect();
        loop {
            let mut new_futures = Vec::new();
            for future in futures.into_iter() {
                let mut future = future;
                let waker = Waker::noop();
                let mut cx = TaskContext::from_waker(waker);
                let poll = Pin::new(&mut future).poll(&mut cx);
                match poll {
                    Poll::Ready(Err(e)) => panic!("async_set failed: {e:?}",),
                    Poll::Ready(Ok(())) => {}
                    Poll::Pending => new_futures.push(future),
                }
            }
            if new_futures.is_empty() {
                break;
            }
            futures = new_futures;
        }

        let duration = start.elapsed();
        debug!(
            &(),
            "Writing {top} points asynchronously took {:.2} sec ({:.2} us per point)",
            duration.as_secs_f64(),
            duration.as_micros() as f64 / top as f64
        );
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

    type TestOnlyAnvilDb = AnvilDb<TestOnlyContext>;
    fn db_with_max_wal_bytes(max_wal_bytes: usize) -> (InMemoryBlobStore, TestOnlyAnvilDb) {
        let store = InMemoryBlobStore::new();
        let db: TestOnlyAnvilDb = AnvilDb::<_>::with_config(
            store.clone(),
            AnvilDbConfig::default()
                .with_max_concurrent_writers(1)
                .with_max_wal_bytes(max_wal_bytes),
        )
        .unwrap();
        (store, db)
    }

    fn write_and_compact(top: usize, max_wal_bytes: usize) -> (InMemoryBlobStore, TestOnlyAnvilDb) {
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
            &(),
            "Writing {} points took {:.2} sec ({:.2} ms per point)",
            top,
            duration.as_secs_f64(),
            duration.as_micros() as f64 / top as f64
        );

        (store, db)
    }

    #[test]
    fn test_user_requested_compactions() {
        let top = 100_000_usize;
        let (_, db) = write_and_compact(top, 8 * 1024 * 1024);
        let start = Instant::now();
        db.compact().unwrap();
        let duration = start.elapsed();
        debug!(&(), "Took {:.2} sec to compact.", duration.as_secs_f64());
        db.close().unwrap();
    }

    #[test]
    fn test_natural_wal_rotation_1() {
        let top = 128;
        let max_wal_bytes = 1024;

        let (store, db) = write_and_compact(top, max_wal_bytes);
        let start = Instant::now();
        loop {
            sleep(Duration::from_millis(1000));
            let blob_names: Vec<_> = store.blob_iter().unwrap().collect();
            debug!(
                &(),
                "{} existing files:\n{}",
                blob_names.len(),
                blob_names.join(" ")
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
                &(),
                "wal_count: {} ;  target_sst_count: {} ; other_count: {}",
                wal_count,
                target_sst_count,
                other_count
            );
            assert!(wal_count > 0);
            if wal_count == 1 && target_sst_count == 1 && other_count == 0 {
                break;
            }
            if start.elapsed().as_secs() > 30 {
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
            unwrap!(db.set(&key, &value));
            if idx % (top / 8) == 0 {
                sleep(Duration::from_millis(100));
                if unwrap!(store.blob_iter()).any(|blob_id| blob_id.ends_with(".sst")) {
                    break;
                }
            }
            assert_ne!(idx, top - 1);
        }

        unwrap!(db.close());
    }
}
