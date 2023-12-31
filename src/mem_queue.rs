use std::fmt::{Debug, Formatter};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};

use crate::concurrent_skip_list::{ConcurrentSkipList, ConcurrentSkipListScanner};
use crate::context::Context;
use crate::kv::{
    MergedHomogenousIter, RangeSet, TombstoneIterator, TombstonePair, TombstoneValue,
    TombstoneValueLike,
};
use crate::mem_table::MemTableError;
use crate::storage::blob_store::BlobStore;
use crate::wal::wal_entry::WalError;
use crate::wal::wal_wrapper::{WalWrapper, WalWrapperConfig};

type WalBlobId = String;
type WrapData = (
    WalBlobId,
    Arc<RwLock<MemQueueEntry>>,
    ConcurrentSkipListScanner<Vec<u8>, TombstoneValue>,
);

#[derive(Clone, Debug)]
pub(crate) struct MemQueueEntry {
    skip_list: ConcurrentSkipList<Vec<u8>, TombstoneValue>,
    wal: WalWrapper,
    next: Option<Arc<RwLock<MemQueueEntry>>>,
    compacted: bool,
    finalized: bool,
}

impl MemQueueEntry {
    pub(crate) fn wrap<B: BlobStore>(
        blob_store: &B,
        id: usize,
        wal_config: WalWrapperConfig,
        other: MemQueueEntry,
    ) -> Result<(MemQueueEntry, WrapData), MemTableError>
    where
        B::WriteCursor: Send + Sync + 'static,
    {
        let wal_blob_id = other.wal.blob_id_ref().to_string();
        other.wal.clone().close()?;
        let iter = other.skip_list.iter();
        let next = Arc::new(RwLock::new(other));
        Ok((
            MemQueueEntry {
                skip_list: ConcurrentSkipList::new(),
                wal: WalWrapper::new(blob_store, wal_config.with_next_wal_id(id))?,
                next: Some(next.clone()),
                compacted: false,
                finalized: false,
            },
            (wal_blob_id, next, iter),
        ))
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<TombstoneValue> {
        if let Some(value) = self.skip_list.get(key).map(|value| (*value).clone()) {
            return Some(value);
        }
        let mut option = self.next.clone();
        loop {
            if let Some(next) = option {
                if let Some(value) = next
                    .read()
                    .unwrap()
                    .skip_list
                    .get(key)
                    .map(|value| (*value).clone())
                {
                    return Some(value);
                }
                option = next.read().unwrap().next.clone();
                continue;
            }
            return None;
        }
    }

    pub(crate) fn set<T: TombstoneValueLike>(&self, key: &[u8], value: &T) -> Result<(), WalError> {
        if self.finalized {
            return Err(WalError::FinalizedWal);
        }
        self.wal.set(key, value)?;
        self.skip_list.set(key, TombstoneValue::from(value));
        Ok(())
    }

    pub(crate) async fn async_set<T: TombstoneValueLike>(
        &self,
        key: &[u8],
        value: &T,
    ) -> Result<(), WalError> {
        if self.finalized {
            return Err(WalError::FinalizedWal);
        }
        self.wal.async_set(key, value).await?;
        self.skip_list.set(key, TombstoneValue::from(value));
        Ok(())
    }

    pub(crate) fn iter(&self) -> MemTableEntryIterator {
        MemTableEntryIterator {
            inner: self.skip_list.iter(),
        }
    }

    /// Mark this WAL as finalized. This happens before the WAL is compacted,
    /// and ensures that any lingering references to the entry cannot write
    /// new values.
    fn mark_finalized(&mut self) {
        self.finalized = true;
    }
}

pub(crate) struct MemTableEntryIterator {
    inner: ConcurrentSkipListScanner<Vec<u8>, TombstoneValue>,
}

impl Debug for MemTableEntryIterator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemTableEntryIterator").finish()
    }
}

impl Iterator for MemTableEntryIterator {
    type Item = Result<TombstonePair, MemTableError>;

    fn next(&mut self) -> Option<Self::Item> {
        let pair = self.inner.next()?;
        let key = pair.key_ref().to_vec();
        let value = pair.value_ref().clone();
        let t_pair = TombstonePair::from((key, value));
        Some(Ok(t_pair))
    }
}

impl RangeSet for MemTableEntryIterator {
    fn from(mut self, key: &[u8]) -> Self {
        self.inner = self.inner.from(key);
        self
    }

    fn to(mut self, key: &[u8]) -> Self {
        self.inner = self.inner.to(key.to_vec());
        self
    }
}

impl TombstoneIterator for MemTableEntryIterator {
    type Error = MemTableError;
}

#[derive(Clone, Debug)]
pub(crate) struct MemQueue<Ctx: Context> {
    ctx: Ctx,
    wal_id: Arc<AtomicUsize>,
    wal_config: WalWrapperConfig,
    head: Arc<RwLock<MemQueueEntry>>,
}

impl<Ctx: Context> MemQueue<Ctx>
where
    <<Ctx as Context>::BlobStore as BlobStore>::WriteCursor: Send + Sync + 'static,
{
    pub(crate) fn new(ctx: Ctx, wal_config: WalWrapperConfig) -> Result<Self, MemTableError> {
        let skip_list = ConcurrentSkipList::new();
        let head = MemQueueEntry {
            wal: WalWrapper::new(ctx.blob_store_ref(), wal_config.clone().with_next_wal_id(0))?,
            skip_list,
            next: None,
            compacted: false,
            finalized: false,
        };
        let head = Arc::new(RwLock::new(head));
        Ok(MemQueue {
            ctx,
            wal_id: Arc::new(AtomicUsize::new(1)),
            wal_config,
            head,
        })
    }

    pub(crate) fn from_recovery(ctx: Ctx, wal: WalWrapper, next_wal_id: usize) -> MemQueue<Ctx> {
        let wal_config = wal.wal_config_ref().clone();
        let skip_list = ConcurrentSkipList::new();
        let head = MemQueueEntry {
            wal,
            skip_list,
            next: None,
            compacted: false,
            finalized: false,
        };
        let head = Arc::new(RwLock::new(head));
        MemQueue {
            ctx,
            wal_id: Arc::new(AtomicUsize::new(next_wal_id)),
            wal_config,
            head,
        }
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<TombstoneValue> {
        self.head.read().unwrap().get(key)
    }

    pub(crate) fn set<T: TombstoneValueLike>(&self, key: &[u8], value: &T) -> Result<(), WalError> {
        self.head.read().unwrap().set(key, value)
    }

    pub(crate) async fn async_set<T: TombstoneValueLike>(
        &self,
        key: &[u8],
        value: &T,
    ) -> Result<(), WalError> {
        loop {
            let head;
            {
                // This should be safe. The underlying WAL cannot be compacted
                // until it is finalized. If the write succeeds, then the values
                // are readable and will be compacted. If not, then a
                // FinalizedWal error will be returned.
                head = self.head.read().unwrap().clone();
            }
            let result = head.async_set(key, value).await;
            if let Err(WalError::FinalizedWal) = result {
                continue;
            }
            return result;
        }
    }

    pub(crate) fn rotate(&self) -> Result<WrapData, MemTableError> {
        let mut head = self.head.write().unwrap();
        head.mark_finalized();
        let wal_id = self
            .wal_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let (new_head, wrap_data) = MemQueueEntry::wrap(
            self.ctx.blob_store_ref(),
            wal_id,
            self.wal_config.clone(),
            head.clone(),
        )?;
        *head = new_head;

        Ok(wrap_data)
    }

    pub(crate) fn mark_minor_compaction(
        &self,
        wal_blob_id: &str,
        sst_blob_id: &str,
    ) -> Result<(), MemTableError> {
        let wal = &self.head.read().unwrap().wal;
        wal.mark_minor_compaction(wal_blob_id, sst_blob_id)?;
        Ok(())
    }

    pub(crate) fn maybe_drop_arc(&self, arc: Arc<RwLock<MemQueueEntry>>) {
        {
            let mut entry = arc.write().unwrap();
            entry.compacted = true;
        }
        self.try_drop_tail();
    }

    fn try_drop_tail(&self) {
        let mut last_non_compacted = None;

        let mut arc = self.head.clone();
        loop {
            let entry = arc.read().unwrap().clone();
            if !entry.compacted {
                last_non_compacted = Some(arc.clone());
                if let Some(next) = entry.next.clone() {
                    arc = next;
                    continue;
                }
                return;
            }
            if let Some(next) = entry.next.clone() {
                arc = next;
                continue;
            }
            break;
        }
        if let Some(arc) = last_non_compacted {
            let mut entry = arc.write().unwrap();
            entry.next = None;
        }
    }

    pub(crate) fn iter(&self) -> MergedHomogenousIter<MemTableEntryIterator> {
        let mut entry = self.head.read().unwrap().clone();
        let mut iters = vec![entry.iter()];
        loop {
            if let Some(next) = entry.next.clone() {
                entry = next.read().unwrap().clone();
                iters.push(entry.iter());
                continue;
            }
            break;
        }
        MergedHomogenousIter::new(iters.into_iter())
    }

    pub(crate) fn close(self) -> Result<(), MemTableError> {
        let entry = self.head.write().unwrap().clone();
        entry.wal.close()?;
        Ok(())
    }
}
