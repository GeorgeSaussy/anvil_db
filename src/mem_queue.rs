use std::fmt::{Debug, Formatter};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};

use crate::helpful_macros::unlock;
use crate::kv::{
    MergedHomogenousIter, RangeSet, TombstoneIterator, TombstonePair, TombstonePairLike,
    TombstoneValue, TombstoneValueLike,
};
use crate::manifest::ManifestUpdate;
use crate::mem_table::MemTableError;
use crate::storage::blob_store::{BlobStore, WriteCursor};
use crate::structs::skip_list::{SkipList, SkipListScanner};
use crate::wal::wal_entry::WalError;
use crate::wal::wal_wrapper::{WalWrapper, WalWrapperConfig};

type WalBlobId = String;
type WrapData = (WalBlobId, SkipListScanner);

#[derive(Debug)]
pub(crate) struct MemQueueEntry<WC> {
    skip_list: Arc<SkipList>,
    wal: WalWrapper<WC>,
    next: Option<Box<MemQueueEntry<WC>>>,
    compacted: bool,
    finalized: bool,
}

impl<WC: WriteCursor> MemQueueEntry<WC> {
    pub(crate) fn wrap<B: BlobStore<WriteCursor = WC>>(
        &mut self,
        blob_store: &B,
        wal_id_itr: &AtomicUsize,
        wal_config: &WalWrapperConfig,
    ) -> Result<WrapData, MemTableError>
    where
        B::WriteCursor: Send + Sync + 'static,
    {
        self.wal.flush()?;
        let old_blob_id = self.wal.blob_id_ref().to_string();

        let mut skip_list = Arc::new(SkipList::default());
        std::mem::swap(&mut self.skip_list, &mut skip_list);
        let old_iter = <SkipListScanner as From<_>>::from(skip_list.clone());
        let id = wal_id_itr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut wal = WalWrapper::new(blob_store, id, wal_config.clone())?;
        std::mem::swap(&mut self.wal, &mut wal);
        let mut next = None;
        std::mem::swap(&mut self.next, &mut next);
        let compacted = self.compacted;
        let finalized = self.finalized;
        self.compacted = false;
        self.finalized = false;
        let other = Box::new(MemQueueEntry {
            skip_list,
            wal,
            next,
            compacted,
            finalized,
        });
        self.next = Some(other);
        Ok((old_blob_id, old_iter))
    }

    fn drop_wal_id(&mut self, wal_blob_id: &str) -> Result<(), MemTableError> {
        debug_assert_ne!(self.wal.blob_id_ref(), wal_blob_id);
        if let Some(next) = &mut self.next {
            if next.wal.blob_id_ref() == wal_blob_id {
                let mut replace = None;
                std::mem::swap(&mut self.next, &mut replace);
                self.next = replace;
                return Ok(());
            } else {
                return next.drop_wal_id(wal_blob_id);
            }
        }
        Ok(())
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<TombstoneValue> {
        if let Some(value) = self.skip_list.search(key) {
            let value = value.map(|v| unsafe { v.as_ref().unwrap().as_slice() });
            return Some(TombstoneValue::from(&value));
        }
        let mut entry_ref = if let Some(next) = &self.next {
            next
        } else {
            return None;
        };
        loop {
            if let Some(value) = entry_ref.skip_list.search(key) {
                let value = value.map(|v| unsafe { v.as_ref().unwrap().as_slice() });
                return Some(TombstoneValue::from(&value));
            }
            if let Some(next) = &entry_ref.next {
                entry_ref = next;
                continue;
            }
            return None;
        }
    }

    /// Returns true if the WAL is full and a rotation is required.
    pub(crate) fn set<T: TombstoneValueLike>(
        &mut self,
        key: &[u8],
        value: &T,
    ) -> Result<bool, WalError> {
        if self.finalized {
            return Err(WalError::FinalizedWal);
        }
        let rotation_required = self.wal.set(key, value)?;
        self.skip_list.insert(key, value.as_ref());
        Ok(rotation_required)
    }

    // TODO(t/1581): This should be called by
    // MemQueue::async_set.
    #[allow(dead_code)]
    pub(crate) async fn async_set<T: TombstoneValueLike>(
        &mut self,
        key: &[u8],
        value: &T,
    ) -> Result<(), WalError> {
        if self.finalized {
            return Err(WalError::FinalizedWal);
        }
        self.wal.async_set(key, value).await?;
        self.skip_list.insert(key, value.as_ref());
        Ok(())
    }

    pub(crate) fn iter(&self) -> MemTableEntryIterator {
        let inner = <SkipListScanner as From<_>>::from(self.skip_list.clone());
        MemTableEntryIterator { inner }
    }

    /// Mark this WAL as finalized. This happens before the WAL is compacted,
    /// and ensures that any lingering references to the entry cannot write
    /// new values.
    fn mark_finalized(&mut self) {
        self.finalized = true;
    }
}

pub(crate) struct MemTableEntryIterator {
    inner: SkipListScanner,
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
        let key = pair.key_ref();
        let value = pair.value_ref();
        let t_pair = TombstonePair::from((key.to_vec(), TombstoneValue::from(&value)));
        Some(Ok(t_pair))
    }
}

impl RangeSet for MemTableEntryIterator {
    fn from(mut self, key: &[u8]) -> Self {
        self.inner = self.inner.from(key);
        self
    }

    fn to(mut self, key: &[u8]) -> Self {
        self.inner = self.inner.to(key);
        self
    }
}

impl TombstoneIterator for MemTableEntryIterator {
    type Error = MemTableError;
}

pub(crate) struct MemQueue<B: BlobStore> {
    blob_store: B,
    wal_id: Arc<AtomicUsize>,
    wal_config: WalWrapperConfig,
    head: Arc<RwLock<MemQueueEntry<B::WriteCursor>>>,
}

// Required because derived Clone for parent structs requires
// the WriteCursor to be Clone.
impl<B: BlobStore> Clone for MemQueue<B> {
    fn clone(&self) -> Self {
        Self {
            blob_store: self.blob_store.clone(),
            wal_id: self.wal_id.clone(),
            wal_config: self.wal_config.clone(),
            head: self.head.clone(),
        }
    }
}

// Required because derived Debug for parent structs requires
// the WriteCursor to be Debug.
impl<B: BlobStore> std::fmt::Debug for MemQueue<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemQueue").finish()
    }
}

impl<B: BlobStore> MemQueue<B>
// where
//     B::WriteCursor: Send + Sync + 'static,
{
    pub(crate) fn new(blob_store: B, wal_config: WalWrapperConfig) -> Result<Self, MemTableError> {
        let skip_list = Arc::new(SkipList::default());
        let head = MemQueueEntry {
            wal: WalWrapper::new(&blob_store, 0, wal_config.clone())?,
            skip_list,
            next: None,
            compacted: false,
            finalized: false,
        };
        let head = Arc::new(RwLock::new(head));
        Ok(MemQueue {
            blob_store,
            wal_id: Arc::new(AtomicUsize::new(1)),
            wal_config,
            head,
        })
    }

    pub(crate) fn from_recovery(
        blob_store: B,
        wal: WalWrapper<B::WriteCursor>,
        next_wal_id: usize,
    ) -> MemQueue<B> {
        let wal_config = wal.wal_config_ref().clone();
        let skip_list = Arc::new(SkipList::default());
        let head = MemQueueEntry {
            wal,
            skip_list,
            next: None,
            compacted: false,
            finalized: false,
        };
        let head = Arc::new(RwLock::new(head));
        MemQueue {
            blob_store,
            wal_id: Arc::new(AtomicUsize::new(next_wal_id)),
            wal_config,
            head,
        }
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<TombstoneValue> {
        unlock!(self.head.read()).get(key)
    }

    /// Returns true if the WAL is full and a rotation is required.
    pub(crate) fn set<T: TombstoneValueLike>(
        &self,
        key: &[u8],
        value: &T,
    ) -> Result<bool, WalError> {
        unlock!(self.head.write()).set(key, value)
    }

    // TODO(t/1581): This should use MemQueueEntry::async_set and the function
    // should be marked as async. This cannot be done yet because it would
    // required awaiting while the head is locked which triggers a lint failure
    // (because it is a dumb thing to do).
    // The body should instead read something like the following but without
    // the lock:
    //
    // > unlock!(self.head.write()).async_set(key, value).await
    pub(crate) async fn async_set<T: TombstoneValueLike>(
        &self,
        key: &[u8],
        value: &T,
    ) -> Result<bool, WalError> {
        unlock!(self.head.write()).set(key, value)
    }

    pub(crate) fn rotate(&self) -> Result<WrapData, MemTableError> {
        let mut head = unlock!(self.head.write());
        (*head).mark_finalized();
        let wrap_data = (*head).wrap(&self.blob_store, &self.wal_id, &self.wal_config.clone())?;
        Ok(wrap_data)
    }

    pub(crate) fn record_index_change(&self, update: &ManifestUpdate) -> Result<(), MemTableError> {
        unlock!(self.head.write()).wal.record_index_change(update)?;
        Ok(())
    }

    pub(crate) fn recovery_mark_minor_compaction(
        &self,
        wal_blob_id: &str,
        sst_blob_ids: &[String],
    ) -> Result<(), MemTableError> {
        unlock!(self.head.write())
            .wal
            .recovery_mark_minor_compaction(wal_blob_id, sst_blob_ids)?;
        Ok(())
    }

    pub(crate) fn drop_wal_id(&self, old_wal_id: &str) -> Result<(), MemTableError> {
        unlock!(self.head.write()).drop_wal_id(old_wal_id)?;
        Ok(())
    }

    pub(crate) fn iter(&self) -> MergedHomogenousIter<MemTableEntryIterator> {
        let entry = unlock!(self.head.read());
        let mut entry_ref: &MemQueueEntry<B::WriteCursor> = &entry;
        let mut iters = vec![entry.iter()];
        loop {
            if let Some(next) = &entry_ref.next {
                entry_ref = next;
                iters.push(entry.iter());
                continue;
            }
            break;
        }
        MergedHomogenousIter::new(iters.into_iter())
    }

    /// WARNING: Do not use the queue after a successful call to this function.
    pub(crate) fn close(&mut self) -> Result<(), MemTableError> {
        unlock!(self.head.write()).wal.flush()?;
        Ok(())
    }
}
