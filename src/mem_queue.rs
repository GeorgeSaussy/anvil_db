use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};

use crate::concurrent_skip_list::{ConcurrentSkipList, ConcurrentSkipListScanner};
use crate::kv::{
    MergedHomogenousIter, TombstoneIterator, TombstonePair, TombstoneValue, TombstoneValueLike,
};
use crate::logging::Logger;
use crate::mem_table::MemTableError;
use crate::storage::blob_store::BlobStore;
use crate::wal::wal_entry::WalError;
use crate::wal::wal_wrapper::{WalWrapper, WalWrapperConfig};

type WalBlobId = String;
type WrapData<B> = (
    WalBlobId,
    Arc<RwLock<MemQueueEntry<B>>>,
    ConcurrentSkipListScanner<Vec<u8>, TombstoneValue>,
);

#[derive(Clone, Debug)]
pub(crate) struct MemQueueEntry<B: BlobStore> {
    skip_list: ConcurrentSkipList<Vec<u8>, TombstoneValue>,
    wal: WalWrapper<B>,
    next: Option<Arc<RwLock<MemQueueEntry<B>>>>,
    compacted: bool,
}

impl<B: BlobStore> MemQueueEntry<B>
where
    B::WC: Send + Sync + 'static,
{
    pub(crate) fn wrap(
        blob_store: B,
        id: usize,
        wal_config: WalWrapperConfig,
        other: MemQueueEntry<B>,
    ) -> Result<(MemQueueEntry<B>, WrapData<B>), MemTableError> {
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
        self.wal.set(key, value)?;
        self.skip_list.set(key, TombstoneValue::from(value));
        Ok(())
    }

    pub(crate) fn iter(&self) -> MemTableEntryIterator {
        MemTableEntryIterator {
            inner: self.skip_list.iter(),
        }
    }
}

pub(crate) struct MemTableEntryIterator {
    inner: ConcurrentSkipListScanner<Vec<u8>, TombstoneValue>,
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

impl TombstoneIterator for MemTableEntryIterator {
    type E = MemTableError;

    fn from(self, _key: &[u8]) -> Self {
        // TODO(t/1373): Implement scans.
        todo!()
    }

    fn to(self, _key: &[u8]) -> Self {
        // TODO(t/1373): Implement scans.
        todo!()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MemQueue<B: BlobStore, L: Logger> {
    wal_id: Arc<AtomicUsize>,
    blob_store: B,
    wal_config: WalWrapperConfig,
    head: Arc<RwLock<MemQueueEntry<B>>>,
    // TODO(t/1380): Improve logging.
    #[allow(dead_code)]
    logger: L,
}

impl<B: BlobStore, L: Logger> MemQueue<B, L>
where
    B::WC: Send + Sync + 'static,
{
    pub(crate) fn new(
        blob_store: B,
        wal_config: WalWrapperConfig,
        logger: L,
    ) -> Result<Self, MemTableError> {
        let skip_list = ConcurrentSkipList::new();
        let head = MemQueueEntry {
            wal: WalWrapper::new(blob_store.clone(), wal_config.clone().with_next_wal_id(0))?,
            skip_list,
            next: None,
            compacted: false,
        };
        let head = Arc::new(RwLock::new(head));
        Ok(MemQueue {
            wal_id: Arc::new(AtomicUsize::new(1)),
            blob_store,
            wal_config,
            head,
            logger,
        })
    }

    pub(crate) fn from_recovery(
        blob_store: B,
        wal: WalWrapper<B>,
        next_wal_id: usize,
        logger: L,
    ) -> MemQueue<B, L> {
        let wal_config = wal.wal_config_ref().clone();
        let skip_list = ConcurrentSkipList::new();
        let head = MemQueueEntry {
            wal,
            skip_list,
            next: None,
            compacted: false,
        };
        let head = Arc::new(RwLock::new(head));
        MemQueue {
            wal_id: Arc::new(AtomicUsize::new(next_wal_id)),
            blob_store,
            wal_config,
            head,
            logger,
        }
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<TombstoneValue> {
        self.head.read().unwrap().get(key)
    }

    pub(crate) fn set<T: TombstoneValueLike>(&self, key: &[u8], value: &T) -> Result<(), WalError> {
        self.head.read().unwrap().set(key, value)
    }

    pub(crate) fn rotate(&self) -> Result<WrapData<B>, MemTableError> {
        let mut head = self.head.write().unwrap();
        let wal_id = self
            .wal_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let (new_head, wrap_data) = MemQueueEntry::wrap(
            self.blob_store.clone(),
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

    pub(crate) fn maybe_drop_arc(&self, arc: Arc<RwLock<MemQueueEntry<B>>>) {
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
