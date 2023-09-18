use std::collections::HashMap;
use std::fmt::Debug;
use std::iter::zip;
use std::sync::mpsc::{Receiver, Sender};
use std::thread::sleep;
use std::time::Duration;

use crate::kv::{
    JoinedIter, MergedHomogenousIter, TombstonePair, TombstonePairLike, TryTombstoneScanner,
};
use crate::logging::DefaultLogger;
use crate::logging::{error, info};
use crate::mem_table::MemTableCompactorIterator;
use crate::sst::block_cache::StorageWrapper;
use crate::sst::common::SstError;
use crate::sst::reader::{SstReader, SstScanner};
use crate::sst::writer::{SstWriteSettings, SstWriter};
use crate::storage::blob_store::{BlobStore, FileError};
use crate::tablet::{CompactOrder, TabletSstStore, TaskCategory};

#[derive(Debug, PartialEq)]
pub(crate) enum CompactorError {
    // list of blob ids that could not be read
    FailedToObtainReader(Vec<String>),
    NothingToCompact,
    // an underlying error occurred with the blob store
    BlobStoreError(String),
    DeletionError,
}

impl From<FileError> for CompactorError {
    fn from(err: FileError) -> Self {
        CompactorError::BlobStoreError(format!("blob store error: {:?}", err))
    }
}

impl From<SstError> for CompactorError {
    fn from(err: SstError) -> Self {
        CompactorError::BlobStoreError(format!("blob store error (via SST error): {:?}", err))
    }
}

impl From<CompactorError> for String {
    fn from(err: CompactorError) -> Self {
        format!("CompactorError: {:?}", err)
    }
}

#[derive(Debug)]
pub(crate) enum CompactorMessage {
    /// The compactor should stop.
    Shutdown,
    /// Run a minor compaction.
    MinorCompact {
        iter: MemTableCompactorIterator,
        done_tx: Sender<Result<String, CompactorError>>,
    },
    // Run a major compaction.
    MajorCompaction {
        done_tx: Sender<Result<(), CompactorError>>,
    },
    // Deletes a set of blobs.
    GarbageCollect {
        blob_ids: Vec<String>,
        done_tx: Option<Sender<Result<(), CompactorError>>>,
    },
    // Garbage collects a single blob.
    DeleteBlob(String),
}

// NOTE: The fields in this struct are only read by the debug struct, which is
// not counted in dead code analysis.
#[derive(Debug)]
struct DebugOrder {
    #[allow(dead_code)]
    order_type: String,
    #[allow(dead_code)]
    minor_ssts: Option<Vec<String>>,
    #[allow(dead_code)]
    levels: Option<HashMap<usize, Vec<String>>>,
}

/// Compactor encapsulates the state of the compactions.
/// The Compactor is a data structure that will actually perform
/// compactions for the embedded jupiter instance. For each
/// jupiter instance, the JupiterDb will start compactors on
/// start up. (Right now there is only one compactor being started,
/// but they may be booted up with more in the future.) These
/// compactors are passed a pointer to the manifest. The compactor
/// will ask the manifest to recommend a compaction. The manifest
/// will return  a list of SstMetadata. The compactor uses
/// this list to read in all of the SSTs represented and compact
/// them to a new file. If the compaction succeeds, then the
/// compactor call the finished compaction function of the manifest.
/// This function takes as an argument the SstMetadata that were
/// compacted, and the output SstMetadata.
/// If the finished compaction request is successful, then the
/// compactor loops. Otherwise, it cleans up all the new files it
/// wrote. A finished compaction call may not be successful if
/// another compactor has already compacted one of the input files.
/// In this case the compactors work was wasted.
pub(crate) struct Compactor<BC: StorageWrapper, T: TabletSstStore> {
    storage_wrapper: BC,
    /// a pointer to the manifest that is being compacted
    sst_store: T,
    /// false if the compactor should stop running
    admin_rx: Receiver<CompactorMessage>,
    garbage_queue: Vec<String>,
    /// passed to new `SstReader` instances upon creation
    compactor_tx: Sender<CompactorMessage>,
    logger: DefaultLogger,
}

impl<BC: StorageWrapper, T: TabletSstStore<BC = BC>> Compactor<BC, T> {
    pub(crate) fn new(
        storage_wrapper: BC,
        sst_store: T,
        compactor_tx: Sender<CompactorMessage>,
        admin_rx: Receiver<CompactorMessage>,
    ) -> Self {
        // TODO(t/1380): This should be a reference to a logger that is passed
        // as an argument when the compactor is created.
        let logger = DefaultLogger::default();
        Compactor {
            storage_wrapper,
            sst_store,
            admin_rx,
            garbage_queue: Vec::new(),
            compactor_tx,
            logger,
        }
    }

    #[cfg(test)]
    pub(crate) fn one_off(storage_wrapper: BC, sst_store: T) -> Self {
        use std::sync::mpsc::channel;

        let (compactor_tx, compactor_rx) = channel::<CompactorMessage>();
        Compactor::new(storage_wrapper, sst_store, compactor_tx, compactor_rx)
    }

    fn tombstone_iter(
        sst_readers: &[SstReader<BC>],
    ) -> Result<MergedHomogenousIter<SstScanner<BC>>, CompactorError> {
        let results = sst_readers
            .iter()
            .map(|reader| (reader.blob_id_ref().to_string(), reader.try_scan()))
            .collect::<Vec<_>>();
        let mut failed_blob_ids = Vec::with_capacity(sst_readers.len());
        for (blob_id, result) in results.iter() {
            if result.is_err() {
                failed_blob_ids.push(blob_id.to_string());
            }
        }
        if !failed_blob_ids.is_empty() {
            return Err(CompactorError::FailedToObtainReader(failed_blob_ids));
        }

        let scanners = results.into_iter().filter_map(|(_, result)| result.ok());
        let iter = MergedHomogenousIter::new(scanners);
        Ok(iter)
    }

    fn fill_minor_order(
        &self,
        minor_sst_readers: &[SstReader<BC>],
        l0_sst_readers: &[SstReader<BC>],
        write_settings: &SstWriteSettings,
    ) -> Result<Vec<SstReader<BC>>, CompactorError> {
        let minor_iter = Compactor::<BC, T>::tombstone_iter(minor_sst_readers)?;
        let l0_iter = Compactor::<BC, T>::tombstone_iter(l0_sst_readers)?;
        let merged_iter = JoinedIter::new(minor_iter, l0_iter);
        let debug_data = DebugOrder {
            order_type: "minor".to_string(),
            minor_ssts: Some(
                minor_sst_readers
                    .iter()
                    .map(|r| r.blob_id_ref().to_string())
                    .collect::<Vec<_>>(),
            ),
            levels: Some(
                vec![(
                    0_usize,
                    l0_sst_readers
                        .iter()
                        .map(|r| r.blob_id_ref().to_string())
                        .collect::<Vec<_>>(),
                )]
                .into_iter()
                .collect::<HashMap<_, _>>(),
            ),
        };
        let debug_message = format!("{:?}", debug_data);
        self.compact(&debug_message, merged_iter, write_settings)
    }

    fn fill_regular_order(
        &self,
        lo_sst_readers: &[SstReader<BC>],
        hi_sst_readers: &[SstReader<BC>],
        target_level: usize,
        write_settings: &SstWriteSettings,
    ) -> Result<Vec<SstReader<BC>>, CompactorError> {
        let lo_iter = Compactor::<BC, T>::tombstone_iter(lo_sst_readers)?;
        let hi_iter = Compactor::<BC, T>::tombstone_iter(hi_sst_readers)?;
        let merged_iter = JoinedIter::new(lo_iter, hi_iter);
        let debug_level_map = zip(
            [target_level - 1, target_level].iter(),
            [lo_sst_readers, hi_sst_readers].iter(),
        )
        .map(|(level, sst_readers)| {
            (
                *level,
                sst_readers
                    .iter()
                    .map(|r| r.blob_id_ref().to_string())
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<HashMap<_, _>>();
        let debug_data = DebugOrder {
            order_type: "regular".to_string(),
            minor_ssts: None,
            levels: Some(debug_level_map),
        };
        let debug_message = format!("{:?}", debug_data);
        self.compact(&debug_message, merged_iter, write_settings)
    }

    fn fill_major_order(
        &self,
        minor_sst_readers: &[SstReader<BC>],
        level_sst_readers: &[Vec<SstReader<BC>>],
        write_settings: &SstWriteSettings,
    ) -> Result<Vec<SstReader<BC>>, CompactorError> {
        let minor_iter = Compactor::<BC, T>::tombstone_iter(minor_sst_readers)?;
        let level_iters = level_sst_readers
            .iter()
            .map(|level_sst_readers| Compactor::<BC, T>::tombstone_iter(level_sst_readers))
            .collect::<Result<Vec<_>, _>>()?;
        let level_iter = MergedHomogenousIter::new(level_iters.into_iter());
        let merged_iter = JoinedIter::new(minor_iter, level_iter);
        let debug_level_map = level_sst_readers
            .iter()
            .enumerate()
            .filter(|(_, sst_readers)| !sst_readers.is_empty())
            .map(|(level, sst_readers)| {
                (
                    level,
                    sst_readers
                        .iter()
                        .map(|r| r.blob_id_ref().to_string())
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<HashMap<_, _>>();
        let debug_data = DebugOrder {
            order_type: "major".to_string(),
            minor_ssts: Some(
                minor_sst_readers
                    .iter()
                    .map(|r| r.blob_id_ref().to_string())
                    .collect::<Vec<_>>(),
            ),
            levels: Some(debug_level_map),
        };
        let debug_message = format!("{:?}", debug_data);
        self.compact(&debug_message, merged_iter, write_settings)
    }

    /// # Returns
    ///
    /// The new `SstReader` instances if successful.
    fn compact<E, I: Iterator<Item = Result<TombstonePair, E>>>(
        &self,
        debug_message: &str,
        pairs: I,
        settings: &SstWriteSettings,
    ) -> Result<Vec<SstReader<BC>>, CompactorError>
    where
        SstError: From<E>,
    {
        let blob_store = self.storage_wrapper.blob_store_ref().clone();

        let writer = SstWriter::new(blob_store, settings.clone());
        let blob_ids = writer.write_all(pairs)?;
        let readers = blob_ids
            .iter()
            .map(|blob_id| {
                SstReader::new(
                    self.storage_wrapper.clone(),
                    blob_id,
                    self.compactor_tx.clone(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        info!(
            &self.logger,
            "compacted to new blobs {:?} from original order {}", blob_ids, debug_message,
        );
        Ok(readers)
    }

    fn compact_result(
        &self,
        order: &CompactOrder<BC>,
    ) -> Result<Vec<SstReader<BC>>, CompactorError> {
        match &order {
            CompactOrder::Minor {
                minor_sst_readers,
                l0_sst_readers,
                write_settings,
            } => self.fill_minor_order(minor_sst_readers, l0_sst_readers, write_settings),
            CompactOrder::Regular {
                lo_sst_readers,
                hi_sst_readers,
                target_level_no,
                write_settings,
            } => self.fill_regular_order(
                lo_sst_readers,
                hi_sst_readers,
                *target_level_no,
                write_settings,
            ),
            CompactOrder::Major {
                minor_sst_readers,
                level_sst_readers,
                write_settings,
                ..
            } => self.fill_major_order(minor_sst_readers, level_sst_readers, write_settings),
        }
    }

    pub(crate) fn run(mut self) -> Result<(), CompactorError> {
        let blob_store = self.storage_wrapper.blob_store_ref().clone();
        let short_time = Duration::from_millis(500);
        loop {
            sleep(short_time);
            if let Ok(message) = self.admin_rx.try_recv() {
                match message {
                    CompactorMessage::Shutdown => {
                        return Ok(());
                    }
                    CompactorMessage::MinorCompact { iter, done_tx } => {
                        done_tx.send(self.minor_compact(iter)).unwrap();
                    }
                    CompactorMessage::MajorCompaction { done_tx } => {
                        if let Some(order) =
                            self.sst_store.request_task(&TaskCategory::MajorCompaction)
                        {
                            match self.compact_result(&order) {
                                Ok(new_sst_readers) => {
                                    self.sst_store
                                        .handle_finished_task(&order, new_sst_readers.into_iter());
                                    done_tx.send(Ok(())).unwrap();
                                }
                                Err(err) => {
                                    done_tx.send(Err(err)).unwrap();
                                }
                            }
                        }
                    }
                    CompactorMessage::GarbageCollect { blob_ids, done_tx } => {
                        let result = self.inner_garbage_collect(&blob_store, &blob_ids);
                        if let Some(done_tx) = done_tx {
                            done_tx
                                .send(match result {
                                    Ok(_) => Ok(()),
                                    Err(err) => Err(err),
                                })
                                .unwrap();
                        }
                    }
                    CompactorMessage::DeleteBlob(blob_id) => {
                        let blob_ids = vec![blob_id];
                        _ = self.inner_garbage_collect(&blob_store, &blob_ids);
                    }
                }
                continue;
            }

            if let Some(order) = self.sst_store.request_task(&TaskCategory::Any) {
                if let Ok(new_sst_readers) = self.compact_result(&order) {
                    self.sst_store
                        .handle_finished_task(&order, new_sst_readers.into_iter());
                    continue;
                }
            }

            let _ = self.inner_garbage_collect(&blob_store, &[]);
        }
    }

    fn inner_garbage_collect(
        &mut self,
        blob_store: &BC::B,
        blob_ids: &[String],
    ) -> Result<(), CompactorError> {
        let old_gq = self.garbage_queue.clone().into_iter().map(|s| (true, s));
        let new_gq = blob_ids.iter().map(|s| (false, s.to_string()));
        self.garbage_queue = Vec::with_capacity(old_gq.len());
        let mut err_found = false;
        for (is_old, blob_id) in old_gq.chain(new_gq) {
            match blob_store.delete(&blob_id) {
                Ok(_) => {
                    if is_old {
                        info!(&self.logger, "garbage collected old blob: {}", blob_id);
                    }
                }
                Err(_) => {
                    if !is_old {
                        err_found = true;
                    }
                    error!(
                        &self.logger,
                        "failed to garbage collect old blob, requeuing for later: {}", blob_id
                    );
                    self.garbage_queue.push(blob_id);
                }
            }
        }
        if err_found {
            return Err(CompactorError::DeletionError);
        }
        Ok(())
    }

    /// Create a new minor SST file from a given key-value list and send it to
    /// the SST store.
    ///
    /// NOTE: The key-value pairs must be lexically sorted by key from lowest to
    /// highest.
    ///
    /// # Arguments
    ///
    /// - iter: An iterator over the key-value pairs to be written to the SST
    ///
    /// # Returns
    ///
    /// The blob ID of the newly created SST file.
    pub(crate) fn minor_compact<L: TombstonePairLike, I: Iterator<Item = L>>(
        &mut self,
        iter: I,
    ) -> Result<String, CompactorError> {
        let blob_store = self.storage_wrapper.blob_store_ref().clone();

        let writer = SstWriter::new(
            blob_store,
            self.sst_store
                .write_settings_ref()
                .clone()
                .keep_tombstones()
                .set_writing_minor_sst(true),
        );
        let pairs = iter.map(|pair| Ok::<L, ()>(pair));
        let mut blob_ids = writer.write_all(pairs)?;
        debug_assert_eq!(blob_ids.len(), 1);
        let blob_id = blob_ids.pop().unwrap();
        let reader = SstReader::new(
            self.storage_wrapper.clone(),
            &blob_id,
            self.compactor_tx.clone(),
        )?;
        self.sst_store.add_minor_sst(reader);
        Ok(blob_id)
    }
}

#[cfg(test)]
mod test {
    use std::fmt::Debug;
    use std::sync::mpsc::channel;

    use super::*;
    use crate::compactor::{Compactor, CompactorMessage};
    use crate::concurrent_skip_list::ConcurrentSkipList;
    use crate::kv::{TombstonePair, TombstoneValue};
    use crate::sst::block_cache::{BlockCache, NoBlockCache, SimpleStorageWrapper, StorageWrapper};
    use crate::sst::writer::SstWriteSettings;
    use crate::storage::blob_store::InMemoryBlobStore;
    use crate::tablet::SmartTablet;

    fn md_data(lo: usize, hi: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut ret = Vec::new();
        for k in lo..hi {
            let l = k as u64;
            let key = u64::to_be_bytes(l).to_vec();
            let value = u64::to_be_bytes((l * l) % 17).to_vec();
            ret.push((key, value));
        }
        ret
    }

    fn md_n_data(n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        if n == 1 {
            return md_data(10, 40);
        }
        if n == 2 {
            return md_data(50, 100);
        }
        if n == 3 {
            return md_data(0, 110);
        }
        panic!()
    }

    type BlobId = String;

    fn write_minor_md<BC: StorageWrapper>(
        storage_wrapper: &BC,
        vec: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<BlobId, String> {
        let blob_store = storage_wrapper.blob_store_ref().clone();
        let sst_writer = SstWriter::new(
            blob_store,
            SstWriteSettings::default().set_writing_minor_sst(true),
        );
        let blob_ids = sst_writer.write_all(vec.iter().map(|(key, value)| {
            Result::<TombstonePair, SstError>::Ok(TombstonePair::new(key.clone(), value.clone()))
        }))?;
        assert_eq!(blob_ids.len(), 1);
        Ok(blob_ids[0].clone())
    }

    fn check_written<BC: StorageWrapper, E: Debug, I: Iterator<Item = Result<TombstonePair, E>>>(
        _storage_wrapper: &BC,
        vec: &Vec<(Vec<u8>, Vec<u8>)>,
        pairs: I,
    ) -> Result<(), String> {
        for (idx, result) in pairs.enumerate() {
            if idx == vec.len() {
                assert!(result.is_err());
                break;
            }
            let pair = result.unwrap();
            assert_eq!(pair.key_ref(), &vec[idx].0);
            assert_eq!(pair.value_ref().as_ref().unwrap(), &vec[idx].1);
        }
        Ok(())
    }

    fn write_starting_sst_vec<BC: StorageWrapper>(
        storage_wrapper: &BC,
        compactor_tx: Sender<CompactorMessage>,
    ) -> Result<Vec<SstReader<BC>>, String> {
        let mut readers = Vec::with_capacity(3);

        let data = md_n_data(1);
        let path_1 = write_minor_md(storage_wrapper, &data).unwrap();
        readers.push(SstReader::new(
            storage_wrapper.clone(),
            &path_1,
            compactor_tx.clone(),
        )?);
        check_written(storage_wrapper, &data, readers[0].try_scan().unwrap()).unwrap();

        let data = md_n_data(2);
        let path_2 = write_minor_md(storage_wrapper, &data).unwrap();
        readers.push(SstReader::new(
            storage_wrapper.clone(),
            &path_2,
            compactor_tx.clone(),
        )?);
        check_written(storage_wrapper, &data, readers[1].try_scan().unwrap()).unwrap();

        let data = md_n_data(3);
        let path_3 = write_minor_md(storage_wrapper, &data).unwrap();
        readers.push(SstReader::new(
            storage_wrapper.clone(),
            &path_3,
            compactor_tx.clone(),
        )?);
        check_written(storage_wrapper, &data, readers[2].try_scan().unwrap()).unwrap();

        Ok(readers)
    }

    #[test]
    fn test_compactor_compact() {
        let blob_store = InMemoryBlobStore::new();
        // TODO(t/1387): This test should be run with the LRU block cache
        // when possible.
        type TestOnlyStorageWrapper = SimpleStorageWrapper<InMemoryBlobStore, NoBlockCache>;
        let storage_wrapper =
            SimpleStorageWrapper::from((blob_store, NoBlockCache::with_capacity(0)));
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

        let sst_readers = write_starting_sst_vec(&storage_wrapper, compactor_tx.clone()).unwrap();
        let pairs = sst_readers.iter().map(|reader| reader.try_scan().unwrap());
        let pairs = MergedHomogenousIter::new(pairs);
        let new_sst_readers = compactor
            .compact("'my blobs!'", pairs, &SstWriteSettings::default())
            .unwrap();

        let skip_list: ConcurrentSkipList<Vec<u8>, TombstoneValue> = ConcurrentSkipList::new();
        for in_data in (1..4).map(md_n_data).rev() {
            for (key, value) in in_data {
                skip_list.set(key, TombstoneValue::Value(value));
            }
        }
        let out_data = skip_list
            .iter()
            .map(|pair| {
                let key = pair.key_ref();
                let value = pair.value_ref();
                (key.clone(), (*value).clone().as_ref().unwrap().clone())
            })
            .collect::<Vec<_>>();

        let new_iters = new_sst_readers
            .iter()
            .map(|reader| reader.try_scan().unwrap());
        let pairs = MergedHomogenousIter::new(new_iters);
        assert!(check_written(&storage_wrapper, &out_data, pairs).is_ok())
    }
}
