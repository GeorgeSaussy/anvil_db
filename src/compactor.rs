use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::sync::mpsc::{Receiver, Sender};
use std::thread::sleep;
use std::time::Duration;

use crate::context::Context;
use crate::kv::{
    JoinedIter, MergedHomogenousIter, TombstonePair, TombstonePairLike, TryTombstoneScanner,
};
use crate::logging::{debug, error, info};
use crate::manifest::{
    LoadedLevelUpdateInfo, LoadedManifestUpdate, ManifestOperation, ManifestUpdateType,
};
use crate::mem_table::{MemTable, MemTableCompactorIterator, MemTableError};
use crate::sst::common::SstError;
use crate::sst::reader::{SstReader, SstScanner};
use crate::sst::writer::{NameGenerator, SstWriteSettings, SstWriter};
use crate::storage::blob_store::{BlobStore, BlobStoreError};
use crate::tablet::Tablet;

#[derive(Debug)]
pub(crate) enum CompactorError {
    // list of blob ids that could not be read
    FailedToObtainReader(Vec<String>),
    // TODO(nit): get rid of `NothingToCompact`
    NothingToCompact,
    // an underlying error occurred with the blob store
    BlobStoreError(BlobStoreError),
    SstError(SstError),
    DeletionError,
    MemTableError(Box<MemTableError>),
}

impl Display for CompactorError {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            CompactorError::FailedToObtainReader(blob_ids) => {
                write!(f, "failed to obtain reader for blobs: {blob_ids:?}")
            }
            CompactorError::NothingToCompact => {
                write!(f, "nothing to compact")
            }
            CompactorError::BlobStoreError(err) => {
                write!(f, "blob store error: {err}")
            }
            CompactorError::SstError(err) => {
                write!(f, "sst error: {err}")
            }
            CompactorError::DeletionError => {
                write!(f, "deletion error")
            }
            CompactorError::MemTableError(mem_table_error) => {
                write!(
                    f,
                    "failed to record index change in wal {mem_table_error:?}"
                )
            }
        }
    }
}

impl Error for CompactorError {}

impl From<BlobStoreError> for CompactorError {
    fn from(err: BlobStoreError) -> Self {
        CompactorError::BlobStoreError(err)
    }
}

impl From<SstError> for CompactorError {
    fn from(err: SstError) -> Self {
        match err {
            SstError::EmptySst(_) => CompactorError::NothingToCompact,
            _ => CompactorError::SstError(err),
        }
    }
}

impl From<MemTableError> for CompactorError {
    fn from(err: MemTableError) -> Self {
        CompactorError::MemTableError(Box::new(err))
    }
}

impl From<CompactorError> for String {
    fn from(err: CompactorError) -> Self {
        format!("CompactorError: {err:?}")
    }
}

impl ManifestOperation {
    pub(crate) fn minor_merge_order<
        I1: Iterator<Item = SstReader>,
        I2: Iterator<Item = SstReader>,
    >(
        minor_sst_readers: I1,
        l0_sst_readers: I2,
        write_settings: SstWriteSettings,
    ) -> Self {
        let minor_sst_readers = minor_sst_readers.map(|r| vec![r]).collect();
        let l0_sst_readers = l0_sst_readers.collect();
        ManifestOperation::MinorMergeCompaction {
            minor_sst_readers,
            l0_sst_readers,
            write_settings,
        }
    }

    pub(crate) fn major_order<
        I1: Iterator<Item = SstReader>,
        I2: Iterator<Item = SstReader>,
        I3: Iterator<Item = (usize, I2)>,
    >(
        minor_sst_readers: I1,
        level_sst_readers: I3,
        write_settings: SstWriteSettings,
        done_tx: Option<Sender<Result<Vec<String>, CompactorError>>>,
    ) -> Self {
        let minor_sst_readers = minor_sst_readers.map(|r| vec![r]).collect();
        let level_sst_readers = level_sst_readers
            .map(|(level, readers)| (level, readers.collect()))
            .collect();

        ManifestOperation::MajorCompaction {
            minor_sst_readers,
            level_sst_readers,
            write_settings,
            done_tx,
        }
    }

    pub(crate) fn merge_order(
        level_sst_readers: Vec<(usize, Vec<SstReader>)>,
        write_settings: SstWriteSettings,
    ) -> Self {
        ManifestOperation::MergeCompaction {
            write_settings,
            level_sst_readers,
        }
    }
}

#[derive(Debug)]
pub(crate) enum CompactorOrder {
    /// The compactor should stop.
    Shutdown {
        done_tx: Sender<()>,
    },
    CompactorOrder(ManifestOperation),
}

impl CompactorOrder {
    pub(crate) fn gc_order(
        blob_ids: Vec<String>,
        done_tx: Option<Sender<Result<Vec<String>, CompactorError>>>,
    ) -> Self {
        CompactorOrder::CompactorOrder(ManifestOperation::GarbageCollect { blob_ids, done_tx })
    }

    pub(crate) fn minor_order(
        iter: MemTableCompactorIterator,
        wal_blob_id: String,
        done_tx: Sender<Result<Vec<String>, CompactorError>>,
    ) -> Self {
        CompactorOrder::CompactorOrder(ManifestOperation::MinorCompact {
            iter,
            wal_blob_id,
            done_tx,
        })
    }
}

/// NOTE: The fields in [DebugOrder] are only read by the debug struct, which is
/// not counted in dead code analysis.
#[derive(Debug)]
struct DebugOrder {
    #[allow(dead_code)]
    order_type: String,
    #[allow(dead_code)]
    minor_ssts: Option<Vec<String>>,
    #[allow(dead_code)]
    levels: Option<HashMap<usize, Vec<String>>>,
}

pub(crate) struct CompactorSettings {
    target_block_size: usize,
    target_keys_per_block: usize,
}

impl Default for CompactorSettings {
    fn default() -> Self {
        let target_block_size = 128 * 1024 * 1024;
        let target_keys_per_block = target_block_size / 32;
        Self {
            target_block_size,
            target_keys_per_block,
        }
    }
}

impl CompactorSettings {
    pub(crate) fn with_target_block_size(mut self, target_block_size: usize) -> Self {
        self.target_block_size = target_block_size;
        self.target_keys_per_block = target_block_size / 32;
        self
    }

    pub(crate) fn apply(&self, write_settings: SstWriteSettings) -> SstWriteSettings {
        write_settings
            .with_target_block_size(self.target_block_size)
            .with_target_keys_per_block(self.target_keys_per_block)
    }
}

type MaybeDoneTx = Option<Sender<Result<Vec<String>, CompactorError>>>;

/// The [Compactor] will ask the [Tablet] to recommend a compaction.
/// The [Tablet] returns a [ManifestOperation] that contains a list of
/// [SstReader]s that are to be compacted.
/// If the compaction succeeds, then the
/// compactor notifies the [Tablet] and the WAL.
///
/// TODO(t/1336): [Compactor] should use async job scheduling.
pub(crate) struct Compactor<Ctx: Context> {
    /// shared context
    ctx: Ctx,
    /// a pointer to the manifest that is being compacted
    sst_store: Tablet,
    /// common settings for compactions
    settings: CompactorSettings,
    /// false if the compactor should stop running
    admin_rx: Receiver<CompactorOrder>,
    garbage_queue: Vec<String>,
    /// passed to new `SstReader` instances upon creation
    compactor_tx: Sender<CompactorOrder>,
    /// used to quickly generate blob names
    gen: NameGenerator,
    /// the name of the compactor, based on the timestamp when it starts
    name: String,
    /// a copy of the mem_table to record compaction and garbage collection
    /// records in the WAL
    mem_table: MemTable<Ctx>,
}

impl<Ctx: Context + 'static> Compactor<Ctx> {
    pub(crate) fn new(
        ctx: Ctx,
        sst_store: Tablet,
        settings: CompactorSettings,
        compactor_tx: Sender<CompactorOrder>,
        admin_rx: Receiver<CompactorOrder>,
        compactor_idx: usize,
        mem_table: MemTable<Ctx>,
    ) -> Self {
        let gen = NameGenerator::new(compactor_idx);
        let name = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            Ok(t) => format!("cptr-{}", t.as_nanos() % 1_000),
            Err(_) => "cptr-tik-tok".to_string(),
        };
        Compactor {
            ctx,
            sst_store,
            settings,
            admin_rx,
            garbage_queue: Vec::new(),
            compactor_tx,
            gen,
            name,
            mem_table,
        }
    }

    pub(crate) fn run(mut self) -> Result<(), CompactorError>
    where
        <<Ctx as Context>::BlobStore as BlobStore>::WriteCursor: Send + Sync + 'static,
    {
        let ctx = self.ctx.clone();
        info!(ctx.logger(), "compactor {} starting", self.name);
        let short_time = Duration::from_millis(500);
        loop {
            sleep(short_time);
            if let Ok(message) = self.admin_rx.try_recv() {
                match message {
                    CompactorOrder::Shutdown { done_tx } => {
                        info!(ctx.logger(), "compactor {} shutting down", self.name);
                        debug!(
                            ctx.logger(),
                            "compactor {} shutting down with sst_store {:?}",
                            self.name,
                            self.sst_store
                        );
                        done_tx.send(()).unwrap();
                        return Ok(());
                    }
                    CompactorOrder::CompactorOrder(order) => {
                        self.handle_compactor_order(&ctx, order);
                        continue;
                    }
                }
            }

            if let Some(order) = self.sst_store.request_task() {
                self.handle_compactor_order(&ctx, order);
                continue;
            }

            let _ = self.inner_garbage_collect(&ctx, &[]);
        }
    }

    fn handle_compactor_order(&mut self, ctx: &Ctx, order: ManifestOperation)
    where
        <<Ctx as Context>::BlobStore as BlobStore>::WriteCursor: Send + Sync + 'static,
    {
        let mut done_tx: MaybeDoneTx = None;
        let update = match self.update_blobs(ctx, order, &mut done_tx) {
            Ok(update) => update,
            Err(err) => {
                if let Some(done_tx) = done_tx {
                    let _ = done_tx.send(Err(err));
                }
                return;
            }
        };

        // The change must be recorded in the WAL before the SST index can be updated.
        let logical_update = update.logical_update();
        let result = match self.mem_table.record_index_change(&logical_update) {
            Ok(_) => {
                info!(
                    ctx.logger(),
                    "compactor {} updated index {:?} with update {update:?}",
                    self.name,
                    self.sst_store,
                );
                // This cannot fail because it is an in-memory operation.
                self.sst_store.update_index(&update);
                Ok(Vec::new())
            }
            Err(err) => Err(err.into()),
        };

        if let Some(done_tx) = done_tx {
            let _ = done_tx.send(result);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn update_blobs(
        &mut self,
        ctx: &Ctx,
        order: ManifestOperation,
        done_tx: &mut MaybeDoneTx,
    ) -> Result<LoadedManifestUpdate, CompactorError> {
        match order {
            ManifestOperation::MinorCompact {
                iter,
                wal_blob_id,
                done_tx: inner_done_tx,
            } => {
                *done_tx = Some(inner_done_tx);
                let sst_write_settings = CompactorSettings::default().apply(
                    self.sst_store
                        .write_settings_ref()
                        .clone()
                        .keep_tombstones()
                        .set_writing_minor_sst(true),
                );
                let mut update = LoadedManifestUpdate::new(ManifestUpdateType::Minor);
                let new_minors =
                    self.fill_minor_order(ctx, &wal_blob_id, iter, sst_write_settings)?;
                update.new_minor_ssts = vec![new_minors];
                update.compacted_wal_ids = vec![wal_blob_id];
                Ok(update)
            }
            ManifestOperation::MajorCompaction {
                minor_sst_readers: inner_minor_sst_readers,
                level_sst_readers: inner_level_sst_readers,
                write_settings,
                done_tx: inner_done_tx,
            } => {
                *done_tx = inner_done_tx;
                let minor_sst_readers = inner_minor_sst_readers;
                let level_sst_readers = inner_level_sst_readers;
                let sst_write_settings = self.settings.apply(write_settings);

                debug_assert!(!(minor_sst_readers.is_empty() && level_sst_readers.is_empty()));
                let level_updates = self.fill_major_order(
                    ctx,
                    &minor_sst_readers,
                    &level_sst_readers,
                    sst_write_settings,
                )?;
                let mut update = LoadedManifestUpdate::new(ManifestUpdateType::Major);
                update.compacted_minor_sst_ids = minor_sst_readers
                    .into_iter()
                    .map(|rs| {
                        rs.into_iter()
                            .map(|r| r.blob_id_ref().to_string())
                            .collect()
                    })
                    .collect();
                update.level_sst_updates = level_updates;
                Ok(update)
            }
            ManifestOperation::GarbageCollect {
                blob_ids,
                done_tx: inner_done_tx,
            } => {
                *done_tx = inner_done_tx;
                let to_delete_blob_ids = blob_ids;
                let mut update = LoadedManifestUpdate::new(ManifestUpdateType::GarbageCollection);
                self.inner_garbage_collect(ctx, &to_delete_blob_ids)?;
                update.garbage_collected_blob_ids = to_delete_blob_ids;
                Ok(update)
            }
            ManifestOperation::MinorMergeCompaction {
                minor_sst_readers: inner_minor_sst_readers,
                l0_sst_readers: inner_l0_sst_readers,
                write_settings,
            } => {
                let minor_sst_readers = inner_minor_sst_readers;
                let level_sst_readers = [(0, inner_l0_sst_readers)];
                let sst_write_settings = self.settings.apply(write_settings);
                debug_assert!(level_sst_readers.len() <= 1);
                let minor_readers = minor_sst_readers.iter().collect::<Vec<_>>();
                let empty = Vec::new();
                let l0_sst_readers = level_sst_readers.first().map(|(_, r)| r).unwrap_or(&empty);
                let new_readers = self.fill_minor_merge_order(
                    ctx,
                    &minor_readers,
                    l0_sst_readers,
                    sst_write_settings,
                )?;
                let mut update = LoadedManifestUpdate::new(ManifestUpdateType::MinorMerge);
                update.compacted_minor_sst_ids = minor_sst_readers
                    .into_iter()
                    .map(|rs| {
                        rs.into_iter()
                            .map(|r| r.blob_id_ref().to_string())
                            .collect()
                    })
                    .collect();
                update.level_sst_updates = vec![LoadedLevelUpdateInfo {
                    level_no: 0,
                    compacted_sst_blob_ids: l0_sst_readers
                        .iter()
                        .map(|reader| reader.blob_id_ref().to_string())
                        .collect(),
                    new_sst_readers: new_readers,
                }];
                Ok(update)
            }
            ManifestOperation::MergeCompaction {
                level_sst_readers: inner_lsr,
                write_settings,
            } => {
                let level_sst_readers = inner_lsr;
                let sst_write_settings = self.settings.apply(write_settings);
                let level_updates =
                    self.fill_merge_order(ctx, &level_sst_readers, sst_write_settings)?;
                let mut update = LoadedManifestUpdate::new(ManifestUpdateType::Merge);
                update.level_sst_updates = level_updates;
                Ok(update)
            }
        }
    }

    pub(crate) fn minor_compact<L: TombstonePairLike, I: Iterator<Item = L>>(
        ctx: &Ctx,
        name: &str,
        gen: &mut NameGenerator,
        wal_blob_id: &str,
        pairs: I,
        write_settings: SstWriteSettings,
        compactor_tx: Sender<CompactorOrder>,
    ) -> Result<Vec<SstReader>, CompactorError> {
        let blob_store = ctx.blob_store_ref();

        let writer = SstWriter::new(write_settings);
        let pairs = pairs.map(|pair| Ok::<L, ()>(pair));
        let blob_ids = writer.write_all(blob_store, pairs, gen)?.1;
        info!(
            ctx.logger(),
            "compactor {name} compacted wal {wal_blob_id} into new minor sst(s) {blob_ids:?}"
        );
        let readers = blob_ids
            .iter()
            .map(|blob_id| SstReader::new(ctx.blob_store_ref(), blob_id, compactor_tx.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(readers)
    }

    fn tombstone_iter<'a>(
        storage_wrapper: &'a Ctx,
        sst_readers: &[SstReader],
    ) -> Result<MergedHomogenousIter<SstScanner<'a, Ctx>>, CompactorError> {
        let results = sst_readers
            .iter()
            .map(|reader| {
                (
                    reader.blob_id_ref().to_string(),
                    reader.try_scan(storage_wrapper),
                )
            })
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

    fn fill_minor_merge_order(
        &mut self,
        ctx: &Ctx,
        minor_sst_readers: &[&Vec<SstReader>],
        l0_sst_readers: &[SstReader],
        write_settings: SstWriteSettings,
    ) -> Result<Vec<SstReader>, CompactorError> {
        let minor_sst_readers: Vec<_> = minor_sst_readers
            .iter()
            .flat_map(|readers| readers.iter().cloned())
            .collect();
        let minor_iter = Compactor::<Ctx>::tombstone_iter(ctx, &minor_sst_readers)?;
        let l0_iter = Compactor::<Ctx>::tombstone_iter(ctx, l0_sst_readers)?;
        let merged_iter: JoinedIter<SstError, _, _> = JoinedIter::new(minor_iter, l0_iter);
        let debug_data = DebugOrder {
            order_type: "minor-merge".to_string(),
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
        let debug_message = format!("{debug_data:?}");
        self.compact(ctx, &debug_message, merged_iter, &write_settings)
    }

    fn fill_merge_order(
        &mut self,
        ctx: &Ctx,
        level_sst_readers: &[(usize, Vec<SstReader>)],
        write_settings: SstWriteSettings,
    ) -> Result<Vec<LoadedLevelUpdateInfo>, CompactorError> {
        let target_level = if let Some(last) = level_sst_readers
            .iter()
            .filter(|x| !x.1.is_empty())
            .next_back()
        {
            last.0
        } else {
            return Ok(vec![]);
        };
        let level_scanners: Result<Vec<_>, _> = level_sst_readers
            .iter()
            .map(|(_, sst_readers)| Compactor::<Ctx>::tombstone_iter(ctx, sst_readers))
            .collect();
        let merged_iter = MergedHomogenousIter::new(level_scanners?.into_iter().rev());
        let debug_data: Vec<_> = level_sst_readers
            .iter()
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
            .collect();
        let mut new_readers = self.compact(ctx, &debug_data, merged_iter, &write_settings)?;
        Ok(level_sst_readers
            .iter()
            .map(|(level_no, old_readers)| {
                let compacted_sst_blob_ids = old_readers
                    .iter()
                    .map(|reader| reader.blob_id_ref().to_string())
                    .collect::<Vec<_>>();

                let new_sst_readers = if *level_no == target_level {
                    std::mem::take(&mut new_readers)
                } else {
                    Vec::new()
                };
                LoadedLevelUpdateInfo {
                    level_no: *level_no,
                    compacted_sst_blob_ids,
                    new_sst_readers,
                }
            })
            .collect())
    }

    fn fill_major_order(
        &mut self,
        ctx: &Ctx,
        minor_sst_readers: &[Vec<SstReader>],
        level_sst_readers: &[(usize, Vec<SstReader>)],
        write_settings: SstWriteSettings,
    ) -> Result<Vec<LoadedLevelUpdateInfo>, CompactorError> {
        let max_level = if let Some(last) = level_sst_readers.iter().last() {
            last.0
        } else {
            return Ok(vec![]);
        };
        let minor_sst_readers = minor_sst_readers
            .iter()
            .flat_map(|readers| readers.iter().cloned())
            .collect::<Vec<_>>();
        let minor_iter = Compactor::<Ctx>::tombstone_iter(ctx, &minor_sst_readers)?;
        let level_scanners: Result<Vec<_>, _> = level_sst_readers
            .iter()
            .map(|(_, sst_readers)| Compactor::<Ctx>::tombstone_iter(ctx, sst_readers))
            .collect();
        let level_iter = MergedHomogenousIter::new(level_scanners?.into_iter().rev());
        let merged_iter: JoinedIter<SstError, _, _> = JoinedIter::new(minor_iter, level_iter);
        let debug_level_map = level_sst_readers
            .iter()
            .filter(|(_, sst_readers)| !sst_readers.is_empty())
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
            order_type: "major".to_string(),
            minor_ssts: Some(
                minor_sst_readers
                    .iter()
                    .map(|r| r.blob_id_ref().to_string())
                    .collect::<Vec<_>>(),
            ),
            levels: Some(debug_level_map),
        };
        let mut new_readers = self.compact(ctx, debug_data, merged_iter, &write_settings)?;
        Ok(level_sst_readers
            .iter()
            .map(|(level_no, old_readers)| {
                let compacted_sst_blob_ids = old_readers
                    .iter()
                    .map(|reader| reader.blob_id_ref().to_string())
                    .collect::<Vec<_>>();

                let new_sst_readers = if *level_no == max_level {
                    std::mem::take(&mut new_readers)
                } else {
                    Vec::new()
                };
                LoadedLevelUpdateInfo {
                    level_no: *level_no,
                    compacted_sst_blob_ids,
                    new_sst_readers,
                }
            })
            .collect())
    }

    /// # Returns
    ///
    /// The new `SstReader` instances if successful.
    fn compact<D: Debug, E, I: Iterator<Item = Result<TombstonePair, E>>>(
        &mut self,
        ctx: &Ctx,
        debug_message: D,
        pairs: I,
        settings: &SstWriteSettings,
    ) -> Result<Vec<SstReader>, CompactorError>
    where
        SstError: From<E>,
    {
        let blob_store = ctx.blob_store_ref();
        let writer = SstWriter::new(settings.clone());
        let (_, blob_ids) = writer.write_all(blob_store, pairs, &mut self.gen)?;
        let readers = blob_ids
            .iter()
            .map(|blob_id| SstReader::new(blob_store, blob_id, self.compactor_tx.clone()))
            .collect::<Result<Vec<_>, _>>()?;

        info!(
            ctx.logger(),
            "compacted to new blobs {blob_ids:?} from original order {debug_message:?}",
        );
        Ok(readers)
    }

    fn inner_garbage_collect(
        &mut self,
        ctx: &Ctx,
        blob_ids: &[String],
    ) -> Result<(), CompactorError> {
        let old_gq = self.garbage_queue.clone().into_iter().map(|s| (true, s));
        let new_gq = blob_ids.iter().map(|s| (false, s.to_string()));
        self.garbage_queue = Vec::with_capacity(old_gq.len());
        let mut err_found = false;
        for (is_old, blob_id) in old_gq.chain(new_gq) {
            match ctx.blob_store_ref().delete(&blob_id) {
                Ok(_) => {
                    if is_old {
                        info!(
                            ctx.logger(),
                            "compactor {} garbage collected after retry: blob_id = {}",
                            self.name,
                            blob_id
                        );
                    } else {
                        info!(
                            ctx.logger(),
                            "compactor {} garbage collected blob on the first try: blob_id = {}",
                            self.name,
                            blob_id
                        );
                    }
                }
                Err(_) => {
                    if !is_old {
                        err_found = true;
                    }
                    error!(
                        ctx.logger(),
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
    /// The blob_ids of the newly created SST files.
    pub(crate) fn fill_minor_order<L: TombstonePairLike, I: Iterator<Item = L>>(
        &mut self,
        ctx: &Ctx,
        wal_blob_id: &str,
        iter: I,
        write_settings: SstWriteSettings,
    ) -> Result<Vec<SstReader>, CompactorError> {
        Compactor::<Ctx>::minor_compact(
            ctx,
            &self.name,
            &mut self.gen,
            wal_blob_id,
            iter,
            write_settings,
            self.compactor_tx.clone(),
        )
    }
}

#[cfg(test)]
mod test {
    use std::sync::mpsc::channel;
    use std::sync::Arc;

    use super::*;
    use crate::context::SimpleContext;
    use crate::helpful_macros::unwrap;
    use crate::logging::DefaultLogger;
    use crate::sst::block_cache::cache::LruBlockCache;
    use crate::sst::block_cache::BlockCache;
    use crate::storage::blob_store::InMemoryBlobStore;
    use crate::structs::skip_list::{SkipList, SkipListScanner};
    use crate::tablet::Tablet;
    use crate::wal::wal_wrapper::WalWrapperConfig;

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

    fn write_minor_md<Ctx: Context>(
        ctx: &Ctx,
        vec: &[(Vec<u8>, Vec<u8>)],
        gen: &mut NameGenerator,
    ) -> Result<BlobId, String> {
        let blob_store = ctx.blob_store_ref();
        let sst_writer = SstWriter::new(SstWriteSettings::default().set_writing_minor_sst(true));
        let (_, blob_ids) = sst_writer.write_all(
            blob_store,
            vec.iter().map(|(key, value)| {
                Result::<TombstonePair, SstError>::Ok(TombstonePair::new(
                    key.clone(),
                    value.clone(),
                ))
            }),
            gen,
        )?;
        assert_eq!(blob_ids.len(), 1);
        Ok(blob_ids[0].clone())
    }

    fn check_written<E: Debug, I: Iterator<Item = Result<TombstonePair, E>>>(
        vec: &[(Vec<u8>, Vec<u8>)],
        pairs: I,
    ) -> Result<(), String> {
        for (idx, result) in pairs.enumerate() {
            if idx == vec.len() {
                assert!(result.is_err());
                break;
            }
            let pair = result.unwrap();
            assert_eq!(pair.key_ref(), &vec[idx].0);
            assert_eq!(pair.value_ref().vec_ref().unwrap(), &vec[idx].1);
        }
        Ok(())
    }

    fn write_starting_sst_vec<Ctx: Context>(
        ctx: &Ctx,
        compactor_tx: Sender<CompactorOrder>,
        gen: &mut NameGenerator,
    ) -> Result<Vec<SstReader>, String> {
        let mut readers = Vec::with_capacity(3);

        let data = md_n_data(1);
        let path_1 = write_minor_md(ctx, &data, gen).unwrap();
        readers.push(SstReader::new(
            ctx.blob_store_ref(),
            &path_1,
            compactor_tx.clone(),
        )?);
        unwrap!(check_written(&data, unwrap!(readers[0].try_scan(ctx))));

        let data = md_n_data(2);
        let path_2 = unwrap!(write_minor_md(ctx, &data, gen));
        readers.push(SstReader::new(
            ctx.blob_store_ref(),
            &path_2,
            compactor_tx.clone(),
        )?);
        unwrap!(check_written(&data, unwrap!(readers[1].try_scan(ctx))));

        let data = md_n_data(3);
        let path_3 = unwrap!(write_minor_md(ctx, &data, gen));
        readers.push(SstReader::new(
            ctx.blob_store_ref(),
            &path_3,
            compactor_tx.clone(),
        )?);
        unwrap!(check_written(&data, unwrap!(readers[2].try_scan(ctx))));

        Ok(readers)
    }

    #[test]
    fn test_compactor_compact() {
        let blob_store = InMemoryBlobStore::new();
        let ctx = SimpleContext::from((
            blob_store,
            LruBlockCache::with_capacity(32),
            DefaultLogger::default(),
        ));
        let minors = Vec::new();
        let levels = Vec::new();
        let (compactor_tx, compactor_rx) = channel::<CompactorOrder>();
        let sst_store = unwrap!(Tablet::new(&ctx, &minors, &levels, compactor_tx.clone(),));
        let mem_table = unwrap!(MemTable::new(
            ctx.clone(),
            compactor_tx.clone(),
            WalWrapperConfig::default()
        ));
        let mut compactor = Compactor::new(
            ctx.clone(),
            sst_store.clone(),
            CompactorSettings::default(),
            compactor_tx.clone(),
            compactor_rx,
            0,
            mem_table.clone(),
        );

        let sst_readers = unwrap!(write_starting_sst_vec(
            &ctx,
            compactor_tx.clone(),
            &mut compactor.gen
        ));
        let pairs = sst_readers
            .iter()
            .map(|reader| unwrap!(reader.try_scan(&ctx)));
        let pairs = MergedHomogenousIter::new(pairs);
        let new_sst_readers = compactor
            .compact(&ctx, "'my blobs!'", pairs, &SstWriteSettings::default())
            .unwrap();

        let skip_list = SkipList::default();
        for in_data in (1..4).map(md_n_data).rev() {
            for (key, value) in in_data {
                skip_list.insert(&key, Some(&value));
            }
        }
        let arc = Arc::new(skip_list);
        let scanner = SkipListScanner::from(arc.clone());
        let out_data: Vec<(Vec<u8>, Vec<u8>)> = scanner
            .map(|pair| {
                let key = pair.key_ref();
                let value = pair.value_ref();
                (key.to_vec(), value.unwrap().to_vec())
            })
            .collect::<Vec<_>>();

        let new_iters = new_sst_readers
            .iter()
            .map(|reader| unwrap!(reader.try_scan(&ctx)));
        let pairs = MergedHomogenousIter::new(new_iters);
        assert!(check_written(&out_data, pairs).is_ok())
    }
}
