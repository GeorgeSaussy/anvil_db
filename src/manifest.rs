use std::sync::mpsc::Sender;

use crate::common::{
    extend_from_string_matrix, extend_from_string_slice, recover_string_matrix, recover_string_vec,
    CastError,
};
use crate::compactor::CompactorError;
use crate::mem_table::MemTableCompactorIterator;
use crate::sst::{reader::SstReader, writer::SstWriteSettings};
use crate::storage::blob_store::{BlobId, ReadCursor};
use crate::var_int::VarInt64;
use crate::wal::wal_entry::{Checksum, WalError};

pub(crate) type WalId = BlobId;
pub(crate) type SstId = BlobId;

#[derive(Debug)]
pub(crate) enum ManifestOperation {
    // TODO(anvil_db): Stop doing minor compactions and use the WALs directly for a
    // merge minor compaction when possible.
    MinorCompact {
        iter: MemTableCompactorIterator,
        wal_blob_id: BlobId,
        done_tx: Sender<Result<Vec<BlobId>, CompactorError>>,
    },
    MinorMergeCompaction {
        minor_sst_readers: Vec<Vec<SstReader>>,
        l0_sst_readers: Vec<SstReader>,
        write_settings: SstWriteSettings,
    },
    MergeCompaction {
        level_sst_readers: Vec<(usize, Vec<SstReader>)>,
        write_settings: SstWriteSettings,
    },
    /// Run a major compaction.
    MajorCompaction {
        minor_sst_readers: Vec<Vec<SstReader>>,
        level_sst_readers: Vec<(usize, Vec<SstReader>)>,
        write_settings: SstWriteSettings,
        done_tx: Option<Sender<Result<Vec<BlobId>, CompactorError>>>,
    },
    /// Deletes a set of blobs.
    GarbageCollect {
        blob_ids: Vec<BlobId>,
        /// The okay value of `done_tx` is [Vec<BlobId>] and not [()]
        /// only for compatibility with the other types of [CompactorOrder].
        /// An ok value should always be an empty vector.
        done_tx: Option<Sender<Result<Vec<BlobId>, CompactorError>>>,
    },
}

/// [LoadedLevelUpdateInfo] contains the level difference information
/// along with opened [SstReader]s.
#[derive(Debug)]
pub(crate) struct LoadedLevelUpdateInfo {
    pub level_no: usize,
    pub compacted_sst_blob_ids: Vec<BlobId>,
    pub new_sst_readers: Vec<SstReader>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ManifestUpdateType {
    GarbageCollection,
    Major,
    Merge,
    MinorMerge,
    Minor,
}

impl TryFrom<u8> for ManifestUpdateType {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ManifestUpdateType::GarbageCollection),
            1 => Ok(ManifestUpdateType::Major),
            2 => Ok(ManifestUpdateType::Merge),
            3 => Ok(ManifestUpdateType::MinorMerge),
            4 => Ok(ManifestUpdateType::Minor),
            _ => Err(value),
        }
    }
}

impl ManifestUpdateType {
    pub fn tag(&self) -> u8 {
        match self {
            ManifestUpdateType::GarbageCollection => 0,
            ManifestUpdateType::Major => 1,
            ManifestUpdateType::Merge => 2,
            ManifestUpdateType::MinorMerge => 3,
            ManifestUpdateType::Minor => 4,
        }
    }
}

/// [LoadedManifestUpdate] contains information about the manifest difference
/// along with opened [SstReader]s.
#[derive(Debug)]
pub(crate) struct LoadedManifestUpdate {
    pub update_type: ManifestUpdateType,
    pub garbage_collected_blob_ids: Vec<BlobId>,
    pub compacted_wal_ids: Vec<BlobId>,
    pub compacted_minor_sst_ids: Vec<Vec<BlobId>>,
    pub new_minor_ssts: Vec<Vec<SstReader>>,
    pub level_sst_updates: Vec<LoadedLevelUpdateInfo>,
}

impl LoadedManifestUpdate {
    pub fn new(update_type: ManifestUpdateType) -> Self {
        Self {
            update_type,
            garbage_collected_blob_ids: Vec::new(),
            compacted_wal_ids: Vec::new(),
            compacted_minor_sst_ids: Vec::new(),
            new_minor_ssts: Vec::new(),
            level_sst_updates: Vec::new(),
        }
    }

    pub fn logical_update(&self) -> ManifestUpdate {
        ManifestUpdate {
            update_type: self.update_type.clone(),
            garbage_collected_blob_ids: self.garbage_collected_blob_ids.clone(),
            compacted_wal_ids: self.compacted_wal_ids.clone(),
            compacted_minor_sst_ids: self.compacted_minor_sst_ids.clone(),
            new_minor_sst_ids: self
                .new_minor_ssts
                .iter()
                .map(|level| {
                    level
                        .iter()
                        .map(|reader| reader.blob_id_ref().to_string())
                        .collect()
                })
                .collect(),
            level_sst_updates: self
                .level_sst_updates
                .iter()
                .map(|info| LevelUpdateInfo {
                    level_no: info.level_no,
                    compacted_sst_blob_ids: info.compacted_sst_blob_ids.clone(),
                    new_sst_blob_ids: info
                        .new_sst_readers
                        .iter()
                        .map(|reader| reader.blob_id_ref().to_string())
                        .collect(),
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LevelUpdateInfo {
    pub level_no: usize,
    pub compacted_sst_blob_ids: Vec<SstId>,
    pub new_sst_blob_ids: Vec<SstId>,
}

/// [ManifestUpdate] contains the logical manifest. A [ManifestUpdate] only
/// contains [BlobId]s
#[derive(Debug, Clone)]
pub(crate) struct ManifestUpdate {
    pub update_type: ManifestUpdateType,
    /// These could be [WalId]s or [SstId]s.
    pub garbage_collected_blob_ids: Vec<BlobId>,
    pub compacted_wal_ids: Vec<WalId>,
    pub compacted_minor_sst_ids: Vec<Vec<SstId>>,
    pub new_minor_sst_ids: Vec<Vec<SstId>>,
    pub level_sst_updates: Vec<LevelUpdateInfo>,
}

impl ManifestUpdate {
    pub(crate) fn serialize_to(&self, entry_buf: &mut Vec<u8>) -> Result<(), CastError> {
        entry_buf.push(self.update_type.tag());

        // garbage_collected_blob_ids
        extend_from_string_slice(entry_buf, &self.garbage_collected_blob_ids)?;

        // compacted_wal_ids
        extend_from_string_slice(entry_buf, &self.compacted_wal_ids)?;

        // compacted_minor_sst_ids
        extend_from_string_matrix(entry_buf, &self.compacted_minor_sst_ids)?;

        // new_minor_sst_ids
        extend_from_string_matrix(entry_buf, &self.new_minor_sst_ids)?;

        // pub level_sst_updates: Vec<LevelUpdateInfo>,
        let level_update_count = VarInt64::try_from(self.level_sst_updates.len())?;
        entry_buf.extend(level_update_count.data_ref());
        for level_update in &self.level_sst_updates {
            // level_no
            let level = VarInt64::try_from(level_update.level_no)?;
            entry_buf.extend(level.data_ref());

            // compacted_sst_blob_ids
            extend_from_string_slice(entry_buf, &level_update.compacted_sst_blob_ids)?;

            // new_sst_blob_ids
            extend_from_string_slice(entry_buf, &level_update.new_sst_blob_ids)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Manifest {
    /// listed oldest to youngest
    pub(crate) minors: Vec<SstId>,
    /// levels of the SST store containing the blob_ids in order by level
    /// from oldest to newest
    pub(crate) levels: Vec<Vec<SstId>>,
    /// These are compacted [WalId]s that can be safely garbage collected
    /// if they haven't been already. The [WalId]s included in this
    /// list have not had their garbage collection confirmed.
    pub(crate) garbage_wal_ids: Vec<WalId>,
    /// These are compacted [SstId]s that can be safely garbage collected
    /// if they haven't been already. The [SstId]s included in this
    /// list have not had their garbage collection confirmed.
    pub(crate) garbage_sst_ids: Vec<SstId>,
}

fn remove_if_found_vec(list: &mut Vec<BlobId>, blob_id: &BlobId) -> bool {
    if let Some(pos) = list.iter().position(|id| id == blob_id) {
        list.remove(pos);
        return true;
    }
    false
}

impl Manifest {
    pub(crate) fn apply(&mut self, update: &ManifestUpdate) -> Result<(), String> {
        // garbage_collected_blob_ids
        'gc: for blob_id in &update.garbage_collected_blob_ids {
            if remove_if_found_vec(&mut self.garbage_wal_ids, blob_id) {
                continue 'gc;
            }
            if remove_if_found_vec(&mut self.garbage_sst_ids, blob_id) {
                continue 'gc;
            }
            return Err(format!("garbage blob_id {blob_id} not found in manifest"));
        }

        // compacted_wal_ids
        for blob_id in &update.compacted_wal_ids {
            self.garbage_wal_ids.push(blob_id.clone());
        }

        // compacted_minor_sst_ids
        for blob_id in update.compacted_minor_sst_ids.iter().flat_map(|v| v.iter()) {
            remove_if_found_vec(&mut self.minors, blob_id);
            self.garbage_sst_ids.push(blob_id.clone());
        }

        // new_minor_sst_ids
        for blob_id in update.new_minor_sst_ids.iter().flat_map(|v| v.iter()) {
            self.minors.push(blob_id.clone());
        }

        // level_sst_updates
        for level_update in &update.level_sst_updates {
            self.apply_level(level_update)?;
        }

        Ok(())
    }

    fn apply_level(&mut self, update: &LevelUpdateInfo) -> Result<(), String> {
        while update.level_no >= self.levels.len() {
            self.levels.push(Vec::new());
        }
        let level = &mut self.levels[update.level_no];
        for blob_id in &update.compacted_sst_blob_ids {
            if !remove_if_found_vec(level, blob_id) {
                return Err(format!("compacted blob_id {blob_id} not found in manifest"));
            }
            self.garbage_sst_ids.push(blob_id.clone());
        }
        for blob_id in &update.new_sst_blob_ids {
            level.push(blob_id.clone());
        }
        Ok(())
    }

    pub(crate) fn serialize_to(&self, buf: &mut Vec<u8>) -> Result<(), CastError> {
        extend_from_string_slice(buf, &self.minors)?;
        extend_from_string_matrix(buf, &self.levels)?;
        extend_from_string_slice(buf, &self.garbage_wal_ids)?;
        extend_from_string_slice(buf, &self.garbage_sst_ids)?;
        Ok(())
    }

    pub(crate) fn recover_from<RC: ReadCursor>(
        reader: &mut RC,
        prev_checksum: Checksum,
    ) -> Result<(Manifest, Checksum), WalError> {
        let (minors, checksum) = recover_string_vec(reader, prev_checksum)?;
        let (levels, checksum) = recover_string_matrix(reader, checksum)?;
        let (garbage_wal_ids, checksum) = recover_string_vec(reader, checksum)?;
        let (garbage_sst_ids, checksum) = recover_string_vec(reader, checksum)?;
        Ok((
            Manifest {
                minors,
                levels,
                garbage_wal_ids,
                garbage_sst_ids,
            },
            checksum,
        ))
    }
}
