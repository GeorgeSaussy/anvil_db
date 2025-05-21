use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::string::FromUtf8Error;

use crate::checksum::crc32;
use crate::common::{
    recover_byte, recover_length_prefixed_buffer, recover_string_matrix, recover_string_vec,
    recover_var_int, try_length_prefix, CastError,
};
use crate::kv::TombstoneValueLike;
use crate::manifest::{LevelUpdateInfo, Manifest, ManifestUpdate, ManifestUpdateType};
use crate::storage::blob_store::{BlobId, BlobStoreError, ReadCursor, WriteCursor};
use crate::var_int::VarInt64;

#[allow(dead_code)] // These fields are only read when printing debugging information.
#[derive(Clone, Debug)]
pub(crate) enum WalError {
    WriteEntryError(String),
    BlobStoreError(String),
    InvalidInput(Option<String>),
    /// [WalRecoveryError] is boxed to keep the size of the error smaller.
    Recovery(Box<WalRecoveryError>),
    FinalizedWal,
    BedManifestUpdateTag(u8),
}

impl Display for WalError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{self:?}")
    }
}

impl Error for WalError {}

impl From<CastError> for WalError {
    fn from(err: CastError) -> Self {
        WalError::InvalidInput(Some(format!("cast error: {err:?}")))
    }
}

impl From<FromUtf8Error> for WalError {
    fn from(err: FromUtf8Error) -> Self {
        WalError::InvalidInput(Some(format!("utf8 error: {err:?}")))
    }
}

impl From<BlobStoreError> for WalError {
    fn from(value: BlobStoreError) -> Self {
        WalError::BlobStoreError(format!("{value:?}"))
    }
}

#[allow(dead_code)] // These fields are only read when printing debugging information.
#[derive(Debug, Clone)]
pub(crate) enum WalRecoveryError {
    IncorrectHeader {
        blob_id: BlobId,
        found: [u8; 4],
        expected: [u8; 4],
    },
    ReadExactFailed {
        blob_id: BlobId,
        err: BlobStoreError,
    },
    IncorrectChecksum {
        blob_id: BlobId,
        found: u32,
        expected: u32,
    },
    UnknownEntryTag {
        blob_id: BlobId,
        tag: u8,
    },
    ManifestMismatch {
        blob_id: BlobId,
        /// the recovered manifest, boxed to keep the size down
        found: Box<Manifest>,
        /// the expected manifest, boxed to keep the size down
        expected: Box<Manifest>,
    },
    BadManifestUpdate(String),
}

impl From<WalRecoveryError> for WalError {
    fn from(value: WalRecoveryError) -> Self {
        WalError::Recovery(Box::new(value))
    }
}

pub(crate) type Checksum = u32;

#[derive(Debug)]
pub(crate) enum WalEntry {
    Set { key: Vec<u8>, value: Vec<u8> },
    Remove { key: Vec<u8> },
    ManifestUpdate(ManifestUpdate),
    ManifestSnapshot(Manifest),
}

impl<T: TombstoneValueLike> From<(&[u8], &T)> for WalEntry {
    fn from((key, value): (&[u8], &T)) -> Self {
        if let Some(value) = value.as_ref() {
            WalEntry::Set {
                key: key.to_vec(),
                value: value.to_vec(),
            }
        } else {
            WalEntry::Remove { key: key.to_vec() }
        }
    }
}

impl WalEntry {
    const MANIFEST_SNAPSHOT_TAG: u8 = 5;
    const MANIFEST_UPDATE_TAG: u8 = 7;
    const REMOVE_TAG: u8 = 1;
    const SET_TAG: u8 = 0;

    fn entry_tag(&self) -> u8 {
        match self {
            WalEntry::Set { .. } => WalEntry::SET_TAG,
            WalEntry::Remove { .. } => WalEntry::REMOVE_TAG,
            WalEntry::ManifestSnapshot { .. } => WalEntry::MANIFEST_SNAPSHOT_TAG,
            WalEntry::ManifestUpdate(_manifest_update) => WalEntry::MANIFEST_UPDATE_TAG,
        }
    }
}

impl TryFrom<&WalEntry> for Vec<u8> {
    type Error = WalError;

    fn try_from(value: &WalEntry) -> Result<Self, WalError> {
        let mut entry_buf = vec![value.entry_tag()];
        match value {
            WalEntry::Set { key, value } => {
                let b1 = try_length_prefix(key)?;
                entry_buf.extend(b1);
                let b2 = try_length_prefix(value)?;
                entry_buf.extend(b2);
            }
            WalEntry::Remove { key } => {
                let b = try_length_prefix(key)?;
                entry_buf.extend(b);
            }
            WalEntry::ManifestSnapshot(manifest) => {
                manifest.serialize_to(&mut entry_buf)?;
            }
            WalEntry::ManifestUpdate(update) => {
                update.serialize_to(&mut entry_buf)?;
            }
        }
        Ok(entry_buf)
    }
}

/// A WalEntryBlock contains a list of WalEntries for serialization and
/// deserialization. Because these entries are typically being written to disk
/// in blocks, it does not make sense to add a checksum to the beginning of each
/// entry. Likewise, there is never a time where only one entry is read from a
/// WAL, so it makes sense to batch both reads and writes.
///
/// The first entry in the block is a VarInt containing how many elements are
/// in the block. Following this is the serialized blocks themselves. Finally,
/// the final entry is a CRC32 checksum of the entire block in little endian
/// form.
///
/// Each entry begins with one byte dictating what kind of entry it is.
///
/// - 0: (reserved)
/// - 1: set
/// - 2: remove
pub(crate) struct WalEntryBlock {
    // TODO(t/1336): Consider using a lock-free hash set here.
    // Something like this?
    // https://preshing.com/20130605/the-worlds-simplest-lock-free-hash-table/
    entries: Vec<WalEntry>,
}

pub(crate) struct BlockWriteData {
    pub(crate) checksum: Checksum,
    pub(crate) bytes_written: usize,
}

impl WalEntryBlock {
    pub(crate) fn new(entries: Vec<WalEntry>) -> Self {
        WalEntryBlock { entries }
    }

    pub(crate) fn recover<RC: ReadCursor>(
        reader: &mut RC,
        blob_id: &str,
        prev_checksum: Checksum,
    ) -> Result<(WalEntryBlock, Checksum), WalError> {
        let (entry_count, mut checksum) = recover_var_int(reader, prev_checksum)?;
        let mut entries = Vec::new();
        for _ in 0..entry_count.value() {
            let (entry, new_checksum) = WalEntryBlock::recover_entry(reader, blob_id, checksum)?;
            checksum = new_checksum;
            entries.push(entry);
        }
        let mut checksum_le_buf = [0_u8; 4];
        if let Err(err) = reader.read_exact(&mut checksum_le_buf) {
            return Err(WalRecoveryError::ReadExactFailed {
                blob_id: blob_id.to_string(),
                err,
            }
            .into());
        }
        if checksum_le_buf != checksum.to_le_bytes() {
            return Err(WalRecoveryError::IncorrectChecksum {
                blob_id: blob_id.to_string(),
                expected: checksum,
                found: u32::from_le_bytes(checksum_le_buf),
            }
            .into());
        }
        Ok((WalEntryBlock::new(entries), checksum))
    }

    fn recover_entry<RC: ReadCursor>(
        reader: &mut RC,
        blob_id: &str,
        prev_checksum: Checksum,
    ) -> Result<(WalEntry, Checksum), WalError> {
        let mut buf = [0_u8];
        if let Err(err) = reader.read_exact(&mut buf) {
            return Err(WalRecoveryError::ReadExactFailed {
                blob_id: blob_id.to_string(),
                err,
            }
            .into());
        };
        let checksum = crc32(prev_checksum, &buf);
        let tag = buf[0];
        match tag {
            WalEntry::SET_TAG => {
                let (key, checksum) = recover_length_prefixed_buffer(reader, checksum)?;
                let (value, checksum) = recover_length_prefixed_buffer(reader, checksum)?;
                Ok((WalEntry::Set { key, value }, checksum))
            }
            WalEntry::REMOVE_TAG => {
                let (key, checksum) = recover_length_prefixed_buffer(reader, checksum)?;
                Ok((WalEntry::Remove { key }, checksum))
            }
            WalEntry::MANIFEST_SNAPSHOT_TAG => {
                let (manifest, checksum) = Manifest::recover_from(reader, checksum)?;
                Ok((WalEntry::ManifestSnapshot(manifest), checksum))
            }
            WalEntry::MANIFEST_UPDATE_TAG => {
                // update_type
                let (update_type_tag, checksum) = recover_byte(reader, checksum)?;
                let update_type = ManifestUpdateType::try_from(update_type_tag)
                    .map_err(WalError::BedManifestUpdateTag)?;

                // garbage_collected_blob_ids
                let (garbage_collected_blob_ids, checksum) = recover_string_vec(reader, checksum)?;

                // compacted_wal_ids
                let (compacted_wal_ids, checksum) = recover_string_vec(reader, checksum)?;

                // compacted_minor_sst_ids
                let (compacted_minor_sst_ids, checksum) = recover_string_matrix(reader, checksum)?;

                // new_minor_sst_ids
                let (new_minor_sst_ids, checksum) = recover_string_matrix(reader, checksum)?;

                // level_sst_updates
                let (level_sst_update_count, mut checksum) = recover_var_int(reader, checksum)?;
                let level_sst_update_count = level_sst_update_count.value() as usize;
                let mut level_sst_updates = Vec::with_capacity(level_sst_update_count);
                for _ in 0..level_sst_update_count {
                    let (level_no, new_checksum) = recover_var_int(reader, checksum)?;
                    checksum = new_checksum;
                    let level_no = level_no.value() as usize;

                    // compacted_sst_blob_ids
                    let (compacted_sst_blob_ids, new_checksum) =
                        recover_string_vec(reader, checksum)?;
                    checksum = new_checksum;

                    // new_sst_blob_ids
                    let (new_sst_blob_ids, new_checksum) = recover_string_vec(reader, checksum)?;
                    checksum = new_checksum;

                    level_sst_updates.push(LevelUpdateInfo {
                        level_no,
                        compacted_sst_blob_ids,
                        new_sst_blob_ids,
                    });
                }
                Ok((
                    WalEntry::ManifestUpdate(ManifestUpdate {
                        update_type,
                        garbage_collected_blob_ids,
                        compacted_wal_ids,
                        compacted_minor_sst_ids,
                        new_minor_sst_ids,
                        level_sst_updates,
                    }),
                    checksum,
                ))
            }
            _ => Err(WalRecoveryError::UnknownEntryTag {
                blob_id: blob_id.to_string(),
                tag,
            }
            .into()),
        }
    }

    pub(crate) fn entries_ref(&self) -> &Vec<WalEntry> {
        &self.entries
    }

    fn write_entries_halved<T: WriteCursor>(
        &self,
        writer: &mut T,
        start: usize,
        len: usize,
        prev: Checksum,
    ) -> Result<BlockWriteData, WalError> {
        let start_1 = start;
        let len_1 = len / 2;
        let block_data = self.write_entries(writer, start_1, len_1, prev)?;
        let prev_bytes_written = block_data.bytes_written;
        let start_2 = start + len_1;
        let len_2 = len - len_1;
        let mut block_data =
            self.write_entries_halved(writer, start_2, len_2, block_data.checksum)?;
        block_data.bytes_written += prev_bytes_written;
        Ok(block_data)
    }

    /// On success, return the checksum of the newly written block and the
    /// the total number of bytes written, including the 4 bytes from the
    /// checksum.
    fn write_entries<T: WriteCursor>(
        &self,
        writer: &mut T,
        start: usize,
        len: usize,
        prev: Checksum,
    ) -> Result<BlockWriteData, WalError> {
        let count = if let Ok(count) = VarInt64::try_from(len as u64) {
            count
        } else {
            return self.write_entries_halved(writer, start, len, prev);
        };
        if let Err(err) = writer.write(count.data_ref()) {
            return Err(WalError::WriteEntryError(format!("{err:?}")));
        }
        let mut checksum = crc32(prev, count.data_ref());
        let mut entry_buf = Vec::new();
        for entry in self.entries.iter().skip(start).take(len) {
            let b: Vec<u8> = entry.try_into()?;
            entry_buf.extend(b);
        }
        checksum = crc32(checksum, &entry_buf);
        entry_buf.extend(checksum.to_le_bytes());
        if let Err(err) = writer.write(&entry_buf) {
            return Err(WalError::WriteEntryError(format!("{err:?}")));
        }
        Ok(BlockWriteData {
            checksum,
            bytes_written: entry_buf.len(),
        })
    }

    /// On success, return the checksum of the newly written block and the
    /// the total number of bytes written, including the 4 bytes from the
    /// checksum.
    pub(crate) fn write<T: WriteCursor>(
        &self,
        writer: &mut T,
        prev_checksum: Checksum,
    ) -> Result<BlockWriteData, WalError> {
        self.write_entries(writer, 0, self.entries.len(), prev_checksum)
    }
}
