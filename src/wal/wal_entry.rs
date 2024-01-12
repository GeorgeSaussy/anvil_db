use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::string::FromUtf8Error;

use crate::checksum::crc32;
use crate::common::{try_length_prefix, CastError};
use crate::storage::blob_store::{BlobStoreError, ReadCursor, WriteCursor};
use crate::var_int::VarInt64;

#[derive(Clone, Debug)]
pub(crate) enum WalError {
    WriteEntryError(String),
    BlobStoreError(String),
    InvalidInput(Option<String>),
    RecoveryError(String),
    RotationRequired,
    FinalizedWal,
}

impl Display for WalError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            WalError::WriteEntryError(err) => write!(f, "error while writing wal entry: {}", err),
            WalError::BlobStoreError(err) => write!(f, "blob store error: {}", err),
            WalError::InvalidInput(Some(err)) => write!(f, "invalid input: {}", err),
            WalError::InvalidInput(None) => write!(f, "invalid input"),
            WalError::RecoveryError(err) => write!(f, "error while recovering wal: {}", err),
            WalError::RotationRequired => write!(f, "wal rotation required"),
            WalError::FinalizedWal => write!(f, "wal is finalized"),
        }
    }
}

impl Error for WalError {}

impl From<CastError> for WalError {
    fn from(err: CastError) -> Self {
        WalError::InvalidInput(Some(format!("cast error: {:?}", err)))
    }
}

impl From<FromUtf8Error> for WalError {
    fn from(err: FromUtf8Error) -> Self {
        WalError::InvalidInput(Some(format!("utf8 error: {:?}", err)))
    }
}

impl From<BlobStoreError> for WalError {
    fn from(value: BlobStoreError) -> Self {
        WalError::BlobStoreError(format!("{:?}", value))
    }
}

pub(crate) type Checksum = u32;

#[derive(Debug)]
pub(crate) enum WalEntry {
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Remove {
        key: Vec<u8>,
    },
    MinorCompaction {
        wal_blob_id: String,
        sst_blob_id: String,
    },
    MergeCompaction {
        /// ordered from oldest to newest
        start_sst_blob_ids: Vec<String>,
        end_sst_blob_id: String,
    },
    MajorCompaction {
        /// ordered from oldest to newest
        start_sst_blob_ids: Vec<String>,
        end_sst_blob_id: String,
    },
    ManifestSnapshot {
        /// ordered from oldest to newest
        sst_blob_ids: Vec<String>,
    },
}

impl WalEntry {
    const MAJOR_COMPACTION_TAG: u8 = 4;
    const MANIFEST_SNAPSHOT_TAG: u8 = 5;
    const MERGE_COMPACTION_TAG: u8 = 3;
    const MINOR_COMPACTION_TAG: u8 = 2;
    const REMOVE_TAG: u8 = 1;
    const SET_TAG: u8 = 0;

    fn entry_tag(&self) -> u8 {
        match self {
            WalEntry::Set { .. } => WalEntry::SET_TAG,
            WalEntry::Remove { .. } => WalEntry::REMOVE_TAG,
            WalEntry::MinorCompaction { .. } => WalEntry::MINOR_COMPACTION_TAG,
            WalEntry::MergeCompaction { .. } => WalEntry::MERGE_COMPACTION_TAG,
            WalEntry::MajorCompaction { .. } => WalEntry::MAJOR_COMPACTION_TAG,
            WalEntry::ManifestSnapshot { .. } => WalEntry::MANIFEST_SNAPSHOT_TAG,
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
            WalEntry::MinorCompaction {
                wal_blob_id,
                sst_blob_id,
            } => {
                let bytes = wal_blob_id.as_bytes();
                let b = try_length_prefix(bytes)?;
                entry_buf.extend(b);
                let bytes = sst_blob_id.as_bytes();
                let b = try_length_prefix(bytes)?;
                entry_buf.extend(b);
            }
            WalEntry::MergeCompaction {
                start_sst_blob_ids,
                end_sst_blob_id,
            } => {
                let start_sst_count = VarInt64::try_from(start_sst_blob_ids.len())?;
                entry_buf.extend(start_sst_count.data_ref());
                let mut bytes = Vec::new();
                for sst_blob_id in start_sst_blob_ids {
                    let b = try_length_prefix(sst_blob_id.as_bytes())?;
                    bytes.extend(b);
                }
                entry_buf.extend(bytes);
                let bytes = end_sst_blob_id.as_bytes();
                let b = try_length_prefix(bytes)?;
                entry_buf.extend(b);
            }
            WalEntry::MajorCompaction {
                start_sst_blob_ids,
                end_sst_blob_id,
            } => {
                let start_sst_count = VarInt64::try_from(start_sst_blob_ids.len())?;
                entry_buf.extend(start_sst_count.data_ref());
                let mut bytes = Vec::new();
                for sst_blob_id in start_sst_blob_ids {
                    let b = try_length_prefix(sst_blob_id.as_bytes())?;
                    bytes.extend(b);
                }
                entry_buf.extend(bytes);
                let bytes = end_sst_blob_id.as_bytes();
                let b = try_length_prefix(bytes)?;
                entry_buf.extend(b);
            }
            WalEntry::ManifestSnapshot { sst_blob_ids } => {
                let sst_count = VarInt64::try_from(sst_blob_ids.len())?;
                entry_buf.extend(sst_count.data_ref());
                let mut bytes = Vec::new();
                for sst_blob_id in sst_blob_ids {
                    let b = try_length_prefix(sst_blob_id.as_bytes())?;
                    bytes.extend(b);
                }
                entry_buf.extend(bytes);
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
        prev_checksum: Checksum,
    ) -> Result<(WalEntryBlock, Checksum), WalError> {
        let (entry_count, mut checksum) = WalEntryBlock::recover_var_int(reader, prev_checksum)?;
        let mut entries = Vec::new();
        for _ in 0..entry_count.value() {
            let (entry, new_checksum) = WalEntryBlock::recover_entry(reader, checksum)?;
            checksum = new_checksum;
            entries.push(entry);
        }
        let mut checksum_le_buf = [0_u8; 4];
        if let Err(err) = reader.read_exact(&mut checksum_le_buf) {
            return Err(WalError::RecoveryError(format!(
                "could not read checksum from blob: {:?}",
                err
            )));
        }
        if checksum_le_buf != checksum.to_le_bytes() {
            return Err(WalError::RecoveryError(
                "checksum failed for block".to_string(),
            ));
        }
        Ok((WalEntryBlock::new(entries), checksum))
    }

    fn recover_var_int<RC: ReadCursor>(
        reader: &mut RC,
        prev_checksum: Checksum,
    ) -> Result<(VarInt64, Checksum), WalError> {
        let i = match VarInt64::try_from_rc(reader) {
            Ok(i) => i,
            Err(_) => return Err(WalError::InvalidInput(None)),
        };
        let checksum = crc32(prev_checksum, i.data_ref());
        Ok((i, checksum))
    }

    fn recover_entry<RC: ReadCursor>(
        reader: &mut RC,
        prev_checksum: Checksum,
    ) -> Result<(WalEntry, Checksum), WalError> {
        let mut buf = [0_u8];
        if let Err(err) = reader.read_exact(&mut buf) {
            return Err(WalError::RecoveryError(format!(
                "could not read entry tag: {:?}",
                err
            )));
        };
        let checksum = crc32(prev_checksum, &buf);
        match buf[0] {
            WalEntry::SET_TAG => {
                let (key, checksum) =
                    WalEntryBlock::recover_length_prefixed_buffer(reader, checksum)?;
                let (value, checksum) =
                    WalEntryBlock::recover_length_prefixed_buffer(reader, checksum)?;
                Ok((WalEntry::Set { key, value }, checksum))
            }
            WalEntry::REMOVE_TAG => {
                let (key, checksum) =
                    WalEntryBlock::recover_length_prefixed_buffer(reader, checksum)?;
                Ok((WalEntry::Remove { key }, checksum))
            }
            WalEntry::MINOR_COMPACTION_TAG => {
                let (wal_blob_id, checksum) =
                    WalEntryBlock::recover_length_prefixed_buffer(reader, checksum)?;
                let wal_blob_id = String::from_utf8(wal_blob_id)?;
                let (sst_blob_id, checksum) =
                    WalEntryBlock::recover_length_prefixed_buffer(reader, checksum)?;
                let sst_blob_id = String::from_utf8(sst_blob_id)?;
                Ok((
                    WalEntry::MinorCompaction {
                        wal_blob_id,
                        sst_blob_id,
                    },
                    checksum,
                ))
            }
            WalEntry::MERGE_COMPACTION_TAG => {
                let (start_sst_count, mut checksum) =
                    WalEntryBlock::recover_var_int(reader, checksum)?;
                let mut start_sst_blob_ids = Vec::with_capacity(start_sst_count.value() as usize);
                for _ in 0..start_sst_count.value() {
                    let (start_sst_blob_id, new_checksum) =
                        WalEntryBlock::recover_length_prefixed_buffer(reader, checksum)?;
                    checksum = new_checksum;
                    start_sst_blob_ids.push(String::from_utf8(start_sst_blob_id)?);
                }
                let (end_sst_blob_id, checksum) =
                    WalEntryBlock::recover_length_prefixed_buffer(reader, checksum)?;
                let end_sst_blob_id = String::from_utf8(end_sst_blob_id)?;
                Ok((
                    WalEntry::MergeCompaction {
                        start_sst_blob_ids,
                        end_sst_blob_id,
                    },
                    checksum,
                ))
            }
            WalEntry::MAJOR_COMPACTION_TAG => {
                let (start_sst_count, mut checksum) =
                    WalEntryBlock::recover_var_int(reader, checksum)?;
                let mut start_sst_blob_ids = Vec::with_capacity(start_sst_count.value() as usize);
                for _ in 0..start_sst_count.value() {
                    let (start_sst_blob_id, new_checksum) =
                        WalEntryBlock::recover_length_prefixed_buffer(reader, checksum)?;
                    checksum = new_checksum;
                    start_sst_blob_ids.push(String::from_utf8(start_sst_blob_id)?);
                }
                let (end_sst_blob_id, checksum) =
                    WalEntryBlock::recover_length_prefixed_buffer(reader, checksum)?;
                let end_sst_blob_id = String::from_utf8(end_sst_blob_id)?;
                Ok((
                    WalEntry::MajorCompaction {
                        start_sst_blob_ids,
                        end_sst_blob_id,
                    },
                    checksum,
                ))
            }
            WalEntry::MANIFEST_SNAPSHOT_TAG => {
                let (sst_count, mut checksum) = WalEntryBlock::recover_var_int(reader, checksum)?;
                let mut sst_blob_ids = Vec::with_capacity(sst_count.value() as usize);
                for _ in 0..sst_count.value() {
                    let (sst_blob_id, new_checksum) =
                        WalEntryBlock::recover_length_prefixed_buffer(reader, checksum)?;
                    checksum = new_checksum;
                    sst_blob_ids.push(String::from_utf8(sst_blob_id)?);
                }
                Ok((WalEntry::ManifestSnapshot { sst_blob_ids }, checksum))
            }
            _ => Err(WalError::RecoveryError(
                "could not recognize entry tag".to_string(),
            )),
        }
    }

    fn recover_length_prefixed_buffer<RC: ReadCursor>(
        reader: &mut RC,
        prev_checksum: Checksum,
    ) -> Result<(Vec<u8>, Checksum), WalError> {
        let (buf_len, checksum) = WalEntryBlock::recover_var_int(reader, prev_checksum)?;
        let mut buf = vec![0_u8; buf_len.value() as usize];
        if let Err(_err) = reader.read_exact(&mut buf) {
            return Err(WalError::RecoveryError(format!(
                "could not read key with length {}",
                buf.len()
            )));
        }
        let checksum = crc32(checksum, &buf);
        Ok((buf, checksum))
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
        let start1 = start;
        let len1 = len / 2;
        let block_data = match self.write_entries(writer, start1, len1, prev) {
            Ok(check) => check,
            Err(err) => return Err(err),
        };
        let prev_bytes_written = block_data.bytes_written;
        let start2 = start + len1;
        let len2 = len - len1;
        let mut block_data =
            match self.write_entries_halved(writer, start2, len2, block_data.checksum) {
                Ok(check) => check,
                Err(err) => return Err(err),
            };
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
            return Err(WalError::WriteEntryError(format!("{:?}", err)));
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
            return Err(WalError::WriteEntryError(format!("{:?}", err)));
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
