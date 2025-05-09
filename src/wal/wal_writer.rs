use std::fmt::Debug;

use crate::checksum::crc32;
use crate::storage::blob_store::WriteCursor;
use crate::wal::wal_entry::{Checksum, WalEntry, WalEntryBlock, WalError};

// TODO(t/1336): Consider using async/await instead of channels.

pub(crate) const INIT_WAL_CHECKSUM: Checksum = 1337;

#[derive(Debug)]
pub(crate) struct WalWriter<WC> {
    blob_writer: WC,
    entry_buf: Vec<WalEntry>,
    prev_checksum: Checksum,
    max_wal_size_bytes: usize,
    total_bytes_written: usize,
    rotation_requested: bool,
}

impl<WC: WriteCursor> WalWriter<WC> {
    pub(crate) fn new(blob_writer: WC, max_wal_size_bytes: usize) -> Self {
        let entry_buf = Vec::new();
        let prev_checksum = crc32(INIT_WAL_CHECKSUM, &[0x47, 0x53, 0x00, 0x00]);
        WalWriter {
            blob_writer,
            entry_buf,
            prev_checksum,
            max_wal_size_bytes,
            total_bytes_written: 0,
            rotation_requested: false,
        }
    }

    /// Returns true if the WAL is full and a rotation is required.
    pub(crate) fn write(&mut self, entry: WalEntry) -> Result<bool, WalError> {
        self.entry_buf.push(entry);
        self.flush_buffer()
    }

    /// Returns true if a rotation is required.
    fn flush_buffer(&mut self) -> Result<bool, WalError> {
        let entries: Vec<WalEntry> = self.entry_buf.drain(..).collect();
        let block = WalEntryBlock::new(entries);
        let block_data = match block.write(&mut self.blob_writer, self.prev_checksum) {
            Ok(block_data) => block_data,
            Err(_err) => {
                // TODO(t/1348): Crash and recover this WAL.
                panic!("not implemented");
            }
        };
        self.prev_checksum = block_data.checksum;
        self.total_bytes_written += block_data.bytes_written;
        if !self.rotation_requested && self.total_bytes_written >= self.max_wal_size_bytes {
            self.rotation_requested = true;
            return Ok(true);
        }
        Ok(false)
    }

    // TODO(anvil_db): This should be called only when the WAL goes out of scope.
    /// WARNING: Do not use after this has been successfully closed.
    pub(crate) fn close(&mut self) -> Result<(), WalError> {
        self.flush_buffer()?;
        self.blob_writer.flush()?;
        Ok(())
    }
}
