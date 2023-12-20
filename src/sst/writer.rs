use std::fs::File;
use std::io::Read;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use crate::common::try_u64;
use crate::common::try_usize;
use crate::kv::TombstonePairLike;
use crate::sst::common::KeyRange;
use crate::sst::common::SstError;
use crate::sst::metadata::MetadataBlock;
use crate::sst::metadata::SstMetadata;
use crate::storage::blob_store::BlobStore;
use crate::storage::blob_store::BlobStoreError;
use crate::storage::blob_store::WriteCursor;
use crate::var_int::VarInt64;

type BlobId = String;

#[derive(Clone, Debug)]
pub(crate) struct SstWriteSettings {
    /// block_size is the size of the block in bytes
    target_block_size_bytes: usize,
    /// block_length is the size of the block in number of pairs
    target_keys_per_block: usize,
    /// whether to keep tombstones
    keep_tombstones: bool,
    /// the target SST size in bytes
    target_sst_size: usize,
    /// whether the current compaction is a minor compaction
    writing_minor_sst: bool,
}

impl SstWriteSettings {
    pub(crate) fn discard_tombstones(mut self) -> Self {
        self.keep_tombstones = false;
        self
    }

    pub(crate) fn keep_tombstones(mut self) -> Self {
        self.keep_tombstones = true;
        self
    }

    fn scale(mut self, blob_up_factor: usize) -> Self {
        self.target_block_size_bytes *= blob_up_factor;
        self.target_keys_per_block *= blob_up_factor;
        self.target_sst_size *= blob_up_factor;
        self
    }

    pub(crate) fn scale_to_level(mut self, level: usize, blob_up_factor: usize) -> Self {
        for _ in 0..level {
            self = self.scale(blob_up_factor);
        }
        self
    }

    pub(crate) fn set_writing_minor_sst(mut self, b: bool) -> Self {
        self.writing_minor_sst = b;
        if b {
            self.keep_tombstones = b;
        }
        self
    }

    pub(crate) fn with_target_block_size(mut self, target_block_size: usize) -> Self {
        self.target_block_size_bytes = target_block_size;
        self
    }

    pub(crate) fn with_target_keys_per_block(mut self, target_keys_per_block: usize) -> Self {
        self.target_keys_per_block = target_keys_per_block;
        self
    }
}

const BASE_SST_SIZE: usize = 8 * 1_024 * 1_024;

impl Default for SstWriteSettings {
    fn default() -> Self {
        SstWriteSettings {
            target_block_size_bytes: 16 * 1024 * 1024, // 16 MB
            target_keys_per_block: 512 * 1024,         /* only hit if average record size
                                                        * < 32 bytes */
            keep_tombstones: true,
            target_sst_size: BASE_SST_SIZE,
            writing_minor_sst: false,
        }
    }
}

// TODO(t/1381): There should be a checksum at the end of each block and at
// the end of the SST.
struct BlockBuilder {
    write_settings: SstWriteSettings,
    // used for prefix delta compression
    previous_key: Vec<u8>,
    last_common_len: usize,
    key_count: u64,
    bytes_written: usize,
}

/// This is for data blocks only.
impl BlockBuilder {
    fn new(write_settings: SstWriteSettings) -> BlockBuilder {
        BlockBuilder {
            write_settings,
            previous_key: Vec::new(),
            last_common_len: 0,
            key_count: 0,
            bytes_written: 0,
        }
    }

    fn write_to_file<WC: WriteCursor>(
        &mut self,
        writer: &mut WC,
        buf: &[u8],
    ) -> Result<usize, SstError> {
        self.bytes_written += buf.len();
        match writer.write(buf) {
            Ok(_) => Ok(buf.len()),
            Err(err) => Err(SstError::Write(format!(
                "could not write to file: {:?}",
                err
            ))),
        }
    }

    fn write<WC: WriteCursor>(
        &mut self,
        writer: &mut WC,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<usize, SstError> {
        self.key_count += 1;

        let mut bytes_written = 0;

        // common key length
        let mut my_common_len = 0;
        let key_data = key;
        let prev_data = self.previous_key.to_vec();
        if !key_data.is_empty() && !prev_data.is_empty() {
            loop {
                if key_data[my_common_len] != prev_data[my_common_len] {
                    break;
                }
                my_common_len += 1;
                if my_common_len >= key_data.len() || my_common_len >= prev_data.len() {
                    break;
                }
            }
        }
        self.last_common_len = my_common_len;
        let common_len = match VarInt64::try_from(my_common_len) {
            Ok(common_len) => common_len,
            Err(err) => {
                return Err(SstError::Parse(format!(
                    "could not parse lengths {}: {:?}",
                    my_common_len, err
                )))
            }
        };
        bytes_written += self.write_to_file(writer, common_len.data_ref())?;

        // key diff value
        let key_len = try_u64(key.len())?;
        let diff_len_value = key_len - common_len.value();
        let diff_len = match VarInt64::try_from(diff_len_value) {
            Ok(diff_len) => diff_len,
            Err(err) => {
                return Err(SstError::Parse(format!(
                    "could not parse lengths {}: {:?}",
                    diff_len_value, err
                )))
            }
        };
        bytes_written += self.write_to_file(writer, diff_len.data_ref())?;
        let common_len_value = try_usize(common_len.value())?;
        bytes_written += self.write_to_file(writer, &key[common_len_value..])?;
        let option = value;
        self.previous_key = key.to_vec();
        let value = if let Some(v) = option {
            v
        } else {
            bytes_written += self.write_to_file(writer, &[0])?;
            return Ok(bytes_written);
        };
        // value
        bytes_written += self.write_to_file(writer, &[0xFF])?;
        let value_len = match VarInt64::try_from(value.len()) {
            Ok(x) => x,
            Err(err) => {
                return Err(SstError::Parse(format!(
                    "could not parse lengths {}: {:?}",
                    value.len(),
                    err
                )))
            }
        };
        bytes_written += self.write_to_file(writer, value_len.data_ref())?;
        bytes_written += self.write_to_file(writer, value)?;
        Ok(bytes_written)
    }

    fn is_done(&self) -> bool {
        self.bytes_written >= self.write_settings.target_keys_per_block
            || self.bytes_written >= self.write_settings.target_block_size_bytes
            || (self.last_common_len == 0 && self.key_count > 1)
    }

    fn is_new(&self) -> bool {
        self.bytes_written == 0
    }
}

struct WriteTracker<WC: WriteCursor> {
    writer: WC,
    bytes_written: usize,
}

impl<WC: WriteCursor> WriteTracker<WC> {
    fn new(writer: WC) -> WriteTracker<WC> {
        WriteTracker {
            writer,
            bytes_written: 0,
        }
    }
}

impl<WC: WriteCursor> WriteCursor for WriteTracker<WC> {
    fn write(&mut self, buf: &[u8]) -> Result<(), BlobStoreError> {
        self.bytes_written += buf.len();
        self.writer.write(buf)
    }

    fn finalize(self) -> Result<(), BlobStoreError> {
        self.writer.finalize()
    }
}
pub(crate) struct SstBuilder<WC: WriteCursor> {
    blob_id: String,
    writer: WriteTracker<WC>,
    write_settings: SstWriteSettings,
    current_block: BlockBuilder,
    metadata_block: MetadataBlock,
    lowest_key: Option<Vec<u8>>,
    highest_key: Option<Vec<u8>>,
}

impl<WC: WriteCursor> SstBuilder<WC> {
    fn new(writer: WC, blob_id: &str, write_settings: SstWriteSettings) -> SstBuilder<WC> {
        SstBuilder {
            blob_id: blob_id.to_string(),
            writer: WriteTracker::new(writer),
            write_settings: write_settings.clone(),
            current_block: BlockBuilder::new(write_settings),
            metadata_block: MetadataBlock::default(),
            lowest_key: None,
            highest_key: None,
        }
    }

    fn write(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<(), SstError> {
        if self.current_block.is_new() {
            let offset = try_u64(self.writer.bytes_written)?;
            self.metadata_block.index(offset, key);
        }

        self.metadata_block.filter_key(key);

        if self.lowest_key.is_none() {
            self.lowest_key = Some(key.to_vec());
        }
        self.highest_key = Some(key.to_vec());

        self.current_block.write(&mut self.writer, key, value)?;

        if self.current_block.is_done() {
            self.current_block = BlockBuilder::new(self.write_settings.clone());
        }
        Ok(())
    }

    fn close(mut self) -> Result<(), SstError> {
        let highest_key = if let Some(k) = self.highest_key {
            k
        } else {
            return Err(SstError::EmptySst("highest key is none".to_string()));
        };
        let lowest_key = if let Some(k) = self.lowest_key {
            k
        } else {
            return Err(SstError::EmptySst("lowest key is none".to_string()));
        };
        let key_range = KeyRange::new(lowest_key, highest_key);
        self.metadata_block
            .set_sst_metadata(SstMetadata::new(&self.blob_id, key_range));
        let buf = Vec::try_from(&self.metadata_block)?;
        let buf_len: u64 = try_u64(buf.len())?;
        let buf_len_bytes = buf_len.to_be_bytes();
        let total = [buf, buf_len_bytes.to_vec()].concat();
        if let Err(err) = self.writer.write(&total) {
            return Err(SstError::Write(format!(
                "could not write metadata block: {:?}",
                err
            )));
        }
        if let Err(err) = self.writer.finalize() {
            return Err(SstError::Write(format!(
                "could not finalize file: {:?}",
                err
            )));
        }
        Ok(())
    }

    fn is_done(&self) -> bool {
        self.writer.bytes_written >= self.write_settings.target_sst_size / 2
    }
}

pub(crate) struct SstWriter<B: BlobStore> {
    blob_store: B,
    write_settings: SstWriteSettings,
}

impl<B: BlobStore> SstWriter<B> {
    pub(crate) fn new(blob_store: B, write_settings: SstWriteSettings) -> SstWriter<B> {
        SstWriter {
            blob_store,
            write_settings,
        }
    }

    fn new_sst_builder(
        &self,
        is_minor_sst: bool,
        includes_tombstones: bool,
    ) -> Result<SstBuilder<B::WriteCursor>, SstError> {
        let mut blob_id: String;
        let blob_writer: <B as BlobStore>::WriteCursor;

        // TODO(t/1374): The maximum retry count should be
        // configurable.
        let max_create_blob_attempts = 3;
        let mut attempt = 0;
        loop {
            // TODO(t/1396): There should be a better way to generate
            // new blob names.
            let mut base_name = "".to_string();

            // first try to get a random number
            if let Ok(mut f) = File::open("/dev/urandom") {
                let mut buf = [0u8; 16];
                if let Ok(n) = f.read(&mut buf) {
                    if n == 16 {
                        base_name = buf.map(|b| b.to_string()).into_iter().collect();
                    }
                }
            }

            // if that fails, try to use the time stamp in epoch nanos
            if base_name.is_empty() {
                if let Ok(d) = SystemTime::now().duration_since(UNIX_EPOCH) {
                    base_name = d.as_nanos().to_string();
                }
            }

            // If that fails, use the constant name "new".
            // If this completely fails more than once, the collision will
            // cause a new SST not to be written.
            if base_name.is_empty() {
                base_name = "new".to_string();
            }

            blob_id = if is_minor_sst {
                format!("{}.minor.sst", base_name)
            } else if !includes_tombstones {
                format!("{}.base.sst", base_name)
            } else {
                format!("{}.sst", base_name)
            };

            let blob_store = self.blob_store.clone();
            match blob_store.create_blob(&blob_id) {
                Ok(writer) => {
                    blob_writer = writer;
                    break;
                }
                Err(e) => {
                    if attempt < max_create_blob_attempts - 1 {
                        attempt += 1;
                        continue;
                    }
                    return Err(SstError::Write(format!("failed to create blob: {:?}", e)));
                }
            }
        }
        let sst_builder = SstBuilder::new(blob_writer, &blob_id, self.write_settings.clone());
        Ok(sst_builder)
    }

    pub(crate) fn write_all<E, L: TombstonePairLike, I: Iterator<Item = Result<L, E>>>(
        self,
        pairs: I,
    ) -> Result<Vec<BlobId>, SstError>
    where
        SstError: From<E>,
    {
        let is_minor_sst = self.write_settings.writing_minor_sst;
        let includes_tombstones = self.write_settings.keep_tombstones;
        let mut sst_builder = self.new_sst_builder(is_minor_sst, includes_tombstones)?;
        let mut blob_ids = vec![sst_builder.blob_id.clone()];
        for result in pairs {
            if !is_minor_sst && sst_builder.is_done() {
                sst_builder.close()?;
                sst_builder = self.new_sst_builder(is_minor_sst, includes_tombstones)?;
                blob_ids.push(sst_builder.blob_id.clone());
            }
            let pair = result?;
            let key_ref = pair.key_ref();
            if let Some(value_ref) = pair.value_ref().as_ref() {
                sst_builder.write(key_ref, Some(value_ref))?;
                continue;
            }
            if !self.write_settings.keep_tombstones {
                continue;
            }
            sst_builder.write(key_ref, None)?;
        }
        sst_builder.close()?;
        Ok(blob_ids)
    }
}
