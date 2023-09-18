use std::cmp::Ordering;
use std::sync::mpsc::Sender;
use std::sync::Arc;

use super::block_cache::StorageWrapper;
use crate::bloom_filter::BasicBloomFilter;
use crate::bloom_filter::BloomFilter;
use crate::common::cmp_key;
use crate::common::try_usize;
use crate::compactor::CompactorMessage;
use crate::kv::TombstoneIterator;
use crate::kv::TombstonePair;
use crate::kv::TombstonePointReader;
use crate::kv::TombstoneValue;
use crate::kv::TryTombstoneScanner;
use crate::sst::common::KeyRange;
use crate::sst::common::KeyRangeCmp;
use crate::sst::common::SstError;
use crate::sst::metadata::MetadataBlock;
use crate::storage::blob_store::{BlobStore, ReadCursor};
use crate::var_int::VarInt64;

#[derive(Debug)]
struct BlobDropper {
    blob_id: String,
    compactor_tx: Sender<CompactorMessage>,
}

impl BlobDropper {
    fn new(blob_id: &str, compactor_tx: Sender<CompactorMessage>) -> BlobDropper {
        BlobDropper {
            blob_id: blob_id.to_string(),
            compactor_tx,
        }
    }
}

impl Drop for BlobDropper {
    fn drop(&mut self) {
        let _ = self
            .compactor_tx
            .send(CompactorMessage::DeleteBlob(self.blob_id.clone()));
    }
}

struct CachedReader<BC: StorageWrapper> {
    storage_wrapper: BC,
    blob_id: String,
    to_skip: usize,
    reader: Option<<<BC as StorageWrapper>::B as BlobStore>::RC>,
}

// TODO(t/1387): This does not actually use the block cache.
impl<BC: StorageWrapper> CachedReader<BC> {
    fn skip(&mut self, offset: usize) -> Result<(), SstError> {
        self.to_skip = offset;
        Ok(())
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), SstError> {
        if let Some(reader) = self.reader.as_mut() {
            reader.read_exact(buf)?;
            return Ok(());
        }
        let blob_store = self.storage_wrapper.blob_store_ref();
        let mut reader = blob_store.read_cursor(&self.blob_id)?;
        if self.to_skip > 0 {
            reader.skip(self.to_skip)?;
            self.to_skip = 0;
        }
        reader.read_exact(buf)?;
        self.reader = Some(reader);
        Ok(())
    }
}

pub(crate) struct SstScanner<BC: StorageWrapper> {
    reader: CachedReader<BC>,
    buffer: Vec<u8>,
    previous_key: Vec<u8>,
    return_next: Option<TombstonePair>,
    last_key: Option<Vec<u8>>,
    poisoned: bool,
    done: bool,
    #[allow(dead_code)]
    blob_dropper: Arc<BlobDropper>,
}

impl<BC: StorageWrapper> SstScanner<BC> {
    fn new(
        storage_wrapper: BC,
        blob_id: &str,
        last_key: Option<Vec<u8>>,
        offset: usize,
        blob_dropper: Arc<BlobDropper>,
    ) -> Result<SstScanner<BC>, SstError> {
        let mut reader = CachedReader {
            storage_wrapper,
            blob_id: blob_id.to_string(),
            to_skip: 0,
            reader: None,
        };
        reader.skip(offset)?;
        Ok(SstScanner {
            reader,
            buffer: Vec::new(),
            previous_key: Vec::new(),
            return_next: None,
            last_key,
            poisoned: false,
            done: false,
            blob_dropper,
        })
    }

    pub(crate) fn start(&mut self) -> Result<TombstonePair, SstError> {
        self.next().unwrap()
    }

    fn poison(&mut self, err: SstError) -> Result<TombstonePair, SstError> {
        self.poisoned = true;
        Err(err)
    }

    pub(crate) fn next(&mut self) -> Option<Result<TombstonePair, SstError>> {
        if self.poisoned || self.done {
            return None;
        }

        if let Some(last_key) = &self.last_key {
            if self.previous_key == *last_key {
                self.done = true;
                return None;
            }
        }

        if let Some(pair) = self.return_next.clone() {
            self.return_next = None;
            self.previous_key = pair.key_ref().to_vec();
            return Some(Ok(pair));
        }

        // common len
        let mut buf = vec![0_u8; 8 - self.buffer.len()];
        if let Err(err) = self.reader.read_exact(&mut buf) {
            return Some(self.poison(SstError::Read(format!(
                "could not read from file: {:?}",
                err
            ))));
        }
        self.buffer.extend_from_slice(&buf);
        let common_len = match VarInt64::try_from(self.buffer.as_slice()) {
            Ok(var_int) => var_int,
            Err(err) => {
                return Some(self.poison(SstError::Read(format!(
                    "could not parse a VarInt64 from bytes: {:?}",
                    err
                ))));
            }
        };
        self.buffer = self.buffer[common_len.len()..].to_vec();

        // diff_len
        let mut buf = vec![0_u8; 8 - self.buffer.len()];
        if let Err(err) = self.reader.read_exact(&mut buf) {
            return Some(self.poison(SstError::Read(format!(
                "could not read from file: {:?}",
                err
            ))));
        }
        self.buffer.extend_from_slice(&buf);
        let diff_len = match VarInt64::try_from(self.buffer.as_slice()) {
            Ok(var_int) => var_int,
            Err(err) => {
                return Some(self.poison(SstError::Read(format!(
                    "could not parse a VarInt64 from bytes: {:?}",
                    err
                ))));
            }
        };
        self.buffer = self.buffer[diff_len.len()..].to_vec();

        // parse key diff
        let diff_len_value = match try_usize(diff_len.value()) {
            Ok(d) => d,
            Err(err) => return Some(self.poison(err.into())),
        };
        if self.buffer.len() < diff_len_value {
            let mut buf = vec![0_u8; diff_len_value - self.buffer.len()];
            if let Err(err) = self.reader.read_exact(&mut buf) {
                return Some(self.poison(SstError::Read(format!(
                    "could not read from file: {:?}",
                    err
                ))));
            }
            self.buffer.extend_from_slice(&buf);
        }
        let key_diff = self.buffer[0..diff_len_value].to_vec();
        self.buffer = self.buffer[diff_len_value..].to_vec();

        // construct key
        let common_len_value = match try_usize(common_len.value()) {
            Ok(d) => d,
            Err(err) => return Some(self.poison(err.into())),
        };
        if common_len_value > self.previous_key.len() {
            return Some(self.poison(SstError::Read(format!(
                "common_len_value is greater than previous_key.len(): {} > {}",
                common_len_value,
                self.previous_key.len()
            ))));
        }
        let common = self.previous_key[0..common_len_value].to_vec();
        let key = [common, key_diff].concat();

        // parse value_or_del_mark
        if self.buffer.is_empty() {
            let mut buf = [0_u8];
            if let Err(err) = self.reader.read_exact(&mut buf) {
                return Some(self.poison(SstError::Read(format!(
                    "could not read from file: {:?}",
                    err
                ))));
            }
            self.buffer.extend_from_slice(&buf);
        }
        let byte = self.buffer[0];
        self.buffer = self.buffer[1..].to_vec();
        if byte == 0 {
            self.previous_key = key.clone();
            return Some(Ok(TombstonePair::deletion_marker(key)));
        }
        // parse val_len
        if self.buffer.len() < 8 {
            let mut buf = vec![0_u8; 8 - self.buffer.len()];
            if let Err(err) = self.reader.read_exact(&mut buf) {
                return Some(self.poison(SstError::Read(format!(
                    "could not read from file: {:?}",
                    err
                ))));
            }
            self.buffer.extend_from_slice(&buf);
        }

        let val_len = match VarInt64::try_from(self.buffer.as_slice()) {
            Ok(var_int) => var_int,
            Err(err) => {
                return Some(self.poison(SstError::Read(format!(
                    "could not parse a VarInt64 from bytes: {:?}",
                    err
                ))))
            }
        };
        self.buffer = self.buffer[val_len.len()..].to_vec();

        if self.buffer.len() < val_len.value() as usize {
            let mut buf = vec![0_u8; val_len.value() as usize - self.buffer.len()];
            if let Err(err) = self.reader.read_exact(&mut buf) {
                return Some(self.poison(SstError::Read(format!(
                    "could not read from file: {:?}",
                    err
                ))));
            }
            self.buffer.extend_from_slice(&buf);
        }
        let value = self.buffer[0..val_len.value() as usize].to_vec();
        self.buffer = self.buffer[val_len.value() as usize..].to_vec();

        if let Some(last_key) = self.last_key.as_ref() {
            if key >= *last_key {
                self.done = true;
            }
        }
        self.previous_key = key.clone();
        Some(Ok(TombstonePair::new(key, value)))
    }
}

impl<BC: StorageWrapper> Iterator for SstScanner<BC> {
    type Item = Result<TombstonePair, SstError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next()
    }
}

impl<BC: StorageWrapper> TombstoneIterator for SstScanner<BC> {
    type E = SstError;

    fn from(mut self, key: &[u8]) -> Self {
        loop {
            if let Some(Ok(pair)) = self.next() {
                if cmp_key(pair.key_ref(), key) == Ordering::Less {
                    continue;
                }
                self.return_next = Some(pair);
                break;
            }
        }
        self
    }

    fn to(mut self, key: &[u8]) -> Self {
        self.last_key = Some(key.to_vec());
        self
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SstReader<BC: StorageWrapper> {
    storage_wrapper: BC,
    blob_id: String,
    bloom_filter: BasicBloomFilter,
    // TODO(t/1389): `offsets` is immutable; is there a better
    // data structure for this workload?
    offsets: Vec<(u64, Vec<u8>)>,
    key_range: KeyRange,
    blob_dropper: Arc<BlobDropper>,
    /// size of the blob in bytes
    blob_size: usize,
}

impl<BC: StorageWrapper> SstReader<BC> {
    pub(crate) fn new(
        storage_wrapper: BC,
        blob_id: &str,
        compactor_tx: Sender<CompactorMessage>,
    ) -> Result<SstReader<BC>, SstError> {
        let blob_store = storage_wrapper.blob_store_ref();
        let (metadata, blob_size) = MetadataBlock::from_blob(blob_store.clone(), blob_id)?;

        let offsets = metadata.values_ref().clone();
        let key_range = metadata.key_range_ref().clone();
        let blob_id = blob_id.to_string();
        let blob_dropper = Arc::new(BlobDropper::new(&blob_id, compactor_tx));
        Ok(SstReader {
            blob_id,
            storage_wrapper,
            bloom_filter: metadata.bloom_filter_ref().clone(),
            offsets,
            key_range,
            blob_dropper,
            blob_size,
        })
    }

    pub(crate) fn blob_id_ref(&self) -> &str {
        &self.blob_id
    }

    pub(crate) fn key_range_ref(&self) -> &KeyRange {
        &self.key_range
    }

    pub(crate) fn size(&self) -> usize {
        self.blob_size
    }
}

impl<BC: StorageWrapper> TombstonePointReader for SstReader<BC> {
    type E = SstError;

    fn get(&self, key: &[u8]) -> Result<Option<TombstoneValue>, SstError> {
        if self.key_range.in_range(key) != KeyRangeCmp::InRange {
            // TODO(gs): Is this check needed?
            return Ok(None);
        }
        if !self.bloom_filter.contains(key) {
            return Ok(None);
        }
        if self.offsets.is_empty() {
            return Err(SstError::Internal("sst offsets are empty".to_string()));
        }
        fn get_scanner<BC2: StorageWrapper>(
            my_storage_wrapper: &BC2,
            my_key: &[u8],
            my_blob_id: &str,
            my_offsets: &Vec<(u64, Vec<u8>)>,
            blob_dropper: Arc<BlobDropper>,
        ) -> Result<Option<SstScanner<BC2>>, SstError> {
            assert!(!my_offsets.is_empty());

            let key_vec = my_key.to_vec();
            let mut offset_idx = 0;
            loop {
                if my_offsets[offset_idx].1.cmp(&key_vec) == Ordering::Greater {
                    if offset_idx == 0 {
                        return Ok(None);
                    }
                    offset_idx -= 1;
                    break;
                }
                offset_idx += 1;
                if offset_idx == my_offsets.len() {
                    offset_idx -= 1;
                    break;
                }
            }
            let offset = my_offsets[offset_idx].0;
            let offset = try_usize(offset)?;
            let scanner = SstScanner::new(
                my_storage_wrapper.clone(),
                my_blob_id,
                None,
                offset,
                blob_dropper,
            )?;
            Ok(Some(scanner))
        }
        let option = get_scanner(
            &self.storage_wrapper,
            key,
            &self.blob_id,
            &self.offsets,
            self.blob_dropper.clone(),
        )?;
        let mut scanner = match option {
            Some(scanner) => scanner,
            None => {
                return Ok(None);
            }
        };
        let mut pair = scanner.start()?;
        while cmp_key(pair.key_ref(), key) == Ordering::Less {
            if let Some(result) = scanner.next() {
                match result {
                    Ok(p) => {
                        pair = p;
                    }
                    Err(err) => {
                        return Err(SstError::Internal(format!(
                            "could not read next pair: {:?}",
                            err
                        )))
                    }
                }
            } else {
                return Ok(None);
            }
            if cmp_key(pair.key_ref(), self.key_range.end_ref()) == Ordering::Equal {
                break;
            }
        }
        if cmp_key(pair.key_ref(), key) != Ordering::Equal {
            return Ok(None);
        }
        Ok(Some(pair.value_ref().clone()))
    }
}

impl<BC: StorageWrapper> TryTombstoneScanner for SstReader<BC> {
    type E = SstError;
    type I = SstScanner<BC>;

    fn try_scan(&self) -> Result<Self::I, Self::E> {
        SstScanner::new(
            self.storage_wrapper.clone(),
            &self.blob_id,
            Some(self.key_range.end_ref().to_vec()),
            0,
            self.blob_dropper.clone(),
        )
    }
}
