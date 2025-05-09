use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::sync::mpsc::Sender;
use std::sync::Arc;

use super::block_cache::block_scanner::BlockScanner;
use super::common::SstReadError;
use crate::bloom_filter::BasicBloomFilter;
use crate::bloom_filter::BloomFilter;
use crate::common::cmp_key;
use crate::common::try_usize;
use crate::compactor::CompactorMessage;
use crate::context::Context;
use crate::kv::TombstonePair;
use crate::kv::TombstonePointReader;
use crate::kv::TombstoneValue;
use crate::kv::TryTombstoneScanner;
use crate::kv::{RangeSet, TombstoneIterator};
use crate::sst::common::KeyRange;
use crate::sst::common::KeyRangeCmp;
use crate::sst::common::SstError;
use crate::sst::metadata::MetadataBlock;
use crate::storage::blob_store::BlobStore;
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

pub(crate) struct SstScanner<'a, Ctx: Context> {
    ctx: &'a Ctx,
    meta: ThinSstMetadata,
    reader: BlockScanner,
    buffer: Vec<u8>,
    previous_key: Vec<u8>,
    return_next: Option<TombstonePair>,
    last_key: Option<Vec<u8>>,
    poisoned: bool,
    done: bool,
}

impl<Ctx: Context> Debug for SstScanner<'_, Ctx> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SstScanner").finish()
    }
}

impl<'a, Ctx: Context> SstScanner<'a, Ctx> {
    fn new(
        ctx: &'a Ctx,
        meta: ThinSstMetadata,
        last_key: Option<Vec<u8>>,
        offset: usize,
    ) -> Result<SstScanner<'a, Ctx>, SstError> {
        let mut reader = BlockScanner::new(meta.blob_id_ref());
        reader.skip(offset)?;
        Ok(SstScanner {
            ctx,
            meta,
            reader,
            buffer: Vec::new(),
            previous_key: Vec::new(),
            return_next: None,
            last_key,
            poisoned: false,
            done: false,
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
        if let Err(err) = self.reader.read_exact(self.ctx, &self.meta, &mut buf) {
            return Some(self.poison(SstReadError::Cache(err).into()));
        }
        self.buffer.extend_from_slice(&buf);
        let common_len = match VarInt64::try_from(self.buffer.as_slice()) {
            Ok(var_int) => var_int,
            Err(err) => {
                return Some(
                    self.poison(
                        SstReadError::PossibleCorruption(format!(
                            "could not parse a VarInt64 from bytes: {err:?}",
                        ))
                        .into(),
                    ),
                );
            }
        };
        self.buffer = self.buffer[common_len.len()..].to_vec();

        // diff_len
        let mut buf = vec![0_u8; 8 - self.buffer.len()];
        if let Err(err) = self.reader.read_exact(self.ctx, &self.meta, &mut buf) {
            return Some(self.poison(SstReadError::Cache(err).into()));
        }
        self.buffer.extend_from_slice(&buf);
        let diff_len = match VarInt64::try_from(self.buffer.as_slice()) {
            Ok(var_int) => var_int,
            Err(err) => {
                return Some(
                    self.poison(
                        SstReadError::PossibleCorruption(format!(
                            "could not parse a VarInt64 from bytes: {err:?}",
                        ))
                        .into(),
                    ),
                );
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
            if let Err(err) = self.reader.read_exact(self.ctx, &self.meta, &mut buf) {
                return Some(self.poison(SstReadError::Cache(err).into()));
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
            return Some(
                self.poison(
                    SstReadError::PossibleCorruption(format!(
                        "common_len_value is greater than previous_key.len(): {common_len_value} \
                         > {}",
                        self.previous_key.len()
                    ))
                    .into(),
                ),
            );
        }
        let common = self.previous_key[0..common_len_value].to_vec();
        let key = [common, key_diff].concat();

        // parse value_or_del_mark
        if self.buffer.is_empty() {
            let mut buf = [0_u8];
            if let Err(err) = self.reader.read_exact(self.ctx, &self.meta, &mut buf) {
                return Some(self.poison(SstReadError::Cache(err).into()));
            }
            self.buffer.extend_from_slice(&buf);
        }
        let byte = self.buffer[0];
        self.buffer = self.buffer[1..].to_vec();
        if byte == 0 {
            self.previous_key.clone_from(&key);
            return Some(Ok(TombstonePair::deletion_marker(key)));
        }
        // parse val_len
        if self.buffer.len() < 8 {
            let mut buf = vec![0_u8; 8 - self.buffer.len()];
            if let Err(err) = self.reader.read_exact(self.ctx, &self.meta, &mut buf) {
                return Some(self.poison(SstReadError::Cache(err).into()));
            }
            self.buffer.extend_from_slice(&buf);
        }

        let val_len = match VarInt64::try_from(self.buffer.as_slice()) {
            Ok(var_int) => var_int,
            Err(err) => {
                return Some(
                    self.poison(
                        SstReadError::PossibleCorruption(format!(
                            "could not parse a VarInt64 from bytes: {err:?}",
                        ))
                        .into(),
                    ),
                )
            }
        };
        self.buffer = self.buffer[val_len.len()..].to_vec();

        if self.buffer.len() < val_len.value() as usize {
            let mut buf = vec![0_u8; val_len.value() as usize - self.buffer.len()];
            if let Err(err) = self.reader.read_exact(self.ctx, &self.meta, &mut buf) {
                return Some(self.poison(SstReadError::Cache(err).into()));
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
        self.previous_key.clone_from(&key);
        Some(Ok(TombstonePair::new(key, value)))
    }
}

impl<Ctx: Context> Iterator for SstScanner<'_, Ctx> {
    type Item = Result<TombstonePair, SstError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next()
    }
}

impl<Ctx: Context> RangeSet for SstScanner<'_, Ctx> {
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

impl<Ctx: Context> TombstoneIterator for SstScanner<'_, Ctx> {
    type Error = SstError;
}

/// ThinSstMetadata is the same as SstReader, but without the Bloom filter
/// data.
#[derive(Clone, Debug)]
pub(crate) struct ThinSstMetadata {
    blob_id: String,
    // TODO(t/1389): `offsets` is immutable; is there a better
    // data structure for this workload?
    offsets: Vec<(usize, Vec<u8>)>,
    key_range: KeyRange,
    /// size of the blob in bytes
    blob_size: usize,
    #[allow(dead_code)]
    blob_dropper: Arc<BlobDropper>,
}

impl ThinSstMetadata {
    pub(crate) fn blob_id_ref(&self) -> &str {
        &self.blob_id
    }

    pub(crate) fn offsets_ref(&self) -> &Vec<(usize, Vec<u8>)> {
        &self.offsets
    }

    pub(crate) fn blob_size(&self) -> usize {
        self.blob_size
    }
}

#[derive(Clone)]
pub(crate) struct SstReader {
    bloom_filter: BasicBloomFilter,
    thin_metadata: ThinSstMetadata,
}

impl Debug for SstReader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SstReader")
            .field("blob_id", &self.thin_metadata.blob_id_ref())
            .finish()
    }
}

impl SstReader {
    pub(crate) fn new<B: BlobStore>(
        blob_store: &B,
        blob_id: &str,
        compactor_tx: Sender<CompactorMessage>,
    ) -> Result<SstReader, SstError> {
        let (metadata, blob_size) = MetadataBlock::from_blob(blob_store, blob_id)?;

        let offsets: Result<Vec<(usize, Vec<u8>)>, _> = metadata
            .values_ref()
            .iter()
            .map(|(offset, key)| {
                let offset = match usize::try_from(*offset) {
                    Ok(offset) => offset,
                    Err(err) => {
                        return Err(SstError::Internal(format!(
                            "could not convert offset to usize: {err:?}",
                        )))
                    }
                };
                Ok((offset, key.clone()))
            })
            .collect();
        let offsets = offsets?;

        let key_range = metadata.key_range_ref().clone();
        let blob_id = blob_id.to_string();
        let blob_dropper = Arc::new(BlobDropper::new(&blob_id, compactor_tx));
        let meta = ThinSstMetadata {
            blob_id,
            offsets,
            key_range,
            blob_dropper,
            blob_size,
        };
        let reader = SstReader {
            bloom_filter: metadata.bloom_filter_ref().clone(),
            thin_metadata: meta,
        };
        Ok(reader)
    }

    pub(crate) fn blob_id_ref(&self) -> &str {
        &self.thin_metadata.blob_id
    }

    pub(crate) fn key_range_ref(&self) -> &KeyRange {
        &self.thin_metadata.key_range
    }

    pub(crate) fn offsets_ref(&self) -> &Vec<(usize, Vec<u8>)> {
        self.thin_metadata.offsets_ref()
    }

    pub(crate) fn blob_size(&self) -> usize {
        self.thin_metadata.blob_size
    }
}

impl TombstonePointReader for SstReader {
    type Error = SstError;

    fn get<Ctx: Context>(&self, ctx: &Ctx, key: &[u8]) -> Result<Option<TombstoneValue>, SstError> {
        if self.key_range_ref().in_range(key) != KeyRangeCmp::InRange {
            // TODO(gs): Is this check needed?
            return Ok(None);
        }
        if !self.bloom_filter.contains(key) {
            return Ok(None);
        }
        if self.offsets_ref().is_empty() {
            return Err(SstError::Internal("sst offsets are empty".to_string()));
        }

        fn get_scanner<'a, Ctx2: Context>(
            my_ctx: &'a Ctx2,
            meta: ThinSstMetadata,
            my_key: &[u8],
        ) -> Result<Option<SstScanner<'a, Ctx2>>, SstError> {
            let offsets = meta.offsets_ref();
            assert!(!offsets.is_empty());

            let key_vec = my_key.to_vec();
            let mut offset_idx = 0;
            loop {
                if offsets[offset_idx].1.cmp(&key_vec) == Ordering::Greater {
                    if offset_idx == 0 {
                        return Ok(None);
                    }
                    offset_idx -= 1;
                    break;
                }
                offset_idx += 1;
                if offset_idx == offsets.len() {
                    offset_idx -= 1;
                    break;
                }
            }
            let offset = offsets[offset_idx].0;
            let scanner = SstScanner::new(my_ctx, meta, None, offset)?;
            Ok(Some(scanner))
        }
        let option = get_scanner(ctx, self.thin_metadata.clone(), key)?;
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
                            "could not read next pair: {err:?}",
                        )))
                    }
                }
            } else {
                return Ok(None);
            }
            if cmp_key(pair.key_ref(), self.key_range_ref().end_ref()) == Ordering::Equal {
                break;
            }
        }
        if cmp_key(pair.key_ref(), key) != Ordering::Equal {
            return Ok(None);
        }
        Ok(Some(pair.value_ref().clone()))
    }
}

impl TryTombstoneScanner for SstReader {
    type Error = SstError;
    type Iter<'a, Ctx>
        = SstScanner<'a, Ctx>
    where
        Ctx: Context + 'a;

    fn try_scan<'a, Ctx: Context>(&self, ctx: &'a Ctx) -> Result<Self::Iter<'a, Ctx>, Self::Error> {
        SstScanner::new(
            ctx,
            self.thin_metadata.clone(),
            Some(self.key_range_ref().end_ref().to_vec()),
            0,
        )
    }
}
