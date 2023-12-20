use super::common::SstReadError;
use crate::bloom_filter::BasicBloomFilter;
use crate::bloom_filter::BloomFilter;
use crate::common::join_byte_arrays;
use crate::common::try_u64;
use crate::common::try_usize;
use crate::sst::common::KeyRange;
use crate::sst::common::SstError;
use crate::storage::blob_store::BlobStore;
use crate::storage::blob_store::ReadCursor;
use crate::var_int::VarInt64;

/// A struct to encapsulate the metadata for an sst.
#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub(crate) struct SstMetadata {
    /// the filepath, serves as unique identifier
    blob_id: String,
    /// the key range in the sst
    key_range: KeyRange,
}

impl SstMetadata {
    /// Get an initialized SstMetadata instance
    /// # Arguments
    /// - id: the identifier for the sst
    /// - level: the sst's level
    /// - blob_id: the filepath for the sst
    /// - key_range: the key range in the sst
    /// # Returns
    /// A new new SstMetadata instance
    pub(crate) fn new(blob_id: &str, key_range: KeyRange) -> SstMetadata {
        SstMetadata {
            blob_id: blob_id.to_string(),
            key_range,
        }
    }

    pub(crate) fn empty() -> SstMetadata {
        SstMetadata {
            blob_id: String::from(""),
            key_range: KeyRange::new(vec![], vec![]),
        }
    }

    #[cfg(test)]
    pub(crate) fn blob_id_ref(&self) -> &String {
        &self.blob_id
    }

    #[cfg(test)]
    pub(crate) fn key_range_ref(&self) -> &KeyRange {
        &self.key_range
    }
}

impl TryFrom<&SstMetadata> for Vec<u8> {
    type Error = SstError;

    fn try_from(metadata: &SstMetadata) -> Result<Vec<u8>, Self::Error> {
        let blob_id_buf = metadata.blob_id.clone().into_bytes();
        let blob_id_len = VarInt64::try_from(blob_id_buf.len())?;

        let start_key = metadata.key_range.start_ref();
        let start_key_len = VarInt64::try_from(start_key.len())?;

        let end_key = metadata.key_range.end_ref();
        let end_key_len = VarInt64::try_from(end_key.len())?;

        let arrays = vec![
            blob_id_len.data_ref(),
            &blob_id_buf,
            start_key_len.data_ref(),
            start_key,
            end_key_len.data_ref(),
            end_key,
        ];
        Ok(join_byte_arrays(arrays))
    }
}

impl TryFrom<&[u8]> for SstMetadata {
    type Error = SstError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let mut offset = 0;

        // parse blob_id
        let blob_id_len = match VarInt64::try_from(&value[offset..]) {
            Ok(var_int) => var_int,
            Err(e) => return Err(SstError::Parse(format!("could not parse id_len: {}", e))),
        };
        offset += blob_id_len.len();

        let blob_id_data = &value[offset..(offset + blob_id_len.value() as usize)];
        let blob_id = match std::str::from_utf8(blob_id_data) {
            Ok(s) => s.to_string(),
            Err(e) => return Err(SstError::Parse(format!("could not parse id: {}", e))),
        };
        offset += blob_id_len.value() as usize;

        // parse start key
        let var_int = match VarInt64::try_from(&value[offset..]) {
            Ok(var_int) => var_int,
            Err(e) => {
                return Err(SstError::Parse(format!(
                    "could not parse start_key_len: {}",
                    e
                )))
            }
        };
        let start_len = var_int.value();
        offset += var_int.len();
        let start = value[offset..(offset + start_len as usize)].to_vec();
        offset += start_len as usize;

        // parse end key
        let var_int = match VarInt64::try_from(value[offset..].as_ref()) {
            Ok(var_int) => var_int,
            Err(e) => {
                return Err(SstError::Parse(format!(
                    "could not parse end_key_len: {}",
                    e
                )))
            }
        };
        let end_len = var_int.value();
        offset += var_int.len();
        let end = value[offset..(offset + end_len as usize)].to_vec();
        // offset += end_len as usize;

        let key_range = KeyRange::new(start, end);
        Ok(SstMetadata { blob_id, key_range })
    }
}

#[derive(Clone)]
pub(crate) struct MetadataBlock {
    sst_metadata: SstMetadata,
    offsets: Vec<(u64, Vec<u8>)>,
    bloom_filter: BasicBloomFilter,
    key_range: KeyRange,
}

impl MetadataBlock {
    pub(crate) fn from_blob<B: BlobStore>(
        store: &B,
        blob_id: &str,
    ) -> Result<(MetadataBlock, usize), SstError> {
        let mut cursor = match store.read_cursor(blob_id) {
            Ok(rc) => rc,
            Err(err) => {
                return Err(
                    SstReadError::BlobStore((format!("blob_id: {:?}", blob_id), err)).into(),
                )
            }
        };
        let blob_size = cursor.size()?;
        if let Err(err) = cursor.seek_from_end(8_usize) {
            return Err(SstReadError::BlobStore((
                "could not seek to offset 8 from end".to_string(),
                err,
            ))
            .into());
        }
        let mut buf = [0_u8; 8];
        if let Err(err) = cursor.read_exact(&mut buf) {
            return Err(SstReadError::BlobStore((
                "could not read metadata length from file".to_string(),
                err,
            ))
            .into());
        }
        let metadata_len = try_usize(u64::from_be_bytes(buf))?;
        if let Err(err) = cursor.seek_from_end(8_usize + metadata_len) {
            return Err(SstReadError::BlobStore((
                format!(
                    "could not seek to offset {} kb from end",
                    (8_usize + metadata_len) / 1024
                ),
                err,
            ))
            .into());
        }
        let mut buf = vec![0; metadata_len];
        if let Err(err) = cursor.read_exact(&mut buf) {
            return Err(SstReadError::BlobStore((
                "could not read metadata from file: {:?}".to_string(),
                err,
            ))
            .into());
        }
        let metadata_block = match MetadataBlock::try_from(buf.as_slice()) {
            Ok(x) => x,
            Err(err) => {
                return Err(SstError::Parse(format!(
                    "could not parse metadata block from bytes: {:?}",
                    err
                )))
            }
        };
        Ok((metadata_block, blob_size))
    }

    pub(crate) fn index(&mut self, offset: u64, key: &[u8]) {
        self.offsets.push((offset, key.to_vec()));
    }

    pub(crate) fn filter_key(&mut self, key: &[u8]) {
        self.bloom_filter.add(key);
    }

    pub(crate) fn bloom_filter_ref(&self) -> &BasicBloomFilter {
        &self.bloom_filter
    }

    pub(crate) fn key_range_ref(&self) -> &KeyRange {
        &self.key_range
    }

    pub(crate) fn sst_metadata(&self) -> SstMetadata {
        self.sst_metadata.clone()
    }

    pub(crate) fn values_ref(&self) -> &Vec<(u64, Vec<u8>)> {
        &self.offsets
    }

    pub(crate) fn set_sst_metadata(&mut self, metadata: SstMetadata) {
        self.sst_metadata = metadata.clone();
        self.key_range = metadata.key_range;
    }
}

impl Default for MetadataBlock {
    fn default() -> Self {
        MetadataBlock {
            offsets: Vec::new(),
            bloom_filter: BasicBloomFilter::new(32, 1024),
            key_range: KeyRange::new(Vec::new(), Vec::new()),
            sst_metadata: SstMetadata::empty(),
        }
    }
}

impl TryFrom<&MetadataBlock> for Vec<u8> {
    type Error = SstError;

    fn try_from(block: &MetadataBlock) -> Result<Vec<u8>, Self::Error> {
        let bf_buf: Vec<u8> = match Vec::try_from(&block.bloom_filter) {
            Ok(buf) => buf,
            Err(e) => {
                return Err(SstError::Parse(format!(
                    "could not parse bloom_filter: {}",
                    e
                )))
            }
        };
        let mut data = Vec::new();
        for pair in &block.offsets {
            let offset = VarInt64::try_from(pair.0)?;
            let len = VarInt64::try_from(offset.len() + pair.1.len())?;

            data.extend_from_slice(len.data_ref());
            data.extend_from_slice(offset.data_ref());
            data.extend_from_slice(&pair.1);
        }
        let bf_len = VarInt64::try_from(bf_buf.len())?;
        let val_buf = data.to_vec();
        let val_len = VarInt64::try_from(val_buf.len())?;
        let start = block.key_range.start_ref();
        let start_len = VarInt64::try_from(start.len())?;
        let end = block.key_range.end_ref();
        let end_len = VarInt64::try_from(end.len())?;
        let metadata = block.sst_metadata();
        let metadata_buf: Vec<u8> = match Vec::try_from(&metadata) {
            Ok(buf) => buf,
            Err(e) => {
                return Err(SstError::Parse(format!(
                    "could not parse metadata: {:?}",
                    e
                )))
            }
        };
        let metadata_len = VarInt64::try_from(metadata_buf.len())?;
        fn u64_len(v: &VarInt64) -> Result<u64, SstError> {
            Ok(try_u64(v.len())?)
        }
        let total_len = match VarInt64::try_from(
            u64_len(&bf_len)?
                + bf_len.value()
                + u64_len(&val_len)?
                + val_len.value()
                + u64_len(&start_len)?
                + start_len.value()
                + u64_len(&end_len)?
                + end_len.value()
                + u64_len(&metadata_len)?
                + metadata_len.value(),
        ) {
            Ok(var_int) => var_int,
            Err(err) => {
                return Err(SstError::Parse(format!(
                    "could not parse total_len: {:?}",
                    err
                )))
            }
        };
        Ok(join_byte_arrays(vec![
            total_len.data_ref(),
            bf_len.data_ref(),
            &bf_buf,
            val_len.data_ref(),
            &val_buf,
            start_len.data_ref(),
            start,
            end_len.data_ref(),
            end,
            metadata_len.data_ref(),
            &metadata_buf,
        ]))
    }
}

impl TryFrom<&[u8]> for MetadataBlock {
    type Error = SstError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let mut offset = 0;

        // parse total_len
        let total_len = match VarInt64::try_from(value) {
            Ok(var_int) => var_int,
            Err(e) => return Err(SstError::Parse(format!("could not parse total_len: {}", e))),
        };
        offset += total_len.len();

        // parse bf_len
        let bf_len = match VarInt64::try_from(&value[offset..]) {
            Ok(var_int) => var_int,
            Err(e) => return Err(SstError::Parse(format!("could not parse bf_len: {}", e))),
        };
        offset += bf_len.len();

        // parse bloom_filter
        let bf_buf = &value[offset..(offset + bf_len.value() as usize)];
        let bloom_filter = match BasicBloomFilter::try_from(bf_buf) {
            Ok(bf) => bf,
            Err(e) => {
                return Err(SstError::Parse(format!(
                    "could not parse bloom_filter: {}",
                    e
                )))
            }
        };
        offset += bf_len.value() as usize;

        // parse val_len
        let val_len = match VarInt64::try_from(&value[offset..]) {
            Ok(var_int) => var_int,
            Err(e) => return Err(SstError::Parse(format!("could not parse val_len: {}", e))),
        };
        offset += val_len.len();

        // parse val
        let last = offset + (val_len.value() as usize);
        let mut values = Vec::new();
        while offset < last {
            let len = match VarInt64::try_from(&value[offset..]) {
                Ok(var_int) => var_int,
                Err(e) => return Err(SstError::Parse(format!("could not parse len: {}", e))),
            };
            offset += len.len();

            let off = match VarInt64::try_from(&value[offset..]) {
                Ok(var_int) => var_int,
                Err(e) => return Err(SstError::Parse(format!("could not parse offset: {}", e))),
            };
            offset += off.len();
            let key: Vec<u8> = value[offset..(offset + len.value() as usize - off.len())].to_vec();
            offset += key.len();
            values.push((off.value(), key));
        }

        // parse start_len
        let start_len = match VarInt64::try_from(&value[offset..]) {
            Ok(var_int) => var_int,
            Err(e) => return Err(SstError::Parse(format!("could not parse start_len: {}", e))),
        };
        offset += start_len.len();

        // parse start
        let start: Vec<u8> = value[offset..(offset + start_len.value() as usize)].to_vec();
        offset += start.len();

        // parse end_len
        let end_len = match VarInt64::try_from(&value[offset..]) {
            Ok(var_int) => var_int,
            Err(e) => return Err(SstError::Parse(format!("could not parse end_len: {}", e))),
        };
        offset += end_len.len();

        // parse end
        let end = value[offset..(offset + end_len.value() as usize)].to_vec();
        offset += end.len();

        // parse metadata_len
        let metadata_len = match VarInt64::try_from(&value[offset..]) {
            Ok(var_int) => var_int,
            Err(e) => {
                return Err(SstError::Parse(format!(
                    "could not parse metadata_len: {}",
                    e
                )))
            }
        };
        offset += metadata_len.len();

        // parse metadata
        let metadata_buf = &value[offset..(offset + metadata_len.value() as usize)];
        let metadata = match SstMetadata::try_from(metadata_buf) {
            Ok(md) => md,
            Err(e) => {
                return Err(SstError::Parse(format!(
                    "could not parse metadata: {:?}",
                    e
                )))
            }
        };
        Ok(MetadataBlock {
            bloom_filter,
            offsets: values,
            key_range: KeyRange::new(start, end),
            sst_metadata: metadata,
        })
    }
}

#[cfg(test)]
mod test {
    use std::cmp::Ordering;

    use super::*;
    use crate::common::cmp_key;

    fn some_start() -> Vec<u8> {
        vec![0, 1, 2]
    }

    fn some_end() -> Vec<u8> {
        vec![3, 4, 5]
    }

    fn some_sst_metadata() -> SstMetadata {
        let start = vec![0, 1, 2];
        let end = vec![3, 4, 5];
        let key_range = KeyRange::new(start, end);
        let blob_id = "/tmp/.sst_test_path";
        let _id = "an id string";
        SstMetadata::new(blob_id, key_range)
    }

    #[test]
    fn test_sst_metadata_new() {
        let _ = some_sst_metadata();
    }

    #[test]
    fn test_sst_metadata_start() {
        let sst_metadata = some_sst_metadata();
        assert_eq!(
            cmp_key(sst_metadata.key_range_ref().start_ref(), &some_start()),
            Ordering::Equal
        );
    }

    #[test]
    fn test_sst_metadata_end() {
        let sst_metadata = some_sst_metadata();
        assert_eq!(
            cmp_key(sst_metadata.key_range_ref().end_ref(), &some_end()),
            Ordering::Equal
        );
    }

    #[test]
    fn test_sst_metadata_serialization() {
        let old_sst_metadata = some_sst_metadata();
        let buf = Vec::try_from(&old_sst_metadata).expect("could not serialize sst metadata");
        let new_sst_metadata =
            SstMetadata::try_from(buf.as_ref()).expect("could not deserialize sst metadata");
        assert_eq!(
            new_sst_metadata.blob_id_ref(),
            old_sst_metadata.blob_id_ref()
        );
        assert_eq!(
            cmp_key(
                new_sst_metadata.key_range_ref().start_ref(),
                old_sst_metadata.key_range_ref().start_ref()
            ),
            Ordering::Equal
        );
        assert_eq!(
            cmp_key(
                new_sst_metadata.key_range_ref().end_ref(),
                old_sst_metadata.key_range_ref().end_ref()
            ),
            Ordering::Equal
        );
    }

    // METADATA BLOCK

    #[test]
    fn test_metadatablock_chordata() {
        let top: u64 = 100;
        let mut mb = MetadataBlock::default();
        for k in 0..top {
            let key = k.to_be_bytes();
            mb.index(k, &key);
            mb.filter_key(&key);
        }
        let sst_metadata = SstMetadata::new(
            "some_id",
            KeyRange::new(
                0_u64.to_be_bytes().into_iter().collect(),
                (top - 1).to_be_bytes().into_iter().collect(),
            ),
        );
        mb.set_sst_metadata(sst_metadata);
        let buf = Vec::try_from(&mb).expect("could not serialize metadata");
        let mb2 = MetadataBlock::try_from(buf.as_ref()).expect("could not deserialize metadata");

        let v1 = mb.values_ref();
        let v2 = mb2.values_ref();
        assert_eq!(v1.len(), v2.len());
        let l = v1.len();
        for k in 0..l {
            assert_eq!(v1[k], v2[k]);
        }
    }
}
