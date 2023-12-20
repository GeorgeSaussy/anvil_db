use super::BlockCache;
use crate::checksum::crc32;
use crate::context::Context;
use crate::sst::block_cache::common::CacheError;
use crate::sst::reader::ThinSstMetadata;

struct CurrentBlockData {
    block_start: usize,
    data: Vec<u8>,
}

pub(crate) struct BlockScanner {
    blob_id_hash: u32,
    offset: usize,
    current_block: Option<CurrentBlockData>,
}

impl BlockScanner {
    pub(crate) fn new(blob_id: &str) -> Self {
        // TODO(gs): Is CRC 32 a good hash for this application?
        BlockScanner {
            blob_id_hash: crc32(1, blob_id.as_bytes()),
            offset: 0,
            current_block: None,
        }
    }

    pub(crate) fn skip(&mut self, offset: usize) -> Result<(), CacheError> {
        self.offset = offset;
        self.current_block = None;
        Ok(())
    }

    pub(crate) fn read_exact<Ctx: Context>(
        &mut self,
        ctx: &Ctx,
        meta: &ThinSstMetadata,
        buf: &mut [u8],
    ) -> Result<(), CacheError> {
        // make sure block is loaded by the scanner
        if self.current_block.is_none() {
            let index_offsets = meta.offsets_ref();
            if index_offsets.is_empty() {
                return Err(CacheError::NoBlocks);
            }
            let mut offset_idx: Option<usize> = None;
            for (idx, (block_start, _)) in index_offsets.iter().enumerate() {
                if self.offset > *block_start {
                    continue;
                }
                if self.offset == *block_start {
                    offset_idx = Some(idx);
                    break;
                }
                offset_idx = Some(idx - 1);
                break;
            }
            let block_start = if let Some(offset_idx) = offset_idx {
                index_offsets[offset_idx].0
            } else {
                index_offsets[index_offsets.len() - 1].0
            };

            let block_cache = ctx.block_cache_ref();
            let blob_store = ctx.blob_store_ref();
            let block_id = crc32(self.blob_id_hash, &block_start.to_be_bytes());
            let block = block_cache.read_block(blob_store, meta, block_id, block_start)?;
            self.current_block = Some(CurrentBlockData {
                block_start,
                data: block,
            });
        }

        let current_block = self.current_block.as_ref().unwrap();
        let write_start = self.offset - current_block.block_start;
        let write_end = if write_start + buf.len() > current_block.data.len() {
            current_block.data.len()
        } else {
            write_start + buf.len()
        };
        buf[..write_end - write_start].copy_from_slice(&current_block.data[write_start..write_end]);
        self.offset += write_end - write_start;
        if self.offset >= current_block.block_start + current_block.data.len() {
            self.current_block = None;
        }
        Ok(())
    }
}
