use std::f64::consts::LN_2;
use std::sync::atomic::{AtomicU8, Ordering};

use crate::common::join_byte_arrays;
use crate::var_int::VarInt64;

fn rotl_32(k: &mut u32, r: u8) {
    *k = (*k << r) | (*k >> (32 - r));
}

fn murmur_hash_3_32(key: &[u8], seed: u32) -> u32 {
    let num_blocks = key.len() / 4;
    let mut h1 = seed;

    const C1: u32 = 0xcc9e2d51;
    const C2: u32 = 0x1b873593;

    for i in 0..num_blocks {
        let block_idx = 4 * i;
        let mut k1 = key[block_idx] as u32
            | (key[block_idx + 1] as u32) << 8
            | (key[block_idx + 2] as u32) << 16
            | (key[block_idx + 3] as u32) << 24;

        k1 = k1.wrapping_mul(C1);
        rotl_32(&mut k1, 15);
        k1 = k1.wrapping_mul(C2);

        h1 ^= k1;
        rotl_32(&mut h1, 13);
        h1 = h1.wrapping_mul(5).wrapping_add(0xe6546b64);
    }

    let block_idx = 4 * num_blocks;
    let mut k1 = 0;
    let r = key.len() & 3;
    if r >= 3 {
        k1 ^= (key[block_idx + 2] as u32) << 16;
    }
    if r >= 2 {
        k1 ^= (key[block_idx + 1] as u32) << 8;
    }
    if r >= 1 {
        k1 ^= key[block_idx] as u32;
    }
    k1 = k1.wrapping_mul(C1);
    rotl_32(&mut k1, 15);
    k1 = k1.wrapping_mul(C2);
    h1 ^= k1;

    let len = if let Ok(len) = u32::try_from(key.len()) {
        len
    } else {
        // NOTE: The hash will not be reliably unique for keys longer
        // than 2^32 bytes.
        u32::MAX
    };
    h1 ^= len;

    h1 ^= h1 >> 16;
    h1 = h1.wrapping_mul(0x85ebca6b);
    h1 ^= h1 >> 13;
    h1 = h1.wrapping_mul(0xc2b2ae35);
    h1 ^= h1 >> 16;

    h1
}

/// Say the Bloom filter has a fixed size of m bits and k hash functions.
/// The probability of a false positive after n insertions is then:
///
/// P = (1 - (1 - 1/m) ** kn) ** k
///
/// This is approximately:
///
/// P = (1 - exp(-kn/m)) ** k
///
/// We want to find the value of k that minimizes the probability of a
/// false positive. The internet says the optimal value of k is:
///
/// k* ~= (m / n) * log(2)
///
/// TODO(t/1388): Derive this expression.
pub(crate) trait BloomFilter {
    fn add(&mut self, key: &[u8]);
    fn contains(&self, key: &[u8]) -> bool;
}

trait ByteLike {
    fn zero() -> Self;
    fn get(&self) -> u8;
    fn or_in_place(&mut self, b: u8);
}
impl ByteLike for u8 {
    fn zero() -> Self {
        0_u8
    }

    fn get(&self) -> u8 {
        *self
    }

    fn or_in_place(&mut self, b: u8) {
        *self |= b;
    }
}
impl ByteLike for AtomicU8 {
    fn zero() -> Self {
        AtomicU8::new(0)
    }

    fn get(&self) -> u8 {
        self.load(Ordering::Acquire)
    }

    fn or_in_place(&mut self, b: u8) {
        self.fetch_or(b, Ordering::Release);
    }
}

#[derive(Clone, Debug)]
struct InnerBloomFilter<T: ByteLike> {
    cells: Vec<T>,
    seeds: Vec<u32>,
}

impl<T: ByteLike> InnerBloomFilter<T> {
    pub(crate) fn new(allowed_bytes: usize, expected_capacity: usize) -> Self {
        let mut num_cells = 2;
        while num_cells < allowed_bytes {
            num_cells *= 2;
        }
        num_cells /= 2;
        let cells = (0..num_cells).map(|_| T::zero()).collect();

        let num_seeds = ((8 * num_cells) as f64 / expected_capacity as f64 * LN_2).ceil() as usize;
        let seeds: Vec<u32> = (0..num_seeds).map(|i| u32::try_from(i).unwrap()).collect();

        InnerBloomFilter { cells, seeds }
    }

    fn get_bit(&self, bit_id: usize) -> bool {
        let cell_id = bit_id / 8;
        let bit_id = bit_id % 8;
        let cell = self.cells[cell_id].get();
        (cell & (1 << bit_id)) != 0
    }
}

impl<T: ByteLike> BloomFilter for InnerBloomFilter<T> {
    fn add(&mut self, key: &[u8]) {
        for seed in &self.seeds {
            let hash = murmur_hash_3_32(key, *seed);
            let bit_id = hash as usize % (8 * self.cells.len());
            let cell_id = bit_id / 8;
            let bit_id = bit_id % 8;
            let byte = 1_u8 << bit_id;
            self.cells[cell_id].or_in_place(byte);
        }
    }

    fn contains(&self, key: &[u8]) -> bool {
        for seed in &self.seeds {
            let hash = murmur_hash_3_32(key, *seed);
            let bit_id = hash as usize % (8 * self.cells.len());
            if !self.get_bit(bit_id) {
                return false;
            }
        }
        true
    }
}

impl<T: ByteLike> TryFrom<&[u8]> for InnerBloomFilter<T> {
    type Error = String;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let total_len = match VarInt64::try_from(value) {
            Ok(var_int) => var_int,
            Err(_) => {
                return Err("failed to parse bloom filter offset".to_string());
            }
        };
        let mut offset: usize = total_len.len();
        let num_seeds = match VarInt64::try_from(value[offset..].as_ref()) {
            Ok(var_int) => var_int,
            Err(_) => {
                return Err("failed to parse bloom filter num_hashed".to_string());
            }
        };
        offset += num_seeds.len();

        let seeds_len = match usize::try_from(num_seeds.value()) {
            Ok(len) => len,
            Err(_) => {
                return Err("failed convert number of seeds to usize".to_string());
            }
        };
        let mut seeds = Vec::with_capacity(seeds_len);
        for _ in 0..num_seeds.value() {
            if value.len() - offset < 4 {
                return Err("not enough room for the number of hash seeds claimed".to_string());
            }
            let be = [
                value[offset],
                value[offset + 1],
                value[offset + 2],
                value[offset + 3],
            ];
            let seed = u32::from_be_bytes(be);
            seeds.push(seed);
            offset += 4;
        }

        let cells_len = match VarInt64::try_from(value[offset..].as_ref()) {
            Ok(var_int) => var_int,
            Err(_) => {
                return Err("failed to parse bloom filter cells_len".to_string());
            }
        };
        offset += cells_len.len();

        let u = match usize::try_from(cells_len.value()) {
            Ok(len) => len,
            Err(_) => {
                return Err("failed convert number of cells to usize".to_string());
            }
        };
        if offset + u > value.len() {
            return Err("not enough room for the number of cells claimed".to_string());
        }
        let cells = value[offset..(offset + u)]
            .iter()
            .map(|b| {
                let mut z = T::zero();
                z.or_in_place(*b);
                z
            })
            .collect::<Vec<T>>();

        Ok(InnerBloomFilter { cells, seeds })
    }
}

impl<T: ByteLike> TryFrom<&InnerBloomFilter<T>> for Vec<u8> {
    type Error = String;

    fn try_from(value: &InnerBloomFilter<T>) -> Result<Self, Self::Error> {
        let num_seeds = match VarInt64::try_from(value.seeds.len()) {
            Ok(num_seeds) => num_seeds,
            Err(_) => {
                return Err("failed to convert num_seeds to usize".to_string());
            }
        };

        let seed_buf = value
            .seeds
            .iter()
            .flat_map(|x: &u32| x.to_be_bytes())
            .collect::<Vec<u8>>();

        let cells_len = match VarInt64::try_from(value.cells.len()) {
            Ok(cell_len) => cell_len,
            Err(_) => {
                return Err("failed to convert cell_len to VarInt64".to_string());
            }
        };

        let cells_buf = value.cells.iter().map(|x| x.get()).collect::<Vec<u8>>();
        let total_len = match VarInt64::try_from(
            num_seeds.len() + seed_buf.len() + cells_len.len() + cells_buf.len(),
        ) {
            Ok(total_len) => total_len,
            Err(_) => {
                return Err("failed to convert total_len to VarInt64".to_string());
            }
        };

        Ok(join_byte_arrays(vec![
            total_len.data_ref(),
            num_seeds.data_ref(),
            seed_buf.as_ref(),
            cells_len.data_ref(),
            cells_buf.as_ref(),
        ]))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct BasicBloomFilter {
    inner: InnerBloomFilter<u8>,
}

impl BasicBloomFilter {
    pub(crate) fn new(allowed_bytes: usize, expected_capacity: usize) -> Self {
        let inner = InnerBloomFilter::new(allowed_bytes, expected_capacity);
        BasicBloomFilter { inner }
    }
}

impl BloomFilter for BasicBloomFilter {
    fn add(&mut self, key: &[u8]) {
        self.inner.add(key);
    }

    fn contains(&self, key: &[u8]) -> bool {
        self.inner.contains(key)
    }
}

impl TryFrom<&[u8]> for BasicBloomFilter {
    type Error = String;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let inner = InnerBloomFilter::try_from(value)?;
        Ok(BasicBloomFilter { inner })
    }
}

impl TryFrom<&BasicBloomFilter> for Vec<u8> {
    type Error = String;

    fn try_from(value: &BasicBloomFilter) -> Result<Self, Self::Error> {
        Vec::try_from(&value.inner)
    }
}

pub(crate) struct ConcurrentBloomFilter {
    inner: InnerBloomFilter<AtomicU8>,
}

impl BloomFilter for ConcurrentBloomFilter {
    fn add(&mut self, key: &[u8]) {
        self.inner.add(key);
    }

    fn contains(&self, key: &[u8]) -> bool {
        self.inner.contains(key)
    }
}

impl From<&BasicBloomFilter> for ConcurrentBloomFilter {
    fn from(value: &BasicBloomFilter) -> Self {
        let cells = value
            .inner
            .cells
            .iter()
            .map(|x| AtomicU8::new(x.get()))
            .collect();
        let inner = InnerBloomFilter {
            cells,
            seeds: value.inner.seeds.clone(),
        };
        ConcurrentBloomFilter { inner }
    }
}

impl TryFrom<&[u8]> for ConcurrentBloomFilter {
    type Error = String;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let inner = InnerBloomFilter::try_from(value)?;
        Ok(ConcurrentBloomFilter { inner })
    }
}

impl TryFrom<&ConcurrentBloomFilter> for Vec<u8> {
    type Error = String;

    fn try_from(value: &ConcurrentBloomFilter) -> Result<Self, Self::Error> {
        Vec::try_from(&value.inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_murmur_hash() {
        assert_eq!(murmur_hash_3_32(b"Hello, World!", 1337), 1074930736);
        assert_eq!(
            murmur_hash_3_32(b"This is a longer string.", 42),
            3413765881
        );
    }

    #[test]
    fn test_bloom_filter() {
        let mut bf = ConcurrentBloomFilter {
            inner: InnerBloomFilter::new(100, 1000),
        };
        bf.add(b"hello");
        assert!(bf.contains(b"hello"));
        assert!(!bf.contains(b"world"));
    }
}
