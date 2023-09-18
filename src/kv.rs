use std::{cmp::Ordering, fmt::Debug};

use crate::{
    common::{cmp_key, join_byte_arrays, try_u64},
    var_int::VarInt64,
};

pub(crate) trait KeyValuePointReader {
    type E;
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::E>;
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum TombstoneValue {
    Value(Vec<u8>),
    Tombstone,
}

impl TombstoneValue {
    pub(crate) fn as_ref(&self) -> Option<&Vec<u8>> {
        match self {
            TombstoneValue::Tombstone => None,
            TombstoneValue::Value(v) => Some(v),
        }
    }

    pub(crate) fn from<T: TombstoneValueLike>(value: &T) -> TombstoneValue {
        match value.as_ref() {
            Some(v) => TombstoneValue::Value(v.to_vec()),
            None => TombstoneValue::Tombstone,
        }
    }
}

impl From<TombstoneValue> for Option<Vec<u8>> {
    fn from(v: TombstoneValue) -> Self {
        match v {
            TombstoneValue::Tombstone => None,
            TombstoneValue::Value(v) => Some(v),
        }
    }
}

pub(crate) trait TombstoneValueLike {
    fn as_ref(&self) -> Option<&[u8]>;
}

impl TombstoneValueLike for TombstoneValue {
    fn as_ref(&self) -> Option<&[u8]> {
        match self {
            TombstoneValue::Tombstone => None,
            TombstoneValue::Value(v) => Some(v.as_ref()),
        }
    }
}

impl TombstoneValueLike for &[u8] {
    fn as_ref(&self) -> Option<&[u8]> {
        Some(self)
    }
}

impl TombstoneValueLike for Option<&[u8]> {
    fn as_ref(&self) -> Option<&[u8]> {
        *self
    }
}

/// A struct to encapsulate a key-value pair.
#[derive(Clone, Debug)]
pub(crate) struct TombstonePair {
    // the key value
    key: Vec<u8>,
    // If value is none, then the pair represents a deletion marker.
    value: TombstoneValue,
}

impl TombstonePair {
    /// Create a new Pair instance.
    /// # Arguments
    /// - key: the key for the new Pair instance
    /// - value: the value for the new Pair instance
    /// # Returns
    /// A new Pair instance.
    pub(crate) fn new(key: Vec<u8>, value: Vec<u8>) -> TombstonePair {
        TombstonePair {
            key,
            value: TombstoneValue::Value(value),
        }
    }

    /// Create a new deletion marker.
    /// # Arguments
    /// - key: the key to delete
    /// # Returns
    /// A deletion marker for a given key.
    pub(crate) fn deletion_marker(key: Vec<u8>) -> TombstonePair {
        TombstonePair {
            key,
            value: TombstoneValue::Tombstone,
        }
    }

    /// Get the key from a Pair.
    /// # Returns
    /// The key value.
    pub(crate) fn key_ref(&self) -> &[u8] {
        &self.key
    }

    /// Get the value of a Pair instance's value.
    /// # Returns
    /// The Pair instance's value.
    pub(crate) fn value_ref(&self) -> &TombstoneValue {
        &self.value
    }
}

impl From<(Vec<u8>, TombstoneValue)> for TombstonePair {
    fn from((key, value): (Vec<u8>, TombstoneValue)) -> Self {
        TombstonePair { key, value }
    }
}

pub(crate) trait TombstonePairLike {
    fn key_ref(&self) -> &[u8];
    fn value_ref(&self) -> &TombstoneValue;
}

impl TombstonePairLike for TombstonePair {
    fn key_ref(&self) -> &[u8] {
        self.key_ref()
    }

    fn value_ref(&self) -> &TombstoneValue {
        self.value_ref()
    }
}

impl TryInto<Vec<u8>> for TombstonePair {
    type Error = String;

    /// Convert a Pair instance into a Vec<u8>.
    /// The layout contains the key and then the value in an encoded format.
    ///
    /// The key is encoded as a VarInt64 containing the length of the key,
    /// followed by the raw bytes of the key.
    ///
    /// The encoding of the value portion of the serialization depends on
    /// whether a value is present or not. If not present, then the entire
    /// value portion is encoded as a single byte of 0x00. If present, then
    /// then the value portion is encoded as a single byte of 0xFF, followed
    /// by a VarInt64 containing the length of the value, followed by the raw
    /// bytes of the value.
    ///
    /// # Returns
    ///
    /// A Vec<u8> representation of a Pair instance or an error message if it
    /// cannot be encoded.
    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        let key_len_u64: u64 = try_u64(self.key.len())?;
        let key_len = VarInt64::try_from(key_len_u64)?;
        let val_cord: Vec<u8> = match self.value_ref() {
            TombstoneValue::Value(v) => {
                let val_len_u64 = try_u64(v.len())?;
                let val_len = VarInt64::try_from(val_len_u64)?;
                join_byte_arrays(vec![&[0xFF], val_len.data_ref(), v])
            }
            TombstoneValue::Tombstone => vec![0x00],
        };
        Ok(join_byte_arrays(vec![
            key_len.data_ref(),
            &self.key,
            &val_cord,
        ]))
    }
}

pub(crate) trait TombstonePointReader {
    type E: Debug;
    fn get(&self, key: &[u8]) -> Result<Option<TombstoneValue>, Self::E>;
}

pub(crate) trait TombstoneIterator: Iterator<Item = Result<TombstonePair, Self::E>> {
    type E;

    fn from(self, key: &[u8]) -> Self;
    fn to(self, key: &[u8]) -> Self;
}

pub(crate) trait TombstoneScanner {
    type E;
    type I: TombstoneIterator<E = Self::E>;

    fn scan(&self) -> Self::I;
}

pub(crate) trait TryTombstoneScanner {
    type E;
    type I: TombstoneIterator<E = Self::E>;

    fn try_scan(&self) -> Result<Self::I, Self::E>;
}

pub(crate) trait TombstoneStore: TombstonePointReader + TombstoneScanner {
    type E;

    fn set<T: TombstoneValueLike>(
        &self,
        key: &[u8],
        value: &T,
    ) -> Result<(), <Self as TombstoneStore>::E>;
}

struct ScanRunner<I: TombstoneIterator> {
    iter: I,
    still_running: bool,
    last_pair: Option<TombstonePair>,
}

impl<I: TombstoneIterator> ScanRunner<I> {
    fn step(&mut self) -> Result<(), I::E> {
        if self.still_running {
            self.still_running = false;
            self.last_pair = None;
            if let Some(result) = self.iter.next() {
                self.still_running = true;
                self.last_pair = match result {
                    Ok(pair) => Some(pair),
                    Err(err) => return Err(err),
                };
            }
        }
        Ok(())
    }
}

pub(crate) struct MergedHomogenousIter<I: TombstoneIterator> {
    started: bool,
    runners: Vec<ScanRunner<I>>,
}

impl<I: TombstoneIterator> MergedHomogenousIter<I> {
    // If a key is occurs in more than one iterator, the later iterator's value
    // will be used.
    pub(crate) fn new<J: Iterator<Item = I>>(iters: J) -> MergedHomogenousIter<I> {
        let runners = iters
            .map(|iter| ScanRunner {
                iter,
                still_running: true,
                last_pair: None,
            })
            .collect();
        MergedHomogenousIter {
            started: false,
            runners,
        }
    }
}

impl<I: TombstoneIterator> Iterator for MergedHomogenousIter<I> {
    type Item = Result<TombstonePair, I::E>;

    fn next(&mut self) -> Option<Self::Item> {
        // start the runners if not yet dequeued
        if !self.started {
            for runner in &mut self.runners {
                if let Err(err) = runner.step() {
                    return Some(Err(err));
                }
            }
            self.started = true;
        }

        // find the lowest pair indexes, including ties
        let mut lowest_idx = Vec::new();
        let mut lowest_pair: Option<TombstonePair> = None;
        for (idx, runner) in self.runners.iter().enumerate() {
            let pair = if let Some(pair) = &runner.last_pair {
                pair
            } else {
                continue;
            };
            if let Some(lp) = lowest_pair.clone() {
                let lk = lp.key_ref();
                match cmp_key(pair.key_ref(), lk) {
                    Ordering::Less => {
                        lowest_idx.clear();
                        lowest_idx.push(idx);
                        lowest_pair = Some(pair.clone());
                    }
                    Ordering::Equal => {
                        lowest_idx.push(idx);
                        lowest_pair = Some(pair.clone());
                    }
                    Ordering::Greater => {}
                }
            } else {
                lowest_idx.push(idx);
                lowest_pair = Some(pair.clone());
            }
        }

        lowest_pair.as_ref()?;

        for idx in lowest_idx {
            if let Err(err) = self.runners[idx].step() {
                return Some(Err(err));
            }
        }

        lowest_pair.map(Ok)
    }
}

impl<I: TombstoneIterator> TombstoneIterator for MergedHomogenousIter<I> {
    type E = I::E;

    fn from(self, key: &[u8]) -> Self {
        let mut runners = Vec::with_capacity(self.runners.len());
        for runner in self.runners {
            runners.push(ScanRunner {
                iter: runner.iter.from(key),
                still_running: runner.still_running,
                last_pair: runner.last_pair,
            });
        }
        MergedHomogenousIter {
            started: self.started,
            runners,
        }
    }

    fn to(self, key: &[u8]) -> Self {
        let mut runners = Vec::with_capacity(self.runners.len());
        for runner in self.runners {
            runners.push(ScanRunner {
                iter: runner.iter.to(key),
                still_running: runner.still_running,
                last_pair: runner.last_pair,
            });
        }
        MergedHomogenousIter {
            started: self.started,
            runners,
        }
    }
}

/// The first iterator's keys take precedence over the second iterator's keys.
pub(crate) struct JoinedIter<E, A: TombstoneIterator<E = E>, B: TombstoneIterator<E = E>> {
    a: ScanRunner<A>,
    b: ScanRunner<B>,
    started: bool,
}

impl<E, A: TombstoneIterator<E = E>, B: TombstoneIterator<E = E>> JoinedIter<E, A, B> {
    pub(crate) fn new(a: A, b: B) -> Self {
        JoinedIter {
            a: ScanRunner {
                iter: a,
                still_running: true,
                last_pair: None,
            },
            b: ScanRunner {
                iter: b,
                still_running: true,
                last_pair: None,
            },
            started: false,
        }
    }
}

impl<E, A: TombstoneIterator<E = E>, B: TombstoneIterator<E = E>> Iterator for JoinedIter<E, A, B> {
    type Item = Result<TombstonePair, E>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.started {
            if let Err(err) = self.a.step() {
                return Some(Err(err));
            }
            if let Err(err) = self.b.step() {
                return Some(Err(err));
            }
            self.started = true;
        }

        let mut step_a = false;
        let mut step_b = false;
        let mut next_pair: Option<TombstonePair> = None;
        if let Some(last_a) = self.a.last_pair.as_ref() {
            if let Some(last_b) = self.b.last_pair.as_ref() {
                match cmp_key(last_a.key_ref(), last_b.key_ref()) {
                    Ordering::Less => {
                        step_a = true;
                        next_pair = Some(last_a.clone());
                    }
                    Ordering::Equal => {
                        step_a = true;
                        step_b = true;
                        next_pair = Some(last_a.clone());
                    }
                    Ordering::Greater => {
                        step_b = true;
                        next_pair = Some(last_b.clone());
                    }
                }
            } else {
                step_a = true;
                next_pair = Some(last_a.clone());
            }
        } else if let Some(last_b) = self.b.last_pair.as_ref() {
            step_b = true;
            next_pair = Some(last_b.clone());
        }
        if step_a {
            if let Err(err) = self.a.step() {
                return Some(Err(err));
            }
        }
        if step_b {
            if let Err(err) = self.b.step() {
                return Some(Err(err));
            }
        }
        next_pair.map(Ok)
    }
}
impl<E, A: TombstoneIterator<E = E>, B: TombstoneIterator<E = E>> TombstoneIterator
    for JoinedIter<E, A, B>
{
    type E = E;

    fn from(self, key: &[u8]) -> Self {
        let runner_a = ScanRunner {
            iter: self.a.iter.from(key),
            still_running: self.a.still_running,
            last_pair: self.a.last_pair,
        };
        let runner_b = ScanRunner {
            iter: self.b.iter.from(key),
            still_running: self.b.still_running,
            last_pair: self.b.last_pair,
        };
        JoinedIter {
            a: runner_a,
            b: runner_b,
            started: self.started,
        }
    }

    fn to(self, key: &[u8]) -> Self {
        let runner_a = ScanRunner {
            iter: self.a.iter.to(key),
            still_running: self.a.still_running,
            last_pair: self.a.last_pair,
        };
        let runner_b = ScanRunner {
            iter: self.b.iter.to(key),
            still_running: self.b.still_running,
            last_pair: self.b.last_pair,
        };
        JoinedIter {
            a: runner_a,
            b: runner_b,
            started: self.started,
        }
    }
}

#[cfg(test)]
mod test {
    use std::cmp::Ordering;

    use super::*;
    use crate::common::cmp_key;

    fn some_key() -> Vec<u8> {
        vec![0, 2, 3]
    }

    fn some_val() -> Vec<u8> {
        vec![1, 3, 5]
    }

    fn some_pair() -> TombstonePair {
        let key = some_key();
        let val = some_val();
        TombstonePair::new(key, val)
    }

    #[test]
    fn test_pair_new() {
        let pair = some_pair();
        assert_eq!(cmp_key(pair.key_ref(), &some_key()), Ordering::Equal);
        assert_eq!(
            cmp_key(pair.value_ref().as_ref().unwrap(), &some_val()),
            Ordering::Equal
        );
    }

    #[test]
    fn test_pair_deletion_marker() {
        let pair = TombstonePair::deletion_marker(some_key());
        assert_eq!(cmp_key(pair.key_ref(), &some_key()), Ordering::Equal);
        assert_eq!(pair.value_ref().as_ref(), None);
    }

    #[test]
    fn test_pair_key() {
        let pair = some_pair();
        assert_eq!(cmp_key(pair.key_ref(), &some_key()), Ordering::Equal);
    }

    #[test]
    fn test_pair_value() {
        let pair = some_pair();
        assert_eq!(
            cmp_key(pair.value_ref().as_ref().unwrap(), &some_val()),
            Ordering::Equal
        );
    }
}
