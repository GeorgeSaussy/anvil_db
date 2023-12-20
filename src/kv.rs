use std::async_iter::AsyncIterator;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::marker::PhantomData;

use crate::common::{cmp_key, join_byte_arrays, try_u64};
use crate::context::Context;
use crate::var_int::VarInt64;

pub(crate) trait KeyValuePointReader {
    type Error;
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;
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
    type Error: Debug;
    fn get<Ctx: Context>(
        &self,
        ctx: &Ctx,
        key: &[u8],
    ) -> Result<Option<TombstoneValue>, Self::Error>;
}

pub(crate) trait RangeSet {
    fn from(self, key: &[u8]) -> Self;
    fn to(self, key: &[u8]) -> Self;
}

pub(crate) trait TombstoneIterator:
    RangeSet + Iterator<Item = Result<TombstonePair, Self::Error>>
{
    type Error;
}

pub(crate) trait AsyncTombstoneIterator:
    RangeSet + AsyncIterator<Item = Result<TombstonePair, Self::Error>>
{
    type Error;
}

pub(crate) trait TombstoneScanner {
    type Error;
    type Iter: TombstoneIterator<Error = Self::Error>;

    fn scan(&self) -> Self::Iter;
}

pub(crate) trait TryTombstoneScanner {
    type Error;
    type Iter<Ctx>: TombstoneIterator<Error = Self::Error>
    where
        Ctx: Context;

    fn try_scan<Ctx: Context>(&self, ctx: &Ctx) -> Result<Self::Iter<Ctx>, Self::Error>;
}

pub(crate) trait TryAsyncTombstoneScanner {
    type Error;
    type Iter: AsyncTombstoneIterator<Error = Self::Error>;

    fn try_async_scan(&self) -> Result<Self::Iter, Self::Error>;
}

pub(crate) trait TombstoneStore: TombstonePointReader + TombstoneScanner {
    type E;

    fn set<T: TombstoneValueLike>(
        &self,
        key: &[u8],
        value: &T,
    ) -> Result<(), <Self as TombstoneStore>::E>;
}

#[derive(Debug)]
struct ScanRunner<I: TombstoneIterator> {
    iter: I,
    still_running: bool,
    last_pair: Option<TombstonePair>,
}

impl<I: TombstoneIterator> ScanRunner<I> {
    fn step(&mut self) -> Result<(), I::Error> {
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

#[derive(Debug)]
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
    type Item = Result<TombstonePair, I::Error>;

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

impl<I: TombstoneIterator> RangeSet for MergedHomogenousIter<I> {
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

impl<I: TombstoneIterator> TombstoneIterator for MergedHomogenousIter<I> {
    type Error = I::Error;
}

/// The first iterator's keys take precedence over the second iterator's keys.
#[derive(Debug)]
pub(crate) struct JoinedIter<E, A: TombstoneIterator, B: TombstoneIterator> {
    a: ScanRunner<A>,
    b: ScanRunner<B>,
    started: bool,
    phantom: PhantomData<E>,
}

impl<E, A: TombstoneIterator, B: TombstoneIterator> JoinedIter<E, A, B> {
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
            phantom: PhantomData,
        }
    }
}

impl<E, A: TombstoneIterator, B: TombstoneIterator> Iterator for JoinedIter<E, A, B>
where
    E: From<<A as TombstoneIterator>::Error> + From<<B as TombstoneIterator>::Error>,
{
    type Item = Result<TombstonePair, E>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.started {
            if let Err(err) = self.a.step() {
                return Some(Err(E::from(err)));
            }
            if let Err(err) = self.b.step() {
                return Some(Err(E::from(err)));
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
                return Some(Err(E::from(err)));
            }
        }
        if step_b {
            if let Err(err) = self.b.step() {
                return Some(Err(E::from(err)));
            }
        }
        next_pair.map(Ok)
    }
}

impl<E, A: TombstoneIterator, B: TombstoneIterator> RangeSet for JoinedIter<E, A, B> {
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
            phantom: PhantomData,
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
            phantom: PhantomData,
        }
    }
}

impl<E, A: TombstoneIterator, B: TombstoneIterator> TombstoneIterator for JoinedIter<E, A, B>
where
    E: From<<A as TombstoneIterator>::Error> + From<<B as TombstoneIterator>::Error>,
{
    type Error = E;
}

#[cfg(test)]
mod test {
    use std::cmp::Ordering;

    use super::*;
    use crate::{
        common::cmp_key,
        concurrent_skip_list::{ConcurrentSkipList, ConcurrentSkipListScanner},
    };

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

    struct SkipListPairIter {
        inner: ConcurrentSkipListScanner<Vec<u8>, Option<Vec<u8>>>,
    }

    impl Iterator for SkipListPairIter {
        type Item = Result<TombstonePair, ()>;

        fn next(&mut self) -> Option<Self::Item> {
            let view = self.inner.next()?;
            let key = view.key_ref().to_vec();
            if let Some(value) = view.value_ref().as_ref() {
                return Some(Ok(TombstonePair::new(key, value.clone())));
            }
            Some(Ok(TombstonePair::deletion_marker(key)))
        }
    }

    impl RangeSet for SkipListPairIter {
        fn from(self, _key: &[u8]) -> Self {
            unimplemented!()
        }

        fn to(self, _key: &[u8]) -> Self {
            unimplemented!()
        }
    }

    impl TombstoneIterator for SkipListPairIter {
        type Error = ();
    }

    #[test]
    fn test_joined_iter() {
        // TODO(t/1388): This number should be bigger.
        let top = 5 * 10_000_usize;

        // create the skip lists
        let mut skip_list_a: ConcurrentSkipList<Vec<u8>, Option<Vec<u8>>> =
            ConcurrentSkipList::new();
        let mut skip_list_b: ConcurrentSkipList<Vec<u8>, Option<Vec<u8>>> =
            ConcurrentSkipList::new();

        // set initial values
        let val = Some(vec![1]);
        for i in 0..(4 * top) {
            let idx = i as u64;
            let key = idx.to_be_bytes();
            skip_list_a.set(key, val.clone());
            skip_list_b.set(key, val.clone());
        }

        // construct joined iterator
        let scanner_a = skip_list_a.iter();
        let scanner_b = skip_list_b.iter();
        let mut joined_iter: JoinedIter<(), SkipListPairIter, SkipListPairIter> = JoinedIter::new(
            SkipListPairIter { inner: scanner_a },
            SkipListPairIter { inner: scanner_b },
        );

        // interleave writing writers and scanning
        let mini = top / 5 - 1;
        let do_nothing = 0;
        let to_remove = 1;
        let to_set = 2;
        for i in 0..mini {
            let idx = (i * 5) as u64;

            let a_action = i % 3;
            let b_action = (i / 3) % 3;
            let programs = [(&mut skip_list_a, a_action), (&mut skip_list_b, b_action)];

            // set i to 2, i+1 to 0, i+2 to 3, leave i+3 as 1, and delete / reset i+4
            for (skip_list_ref, action) in programs {
                skip_list_ref.set(idx.to_be_bytes(), vec![2]);
                skip_list_ref.set((idx + 1).to_be_bytes(), vec![0]);
                skip_list_ref.set((idx + 2).to_be_bytes(), vec![3]);
                if action == to_remove {
                    skip_list_ref.remove(&(idx + 4).to_be_bytes().to_vec());
                    continue;
                }
                if action == to_set {
                    skip_list_ref.set((idx + 4).to_be_bytes(), None);
                    continue;
                }
                assert_eq!(action, do_nothing);
            }

            // scan over the new values
            {
                // The scanner pre-loads one read ahead, so a mutation that
                // affects the next value may not be updated.
                //
                // However, ideally it should be able to read up-to-date
                // values. For now, the documentation should say users should
                // expect reads to the next value may or may not be updated
                // so that it can be changed later.
                let view = joined_iter.next().unwrap().unwrap();
                assert_eq!(view.key_ref().to_vec(), idx.to_be_bytes());
                let value = view.value_ref().as_ref().unwrap().to_vec();
                assert_eq!(value.len(), 1);
                assert!(value[0] == 1 || value[0] == 2);
            }
            let view = joined_iter.next().unwrap().unwrap();
            assert_eq!(view.key_ref().to_vec(), (idx + 1).to_be_bytes());
            assert_eq!(view.value_ref().as_ref().unwrap().to_vec(), vec![0]);
            let view = joined_iter.next().unwrap().unwrap();
            assert_eq!(view.key_ref().to_vec(), (idx + 2).to_be_bytes());
            assert_eq!(view.value_ref().as_ref().unwrap().to_vec(), vec![3]);
            let view = joined_iter.next().unwrap().unwrap();
            assert_eq!(view.key_ref().to_vec(), (idx + 3).to_be_bytes());
            assert_eq!(view.value_ref().as_ref().unwrap().to_vec(), vec![1]);
            if a_action != to_remove || b_action != to_remove {
                let expected = if a_action == do_nothing {
                    Some(vec![1])
                } else if a_action == to_set {
                    None
                } else if b_action == do_nothing {
                    assert_eq!(a_action, to_remove);
                    Some(vec![1])
                } else {
                    assert_eq!(a_action, to_remove);
                    assert_eq!(b_action, to_set);
                    None
                };

                let pair = joined_iter.next().unwrap().unwrap();
                let found = pair.value_ref().as_ref().cloned();
                assert_eq!(pair.key_ref().to_vec(), (idx + 4).to_be_bytes());
                assert_eq!(found, expected);
            }
        }
    }
}
