use std::cmp::Ordering;

use crate::common::{cmp_key, CastError};
use crate::storage::blob_store::FileError;

#[derive(Debug)]
pub(crate) enum SstError {
    Parse(String),
    Write(String),
    Read(String),
    Internal(String),
    EmptySst(String),
}

impl From<CastError> for SstError {
    fn from(err: CastError) -> Self {
        SstError::Parse(format!("could not parse u64 from usize: {:?}", err))
    }
}

impl From<FileError> for SstError {
    fn from(err: FileError) -> Self {
        SstError::Internal(format!("could not write to file: {:?}", err))
    }
}

impl From<SstError> for String {
    fn from(err: SstError) -> Self {
        format!("SstError: {:?}", err)
    }
}

impl From<()> for SstError {
    fn from(_: ()) -> Self {
        SstError::Internal("".to_string())
    }
}

impl SstError {
    pub(crate) fn read(err: FileError) -> SstError {
        SstError::Read(format!("could not read from file: {:?}", err))
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum KeyRangeCmp {
    Less,
    InRange,
    Greater,
}

// A range of keys, from 'start' to 'end' inclusive.
#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub(crate) struct KeyRange {
    /// a start key
    start: Vec<u8>,
    /// an end key
    end: Vec<u8>,
}

impl KeyRange {
    /// Get a new KeyRange instance.
    /// # Arguments
    /// - start: the start key
    /// - end: the end key
    pub(crate) fn new(start: Vec<u8>, end: Vec<u8>) -> KeyRange {
        KeyRange { start, end }
    }

    /// Get the end key for a key range.
    /// # Returns
    /// An end key.
    pub(crate) fn end_ref(&self) -> &[u8] {
        &self.end
    }

    /// Get the start key for a key range.
    /// # Returns
    /// A start key.
    pub(crate) fn start_ref(&self) -> &[u8] {
        &self.start
    }

    /// # Returns
    ///
    /// - KeyRangeCmp::Less if key < self's range
    /// - KeyRangeCmp::InRange if key in self's range
    /// - KeyRangeCmp::Greater if key > self's range
    pub(crate) fn in_range(&self, key: &[u8]) -> KeyRangeCmp {
        if cmp_key(&self.start, key) == Ordering::Greater {
            return KeyRangeCmp::Less;
        }
        if cmp_key(&self.end, key) == Ordering::Less {
            return KeyRangeCmp::Greater;
        }
        KeyRangeCmp::InRange
    }

    /// # Returns
    ///
    /// - KeyRangeCmp::Less if others's range < self's range
    /// - KeyRangeCmp::InRange if other's range in self's range
    /// - KeyRangeCmp::Greater if other's range > self's range
    pub(crate) fn intersects_range<T: KeyRangeLike>(&self, other: &T) -> KeyRangeCmp {
        if cmp_key(&self.start, other.end_ref()) == Ordering::Greater {
            return KeyRangeCmp::Less;
        }
        if cmp_key(&self.end, other.start_ref()) == Ordering::Less {
            return KeyRangeCmp::Greater;
        }
        KeyRangeCmp::InRange
    }
}

pub(crate) trait KeyRangeLike {
    fn start_ref(&self) -> &[u8];
    fn end_ref(&self) -> &[u8];
}

impl KeyRangeLike for KeyRange {
    fn start_ref(&self) -> &[u8] {
        self.start_ref()
    }

    fn end_ref(&self) -> &[u8] {
        self.end_ref()
    }
}

pub(crate) struct RefKeyRange<'a> {
    start: &'a [u8],
    end: &'a [u8],
}

impl<'a> RefKeyRange<'a> {
    pub(crate) fn new(start: &'a [u8], end: &'a [u8]) -> RefKeyRange<'a> {
        RefKeyRange { start, end }
    }
}

impl<'a> KeyRangeLike for RefKeyRange<'a> {
    fn start_ref(&self) -> &[u8] {
        self.start
    }

    fn end_ref(&self) -> &[u8] {
        self.end
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_key_range_new() {
        // Only test this does not crash
        let start = vec![0, 1, 2];
        let end = vec![3, 4, 5];
        let _ = KeyRange::new(start, end);
    }

    #[test]
    fn test_key_range_end() {
        let key_range = KeyRange::new(vec![1, 2, 3], vec![3, 4, 5]);
        let end = key_range.end_ref();
        assert_eq!(end.to_vec(), vec![3, 4, 5]);
    }

    #[test]
    fn test_key_range_start() {
        let start = vec![1, 3, 3, 7];
        let end = vec![4, 2];
        assert_eq!(cmp_key(&start, &end), Ordering::Less);
        let key_range = KeyRange::new(start, end);
        let start = key_range.start_ref();
        assert_eq!(start.to_vec(), vec![1, 3, 3, 7]);
    }
}
