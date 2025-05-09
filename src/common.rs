use std::{cmp::Ordering, future::Future, sync::mpsc::Receiver, task::Poll};

use crate::var_int::VarInt64;

pub(crate) fn join_byte_arrays(arrays: Vec<&[u8]>) -> Vec<u8> {
    let mut result: Vec<u8> = Vec::new();
    for array in arrays {
        for byte in array {
            result.push(*byte);
        }
    }
    result
}

pub(crate) fn cmp_key(a: &[u8], b: &[u8]) -> Ordering {
    let mut a_iter = a.iter();
    let mut b_iter = b.iter();
    loop {
        let a_byte = a_iter.next();
        let b_byte = b_iter.next();
        match (a_byte, b_byte) {
            (Some(a_byte), Some(b_byte)) => match a_byte.cmp(b_byte) {
                Ordering::Equal => {}
                ordering => return ordering,
            },
            (Some(_), None) => return Ordering::Greater,
            (None, Some(_)) => return Ordering::Less,
            (None, None) => return Ordering::Equal,
        }
    }
}

#[derive(Debug)]
pub(crate) struct CastError {
    message: String,
}

impl From<CastError> for String {
    fn from(err: CastError) -> Self {
        err.message
    }
}

impl From<String> for CastError {
    fn from(message: String) -> Self {
        CastError { message }
    }
}

pub(crate) fn try_u64(val: usize) -> Result<u64, CastError> {
    match u64::try_from(val) {
        Ok(val) => Ok(val),
        Err(err) => Err(CastError {
            message: format!("could not parse u64 from usize {val}: {err:?}",),
        }),
    }
}

pub(crate) fn try_usize(val: u64) -> Result<usize, CastError> {
    match usize::try_from(val) {
        Ok(val) => Ok(val),
        Err(err) => Err(CastError {
            message: format!("could not parse usize from u64 {val}: {err:?}",),
        }),
    }
}

pub(crate) fn try_length_prefix(buf: &[u8]) -> Result<Vec<u8>, CastError> {
    let len = VarInt64::try_from(buf.len())?;
    Ok(join_byte_arrays(vec![len.data_ref(), buf]))
}

pub(crate) struct ResultPoller<T> {
    receiver: Receiver<T>,
}

impl<T> ResultPoller<T> {
    // TODO(t/1581): Use me.
    #[allow(dead_code)]
    pub(crate) fn new(receiver: Receiver<T>) -> Self {
        ResultPoller { receiver }
    }
}

impl<T> Future for ResultPoller<T> {
    type Output = T;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if let Ok(t) = self.receiver.try_recv() {
            return Poll::Ready(t);
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_try_usize() {
        assert_eq!(try_usize(0).unwrap(), 0);
        assert_eq!(try_usize(1).unwrap(), 1);
        assert_eq!(try_usize(usize::MAX as u64).unwrap(), usize::MAX);
    }

    #[test]
    fn test_try_u64() {
        assert_eq!(try_u64(0).unwrap(), 0);
        assert_eq!(try_u64(1).unwrap(), 1);
        assert_eq!(try_u64(usize::MAX).unwrap(), u64::MAX);
    }
}
