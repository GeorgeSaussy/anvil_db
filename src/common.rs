use std::{cmp::Ordering, future::Future, sync::mpsc::Receiver, task::Poll};

use crate::checksum::crc32;
use crate::storage::blob_store::ReadCursor;
use crate::var_int::VarInt64;
use crate::wal::wal_entry::{Checksum, WalError, WalRecoveryError};

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

#[allow(dead_code)]
pub(crate) fn hex(bytes: &[u8]) -> String {
    let mut hex_string = "0x".to_string();
    for byte in bytes {
        hex_string.push_str(&format!("{byte:02x}"));
    }
    hex_string
}

pub(crate) fn try_length_prefix(buf: &[u8]) -> Result<Vec<u8>, CastError> {
    let len = VarInt64::try_from(buf.len())?;
    Ok(join_byte_arrays(vec![len.data_ref(), buf]))
}

fn extend_with_len_prefix(buf: &mut Vec<u8>, data: &[u8]) -> Result<(), CastError> {
    let len = VarInt64::try_from(data.len())?;
    buf.extend(len.data_ref());
    buf.extend(data);
    Ok(())
}

/// Use this for extending from a list of byte arrays.
fn extend_count_with_len_prefix(buf: &mut Vec<u8>, byte_arrays: &[&[u8]]) -> Result<(), CastError> {
    let len = VarInt64::try_from(byte_arrays.len())?;
    buf.extend(len.data_ref());
    for data in byte_arrays {
        extend_with_len_prefix(buf, data)?;
    }
    Ok(())
}

fn string_slice_to_byte_arrays(string_slice: &[String]) -> Vec<&[u8]> {
    let mut byte_arrays = Vec::with_capacity(string_slice.len());
    for s in string_slice {
        let b = s.as_bytes();
        byte_arrays.push(b);
    }
    byte_arrays
}

pub(crate) fn extend_from_string_slice(
    buf: &mut Vec<u8>,
    string_slice: &[String],
) -> Result<(), CastError> {
    let byte_arrays = string_slice_to_byte_arrays(string_slice);
    extend_count_with_len_prefix(buf, byte_arrays.as_slice())
}

pub(crate) fn extend_from_string_matrix(
    buf: &mut Vec<u8>,
    string_matrix: &[Vec<String>],
) -> Result<(), CastError> {
    let row_count = VarInt64::try_from(string_matrix.len())?;
    buf.extend(row_count.data_ref());
    for row in string_matrix {
        extend_from_string_slice(buf, row)?;
    }
    Ok(())
}

pub(crate) fn recover_byte<RC: ReadCursor>(
    reader: &mut RC,
    prev_checksum: Checksum,
) -> Result<(u8, Checksum), WalError> {
    let mut buf = [0_u8];
    if let Err(err) = reader.read_exact(&mut buf) {
        return Err(WalRecoveryError::ReadExactFailed {
            blob_id: "unknown".to_string(),
            err,
        }
        .into());
    }
    let checksum = crc32(prev_checksum, &buf);
    Ok((buf[0], checksum))
}

pub(crate) fn recover_var_int<RC: ReadCursor>(
    reader: &mut RC,
    prev_checksum: Checksum,
) -> Result<(VarInt64, Checksum), WalError> {
    let i = match VarInt64::try_from_rc(reader) {
        Ok(i) => i,
        Err(_) => return Err(WalError::InvalidInput(None)),
    };
    let checksum = crc32(prev_checksum, i.data_ref());
    Ok((i, checksum))
}

pub(crate) fn recover_length_prefixed_buffer<RC: ReadCursor>(
    reader: &mut RC,
    prev_checksum: Checksum,
) -> Result<(Vec<u8>, Checksum), WalError> {
    let (buf_len, checksum) = recover_var_int(reader, prev_checksum)?;
    let mut buf = vec![0_u8; buf_len.value() as usize];
    if let Err(err) = reader.read_exact(&mut buf) {
        return Err(WalRecoveryError::ReadExactFailed {
            blob_id: "unknown".to_string(),
            err,
        }
        .into());
    }
    let checksum = crc32(checksum, &buf);
    Ok((buf, checksum))
}

// TODO(nit): Refactor other encoding functions to use this.
pub(crate) fn recover_string_vec<RC: ReadCursor>(
    reader: &mut RC,
    prev_checksum: Checksum,
) -> Result<(Vec<String>, Checksum), WalError> {
    let (count, mut checksum) = recover_var_int(reader, prev_checksum)?;
    let mut buf_vec = vec!["".to_string(); count.value() as usize];
    for b in buf_vec.iter_mut() {
        let (buf, new_checksum) = recover_length_prefixed_buffer(reader, checksum)?;
        *b = String::from_utf8(buf)?;
        checksum = new_checksum;
    }
    Ok((buf_vec, checksum))
}

pub(crate) fn recover_string_matrix<RC: ReadCursor>(
    reader: &mut RC,
    prev_checksum: Checksum,
) -> Result<(Vec<Vec<String>>, Checksum), WalError> {
    let (row_count, mut checksum) = recover_var_int(reader, prev_checksum)?;
    let row_count = row_count.value() as usize;
    let mut rows = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        let (buf_vec, new_checksum) = recover_string_vec(reader, checksum)?;
        checksum = new_checksum;
        rows.push(buf_vec);
    }
    Ok((rows, checksum))
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
            message: format!("could not parse u64 from usize {val}: {err:?}"),
        }),
    }
}

pub(crate) fn try_usize(val: u64) -> Result<usize, CastError> {
    match usize::try_from(val) {
        Ok(val) => Ok(val),
        Err(err) => Err(CastError {
            message: format!("could not parse usize from u64 {val}: {err:?}"),
        }),
    }
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
