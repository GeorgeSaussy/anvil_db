use crate::{common::CastError, storage::blob_store::ReadCursor};

/// Here lies a var_int_64 implementation, based on
/// [Google's varint128 documentation](https://developers.google.com/protocol-buffers/docs/encoding#varints)

/// A structure to encapsulate a var_int_64.
/// # Instance Variables
/// - bytes: the bytes in the var_int_64
#[derive(Debug, Clone)]
pub(crate) struct VarInt64 {
    bytes: Vec<u8>,
}

/// Implementation of var_int_64 methods.
impl VarInt64 {
    /// The maximum value that can be represented by a VarInt64.
    const MAX: u64 = 2_u64.pow(56);

    /// Get the value of a VarInt64 instance as a u64.
    /// # Returns
    /// The value of the VarInt64 instance as a u64.
    pub(crate) fn value(&self) -> u64 {
        let mut ret: u64 = 0;
        let mut bytes = self.bytes.clone();
        bytes.reverse();
        for byte in bytes {
            ret = (1 << 7) * ret + ((byte % (1 << 7)) as u64);
        }
        ret
    }

    /// Get the var_int_64 as an array of bytes.
    /// # Returns
    /// A reference to the inner value.
    pub(crate) fn data_ref(&self) -> &[u8] {
        &self.bytes
    }

    /// Get the length of a var_int_64.
    /// # Returns
    /// The length of the byte array representation of the var_int_64.
    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    fn try_from_buffer<T: AsRef<[u8]>>(buf: T) -> Result<VarInt64, String> {
        let bytes = buf.as_ref();
        if bytes.is_empty() {
            return Err("Cannot parse buffer of zero length".to_string());
        }
        let mut index: i64 = -1; // overkill, but consistent
        for (i, byte) in bytes.iter().enumerate() {
            if byte & (1 << 7) == 0 && i < 8 {
                index = i as i64;
                break;
            } else if i >= 8 {
                break;
            }
        }
        if index == -1 {
            return Err("Cannot parse cord.".to_string());
        }
        let mut new_bytes: Vec<u8> = Vec::new();
        for byte in bytes.iter().take((index + 1) as usize) {
            new_bytes.push(*byte);
        }
        Ok(VarInt64 { bytes: new_bytes })
    }

    pub(crate) fn try_from_rc<RC: ReadCursor>(reader: &mut RC) -> Result<VarInt64, String> {
        let mut my_bytes = Vec::new();
        let mut i = 0;
        loop {
            let mut buf = [0_u8];
            if let Err(err) = reader.read_exact(&mut buf) {
                return Err(format!("{:?}", err));
            }
            let byte = buf[0];
            my_bytes.push(byte);
            if byte & (1 << 7) == 0 && i < 8 {
                break;
            } else if i >= 8 {
                return Err("could not parse RC".to_string());
            }
            i += 1;
        }
        Ok(VarInt64 { bytes: my_bytes })
    }
}

impl TryFrom<u64> for VarInt64 {
    type Error = CastError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if value > VarInt64::MAX {
            return Err(CastError::from(format!(
                "value {} is too big to convert to VarInt64 (max is {})",
                value,
                VarInt64::MAX
            )));
        }
        let mut ongoing = value;
        let mut bytes: Vec<u8> = Vec::new();
        while ongoing > 0 {
            let byte: u8 = (ongoing % (1 << 7)) as u8;
            ongoing /= 1 << 7;
            bytes.push(byte + (1 << 7));
        }
        if bytes.is_empty() {
            bytes.push(0);
        }
        let last = bytes.len() - 1;
        bytes[last] &= !(1 << 7);
        Ok(VarInt64 { bytes })
    }
}

impl TryFrom<usize> for VarInt64 {
    type Error = CastError;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        let value = match u64::try_from(value) {
            Ok(x) => x,
            Err(err) => return Err(format!("could not convert usize {} to u64: {}", value, err))?,
        };
        VarInt64::try_from(value)
    }
}

impl TryFrom<&[u8]> for VarInt64 {
    type Error = String;

    fn try_from(buf: &[u8]) -> Result<VarInt64, String> {
        VarInt64::try_from_buffer(buf)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_var_int_64_from_u64() {
        let vi = VarInt64::try_from(100_u64).expect("unexpected error making var_int_64");
        assert_eq!(vi.value(), 100);

        let vi = VarInt64::try_from(1_u64).expect("unexpected error making var_int_64");
        assert_eq!(vi.value(), 1);

        assert!(VarInt64::try_from(2_u64.pow(60)).is_err());
    }

    #[test]
    fn test_var_int_64_data() {
        let leet = 1337_u64;
        let start_vi = VarInt64::try_from(leet).expect("unexpected error making var_int_64");
        let data = start_vi.data_ref();
        let end_vi = VarInt64::try_from(data).expect("unexpected error making var_int_64");
        assert_eq!(end_vi.value(), leet);
    }

    #[test]
    fn test_var_int_64_length() {
        let vi = VarInt64::try_from(1337_u64).expect("unexpected error making var_int_64");
        let data = vi.data_ref();
        assert!(data.len() == vi.len());
    }

    #[test]
    fn test_var_int_64_from_cord() {
        let vi1 = VarInt64::try_from(1337_u64).expect("unexpected error making var_int_64");
        assert_eq!(vi1.value(), 1337);
        let vi2 = VarInt64::try_from(vi1.data_ref()).expect("unexpected error making var_int_64");
        assert_eq!(vi2.value(), 1337);
    }

    #[test]
    fn test_var_int_64_cord() {
        let vi = match VarInt64::try_from(300_u64) {
            Ok(vi) => vi,
            Err(_) => panic!("unexpected error making var_int_64"),
        };
        assert_eq!(vi.value(), 300);
        let buf = vi.data_ref();
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0], 172);
        assert_eq!(buf[1], 2);
    }

    #[test]
    fn test_var_int_end_to_end_check() {
        // check u64 conversion
        let vals = vec![0, 1, 50, 64, 100, 1_000_000];
        for value in vals {
            let val: u64 = value;
            let vi = match VarInt64::try_from(val) {
                Ok(vi) => vi,
                Err(_) => panic!("unexpected error making var_int_64"),
            };
            assert_eq!(vi.value(), val);
        }
    }
}
