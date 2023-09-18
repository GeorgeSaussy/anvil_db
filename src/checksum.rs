/// See [1] for the spec that this is based on.
/// 1. https://datatracker.ietf.org/doc/html/rfc1952#section-8
///
/// TODO(t/1394): The CRC hash table should be kept
/// in an immutable static variable.
pub(crate) fn crc32(prev: u32, buf: &[u8]) -> u32 {
    // make the CRC table
    let mut table = vec![0_u32; 256];

    for (n, table_cell) in table.iter_mut().enumerate() {
        let mut c = n as u32;
        for _ in 0..8 {
            if c & 1 == 1 {
                c = 0xedb88320 ^ (c >> 1);
            } else {
                c >>= 1;
            }
        }
        *table_cell = c;
    }

    // compute the CRC hash
    let mut c = prev ^ 0xffffffff;
    for byte in buf {
        let idx = ((c ^ (*byte as u32)) & 0xff) as usize;
        c = table[idx] ^ (c >> 8);
    }
    c ^ 0xffffffff
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_buffer() {
        let data = b"";
        let checksum = crc32(0, data);
        assert_eq!(checksum, 0x0);
    }

    #[test]
    fn test_crc32() {
        let test_vectors = [
            ("123456789", 0xCBF43926),
            ("The quick brown fox jumps over the lazy dog", 0x414FA339),
        ];

        for (data, expected_crc) in test_vectors {
            let actual_crc = crc32(0, data.as_bytes());
            assert_eq!(actual_crc, expected_crc);
        }
    }

    #[test]
    fn test_single_byte_buffer() {
        let data = b"a";
        let checksum = crc32(0, data);

        let expect = 3904355907;
        assert_eq!(
            checksum, expect,
            "expected {:#02X} but got {:#02X}",
            expect, checksum
        );
    }

    #[test]
    fn test_hello_world_checksum() {
        let data = b"Hello, World!";
        let checksum = crc32(0, data);
        assert_eq!(checksum, 3964322768);
    }
}
