use primitive_types::H256;
use std::cmp::Ordering;
use std::ops::Add;
use std::str::FromStr;
use substreams::prelude::BigInt;
use substreams::{hex, log, Hex};
use substreams_ethereum::pb::eth::v2::StorageChange;
use tiny_keccak::{Hasher, Keccak};

pub fn read_bytes(buf: Vec<u8>, offset: usize, number_of_bytes: usize) -> Vec<u8> {
    let buf_length = buf.len();
    if buf_length < number_of_bytes {
        panic!(
            "attempting to read {number_of_bytes} bytes in buffer  size {buf_size}",
            number_of_bytes = number_of_bytes,
            buf_size = buf.len()
        )
    }

    if offset > (buf_length - 1) {
        panic!(
            "offset {offset} exceeds buffer size {buf_size}",
            offset = offset,
            buf_size = buf.len()
        )
    }

    let end = buf_length - 1 - offset;
    let start_opt = (end + 1).checked_sub(number_of_bytes);
    if start_opt.is_none() {
        panic!(
            "number of bytes {number_of_bytes} with offset {offset} exceeds buffer size {buf_size}",
            number_of_bytes = number_of_bytes,
            offset = offset,
            buf_size = buf.len()
        )
    }
    let start = start_opt.unwrap();

    let out = &buf[start..=end];
    return out.to_vec();
}

#[cfg(test)]
mod tests {
    use crate::slotlayout::read_bytes;
    use std::fmt::Write;
    use std::num::ParseIntError;
    use substreams::scalar::BigInt;

    #[test]
    #[should_panic]
    fn read_bytes_buf_too_small() {
        let buf = decode_hex("ff").unwrap();
        let offset = 0;
        let number_of_bytes = 3;
        let _ = read_bytes(buf, offset, number_of_bytes);
    }

    #[test]
    fn read_one_byte_with_no_offset() {
        let buf = decode_hex("aabb").unwrap();
        let offset = 0;
        let number_of_bytes = 1;
        assert_eq!(read_bytes(buf, offset, number_of_bytes), vec![187u8]);
    }

    #[test]
    fn read_one_byte_with_offset() {
        let buf = decode_hex("aabb").unwrap();
        let offset = 1;
        let number_of_bytes = 1;
        assert_eq!(read_bytes(buf, offset, number_of_bytes), vec![170u8]);
    }

    #[test]
    #[should_panic]
    fn read_bytes_overflow() {
        let buf = decode_hex("aabb").unwrap();
        let offset = 1;
        let number_of_bytes = 2;
        let _ = read_bytes(buf, offset, number_of_bytes);
    }

    #[test]
    fn read_bytes_with_no_offset() {
        let buf =
            decode_hex("ffffffffffffffffffffecb6826b89a60000000000000000000013497d94765a").unwrap();
        let offset = 0;
        let number_of_bytes = 16;
        let out = read_bytes(buf, offset, number_of_bytes);
        assert_eq!(
            encode_hex(out.as_slice()),
            "0000000000000000000013497d94765a".to_string()
        );
    }

    #[test]
    fn read_byte_with_big_offset() {
        let buf =
            decode_hex("0100000000000000000000000000000000000000000000000000000000000000").unwrap();
        let offset = 31;
        let number_of_bytes = 1;
        let out = read_bytes(buf, offset, number_of_bytes);
        assert_eq!(encode_hex(out.as_slice()), "01".to_string());
    }

    fn decode_hex(s: &str) -> Result<Vec<u8>, ParseIntError> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
            .collect()
    }

    fn encode_hex(bytes: &[u8]) -> String {
        let mut s = String::with_capacity(bytes.len() * 2);
        for &b in bytes {
            write!(&mut s, "{:02x}", b).unwrap();
        }
        s
    }
}
