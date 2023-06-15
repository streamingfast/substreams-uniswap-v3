use std::ops::Add;
use substreams::scalar::BigInt;
use substreams_ethereum::pb::eth::v2::StorageChange;
use tiny_keccak::{Hasher, Keccak};

pub fn get_storage_change<'a>(
    storage_changes: &'a Vec<&StorageChange>,
    slot_key: [u8; 32],
    offset: usize,
    number_of_bytes: usize,
) -> Option<(&'a [u8], &'a [u8])> {
    let storage_change_opt = storage_changes
        .iter()
        // filtering out storage changes which keys do not match ours
        .filter(|&&storage_change| storage_change.key.eq(slot_key.as_slice()))
        // keeping old the storage change slot where our values actually changed
        .filter(|&&storage| {
            let old_data = read_bytes(&storage.old_value, offset, number_of_bytes);
            let new_data = read_bytes(&storage.new_value, offset, number_of_bytes);
            return !old_data.eq(new_data);
        })
        // take the change with the highest ordinal
        .max_by(|x, y| x.ordinal.cmp(&y.ordinal));

    if storage_change_opt.is_none() {
        return None;
    }
    let storage = storage_change_opt.unwrap();

    let old_data = read_bytes(&storage.old_value, offset, number_of_bytes);
    let new_data = read_bytes(&storage.new_value, offset, number_of_bytes);
    Some((old_data, new_data))
}

pub fn calc_map_slot(map_index: &[u8; 32], base_slot: &[u8; 32]) -> [u8; 32] {
    let mut output = [0u8; 32];
    let mut hasher = Keccak::v256();
    hasher.update(map_index);
    hasher.update(base_slot);
    hasher.finalize(&mut output);
    return output;
}

pub fn calc_struct_slot(struct_slot: &[u8; 32], member_slot: BigInt) -> [u8; 32] {
    let mut key = BigInt::from_signed_bytes_be(struct_slot.as_slice());
    key = key.add(member_slot);
    left_pad_from_bigint(&key)
}

pub fn left_pad_from_bigint(input: &BigInt) -> [u8; 32] {
    if input.lt(&BigInt::zero()) {
        return left_pad(&input.to_signed_bytes_be(), 255);
    }

    return left_pad(&input.to_signed_bytes_be(), 0);
}

pub fn left_pad(input: &Vec<u8>, padding_value: u8) -> [u8; 32] {
    if input.len() > 32 {
        panic!("cannot convert vec<u8> to H256");
    }
    let mut data = [padding_value; 32];
    let offset = 32 - input.len();
    for i in 0..input.len() {
        data[offset + i] = input[i];
    }

    return data;
}

pub fn contract_pad(input: &Vec<u8>) -> [u8; 20] {
    if input.len() > 20 {
        panic!("cannot convert vec<u8> to H256");
    }
    let mut data = [0u8; 20];
    let offset = 20 - input.len();
    for i in 0..input.len() {
        data[offset + i] = input[i];
    }

    return data;
}

pub fn read_bytes(buf: &Vec<u8>, offset: usize, number_of_bytes: usize) -> &[u8] {
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

    &buf[start..=end]
}

#[cfg(test)]
mod tests {
    use crate::storage::utils::{get_storage_change, left_pad, read_bytes};
    use std::{fmt::Write, num::ParseIntError};
    use substreams::hex;
    use substreams_ethereum::pb::eth::v2::StorageChange;

    #[test]
    fn left_pad_lt_32_bytes() {
        let input = vec![221u8, 98u8, 237u8, 62u8];
        assert_eq!(
            [
                0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
                0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 221u8, 98u8, 237u8, 62u8
            ],
            left_pad(&input, 0)
        )
    }

    #[test]
    fn left_pad_eq_32_bytes() {
        let input = vec![
            0u8, 0u8, 0u8, 0u8, 10u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 93u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
            0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 221u8, 98u8, 237u8, 62u8,
        ];
        assert_eq!(
            [
                0u8, 0u8, 0u8, 0u8, 10u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 93u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
                0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 221u8, 98u8, 237u8, 62u8
            ],
            left_pad(&input, 0)
        )
    }

    #[test]
    #[should_panic]
    fn left_pad_gt_32_bytes() {
        let input = vec![
            7u8, 0u8, 0u8, 0u8, 0u8, 10u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 93u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
            0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 221u8, 98u8, 237u8, 62u8,
        ];
        let _ = left_pad(&input, 0);
    }

    #[test]
    #[should_panic]
    fn read_bytes_buf_too_small() {
        let buf = decode_hex("ff").unwrap();
        let offset = 0;
        let number_of_bytes = 3;
        let _ = read_bytes(&buf, offset, number_of_bytes);
    }

    #[test]
    fn read_one_byte_with_no_offset() {
        let buf = decode_hex("aabb").unwrap();
        let offset = 0;
        let number_of_bytes = 1;
        assert_eq!(read_bytes(&buf, offset, number_of_bytes), [187u8]);
    }

    #[test]
    fn read_one_byte_with_offset() {
        let buf = decode_hex("aabb").unwrap();
        let offset = 1;
        let number_of_bytes = 1;
        assert_eq!(read_bytes(&buf, offset, number_of_bytes), vec![170u8]);
    }

    #[test]
    #[should_panic]
    fn read_bytes_overflow() {
        let buf = decode_hex("aabb").unwrap();
        let offset = 1;
        let number_of_bytes = 2;
        let _ = read_bytes(&buf, offset, number_of_bytes);
    }

    #[test]
    fn read_bytes_with_no_offset() {
        let buf = decode_hex("ffffffffffffffffffffecb6826b89a60000000000000000000013497d94765a").unwrap();
        let offset = 0;
        let number_of_bytes = 16;
        let out = read_bytes(&buf, offset, number_of_bytes);
        assert_eq!(encode_hex(out), "0000000000000000000013497d94765a".to_string());
    }

    #[test]
    fn read_byte_with_big_offset() {
        let buf = decode_hex("0100000000000000000000000000000000000000000000000000000000000000").unwrap();
        let offset = 31;
        let number_of_bytes = 1;
        let out = read_bytes(&buf, offset, number_of_bytes);
        assert_eq!(encode_hex(out), "01".to_string());
    }

    #[test]
    fn get_storage_change_keep_last_change() {
        let storage_changes = vec![
            StorageChange {
                address: hex!("7858e59e0c01ea06df3af3d20ac7b0003275d4bf").to_vec(),
                key: hex!("59d3454e6bb14d1f2ae9ab5d64a71e9d2d3eec41710c33f701d47eb206f29616").to_vec(),
                old_value: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
                new_value: hex!("006091bfa60000000000000000314c3c8ef0a2c4b9b2ce9d0900000041d2241f").to_vec(),
                ordinal: 0,
            },
            StorageChange {
                address: hex!("7858e59e0c01ea06df3af3d20ac7b0003275d4bf").to_vec(),
                key: hex!("59d3454e6bb14d1f2ae9ab5d64a71e9d2d3eec41710c33f701d47eb206f29616").to_vec(),
                old_value: hex!("006091bfa60000000000000000314c3c8ef0a2c4b9b2ce9d0900000041d2241f").to_vec(),
                new_value: hex!("016091bfa60000000000000000314c3c8ef0a2c4b9b2ce9d0900000041d2241f").to_vec(),
                ordinal: 1,
            },
        ];
        let slot_key = hex!("59d3454e6bb14d1f2ae9ab5d64a71e9d2d3eec41710c33f701d47eb206f29616");
        let offset = 31;
        let number_of_bytes = 1;
        let filered = storage_changes.iter().collect();
        let v_opt = get_storage_change(&filered, slot_key, offset, number_of_bytes);

        let expect_old_value: [u8; 1] = [0];
        let expect_new_value: [u8; 1] = [1];

        assert_eq!(Some((expect_old_value.as_slice(), expect_new_value.as_slice())), v_opt);
    }

    #[test]
    fn get_storage_change_ignore_last_chane() {
        let storage_changes = vec![
            StorageChange {
                address: hex!("7858e59e0c01ea06df3af3d20ac7b0003275d4bf").to_vec(),
                key: hex!("000000000000000000000000000000000000000000000000000000000000000d").to_vec(),
                old_value: hex!("0000000000000000000100000000000000000000000000000000000000000001").to_vec(),
                new_value: hex!("0000000000000000000100000000000000000000000000000000000000000002").to_vec(),
                ordinal: 0,
            },
            StorageChange {
                address: hex!("7858e59e0c01ea06df3af3d20ac7b0003275d4bf").to_vec(),
                key: hex!("000000000000000000000000000000000000000000000000000000000000000d").to_vec(),
                old_value: hex!("0000000000000000000100000000000000000000000000000000000000000002").to_vec(),
                new_value: hex!("0000000000000000000200000000000000000000000000000000000000000002").to_vec(),
                ordinal: 1,
            },
        ];

        let slot_key = hex!("000000000000000000000000000000000000000000000000000000000000000d");
        let offset = 0;
        let number_of_bytes = 22;
        let filered = storage_changes.iter().collect();
        let v_opt = get_storage_change(&filered, slot_key, offset, number_of_bytes);

        let expect_old_value: [u8; 22] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        let expect_new_value: [u8; 22] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2];

        assert_eq!(Some((expect_old_value.as_slice(), expect_new_value.as_slice())), v_opt);
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
