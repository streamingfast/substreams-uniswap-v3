use std::ops::Add;
use substreams::scalar::BigInt;
use substreams_ethereum::pb::eth::v2::StorageChange;
use tiny_keccak::{Hasher, Keccak};

pub struct UniswapPoolStorage<'a> {
    storage_changes: &'a Vec<StorageChange>,
    contract_addr: [u8; 20],
}

fn calc_map_slot(map_index: &[u8; 32], base_slot: &[u8; 32]) -> [u8; 32] {
    let mut output = [0u8; 32];
    let mut hasher = Keccak::v256();
    hasher.update(map_index);
    hasher.update(base_slot);
    hasher.finalize(&mut output);
    return output;
}

fn calc_struct_slot(struct_slot: &[u8; 32], member_slot: BigInt) -> [u8; 32] {
    let mut key = BigInt::from_signed_bytes_be(struct_slot.as_slice());
    key = key.add(member_slot);
    left_pad_from_bigint(&key)
}

impl<'a> UniswapPoolStorage<'a> {
    pub fn new(storage_changes: &'a Vec<StorageChange>, contract_addr: &Vec<u8>) -> UniswapPoolStorage<'a> {
        return Self {
            storage_changes,
            contract_addr: contract_pad(contract_addr),
        };
    }

    pub fn get_fee_growth_global0x128(&self) -> Option<(BigInt, BigInt)> {
        let feeGrowthGlobal0X128_slot = BigInt::from(1);
        let offset = 0;
        let number_of_bytes = 32;

        // ----
        let slot_key = left_pad_from_bigint(&feeGrowthGlobal0X128_slot);
        // ----

        if let Some((old_data, new_data)) = self.get_storage_changes(slot_key, offset, number_of_bytes) {
            Some((
                BigInt::from_unsigned_bytes_be(old_data),
                BigInt::from_unsigned_bytes_be(new_data),
            ))
        } else {
            None
        }
    }

    pub fn get_fee_growth_global1x128(&self) -> Option<(BigInt, BigInt)> {
        let feeGrowthGlobal1X128_slot = BigInt::from(2);
        let offset = 0;
        let number_of_bytes = 32;

        // ----
        let slot_key = left_pad_from_bigint(&feeGrowthGlobal1X128_slot);
        // ----

        if let Some((old_data, new_data)) = self.get_storage_changes(slot_key, offset, number_of_bytes) {
            Some((
                BigInt::from_unsigned_bytes_be(old_data),
                BigInt::from_unsigned_bytes_be(new_data),
            ))
        } else {
            None
        }
    }

    pub fn get_slot0_sqrt_price_x96(&self) -> Option<(BigInt, BigInt)> {
        let slot0_slot = BigInt::from(0);
        let slot0_struct_sqrt_price_x96_slot = BigInt::zero();
        let offset = 0;
        let number_of_bytes = 20;

        // ----
        let slot_key = calc_struct_slot(&left_pad_from_bigint(&slot0_slot), slot0_struct_sqrt_price_x96_slot);
        // ----

        if let Some((old_data, new_data)) = self.get_storage_changes(slot_key, offset, number_of_bytes) {
            Some((
                BigInt::from_unsigned_bytes_be(old_data),
                BigInt::from_unsigned_bytes_be(new_data),
            ))
        } else {
            None
        }
    }

    pub fn get_ticks_initialized(&self, tick_idx: &BigInt) -> Option<(bool, bool)> {
        let ticks_slot = BigInt::from(5);
        let tick_info_struct_initialized_slot = BigInt::from(3);
        let offset = 31;
        let number_of_bytes = 1;

        // ----
        let ticker_struct_slot = calc_map_slot(&left_pad_from_bigint(&tick_idx), &left_pad_from_bigint(&ticks_slot));
        let slot_key = calc_struct_slot(&ticker_struct_slot, tick_info_struct_initialized_slot);
        // ----

        if let Some((old_data, new_data)) = self.get_storage_changes(slot_key, offset, number_of_bytes) {
            Some((old_data == [01u8], new_data == [01u8]))
        } else {
            None
        }
    }

    pub fn get_ticks_fee_growth_outside_0_x128(&self, tick_idx: &BigInt) -> Option<(BigInt, BigInt)> {
        let ticks_slot = BigInt::from(5);
        let tick_info_struct_initialized_slot = BigInt::from(1);
        let offset = 0;
        let number_of_bytes = 32;

        // ----
        let ticker_struct_slot = calc_map_slot(&left_pad_from_bigint(&tick_idx), &left_pad_from_bigint(&ticks_slot));
        let slot_key = calc_struct_slot(&ticker_struct_slot, tick_info_struct_initialized_slot);
        // ----

        if let Some((old_data, new_data)) = self.get_storage_changes(slot_key, offset, number_of_bytes) {
            Some((
                BigInt::from_unsigned_bytes_be(old_data),
                BigInt::from_unsigned_bytes_be(new_data),
            ))
        } else {
            None
        }
    }

    pub fn get_ticks_fee_growth_outside_1_x128(&self, tick_idx: &BigInt) -> Option<(BigInt, BigInt)> {
        let ticks_slot = BigInt::from(5);
        let tick_info_struct_initialized_slot = BigInt::from(2);
        let offset = 0;
        let number_of_bytes = 32;

        // ----
        let ticker_struct_slot = calc_map_slot(&left_pad_from_bigint(&tick_idx), &left_pad_from_bigint(&ticks_slot));
        let slot_key = calc_struct_slot(&ticker_struct_slot, tick_info_struct_initialized_slot);
        // ----

        if let Some((old_data, new_data)) = self.get_storage_changes(slot_key, offset, number_of_bytes) {
            Some((
                BigInt::from_unsigned_bytes_be(old_data),
                BigInt::from_unsigned_bytes_be(new_data),
            ))
        } else {
            None
        }
    }

    fn get_storage_changes(&self, slot_key: [u8; 32], offset: usize, number_of_bytes: usize) -> Option<(&[u8], &[u8])> {
        let storage_change_opt = self.storage_changes.iter().find(|storage_change| {
            storage_change.address == self.contract_addr && storage_change.key.eq(slot_key.as_slice())
        });

        if storage_change_opt.is_none() {
            return None;
        }
        let storage = storage_change_opt.unwrap();

        let old_data = read_bytes(&storage.old_value, offset, number_of_bytes);
        let new_data = read_bytes(&storage.new_value, offset, number_of_bytes);
        Some((old_data, new_data))
    }
}

pub fn left_pad_from_bigint(input: &BigInt) -> [u8; 32] {
    return left_pad(&input.to_signed_bytes_be());
}

pub fn left_pad(input: &Vec<u8>) -> [u8; 32] {
    if input.len() > 32 {
        panic!("cannot convert vec<u8> to H256");
    }
    let mut data = [0u8; 32];
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
    use crate::storage::{left_pad, left_pad_from_bigint, read_bytes, UniswapPoolStorage};
    use std::ops::Add;
    use std::str::FromStr;
    use std::{fmt::Write, num::ParseIntError};
    use substreams::scalar::BigInt;
    use substreams::{hex, Hex};
    use substreams_ethereum::pb::eth::v2::StorageChange;
    use tiny_keccak::{Hasher, Keccak};

    #[test]
    fn left_pad_lt_32_bytes() {
        let input = vec![221u8, 98u8, 237u8, 62u8];
        assert_eq!(
            [
                0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
                0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 221u8, 98u8, 237u8, 62u8
            ],
            left_pad(&input)
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
            left_pad(&input)
        )
    }

    #[test]
    #[should_panic]
    fn left_pad_gt_32_bytes() {
        let input = vec![
            7u8, 0u8, 0u8, 0u8, 0u8, 10u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 93u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
            0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 221u8, 98u8, 237u8, 62u8,
        ];
        let _ = left_pad(&input);
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
    fn get_slot0_sqrtPriceX96() {
        let storage_changes = vec![StorageChange {
            address: hex!("7858e59e0c01ea06df3af3d20ac7b0003275d4bf").to_vec(),
            key: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
            old_value: hex!("0001000001000100000000000000000000000001000000000000000000000000").to_vec(),
            new_value: hex!("000100000100010000000000000000000000000100000402dad5eda8db022960").to_vec(),
            ordinal: 0,
        }];

        let storage = UniswapPoolStorage::new(
            &storage_changes,
            &hex!("7858e59e0c01ea06df3af3d20ac7b0003275d4bf").to_vec(),
        );
        let v_opt = storage.get_slot0_sqrt_price_x96();
        assert_eq!(
            Some((
                BigInt::from_str("79228162514264337593543950336").unwrap(),
                BigInt::from_str("79228181456392528199336208736").unwrap()
            )),
            v_opt
        );
    }

    #[test]
    fn get_slot0_initialized() {
        let storage_changes = vec![StorageChange {
            address: hex!("7858e59e0c01ea06df3af3d20ac7b0003275d4bf").to_vec(),
            key: hex!("a18b128af1c8fc61ff46f02d146e54546f34d340574cf2cef6a753cba6b67020").to_vec(),
            old_value: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
            new_value: hex!("0100000000000000000000000000000000000000000000000000000000000000").to_vec(),
            ordinal: 0,
        }];

        let storage = UniswapPoolStorage::new(
            &storage_changes,
            &hex!("7858e59e0c01ea06df3af3d20ac7b0003275d4bf").to_vec(),
        );
        let tick_idx = BigInt::from(10);
        let v_opt = storage.get_ticks_initialized(&tick_idx);
        assert_eq!(Some((false, true)), v_opt);
    }

    fn test_tick_storage_initialized() {
        //  inputs from AST
        let ticks_mapping_slot = BigInt::from(5);
        let ticks_struct_initialized_slot = BigInt::from(3);
        let tick_idx = BigInt::from(10);

        // we create a hasher
        let mut hasher = Keccak::v256();
        // append the map key
        hasher.update(left_pad_from_bigint(&tick_idx).as_slice());

        // append the slot
        hasher.update(left_pad_from_bigint(&ticks_mapping_slot).as_slice());

        let mut output = [0u8; 32];
        hasher.finalize(&mut output);

        let mut next_key = BigInt::from_signed_bytes_be(&output);
        next_key = next_key.add(ticks_struct_initialized_slot);
        assert_eq!(
            encode_hex(next_key.to_signed_bytes_be().as_slice()),
            "a18b128af1c8fc61ff46f02d146e54546f34d340574cf2cef6a753cba6b67020"
        );
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
