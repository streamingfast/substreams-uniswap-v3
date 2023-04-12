use crate::pb::uniswap::PoolLiquidity;
use primitive_types::H256;
use std::ops::Add;
use std::str::FromStr;
use substreams::prelude::BigInt;
use substreams::{hex, log, Hex};
use substreams_ethereum::pb::eth::v2::StorageChange;
use tiny_keccak::{Hasher, Keccak};

pub fn tick_info_mapping_initialized_changed(
    storage_changes: &Vec<StorageChange>,
    tick_index: &BigInt,
) -> bool {
    let mut hasher = Keccak::v256();
    let mut output = [0u8];

    let mut tick_index_slot = H256::from_slice(&tick_index.to_signed_bytes_le());
    hasher.update(&tick_index_slot.as_bytes()); // slot for `mapping(uint24 => Tick.Info) ticks`
    hasher.update(&H256::from_low_u64_be(3).as_bytes());
    hasher.finalize(&mut output);

    // Take the THIRD slot after `output`
    let mut next_key = BigInt::from_signed_bytes_le(&output);
    next_key = next_key.add(BigInt::from(2));
    let mut offset3_key = H256::from_slice(&next_key.to_signed_bytes_le());

    let mut hasher2 = Keccak::v256();
    hasher2.update(&output);
    hasher2.update(&offset3_key.as_bytes());
    hasher2.finalize(&mut output);

    let storage_change = storage_changes
        .iter()
        .find(|storage_change| storage_change.key.eq(&output));

    log::debug!("{}, {:?}", tick_index, storage_change);

    if storage_change.is_none() {
        return false;
    }

    // TODO: Now analyze the `value` in the returned storage change
    // if the offset 31 (the last byte really, the boolean representing the `initialized` field)
    // has CHANGED between `old_value` and `new_value`, then return `true`.

    return storage_change.is_some();
}


pub fn to_h256_from_bigint(input: &BigInt) -> H256 {
    return to_h256(&input.to_signed_bytes_be());
}

pub fn to_h256(input: &Vec<u8>) -> H256 {
    if input.len() == 32 {
        return H256::from_slice(input.as_slice())
    }
    if input.len() >32 {
        panic!("cannot convert vec<u8> to H256");
    }
    let mut data = input.clone();
    let diff = 32 - data.len();
    data.resize(32, 0);
    data.rotate_right(diff);
    return H256::from_slice(data.as_slice())
}


#[cfg(test)]
mod tests {
    use std::num::ParseIntError;
    use std::fmt::Write;
    use std::ops::Add;
    use std::str::FromStr;
    use primitive_types::H256;
    use substreams::scalar::BigInt;
    use tiny_keccak::{Hasher, Keccak};
    use crate::storage::{to_h256, to_h256_from_bigint};


    #[test]
    fn to_h256_lt_32_bytes() {
        let input = vec![221u8, 98u8, 237u8, 62u8];
        assert_eq!(
            H256([0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,221u8, 98u8, 237u8, 62u8]),
            to_h256(&input)
        )
    }

    #[test]
    fn to_h256_eq_32_bytes() {
        let input = vec![0u8,0u8,0u8,0u8,10u8,0u8,0u8,0u8,0u8,0u8,0u8,93u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,221u8, 98u8, 237u8, 62u8];
        assert_eq!(
            H256([0u8,0u8,0u8,0u8,10u8,0u8,0u8,0u8,0u8,0u8,0u8,93u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,221u8, 98u8, 237u8, 62u8]),
            to_h256(&input)
        )
    }

    #[test]
    #[should_panic]
    fn to_h256_gt_32_bytes() {
        let input = vec![7u8,0u8,0u8,0u8,0u8,10u8,0u8,0u8,0u8,0u8,0u8,0u8,93u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8,221u8, 98u8, 237u8, 62u8];
        let _ = to_h256(&input);
    }

    #[test]
    fn test_tick_storage_initialized() {
       //  inputs from AST
        let ticks_mapping_slot = BigInt::from(5);
        let ticks_struct_initialized_slot = BigInt::from(3);
        let tick_idx = BigInt::from(10);

        // we create a hasher
        let mut hasher = Keccak::v256();
        // append the map key
        let da = to_h256_from_bigint(&tick_idx);
        hasher.update(da.as_bytes());

        // append the slot
        let db = to_h256_from_bigint(&ticks_mapping_slot);
        hasher.update(db.as_bytes());

        let mut output = [0u8; 32];
        hasher.finalize(&mut output);

        let mut next_key = BigInt::from_signed_bytes_be(&output);
        next_key = next_key.add(ticks_struct_initialized_slot);
        assert_eq!(encode_hex(next_key.to_signed_bytes_be().as_slice()), "a18b128af1c8fc61ff46f02d146e54546f34d340574cf2cef6a753cba6b67020");
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