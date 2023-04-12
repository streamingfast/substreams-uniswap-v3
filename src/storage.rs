use crate::pb::uniswap::PoolLiquidity;
use primitive_types::H256;
use std::ops::Add;
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

pub fn extract_pool_liquidity(
    log_ordinal: u64,
    pool_address: &Vec<u8>,
    storage_changes: &Vec<StorageChange>,
) -> Option<PoolLiquidity> {
    for storage_change in storage_changes {
        if pool_address.eq(&storage_change.address) {
            if storage_change.key[storage_change.key.len() - 1] == 4 {
                return Some(PoolLiquidity {
                    pool_address: Hex(&pool_address).to_string(),
                    liquidity: Some(BigInt::from_signed_bytes_be(&storage_change.new_value).into()),
                    log_ordinal,
                });
            }
        }
    }
    None
}
