use std::fmt::format;
use std::ops::Add;
use std::str::FromStr;
use std::string::ToString;
use substreams::hex;
use substreams::scalar::BigInt;
use substreams_ethereum::pb::eth::v2::StorageChange;
use tiny_keccak::{Hasher, Keccak};
use crate::slotlayout::read_bytes;
use crate::storage::to_h256_from_bigint;

struct StorageLayout {
    storages: Vec<Storage>,
    types: Vec<StorageType>
}

struct Storage {
    ast: u64,
    contract: String,
    label: String,
    offset: u64,
    slot: String,
    storage_type: String,
}

impl Storage {
    fn slot_num(self) -> BigInt {
        return BigInt::from_str(self.slot.as_str()).unwrap();
    }
}

enum SlotType {
    //data is laid out contiguously in storage
    inplace,
    //Keccak-256 hash-based method
    mapping,
    //Keccak-256 hash-based method
    dynamic_array,
    //single slot or Keccak-256 hash-based depending on the data size
    bytes
}


struct StorageType {
    name: String,
    encoding: SlotType,
    label: String,
    //is the number of used bytes (as a decimal string). Note that if number_of_bytes > 32 this means that more than one slot is used.
    number_of_bytes: String,
    value: String,
    key: String,
    members: Vec<Storage>,
}

struct UniswapPoolStorage<'a> {
    storage_changes: &'a Vec<StorageChange>
}

impl<'a> UniswapPoolStorage<'a> {
    const CONTRACT_ADDR: [u8; 20] = hex!("7858e59e0c01ea06df3af3d20ac7b0003275d4bf");

    pub fn new(storage_changes: &'a Vec<StorageChange>)-> UniswapPoolStorage<'a> {
        return  Self {
            storage_changes,
        }
    }

    pub fn get_slot0_sqrt_price_x96(&self) -> Option<BigInt> {
        let slot = BigInt::from(0);
        let slot_bytes = to_h256_from_bigint(&slot);
        let mut output = [0u8; 32];

        // if mapping or array dynamic do more {
        if false {

        }else {
            output = slot_bytes.to_fixed_bytes();
        }
        //
        //
        // }

        let storage_change_opt = self.storage_changes
            .iter()
            .find(|storage_change| storage_change.address == Self::CONTRACT_ADDR && storage_change.key.eq(output.as_slice()));

        if storage_change_opt.is_none() {
            return None;
        }
        let storage = storage_change_opt.unwrap();

        let offset = 0;
        let number_of_bytes = 20;

        let data = read_bytes(storage.new_value.to_vec(), offset,number_of_bytes);
        Some(BigInt::from_unsigned_bytes_be(data.as_slice()))
    }

    pub fn get_ticks_initialized(&self, tick_idx: &BigInt) -> Option<(bool, bool)> {
        let slot = BigInt::from(5);
        let initialized_slot_in_struct = BigInt::from(3);
        let slot_bytes = to_h256_from_bigint(&slot);
        let mut output = [0u8; 32];



        // if mapping or array dynamic do more {
        if true {
            let mut hasher = Keccak::v256();
            hasher.update(to_h256_from_bigint(&tick_idx).as_bytes());
            hasher.update(to_h256_from_bigint(&slot).as_bytes());
            hasher.finalize(&mut output);

            let mut next_key = BigInt::from_signed_bytes_be(&output);
            next_key = next_key.add(initialized_slot_in_struct);
            output = to_h256_from_bigint(&next_key).to_fixed_bytes();
        }else {
            output = slot_bytes.to_fixed_bytes();
        }


        let storage_change_opt = self.storage_changes
            .iter()
            .find(|storage_change| storage_change.address == Self::CONTRACT_ADDR && storage_change.key.eq(output.as_slice()));

        if storage_change_opt.is_none() {
            return None;
        }
        let storage = storage_change_opt.unwrap();

        let offset = 31;
        let number_of_bytes = 1;

        let old_data = read_bytes(storage.old_value.to_vec(), offset,number_of_bytes);
        let new_data = read_bytes(storage.new_value.to_vec(), offset,number_of_bytes);
        Some((old_data == [01u8],new_data == [01u8]))
    }


}


#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use substreams::hex;
    use substreams::scalar::BigInt;
    use substreams_ethereum::pb::eth::v2::StorageChange;
    use crate::ast::UniswapPoolStorage;

    #[test]
    fn get_slot0_sqrtPriceX96() {
        let storage_changes = vec![
            StorageChange{
                address: hex!("7858e59e0c01ea06df3af3d20ac7b0003275d4bf").to_vec(),
                key: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
                old_value: hex!("0001000001000100000000000000000000000001000000000000000000000000").to_vec(),
                new_value: hex!("000100000100010000000000000000000000000100000402dad5eda8db022960").to_vec(),
                ordinal: 0,
            }
        ];

        let storage = UniswapPoolStorage::new(&storage_changes);
        let v_opt = storage.get_slot0_sqrt_price_x96();
        assert_eq!(Some(BigInt::from_str("79228181456392528199336208736").unwrap()),v_opt);
    }

    #[test]
    fn get_slot0_initialized() {
        let storage_changes = vec![
            StorageChange{
                address: hex!("7858e59e0c01ea06df3af3d20ac7b0003275d4bf").to_vec(),
                key: hex!("a18b128af1c8fc61ff46f02d146e54546f34d340574cf2cef6a753cba6b67020").to_vec(),
                old_value: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
                new_value: hex!("0100000000000000000000000000000000000000000000000000000000000000").to_vec(),
                ordinal: 0,
            }
        ];

        let storage = UniswapPoolStorage::new(&storage_changes);
        let tick_idx =  BigInt::from(10);
        let v_opt = storage.get_ticks_initialized(&tick_idx);
        assert_eq!(Some((false, true)),v_opt);
    }
}

