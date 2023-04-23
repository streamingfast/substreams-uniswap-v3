use substreams_ethereum::pb::eth::v2::StorageChange;
use substreams::scalar::BigInt;
use crate::storage::utils;

pub struct UniswapPoolStorage<'a> {
    pub storage_changes: &'a Vec<StorageChange>,
    pub contract_addr: [u8; 20],
}

impl<'a> UniswapPoolStorage<'a> {
    pub fn new(storage_changes: &'a Vec<StorageChange>, contract_addr: &Vec<u8>) -> UniswapPoolStorage<'a> {
        return Self {
            storage_changes,
            contract_addr: utils::contract_pad(contract_addr),
        };
    }

    pub fn fee_growth_global0x128(&self) -> Option<(BigInt, BigInt)> {
        let fee_growth_global0x128_slot = BigInt::from(1);
        let offset = 0;
        let number_of_bytes = 32;

        // ----
        let slot_key = utils::left_pad_from_bigint(&fee_growth_global0x128_slot);
        // ----

        if let Some((old_data, new_data)) = self.get_storage_changes(slot_key, offset, number_of_bytes) {
            Some((
                BigInt::from_signed_bytes_be(old_data),
                BigInt::from_signed_bytes_be(new_data),
            ))
        } else {
            None
        }
    }

    pub fn fee_growth_global1x128(&self) -> Option<(BigInt, BigInt)> {
        let fee_growth_global1x128_slot = BigInt::from(2);
        let offset = 0;
        let number_of_bytes = 32;

        // ----
        let slot_key = utils::left_pad_from_bigint(&fee_growth_global1x128_slot);
        // ----

        if let Some((old_data, new_data)) = self.get_storage_changes(slot_key, offset, number_of_bytes) {
            Some((
                BigInt::from_signed_bytes_be(old_data),
                BigInt::from_signed_bytes_be(new_data),
            ))
        } else {
            None
        }
    }

    pub fn slot0(&self) -> Slot0Struct {
        let filtered_changes: Vec<&StorageChange> = self.storage_changes.iter()
            .filter(|change| { change.address == self.contract_addr })
            .collect();

        let slot0_slot = utils::left_pad_from_bigint(&BigInt::from(0));
        return Slot0Struct::new(filtered_changes, slot0_slot)
    }

    pub fn ticks(&self, tick_idx: &BigInt) -> TickStruct {
        let filtered_changes: Vec<&StorageChange> = self.storage_changes.iter()
            .filter(|change| { change.address == self.contract_addr })
            .collect();

        let ticks_slot = utils::left_pad_from_bigint(&BigInt::from(5));
        let ticker_struct_slot = utils::calc_map_slot(&utils::left_pad_from_bigint(&tick_idx), &ticks_slot);
        return TickStruct::new(filtered_changes, ticker_struct_slot)
    }

    fn get_storage_changes(&self, slot_key: [u8; 32], offset: usize, number_of_bytes: usize) -> Option<(&[u8], &[u8])> {
        let storage_change_opt = self.storage_changes.iter().find(|storage_change| {
            storage_change.address == self.contract_addr && storage_change.key.eq(slot_key.as_slice())
        });

        if storage_change_opt.is_none() {
            return None;
        }
        let storage = storage_change_opt.unwrap();

        let old_data = utils::read_bytes(&storage.old_value, offset, number_of_bytes);
        let new_data = utils::read_bytes(&storage.new_value, offset, number_of_bytes);
        Some((old_data, new_data))
    }
}

pub struct Slot0Struct<'a> {
    pub storage_changes: Vec<&'a StorageChange>,
    pub struct_slot: [u8; 32],
}

impl<'a> Slot0Struct<'a> {
    pub fn new(storage_changes: Vec<&'a StorageChange>, struct_slot: [u8;32]) -> Slot0Struct<'a> {
        return Self {
            struct_slot: struct_slot,
            storage_changes: storage_changes,
        };
    }

    // the current price
    pub fn sqrt_price_x96(&self) -> Option<(BigInt, BigInt)> {
        let slot = BigInt::zero();
        let offset = 0;
        let number_of_bytes = 20;
        // &left_pad_from_bigint(&slot0_slot)

        let slot_key = utils::calc_struct_slot(&self.struct_slot, slot);


        if let Some((old_data, new_data)) = self.get_storage_changes(slot_key, offset, number_of_bytes) {
            Some((
                BigInt::from_signed_bytes_be(old_data),
                BigInt::from_signed_bytes_be(new_data),
            ))
        } else {
            None
        }
    }

    // the current tick
    pub fn tick(&self) -> Option<(BigInt, BigInt)>{
        let slot = BigInt::zero();
        let offset = 20;
        let number_of_bytes = 3;
        // &left_pad_from_bigint(&slot0_slot)

        let slot_key = utils::calc_struct_slot(&self.struct_slot, slot);


        if let Some((old_data, new_data)) = self.get_storage_changes(slot_key, offset, number_of_bytes) {
            Some((
                BigInt::from_signed_bytes_be(old_data),
                BigInt::from_signed_bytes_be(new_data),
            ))
        } else {
            None
        }

    }

    // the most-recently updated index of the observations array
    pub fn observation_index(&self) -> Option<(BigInt, BigInt)>{
        let slot = BigInt::zero();
        let offset = 23;
        let number_of_bytes = 2;
        // &left_pad_from_bigint(&slot0_slot)

        let slot_key = utils::calc_struct_slot(&self.struct_slot, slot);


        if let Some((old_data, new_data)) = self.get_storage_changes(slot_key, offset, number_of_bytes) {
            Some((
                BigInt::from_signed_bytes_be(old_data),
                BigInt::from_signed_bytes_be(new_data),
            ))
        } else {
            None
        }
    }

    // the current maximum number of observations that are being stored
    pub fn observation_cardinality(&self) -> Option<(BigInt, BigInt)>{
        let slot = BigInt::zero();
        let offset = 25;
        let number_of_bytes = 2;
        // &left_pad_from_bigint(&slot0_slot)

        let slot_key = utils::calc_struct_slot(&self.struct_slot, slot);


        if let Some((old_data, new_data)) = self.get_storage_changes(slot_key, offset, number_of_bytes) {
            Some((
                BigInt::from_signed_bytes_be(old_data),
                BigInt::from_signed_bytes_be(new_data),
            ))
        } else {
            None
        }
    }

    // the next maximum number of observations to store, triggered in observations.write
    pub fn observation_cardinality_next(&self) -> Option<(BigInt, BigInt)>{
        let slot = BigInt::zero();
        let offset = 27;
        let number_of_bytes = 2;
        // &left_pad_from_bigint(&slot0_slot)

        let slot_key = utils::calc_struct_slot(&self.struct_slot, slot);


        if let Some((old_data, new_data)) = self.get_storage_changes(slot_key, offset, number_of_bytes) {
            Some((
                BigInt::from_signed_bytes_be(old_data),
                BigInt::from_signed_bytes_be(new_data),
            ))
        } else {
            None
        }
    }

    // the current protocol fee as a percentage of the swap fee taken on withdrawal
    // represented as an integer denominator (1/x)%
    pub fn fee_protocol(&self) -> Option<(BigInt, BigInt)>{
        let slot = BigInt::zero();
        let offset = 29;
        let number_of_bytes = 1;
        // &left_pad_from_bigint(&slot0_slot)

        let slot_key = utils::calc_struct_slot(&self.struct_slot, slot);


        if let Some((old_data, new_data)) = self.get_storage_changes(slot_key, offset, number_of_bytes) {
            Some((
                BigInt::from_signed_bytes_be(old_data),
                BigInt::from_signed_bytes_be(new_data),
            ))
        } else {
            None
        }
    }

    // whether the pool is locked
    pub fn unlocked(&self) -> Option<(bool, bool)>{
        let slot = BigInt::zero();
        let offset = 30;
        let number_of_bytes = 1;
        // &left_pad_from_bigint(&slot0_slot)

        let slot_key = utils::calc_struct_slot(&self.struct_slot, slot);


        if let Some((old_data, new_data)) = self.get_storage_changes(slot_key, offset, number_of_bytes) {
            Some((old_data == [01u8], new_data == [01u8]))
        } else {
            None
        }
    }

    fn get_storage_changes(&self, slot_key: [u8; 32], offset: usize, number_of_bytes: usize) -> Option<(&[u8], &[u8])> {
        let storage_change_opt = self.storage_changes.iter().find(|storage_change| {
            storage_change.key.eq(slot_key.as_slice())
        });

        if storage_change_opt.is_none() {
            return None;
        }
        let storage = storage_change_opt.unwrap();

        let old_data = utils::read_bytes(&storage.old_value, offset, number_of_bytes);
        let new_data = utils::read_bytes(&storage.new_value, offset, number_of_bytes);
        Some((old_data, new_data))
    }
}

pub struct TickStruct<'a> {
    pub storage_changes: Vec<&'a StorageChange>,
    pub struct_slot: [u8; 32],
}

impl<'a> TickStruct<'a> {
    pub fn new(storage_changes: Vec<&'a StorageChange>, struct_slot: [u8;32]) -> TickStruct<'a> {
        return Self {
            struct_slot: struct_slot,
            storage_changes: storage_changes,
        };
    }

    pub fn initialized(&self) -> Option<(bool, bool)> {
        let slot = BigInt::from(3);
        let offset = 31;
        let number_of_bytes = 1;

        let slot_key = utils::calc_struct_slot(&self.struct_slot, slot);

        if let Some((old_data, new_data)) = self.get_storage_changes(slot_key, offset, number_of_bytes) {
            Some((old_data == [01u8], new_data == [01u8]))
        } else {
            None
        }
    }

    pub fn fee_growth_outside_0_x128(&self) -> Option<(BigInt, BigInt)> {
        let slot = BigInt::from(1);
        let offset = 0;
        let number_of_bytes = 32;

        let slot_key = utils::calc_struct_slot(&self.struct_slot, slot);

        if let Some((old_data, new_data)) = self.get_storage_changes(slot_key, offset, number_of_bytes) {
            Some((
                BigInt::from_signed_bytes_be(old_data),
                BigInt::from_signed_bytes_be(new_data),
            ))
        } else {
            None
        }
    }

    pub fn fee_growth_outside_1_x128(&self) -> Option<(BigInt, BigInt)> {
        let slot = BigInt::from(2);
        let offset = 0;
        let number_of_bytes = 32;

        let slot_key = utils::calc_struct_slot(&self.struct_slot, slot);

        if let Some((old_data, new_data)) = self.get_storage_changes(slot_key, offset, number_of_bytes) {
            Some((
                BigInt::from_signed_bytes_be(old_data),
                BigInt::from_signed_bytes_be(new_data),
            ))
        } else {
            None
        }
    }


    fn get_storage_changes(&self, slot_key: [u8; 32], offset: usize, number_of_bytes: usize) -> Option<(&[u8], &[u8])> {
        let storage_change_opt = self.storage_changes.iter().find(|storage_change| {
            storage_change.key.eq(slot_key.as_slice())
        });

        if storage_change_opt.is_none() {
            return None;
        }
        let storage = storage_change_opt.unwrap();

        let old_data = utils::read_bytes(&storage.old_value, offset, number_of_bytes);
        let new_data = utils::read_bytes(&storage.new_value, offset, number_of_bytes);
        Some((old_data, new_data))
    }

}


#[cfg(test)]
mod tests {
    use std::ops::Add;
    use std::str::FromStr;
    use std::{fmt::Write, num::ParseIntError};
    use substreams::scalar::BigInt;
    use substreams::{hex, Hex};
    use substreams_ethereum::pb::eth::v2::StorageChange;
    use tiny_keccak::{Hasher, Keccak};
    use crate::storage::uniswap_v3_pool::UniswapPoolStorage;

    #[test]
    fn slot0_sqrt_price_x96() {
        // derived from: https://etherscan.io/tx/0x37d8f4b1b371fde9e4b1942588d16a1cbf424b7c66e731ec915aca785ca2efcf#statechange
        let storage_changes = vec![StorageChange {
            address: hex!("7858e59e0c01ea06df3af3d20ac7b0003275d4bf").to_vec(),
            key: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
            old_value: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
            new_value: hex!("000100000100010000ff556d00000000000000001cd851cd075726f0cf78926d").to_vec(),
            ordinal: 0,
        }];

        let storage = UniswapPoolStorage::new(
            &storage_changes,
            &hex!("7858e59e0c01ea06df3af3d20ac7b0003275d4bf").to_vec(),
        );
        let v_opt = storage.slot0().sqrt_price_x96();
        assert_eq!(
            Some((
                BigInt::from_str("0").unwrap(),
                BigInt::from_str("8927094545831003674704908909").unwrap()
            )),
            v_opt
        );
    }

    #[test]
    fn slot0_tick() {
        // derived from: https://etherscan.io/tx/0x37d8f4b1b371fde9e4b1942588d16a1cbf424b7c66e731ec915aca785ca2efcf#statechange
        let storage_changes = vec![StorageChange {
            address: hex!("7858e59e0c01ea06df3af3d20ac7b0003275d4bf").to_vec(),
            key: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
            old_value: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
            new_value: hex!("000100000100010000ff556d00000000000000001cd851cd075726f0cf78926d").to_vec(),
            ordinal: 0,
        }];

        let storage = UniswapPoolStorage::new(
            &storage_changes,
            &hex!("7858e59e0c01ea06df3af3d20ac7b0003275d4bf").to_vec(),
        );
        let v_opt = storage.slot0().tick();
        assert_eq!(
            Some((
                BigInt::from_str("0").unwrap(),
                BigInt::from_str("-43667").unwrap()
            )),
            v_opt
        );
    }

    #[test]
    fn slot0_observation_index(){
        // derived from: https://etherscan.io/tx/0x37d8f4b1b371fde9e4b1942588d16a1cbf424b7c66e731ec915aca785ca2efcf#statechange
        let storage_changes = vec![StorageChange {
            address: hex!("7858e59e0c01ea06df3af3d20ac7b0003275d4bf").to_vec(),
            key: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
            old_value: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
            new_value: hex!("000100000100010000ff556d00000000000000001cd851cd075726f0cf78926d").to_vec(),
            ordinal: 0,
        }];

        let storage = UniswapPoolStorage::new(
            &storage_changes,
            &hex!("7858e59e0c01ea06df3af3d20ac7b0003275d4bf").to_vec(),
        );
        let v_opt = storage.slot0().observation_index();
        assert_eq!(
            Some((
                BigInt::from_str("0").unwrap(),
                BigInt::from_str("0").unwrap()
            )),
            v_opt
        );
    }

    #[test]
    fn slot0_observation_cardinality(){
        // derived from: https://etherscan.io/tx/0x37d8f4b1b371fde9e4b1942588d16a1cbf424b7c66e731ec915aca785ca2efcf#statechange
        let storage_changes = vec![StorageChange {
            address: hex!("7858e59e0c01ea06df3af3d20ac7b0003275d4bf").to_vec(),
            key: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
            old_value: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
            new_value: hex!("000100000100010000ff556d00000000000000001cd851cd075726f0cf78926d").to_vec(),
            ordinal: 0,
        }];

        let storage = UniswapPoolStorage::new(
            &storage_changes,
            &hex!("7858e59e0c01ea06df3af3d20ac7b0003275d4bf").to_vec(),
        );
        let v_opt = storage.slot0().observation_cardinality();
        assert_eq!(
            Some((
                BigInt::from_str("0").unwrap(),
                BigInt::from_str("1").unwrap()
            )),
            v_opt
        );
    }

    #[test]
    fn slot0_observation_cardinality_next(){
        // derived from: https://etherscan.io/tx/0x37d8f4b1b371fde9e4b1942588d16a1cbf424b7c66e731ec915aca785ca2efcf#statechange
        let storage_changes = vec![StorageChange {
            address: hex!("7858e59e0c01ea06df3af3d20ac7b0003275d4bf").to_vec(),
            key: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
            old_value: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
            new_value: hex!("000100000100010000ff556d00000000000000001cd851cd075726f0cf78926d").to_vec(),
            ordinal: 0,
        }];

        let storage = UniswapPoolStorage::new(
            &storage_changes,
            &hex!("7858e59e0c01ea06df3af3d20ac7b0003275d4bf").to_vec(),
        );
        let v_opt = storage.slot0().observation_cardinality_next();
        assert_eq!(
            Some((
                BigInt::from_str("0").unwrap(),
                BigInt::from_str("1").unwrap()
            )),
            v_opt
        );
    }

    #[test]
    fn slot0_fee_protocol(){
        // derived from: https://etherscan.io/tx/0x37d8f4b1b371fde9e4b1942588d16a1cbf424b7c66e731ec915aca785ca2efcf#statechange
        let storage_changes = vec![StorageChange {
            address: hex!("7858e59e0c01ea06df3af3d20ac7b0003275d4bf").to_vec(),
            key: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
            old_value: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
            new_value: hex!("000100000100010000ff556d00000000000000001cd851cd075726f0cf78926d").to_vec(),
            ordinal: 0,
        }];

        let storage = UniswapPoolStorage::new(
            &storage_changes,
            &hex!("7858e59e0c01ea06df3af3d20ac7b0003275d4bf").to_vec(),
        );
        let v_opt = storage.slot0().fee_protocol();
        assert_eq!(
            Some((
                BigInt::from_str("0").unwrap(),
                BigInt::from_str("0").unwrap()
            )),
            v_opt
        );
    }

    #[test]
    fn slot0_unlocked(){
        // derived from: https://etherscan.io/tx/0x37d8f4b1b371fde9e4b1942588d16a1cbf424b7c66e731ec915aca785ca2efcf#statechange
        let storage_changes = vec![StorageChange {
            address: hex!("7858e59e0c01ea06df3af3d20ac7b0003275d4bf").to_vec(),
            key: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
            old_value: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
            new_value: hex!("000100000100010000ff556d00000000000000001cd851cd075726f0cf78926d").to_vec(),
            ordinal: 0,
        }];

        let storage = UniswapPoolStorage::new(
            &storage_changes,
            &hex!("7858e59e0c01ea06df3af3d20ac7b0003275d4bf").to_vec(),
        );
        let v_opt = storage.slot0().unlocked();
        assert_eq!(
            Some((
                false,
                true
            )),
            v_opt
        );
    }

    #[test]
    fn tick_initialized() {
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
        let v_opt = storage.ticks(&BigInt::from(10)).initialized();
        assert_eq!(Some((false, true)), v_opt);
    }
}
