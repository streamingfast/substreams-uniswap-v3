use crate::storage::utils;
use std::iter::Filter;
use std::ops::Add;
use std::slice::Iter;
use substreams::scalar::BigInt;
use substreams::{log, Hex};
use substreams_ethereum::pb::eth::v2::StorageChange;
use tiny_keccak::{Hasher, Keccak};

pub struct PositionManagerStorage<'a> {
    pub storage_changes: &'a Vec<StorageChange>,
    pub contract_addr: [u8; 20],
}

impl<'a> PositionManagerStorage<'a> {
    pub fn new(storage_changes: &'a Vec<StorageChange>, contract_addr: &Vec<u8>) -> PositionManagerStorage<'a> {
        return Self {
            storage_changes,
            contract_addr: utils::contract_pad(contract_addr),
        };
    }

    pub fn next_id(&self) -> Option<(BigInt, BigInt)> {
        let slot = BigInt::from(13);
        let offset = 0;
        let number_of_bytes = 22;

        let slot_key = utils::left_pad_from_bigint(&slot);

        if let Some((old_data, new_data)) =
            utils::get_storage_change(&self.filtered_changes(), slot_key, offset, number_of_bytes)
        {
            Some((
                BigInt::from_signed_bytes_be(old_data),
                BigInt::from_signed_bytes_be(new_data),
            ))
        } else {
            None
        }
    }

    pub fn next_pool_id(&self) -> Option<(BigInt, BigInt)> {
        let slot = BigInt::from(13);
        let offset = 22;
        let number_of_bytes = 10;

        let slot_key = utils::left_pad_from_bigint(&slot);

        if let Some((old_data, new_data)) =
            utils::get_storage_change(&self.filtered_changes(), slot_key, offset, number_of_bytes)
        {
            Some((
                BigInt::from_signed_bytes_be(old_data),
                BigInt::from_signed_bytes_be(new_data),
            ))
        } else {
            None
        }
    }

    pub fn positions(&self, position_idx: &BigInt) -> PositionStruct {
        let positions_slot = utils::left_pad_from_bigint(&BigInt::from(12));
        let position_struct_slot = utils::calc_map_slot(&utils::left_pad_from_bigint(&position_idx), &positions_slot);

        return PositionStruct::new(self.filtered_changes(), position_struct_slot);
    }

    pub fn pool_id_to_pool_key(&self, poold_id: &BigInt) -> PoolKeyStruct {
        let pool_id_to_pool_key_slot = utils::left_pad_from_bigint(&BigInt::from(11));
        let pool_id_to_pool_ke_struct_slot =
            utils::calc_map_slot(&utils::left_pad_from_bigint(&poold_id), &pool_id_to_pool_key_slot);

        return PoolKeyStruct::new(self.filtered_changes(), pool_id_to_pool_ke_struct_slot);
    }

    pub fn pool_ids(&self, pool_address: &[u8; 20]) -> Option<(BigInt, BigInt)> {
        let pool_ids_slot = utils::left_pad_from_bigint(&BigInt::from(10));
        let pool_ids_address_slot = utils::calc_map_slot(&utils::left_pad(&pool_address.to_vec(), 0), &pool_ids_slot);

        if let Some((old_data, new_data)) =
            utils::get_storage_change(&self.filtered_changes(), pool_ids_address_slot, 0, 20)
        {
            Some((
                BigInt::from_signed_bytes_be(old_data),
                BigInt::from_signed_bytes_be(new_data),
            ))
        } else {
            None
        }
    }

    fn filtered_changes(&self) -> Vec<&StorageChange> {
        return self
            .storage_changes
            .iter()
            .filter(|change| change.address == self.contract_addr)
            .collect();
    }
}

pub struct PositionStruct<'a> {
    pub storage_changes: Vec<&'a StorageChange>,
    pub struct_slot: [u8; 32],
}

impl<'a> PositionStruct<'a> {
    pub fn new(storage_changes: Vec<&'a StorageChange>, struct_slot: [u8; 32]) -> PositionStruct<'a> {
        return Self {
            struct_slot: struct_slot,
            storage_changes: storage_changes,
        };
    }

    pub fn nonce(&self) -> Option<(BigInt, BigInt)> {
        let slot = BigInt::from(0);
        let offset = 0;
        let number_of_bytes = 12;

        let slot_key = utils::calc_struct_slot(&self.struct_slot, slot);

        if let Some((old_data, new_data)) =
            utils::get_storage_change(&self.storage_changes, slot_key, offset, number_of_bytes)
        {
            Some((
                BigInt::from_signed_bytes_be(old_data),
                BigInt::from_signed_bytes_be(new_data),
            ))
        } else {
            None
        }
    }

    pub fn address(&self) -> Option<([u8; 20], [u8; 20])> {
        let slot = BigInt::from(0);
        let offset = 12;
        let number_of_bytes = 20;

        let slot_key = utils::calc_struct_slot(&self.struct_slot, slot);

        if let Some((old_data, new_data)) =
            utils::get_storage_change(&self.storage_changes, slot_key, offset, number_of_bytes)
        {
            Some((
                <[u8; 20]>::try_from(old_data).unwrap(),
                <[u8; 20]>::try_from(new_data).unwrap(),
            ))
        } else {
            None
        }
    }

    pub fn pool_id(&self) -> Option<(BigInt, BigInt)> {
        let slot = BigInt::from(1);
        let offset = 0;
        let number_of_bytes = 10;

        let slot_key = utils::calc_struct_slot(&self.struct_slot, slot);

        if let Some((old_data, new_data)) =
            utils::get_storage_change(&self.storage_changes, slot_key, offset, number_of_bytes)
        {
            Some((
                BigInt::from_signed_bytes_be(old_data),
                BigInt::from_signed_bytes_be(new_data),
            ))
        } else {
            None
        }
    }

    pub fn tick_lower(&self) -> Option<(BigInt, BigInt)> {
        let slot = BigInt::from(1);
        let offset = 10;
        let number_of_bytes = 3;

        let slot_key = utils::calc_struct_slot(&self.struct_slot, slot);

        if let Some((old_data, new_data)) =
            utils::get_storage_change(&self.storage_changes, slot_key, offset, number_of_bytes)
        {
            Some((
                BigInt::from_signed_bytes_be(old_data),
                BigInt::from_signed_bytes_be(new_data),
            ))
        } else {
            None
        }
    }

    pub fn tick_upper(&self) -> Option<(BigInt, BigInt)> {
        let slot = BigInt::from(1);
        let offset = 13;
        let number_of_bytes = 3;

        let slot_key = utils::calc_struct_slot(&self.struct_slot, slot);

        if let Some((old_data, new_data)) =
            utils::get_storage_change(&self.storage_changes, slot_key, offset, number_of_bytes)
        {
            Some((
                BigInt::from_signed_bytes_be(old_data),
                BigInt::from_signed_bytes_be(new_data),
            ))
        } else {
            None
        }
    }

    pub fn liquidity(&self) -> Option<(BigInt, BigInt)> {
        let slot = BigInt::from(1);
        let offset = 16;
        let number_of_bytes = 16;

        let slot_key = utils::calc_struct_slot(&self.struct_slot, slot);

        if let Some((old_data, new_data)) =
            utils::get_storage_change(&self.storage_changes, slot_key, offset, number_of_bytes)
        {
            Some((
                BigInt::from_signed_bytes_be(old_data),
                BigInt::from_signed_bytes_be(new_data),
            ))
        } else {
            None
        }
    }

    pub fn fee_growth_inside0last_x128(&self) -> Option<(BigInt, BigInt)> {
        let slot = BigInt::from(2);
        let offset = 0;
        let number_of_bytes = 32;

        let slot_key = utils::calc_struct_slot(&self.struct_slot, slot);

        if let Some((old_data, new_data)) =
            utils::get_storage_change(&self.storage_changes, slot_key, offset, number_of_bytes)
        {
            Some((
                BigInt::from_signed_bytes_be(old_data),
                BigInt::from_signed_bytes_be(new_data),
            ))
        } else {
            None
        }
    }

    pub fn fee_growth_inside1last_x128(&self) -> Option<(BigInt, BigInt)> {
        let slot = BigInt::from(3);
        let offset = 0;
        let number_of_bytes = 32;

        let slot_key = utils::calc_struct_slot(&self.struct_slot, slot);

        if let Some((old_data, new_data)) =
            utils::get_storage_change(&self.storage_changes, slot_key, offset, number_of_bytes)
        {
            Some((
                BigInt::from_signed_bytes_be(old_data),
                BigInt::from_signed_bytes_be(new_data),
            ))
        } else {
            None
        }
    }
}

pub struct PoolKeyStruct<'a> {
    pub storage_changes: Vec<&'a StorageChange>,
    pub struct_slot: [u8; 32],
}

impl<'a> PoolKeyStruct<'a> {
    pub fn new(storage_changes: Vec<&'a StorageChange>, struct_slot: [u8; 32]) -> PoolKeyStruct<'a> {
        return Self {
            struct_slot: struct_slot,
            storage_changes: storage_changes,
        };
    }

    pub fn token0(&self) -> Option<([u8; 20], [u8; 20])> {
        let slot = BigInt::from(0);
        let offset = 0;
        let number_of_bytes = 20;

        let slot_key = utils::calc_struct_slot(&self.struct_slot, slot);

        if let Some((old_data, new_data)) =
            utils::get_storage_change(&self.storage_changes, slot_key, offset, number_of_bytes)
        {
            Some((
                <[u8; 20]>::try_from(old_data).unwrap(),
                <[u8; 20]>::try_from(new_data).unwrap(),
            ))
        } else {
            None
        }
    }

    pub fn token1(&self) -> Option<([u8; 20], [u8; 20])> {
        let slot = BigInt::from(1);
        let offset = 0;
        let number_of_bytes = 20;

        let slot_key = utils::calc_struct_slot(&self.struct_slot, slot);

        if let Some((old_data, new_data)) =
            utils::get_storage_change(&self.storage_changes, slot_key, offset, number_of_bytes)
        {
            Some((
                <[u8; 20]>::try_from(old_data).unwrap(),
                <[u8; 20]>::try_from(new_data).unwrap(),
            ))
        } else {
            None
        }
    }

    pub fn fee(&self) -> Option<(BigInt, BigInt)> {
        let slot = BigInt::from(1);
        let offset = 20;
        let number_of_bytes = 3;

        let slot_key = utils::calc_struct_slot(&self.struct_slot, slot);

        if let Some((old_data, new_data)) =
            utils::get_storage_change(&self.storage_changes, slot_key, offset, number_of_bytes)
        {
            Some((
                BigInt::from_signed_bytes_be(old_data),
                BigInt::from_signed_bytes_be(new_data),
            ))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::position_manager::PositionManagerStorage;
    use std::str::FromStr;
    use substreams::hex;
    use substreams::scalar::BigInt;
    use substreams_ethereum::pb::eth::v2::StorageChange;

    #[test]
    fn next_id() {
        let changes = get_store_changes();
        let storage = get_position_manager(&changes);
        let v_opt = storage.next_id();
        assert_eq!(
            Some((BigInt::from_str("1").unwrap(), BigInt::from_str("2").unwrap())),
            v_opt
        );
    }

    #[test]
    fn next_pool_id() {
        let changes = get_store_changes();
        let storage = get_position_manager(&changes);
        let v_opt = storage.next_pool_id();
        assert_eq!(
            Some((BigInt::from_str("2").unwrap(), BigInt::from_str("3").unwrap())),
            v_opt
        );
    }

    #[test]
    fn position_nonce() {
        let changes = get_store_changes();
        let storage = get_position_manager(&changes);
        let v_opt = storage.positions(&BigInt::from_str("1").unwrap()).nonce();
        assert_eq!(None, v_opt);
    }

    #[test]
    fn position_address() {
        let changes = get_store_changes();
        let storage = get_position_manager(&changes);
        let v_opt = storage.positions(&BigInt::from_str("1").unwrap()).address();
        assert_eq!(None, v_opt);
    }

    #[test]
    fn position_pool_id() {
        let changes = get_store_changes();
        let storage = get_position_manager(&changes);
        let v_opt = storage.positions(&BigInt::from_str("1").unwrap()).pool_id();
        assert_eq!(
            Some((BigInt::from_str("0").unwrap(), BigInt::from_str("1").unwrap())),
            v_opt
        );
    }

    #[test]
    fn position_tick_lower() {
        let changes = get_store_changes();
        let storage = get_position_manager(&changes);
        let v_opt = storage.positions(&BigInt::from_str("1").unwrap()).tick_lower();
        assert_eq!(
            Some((BigInt::from_str("0").unwrap(), BigInt::from_str("-50580").unwrap())),
            v_opt
        );
    }

    #[test]
    fn position_tick_upper() {
        let changes = get_store_changes();
        let storage = get_position_manager(&changes);
        let v_opt = storage.positions(&BigInt::from_str("1").unwrap()).tick_upper();
        assert_eq!(
            Some((BigInt::from_str("0").unwrap(), BigInt::from_str("-36720").unwrap())),
            v_opt
        );
    }

    #[test]
    fn position_liquidity() {
        let changes = get_store_changes();
        let storage = get_position_manager(&changes);
        let v_opt = storage.positions(&BigInt::from_str("1").unwrap()).liquidity();
        assert_eq!(
            Some((
                BigInt::from_str("0").unwrap(),
                BigInt::from_str("383995753785830744").unwrap()
            )),
            v_opt
        );
    }

    #[test]
    fn position_fee_growth_inside0last_x128() {
        let changes = get_store_changes();
        let storage = get_position_manager(&changes);
        let v_opt = storage
            .positions(&BigInt::from_str("1").unwrap())
            .fee_growth_inside0last_x128();
        assert_eq!(None, v_opt);
    }

    #[test]
    fn position_fee_growth_inside1last_x128() {
        let changes = get_store_changes();
        let storage = get_position_manager(&changes);
        let v_opt = storage
            .positions(&BigInt::from_str("1").unwrap())
            .fee_growth_inside1last_x128();
        assert_eq!(None, v_opt);
    }

    #[test]
    fn pool_ids() {
        let changes = get_store_changes();
        let storage = get_position_manager(&changes);
        let pool_address = hex!("1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801");
        let v_opt = storage.pool_ids(&pool_address);
        assert_eq!(
            Some((BigInt::from_str("0").unwrap(), BigInt::from_str("1").unwrap())),
            v_opt
        );
    }

    #[test]
    fn pool_key_token0() {
        let changes = get_store_changes();
        let storage = get_position_manager(&changes);
        let v_opt = storage.pool_id_to_pool_key(&BigInt::from_str("1").unwrap()).token0();
        assert_eq!(
            Some((
                hex!("0000000000000000000000000000000000000000"),
                hex!("1f9840a85d5af5bf1d1762f925bdaddc4201f984"),
            )),
            v_opt
        );
    }

    #[test]
    fn pool_key_token1() {
        let changes = get_store_changes();
        let storage = get_position_manager(&changes);
        let v_opt = storage.pool_id_to_pool_key(&BigInt::from_str("1").unwrap()).token1();
        assert_eq!(
            Some((
                hex!("0000000000000000000000000000000000000000"),
                hex!("c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"),
            )),
            v_opt
        );
    }

    #[test]
    fn pool_key_fee() {
        let changes = get_store_changes();
        let storage = get_position_manager(&changes);
        let v_opt = storage.pool_id_to_pool_key(&BigInt::from_str("1").unwrap()).fee();
        assert_eq!(
            Some((BigInt::from_str("0").unwrap(), BigInt::from_str("3000").unwrap())),
            v_opt
        );
    }

    fn get_store_changes() -> Vec<StorageChange> {
        return vec![
            StorageChange {
                address: hex!("C36442b4a4522E871399CD717aBDD847Ab11FE88").to_vec(),
                key: hex!("0000000000000000000000000000000000000000000000000000000000000002").to_vec(),
                old_value: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
                new_value: hex!("0000000000000000000000000000000000000000000000000000000000000001").to_vec(),
                ordinal: 0,
            },
            StorageChange {
                address: hex!("C36442b4a4522E871399CD717aBDD847Ab11FE88").to_vec(),
                key: hex!("000000000000000000000000000000000000000000000000000000000000000d").to_vec(),
                // manually changes this value to have a more robust test
                old_value: hex!("0000000000000000000200000000000000000000000000000000000000000001").to_vec(),
                new_value: hex!("0000000000000000000300000000000000000000000000000000000000000002").to_vec(),
                ordinal: 0,
            },
            StorageChange {
                address: hex!("C36442b4a4522E871399CD717aBDD847Ab11FE88").to_vec(),
                key: hex!("405787fa12a823e0f2b7631cc41b3ba8828b3321ca811111fa75cd3aa3bb5ace").to_vec(),
                old_value: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
                new_value: hex!("0000000000000000000000000000000000000000000000000000000000000001").to_vec(),
                ordinal: 0,
            },
            StorageChange {
                address: hex!("C36442b4a4522E871399CD717aBDD847Ab11FE88").to_vec(),
                key: hex!("405787fa12a823e0f2b7631cc41b3ba8828b3321ca811111fa75cd3aa3bb5acf").to_vec(),
                old_value: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
                new_value: hex!("00000000000000000000000011e4857bb9993a50c685a79afad4e6f65d518dda").to_vec(),
                ordinal: 0,
            },
            StorageChange {
                address: hex!("C36442b4a4522E871399CD717aBDD847Ab11FE88").to_vec(),
                key: hex!("41398631b2683820be102d6dad9a4203cddec451d10132ba7fcf563465fe521f").to_vec(),
                old_value: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
                new_value: hex!("0000000000000000000000000000000000000000000000000000000000000001").to_vec(),
                ordinal: 0,
            },
            StorageChange {
                address: hex!("C36442b4a4522E871399CD717aBDD847Ab11FE88").to_vec(),
                key: hex!("4d52bcbfde3f67abcc01436c6c962e55a565d1d42129b549e8f24ba6cfe11f79").to_vec(),
                old_value: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
                new_value: hex!("0000000000000000000000000000000000000000000000000000000000000001").to_vec(),
                ordinal: 0,
            },
            StorageChange {
                address: hex!("C36442b4a4522E871399CD717aBDD847Ab11FE88").to_vec(),
                key: hex!("618c2f39f195cd5ba7b0eaa6f4d8f02ed74f73e06ab4195212db4d8a21ff6118").to_vec(),
                old_value: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
                new_value: hex!("0000000000000000000000000000000000000000000000000000000000000001").to_vec(),
                ordinal: 0,
            },
            StorageChange {
                address: hex!("C36442b4a4522E871399CD717aBDD847Ab11FE88").to_vec(),
                key: hex!("72c6bfb7988af3a1efa6568f02a999bc52252641c659d85961ca3d372b57d5cf").to_vec(),
                old_value: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
                new_value: hex!("0000000000000000000000001f9840a85d5af5bf1d1762f925bdaddc4201f984").to_vec(),
                ordinal: 0,
            },
            StorageChange {
                address: hex!("C36442b4a4522E871399CD717aBDD847Ab11FE88").to_vec(),
                key: hex!("72c6bfb7988af3a1efa6568f02a999bc52252641c659d85961ca3d372b57d5d0").to_vec(),
                old_value: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
                new_value: hex!("000000000000000000000bb8c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2").to_vec(),
                ordinal: 0,
            },
            StorageChange {
                address: hex!("C36442b4a4522E871399CD717aBDD847Ab11FE88").to_vec(),
                key: hex!("a15bc60c955c405d20d9149c709e2460f1c2d9a497496a7f46004d1772c3054c").to_vec(),
                old_value: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
                new_value: hex!("0000000000000000000000000000000000000000000000000000000000000001").to_vec(),
                ordinal: 0,
            },
            StorageChange {
                address: hex!("C36442b4a4522E871399CD717aBDD847Ab11FE88").to_vec(),
                key: hex!("d421a5181c571bba3f01190c922c3b2a896fc1d84e86c9f17ac10e67ebef8b5d").to_vec(),
                old_value: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
                new_value: hex!("000000000000000005543a1a83a9ad58ff7090ff3a6c00000000000000000001").to_vec(),
                ordinal: 0,
            },
            StorageChange {
                address: hex!("C36442b4a4522E871399CD717aBDD847Ab11FE88").to_vec(),
                key: hex!("e111376613354238677588ac67fd7b85be95c801d8c256c0593b86b39ea8ff83").to_vec(),
                old_value: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
                new_value: hex!("0000000000000000000000000000000000000000000000000000000000000000").to_vec(),
                ordinal: 0,
            },
        ];
    }

    fn get_position_manager<'a>(changes: &'a Vec<StorageChange>) -> PositionManagerStorage<'a> {
        return PositionManagerStorage::new(changes, &hex!("C36442b4a4522E871399CD717aBDD847Ab11FE88").to_vec());
    }
}
