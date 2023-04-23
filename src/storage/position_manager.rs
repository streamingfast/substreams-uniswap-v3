use std::iter::Filter;
use std::ops::Add;
use std::slice::Iter;
use substreams::scalar::BigInt;
use substreams::{log, Hex};
use substreams_ethereum::pb::eth::v2::StorageChange;
use tiny_keccak::{Hasher, Keccak};
use crate::storage::utils;


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

    pub fn get_token0_id(&self) -> Option<String> {
        let contract_slot_position = BigInt::from(1);
        let token0_slot = BigInt::from(0);
        let offset = 0;
        let number_of_bytes = 20;
        return None;
    }

    pub fn get_token1_id(&self) -> Option<String> {
        let contract_slot_position = BigInt::from(1);
        let token1_slot = BigInt::from(1);
        let offset = 0;
        let number_of_bytes = 20;
        return None;
    }

    pub fn get_fee(&self) -> Option<BigInt> {
        let contract_slot_position = BigInt::from(1);
        let fee_slot = BigInt::from(1);
        let offset = 20;
        let number_of_bytes = 3;
        return None;
    }

    pub fn get_tick_lower(&self) -> Option<BigInt> {
        let contract_slot_position = BigInt::from(4);
        // let tick_lower_slot = BigInt::from();
        return None;
    }

    pub fn get_tick_upper(&self) -> Option<BigInt> {
        let contract_slot_position = BigInt::from(4);
        return None;
    }

    pub fn get_fee_growth_inside0_last_x128(&self) -> Option<BigInt> {
        let contract_slot_position = BigInt::from(4);
        return None;
    }

    pub fn get_fee_growth_inside1_last_x128(&self) -> Option<BigInt> {
        let contract_slot_position = BigInt::from(4);
        return None;
    }
}