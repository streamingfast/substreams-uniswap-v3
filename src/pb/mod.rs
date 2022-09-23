use crate::pb::position_event::PositionEventType;
use crate::uniswap::position::PositionType;
use crate::{Collect, DecreaseLiquidity, IncreaseLiquidity, Transfer};
use substreams::pb::substreams::StoreDelta;
use ethabi::Uint;
use num_bigint::BigInt;
use std::str::FromStr;

#[path = "./uniswap.types.v1.rs"]
pub mod uniswap;

#[path = "./sf.ethereum.tokens.v1.rs"]
pub mod tokens;

#[path = "./substreams.entity.v1.rs"]
pub mod entity;

#[derive(Clone, Debug, PartialEq)]
pub struct PositionEvent {
    pub event: PositionEventType,
}

pub mod position_event {
    use crate::abi::positionmanager::events::{
        Collect, DecreaseLiquidity, IncreaseLiquidity, Transfer,
    };

    #[derive(Clone, Debug, PartialEq)]
    pub enum PositionEventType {
        IncreaseLiquidity(IncreaseLiquidity),
        DecreaseLiquidity(DecreaseLiquidity),
        Collect(Collect),
        Transfer(Transfer),
    }
}

impl entity::EntityChange {
    pub fn new(entity: String, id: String, ordinal: u64) -> EntityChange {
	//"Token", "id", 1, Operation::Create,
	return
    }
    pub fn update_bigdecimal_from_delta(&self, name: &String, delta: StoreDelta) -> EntityChange {
	// WARN: si la value de la `STRING` est VIDE ici, on considère cela comme un NULL value.
	// This is to honor the new `optional` field presence when the string is empty
	// on the StoreDelta side.
	self.fields.push(Field{
	    name: name.clone(),
	    new_value: entity::value::Typed::Bigdecimal(std::from_utf8(delta.new_value).to_string()),
	    old_value: entity::value::Typed::Bigdecimal(std::from_utf8(delta.old_value).to_string()),
	});
	self
    }
    pub fn create_bigdecimal_from_string(&self, name: &String, value: &String) {
	self.fields.push(Field{
	    name: name.clone(),
	    new_value: entity::value::Typed::Bigdecimal(value),
	});
	self
    }
    pub fn create_bigdecimal(&self, name: &String, value: &Bigdecimal) {
	self.fields.push(Field{
	    name: name.clone(),
	    new_value: entity::value::Typed::Bigdecimal(value.to_string()),
	});
	self
    }

    // WARN: also here, check for nullability when the input string is empty in the Delta
    pub fn update_bigint_from_delta();
    pub fn update_int32_from_delta();
    pub fn update_string_from_delta();
    pub fn update_bytes_from_delta();
    pub fn update_bool_from_delta();

    
}

impl PositionEvent {
    //todo: create methods to get the data with the various types
    // which some of them will return nothing
    pub fn get_token_id(&self) -> Uint {
        return match &self.event {
            PositionEventType::IncreaseLiquidity(evt) => evt.token_id,
            PositionEventType::DecreaseLiquidity(evt) => evt.token_id,
            PositionEventType::Collect(evt) => evt.token_id,
            PositionEventType::Transfer(evt) => evt.token_id,
        };
    }

    pub fn get_liquidity(&self) -> String {
        return match &self.event {
            PositionEventType::IncreaseLiquidity(evt) => evt.liquidity.to_string(),
            PositionEventType::DecreaseLiquidity(evt) => evt.liquidity.to_string(),
            PositionEventType::Collect(evt) => "0".to_string(),
            PositionEventType::Transfer(evt) => "0".to_string(),
        };
    }

    pub fn get_amount0(&self) -> BigInt {
        return match &self.event {
            PositionEventType::IncreaseLiquidity(evt) => {
                BigInt::from_str(evt.amount0.to_string().as_str()).unwrap()
            }
            PositionEventType::DecreaseLiquidity(evt) => {
                BigInt::from_str(evt.amount0.to_string().as_str()).unwrap()
            }
            PositionEventType::Collect(evt) => {
                BigInt::from_str(evt.amount0.to_string().as_str()).unwrap()
            }
            PositionEventType::Transfer(_) => BigInt::from(0 as i32),
        };
    }

    pub fn get_amount1(&self) -> BigInt {
        return match &self.event {
            PositionEventType::IncreaseLiquidity(evt) => {
                BigInt::from_str(evt.amount1.to_string().as_str()).unwrap()
            }
            PositionEventType::DecreaseLiquidity(evt) => {
                BigInt::from_str(evt.amount1.to_string().as_str()).unwrap()
            }
            PositionEventType::Collect(evt) => {
                BigInt::from_str(evt.amount1.to_string().as_str()).unwrap()
            }
            PositionEventType::Transfer(_) => BigInt::from(0 as i32),
        };
    }
}

impl PositionType {
    pub fn to_string(self) -> String {
        return match self {
            IncreaseLiquidity => "IncreaseLiquidity".to_string(),
            DecreaseLiquidity => "DecreaseLiquidity".to_string(),
            Collect => "Collect".to_string(),
            Transfer => "Transfer".to_string(),
            _ => "".to_string(),
        };
    }

    pub fn get_position_type(i: i32) -> PositionType {
        return if i == 1 {
            IncreaseLiquidity
        } else if i == 2 {
            Collect
        } else if i == 3 {
            DecreaseLiquidity
        } else if i == 4 {
            Transfer
        } else {
            panic!("Unset should never have occurred");
        };
    }
}
