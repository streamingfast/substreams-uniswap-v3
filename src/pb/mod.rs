use crate::pb::position_event::PositionEventType;
use crate::uniswap::position::PositionType;
use crate::{Collect, DecreaseLiquidity, IncreaseLiquidity, Transfer};
use ethabi::Uint;
use num_bigint::BigInt;
use std::str::FromStr;

#[path = "./uniswap.types.v1.rs"]
pub mod uniswap;

#[path = "./sf.ethereum.tokens.v1.rs"]
pub mod tokens;

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
