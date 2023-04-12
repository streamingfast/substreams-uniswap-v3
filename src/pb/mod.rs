use crate::pb::position_event::PositionEventType;
use crate::pb::uniswap::events::pool_event::Type;
use crate::pb::uniswap::events::PoolEvent;
use crate::uniswap::events::position::PositionType;
use crate::uniswap::events::{PoolSqrtPrice, Position};
use crate::uniswap::{BigDecimal as PbBigDecimal, BigInt as PbBigInt};
use crate::utils::ZERO_ADDRESS;
use crate::{BigInt, Collect, DecreaseLiquidity, Erc20Token, IncreaseLiquidity, Pool, Transfer};
use ethabi::Uint;
use std::str::FromStr;
use substreams::scalar::BigDecimal;
use substreams::{log, Hex};

#[allow(unused_imports)]
#[allow(dead_code)]
#[path = "./uniswap.types.v1.rs"]
pub mod uniswap;

#[allow(unused_imports)]
#[allow(dead_code)]
#[path = "./sf.ethereum.tokens.v1.rs"]
pub mod tokens;

impl Erc20Token {
    pub fn log(&self) {
        log::info!(
            "token addr: {}, name: {}, symbol: {}, decimals: {}",
            self.address,
            self.decimals,
            self.symbol,
            self.name
        );
    }
}

impl From<PbBigInt> for BigDecimal {
    fn from(bi: PbBigInt) -> Self {
        let big_int: BigInt = bi.into();
        BigDecimal::from(big_int)
    }
}

impl From<PbBigInt> for BigInt {
    fn from(bi: PbBigInt) -> Self {
        BigInt::from_str(bi.value.as_str()).unwrap()
    }
}

impl From<&PbBigInt> for BigInt {
    fn from(bi: &PbBigInt) -> Self {
        BigInt::from_str(bi.value.as_str()).unwrap()
    }
}

impl Into<PbBigInt> for &BigInt {
    fn into(self) -> PbBigInt {
        PbBigInt {
            value: self.to_string(),
        }
    }
}

impl Into<PbBigInt> for BigInt {
    fn into(self) -> PbBigInt {
        PbBigInt {
            value: self.to_string(),
        }
    }
}

impl Into<PbBigInt> for Uint {
    fn into(self) -> PbBigInt {
        PbBigInt {
            value: self.to_string(),
        }
    }
}

impl From<PbBigInt> for u32 {
    fn from(bi: PbBigInt) -> Self {
        u32::from_str(bi.value.as_str()).unwrap()
    }
}

impl From<&PbBigInt> for u32 {
    fn from(bi: &PbBigInt) -> Self {
        u32::from_str(bi.value.as_str()).unwrap()
    }
}

impl Into<PbBigInt> for u32 {
    fn into(self) -> PbBigInt {
        PbBigInt {
            value: self.to_string(),
        }
    }
}

impl From<PbBigDecimal> for BigDecimal {
    fn from(bd: PbBigDecimal) -> Self {
        BigDecimal::from_str(bd.value.as_str()).unwrap()
    }
}

impl From<&PbBigDecimal> for BigDecimal {
    fn from(bd: &PbBigDecimal) -> Self {
        BigDecimal::from_str(bd.value.as_str()).unwrap()
    }
}

impl From<BigDecimal> for PbBigDecimal {
    fn from(bd: BigDecimal) -> Self {
        PbBigDecimal {
            value: bd.to_string(),
        }
    }
}

impl From<&BigDecimal> for PbBigDecimal {
    fn from(bd: &BigDecimal) -> Self {
        PbBigDecimal {
            value: bd.to_string(),
        }
    }
}

impl PoolSqrtPrice {
    pub fn sqrt_price(&self) -> BigInt {
        return match &self.sqrt_price {
            None => BigInt::zero(),
            Some(value) => BigInt::from(value),
        };
    }

    pub fn tick(&self) -> BigInt {
        return match &self.tick {
            None => BigInt::zero(),
            Some(value) => BigInt::from(value),
        };
    }
}

impl Pool {
    pub fn should_handle_swap(&self) -> bool {
        if self.ignore_pool {
            return false;
        }
        return &self.address != "9663f2ca0454accad3e094448ea6f77443880454";
    }

    pub fn should_handle_mint_and_burn(&self) -> bool {
        if self.ignore_pool {
            return false;
        }
        return true;
    }

    pub fn token0_ref(&self) -> &Erc20Token {
        self.token0.as_ref().unwrap()
    }

    pub fn token1_ref(&self) -> &Erc20Token {
        self.token1.as_ref().unwrap()
    }

    pub fn token0(&self) -> Erc20Token {
        self.clone().token0.unwrap()
    }

    pub fn token1(&self) -> Erc20Token {
        self.clone().token1.unwrap()
    }

    pub fn fee_tier_value(&self) -> String {
        self.clone().fee_tier.unwrap().value
    }
}

impl Erc20Token {
    pub fn address(&self) -> &String {
        &self.address
    }
}

impl Position {
    pub fn convert_position_type(&self) -> PositionType {
        match self.position_type {
            pt if pt == PositionType::Unset as i32 => return PositionType::Unset,
            pt if pt == PositionType::IncreaseLiquidity as i32 => {
                return PositionType::IncreaseLiquidity
            }
            pt if pt == PositionType::DecreaseLiquidity as i32 => {
                return PositionType::DecreaseLiquidity
            }
            pt if pt == PositionType::Collect as i32 => return PositionType::Collect,
            pt if pt == PositionType::Transfer as i32 => return PositionType::Transfer,
            _ => panic!("unhandled operation: {}", self.position_type),
        }
    }
}

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
    pub fn get_token_id(&self) -> BigInt {
        return match &self.event {
            PositionEventType::IncreaseLiquidity(evt) => evt.token_id.clone(),
            PositionEventType::DecreaseLiquidity(evt) => evt.token_id.clone(),
            PositionEventType::Collect(evt) => evt.token_id.clone(),
            PositionEventType::Transfer(evt) => evt.token_id.clone(),
        };
    }

    pub fn get_liquidity(&self) -> String {
        return match &self.event {
            PositionEventType::IncreaseLiquidity(evt) => evt.liquidity.to_string(),
            PositionEventType::DecreaseLiquidity(evt) => evt.liquidity.to_string(),
            PositionEventType::Collect(_) => "0".to_string(),
            PositionEventType::Transfer(_) => "0".to_string(),
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

    pub fn get_owner(&self) -> String {
        return match &self.event {
            PositionEventType::IncreaseLiquidity(_)
            | PositionEventType::DecreaseLiquidity(_)
            | PositionEventType::Collect(_) => Hex(ZERO_ADDRESS).to_string(),
            PositionEventType::Transfer(evt) => Hex(&evt.to).to_string(),
        };
    }
}

pub struct TokenAmounts {
    pub amount0: BigDecimal,
    pub amount1: BigDecimal,
    pub token0_addr: String,
    pub token1_addr: String,
}

impl PoolEvent {
    pub fn get_amounts(&self) -> Option<TokenAmounts> {
        return match self.r#type.as_ref().unwrap().clone() {
            Type::Mint(evt) => Some(TokenAmounts {
                amount0: evt.amount_0.unwrap().into(),
                amount1: evt.amount_1.unwrap().into(),
                token0_addr: self.token0.clone(),
                token1_addr: self.token1.clone(),
            }),
            Type::Burn(evt) => Some(TokenAmounts {
                amount0: <uniswap::BigDecimal as Into<BigDecimal>>::into(evt.amount_0.unwrap())
                    .neg(),
                amount1: <uniswap::BigDecimal as Into<BigDecimal>>::into(evt.amount_1.unwrap())
                    .neg(),
                // amount1: evt.amount_1.unwrap().into().neg(),
                token0_addr: self.token0.clone(),
                token1_addr: self.token1.clone(),
            }),
            Type::Swap(evt) => Some(TokenAmounts {
                amount0: evt.amount_0.unwrap().into(),
                amount1: evt.amount_1.unwrap().into(),
                token0_addr: self.token0.clone(),
                token1_addr: self.token1.clone(),
            }),
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
            PositionType::IncreaseLiquidity
        } else if i == 2 {
            PositionType::Collect
        } else if i == 3 {
            PositionType::DecreaseLiquidity
        } else if i == 4 {
            PositionType::Transfer
        } else {
            panic!("Unset should never have occurred");
        };
    }
}
