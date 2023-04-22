use crate::pb::position_event::PositionEventType;
use crate::pb::uniswap::events::pool_event::Type;
use crate::pb::uniswap::events::PoolEvent;
use crate::uniswap::events::position::PositionType;
use crate::uniswap::events::{PoolSqrtPrice, Position};
use crate::utils::ZERO_ADDRESS;
use crate::{BigInt, Collect, DecreaseLiquidity, Erc20Token, IncreaseLiquidity, Pool, Transfer};
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
            pt if pt == PositionType::IncreaseLiquidity as i32 => return PositionType::IncreaseLiquidity,
            pt if pt == PositionType::DecreaseLiquidity as i32 => return PositionType::DecreaseLiquidity,
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
    use crate::abi::positionmanager::events::{Collect, DecreaseLiquidity, IncreaseLiquidity, Transfer};

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
            PositionEventType::IncreaseLiquidity(evt) => BigInt::from_str(evt.amount0.to_string().as_str()).unwrap(),
            PositionEventType::DecreaseLiquidity(evt) => BigInt::from_str(evt.amount0.to_string().as_str()).unwrap(),
            PositionEventType::Collect(evt) => BigInt::from_str(evt.amount0.to_string().as_str()).unwrap(),
            PositionEventType::Transfer(_) => BigInt::from(0 as i32),
        };
    }

    pub fn get_amount1(&self) -> BigInt {
        return match &self.event {
            PositionEventType::IncreaseLiquidity(evt) => BigInt::from_str(evt.amount1.to_string().as_str()).unwrap(),
            PositionEventType::DecreaseLiquidity(evt) => BigInt::from_str(evt.amount1.to_string().as_str()).unwrap(),
            PositionEventType::Collect(evt) => BigInt::from_str(evt.amount1.to_string().as_str()).unwrap(),
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

pub struct AdjustedAmounts {
    // pub token0: BigDecimal,
    // pub token0_abs: BigDecimal,
    // pub token0_eth: BigDecimal,
    // pub token0_usd: BigDecimal,
    // pub token1: BigDecimal,
    // pub token1_abs: BigDecimal,
    // pub token1_eth: BigDecimal,
    // pub token1_usd: BigDecimal,
    pub stable_eth: BigDecimal,
    pub stable_usd: BigDecimal,
    pub stable_eth_untracked: BigDecimal,
    pub stable_usd_untracked: BigDecimal,
}

impl PoolEvent {
    pub fn get_amounts(&self) -> Option<TokenAmounts> {
        return match self.r#type.as_ref().unwrap().clone() {
            Type::Mint(evt) => Some(TokenAmounts {
                amount0: BigDecimal::try_from(evt.amount_0).unwrap(),
                amount1: BigDecimal::try_from(evt.amount_1).unwrap(),
                token0_addr: self.token0.clone(),
                token1_addr: self.token1.clone(),
            }),
            Type::Burn(evt) => Some(TokenAmounts {
                amount0: BigDecimal::try_from(evt.amount_0).unwrap().neg(),
                amount1: BigDecimal::try_from(evt.amount_1).unwrap().neg(),
                token0_addr: self.token0.clone(),
                token1_addr: self.token1.clone(),
            }),
            Type::Swap(evt) => Some(TokenAmounts {
                amount0: BigDecimal::try_from(evt.amount_0).unwrap(),
                amount1: BigDecimal::try_from(evt.amount_1).unwrap(),
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
