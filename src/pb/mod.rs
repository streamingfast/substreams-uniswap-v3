use crate::pb::uniswap::events::pool_event::Type;
use crate::pb::uniswap::events::PoolEvent;
use crate::uniswap::events::position::PositionType;
use crate::{Collect, DecreaseLiquidity, Erc20Token, IncreaseLiquidity, Pool, Transfer};
use substreams::scalar::BigDecimal;
use substreams::{log};

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
