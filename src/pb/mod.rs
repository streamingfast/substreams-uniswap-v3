use crate::pb::uniswap::events::pool_event::Type;
use crate::pb::uniswap::events::position_event::Type::{
    CollectPosition, CreatedPosition, DecreaseLiquidityPosition, IncreaseLiquidityPosition, TransferPosition,
};
use crate::pb::uniswap::events::PoolEvent;
use crate::pb::uniswap::events::PositionEvent;
use crate::utils::ERROR_POOL;
use crate::{Erc20Token, Pool};
use substreams::scalar::BigDecimal;
use substreams::{log, Hex};

#[allow(unused_imports)]
#[allow(dead_code)]
#[path = "./uniswap.types.v1.rs"]
pub mod uniswap;

impl PositionEvent {
    pub fn get_ordinal(&self) -> u64 {
        return match self.r#type.as_ref().unwrap() {
            CreatedPosition(item) => item.log_ordinal,
            IncreaseLiquidityPosition(item) => item.log_ordinal,
            DecreaseLiquidityPosition(item) => item.log_ordinal,
            CollectPosition(item) => item.log_ordinal,
            TransferPosition(item) => item.log_ordinal,
        };
    }
}

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
        return &self.address != &Hex(ERROR_POOL).to_string();
    }

    pub fn should_handle_mint_and_burn(&self) -> bool {
        if self.ignore_pool {
            return false;
        }
        return &self.address != &Hex(ERROR_POOL).to_string();
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
    pub delta_tvl_eth: BigDecimal,
    pub delta_tvl_usd: BigDecimal,
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
