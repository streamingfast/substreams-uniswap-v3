use std::borrow::Borrow;
use std::ops::{Add, Div, Mul};
use num_bigint::BigInt;
use bigdecimal::{BigDecimal, FromPrimitive, One, Zero};
use bigdecimal::ParseBigDecimalError::ParseBigInt;
use prost::DecodeError;
use substreams::{proto, store};
use crate::{pb, Pool};
use crate::pb::tokens;
use substreams::pb::substreams::module::input::Store;
use substreams::store::StoreGet;
use crate::pb::uniswap::Pool as uniswapPool;

const Q192 : BigDecimal = BigDecimal::from(2 ^ 192);
const BD_ZERO: BigDecimal = BigDecimal::from(0);
const BD_ONE: BigDecimal = BigDecimal::from(1);
const BI_ONE: BigInt = BigInt::from(1);
const BD_TEN: BigDecimal = BigDecimal::from(10);
const BD_ZERO_P: &BigDecimal = &BigDecimal::from(0);

const DAI_USD_KEY : &String = &"".to_string();

pub fn compute_prices(
    sqrt_price: &BigInt,
    token_0: tokens::Token, // fixme: this is a hack, need to get the token from substreams_eth_tokens
    token_1: tokens::Token // fixme: this is a hack, need to get the token from substreams_eth_tokens
) -> (BigDecimal, BigDecimal) {
    let price: BigDecimal = BigDecimal::from(sqrt_price * sqrt_price);

    let token0_decimals: BigInt = BigInt::from(token_0.decimals);
    let token1_decimals: BigInt = BigInt::from(token_1.decimals);

    let price1 = price
        .div(q192)
        .mul(exponent_to_big_decimal(token0_decimals.clone()))
        .div(exponent_to_big_decimal(token1_decimals.clone()));

    let price1 = price.div(Q192).mul(exponent_to_big_decimal(token0_decimals)).div(exponent_to_big_decimal(token1_decimals));
    let price0 = safe_div(BD_ONE, price1.clone());

    return (price0, price1);
}

pub fn get_eth_price_in_usd(pool_store: StoreGet, token_store: StoreGet) -> BigDecimal {
    match pool_store.get_last(DAI_USD_KEY) {
        None => {
            return BD_ZERO;
        }
        Some(pool_bytes) => {
            pool : Pool = proto::decode(&pool_bytes).unwrap();

            match token_store.get_last(&"".to_string()) {
                None => {
                    return BD_ZERO;
                }
                Some(token_bytes) => {
                    token : tokens::Token = proto::decode(&token_bytes).unwrap();

                }
            }
        }
    }
    return BD_ZERO
}

pub fn safe_div(amount0: BigDecimal, amount1: BigDecimal) -> BigDecimal {
    return if amount1.eq(BD_ZERO_P) {
        BigDecimal::from(0)
    } else {
        amount0.div(amount1)
    }
}

pub fn exponent_to_big_decimal(decimals: BigInt) -> BigDecimal {
    let mut result = BigDecimal::one();

    let mut i = BigInt::from(0);
    while i.lt(decimals.borrow()) {
        result = result.times(BD_TEN);
        i = i.add(BI_ONE);
    }

    return result
}

pub fn get_last_token(tokens: &store::StoreGet, token_address: &str) -> tokens::Token {
    proto::decode(&tokens.get_last(&format!("token:{}", token_address)).unwrap()).unwrap()
}

pub fn get_last_pool(pools_store: &store::StoreGet, pool_address: &str) -> Pool {
    proto::decode(&pools_store.get_last(&format!("pool:{}", pool_address)).unwrap()).unwrap()
}