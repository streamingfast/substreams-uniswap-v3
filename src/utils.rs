use std::borrow::Borrow;
use std::ops::{Add, Div, Mul};
use num_bigint::{BigInt, Sign};
use bigdecimal::{BigDecimal, FromPrimitive, One, Zero};
use bigdecimal::ParseBigDecimalError::ParseBigInt;
use prost::DecodeError;
use substreams::{proto, store};
use crate::{pb, Pool};
use substreams::pb::substreams::module::input::Store;
use substreams::store::StoreGet;
use crate::pb::tokens::Token;
use crate::pb::uniswap::Pool as uniswapPool;

const DAI_USD_KEY : &str = "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8";

pub fn compute_prices(
    sqrt_price: &BigInt,
    token_0: Token,
    token_1: Token
) -> (BigDecimal, BigDecimal) {
    let price: BigDecimal = BigDecimal::from(sqrt_price * sqrt_price);

    let token0_decimals: BigInt = BigInt::from(token_0.decimals);
    let token1_decimals: BigInt = BigInt::from(token_1.decimals);
    let q192: BigDecimal = BigDecimal::from((2 ^ 192) as u64);

    let price1 = price
        .div(q192)
        .mul(exponent_to_big_decimal(&token0_decimals))
        .div(exponent_to_big_decimal(&token1_decimals));
    let price0 = safe_div(BigDecimal::one(), price1.clone());

    return (price0, price1);
}

pub fn get_eth_price_in_usd(pool_store: StoreGet, token_store: StoreGet, token_address: &str) -> BigDecimal {
    match pool_store.get_last(&format!("pool:{}", DAI_USD_KEY)) {
        None => {
            return BigDecimal::zero();
        }
        Some(pool_bytes) => {
            let pool: Pool = proto::decode(&pool_bytes).unwrap();

            // todo: need to pass in the token0/token1 address
            match token_store.get_last(&format!("token:{}", token_address)) {
                None => {
                    return BigDecimal::zero();
                }
                Some(token_bytes) => {
                    let token: Token = proto::decode(&token_bytes).unwrap();
                }
            }
        }
    }
    return BigDecimal::zero()
}

pub fn safe_div(amount0: BigDecimal, amount1: BigDecimal) -> BigDecimal {
    let big_decimal_zero_ptr: &BigDecimal = &BigDecimal::zero();
    return if amount1.eq(big_decimal_zero_ptr) {
        BigDecimal::from(0 as u64)
    } else {
        amount0.div(amount1)
    }
}

pub fn exponent_to_big_decimal(decimals: &BigInt) -> BigDecimal {
    let mut result = BigDecimal::one();
    let big_decimal_ten: &BigDecimal = &BigDecimal::from(10 as u64);
    let big_int_zero: &BigInt = &BigInt::zero();

    let mut i = BigInt::zero();
    while i.lt(decimals.borrow()) {
        result = result.mul(big_decimal_ten);
        i = i.add(big_int_zero);
    }

    return result
}

pub fn get_last_token(tokens: &store::StoreGet, token_address: &str) -> Token {
    proto::decode(&tokens.get_last(&format!("token:{}", token_address)).unwrap()).unwrap()
}

pub fn get_last_pool(pools_store: &store::StoreGet, pool_address: &str) -> Pool {
    proto::decode(&pools_store.get_last(&format!("pool:{}", pool_address)).unwrap()).unwrap()
}