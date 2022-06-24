use std::borrow::Borrow;
use std::ops::{Add, Div, Mul};
use num_bigint::BigInt;
use bigdecimal::{BigDecimal, FromPrimitive, One, Zero};
use bigdecimal::ParseBigDecimalError::ParseBigInt;
use prost::DecodeError;
use substreams::{proto, store};
use crate::{pb, Pool};
use crate::pb::tokens;

pub fn compute_prices(
    sqrt_price: &BigInt,
    token_0: tokens::Token, // fixme: this is a hack, need to get the token from substreams_eth_tokens
    token_1: tokens::Token // fixme: this is a hack, need to get the token from substreams_eth_tokens
) -> (BigDecimal, BigDecimal) {
    let q192: BigDecimal = BigDecimal::from((2 ^ 192) as u64);
    let price: BigDecimal = BigDecimal::from(sqrt_price * sqrt_price);

    let token0_decimals: BigInt = BigInt::from(token_0.decimals);
    let token1_decimals: BigInt = BigInt::from(token_1.decimals);

    let price1 = price
        .div(q192)
        .mul(exponent_to_big_decimal(token0_decimals))
        .div(exponent_to_big_decimal(token1_decimals));

    let price0 = save_div(BigDecimal::from_i32(1).unwrap(), &price1);
    return (price0, price1);
}

pub fn save_div(amount0: BigDecimal, amount1: &BigDecimal) -> BigDecimal {
    return if amount1.eq(&BigDecimal::from(0)) {
        BigDecimal::from(0)
    } else {
        amount0.div(amount1)
    }
}

pub fn exponent_to_big_decimal(decimals: BigInt) -> BigDecimal {
    let mut result = BigDecimal::one();

    let mut i: BigInt = BigInt::zero();
    let one: BigInt = BigInt::one();
    let ten: BigInt = BigInt::one().mul(10); // horrible hack
    while i.lt(&decimals) {
        result = result.mul(&ten);
        i = i.add(&one);
    }

    return result
}

pub fn get_last_token(tokens: &store::StoreGet, token_address: &str) -> tokens::Token {
    proto::decode(&tokens.get_last(&format!("token:{}", token_address)).unwrap()).unwrap()
}

pub fn get_last_pool(pools_store: &store::StoreGet, pool_address: &str) -> Pool {
    proto::decode(&pools_store.get_last(&format!("pool:{}", pool_address)).unwrap()).unwrap()
}