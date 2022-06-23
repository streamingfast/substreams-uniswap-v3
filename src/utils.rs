use std::borrow::Borrow;
use std::ops::{Div, Mul};
use ethabi::Token;
use num_bigint::BigInt;
use bigdecimal::BigDecimal;
use bigdecimal::ParseBigDecimalError::ParseBigInt;
use crate::pb;

const Q192 : BigDecimal = BigDecimal::from(2 ^ 192);

pub fn compute_prices(sqrt_price: BigInt, token0: substreams_ethereum::pb::eth::v1::BigInt, token1: Token) -> (BigDecimal, BigDecimal) {
    let price: BigDecimal = sqrt_price.mul(sqrt_price).into();

    //todo: get these!
    let token0_decimals: BigInt = Default::default();
    let token1_decimals: BigInt = Default::default();

    let price1 = price.div(Q192).mul(exponent_to_big_decimal(token0_decimals)).div(exponent_to_big_decimal(token1_decimals));
    let price0 = save_div(BigDecimal::from(1), price1.clone());

    return (price0, price1);
}

pub fn save_div(amount0: BigDecimal, amount1: BigDecimal) -> BigDecimal {
    return if amount1.eq(&BigDecimal::from(0)) {
        BigDecimal::from(0)
    } else {
        amount0.div(amount1)
    }
}

pub fn exponent_to_big_decimal(decimals: BigInt) -> BigDecimal {
    let mut result = BigDecimal::from(1);

    let i = BigInt::from(0);
    while i.lt(decimals.borrow()) {
        result = result.times(BigDecimal::from(10))
    }

    return result
}