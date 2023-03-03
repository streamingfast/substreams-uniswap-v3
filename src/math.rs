use crate::BigInt;
use std::borrow::Borrow;
use std::ops::{Add, Div, Mul};
use substreams::log;
use substreams::scalar::BigDecimal;

pub fn big_decimal_exponated(amount: BigDecimal, exponent: BigInt) -> BigDecimal {
    if exponent.is_zero() {
        return BigDecimal::one();
    }
    let negative_exponent = exponent.lt(&BigInt::zero());
    let mut result = amount.clone();
    let mut exponent_abs = exponent.clone();

    if exponent.lt(&BigInt::zero()) {
        exponent_abs = exponent.clone().mul(BigInt::one().neg());
    }

    let mut i = BigInt::zero();
    while i.lt(exponent_abs.borrow()) {
        result = result.mul(amount.clone()).with_prec(100);
        i = i.add(BigInt::one());
    }

    if negative_exponent {
        result = safe_div(&BigDecimal::one(), &result);
    }

    return result;
}

pub fn safe_div(amount0: &BigDecimal, amount1: &BigDecimal) -> BigDecimal {
    let big_decimal_zero: &BigDecimal = &BigDecimal::zero();
    return if amount1.eq(big_decimal_zero) {
        BigDecimal::zero()
    } else {
        amount0.clone().div(amount1.clone())
    };
}

pub fn exponent_to_big_decimal(decimals: &BigInt) -> BigDecimal {
    let mut result = BigDecimal::one();
    let big_decimal_ten: &BigDecimal = &BigDecimal::from(10 as i32);
    let big_int_one: &BigInt = &BigInt::one();

    let mut i = BigInt::zero();
    while i.lt(decimals) {
        result = result.mul(big_decimal_ten.clone());
        i = i.add(big_int_one.clone());
    }

    return result;
}
