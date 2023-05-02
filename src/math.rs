use crate::ticks_idx::ONE_POINT_0001;
use crate::BigInt;
use std::borrow::Borrow;
use std::ops::{Add, Div, Mul};
use substreams::scalar::BigDecimal;

pub fn big_decimal_exponated(amount: BigDecimal, exponent: i32) -> BigDecimal {
    if exponent == 0 {
        return BigDecimal::one();
    }
    let negative_exponent = exponent < 0;
    let mut result = amount.clone();
    let mut exponent_abs = exponent;

    if exponent < 0 {
        exponent_abs = exponent * -1;
    }

    let mut i = 1;
    while i < exponent_abs {
        result = result.mul(amount.clone()).with_prec(34);
        i += 1;
    }

    if negative_exponent {
        result = safe_div(&BigDecimal::one(), &result).with_prec(34);
    }

    return result;
}

pub fn compute_price_from_tick_idx(desired_tick_idx: i32) -> BigDecimal {
    let base = desired_tick_idx - (desired_tick_idx % 1000);
    let ratio = BigDecimal::try_from(1.0001).unwrap().with_prec(34);
    let mut val = BigDecimal::try_from(*ONE_POINT_0001.get(&base).unwrap()).unwrap();

    let mut idx = base;
    while idx <= desired_tick_idx {
        val = val.mul(ratio.clone()).with_prec(100);
        idx += 1;
    }

    return val;
}

pub fn safe_div(amount0: &BigDecimal, amount1: &BigDecimal) -> BigDecimal {
    let big_decimal_zero: &BigDecimal = &BigDecimal::zero();
    return if amount1.eq(big_decimal_zero) {
        BigDecimal::zero()
    } else {
        amount0.clone().div(amount1.clone())
    };
}

pub fn exponent_to_big_decimal(decimals: u64) -> BigDecimal {
    let mut result = BigDecimal::one();
    let big_decimal_ten: &BigDecimal = &BigDecimal::from(10 as i32);

    let mut i = 1 as u64;
    while i < decimals {
        result = result.mul(big_decimal_ten.clone());
        i += 1;
    }

    return result;
}
