use crate::BigInt;
use num_bigint::BigUint;
use pad::PadStr;
use std::borrow::Borrow;
use std::ops::{Add, Div, Mul, Neg};
use std::str::FromStr;
use substreams::scalar::BigDecimal;

pub fn big_decimal_exponated(amount: BigDecimal, exponent: BigInt) -> BigDecimal {
    if exponent.is_zero() {
        return BigDecimal::one().with_prec(100);
    }
    if exponent.is_one() {
        return amount;
    }
    if exponent.lt(&BigInt::zero()) {
        return safe_div(
            &BigDecimal::one().with_prec(100),
            &big_decimal_exponated(amount, exponent.neg()),
        );
    }

    let mut result = amount.clone();
    let big_int_one: BigInt = BigInt::one();

    let mut i = BigInt::zero();
    while i.lt(exponent.borrow()) {
        result = result.mul(amount.clone()).with_prec(100);
        i = i.add(big_int_one.clone());
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

// converts the string representation (in bytes) of a decimal
pub fn decimal_from_bytes(price_bytes: &Vec<u8>) -> BigDecimal {
    if price_bytes.len() == 0 {
        return BigDecimal::zero();
    }
    let price_str = std::str::from_utf8(price_bytes.as_slice()).unwrap();
    return BigDecimal::from_str(price_str).unwrap();
}

pub fn decimal_from_hex_be_bytes(price_bytes: &Vec<u8>) -> BigDecimal {
    let big_uint_amount = BigUint::from_bytes_be(price_bytes.as_slice());
    return BigDecimal::from_str(big_uint_amount.to_string().as_str()).unwrap();
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

pub fn divide_by_decimals(big_float_amount: BigDecimal, decimals: u64) -> BigDecimal {
    let bd = BigDecimal::from_str(
        "1".pad_to_width_with_char((decimals + 1) as usize, '0')
            .as_str(),
    )
    .unwrap()
    .with_prec(100);
    return big_float_amount.div(bd);
}
