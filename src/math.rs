use std::ops::{Add, Div, Mul};
use std::str::FromStr;
use bigdecimal::{BigDecimal, One, Zero};
use num_bigint::BigInt;
use pad::PadStr;

pub fn safe_div(amount0: &BigDecimal, amount1: &BigDecimal) -> BigDecimal {
    let big_decimal_zero: &BigDecimal = &BigDecimal::zero();
    return if amount1.eq(big_decimal_zero) {
        BigDecimal::zero()
    } else {
        amount0.div(amount1)
    };
}

//decode_price_bytes_to_big_decimal
pub fn price_from_bytes(price_bytes: &Vec<u8>) -> BigDecimal {
    let price_str = std::str::from_utf8(price_bytes.as_slice()).unwrap();
    return BigDecimal::from_str(price_str)
        .unwrap()
        .with_prec(100);
}


pub fn exponent_to_big_decimal(decimals: &BigInt) -> BigDecimal {
    let mut result = BigDecimal::one();
    let big_decimal_ten: &BigDecimal = &BigDecimal::from(10);
    let big_int_one: &BigInt = &BigInt::one();

    let mut i = BigInt::zero();
    while i.lt(decimals) {
        result = result.mul(big_decimal_ten);
        i = i.add(big_int_one);
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
    return big_float_amount.div(bd).with_prec(100);
}
