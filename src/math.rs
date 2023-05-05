use crate::ticks_idx::ONE_POINT_0001;
use std::ops::{Div, Mul};
use substreams::scalar::BigDecimal;

pub fn compute_price_from_tick_idx(desired_tick_idx: i32) -> BigDecimal {
    if desired_tick_idx == 0 {
        return BigDecimal::one();
    }

    let desired_tick_idx_abs = desired_tick_idx.abs();
    let base_abs = desired_tick_idx_abs - (desired_tick_idx_abs % 1000);
    let ratio = BigDecimal::try_from(1.0001).unwrap().with_prec(100);
    let mut val = BigDecimal::try_from(*ONE_POINT_0001.get(&base_abs).unwrap()).unwrap();

    let mut idx = base_abs;
    while idx < desired_tick_idx_abs {
        val = val.mul(ratio.clone()).with_prec(100);
        idx += 1;
    }

    if desired_tick_idx.lt(&0) {
        val = safe_div(&BigDecimal::one(), &val).with_prec(100);
    }

    return val.with_prec(100);
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

#[cfg(test)]
mod test {
    use crate::math::compute_price_from_tick_idx;
    use std::str::FromStr;
    use substreams::prelude::BigDecimal;

    #[test]
    fn test_positive_tick_idx() {
        let tick_idx = 257820;
        let actual_value = compute_price_from_tick_idx(tick_idx);
        let expected_value = BigDecimal::from_str(
            "157188409912.8279800665572784382799429044388135818770675416117949775512036146406973508030483126972441",
        )
        .unwrap();
        assert_eq!(expected_value, actual_value);
    }

    #[test]
    fn test_negative_tick_idx() {
        let tick_idx = -16200;
        let actual_value = compute_price_from_tick_idx(tick_idx);
        let expected_value = BigDecimal::from_str(
            "0.1979147284588052764428880652056914101428568621377186361720748341060154174725108347202390730358454528",
        )
        .unwrap();
        assert_eq!(expected_value, actual_value);
    }
}
