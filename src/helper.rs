use bigdecimal::{BigDecimal, Zero};
use substreams::{errors::Error, proto, store::StoreGet};
use crate::{keyer, math, pb};

pub fn get_pool_sqrt_price(
    pool_sqrt_price_store: &StoreGet,
    pool_address: &String,
) -> Result<pb::uniswap::PoolSqrtPrice, Error> {
    return match &pool_sqrt_price_store.get_last(&keyer::pool_sqrt_price_key(&pool_address)) {
        None => Err(Error::Unexpected("no pool sqrt price found".to_string())),
        Some(bytes) => Ok(proto::decode(bytes).unwrap()),
    };
}

pub fn get_pool(
    pool_store: &StoreGet,
    pool_address: &String,
) -> Result<pb::uniswap::Pool, Error> {
    return match &pool_store.get_last(&keyer::pool_key(&pool_address)) {
        None => Err(Error::Unexpected("pool not dount".to_string())),
        Some(bytes) => Ok(proto::decode(bytes).unwrap()),
    };
}

pub fn get_price(
    prices_store: &StoreGet,
    token_numerator_address: &String,
    token_denominator_address: &String
) -> Result<BigDecimal, Error> {
    return match &prices_store.get_last(&keyer::prices_token_pair(token_numerator_address, token_denominator_address)) {
        None => Err(Error::Unexpected("price not found".to_string())),
        Some(bytes) => Ok(math::price_from_bytes(&bytes)),
    };
}

pub fn get_price_at(
    prices_store: &StoreGet,
    ordinal: u64,
    token_numerator_address: &String,
    token_denominator_address: &String
) -> Result<BigDecimal, Error> {
    return match &prices_store.get_at(ordinal, &keyer::prices_token_pair(token_numerator_address, token_denominator_address)) {
        None => Err(Error::Unexpected("price not found".to_string())),
        Some(bytes) => Ok(math::price_from_bytes(&bytes)),
    };
}

pub fn get_pool_total_value_locked_token_or_zero(
    total_value_locked_store: &StoreGet,
    pool_address: &String,
    token_address: &String,
) -> BigDecimal {
    return match &total_value_locked_store.get_last(&keyer::pool_total_value_locked_token(pool_address, token_address)) {
        None => BigDecimal::zero().with_prec(100),
        Some(bytes) => BigDecimal::parse_bytes(bytes.as_slice(), 10)
            .unwrap()
            .with_prec(100),
    };
}
