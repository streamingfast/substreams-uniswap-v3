use crate::{ Erc20Token, math, helper};
use bigdecimal::{BigDecimal, One, Zero};
use num_bigint::BigInt;
use std::borrow::Borrow;
use std::ops::{Div, Mul};
use std::str;
use std::str::FromStr;
use substreams::{ log };
use substreams::store::StoreGet;

const USDC_WETH_03_POOL: &str = "8ad599c3a0ff1de082011efddc58f1908eb6e6d8";
const USDC_ADDRESS: &str = "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
const WETH_ADDRESS: &str = "c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2";
pub const WHITELIST_TOKENS: [&str; 21] = [
    "c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", // WETH
    "6b175474e89094c44da98b954eedeac495271d0f", // DAI
    "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", // USDC
    "dac17f958d2ee523a2206206994597c13d831ec7", // USDT
    "0000000000085d4780b73119b644ae5ecd22b376", // TUSD
    "2260fac5e5542a773aa44fbcfedf7c193bc2c599", // WBTC
    "5d3a536e4d6dbd6114cc1ead35777bab948e3643", // cDAI
    "39aa39c021dfbae8fac545936693ac917d5e7563", // cUSDC
    "86fadb80d8d2cff3c3680819e4da99c10232ba0f", // EBASE
    "57ab1ec28d129707052df4df418d58a2d46d5f51", // sUSD
    "9f8f72aa9304c8b593d555f12ef6589cc3a579a2", // MKR
    "c00e94cb662c3520282e6f5717214004a7f26888", // COMP
    "514910771af9ca656af840dff83e8264ecf986ca", // LINK
    "c011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f", // SNX
    "0bc529c00c6401aef6d220be8c6ea1667f6ad93e", // YFI
    "111111111117dc0aa78b770fa6a738034120c302", // 1INCH
    "df5e0e81dff6faf3a7e52ba697820c5e32d806a8", // yCurv
    "956f47f50a910163d8bf957cf5846d573e7f87ca", // FEI
    "7d1afa7b718fb893db30a3abc0cfc608aacfebb0", // MATIC
    "7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9", // AAVE
    "fe2e637202056d30016725477c5da089ab0a043a", // sETH2
];

pub fn sqrt_price_x96_to_token_prices(
    sqrt_price: &BigDecimal,
    token_0: &Erc20Token,
    token_1: &Erc20Token,
) -> (BigDecimal, BigDecimal) {
    log::debug!(
        "Computing prices for {} {} and {} {}",
        token_0.symbol,
        token_0.decimals,
        token_1.symbol,
        token_1.decimals
    );

    let price: BigDecimal = sqrt_price.mul(sqrt_price);
    let token0_decimals: BigInt = BigInt::from(token_0.decimals);
    let token1_decimals: BigInt = BigInt::from(token_1.decimals);
    let denominator: BigDecimal =
        BigDecimal::from_str("6277101735386680763835789423207666416102355444464034512896").unwrap();

    let price1 = price
        .div(denominator)
        .mul(math::exponent_to_big_decimal(&token0_decimals))
        .div(math::exponent_to_big_decimal(&token1_decimals));

    log::info!("price1: {}", price1);
    let price0 = math::safe_div(&BigDecimal::one(), &price1);

    return (price0, price1);
}

pub fn find_eth_per_token(
    log_ordinal: u64,
    pool_address: &String,
    token_address: &String,
    total_native_value_locked_store: &StoreGet,
    prices_store: &StoreGet,
) -> BigDecimal {
    log::debug!("finding ETH per token for {}", token_address);
    if token_address.eq(WETH_ADDRESS) {
        return BigDecimal::one();
    }

    let direct_to_eth_price= match  helper::get_price(prices_store, &WETH_ADDRESS.to_string(), token_address){
            Err(_) => BigDecimal::zero(),
            Ok(price) => price
        };

    if direct_to_eth_price.ne(&BigDecimal::zero().with_prec(100)) {
        return direct_to_eth_price;
    }

    let minimum_eth_locked = BigDecimal::from_str("60").unwrap();

    // loop all whitelist for a matching token
    for major_token in WHITELIST_TOKENS {
        log::info!(
            "checking for major_token: {} and pool address: {}",
            major_token,
            pool_address
        );

        let major_to_eth_price = match helper::get_price_at(prices_store, log_ordinal, &major_token.to_string(), &WETH_ADDRESS.to_string()){
            Err(_) => continue,
            Ok(price) => price
        };


        let tiny_to_major_price = match helper::get_price_at(prices_store, log_ordinal, token_address, &major_token.to_string()){
            Err(_) => continue,
            Ok(price) => price
        };

        let major_reserve = helper::get_pool_total_value_locked_token_or_zero(
            total_native_value_locked_store,
            pool_address,
            token_address
        );

        let eth_reserve_in_major_pair = major_to_eth_price.borrow().mul(major_reserve);
        if eth_reserve_in_major_pair.le(&minimum_eth_locked) {
            continue;
        }

        return BigDecimal::one().div(tiny_to_major_price.mul(major_to_eth_price));
    }

    return BigDecimal::zero().with_prec(100);
}


pub fn get_eth_price_in_usd(
    prices_store: &StoreGet,
) -> BigDecimal {
    match helper::get_pool_price(prices_store, &USDC_WETH_03_POOL.to_string(), &USDC_ADDRESS.to_string()) {
        Err(_) => BigDecimal::zero(),
        Ok(price) => price
    }
}
