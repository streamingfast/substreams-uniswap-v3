use std::borrow::Borrow;
use std::ops::{Add, Div, Mul, Neg};
use std::str;
use std::str::FromStr;
use num_bigint::BigInt;
use bigdecimal::{BigDecimal, Num, One, Zero};
use prost::DecodeError;
use substreams::{proto};
use crate::{pb, Pool, UniswapToken};
use substreams::store::StoreGet;
use substreams::log;

const _DAI_USD_KEY: &str = "8ad599c3a0ff1de082011efddc58f1908eb6e6d8";
const _USDC_ADDRESS: &str = "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
const _USDC_WETH_03_POOL: &str = "8ad599c3a0ff1de082011efddc58f1908eb6e6d8";
const WETH_ADDRESS: &str = "c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2";

pub const UNISWAP_V3_FACTORY: &str = "1f98431c8ad98523631ae4a59f267346ea31f984";

pub const _STABLE_COINS: [&str; 6] = [
    "6b175474e89094c44da98b954eedeac495271d0f",
    "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    "dac17f958d2ee523a2206206994597c13d831ec7",
    "0000000000085d4780b73119b644ae5ecd22b376",
    "956f47f50a910163d8bf957cf5846d573e7f87ca",
    "4dd28568d05f09b02220b09c2cb307bfd837cb95",
];

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

// hard-coded tokens which have various behaviours but for which a UniswapV3 valid pool
// exists, some are tokens which were migrated to a new address, etc.
pub fn get_static_uniswap_tokens(token_address: &str) -> Option<UniswapToken> {
    return match token_address {
        "e0b7927c4af23765cb51314a0e0521a9645f0e2a" => Some(UniswapToken{ // add DGD
            address: "e0b7927c4af23765cb51314a0e0521a9645f0e2a".to_string(),
            name: "DGD".to_string(),
            symbol: "DGD".to_string(),
            decimals: 9
        }),
        "7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9" => Some(UniswapToken{ // add AAVE
            address: "7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9".to_string(),
            name: "Aave Token".to_string(),
            symbol: "AAVE".to_string(),
            decimals: 18
        }),
        "eb9951021698b42e4399f9cbb6267aa35f82d59d" => Some(UniswapToken{ // add LIF
            address: "eb9951021698b42e4399f9cbb6267aa35f82d59d".to_string(),
            name: "LIF".to_string(),
            symbol: "LIF".to_string(),
            decimals: 18
        }),
        "bdeb4b83251fb146687fa19d1c660f99411eefe3" => Some(UniswapToken{ // add SVD
            address: "bdeb4b83251fb146687fa19d1c660f99411eefe3".to_string(),
            name: "savedroid".to_string(),
            symbol: "SVD".to_string(),
            decimals: 18
        }),
        "bb9bc244d798123fde783fcc1c72d3bb8c189413" => Some(UniswapToken{ // add TheDAO
            address: "bb9bc244d798123fde783fcc1c72d3bb8c189413".to_string(),
            name: "TheDAO".to_string(),
            symbol: "TheDAO".to_string(),
            decimals: 16
        }),
        "38c6a68304cdefb9bec48bbfaaba5c5b47818bb2" => Some(UniswapToken{ // add HPB
            address: "38c6a68304cdefb9bec48bbfaaba5c5b47818bb2".to_string(),
            name: "HPBCoin".to_string(),
            symbol: "HPB".to_string(),
            decimals: 18
        }),
        _ => None,
    };
}

pub fn sqrt_price_x96_to_token_prices(
    sqrt_price: &BigDecimal,
    token_0: &UniswapToken,
    token_1: &UniswapToken
) -> (BigDecimal, BigDecimal) {
    log::debug!("Computing prices for {} {} and {} {}", token_0.symbol, token_0.decimals, token_1.symbol, token_1.decimals);

    let price: BigDecimal = sqrt_price.mul(sqrt_price);
    let token0_decimals: BigInt = BigInt::from(token_0.decimals);
    let token1_decimals: BigInt = BigInt::from(token_1.decimals);
    let denominator: BigDecimal = BigDecimal::from_str("6277101735386680763835789423207666416102355444464034512896").unwrap();

    let price1 = price
        .div(denominator)
        .mul(exponent_to_big_decimal(&token0_decimals))
        .div(exponent_to_big_decimal(&token1_decimals));

    log::info!("price1: {}", price1);
    let price0 = safe_div(&BigDecimal::one(), &price1);

    return (price0, price1);
}

pub fn find_eth_per_token(
    log_ordinal: u64,
    pool_address: &str,
    token_address: &str,
    liquidity_store: &StoreGet,
    prices_store: &StoreGet,
) -> BigDecimal {
    log::debug!("Finding ETH per token for {}", token_address);

    if token_address.eq(WETH_ADDRESS) {
        return BigDecimal::one();
    }

    let bd_zero = BigDecimal::zero();

    let direct_to_eth_price = match prices_store.get_last(
        &format!("price:{}:{}", WETH_ADDRESS, token_address)
    ) {
        None => bd_zero, // maybe do the check the other way around
        Some(price_bytes) => decode_price_bytes_to_big_decimal(&price_bytes)
    };

    if direct_to_eth_price.ne(&zero_big_decimal()) {
        return direct_to_eth_price;
    }

    let minimum_eth_locked = BigDecimal::from_str("60").unwrap();

    // loop all whitelist for a matching token
    for major_token in WHITELIST_TOKENS {
        let major_to_eth_price = match prices_store.get_at(
            log_ordinal,
            &format!("price:{}:{}", major_token, token_address)
        ) {
            None => continue,
            Some(price_bytes) => {
                log::info!("major_to_eth_price: {}", decode_price_bytes_to_big_decimal(&price_bytes));
                decode_price_bytes_to_big_decimal(&price_bytes)
            },
        };

        let tiny_to_major_price = match prices_store.get_at(
            log_ordinal,
            &format!("price:{}:{}", token_address, major_token)
        ) {
            None => continue,
            Some(price_bytes) => {
                log::info!("tiny_to_major_price: {}", decode_price_bytes_to_big_decimal(&price_bytes));
                decode_price_bytes_to_big_decimal(&price_bytes)
            },
        };

        let major_reserve = get_last_total_value_locked_or_zero(liquidity_store, pool_address, token_address);

        let eth_reserve_in_major_pair = major_to_eth_price.borrow().mul(major_reserve);
        if eth_reserve_in_major_pair.le(&minimum_eth_locked) {
            continue;
        }

        log::info!("tiny to major price: {}, major to eth price: {}", tiny_to_major_price, major_to_eth_price);
        return tiny_to_major_price.mul(major_to_eth_price);
    }

    return zero_big_decimal();
}

pub fn safe_div(amount0: &BigDecimal, amount1: &BigDecimal) -> BigDecimal {
    let big_decimal_zero: &BigDecimal = &BigDecimal::zero();
    return if amount1.eq(big_decimal_zero) {
        BigDecimal::zero()
    } else {
        amount0.div(amount1)
    }
}

pub fn big_decimal_exponated(amount: BigDecimal, exponent: BigInt) -> BigDecimal {
    if exponent.is_zero() {
        return BigDecimal::one().with_prec(100);
    }
    if exponent.is_one() {
        return amount;
    }
    if exponent.lt(&BigInt::zero()) {
        return safe_div(&BigDecimal::one().with_prec(100), &big_decimal_exponated(amount, exponent.neg()));
    }

    let mut result = amount.clone();
    let big_int_one: &BigInt = &BigInt::one();

    let mut i = BigInt::zero();
    while i.lt(exponent.borrow()) {

        result = result.mul(amount.clone()).with_prec(100);
        i = i.add(big_int_one);
    }

    return result
}

pub fn get_last_pool(pools_store: &StoreGet, pool_address: &str) -> Result<Pool, DecodeError> {
    proto::decode(&pools_store.get_last(&format!("pool:{}", pool_address)).unwrap())
}

pub fn get_last_pool_tick(pool_init_store: &StoreGet, swap_store: &StoreGet, pool_address: &str) -> Result<BigDecimal, DecodeError> {
    return match get_last_swap(swap_store, pool_address) {
        Ok(swap) => {
            Ok(BigDecimal::from_str_radix(swap.tick.to_string().as_str(), 10).unwrap())
        }
        Err(_) => {
            //fallback to pool init
            match get_pool_init(pool_init_store, pool_address) {
                Ok(pool_init) => {
                    Ok(BigDecimal::from_str_radix(&pool_init.tick, 10).unwrap())
                }
                Err(_) => {
                    Err(DecodeError::new(format!("No pool init or swap: {}", pool_address)))
                }
            }
        }
    }
}

pub fn generate_tokens_key(token0: &str, token1: &str) -> String {
    if token0 > token1 {
        return format!("{}:{}", token1, token0);
    }
    return format!("{}:{}", token0, token1);
}

fn get_last_total_value_locked_or_zero(liquidity_store: &StoreGet, pool_address: &str, token_address: &str) -> BigDecimal {
    return match &liquidity_store.get_last(&format!("total_value_locked:{}:{}", pool_address, token_address)) {
        None => {
            BigDecimal::zero().with_prec(100)
        }
        Some(tvl_bytes) => {
            BigDecimal::parse_bytes(tvl_bytes.as_slice(), 10).unwrap().with_prec(100)
        }
    }
}

fn exponent_to_big_decimal(decimals: &BigInt) -> BigDecimal {
    let mut result = BigDecimal::one();
    let big_decimal_ten: &BigDecimal = &BigDecimal::from(10);
    let big_int_one: &BigInt = &BigInt::one();

    let mut i = BigInt::zero();
    while i.lt(decimals) {
        result = result.mul(big_decimal_ten);
        i = i.add(big_int_one);
    }

    return result
}

fn get_last_swap(swap_store: &StoreGet, pool_address: &str) -> Result<pb::uniswap::Swap, DecodeError> {
    return match &swap_store.get_last(&format!("swap:{}", pool_address)) {
        None => {
            Err(DecodeError::new("No swap found"))
        }
        Some(swap_bytes) => {
            Ok(proto::decode(swap_bytes).unwrap())
        }
    }
}

fn get_pool_init(pool_init_store: &StoreGet, pool_address: &str) -> Result<pb::uniswap::PoolInitialization, DecodeError> {
    return match &pool_init_store.get_last(&format!("pool_init:{}", pool_address)) {
        None => {
            Err(DecodeError::new("No pool init found"))
        }
        Some(pool_init_bytes) => {
            Ok(proto::decode(pool_init_bytes).unwrap())
        }
    }
}

fn decode_price_bytes_to_big_decimal(price_bytes: &Vec<u8>) -> BigDecimal {
    let price_from_store_decoded = str::from_utf8(price_bytes.as_slice()).unwrap();
    return BigDecimal::from_str(price_from_store_decoded)
        .unwrap()
        .with_prec(100);
}

fn zero_big_decimal() -> BigDecimal {
    BigDecimal::zero().with_prec(100)
}
