use std::borrow::Borrow;
use std::ops::{Add, Div, Mul, Neg};
use std::str;
use std::str::FromStr;
use num_bigint::BigInt;
use bigdecimal::{BigDecimal, Num, One, Zero};
use prost::DecodeError;
use substreams::{proto};
use crate::{pb, Pool, SqrtPriceUpdate, SqrtPriceUpdates, UniswapToken};
use substreams::store::StoreGet;
use substreams::log;

const _DAI_USD_KEY: &str = "8ad599c3a0ff1de082011efddc58f1908eb6e6d8";
const _USDC_ADDRESS: &str = "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
const WETH_ADDRESS: &str = "c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2";
const USDC_WETH_03_POOL: &str = "8ad599c3a0ff1de082011efddc58f1908eb6e6d8";

pub const UNISWAP_V3_FACTORY: &str = "1f98431c8ad98523631ae4a59f267346ea31f984";

pub const STABLE_COINS: [&str; 6] = [
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

pub fn get_eth_price_in_usd(sqrt_price_store: &StoreGet, pools_store: &StoreGet, tokens_store: &StoreGet) -> BigDecimal {
    return match pools_store.get_last(&format!("pool:{}", USDC_WETH_03_POOL)) {
        None => {
            BigDecimal::zero()
        }
        Some(pool_bytes) => {
            let pool: Pool = proto::decode(&pool_bytes).unwrap();

            let token_0: UniswapToken = match tokens_store.get_last(&pool.token0_address) {
                None => {
                    return BigDecimal::zero();
                }
                Some(token_bytes) => {
                    proto::decode(&token_bytes).unwrap()
                }
            };

            let token_1: UniswapToken = match tokens_store.get_last(&pool.token1_address) {
                None => {
                    return BigDecimal::zero();
                }
                Some(token_bytes) => {
                    proto::decode(&token_bytes).unwrap()
                }
            };

            let sqrt_price = get_last_sqrt_price(sqrt_price_store, USDC_WETH_03_POOL).unwrap();
            sqrt_price_x96_to_token_prices(&sqrt_price, &token_0, &token_1).0 // token 0 is USDC
        }
    }
}

pub fn find_eth_per_token(
    log_ordinal: u64,
    token_address: &str,
    prices_store: &StoreGet,
) -> BigDecimal {
    log::debug!("Finding ETH per token for {}", token_address);

    if token_address.eq(WETH_ADDRESS) {
        return BigDecimal::one();
    }

    let bd_zero= BigDecimal::zero();

    let direct_to_eth_price = match prices_store.get_last(
        &format!("price:{}:{}", WETH_ADDRESS, token_address)
    ) {
        None => bd_zero, // maybe do the check the other way around
        Some(price_bytes) => decode_price_bytes_to_big_decimal(&price_bytes)
    };

    if direct_to_eth_price.ne(&zero_big_decimal()) {
        return direct_to_eth_price;
    }

    // loop all whitelist for a matching pool
    for major_token in WHITELIST_TOKENS {
        // not sure about the double match here
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

        // not sure about the double match here
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

pub fn exponent_to_big_decimal(decimals: &BigInt) -> BigDecimal {
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

pub fn get_last_token(tokens_store: &StoreGet, token_address: &str) -> Result<UniswapToken, DecodeError> {
    proto::decode(&tokens_store.get_last(&format!("token:{}", token_address)).unwrap())
}

pub fn get_last_pool(pools_store: &StoreGet, pool_address: &str) -> Result<Pool, DecodeError> {
    proto::decode(&pools_store.get_last(&format!("pool:{}", pool_address)).unwrap())
}

fn get_last_sqrt_price_update(sqrt_price_store: &StoreGet, pool_address: &str) -> Result<SqrtPriceUpdate, DecodeError> {
    proto::decode(&sqrt_price_store.get_last(&format!("sqrt_price:{}", pool_address)).unwrap())
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

pub fn get_last_liquidity_or_zero(liquidity_store: &StoreGet, pool_address: &str) -> BigDecimal {
    return match &liquidity_store.get_last(&format!("pool:{}:liquidity", pool_address)) {
        None => {
            BigDecimal::zero().with_prec(100)
        }
        Some(liquidity_bytes) => {
            BigDecimal::parse_bytes(liquidity_bytes.as_slice(), 10).unwrap().with_prec(100)
        }
    }
}

pub fn get_last_total_value_locked_or_zero(liquidity_store: &StoreGet, pool_address: &str, token_address: &str) -> BigDecimal {
    return match &liquidity_store.get_last(&format!("pool:{}:token:{}:total_value_locked", pool_address, token_address)) {
        None => {
            BigDecimal::zero().with_prec(100)
        }
        Some(tvl_bytes) => {
            BigDecimal::parse_bytes(tvl_bytes.as_slice(), 10).unwrap().with_prec(100)
        }
    }
}

pub fn get_pool_init(pool_init_store: &StoreGet, pool_address: &str) -> Result<pb::uniswap::PoolInitialization, DecodeError> {
    return match &pool_init_store.get_last(&format!("pool_init:{}", pool_address)) {
        None => {
            Err(DecodeError::new("No pool init found"))
        }
        Some(pool_init_bytes) => {
            Ok(proto::decode(pool_init_bytes).unwrap())
        }
    }
}

pub fn get_last_sqrt_price(sqrt_price_store: &StoreGet, pool_address: &str) -> Result<BigDecimal, DecodeError> {
    return match get_last_sqrt_price_update(sqrt_price_store, pool_address) {
        Ok(sqrt_price_update) => {
            Ok(BigDecimal::from_str(sqrt_price_update.sqrt_price.as_str()).unwrap())
        }
        Err(_) => {
            Err(DecodeError::new(format!("no pool init or swap for the pool: {}", pool_address)))
        }
    }
}

pub fn get_last_pool_sqrt_price(pool_init_store: &StoreGet, swap_store: &StoreGet, pool_address: &str) -> Result<BigDecimal, DecodeError> {
    return match get_last_swap(swap_store, pool_address) {
        Ok(swap) => {
            Ok(BigDecimal::from_str_radix(&swap.sqrt_price, 10).unwrap())
        }
        Err(_) => {
            //fallback to pool init
            println!("No swap found, falling back to pool init");
            match get_pool_init(pool_init_store, pool_address) {
                Ok(pool_init) => {
                    Ok(BigDecimal::from_str_radix(&pool_init.sqrt_price, 10).unwrap())
                }
                Err(_) => {
                    Err(DecodeError::new("No pool init or swap"))
                }
            }
        }
    }
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

fn decode_price_bytes_to_big_decimal(price_bytes: &Vec<u8>) -> BigDecimal {
    let price_from_store_decoded = str::from_utf8(price_bytes.as_slice()).unwrap();
    return BigDecimal::from_str(price_from_store_decoded)
        .unwrap()
        .with_prec(100);
}

fn zero_big_decimal() -> BigDecimal {
    BigDecimal::zero().with_prec(100)
}
