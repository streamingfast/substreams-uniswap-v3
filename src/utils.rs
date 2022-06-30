use std::borrow::Borrow;
use std::ops::{Add, Div, Mul};
use std::str::FromStr;
use num_bigint::BigInt;
use bigdecimal::{BigDecimal, FromPrimitive, Num, One, Zero};
use substreams::{proto, store};
use crate::{Pool, UniswapToken};
use substreams::store::StoreGet;
use crate::pb::tokens::Token;

const DAI_USD_KEY: &str = "8ad599c3a0ff1de082011efddc58f1908eb6e6d8";
const USDC_ADDRESS: &str = "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
const WETH_ADDRESS: &str = "c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2";
const USDC_WETH_03_POOL: &str = "8ad599c3a0ff1de082011efddc58f1908eb6e6d8";

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

pub const BLACKLISTED_POOLS: [&str; 1] = [
    "1c74dde716d3f2a1df5097b7c2543c5d97cfa4d3" // WBTC-UNI-V3-POS
];

pub fn compute_prices(
    sqrt_price: &BigDecimal,
    token_0: &UniswapToken,
    token_1: &UniswapToken
) -> (BigDecimal, BigDecimal) {
    let price: BigDecimal = BigDecimal::from(sqrt_price * sqrt_price);

    let token0_decimals: BigInt = BigInt::from(token_0.decimals);
    let token1_decimals: BigInt = BigInt::from(token_1.decimals);
    let q192: BigDecimal = BigDecimal::from((2 ^ 192) as u64);

    let price1 = price
        .div(q192)
        .mul(exponent_to_big_decimal(&token0_decimals))
        .div(exponent_to_big_decimal(&token1_decimals));
    let price0 = safe_div(BigDecimal::one(), price1.clone());

    return (price0, price1);
}

// todo(colin): do we even need this function?
// // can we do this differently and take the eth price from another place?
// pub fn get_eth_price_in_usd(token_store: StoreGet) -> BigDecimal {
//     return match token_store.get_last(&format!("token:{}", USDC_ADDRESS)) {
//         None => {
//             BigDecimal::zero()
//         }
//         Some(token_bytes) => {
//             let token: Token = proto::decode(&token_bytes).unwrap();
//             // let token0_price, _ = compute_prices();
//
//             BigDecimal::zero()
//         }
//     }
// }

pub fn find_eth_per_token(
    log_ordinal: u64,
    token_address: &str,
    pools_store: &StoreGet,
    tokens_store: &StoreGet,
    whitelist_pools_store: &StoreGet,
    liquidity_store: &StoreGet,
) -> BigDecimal {
    if token_address.eq(WETH_ADDRESS) {
        return BigDecimal::one();
    }

    let bd_one = BigDecimal::one().with_prec(100);
    let bi_zero= BigInt::zero();
    let mut largest_liquidity_eth = BigDecimal::zero().with_prec(100);
    let mut price_so_far = BigDecimal::zero().with_prec(100);
    let mut whitelist_pools: Vec<&str> = vec![];
    let mut whitelist_pools_proto: String = String::default();

    let eth_price_usd = BigDecimal::one(); // todo

    match whitelist_pools_store.get_last(&format!("token:{}", token_address)) {
        None => {
            // do nothing
        }
        Some(whitelist_pools_bytes) => {
            whitelist_pools_proto = proto::decode(&whitelist_pools_bytes).unwrap();
        }
    }

    for p in whitelist_pools_proto.split(";") {
        whitelist_pools.push(p);
    }

    if STABLE_COINS.contains(&token_address) {
        price_so_far = safe_div(bd_one, eth_price_usd);
    } else {
        for pool_address in whitelist_pools.iter() {
            let pool = get_last_pool(&pools_store, pool_address);

            let liquidity : BigDecimal = match liquidity_store.get_last(&format!("pool:{}:liquidity", pool_address)) {
                None => {
                    BigDecimal::zero().with_prec(100)
                }
                Some(liquidity_bytes) => {
                    BigDecimal::parse_bytes(liquidity_bytes.as_slice(), 10).unwrap()
                }
            };

            if liquidity.gt(&BigDecimal::zero().with_prec(100)) {
                let token0 : UniswapToken = get_last_token(tokens_store, &pool.token0_address);
                let token1 : UniswapToken = get_last_token(tokens_store, &pool.token1_address);
                let prices = compute_prices(
                    &BigDecimal::from_str_radix(&pool.sqrt_price, 10).unwrap(),
                    &token0,
                    &token1,
                );

                if pool.token0_address == token_address {
                    let token_1_derived_eth = find_eth_per_token(
                        log_ordinal,
                        &pool.token0_address,
                        &pools_store,
                        &tokens_store,
                        &whitelist_pools_store,
                        &liquidity_store,
                    );
                    let eth_locked = match liquidity_store.get_last(&format!("pool:{}:token:{}:total_value_locked", pool_address, pool.token0_address)) {
                        None => {
                            BigDecimal::zero().with_prec(100)
                        }
                        Some(tvl_bytes) => {
                            BigDecimal::parse_bytes(tvl_bytes.as_slice(), 10).unwrap()
                        }
                    }.mul(&token_1_derived_eth);
                    if eth_locked.gt(&largest_liquidity_eth) && eth_locked.gt( &BigDecimal::from(60 as i64)) {
                        largest_liquidity_eth = eth_locked;
                        price_so_far = prices.0.mul(token_1_derived_eth);
                    }
                }
                if pool.token1_address == token_address {
                    let token0_derived_eth = find_eth_per_token(
                        log_ordinal,
                        &pool.token1_address,
                        &pools_store,
                        &tokens_store,
                        &whitelist_pools_store,
                        &liquidity_store,
                    );
                    let eth_locked = match liquidity_store.get_last(&format!("pool:{}:token:{}:total_value_locked", pool_address, pool.token1_address)) {
                        None => {
                            BigDecimal::zero().with_prec(100)
                        }
                        Some(tvl_bytes) => {
                            BigDecimal::parse_bytes(tvl_bytes.as_slice(), 10).unwrap()
                        }
                    }.mul(&token0_derived_eth);
                    if eth_locked.gt(&largest_liquidity_eth) && eth_locked.gt(&BigDecimal::from(60 as i64)) {
                        largest_liquidity_eth = eth_locked;
                        price_so_far = prices.1.mul(token0_derived_eth);
                    }
                }
            }
        }
    }

    return price_so_far;
}

pub fn safe_div(amount0: BigDecimal, amount1: BigDecimal) -> BigDecimal {
    let big_decimal_zero_ptr: &BigDecimal = &BigDecimal::zero().with_prec(100);
    return if amount1.eq(big_decimal_zero_ptr) {
        BigDecimal::from(0 as u64)
    } else {
        amount0.div(amount1)
    }
}

pub fn exponent_to_big_decimal(decimals: &BigInt) -> BigDecimal {
    let mut result = BigDecimal::one();
    let big_decimal_ten: &BigDecimal = &BigDecimal::from(10 as u64);
    let big_int_zero: &BigInt = &BigInt::zero();

    let mut i = BigInt::zero();
    while i.lt(decimals.borrow()) {
        result = result.mul(big_decimal_ten);
        i = i.add(big_int_zero);
    }

    return result
}

pub fn get_last_token(tokens_store: &StoreGet, token_address: &str) -> UniswapToken {
    proto::decode(&tokens_store.get_last(&format!("token:{}", token_address)).unwrap()).unwrap()
}

pub fn get_last_pool(pools_store: &StoreGet, pool_address: &str) -> Pool {
    proto::decode(&pools_store.get_last(&format!("pool:{}", pool_address)).unwrap()).unwrap()
}