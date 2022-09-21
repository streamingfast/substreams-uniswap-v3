use crate::ethpb::v2::TransactionTrace;
use crate::pb::PositionEvent;
use crate::uniswap::position::PositionType;
use crate::uniswap::Transaction;
use crate::{
    keyer, math, rpc, Erc20Token, IncreaseLiquidity, Pool, PoolLiquidity, Position, StorageChange,
    ToPrimitive, WHITELIST_TOKENS,
};
use bigdecimal::{BigDecimal, Zero};
use ethabi::Uint;
use num_bigint::BigInt;
use std::ops::{Add, Mul};
use std::str;
use std::str::FromStr;
use substreams::store::StoreGet;
use substreams::{hex, log, proto, Hex};
use substreams_ethereum::pb::eth;
use substreams_ethereum::pb::eth::v2::Block;

// const _DAI_USD_KEY: &str = "8ad599c3a0ff1de082011efddc58f1908eb6e6d8";
// const _USDC_ADDRESS: &str = "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
// const _USDC_WETH_03_POOL: &str = "8ad599c3a0ff1de082011efddc58f1908eb6e6d8";

pub const UNISWAP_V3_FACTORY: [u8; 20] = hex!("1f98431c8ad98523631ae4a59f267346ea31f984");
pub const ZERO_ADDRESS: [u8; 20] = hex!("0000000000000000000000000000000000000000");
pub const NON_FUNGIBLE_POSITION_MANAGER: [u8; 20] =
    hex!("c36442b4a4522e871399cd717abdd847ab11fe88");

pub const _STABLE_COINS: [&str; 6] = [
    "6b175474e89094c44da98b954eedeac495271d0f",
    "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    "dac17f958d2ee523a2206206994597c13d831ec7",
    "0000000000085d4780b73119b644ae5ecd22b376",
    "956f47f50a910163d8bf957cf5846d573e7f87ca",
    "4dd28568d05f09b02220b09c2cb307bfd837cb95",
];

// hard-coded tokens which have various behaviours but for which a UniswapV3 valid pool
// exists, some are tokens which were migrated to a new address, etc.
pub fn get_static_uniswap_tokens(token_address: &str) -> Option<Erc20Token> {
    return match token_address {
        "e0b7927c4af23765cb51314a0e0521a9645f0e2a" => Some(Erc20Token {
            // add DGD
            address: "e0b7927c4af23765cb51314a0e0521a9645f0e2a".to_string(),
            name: "DGD".to_string(),
            symbol: "DGD".to_string(),
            decimals: 9,
            total_supply: "".to_string(), // subgraph doesn't check the total supply
            whitelist_pools: vec![],
        }),
        "7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9" => Some(Erc20Token {
            // add AAVE
            address: "7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9".to_string(),
            name: "Aave Token".to_string(),
            symbol: "AAVE".to_string(),
            decimals: 18,
            total_supply: "".to_string(), // subgraph doesn't check the total supply
            whitelist_pools: vec![],
        }),
        "eb9951021698b42e4399f9cbb6267aa35f82d59d" => Some(Erc20Token {
            // add LIF
            address: "eb9951021698b42e4399f9cbb6267aa35f82d59d".to_string(),
            name: "LIF".to_string(),
            symbol: "LIF".to_string(),
            decimals: 18,
            total_supply: "".to_string(), // subgraph doesn't check the total supply
            whitelist_pools: vec![],
        }),
        "bdeb4b83251fb146687fa19d1c660f99411eefe3" => Some(Erc20Token {
            // add SVD
            address: "bdeb4b83251fb146687fa19d1c660f99411eefe3".to_string(),
            name: "savedroid".to_string(),
            symbol: "SVD".to_string(),
            decimals: 18,
            total_supply: "".to_string(), // subgraph doesn't check the total supply
            whitelist_pools: vec![],
        }),
        "bb9bc244d798123fde783fcc1c72d3bb8c189413" => Some(Erc20Token {
            // add TheDAO
            address: "bb9bc244d798123fde783fcc1c72d3bb8c189413".to_string(),
            name: "TheDAO".to_string(),
            symbol: "TheDAO".to_string(),
            decimals: 16,
            total_supply: "".to_string(), // subgraph doesn't check the total supply
            whitelist_pools: vec![],
        }),
        "38c6a68304cdefb9bec48bbfaaba5c5b47818bb2" => Some(Erc20Token {
            // add HPB
            address: "38c6a68304cdefb9bec48bbfaaba5c5b47818bb2".to_string(),
            name: "HPBCoin".to_string(),
            symbol: "HPB".to_string(),
            decimals: 18,
            total_supply: "".to_string(), // subgraph doesn't check the total supply
            whitelist_pools: vec![],
        }),
        _ => None,
    };
}

pub fn convert_token_to_decimal(amount: &BigInt, decimals: u64) -> BigDecimal {
    let big_float_amount = BigDecimal::from_str(amount.to_string().as_str())
        .unwrap()
        .with_prec(100);

    return math::divide_by_decimals(big_float_amount, decimals);
}

pub fn should_handle_swap(pool: &Pool) -> bool {
    if pool.ignore_pool {
        return false;
    }
    return &pool.address != "9663f2ca0454accad3e094448ea6f77443880454";
}

pub fn should_handle_mint_and_burn(pool: &Pool) -> bool {
    if pool.ignore_pool {
        return false;
    }
    return true;
}

pub fn extract_pool_liquidity(
    log_ordinal: u64,
    pool_address: &Vec<u8>,
    storage_changes: &Vec<StorageChange>,
) -> Option<PoolLiquidity> {
    for sc in storage_changes {
        if pool_address.eq(&sc.address) {
            if sc.key[sc.key.len() - 1] == 4 {
                return Some(PoolLiquidity {
                    pool_address: Hex(&pool_address).to_string(),
                    liquidity: math::decimal_from_hex_be_bytes(&sc.new_value).to_string(),
                    log_ordinal,
                });
            }
        }
    }
    None
}

pub fn log_token(token: &Erc20Token, index: u64) {
    log::info!(
        "token {} addr: {}, name: {}, symbol: {}, decimals: {}",
        index,
        token.address,
        token.decimals,
        token.symbol,
        token.name
    );
}

pub fn calculate_amount_usd(
    amount0: &BigDecimal,
    amount1: &BigDecimal,
    token0_derived_eth_price: &BigDecimal,
    token1_derived_eth_price: &BigDecimal,
    bundle_eth_price: &BigDecimal,
) -> BigDecimal {
    return amount0
        .mul(token0_derived_eth_price.mul(bundle_eth_price.clone()))
        .add(amount1.mul(token1_derived_eth_price.mul(bundle_eth_price)));
}

pub fn decode_bytes_to_big_decimal(bytes: Vec<u8>) -> BigDecimal {
    if bytes.len() == 0 {
        return BigDecimal::zero();
    }
    let bytes_as_str = str::from_utf8(bytes.as_slice()).unwrap();
    return BigDecimal::from_str(bytes_as_str).unwrap().with_prec(100);
}

pub fn decode_bytes_to_big_int(bytes: Vec<u8>) -> BigInt {
    if bytes.len() == 0 {
        return BigInt::zero();
    }
    let bytes_as_str = str::from_utf8(bytes.as_slice()).unwrap();
    return BigInt::from_str(bytes_as_str).unwrap();
}

pub fn get_tracked_amount_usd(
    token0_id: &String,
    token1_id: &String,
    token0_derived_eth_price: &BigDecimal,
    token1_derived_eth_price: &BigDecimal,
    amount0_abs: &BigDecimal,
    amount1_abs: &BigDecimal,
    eth_price_in_usd: &BigDecimal,
) -> BigDecimal {
    let price0_usd = token0_derived_eth_price.mul(eth_price_in_usd.clone());
    let price1_usd = token1_derived_eth_price.mul(eth_price_in_usd);

    // both are whitelist tokens, return sum of both amounts
    if WHITELIST_TOKENS.contains(&token0_id.as_str())
        && WHITELIST_TOKENS.contains(&token0_id.as_str())
    {
        return amount0_abs.mul(price0_usd).add(amount1_abs.mul(price1_usd));
    }

    // take double value of the whitelisted token amount
    if WHITELIST_TOKENS.contains(&token0_id.as_str())
        && !WHITELIST_TOKENS.contains(&token1_id.as_str())
    {
        return amount0_abs.mul(price0_usd).mul(BigDecimal::from(2 as i32));
    }

    // take double value of the whitelisted token amount
    if !WHITELIST_TOKENS.contains(&token0_id.as_str())
        && WHITELIST_TOKENS.contains(&token1_id.as_str())
    {
        return amount1_abs.mul(price1_usd).mul(BigDecimal::from(2 as i32));
    }

    // neither token is on white list, tracked amount is 0
    return BigDecimal::from(0 as i32);
}

pub fn load_transaction(
    block_number: u64,
    timestamp: u64,
    log_ordinal: u64,
    transaction: &TransactionTrace,
) -> Transaction {
    return Transaction {
        id: Hex(&transaction.hash).to_string(),
        block_number,
        timestamp,
        gas_used: transaction.gas_used,
        gas_price: BigInt::from_signed_bytes_be(
            transaction.clone().gas_price.unwrap().bytes.as_slice(),
        )
        .to_string(),
        log_ordinal,
    };
}

pub fn get_position(
    store_pool: &StoreGet,
    log_address: &String,
    transaction_hash: &Vec<u8>,
    position_type: PositionType,
    log_ordinal: u64,
    timestamp: u64,
    block_number: u64,
    event: PositionEvent,
) -> Option<Position> {
    if let Some(positions_call_result) = rpc::positions_call(log_address, event.get_token_id()) {
        let token0_bytes = positions_call_result.0;
        let token1_bytes = positions_call_result.1;
        let fee = positions_call_result.2;
        let tick_lower = positions_call_result.3;
        let tick_upper = positions_call_result.4;
        let fee_growth_inside_0_last_x128 = positions_call_result.5;
        let fee_growth_inside_1_last_x128 = positions_call_result.6;

        let token0: String = Hex(&token0_bytes.as_slice()).to_string();
        let token1: String = Hex(&token1_bytes.as_slice()).to_string();
        let mut pool: Pool = Pool {
            ..Default::default()
        };

        match store_pool.get_last(keyer::pool_token_index_key(
            &token0,
            &token1,
            &fee.to_string(),
        )) {
            None => {
                log::info!(
                    "pool does not exist for token0 {} and token1 {}",
                    token0,
                    token1
                );
                return None;
            }
            Some(pool_bytes) => pool = proto::decode(&pool_bytes).unwrap(),
        }

        let amount0 = convert_token_to_decimal(&event.get_amount0(), pool.token0.unwrap().decimals);
        let amount1 = convert_token_to_decimal(&event.get_amount1(), pool.token1.unwrap().decimals);

        return Some(Position {
            id: event.get_token_id().to_string(),
            owner: Hex(ZERO_ADDRESS).to_string(),
            pool: pool.address.clone(),
            token0,
            token1,
            tick_lower: format!("{}#{}", pool.address, tick_lower.to_i32().unwrap()),
            tick_upper: format!("{}#{}", pool.address, tick_upper.to_i32().unwrap()),
            transaction: Hex(&transaction_hash).to_string(),
            fee_growth_inside_0_last_x_128: fee_growth_inside_0_last_x128.to_string(),
            fee_growth_inside_1_last_x_128: fee_growth_inside_1_last_x128.to_string(),
            liquidity: event.get_liquidity(),
            amount0: amount0.to_string(),
            amount1: amount1.to_string(),
            position_type: position_type as i32,
            log_ordinal,
            timestamp,
            block_number,
        });
    }
    return None;
}
