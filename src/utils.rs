use crate::ethpb::v2::TransactionTrace;
use crate::pb::uniswap::events;
use crate::pb::uniswap::events::PoolSqrtPrice;
use crate::pb::{AdjustedAmounts, PositionEvent};
use crate::tables::Tables;
use crate::uniswap::events::position::PositionType;
use crate::uniswap::events::Transaction;
use crate::{key, keyer, rpc, storage, Erc20Token, Pool, StorageChange, WHITELIST_TOKENS};
use std::fmt::Display;
use std::ops::{Add, Mul};
use std::string::ToString;
use substreams::prelude::{DeltaBigDecimal, DeltaProto, StoreGetBigDecimal};
use substreams::scalar::{BigDecimal, BigInt};
use substreams::store::{DeltaBigInt, StoreGet, StoreGetProto};
use substreams::{hex, log, Hex};

pub const UNISWAP_V3_FACTORY: [u8; 20] = hex!("1f98431c8ad98523631ae4a59f267346ea31f984");

pub const ZERO_ADDRESS: [u8; 20] = hex!("0000000000000000000000000000000000000000");
pub const NON_FUNGIBLE_POSITION_MANAGER: [u8; 20] = hex!("c36442b4a4522e871399cd717abdd847ab11fe88");

const DGD_TOKEN_ADDRESS: [u8; 20] = hex!("e0b7927c4af23765cb51314a0e0521a9645f0e2a");
const AAVE_TOKEN_ADDRESS: [u8; 20] = hex!("7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9");
const LIF_TOKEN_ADDRESS: [u8; 20] = hex!("eb9951021698b42e4399f9cbb6267aa35f82d59d");
const SVD_TOKEN_ADDRESS: [u8; 20] = hex!("bdeb4b83251fb146687fa19d1c660f99411eefe3");
const THEDAO_TOKEN_ADDRESS: [u8; 20] = hex!("bb9bc244d798123fde783fcc1c72d3bb8c189413");
const HPB_TOKEN_ADDRESS: [u8; 20] = hex!("38c6a68304cdefb9bec48bbfaaba5c5b47818bb2");

// hard-coded tokens which have various behaviours but for which a UniswapV3 valid pool
// exists, some are tokens which were migrated to new addresses
pub fn get_static_uniswap_tokens(token_address: &[u8]) -> Option<Erc20Token> {
    match token_address {
        x if x == DGD_TOKEN_ADDRESS => Some(Erc20Token {
            address: Hex(&DGD_TOKEN_ADDRESS).to_string(),
            name: "DGD".to_string(),
            symbol: "DGD".to_string(),
            decimals: 9,
            total_supply: "".to_string(), // subgraph doesn't check the total supply
            whitelist_pools: vec![],
        }),
        x if x == AAVE_TOKEN_ADDRESS => Some(Erc20Token {
            address: Hex(&AAVE_TOKEN_ADDRESS).to_string(),
            name: "Aave Token".to_string(),
            symbol: "AAVE".to_string(),
            decimals: 18,
            total_supply: "".to_string(), // subgraph doesn't check the total supply
            whitelist_pools: vec![],
        }),
        x if x == LIF_TOKEN_ADDRESS => Some(Erc20Token {
            address: Hex(&LIF_TOKEN_ADDRESS).to_string(),
            name: "LIF".to_string(),
            symbol: "LIF".to_string(),
            decimals: 18,
            total_supply: "".to_string(), // subgraph doesn't check the total supply
            whitelist_pools: vec![],
        }),
        x if x == SVD_TOKEN_ADDRESS => Some(Erc20Token {
            address: Hex(&SVD_TOKEN_ADDRESS).to_string(),
            name: "savedroid".to_string(),
            symbol: "SVD".to_string(),
            decimals: 18,
            total_supply: "".to_string(), // subgraph doesn't check the total supply
            whitelist_pools: vec![],
        }),
        x if x == THEDAO_TOKEN_ADDRESS => Some(Erc20Token {
            address: Hex(&THEDAO_TOKEN_ADDRESS).to_string(),
            name: "TheDAO".to_string(),
            symbol: "TheDAO".to_string(),
            decimals: 16,
            total_supply: "".to_string(), // subgraph doesn't check the total supply
            whitelist_pools: vec![],
        }),
        x if x == HPB_TOKEN_ADDRESS => Some(Erc20Token {
            address: Hex(&HPB_TOKEN_ADDRESS).to_string(),
            name: "HPBCoin".to_string(),
            symbol: "HPB".to_string(),
            decimals: 18,
            total_supply: "".to_string(), // subgraph doesn't check the total supply
            whitelist_pools: vec![],
        }),
        _ => None,
    }
}

pub fn extract_pool_fee_growth_global_updates(
    log_ordinal: u64,
    pool_address: &Vec<u8>,
    storage_changes: &Vec<StorageChange>,
) -> Vec<events::FeeGrowthGlobal> {
    let mut fee_growth_global = vec![];

    let fee_growth_global_0 = hex!("0000000000000000000000000000000000000000000000000000000000000001");
    let _fee_growth_global_1 = hex!("0000000000000000000000000000000000000000000000000000000000000002");

    let storage = storage::uniswap_v3_pool::UniswapPoolStorage::new(storage_changes, pool_address);

    if let Some((old_value, new_value)) = storage.fee_growth_global0x128() {
        fee_growth_global.push(events::FeeGrowthGlobal {
            pool_address: Hex(&pool_address).to_string(),
            ordinal: log_ordinal,
            token_idx: 0,
            new_value: new_value.into(),
        })
    }

    if let Some((old_value, new_value)) = storage.fee_growth_global1x128() {
        fee_growth_global.push(events::FeeGrowthGlobal {
            pool_address: Hex(&pool_address).to_string(),
            ordinal: log_ordinal,
            token_idx: 1,
            new_value: new_value.into(),
        })
    }

    return fee_growth_global;
}

pub fn _get_storage_change<'a>(
    emitter_address: &'a Vec<u8>,
    key: [u8; 32],
    storage_changes: &'a Vec<StorageChange>,
) -> Option<&'a StorageChange> {
    for storage_change in storage_changes {
        if emitter_address.eq(&storage_change.address) {
            if key.to_vec() == storage_change.key {
                return Some(storage_change);
            }
        }
    }
    return None;
}

pub fn calculate_amount_usd(
    amount0: &BigDecimal,
    amount1: &BigDecimal,
    token0_derived_eth_price: &BigDecimal,
    token1_derived_eth_price: &BigDecimal,
    bundle_eth_price: &BigDecimal,
) -> BigDecimal {
    return amount0
        .clone()
        .mul(token0_derived_eth_price.clone().mul(bundle_eth_price.clone()))
        .add(
            amount1
                .clone()
                .mul(token1_derived_eth_price.clone().mul(bundle_eth_price.clone())),
        );
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
    let price0_usd = token0_derived_eth_price.clone().mul(eth_price_in_usd.clone());
    let price1_usd = token1_derived_eth_price.clone().mul(eth_price_in_usd.clone());

    log::info!("price0_usd: {}", price0_usd);
    log::info!("price1_usd: {}", price1_usd);

    // both are whitelist tokens, return sum of both amounts
    if WHITELIST_TOKENS.contains(&token0_id.as_str()) && WHITELIST_TOKENS.contains(&token1_id.as_str()) {
        return amount0_abs
            .clone()
            .mul(price0_usd)
            .add(amount1_abs.clone().mul(price1_usd));
    }

    // take double value of the whitelisted token amount
    if WHITELIST_TOKENS.contains(&token0_id.as_str()) && !WHITELIST_TOKENS.contains(&token1_id.as_str()) {
        return amount0_abs.clone().mul(price0_usd).mul(BigDecimal::from(2 as i32));
    }

    // take double value of the whitelisted token amount
    if !WHITELIST_TOKENS.contains(&token0_id.as_str()) && WHITELIST_TOKENS.contains(&token1_id.as_str()) {
        return amount1_abs.clone().mul(price1_usd).mul(BigDecimal::from(2 as i32));
    }

    // neither token is on white list, tracked amount is 0
    return BigDecimal::from(0 as i32);
}

pub fn get_adjusted_amounts(
    token0_addr: &String,
    token1_addr: &String,
    token0_amount: &BigDecimal,
    token1_amount: &BigDecimal,
    token0_derived_eth_price: &BigDecimal,
    token1_derived_eth_price: &BigDecimal,
    bundle_eth_price_usd: &BigDecimal,
) -> AdjustedAmounts {
    let mut adjusted_amounts = AdjustedAmounts {
        stable_eth: BigDecimal::zero(),
        stable_usd: BigDecimal::zero(),
        stable_eth_untracked: BigDecimal::zero(),
        stable_usd_untracked: BigDecimal::zero(),
    };

    if bundle_eth_price_usd.eq(&BigDecimal::zero()) {
        return adjusted_amounts;
    }

    let mut eth = BigDecimal::zero();

    let eth_untracked = token0_amount
        .clone()
        .mul(token0_derived_eth_price.clone())
        .add(token1_amount.clone().mul(token1_derived_eth_price.clone()));

    if WHITELIST_TOKENS.contains(&token0_addr.as_str()) && WHITELIST_TOKENS.contains(&token1_addr.as_str()) {
        eth = eth_untracked.clone()
    }

    if WHITELIST_TOKENS.contains(&token0_addr.as_str()) && !WHITELIST_TOKENS.contains(&token1_addr.as_str()) {
        eth = token0_amount
            .clone()
            .mul(token0_derived_eth_price.clone())
            .mul(BigDecimal::from(2 as i32));
    }

    if !WHITELIST_TOKENS.contains(&token0_addr.as_str()) && WHITELIST_TOKENS.contains(&token1_addr.as_str()) {
        eth = token1_amount
            .clone()
            .mul(token1_derived_eth_price.clone())
            .mul(BigDecimal::from(2 as i32));
    }

    let usd = eth.clone().mul(bundle_eth_price_usd.clone());
    let usd_untracked = eth_untracked.clone().mul(bundle_eth_price_usd.clone());

    adjusted_amounts.stable_eth = eth;
    adjusted_amounts.stable_usd = usd;
    adjusted_amounts.stable_eth_untracked = eth_untracked;
    adjusted_amounts.stable_usd_untracked = usd_untracked;

    return adjusted_amounts;
}

pub fn load_transaction(
    block_number: u64,
    timestamp: u64,
    log_ordinal: u64,
    transaction_trace: &TransactionTrace,
) -> Transaction {
    let mut transaction = Transaction {
        id: Hex(&transaction_trace.hash).to_string(),
        block_number,
        timestamp,
        gas_used: transaction_trace.gas_used,
        gas_price: Default::default(),
        log_ordinal,
    };
    if let Some(gas_price) = &transaction_trace.gas_price {
        let gas_price: BigInt = BigInt::from_signed_bytes_be(&gas_price.bytes);
        transaction.gas_price = gas_price.to_string();
    }

    transaction
}

pub fn extract_pool_liquidity(
    log_ordinal: u64,
    pool_address: &Vec<u8>,
    storage_changes: &Vec<StorageChange>,
) -> Option<events::PoolLiquidity> {
    for storage_change in storage_changes {
        if pool_address.eq(&storage_change.address) {
            if storage_change.key[storage_change.key.len() - 1] == 4 {
                return Some(events::PoolLiquidity {
                    pool_address: Hex(&pool_address).to_string(),
                    liquidity: BigInt::from_signed_bytes_be(&storage_change.new_value).to_string(),
                    log_ordinal,
                });
            }
        }
    }
    None
}

pub fn get_derived_eth_price(ordinal: u64, token_addr: &String, eth_prices_store: &StoreGetBigDecimal) -> BigDecimal {
    return match eth_prices_store.get_at(ordinal, &keyer::token_eth_price(&token_addr)) {
        None => panic!("token eth price not found for token {}", token_addr),
        Some(price) => price,
    };
}

pub fn get_total_value_locked_token(
    ordinal: u64,
    token_addr: &String,
    total_value_locked_store: &StoreGetBigDecimal,
) -> BigDecimal {
    return match total_value_locked_store.get_at(ordinal, &format!("token:{token_addr}")) {
        None => {
            panic!("impossible")
        }
        Some(val) => val,
    };
}

pub fn extract_item_from_key_last_item(delta_key: &String) -> String {
    return delta_key.as_str().split(":").last().unwrap().to_string();
}

pub fn extract_item_from_key_at_position(delta_key: &String, position: usize) -> String {
    return delta_key.split(":").nth(position).unwrap().to_string();
}

pub fn pool_time_data_id<T: AsRef<str> + Display>(pool_address: T, time_id: T) -> String {
    format!("0x{}-{}", pool_address, time_id)
}

pub fn token_time_data_id<T: AsRef<str> + Display>(token_address: T, time_id: T) -> String {
    format!("0x{}-{}", token_address, time_id)
}

pub fn extract_last_item_time_id_as_i64(delta_key: &String) -> i64 {
    return delta_key.as_str().split(":").last().unwrap().parse::<i64>().unwrap();
}

pub fn extract_at_position_time_id_as_i64(delta_key: &String, position: usize) -> i64 {
    return delta_key
        .as_str()
        .split(":")
        .nth(position)
        .unwrap()
        .parse::<i64>()
        .unwrap();
}

pub fn extract_at_position_pool_address_as_str(delta_key: &String, position: usize) -> &str {
    return delta_key.as_str().split(":").nth(position).unwrap();
}

pub fn extract_at_position_token_address_as_str(delta_key: &String, position: usize) -> &str {
    return delta_key.as_str().split(":").nth(position).unwrap();
}

pub fn extract_swap_volume_pool_entity_change_name(delta_key: &String) -> Option<&str> {
    return match delta_key.as_str().split(":").last().unwrap() {
        "volumeToken0" => Some("volumeToken0"), // TODO: validate data
        "volumeToken1" => Some("volumeToken1"), // TODO: validate data
        "volumeUSD" => Some("volumeUSD"),       // TODO: validate data
        "feesUSD" => Some("feesUSD"),           // TODO: validate data
        _ => None,
    };
}

pub fn extract_swap_volume_token_entity_change_name(delta_key: &String) -> Option<&str> {
    return match delta_key.as_str().split(":").last().unwrap() {
        //TODO: need to add the :volume key
        "volumeToken0" => Some("volumeToken0"), // TODO: validate data
        "volumeToken1" => Some("volumeToken1"), // TODO: validate data
        "volumeUSD" => Some("volumeUSD"),       // TODO: validate data
        "feesUSD" => Some("feesUSD"),           // TODO: validate data
        _ => None,
    };
}

// ---------------------------------
// Pool Day/Hour Data Entity Change
// ---------------------------------

pub fn update_fee_growth_global_x128_pool(tables: &mut Tables, table_name: &str, delta: &DeltaBigInt) {
    let time_id = extract_last_item_time_id_as_i64(&delta.key).to_string();
    let pool_address = extract_at_position_pool_address_as_str(&delta.key, 1);

    if let Some(name) = match delta.key.as_str().split(":").nth(2).unwrap() {
        "token0" => Some("feeGrowthGlobal0X128"),
        "token1" => Some("feeGrowthGlobal1X128"),
        _ => None,
    } {
        tables
            .update_row(table_name, pool_time_data_id(pool_address, &time_id).as_str())
            .set(name, &delta.new_value);
    }
}

pub fn update_total_value_locked_usd_pool(tables: &mut Tables, table_name: &str, delta: &DeltaBigDecimal) {
    let time_id = extract_last_item_time_id_as_i64(&delta.key).to_string();
    let pool_address = extract_at_position_pool_address_as_str(&delta.key, 1);

    tables
        .update_row(table_name, pool_time_data_id(pool_address, &time_id).as_str())
        .set("totalValueLockedUSD", &delta.new_value);
}

// ---------------------------------
// Token Day/Hour Data Entity Change
// ---------------------------------
pub fn update_total_value_locked_usd_token(tables: &mut Tables, table_name: &str, delta: &DeltaBigDecimal) {
    let time_id = key::last_segment(&delta.key);
    let token_address = key::segment(&delta.key, 1);

    tables
        .update_row(table_name, format!("0x{token_address}-{time_id}"))
        .set("totalValueLockedUSD", &delta.new_value);
}

pub fn update_total_value_locked_token(tables: &mut Tables, table_name: &str, delta: &DeltaBigDecimal) {
    let time_id = key::last_segment(&delta.key);
    let token_address = key::segment(&delta.key, 1);

    tables
        .update_row(table_name, format!("0x{token_address}-{time_id}"))
        .set("totalValueLocked", &delta.new_value);
}

pub fn update_token_prices_token(tables: &mut Tables, table_name: &str, delta: &DeltaBigDecimal) {
    let time_id = extract_last_item_time_id_as_i64(&delta.key).to_string();
    let token_address = extract_at_position_token_address_as_str(&delta.key, 1);

    tables
        .update_row(table_name, token_time_data_id(token_address, &time_id).as_str())
        .set("tokenPrice", &delta.new_value);
}
