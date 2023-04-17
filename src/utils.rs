use crate::ethpb::v2::TransactionTrace;
use crate::pb::uniswap::events;
use crate::pb::uniswap::events::PoolSqrtPrice;
use crate::pb::PositionEvent;
use crate::tables::Tables;
use crate::uniswap::events::position::PositionType;
use crate::uniswap::events::Transaction;
use crate::uniswap::BigInt as PbBigInt;
use crate::{keyer, rpc, storage, Erc20Token, Pool, StorageChange, WHITELIST_TOKENS};
use std::fmt::Display;
use std::ops::{Add, Mul};
use std::str::FromStr;
use substreams::prelude::{DeltaBigDecimal, DeltaProto};
use substreams::scalar::{BigDecimal, BigInt};
use substreams::store::{DeltaBigInt, StoreGet, StoreGetProto};
use substreams::{hex, log, Hex};

pub const UNISWAP_V3_FACTORY: [u8; 20] = hex!("1f98431c8ad98523631ae4a59f267346ea31f984");
pub const ZERO_ADDRESS: [u8; 20] = hex!("0000000000000000000000000000000000000000");
pub const NON_FUNGIBLE_POSITION_MANAGER: [u8; 20] =
    hex!("c36442b4a4522e871399cd717abdd847ab11fe88");

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

    let fee_growth_global_0 =
        hex!("0000000000000000000000000000000000000000000000000000000000000001");
    let fee_growth_global_1 =
        hex!("0000000000000000000000000000000000000000000000000000000000000002");

    let storage = storage::UniswapPoolStorage::new(storage_changes, pool_address);

    if let Some((old_value, new_value)) = storage.get_fee_growth_global0x128() {
        fee_growth_global.push(events::FeeGrowthGlobal {
            pool_address: Hex(&pool_address).to_string(),
            ordinal: log_ordinal,
            token_idx: 0,
            new_value: Some(new_value.into()),
        })
    }

    if let Some((old_value, new_value)) = storage.get_fee_growth_global1x128() {
        fee_growth_global.push(events::FeeGrowthGlobal {
            pool_address: Hex(&pool_address).to_string(),
            ordinal: log_ordinal,
            token_idx: 1,
            new_value: Some(new_value.into()),
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
        .mul(
            token0_derived_eth_price
                .clone()
                .mul(bundle_eth_price.clone()),
        )
        .add(
            amount1.clone().mul(
                token1_derived_eth_price
                    .clone()
                    .mul(bundle_eth_price.clone()),
            ),
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
    let price0_usd = token0_derived_eth_price
        .clone()
        .mul(eth_price_in_usd.clone());
    let price1_usd = token1_derived_eth_price
        .clone()
        .mul(eth_price_in_usd.clone());

    log::info!("price0_usd: {}", price0_usd);
    log::info!("price1_usd: {}", price1_usd);

    // both are whitelist tokens, return sum of both amounts
    if WHITELIST_TOKENS.contains(&token0_id.as_str())
        && WHITELIST_TOKENS.contains(&token1_id.as_str())
    {
        return amount0_abs
            .clone()
            .mul(price0_usd)
            .add(amount1_abs.clone().mul(price1_usd));
    }

    // take double value of the whitelisted token amount
    if WHITELIST_TOKENS.contains(&token0_id.as_str())
        && !WHITELIST_TOKENS.contains(&token1_id.as_str())
    {
        return amount0_abs
            .clone()
            .mul(price0_usd)
            .mul(BigDecimal::from(2 as i32));
    }

    // take double value of the whitelisted token amount
    if !WHITELIST_TOKENS.contains(&token0_id.as_str())
        && WHITELIST_TOKENS.contains(&token1_id.as_str())
    {
        return amount1_abs
            .clone()
            .mul(price1_usd)
            .mul(BigDecimal::from(2 as i32));
    }

    // neither token is on white list, tracked amount is 0
    return BigDecimal::from(0 as i32);
}

pub struct AdjustedAmounts {
    eth: BigDecimal,
    usd: BigDecimal,
    eth_untracked: BigDecimal,
    usd_untracked: BigDecimal,
}

pub fn _get_adjusted_amounts() -> AdjustedAmounts {
    todo!("implement me")
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
    transaction.gas_price = match transaction_trace.clone().gas_price {
        None => None,
        Some(gas_price) => {
            let gas_price: BigInt = BigInt::from_signed_bytes_be(&gas_price.bytes);
            Some(gas_price.into())
        }
    };
    transaction
}

pub fn get_position(
    store_pool: &StoreGetProto<Pool>,
    log_address: &String,
    transaction_id: &String,
    position_type: PositionType,
    log_ordinal: u64,
    timestamp: u64,
    block_number: u64,
    event: PositionEvent,
) -> Option<events::Position> {
    if let Some(positions_call_result) = rpc::positions_call(log_address, event.get_token_id()) {
        let token_id_0_bytes = positions_call_result.0;
        let token_id_1_bytes = positions_call_result.1;
        let fee = positions_call_result.2;
        let tick_lower: BigInt = positions_call_result.3.into();
        let tick_upper: BigInt = positions_call_result.4.into();
        let fee_growth_inside_0_last_x128: BigInt = positions_call_result.5.into();
        let fee_growth_inside_1_last_x128: BigInt = positions_call_result.6.into();

        let token0: String = Hex(&token_id_0_bytes.as_slice()).to_string();
        let token1: String = Hex(&token_id_1_bytes.as_slice()).to_string();

        let pool: Pool =
            match store_pool.get_last(keyer::pool_token_index_key(&token0, &token1, fee.into())) {
                None => {
                    log::info!(
                        "pool does not exist for token0 {} and token1 {}",
                        token0,
                        token1
                    );
                    return None;
                }
                Some(pool) => pool,
            };

        let amount0 = event.get_amount0().to_decimal(pool.token0_ref().decimals);
        let amount1 = event.get_amount1().to_decimal(pool.token1_ref().decimals);

        return Some(events::Position {
            token_id: event.get_token_id().to_string(),
            owner: event.get_owner(),
            pool: pool.address.clone(),
            token0,
            token1,
            tick_lower: tick_lower.to_string(),
            tick_upper: tick_upper.to_string(),
            transaction: transaction_id.to_string(),
            fee_growth_inside_0_last_x_128: Some(fee_growth_inside_0_last_x128.into()),
            fee_growth_inside_1_last_x_128: Some(fee_growth_inside_1_last_x128.into()),
            liquidity: Some(PbBigInt {
                value: event.get_liquidity(),
            }),
            amount0: Some(amount0.into()),
            amount1: Some(amount1.into()),
            position_type: position_type as i32,
            log_ordinal,
            timestamp,
            block_number,
        });
    }
    return None;
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
                    liquidity: Some(BigInt::from_signed_bytes_be(&storage_change.new_value).into()),
                    log_ordinal,
                });
            }
        }
    }
    None
}

pub fn pool_time_data_id<T: AsRef<str> + Display>(pool_address: T, time_id: T) -> String {
    format!("{}-{}", pool_address, time_id)
}

pub fn token_time_data_id<T: AsRef<str> + Display>(token_address: T, time_id: T) -> String {
    format!("{}-{}", token_address, time_id)
}

pub fn extract_last_item_time_id_as_i64(delta_key: &String) -> i64 {
    return delta_key
        .as_str()
        .split(":")
        .last()
        .unwrap()
        .parse::<i64>()
        .unwrap();
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
pub fn update_tx_count_pool_entity_change(
    tables: &mut Tables,
    table_name: &str,
    delta: &DeltaBigInt,
) {
    let time_id = extract_last_item_time_id_as_i64(&delta.key).to_string();
    let pool_address = extract_at_position_pool_address_as_str(&delta.key, 1);

    tables
        .update_row(
            table_name,
            pool_time_data_id(pool_address, &time_id).as_str(),
        )
        .set("txCount", delta);
}

pub fn update_liquidities_pool_entity_change(
    tables: &mut Tables,
    table_name: &str,
    delta: &DeltaBigInt,
) {
    let time_id = extract_last_item_time_id_as_i64(&delta.key).to_string();
    let pool_address = extract_at_position_pool_address_as_str(&delta.key, 1);

    tables
        .update_row(
            table_name,
            pool_time_data_id(pool_address, &time_id).as_str(),
        )
        .set("liquidity", delta);
}

pub fn update_fee_growth_global_x128_pool_entity_change(
    tables: &mut Tables,
    table_name: &str,
    delta: &DeltaBigInt,
) {
    let time_id = extract_last_item_time_id_as_i64(&delta.key).to_string();
    let pool_address = extract_at_position_pool_address_as_str(&delta.key, 1);

    if let Some(name) = match delta.key.as_str().split(":").nth(2).unwrap() {
        "token0" => Some("feeGrowthGlobal0X128"),
        "token1" => Some("feeGrowthGlobal1X128"),
        _ => None,
    } {
        tables
            .update_row(
                table_name,
                pool_time_data_id(pool_address, &time_id).as_str(),
            )
            .set(name, delta);
    }
}

pub fn update_total_value_locked_usd_pool_entity_change(
    tables: &mut Tables,
    table_name: &str,
    delta: &DeltaBigDecimal,
) {
    let time_it = extract_last_item_time_id_as_i64(&delta.key).to_string();
    let pool_address = extract_at_position_pool_address_as_str(&delta.key, 1);

    tables
        .update_row(
            table_name,
            pool_time_data_id(pool_address, &time_it).as_str(),
        )
        .set("totalValueLockedUSD", delta);
}

pub fn update_sqrt_price_and_tick_pool_entity_change(
    tables: &mut Tables,
    table_name: &str,
    delta: &DeltaProto<PoolSqrtPrice>,
) {
    let time_id = extract_last_item_time_id_as_i64(&delta.key).to_string();
    let pool_address = extract_at_position_pool_address_as_str(&delta.key, 1);

    let sqrt_price: BigInt =
        BigInt::from_str(delta.new_value.sqrt_price.as_ref().unwrap().value.as_str()).unwrap();
    let tick: BigInt =
        BigInt::from_str(delta.new_value.tick.as_ref().unwrap().value.as_str()).unwrap();

    tables
        .update_row(
            table_name,
            pool_time_data_id(pool_address, &time_id).as_str(),
        )
        .set("sqrtPrice", sqrt_price)
        .set("tick", tick);
}

// ---------------------------------
// Token Day/Hour Data Entity Change
// ---------------------------------
pub fn update_total_value_locked_usd_token_entity_change(
    tables: &mut Tables,
    table_name: &str,
    delta: &DeltaBigDecimal,
) {
    let time_id = extract_last_item_time_id_as_i64(&delta.key).to_string();
    let token_address = extract_at_position_token_address_as_str(&delta.key, 1);

    tables
        .update_row(
            table_name,
            token_time_data_id(token_address, &time_id).as_str(),
        )
        .set("totalValueLockedUSD", delta);
}

pub fn update_total_value_locked_token_entity_change(
    tables: &mut Tables,
    table_name: &str,
    delta: &DeltaBigDecimal,
) {
    let time_id = extract_last_item_time_id_as_i64(&delta.key).to_string();
    let token_address = extract_at_position_token_address_as_str(&delta.key, 1);

    tables
        .update_row(
            table_name,
            token_time_data_id(token_address, &time_id).as_str(),
        )
        .set("totalValueLocked", delta);
}

pub fn update_token_prices_token_entity_change(
    tables: &mut Tables,
    table_name: &str,
    delta: &DeltaBigDecimal,
) {
    let time_id = extract_last_item_time_id_as_i64(&delta.key).to_string();
    let token_address = extract_at_position_token_address_as_str(&delta.key, 1);

    tables
        .update_row(
            table_name,
            token_time_data_id(token_address, &time_id).as_str(),
        )
        .set("tokenPrice", delta);
}
