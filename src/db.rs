use std::ops::Div;
use substreams::pb::substreams::store_delta;
use substreams::prelude::StoreGetInt64;
use substreams::scalar::{BigDecimal, BigInt};
use substreams::store::{
    DeltaArray, DeltaBigDecimal, DeltaBigInt, DeltaProto, Deltas, StoreGet, StoreGetBigDecimal, StoreGetBigInt,
    StoreGetProto,
};
use substreams::{log, Hex};

use crate::keyer::{POOL_DAY_DATA, POOL_HOUR_DATA, TOKEN_DAY_DATA, TOKEN_HOUR_DATA};
use crate::pb::uniswap::events;
use crate::pb::uniswap::events::pool_event::Type::{Burn as BurnEvent, Mint as MintEvent, Swap as SwapEvent};
use crate::pb::uniswap::events::position_event::Type;
use crate::pb::uniswap::events::{IncreaseLiquidityPosition, PositionEvent};
use crate::tables::Tables;
use crate::uniswap::{Erc20Token, Pools};
use crate::{key, keyer, utils};

// -------------------
//  Map Bundle Entities
// -------------------
pub fn created_bundle_entity_change(tables: &mut Tables) {
    tables
        .create_row("Bundle", "1")
        .set_bigdecimal("ethPriceUSD", &"0.0".to_string());
}

pub fn bundle_store_eth_price_usd_bundle_entity_change(
    tables: &mut Tables,
    derived_eth_prices_deltas: &Deltas<DeltaBigDecimal>,
) {
    for delta in key::filter_first_segment_eq(&derived_eth_prices_deltas, "bundle") {
        tables.update_row("Bundle", "1").set("ethPriceUSD", &delta.new_value);
    }
}

// -------------------
//  Map Factory Entities
// -------------------
pub fn factory_created_factory_entity_change(tables: &mut Tables) {
    let id = "0x1F98431c8aD98523631AE4a59f267346ea31F984".to_string();

    let bigint0 = BigInt::zero();
    let bigdecimal0 = BigDecimal::zero();
    tables
        .create_row("Factory", &id)
        .set("poolCount", &bigint0)
        .set("txCount", &bigint0)
        .set("totalVolumeUSD", &bigdecimal0)
        .set("totalVolumeETH", &bigdecimal0)
        .set("totalFeesUSD", &bigdecimal0)
        .set("totalFeesETH", &bigdecimal0)
        .set("untrackedVolumeUSD", &bigdecimal0)
        .set("totalValueLockedUSD", &bigdecimal0)
        .set("totalValueLockedETH", &bigdecimal0)
        .set("totalValueLockedUSDUntracked", &bigdecimal0)
        .set("totalValueLockedETHUntracked", &bigdecimal0)
        .set("owner", &format!("0x{}", Hex(utils::ZERO_ADDRESS).to_string()));
}

pub fn pool_created_factory_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in deltas.deltas.iter() {
        let id = "0x1F98431c8aD98523631AE4a59f267346ea31F984".to_string();
        tables.update_row("Factory", &id).set("poolCount", &delta.new_value);
    }
}

pub fn tx_count_factory_entity_change(tables: &mut Tables, tx_count_deltas: &Deltas<DeltaBigInt>) {
    for delta in key::filter_first_segment_eq(&tx_count_deltas, "factory") {
        tables
            .update_row("Factory", "0x1F98431c8aD98523631AE4a59f267346ea31F984".to_string())
            .set("txCount", &delta.new_value);
    }
}

pub fn swap_volume_factory_entity_change(tables: &mut Tables, swaps_volume_deltas: &Deltas<DeltaBigDecimal>) {
    for delta in key::filter_first_segment_eq(&swaps_volume_deltas, "factory") {
        let field_name = match key::last_segment(&delta.key) {
            "totalVolumeUSD" => "totalVolumeUSD",
            "untrackedVolumeUSD" => "untrackedVolumeUSD",
            "totalFeesUSD" => "totalFeesUSD",
            "totalVolumeETH" => "totalVolumeETH",
            "totalFeesETH" => "totalFeesETH",
            _ => continue,
        };

        tables
            .update_row("Factory", "0x1F98431c8aD98523631AE4a59f267346ea31F984".to_string())
            .set(field_name, &delta.new_value);
    }
}

pub fn tvl_factory_entity_change(tables: &mut Tables, derived_factory_tvl_deltas: &Deltas<DeltaBigDecimal>) {
    for delta in key::filter_first_segment_eq(&derived_factory_tvl_deltas, "factory") {
        let field_name = match key::last_segment(&delta.key) {
            "totalValueLockedUSD" => "totalValueLockedUSD",
            "totalValueLockedUSDUntracked" => "totalValueLockedUSDUntracked",
            "totalValueLockedETH" => "totalValueLockedETH",
            "totalValueLockedETHUntracked" => "totalValueLockedETHUntracked",
            _ => continue,
        };

        tables
            .update_row("Factory", "0x1F98431c8aD98523631AE4a59f267346ea31F984".to_string())
            .set(field_name, &delta.new_value);
    }
}

// -------------------
//  Map Pool Entities
// -------------------
pub fn pools_created_pool_entity_change(tables: &mut Tables, pools: &Pools) {
    let bigint0 = BigInt::zero();
    let bigdecimal0 = BigDecimal::zero();

    for pool in &pools.pools {
        tables
            .create_row("Pool", format!("0x{}", pool.address))
            .set("createdAtTimestamp", BigInt::from(pool.created_at_timestamp))
            .set("createdAtBlockNumber", pool.created_at_block_number)
            .set("token0", format!("0x{}", pool.token0.as_ref().unwrap().address))
            .set("token1", format!("0x{}", pool.token1.as_ref().unwrap().address))
            .set_bigint("feeTier", &pool.fee_tier)
            .set("liquidity", &bigint0)
            .set("sqrtPrice", &bigint0)
            .set("feeGrowthGlobal0X128", &bigint0)
            .set("feeGrowthGlobal1X128", &bigint0)
            .set("token0Price", &bigdecimal0)
            .set("token1Price", &bigdecimal0)
            .set("tick", &bigint0)
            .set("observationIndex", &bigint0)
            .set("volumeToken0", &bigdecimal0)
            .set("volumeToken1", &bigdecimal0)
            .set("volumeUSD", &bigdecimal0)
            .set("untrackedVolumeUSD", &bigdecimal0)
            .set("feesUSD", &bigdecimal0)
            .set("txCount", &bigint0)
            .set("collectedFeesToken0", &bigdecimal0)
            .set("collectedFeesToken1", &bigdecimal0)
            .set("collectedFeesUSD", &bigdecimal0)
            .set("totalValueLockedToken0", &bigdecimal0)
            .set("totalValueLockedToken1", &bigdecimal0)
            .set("totalValueLockedETH", &bigdecimal0)
            .set("totalValueLockedUSD", &bigdecimal0)
            .set("totalValueLockedUSDUntracked", &bigdecimal0)
            .set("totalValueLockedETHUntracked", &bigdecimal0)
            .set("liquidityProviderCount", &bigint0);
    }
}

pub fn sqrt_price_and_tick_pool_entity_change(
    tables: &mut Tables,
    pool_sqrt_price_deltas: &Deltas<DeltaProto<events::PoolSqrtPrice>>,
) {
    for delta in key::filter_first_segment_eq(&pool_sqrt_price_deltas, "pool") {
        let pool_address = key::segment(&delta.key, 1);

        tables
            .update_row("Pool", &format!("0x{pool_address}"))
            .set_bigint("sqrtPrice", &delta.new_value.sqrt_price)
            .set_bigint("tick", &delta.new_value.tick);
    }
}

pub fn liquidities_pool_entity_change(tables: &mut Tables, pool_liquidities_store_deltas: &Deltas<DeltaBigInt>) {
    for delta in key::filter_first_segment_eq(&pool_liquidities_store_deltas, "pool") {
        let pool_address = key::segment(&delta.key, 1);
        tables
            .update_row("Pool", &format!("0x{pool_address}"))
            .set("liquidity", &delta.new_value);
    }
}

pub fn fee_growth_global_pool_entity_change(tables: &mut Tables, updates: Vec<events::FeeGrowthGlobal>) {
    for update in updates {
        let pool_address = &update.pool_address;
        let row = tables.update_row("Pool", &format!("0x{pool_address}"));
        if update.token_idx == 0 {
            row.set_bigint("feeGrowthGlobal0X128", &update.new_value);
        } else if update.token_idx == 1 {
            row.set_bigint("feeGrowthGlobal1X128", &update.new_value);
        }
    }
}

pub fn total_value_locked_pool_entity_change(tables: &mut Tables, derived_tvl_deltas: &Deltas<DeltaBigDecimal>) {
    for delta in key::filter_first_segment_eq(derived_tvl_deltas, "pool") {
        let pool_address = key::segment(&delta.key, 1);
        let field_name = match key::last_segment(&delta.key) {
            "usd" => "totalValueLockedUSD",
            "eth" => "totalValueLockedETH",
            "usdUntracked" => "totalValueLockedUSDUntracked",
            "ethUntracked" => "totalValueLockedETHUntracked",
            _ => continue,
        };

        tables
            .update_row("Pool", &format!("0x{pool_address}"))
            .set(field_name, &delta.new_value);
    }
}

pub fn total_value_locked_by_token_pool_entity_change(tables: &mut Tables, token_tvl_deltas: &Deltas<DeltaBigDecimal>) {
    for delta in key::filter_first_segment_eq(token_tvl_deltas, "pool") {
        let pool_address = key::segment(&delta.key, 1);
        let field_name = match key::last_segment(&delta.key) {
            "token0" => "totalValueLockedToken0",
            "token1" => "totalValueLockedToken1",
            _ => continue,
        };

        tables
            .update_row("Pool", &format!("0x{pool_address}"))
            .set(field_name, &delta.new_value);
    }
}

pub fn fee_growth_global_x128_pool_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in key::filter_first_segment_eq(deltas, "pool") {
        let pool_address = key::segment(&delta.key, 1);
        let name = match key::last_segment(&delta.key) {
            "token0" => "feeGrowthGlobal0X128",
            "token1" => "feeGrowthGlobal1X128",
            _ => continue,
        };

        tables
            .update_row("Pool", &format!("0x{pool_address}"))
            .set(name, &delta.new_value);
    }
}

pub fn price_pool_entity_change(tables: &mut Tables, price_deltas: &Deltas<DeltaBigDecimal>) {
    for delta in key::filter_first_segment_eq(price_deltas, "pool") {
        let pool_address = key::segment(&delta.key, 1);
        let name: &str = match key::last_segment(&delta.key) {
            "token0" => "token0Price",
            "token1" => "token1Price",
            _ => continue,
        };

        tables
            .update_row("Pool", &format!("0x{pool_address}"))
            .set(name, &delta.new_value);
    }
}

pub fn tx_count_pool_entity_change(tables: &mut Tables, tx_count_deltas: &Deltas<DeltaBigInt>) {
    for delta in key::filter_first_segment_eq(tx_count_deltas, "pool") {
        let pool_address = key::segment(&delta.key, 1);
        tables
            .update_row("Pool", &format!("0x{pool_address}"))
            .set("txCount", &delta.new_value);
    }
}

pub fn swap_volume_pool_entity_change(tables: &mut Tables, swaps_volume_deltas: &Deltas<DeltaBigDecimal>) {
    for delta in key::filter_first_segment_eq(swaps_volume_deltas, "pool") {
        let pool_address = key::segment(&delta.key, 1);
        let field_name = match key::last_segment(&delta.key) {
            "volumeToken0" => "volumeToken0",
            "volumeToken1" => "volumeToken1",
            "volumeUSD" => "volumeUSD",
            "volumeUntrackedUSD" => "untrackedVolumeUSD",
            "feesUSD" => "feesUSD",
            "liquidityProviderCount" => "liquidityProviderCount",
            _ => continue,
        };

        if field_name == "liquidityProviderCount" {
            tables
                .update_row("Pool", &format!("0x{pool_address}"))
                .set("liquidityProviderCount", &delta.new_value.to_bigint());
            continue;
        } else {
            tables
                .update_row("Pool", &format!("0x{pool_address}"))
                .set(field_name, &delta.new_value);
        }
    }
}

// --------------------
//  Map Token Entities
// --------------------
pub fn tokens_created_token_entity_change(tables: &mut Tables, pools: &Pools, tokens_store: StoreGetInt64) {
    for pool in &pools.pools {
        let ord = pool.log_ordinal;
        let pool_address = &pool.address;
        let token0_addr = pool.token0_ref().address();
        let token1_addr = pool.token1_ref().address();
        match tokens_store.get_at(ord, format!("token:{token0_addr}")) {
            Some(value) => {
                if value.eq(&1) {
                    add_token_entity_change(tables, pool.token0_ref());
                }
            }
            None => {
                panic!("pool contains token that doesn't exist {}", pool_address.as_str())
            }
        }

        match tokens_store.get_at(ord, format!("token:{token1_addr}")) {
            Some(value) => {
                if value.eq(&1) {
                    add_token_entity_change(tables, pool.token1_ref());
                }
            }
            None => {
                panic!("pool contains token that doesn't exist {}", pool_address.as_str())
            }
        }
    }
}

pub fn swap_volume_token_entity_change(tables: &mut Tables, swaps_volume_deltas: &Deltas<DeltaBigDecimal>) {
    for delta in key::filter_first_segment_eq(swaps_volume_deltas, "token") {
        let token_address = key::segment(&delta.key, 1);
        let field_name: &str = match key::last_segment(&delta.key) {
            "token0" | "token1" => "volume",
            "usd" => "volumeUSD",
            "untrackedUSD" => "untrackedVolumeUSD",
            "feesUSD" => "feesUSD",
            _ => continue,
        };

        tables
            .update_row("Token", format!("0x{token_address}"))
            .set(field_name, &delta.new_value);
    }
}

pub fn tx_count_token_entity_change(tables: &mut Tables, tx_count_deltas: &Deltas<DeltaBigInt>) {
    for delta in key::filter_first_segment_eq(tx_count_deltas, "token") {
        let token_address = key::segment(&delta.key, 1);

        tables
            .update_row("Token", format!("0x{token_address}"))
            .set("txCount", &delta.new_value);
    }
}

pub fn total_value_locked_by_token_token_entity_change(
    tables: &mut Tables,
    token_tvl_deltas: &Deltas<DeltaBigDecimal>,
) {
    for delta in key::filter_first_segment_eq(token_tvl_deltas, "token") {
        let token_address = key::last_segment(&delta.key);

        tables
            .update_row("Token", format!("0x{token_address}"))
            .set("totalValueLocked", &delta.new_value);
    }
}

pub fn total_value_locked_usd_token_entity_change(tables: &mut Tables, derived_tvl_deltas: &Deltas<DeltaBigDecimal>) {
    for delta in key::filter_first_segment_eq(derived_tvl_deltas, "token") {
        let token_address = key::segment(&delta.key, 1);
        let name = match key::last_segment(&delta.key) {
            "usd" => "totalValueLockedUSD",
            _ => continue,
        };

        tables
            .update_row("Token", format!("0x{token_address}"))
            .set(name, &delta.new_value);
    }
}

pub fn derived_eth_prices_token_entity_change(
    tables: &mut Tables,
    derived_eth_prices_deltas: &Deltas<DeltaBigDecimal>,
) {
    for delta in key::filter_first_segment_eq(derived_eth_prices_deltas, "token") {
        let token_address = key::segment(&delta.key, 1);
        let field_name: &str = match key::last_segment(&delta.key) {
            "eth" => "derivedETH",
            _ => continue,
        };

        tables
            .update_row("Token", format!("0x{token_address}"))
            .set(field_name, &delta.new_value);
    }
}

pub fn whitelist_token_entity_change(tables: &mut Tables, tokens_whitelist_pools_deltas: Deltas<DeltaArray<String>>) {
    for delta in tokens_whitelist_pools_deltas.deltas {
        let token_address = key::segment(&delta.key, 1);
        let mut whitelist = delta.new_value;
        whitelist = whitelist.iter().map(|item| format!("0x{}", item)).collect();

        tables
            .update_row("Token", format!("0x{token_address}"))
            .set("whitelistPools", &whitelist);
    }
}

fn add_token_entity_change(tables: &mut Tables, token: &Erc20Token) {
    let bigdecimal0 = BigDecimal::from(0);
    let bigint0 = BigInt::from(0);

    let token_addr = &token.address;
    let mut whitelist = token.clone().whitelist_pools;
    whitelist = whitelist.iter().map(|item| format!("0x{item}")).collect();

    tables
        .create_row("Token", format!("0x{token_addr}"))
        .set("symbol", &token.symbol)
        .set("name", &token.name)
        .set("decimals", token.decimals)
        .set_bigint("totalSupply", &token.total_supply)
        .set("volume", &bigdecimal0)
        .set("volumeUSD", &bigdecimal0)
        .set("untrackedVolumeUSD", &bigdecimal0)
        .set("feesUSD", &bigdecimal0)
        .set("txCount", &bigint0)
        .set("poolCount", &bigint0)
        .set("totalValueLocked", &bigdecimal0)
        .set("totalValueLockedUSD", &bigdecimal0)
        .set("totalValueLockedUSDUntracked", &bigdecimal0)
        .set("derivedETH", &bigdecimal0)
        .set("whitelistPools", &whitelist);
}

// --------------------
//  Map Tick Entities
// --------------------
pub fn create_tick_entity_change(tables: &mut Tables, ticks_created: &Vec<events::TickCreated>) {
    let bigdecimal0 = BigDecimal::from(0);
    let bigint0 = BigInt::from(0);

    for tick in ticks_created {
        let pool_address = &tick.pool_address;
        let tick_idx = &tick.idx;
        tables
            .create_row("Tick", format!("0x{pool_address}#{tick_idx}"))
            .set("poolAddress", format!("0x{}", &tick.pool_address))
            .set_bigint("tickIdx", &tick.idx)
            .set("pool", &format!("0x{pool_address}"))
            .set("liquidityGross", &bigint0)
            .set("liquidityNet", &bigint0)
            .set_bigdecimal("price0", &tick.price0)
            .set_bigdecimal("price1", &tick.price1)
            .set("volumeToken0", &bigdecimal0)
            .set("volumeToken1", &bigdecimal0)
            .set("volumeUSD", &bigdecimal0)
            .set("untrackedVolumeUSD", &bigdecimal0)
            .set("feesUSD", &bigdecimal0)
            .set("collectedFeesToken0", &bigdecimal0)
            .set("collectedFeesToken1", &bigdecimal0)
            .set("collectedFeesUSD", &bigdecimal0)
            .set("createdAtTimestamp", tick.created_at_timestamp)
            .set("createdAtBlockNumber", tick.created_at_block_number)
            .set("liquidityProviderCount", &bigint0)
            .set("feeGrowthOutside0X128", &bigint0)
            .set("feeGrowthOutside1X128", &bigint0);
    }
}

pub fn update_tick_entity_change(tables: &mut Tables, ticks_updated: &Vec<events::TickUpdated>) {
    for tick in ticks_updated {
        let pool_address = &tick.pool_address;
        let tick_idx = &tick.idx;
        let row = tables.update_row("Tick", format!("0x{pool_address}#{tick_idx}"));
        if tick.fee_growth_outside_0x_128.len() != 0 {
            row.set_bigint("feeGrowthOutside0X128", &tick.fee_growth_outside_0x_128);
        }
        if tick.fee_growth_outside_1x_128.len() != 0 {
            row.set_bigint("feeGrowthOutside1X128", &tick.fee_growth_outside_1x_128);
        }
    }
}

pub fn liquidities_tick_entity_change(tables: &mut Tables, ticks_liquidities_deltas: &Deltas<DeltaBigInt>) {
    for delta in key::filter_first_segment_eq(&ticks_liquidities_deltas, "tick") {
        let pool_id = key::segment(&delta.key, 1);
        let tick_idx = key::segment(&delta.key, 2);

        let field_name = match key::last_segment(&delta.key) {
            "liquidityNet" => "liquidityNet",
            "liquidityGross" => "liquidityGross",
            _ => continue,
        };

        tables
            .update_row("Tick", &format!("0x{pool_id}#{tick_idx}"))
            .set(field_name, &delta.new_value);
    }
}

// -----------------------
//  Map Tick Day/Hour data
// -----------------------
pub fn create_entity_tick_windows(tables: &mut Tables, ticks_created: &Vec<events::TickCreated>) {
    for tick in ticks_created {
        let day_id = tick.created_at_timestamp / 86400;
        let hour_id = tick.created_at_timestamp / 3600;

        let pool_address = &tick.pool_address;
        let tick_idx = &tick.idx;
        create_tick_windows(tables, "TickDayData", pool_address.as_str(), tick_idx, day_id);
        create_tick_windows(tables, "TickHourData", pool_address.as_str(), tick_idx, hour_id);
    }
}

pub fn update_tick_windows(tables: &mut Tables, ticks_updated: &Vec<events::TickUpdated>) {
    for tick in ticks_updated {
        let day_id = tick.timestamp / 86400;
        let hour_id = tick.timestamp / 3600;

        let tick_idx = &tick.idx;
        let pool_address = &tick.pool_address;

        if tick.fee_growth_outside_0x_128.len() != 0 {
            tables
                .update_row("TickDayData", format!("0x{pool_address}#{tick_idx}-{day_id}"))
                .set_bigint("feeGrowthOutside0X128", &tick.fee_growth_outside_0x_128);
            tables
                .update_row("TickHourData", format!("0x{pool_address}#{tick_idx}-{hour_id}"))
                .set_bigint("feeGrowthOutside0X128", &tick.fee_growth_outside_0x_128);
        }
        if tick.fee_growth_outside_1x_128.len() != 0 {
            tables
                .update_row("TickDayData", format!("0x{pool_address}#{tick_idx}-{day_id}"))
                .set_bigint("feeGrowthOutside1X128", &tick.fee_growth_outside_1x_128);
            tables
                .update_row("TickHourData", format!("0x{pool_address}#{tick_idx}-{hour_id}"))
                .set_bigint("feeGrowthOutside1X128", &tick.fee_growth_outside_1x_128);
        }
    }
}

pub fn liquidities_tick_windows(tables: &mut Tables, ticks_liquidities_deltas: &Deltas<DeltaBigInt>) {
    for delta in ticks_liquidities_deltas.deltas.iter() {
        let table_name = match key::first_segment(&delta.key) {
            "TickDayData" => "TickDayData",
            "TickHourData" => "TickHourData",
            _ => continue,
        };
        let time_id = key::segment(&delta.key, 1);
        let pool_address = key::segment(&delta.key, 2);
        let tick_idx = key::segment(&delta.key, 3);

        let field_name = match key::last_segment(&delta.key) {
            "liquidityNet" => "liquidityNet",
            "liquidityGross" => "liquidityGross",
            _ => continue,
        };

        tables
            .update_row(table_name, format!("0x{pool_address}#{tick_idx}-{time_id}"))
            .set(field_name, &delta.new_value);
    }
}

fn create_tick_windows(tables: &mut Tables, table_name: &str, pool_address: &str, tick_idx: &String, time_id: u64) {
    let bigdecimal0 = BigDecimal::from(0);
    let bigint0 = BigInt::from(0);

    let row = tables
        .create_row(table_name, format!("0x{pool_address}#{tick_idx}-{time_id}"))
        .set("pool", &format!("0x{pool_address}"))
        .set("tick", &format!("0x{pool_address}#{tick_idx}"))
        .set("liquidityGross", &bigint0)
        .set("liquidityNet", &bigint0)
        .set("volumeToken0", &bigdecimal0)
        .set("volumeToken1", &bigdecimal0)
        .set("volumeUSD", &bigdecimal0)
        .set("feesUSD", &bigdecimal0)
        .set("feeGrowthOutside0X128", &bigint0)
        .set("feeGrowthOutside1X128", &bigint0);

    match table_name {
        "TickDayData" => {
            row.set("date", (time_id * 86400) as i32);
        }
        "TickHourData" => {
            row.set("periodStartUnix", (time_id * 3600) as i32);
        }
        _ => {}
    }
}

// --------------------
//  Map Position Entities
// --------------------
pub fn position_create_entity_change(tables: &mut Tables, positions: &Vec<events::CreatedPosition>) {
    for position in positions {
        let bigdecimal0 = BigDecimal::from(0);
        tables
            .create_row("Position", position.token_id.clone().as_str())
            .set("id", &position.token_id)
            .set("owner", &Hex(utils::ZERO_ADDRESS).to_string().into_bytes())
            .set("pool", format!("0x{}", &position.pool))
            .set("token0", format!("0x{}", position.token0))
            .set("token1", format!("0x{}", position.token1))
            .set("tickLower", format!("0x{}#{}", &position.pool, &position.tick_lower))
            .set("tickUpper", format!("0x{}#{}", &position.pool, &position.tick_upper))
            .set_bigint("liquidity", &"0".to_string())
            .set("depositedToken0", &bigdecimal0)
            .set("depositedToken1", &bigdecimal0)
            .set("withdrawnToken0", &bigdecimal0)
            .set("withdrawnToken1", &bigdecimal0)
            .set("collectedFeesToken0", &bigdecimal0)
            .set("collectedFeesToken1", &bigdecimal0)
            .set("transaction", format!("0x{}", position.transaction))
            .set_bigint(
                "feeGrowthInside0LastX128",
                &position.fee_growth_inside0_last_x128.clone().unwrap_or("0".to_string()),
            )
            .set_bigint(
                "feeGrowthInside1LastX128",
                &position.fee_growth_inside1_last_x128.clone().unwrap_or("0".to_string()),
            );
    }
}

pub fn increase_liquidity_position_entity_change(tables: &mut Tables, positions: &Vec<IncreaseLiquidityPosition>) {
    for position in positions {
        let token_id = &position.token_id;
        tables
            .update_row("Position", token_id)
            .set("liquidity", BigInt::try_from(&position.liquidity).unwrap())
            .set_bigdecimal("depositedToken0", &position.deposited_token0)
            .set_bigdecimal("depositedToken1", &position.deposited_token1);

        if let Some(fee_growth_inside0_last_x128) = &position.fee_growth_inside0_last_x128 {
            tables
                .update_row("Position", token_id)
                .set_bigint("feeGrowthInside0LastX128", fee_growth_inside0_last_x128);
        }

        if let Some(fee_growth_inside1_last_x128) = &position.fee_growth_inside1_last_x128 {
            tables
                .update_row("Position", token_id)
                .set_bigint("feeGrowthInside1LastX128", fee_growth_inside1_last_x128);
        }
    }
}

pub fn decrease_liquidity_position_entity_change(
    tables: &mut Tables,
    positions: &Vec<events::DecreaseLiquidityPosition>,
) {
    for position in positions {
        let token_id = position.token_id.clone();
        tables
            .update_row("Position", &token_id)
            .set_bigint("liquidity", &position.liquidity)
            .set_bigdecimal("withdrawnToken0", &position.withdrawn_token0)
            .set_bigdecimal("withdrawnToken1", &position.withdrawn_token1);

        if let Some(fee_growth_inside0_last_x128) = &position.fee_growth_inside0_last_x128 {
            tables
                .update_row("Position", &token_id)
                .set_bigint("feeGrowthInside0LastX128", fee_growth_inside0_last_x128);
        }

        if let Some(fee_growth_inside1_last_x128) = &position.fee_growth_inside1_last_x128 {
            tables
                .update_row("Position", &token_id)
                .set_bigint("feeGrowthInside1LastX128", fee_growth_inside1_last_x128);
        }
    }
}

pub fn collect_position_entity_change(tables: &mut Tables, positions: &Vec<events::CollectPosition>) {
    for position in positions {
        let token_id = position.token_id.clone();
        log::info!("collected_fees_token0 {}", position.collected_fees_token0);
        log::info!("collected_fees_token1 {}", position.collected_fees_token1);
        tables
            .update_row("Position", &token_id)
            .set_bigdecimal("collectedFeesToken0", &position.collected_fees_token0)
            .set_bigdecimal("collectedFeesToken1", &position.collected_fees_token1);

        if let Some(fee_growth_inside0_last_x128) = &position.fee_growth_inside0_last_x128 {
            tables
                .update_row("Position", &token_id)
                .set_bigint("feeGrowthInside0LastX128", fee_growth_inside0_last_x128);
        }

        if let Some(fee_growth_inside1_last_x128) = &position.fee_growth_inside1_last_x128 {
            tables
                .update_row("Position", &token_id)
                .set_bigint("feeGrowthInside1LastX128", fee_growth_inside1_last_x128);
        }
    }
}

pub fn transfer_position_entity_change(tables: &mut Tables, positions: &Vec<events::TransferPosition>) {
    for position in positions {
        tables
            .update_row("Position", position.token_id.clone())
            .set("owner", &hex::decode(&position.owner).unwrap());
    }
}

// --------------------
//  Map Snapshot Position Entities
// --------------------
pub fn snapshot_positions_create_entity_change(tables: &mut Tables, positions: &Vec<events::CreatedPosition>) {
    for position in positions {
        let id = format!("{}#{}", position.token_id, position.block_number);
        create_snapshot_position(tables, &id, position);
    }
}

fn create_snapshot_position(tables: &mut Tables, id: &String, position: &events::CreatedPosition) {
    tables
        .create_row("PositionSnapshot", &id)
        .set("id", id)
        .set("owner", &utils::ZERO_ADDRESS.to_vec())
        .set("pool", format!("0x{}", &position.pool))
        .set("position", &position.token_id)
        .set("blockNumber", position.block_number)
        .set("timestamp", position.timestamp)
        .set_bigint("liquidity", &"0".to_string())
        .set_bigdecimal("depositedToken0", &"0".to_string())
        .set_bigdecimal("depositedToken1", &"0".to_string())
        .set_bigdecimal("withdrawnToken0", &"0".to_string())
        .set_bigdecimal("withdrawnToken1", &"0".to_string())
        .set_bigdecimal("collectedFeesToken0", &"0".to_string())
        .set_bigdecimal("collectedFeesToken1", &"0".to_string())
        .set("transaction", &format!("0x{}", &position.transaction))
        .set_bigint(
            "feeGrowthInside0LastX128",
            &position.fee_growth_inside0_last_x128.clone().unwrap_or("0".to_string()),
        )
        .set_bigint(
            "feeGrowthInside1LastX128",
            &position.fee_growth_inside1_last_x128.clone().unwrap_or("0".to_string()),
        );
}

pub fn increase_liquidity_snapshot_position_entity_change(
    tables: &mut Tables,
    block_number: u64,
    positions: &Vec<IncreaseLiquidityPosition>,
    store_positions: &StoreGetProto<PositionEvent>,
) {
    for position in positions {
        let id = format!("{}#{}", position.token_id, block_number);
        fetch_and_update_snapshot_position(tables, &position.token_id, &id, &store_positions);
        increase_liquidity_snapshot_position(tables, &id, &position)
    }
}

fn increase_liquidity_snapshot_position(tables: &mut Tables, id: &String, position: &IncreaseLiquidityPosition) {
    tables
        .update_row("PositionSnapshot", &id)
        .set_bigint("liquidity", &position.liquidity)
        .set_bigdecimal("depositedToken0", &position.deposited_token0)
        .set_bigdecimal("depositedToken1", &position.deposited_token1);

    if let Some(fee_growth_inside0_last_x128) = &position.fee_growth_inside0_last_x128 {
        tables
            .update_row("PositionSnapshot", &id)
            .set_bigint("feeGrowthInside0LastX128", fee_growth_inside0_last_x128);
    }

    if let Some(fee_growth_inside1_last_x128) = &position.fee_growth_inside1_last_x128 {
        tables
            .update_row("PositionSnapshot", &id)
            .set_bigint("feeGrowthInside1LastX128", fee_growth_inside1_last_x128);
    }
}

pub fn decrease_liquidity_snapshot_position_entity_change(
    tables: &mut Tables,
    block_number: u64,
    positions: &Vec<events::DecreaseLiquidityPosition>,
    store_positions: &StoreGetProto<PositionEvent>,
) {
    for position in positions {
        let id = format!("{}#{}", position.token_id, block_number);
        fetch_and_update_snapshot_position(tables, &position.token_id, &id, &store_positions);
        decrease_liquidity_snapshot_position(tables, &id, &position)
    }
}

fn decrease_liquidity_snapshot_position(
    tables: &mut Tables,
    id: &String,
    position: &events::DecreaseLiquidityPosition,
) {
    tables
        .update_row("PositionSnapshot", &id)
        .set_bigint("liquidity", &position.liquidity)
        .set_bigdecimal("withdrawnToken0", &position.withdrawn_token0)
        .set_bigdecimal("withdrawnToken1", &position.withdrawn_token1);

    if let Some(fee_growth_inside0_last_x128) = &position.fee_growth_inside0_last_x128 {
        tables
            .update_row("PositionSnapshot", &id)
            .set_bigint("feeGrowthInside0LastX128", fee_growth_inside0_last_x128);
    }

    if let Some(fee_growth_inside1_last_x128) = &position.fee_growth_inside1_last_x128 {
        tables
            .update_row("PositionSnapshot", &id)
            .set_bigint("feeGrowthInside1LastX128", fee_growth_inside1_last_x128);
    }
}

pub fn collect_snapshot_position_entity_change(
    tables: &mut Tables,
    block_number: u64,
    positions: &Vec<events::CollectPosition>,
    store_positions: &StoreGetProto<PositionEvent>,
) {
    for position in positions {
        let id = format!("{}#{}", position.token_id, block_number);
        fetch_and_update_snapshot_position(tables, &position.token_id, &id, &store_positions);
        collection_snapshot_position(tables, &id, &position);
    }
}

fn collection_snapshot_position(tables: &mut Tables, id: &String, position: &events::CollectPosition) {
    tables
        .update_row("PositionSnapshot", &id)
        .set_bigdecimal("collectedFeesToken0", &position.collected_fees_token0)
        .set_bigdecimal("collectedFeesToken1", &position.collected_fees_token1);

    if let Some(fee_growth_inside0_last_x128) = &position.fee_growth_inside0_last_x128 {
        tables
            .update_row("PositionSnapshot", &id)
            .set_bigint("feeGrowthInside0LastX128", fee_growth_inside0_last_x128);
    }

    if let Some(fee_growth_inside1_last_x128) = &position.fee_growth_inside1_last_x128 {
        tables
            .update_row("PositionSnapshot", &id)
            .set_bigint("feeGrowthInside1LastX128", fee_growth_inside1_last_x128);
    }
}

pub fn transfer_snapshot_position_entity_change(
    tables: &mut Tables,
    block_number: u64,
    positions: &Vec<events::TransferPosition>,
    store_positions: &StoreGetProto<PositionEvent>,
) {
    for position in positions {
        let id = format!("{}#{}", position.token_id, block_number);
        fetch_and_update_snapshot_position(tables, &position.token_id, &id, &store_positions);
        transfer_snapshot_position(tables, &id, &position);
    }
}

fn transfer_snapshot_position(tables: &mut Tables, id: &String, position: &events::TransferPosition) {
    tables
        .update_row("PositionSnapshot", id)
        .set("owner", &hex::decode(&position.owner).unwrap());
}

fn fetch_and_update_snapshot_position(
    tables: &mut Tables,
    token_id: &String,
    snapshot_id: &String,
    store_positions: &StoreGetProto<PositionEvent>,
) {
    if let Some(position) = store_positions.get_last(format!("position_created:{}", token_id)) {
        match position.r#type.unwrap() {
            Type::CreatedPosition(position) => create_snapshot_position(tables, snapshot_id, &position),
            _ => {}
        }
    }

    if let Some(position) = store_positions.get_last(format!("position_increase_liquidity:{}", token_id)) {
        match position.r#type.unwrap() {
            Type::IncreaseLiquidityPosition(position) => {
                increase_liquidity_snapshot_position(tables, snapshot_id, &position)
            }
            _ => {}
        }
    }

    if let Some(position) = store_positions.get_last(format!("position_decrease_liquidity:{}", token_id)) {
        match position.r#type.unwrap() {
            Type::DecreaseLiquidityPosition(position) => {
                decrease_liquidity_snapshot_position(tables, snapshot_id, &position)
            }
            _ => {}
        }
    }

    if let Some(position) = store_positions.get_last(format!("position_collect:{}", token_id)) {
        match position.r#type.unwrap() {
            Type::CollectPosition(position) => collection_snapshot_position(tables, snapshot_id, &position),
            _ => {}
        }
    }

    if let Some(position) = store_positions.get_last(format!("position_transfer:{}", token_id)) {
        match position.r#type.unwrap() {
            Type::TransferPosition(position) => transfer_snapshot_position(tables, snapshot_id, &position),
            _ => {}
        }
    }
}

// --------------------
//  Map Transaction Entities
// --------------------
pub fn transaction_entity_change(tables: &mut Tables, transactions: Vec<events::Transaction>) {
    for transaction in transactions {
        let id = transaction.id;
        tables
            .update_row("Transaction", format!("0x{id}"))
            .set("blockNumber", transaction.block_number)
            .set("timestamp", transaction.timestamp)
            .set("gasUsed", transaction.gas_used)
            .set_bigint_or_zero("gasPrice", &transaction.gas_price);
    }
}

// --------------------
//  Map Swaps Mints Burns Entities
// --------------------
pub fn swaps_mints_burns_created_entity_change(
    tables: &mut Tables,
    events: Vec<events::PoolEvent>,
    tx_count_store: StoreGetBigInt,
    store_eth_prices: StoreGetBigDecimal,
) {
    for event in events {
        if event.r#type.is_none() {
            continue;
        }

        let ord = event.log_ordinal;
        let token0_addr = &event.token0;
        let token1_addr = &event.token1;

        if event.r#type.is_some() {
            let pool_address = &event.pool_address;
            let transaction_count: i32 = tx_count_store
                .get_at(ord, format!("pool:{pool_address}"))
                .unwrap_or_default()
                .to_u64() as i32;

            let transaction_id = &event.transaction_id;
            let event_primary_key: String = format!("0x{transaction_id}#{transaction_count}");

            // initializePool has occurred beforehand so there should always be a price
            // maybe just ? instead of returning 1 and bubble up the error if there is one
            let token0_derived_eth_price = store_eth_prices
                .get_at(ord, format!("token:{token0_addr}:dprice:eth"))
                .unwrap_or_default();
            let token1_derived_eth_price = store_eth_prices
                .get_at(ord, format!("token:{token1_addr}:dprice:eth"))
                .unwrap_or_default();

            let bundle_eth_price = store_eth_prices.get_at(ord, "bundle").unwrap_or_default();

            return match event.r#type.unwrap() {
                SwapEvent(swap) => {
                    let amount0 = BigDecimal::try_from(swap.amount_0).unwrap();
                    let amount1 = BigDecimal::try_from(swap.amount_1).unwrap();

                    let amount0_abs = amount0.absolute();
                    let amount1_abs = amount1.absolute();

                    let amount_total_usd_tracked = utils::get_tracked_amount_usd(
                        &event.token0,
                        &event.token1,
                        &token0_derived_eth_price,
                        &token1_derived_eth_price,
                        &amount0_abs,
                        &amount1_abs,
                        &bundle_eth_price, // get the value from the store_eth_price
                    )
                    .div(BigDecimal::from(2 as i32));

                    tables
                        .create_row("Swap", &event_primary_key)
                        .set("transaction", format!("0x{transaction_id}"))
                        .set("timestamp", event.timestamp)
                        .set("pool", format!("0x{pool_address}"))
                        .set("token0", format!("0x{}", event.token0))
                        .set("token1", format!("0x{}", event.token1))
                        .set("sender", &hex::decode(&swap.sender).unwrap())
                        .set("recipient", &hex::decode(&swap.recipient).unwrap())
                        .set("origin", &hex::decode(&swap.origin).unwrap())
                        .set("amount0", &amount0)
                        .set("amount1", &amount1)
                        .set("amountUSD", &amount_total_usd_tracked)
                        .set("sqrtPriceX96", &BigInt::try_from(swap.sqrt_price).unwrap())
                        .set("tick", &BigInt::try_from(swap.tick).unwrap())
                        .set("logIndex", event.log_index);
                }
                MintEvent(mint) => {
                    let amount0 = BigDecimal::try_from(mint.amount_0).unwrap();
                    log::info!("mint amount 0 {}", amount0);
                    let amount1 = BigDecimal::try_from(mint.amount_1).unwrap();

                    let amount_usd: BigDecimal = utils::calculate_amount_usd(
                        &amount0,
                        &amount1,
                        &token0_derived_eth_price,
                        &token1_derived_eth_price,
                        &bundle_eth_price,
                    );

                    log::info!("sending create row for MINT");
                    tables
                        .create_row("Mint", event_primary_key)
                        .set("transaction", format!("0x{transaction_id}"))
                        .set("timestamp", event.timestamp)
                        .set("pool", format!("0x{pool_address}"))
                        .set("token0", format!("0x{}", event.token0))
                        .set("token1", format!("0x{}", event.token1))
                        .set("owner", &hex::decode(&mint.owner).unwrap())
                        .set("sender", &hex::decode(&mint.sender).unwrap())
                        .set("origin", &hex::decode(&mint.origin).unwrap())
                        .set_bigint("amount", &mint.amount)
                        .set("amount0", amount0)
                        .set("amount1", amount1)
                        .set("amountUSD", amount_usd)
                        .set_bigint("tickLower", &mint.tick_lower)
                        .set_bigint("tickUpper", &mint.tick_upper)
                        .set("logIndex", event.log_index);
                    log::info!("sent create row for MINT");
                }
                BurnEvent(burn) => {
                    let amount0: BigDecimal = BigDecimal::try_from(burn.amount_0).unwrap();
                    let amount1: BigDecimal = BigDecimal::try_from(burn.amount_1).unwrap();

                    let amount_usd: BigDecimal = utils::calculate_amount_usd(
                        &amount0,
                        &amount1,
                        &token0_derived_eth_price,
                        &token1_derived_eth_price,
                        &bundle_eth_price,
                    );
                    tables
                        .create_row("Burn", &event_primary_key)
                        .set("transaction", format!("0x{transaction_id}"))
                        .set("pool", format!("0x{pool_address}"))
                        .set("token0", format!("0x{}", event.token0))
                        .set("token1", format!("0x{}", event.token1))
                        .set("timestamp", event.timestamp)
                        .set("owner", &hex::decode(&burn.owner).unwrap())
                        .set("origin", &hex::decode(&burn.origin).unwrap())
                        .set_bigint("amount", &burn.amount)
                        .set("amount0", amount0)
                        .set("amount1", amount1)
                        .set("amountUSD", amount_usd)
                        .set_bigint("tickLower", &burn.tick_lower)
                        .set_bigint("tickUpper", &burn.tick_upper)
                        .set("logIndex", event.log_index);
                }
            };
        }
    }
}

// --------------------
//  Map Flashes Entities
// --------------------
pub fn flashes_update_pool_fee_entity_change(tables: &mut Tables, flashes: Vec<events::Flash>) {
    // TODO: wut? flash updates would affect `fee_growth_global_0x_128` and `fee_growth_global_1x_128`?
    //  it's the business of TickUpdate, not of Flashes. Don't flashes produce some such updates?
    for flash in flashes {
        // tables.update_row("Pool", flash.pool_address.as_str());
        // .set(
        //     "feeGrowthGlobal0X128",
        //     BigInt::from(flash.fee_growth_global_0x_128.unwrap()),
        // )
        // .set(
        //     "feeGrowthGlobal1X128",
        //     BigInt::from(flash.fee_growth_global_1x_128.unwrap()),
        // );
    }
}

// --------------------
//  Map Uniswap Day Data Entities
// --------------------
pub fn uniswap_day_data_create_entity_change(tables: &mut Tables, tx_count_deltas: &Deltas<DeltaBigInt>) {
    for delta in key::filter_first_segment_eq(tx_count_deltas, "UniswapDayData") {
        if !delta.new_value.eq(&BigInt::one()) {
            continue;
        }

        if delta.operation == store_delta::Operation::Delete {
            // TODO: need to fix the delete operation
            // tables.delete_row(POOL_HOUR_DATA, &hour_id).mark_final();
            continue;
        }

        let day_id = key::segment(&delta.key, 1).parse::<i64>().unwrap();
        let day_start_timestamp = (day_id * 86400) as i32;
        create_uniswap_day_data(tables, day_id, day_start_timestamp, &delta);
    }
}

pub fn tx_count_uniswap_day_data_entity_change(tables: &mut Tables, tx_count_deltas: &Deltas<DeltaBigInt>) {
    for delta in key::filter_first_segment_eq(tx_count_deltas, "UniswapDayData") {
        if delta.operation == store_delta::Operation::Delete {
            // TODO: need to fix the delete operation
            // tables.delete_row(POOL_HOUR_DATA, &hour_id).mark_final();
            continue;
        }

        let day_id = key::segment(&delta.key, 1);

        tables
            .update_row("UniswapDayData", day_id)
            .set("txCount", &delta.new_value);
    }
}

pub fn totals_uniswap_day_data_entity_change(
    tables: &mut Tables,
    derived_factory_tvl_deltas: &Deltas<DeltaBigDecimal>,
) {
    for delta in key::filter_first_segment_eq(derived_factory_tvl_deltas, "UniswapDayData") {
        if delta.operation == store_delta::Operation::Delete {
            // TODO: need to fix the delete operation
            // tables.delete_row(POOL_HOUR_DATA, &hour_id).mark_final();
            continue;
        }

        let day_id = key::segment(&delta.key, 1);

        tables
            .update_row("UniswapDayData", day_id)
            .set("totalValueLockedUSD", &delta.new_value);
    }
}
pub fn volumes_uniswap_day_data_entity_change(tables: &mut Tables, swaps_volume_deltas: &Deltas<DeltaBigDecimal>) {
    for delta in key::filter_first_segment_eq(swaps_volume_deltas, "UniswapDayData") {
        if delta.operation == store_delta::Operation::Delete {
            // TODO: need to fix the delete operation
            // tables.delete_row(POOL_HOUR_DATA, &hour_id).mark_final();
            continue;
        }

        let day_id = key::segment(&delta.key, 1);

        let name = match key::last_segment(&delta.key) {
            "volumeETH" => "volumeETH", // TODO: validate data
            "volumeUSD" => "volumeUSD", // TODO: validate data
            "feesUSD" => "feesUSD",     // TODO: validate data
            _ => continue,
        };

        // TODO: should this be done on all the updates?
        if delta.operation == store_delta::Operation::Delete {
            // TODO: need to fix the delete operation
            // tables.delete_row(keyer::UNISWAP_DAY_DATA, &day_id).mark_final();
            continue;
        }

        tables
            .update_row(keyer::UNISWAP_DAY_DATA, &day_id)
            .set(name, &delta.new_value);
    }
}

fn create_uniswap_day_data(tables: &mut Tables, day_id: i64, day_start_timestamp: i32, delta: &DeltaBigInt) {
    let bigdecimal0 = BigDecimal::zero();
    let id = day_id.to_string();
    tables
        .create_row("UniswapDayData", &id)
        .set("date", day_start_timestamp)
        .set("volumeETH", &bigdecimal0)
        .set("volumeUSD", &bigdecimal0)
        .set("volumeUSDUntracked", &bigdecimal0) // TODO: NEED TO SET THIS VALUE IN THE SUBSTREAMS
        .set("totalValueLockedUSD", &bigdecimal0)
        .set("feesUSD", &bigdecimal0)
        .set("txCount", &delta.new_value);
}

// -----------------------
//  Map Pool Day/Hour Data
// -----------------------
pub fn create_entity_change_pool_windows(tables: &mut Tables, tx_count_deltas: &Deltas<DeltaBigInt>) {
    for delta in tx_count_deltas.deltas.iter() {
        let table_name = match key::first_segment(&delta.key) {
            "PoolDayData" => "PoolDayData",
            "PoolHourData" => "PoolHourData",
            _ => continue,
        };

        if !delta.new_value.eq(&BigInt::one()) {
            continue;
        }

        if delta.operation == store_delta::Operation::Delete {
            // TODO: need to fix the delete operation
            // tables.delete_row(POOL_HOUR_DATA, &hour_id).mark_final();
            continue;
        }

        let time_id = key::segment(&delta.key, 1).parse::<i64>().unwrap();
        let pool_address = key::segment(&delta.key, 2);

        let pool_time_id = format!("0x{pool_address}-{time_id}");
        create_pool_windows(tables, table_name, time_id, &pool_time_id, pool_address, delta);
    }
}

fn create_pool_windows(
    tables: &mut Tables,
    table_name: &str,
    time_id: i64,
    pool_time_id: &String,
    pool_addr: &str,
    delta: &DeltaBigInt,
) {
    let row = tables
        .create_row(table_name, pool_time_id)
        .set("pool", format!("0x{}", pool_addr))
        .set("liquidity", BigInt::zero())
        .set("sqrtPrice", BigInt::zero())
        .set("token0Price", BigDecimal::zero())
        .set("token1Price", BigDecimal::zero())
        .set("tick", BigInt::zero())
        .set("feeGrowthGlobal0X128", BigInt::zero())
        .set("feeGrowthGlobal1X128", BigInt::zero())
        .set("totalValueLockedUSD", BigDecimal::zero())
        .set("volumeToken0", BigDecimal::zero())
        .set("volumeToken1", BigDecimal::zero())
        .set("volumeUSD", BigDecimal::zero())
        .set("feesUSD", BigDecimal::zero())
        .set("txCount", &delta.new_value)
        .set("open", BigDecimal::zero())
        .set("high", BigDecimal::zero())
        .set("low", BigDecimal::zero())
        .set("close", BigDecimal::zero());

    match table_name {
        "PoolDayData" => {
            row.set("date", (time_id * 86400) as i32);
        }
        "PoolHourData" => {
            row.set("periodStartUnix", (time_id * 3600) as i32);
        }
        _ => {}
    }
}

pub fn tx_count_pool_windows(tables: &mut Tables, tx_count_deltas: &Deltas<DeltaBigInt>) {
    for delta in tx_count_deltas.deltas.iter() {
        let table_name = match key::first_segment(&delta.key) {
            "PoolDayData" => "PoolDayData",
            "PoolHourData" => "PoolHourData",
            _ => continue,
        };

        if delta.operation == store_delta::Operation::Delete {
            // TODO: need to fix the delete operation
            // tables.delete_row(POOL_HOUR_DATA, &hour_id).mark_final();
            continue;
        }

        let time_id = key::segment(&delta.key, 1);
        let pool_address = key::last_segment(&delta.key);

        tables
            .update_row(table_name, format!("0x{pool_address}-{time_id}"))
            .set("txCount", &delta.new_value);
    }
}

pub fn prices_pool_windows(tables: &mut Tables, price_deltas: &Deltas<DeltaBigDecimal>) {
    for delta in price_deltas.deltas.iter() {
        let table_name = match key::first_segment(&delta.key) {
            "PoolDayData" => "PoolDayData",
            "PoolHourData" => "PoolHourData",
            _ => continue,
        };

        if delta.operation == store_delta::Operation::Delete {
            // TODO: need to fix the delete operation
            // tables.delete_row(POOL_HOUR_DATA, &hour_id).mark_final();
            continue;
        }

        let time_id = key::segment(&delta.key, 1);
        let pool_address = key::segment(&delta.key, 2);

        let field_name = match key::last_segment(&delta.key) {
            "token0" => "token0Price",
            "token1" => "token1Price",
            _ => continue,
        };

        let pool_hour_id = format!("0x{pool_address}-{time_id}");

        let old_value = delta.old_value.clone();
        let new_value = delta.new_value.clone();

        // TODO: create a StorePricesMin and StorePricesMax to set the low and high values
        let mut low = BigDecimal::zero();
        let mut high = BigDecimal::zero();
        //TODO: these are not properly set in the subgraph so we
        // let mut open = BigDecimal::zero();
        // let mut close = BigDecimal::zero();

        if new_value.gt(&old_value) {
            high = new_value.clone();
        }

        if new_value.lt(&old_value) {
            low = new_value.clone();
        }

        let mut row = tables
            .update_row(table_name, &pool_hour_id)
            .set(field_name, &delta.new_value);

        if !high.eq(&BigDecimal::zero()) {
            row.set("high", high);
        }

        if !low.eq(&BigDecimal::zero()) {
            row.set("low", low);
        }
    }
}

pub fn liquidities_pool_windows(tables: &mut Tables, pool_liquidities_store_deltas: &Deltas<DeltaBigInt>) {
    for delta in pool_liquidities_store_deltas.deltas.iter() {
        let table_name = match key::first_segment(&delta.key) {
            "PoolDayData" => "PoolDayData",
            "PoolHourData" => "PoolHourData",
            _ => continue,
        };

        if delta.operation == store_delta::Operation::Delete {
            // TODO: need to fix the delete operation
            // tables.delete_row(POOL_HOUR_DATA, &hour_id).mark_final();
            continue;
        }

        let time_id = key::segment(&delta.key, 1);
        let pool_address = key::segment(&delta.key, 2);

        tables
            .update_row(table_name, format!("0x{pool_address}-{time_id}"))
            .set("liquidity", &delta.new_value);
    }
}

pub fn sqrt_price_and_tick_pool_windows(
    tables: &mut Tables,
    pool_sqrt_price_deltas: &Deltas<DeltaProto<events::PoolSqrtPrice>>,
) {
    for delta in pool_sqrt_price_deltas.deltas.iter() {
        let table_name = match key::first_segment(&delta.key) {
            "PoolDayData" => "PoolDayData",
            "PoolHourData" => "PoolHourData",
            _ => continue,
        };

        if delta.operation == store_delta::Operation::Delete {
            // TODO: need to fix the delete operation
            // tables.delete_row(POOL_HOUR_DATA, &hour_id).mark_final();
            continue;
        }

        let time_id = key::segment(&delta.key, 1);
        let pool_address = key::segment(&delta.key, 2);

        let sqrt_price = BigInt::try_from(&delta.new_value.sqrt_price).unwrap();
        let tick = BigInt::try_from(&delta.new_value.tick).unwrap();

        tables
            .update_row(table_name, format!("0x{pool_address}-{time_id}"))
            .set("sqrtPrice", sqrt_price)
            .set("tick", tick);
    }
}

pub fn swap_volume_pool_windows(tables: &mut Tables, swaps_volume_deltas: &Deltas<DeltaBigDecimal>) {
    for delta in swaps_volume_deltas.deltas.iter() {
        let table_name = match key::first_segment(&delta.key) {
            "PoolDayData" => "PoolDayData",
            "PoolHourData" => "PoolHourData",
            _ => continue,
        };

        if delta.operation == store_delta::Operation::Delete {
            // TODO: need to fix the delete operation
            // tables.delete_row(POOL_HOUR_DATA, &hour_id).mark_final();
            continue;
        }

        let time_id = key::segment(&delta.key, 1);
        let pool_address = key::segment(&delta.key, 2);

        let field_name = match key::last_segment(&delta.key) {
            "volumeToken0" => "volumeToken0", // TODO: validate data
            "volumeToken1" => "volumeToken1", // TODO: validate data
            "volumeUSD" => "volumeUSD",       // TODO: validate data
            "feesUSD" => "feesUSD",           // TODO: validate data
            _ => continue,
        };

        tables
            .update_row(table_name, format!("0x{pool_address}-{time_id}"))
            .set(field_name, &delta.new_value);
    }
}

pub fn fee_growth_global_x128_pool_windows(
    tables: &mut Tables,
    pool_fee_growth_global_x128_deltas: &Deltas<DeltaBigInt>,
) {
    for delta in pool_fee_growth_global_x128_deltas.deltas.iter() {
        let table_name = match key::first_segment(&delta.key) {
            "PoolDayData" => "PoolDayData",
            "PoolHourData" => "PoolHourData",
            _ => continue,
        };

        if delta.operation == store_delta::Operation::Delete {
            // TODO: need to fix the delete operation
            // tables.delete_row(POOL_HOUR_DATA, &hour_id).mark_final();
            continue;
        }

        let time_id = key::segment(&delta.key, 1);
        let pool_address = key::segment(&delta.key, 2);

        let field_name = match key::last_segment(&delta.key) {
            "token0" => "feeGrowthGlobal0X128",
            "token1" => "feeGrowthGlobal1X128",
            _ => continue,
        };

        tables
            .update_row(table_name, format!("0x{pool_address}-{time_id}"))
            .set(field_name, &delta.new_value);
    }
}

pub fn total_value_locked_usd_pool_windows(tables: &mut Tables, derived_tvl_deltas: &Deltas<DeltaBigDecimal>) {
    for delta in derived_tvl_deltas.deltas.iter() {
        let table_name = match key::first_segment(&delta.key) {
            "PoolDayData" => "PoolDayData",
            "PoolHourData" => "PoolHourData",
            _ => continue,
        };

        if delta.operation == store_delta::Operation::Delete {
            // TODO: need to fix the delete operation
            // tables.delete_row(POOL_HOUR_DATA, &hour_id).mark_final();
            continue;
        }

        let time_id = key::segment(&delta.key, 1);
        let pool_address = key::segment(&delta.key, 2);

        tables
            .update_row(table_name, format!("0x{pool_address}-{time_id}"))
            .set("totalValueLockedUSD", &delta.new_value);
    }
}

// ---------------------------------
//  Map Token Day/Hour Data Entities
// ---------------------------------
pub fn create_entity_change_token_windows(tables: &mut Tables, tx_count_deltas: &Deltas<DeltaBigInt>) {
    for delta in tx_count_deltas.deltas.iter() {
        let table_name = match key::first_segment(&delta.key) {
            "TokenDayData" => "TokenDayData",
            "TokenHourData" => "TokenHourData",
            _ => continue,
        };

        if !delta.new_value.eq(&BigInt::one()) {
            continue;
        }

        if delta.operation == store_delta::Operation::Delete {
            // TODO: need to fix the delete operation
            // tables.delete_row(POOL_HOUR_DATA, &hour_id).mark_final();
            continue;
        }

        let time_id = key::segment(&delta.key, 1).parse::<i64>().unwrap();
        let token_address = key::segment(&delta.key, 2);

        let token_time_id = format!("0x{token_address}-{time_id}");
        create_token_windows(tables, table_name, time_id, &token_time_id, token_address);
    }
}

fn create_token_windows(
    tables: &mut Tables,
    table_name: &str,
    time_id: i64,
    token_day_time_id: &String,
    token_addr: &str,
) {
    let row = tables
        .create_row(table_name, token_day_time_id)
        .set("token", format!("0x{}", token_addr.to_string()))
        .set("volume", BigDecimal::zero())
        .set("volumeUSD", BigDecimal::zero())
        .set("volumeUSDUntracked", BigDecimal::zero()) // TODO: NEED TO SET THIS VALUE IN THE SUBSTREAMS
        .set("totalValueLocked", BigDecimal::zero())
        .set("totalValueLockedUSD", BigDecimal::zero())
        .set("priceUSD", BigDecimal::zero())
        .set("feesUSD", BigDecimal::zero())
        .set("open", BigDecimal::zero())
        .set("high", BigDecimal::zero())
        .set("low", BigDecimal::zero())
        .set("close", BigDecimal::zero());

    match table_name {
        "TokenDayData" => {
            row.set("date", (time_id * 86400) as i32);
        }
        "TokenHourData" => {
            row.set("periodStartUnix", (time_id * 3600) as i32);
        }
        _ => {}
    }
}

pub fn swap_volume_token_windows(tables: &mut Tables, swaps_volume_deltas: &Deltas<DeltaBigDecimal>) {
    for delta in swaps_volume_deltas.deltas.iter() {
        let table_name = match key::first_segment(&delta.key) {
            "TokenDayData" => "TokenDayData",
            "TokenHourData" => "TokenHourData",
            _ => continue,
        };

        let time_id = key::segment(&delta.key, 1);
        let token_address = key::segment(&delta.key, 2);

        let field_name = match key::last_segment(&delta.key) {
            "volume" => "volume",       // TODO: validate data
            "volumeUSD" => "volumeUSD", // TODO: validate data
            "feesUSD" => "feesUSD",     // TODO: validate data
            _ => continue,              // TODO: need to add the :volume key
        };

        if delta.operation == store_delta::Operation::Delete {
            // TODO: need to fix the delete operation
            // tables.delete_row(TOKEN_DAY_DATA, &day_id).mark_final();
            continue;
        }

        tables
            .update_row(table_name, format!("0x{token_address}-{time_id}"))
            .set(field_name, &delta.new_value);
    }
}

pub fn total_value_locked_usd_token_windows(tables: &mut Tables, derived_tvl_deltas: &Deltas<DeltaBigDecimal>) {
    for delta in derived_tvl_deltas.deltas.iter() {
        let table_name = match key::first_segment(&delta.key) {
            "TokenDayData" => "TokenDayData",
            "TokenHourData" => "TokenHourData",
            _ => continue,
        };

        if delta.operation == store_delta::Operation::Delete {
            // TODO: need to fix the delete operation
            // tables.delete_row(POOL_HOUR_DATA, &hour_id).mark_final();
            continue;
        }

        let time_id = key::segment(&delta.key, 1);
        let token_address = key::segment(&delta.key, 2);

        tables
            .update_row(table_name, format!("0x{token_address}-{time_id}"))
            .set("totalValueLockedUSD", &delta.new_value);
    }
}

pub fn total_value_locked_token_windows(tables: &mut Tables, token_tvl_deltas: &Deltas<DeltaBigDecimal>) {
    for delta in token_tvl_deltas.deltas.iter() {
        let table_name = match key::first_segment(&delta.key) {
            "TokenDayData" => "TokenDayData",
            "TokenHourData" => "TokenHourData",
            _ => continue,
        };

        if delta.operation == store_delta::Operation::Delete {
            // TODO: need to fix the delete operation
            // tables.delete_row(POOL_HOUR_DATA, &hour_id).mark_final();
            continue;
        }

        let time_id = key::segment(&delta.key, 1);
        let token_address = key::segment(&delta.key, 2);

        tables
            .update_row(table_name, format!("0x{token_address}-{time_id}"))
            .set("totalValueLocked", &delta.new_value);
    }
}

pub fn total_prices_token_windows(tables: &mut Tables, derived_eth_prices_deltas: &Deltas<DeltaBigDecimal>) {
    for delta in derived_eth_prices_deltas.deltas.iter() {
        let table_name = match key::first_segment(&delta.key) {
            "TokenDayData" => "TokenDayData",
            "TokenHourData" => "TokenHourData",
            _ => continue,
        };

        if delta.operation == store_delta::Operation::Delete {
            // TODO: need to fix the delete operation
            // tables.delete_row(POOL_HOUR_DATA, &hour_id).mark_final();
            continue;
        }

        let time_id = key::segment(&delta.key, 1);
        let token_address = key::segment(&delta.key, 2);

        tables
            .update_row(table_name, format!("0x{token_address}-{time_id}"))
            .set("priceUSD", &delta.new_value);
    }
}
