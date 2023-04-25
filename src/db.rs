use std::ops::{Div, Mul};

use substreams::pb::substreams::store_delta;
use substreams::prelude::StoreGetInt64;
use substreams::scalar::{BigDecimal, BigInt};
use substreams::store::{
    DeltaArray, DeltaBigDecimal, DeltaBigInt, DeltaInt64, DeltaProto, Deltas, StoreGet, StoreGetBigDecimal,
    StoreGetBigInt,
};
use substreams::{log, Hex};

use crate::keyer::{POOL_DAY_DATA, POOL_HOUR_DATA, TOKEN_DAY_DATA, TOKEN_HOUR_DATA};
use crate::pb::uniswap::events;
use crate::pb::uniswap::events::pool_event::Type::{Burn as BurnEvent, Mint as MintEvent, Swap as SwapEvent};
use crate::tables::Tables;
use crate::uniswap::{Erc20Token, Pools};
use crate::utils::{extract_item_from_key_at_position, extract_item_from_key_last_item};
use crate::{key, keyer, utils};

// -------------------
//  Map Bundle Entities
// -------------------
pub fn created_bundle_entity_change(tables: &mut Tables) {
    tables
        .update_row("Bundle", "1")
        .set_bigdecimal("ethPriceUSD", &"0.0".to_string());
}

pub fn bundle_store_eth_price_usd_bundle_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in &deltas.deltas {
        if key::first_segment(&delta.key) != "bundle" {
            continue;
        }
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
        .update_row("Factory", &id)
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

pub fn tx_count_factory_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in &deltas.deltas {
        if key::first_segment(&delta.key) != "factory" {
            continue;
        }
        tables
            .update_row("Factory", "0x1F98431c8aD98523631AE4a59f267346ea31F984".to_string())
            .set("txCount", &delta.new_value);
    }
}

pub fn swap_volume_factory_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in &deltas.deltas {
        if key::first_segment(&delta.key) != "factory" {
            continue;
        }
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

pub fn tvl_factory_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in deltas.deltas.iter() {
        if key::first_segment(&delta.key) != "factory" {
            continue;
        }
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
            .update_row("Pool", format!("0x{}", pool.address))
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
            .set("liquidityProviderCount", &bigint0);
    }
}

pub fn sqrt_price_and_tick_pool_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaProto<events::PoolSqrtPrice>>) {
    for delta in deltas.deltas.iter() {
        let pool_address = key::segment(&delta.key, 1);

        tables
            .update_row("Pool", &format!("0x{pool_address}"))
            .set_bigint("sqrtPrice", &delta.new_value.sqrt_price)
            .set_bigint("tick", &delta.new_value.tick);
    }
}

pub fn liquidities_pool_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in deltas.deltas.iter() {
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
            // TODO: rather check if `update.new_value.len() != 0` ?
            row.set_bigint("feeGrowthGlobal0X128", &update.new_value);
        } else if update.token_idx == 1 {
            row.set_bigint("feeGrowthGlobal1X128", &update.new_value);
        }
    }
}

pub fn total_value_locked_pool_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in key::filter_first_segment_eq(deltas, "pool") {
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

pub fn total_value_locked_by_token_pool_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in key::filter_first_segment_eq(deltas, "pool") {
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

pub fn price_pool_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in key::filter_first_segment_eq(deltas, "pool") {
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

pub fn tx_count_pool_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in key::filter_first_segment_eq(deltas, "pool") {
        let pool_address = key::segment(&delta.key, 1);
        tables
            .update_row("Pool", &format!("0x{pool_address}"))
            .set("txCount", &delta.new_value);
    }
}

pub fn swap_volume_pool_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in key::filter_first_segment_eq(deltas, "pool") {
        let pool_address = key::segment(&delta.key, 1);
        let field_name = match key::last_segment(&delta.key) {
            "volumeToken0" => "volumeToken0",
            "volumeToken1" => "volumeToken1",
            "volumeUSD" => "volumeUSD",
            "untrackedVolumeUSD" => "untrackedVolumeUSD",
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

pub fn swap_volume_token_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in key::filter_first_segment_eq(deltas, "token") {
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

pub fn tx_count_token_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in key::filter_first_segment_eq(deltas, "token") {
        log::info!("delta key {}", delta.key);
        let token_address = key::segment(&delta.key, 1);

        tables
            .update_row("Token", format!("0x{token_address}"))
            .set("txCount", &delta.new_value);
    }
}

pub fn total_value_locked_by_token_token_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in key::filter_first_segment_eq(deltas, "token") {
        let token_address = key::last_segment(&delta.key);

        tables
            .update_row("Token", format!("0x{token_address}"))
            .set("totalValueLocked", &delta.new_value);
    }
}

pub fn total_value_locked_usd_token_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in key::filter_first_segment_eq(deltas, "token") {
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

pub fn derived_eth_prices_token_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in key::filter_first_segment_eq(deltas, "token") {
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

pub fn whitelist_token_entity_change(tables: &mut Tables, deltas: Deltas<DeltaArray<String>>) {
    for delta in deltas.deltas {
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
        .update_row("Token", format!("0x{token_addr}"))
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
pub fn liquidities_tick_entity_change(tables: &mut Tables, deltas: Deltas<DeltaBigInt>) {
    for delta in deltas.deltas {
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

pub fn create_tick_entity_change(tables: &mut Tables, ticks: Vec<events::TickCreated>) {
    let bigdecimal0 = BigDecimal::from(0);
    let bigint0 = BigInt::from(0);

    for tick in ticks {
        let pool_address = &tick.pool_address;
        let tick_idx = &tick.idx;
        tables
            .update_row("Tick", format!("0x{pool_address}#{tick_idx}"))
            .set("poolAddress", &tick.pool_address)
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

pub fn update_tick_entity_change(tables: &mut Tables, ticks: Vec<events::TickUpdated>) {
    for tick in ticks {
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

// --------------------
//  Map Position Entities
// --------------------
pub fn position_create_entity_change(tables: &mut Tables, positions: &Vec<events::CreatedPosition>) {
    for position in positions {
        let bigdecimal0 = BigDecimal::from(0);
        tables
            .update_row("Position", position.token_id.clone().as_str())
            .set("id", &position.token_id)
            .set("owner", format!("0x{}", Hex(utils::ZERO_ADDRESS).to_string()))
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

pub fn increase_liquidity_position_entity_change(
    tables: &mut Tables,
    positions: &Vec<events::IncreaseLiquidityPosition>,
) {
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
            .set("owner", &position.owner.clone().into_bytes());
    }
}

// --------------------
//  Map Snapshot Position Entities
// --------------------
pub fn snapshot_positions_create_entity_change(tables: &mut Tables, positions: &Vec<events::CreatedPosition>) {
    for position in positions {
        let id = format!("{}#{}", position.token_id, position.block_number);
        // TODO: decode `PositionsSnapshot.owner` from hex into bytes, and use in "owner" below
        tables
            .update_row("PositionSnapshot", &id)
            .set("id", &id)
            .set("owner", format!("0x{}", Hex(utils::ZERO_ADDRESS).to_string()))
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
}

pub fn increase_liquidity_snapshot_position_entity_change(
    tables: &mut Tables,
    block_number: u64,
    positions: &Vec<events::IncreaseLiquidityPosition>,
) {
    for position in positions {
        let id = format!("{}#{}", position.token_id, block_number);
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
}

pub fn decrease_liquidity_snapshot_position_entity_change(
    tables: &mut Tables,
    block_number: u64,
    positions: &Vec<events::DecreaseLiquidityPosition>,
) {
    for position in positions {
        let id = format!("{}#{}", position.token_id, block_number);
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
}

pub fn collect_snapshot_position_entity_change(
    tables: &mut Tables,
    block_number: u64,
    positions: &Vec<events::CollectPosition>,
) {
    for position in positions {
        let id = format!("{}#{}", position.token_id, block_number);
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
}

pub fn transfer_snapshot_position_entity_change(
    tables: &mut Tables,
    block_number: u64,
    positions: &Vec<events::TransferPosition>,
) {
    for position in positions {
        let id = format!("{}#{}", position.token_id, block_number);
        tables
            .update_row("PositionSnapshot", id)
            .set("owner", &position.owner.clone().into_bytes());
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

                    let mut amount0_abs = amount0.absolute();
                    let mut amount1_abs = amount1.absolute();

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
                        .set("sender", format!("0x{}", swap.sender))
                        .set("recipient", &swap.recipient.into_bytes())
                        .set("origin", &swap.origin.into_bytes())
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

                    tables
                        .create_row("Mint", event_primary_key)
                        .set("transaction", format!("0x{transaction_id}"))
                        .set("timestamp", event.timestamp)
                        .set("pool", format!("0x{pool_address}"))
                        .set("token0", format!("0x{}", event.token0))
                        .set("token1", format!("0x{}", event.token1))
                        .set("owner", &Hex::decode(mint.owner).unwrap())
                        .set("sender", &Hex::decode(mint.sender).unwrap())
                        .set("origin", &Hex::decode(mint.origin).unwrap())
                        .set_bigint("amount", &mint.amount)
                        .set("amount0", amount0)
                        .set("amount1", amount1)
                        .set("amountUSD", amount_usd)
                        .set_bigint("tickLower", &mint.tick_lower)
                        .set_bigint("tickUpper", &mint.tick_upper)
                        .set("logIndex", event.log_index);
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
                        .update_row("Burn", &event_primary_key)
                        .set("transaction", format!("0x{transaction_id}"))
                        .set("pool", format!("0x{pool_address}"))
                        .set("token0", format!("0x{}", event.token0))
                        .set("token1", format!("0x{}", event.token1))
                        .set("timestamp", event.timestamp)
                        .set("owner", &Hex::decode(&burn.owner).unwrap())
                        .set("origin", &Hex::decode(&burn.origin).unwrap())
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
        tables.update_row("Pool", flash.pool_address.as_str());
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
pub fn uniswap_day_data_create_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in key::filter_first_segment_eq(deltas, "UniswapDayData") {
        if !delta.new_value.eq(&BigInt::one()) {
            continue; // From 0 to 1 means the creation of a new day
        }

        let day_id = key::last_segment(&delta.key).parse::<i64>().unwrap();
        let day_start_timestamp = (day_id * 86400) as i32;
        create_uniswap_day_data(tables, day_id, day_start_timestamp, &delta);
    }
}

pub fn tx_count_uniswap_day_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in key::filter_first_segment_eq(deltas, "UniswapDayData") {
        let day_id = key::last_segment(&delta.key);

        tables
            .update_row("UniswapDayData", day_id)
            .set("txCount", &delta.new_value);
    }
}

pub fn totals_uniswap_day_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in key::filter_first_segment_eq(deltas, "UniswapDayData") {
        let day_id = key::last_segment(&delta.key);

        tables
            .update_row("UniswapDayData", day_id)
            .set("totalValueLockedUSD", &delta.new_value);
    }
}

pub fn volumes_uniswap_day_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in key::filter_first_segment_eq(deltas, "UniswapDayData") {
        let day_id = key::segment(&delta.key, 1);

        let name = match key::last_segment(&delta.key) {
            "volumeETH" => "volumeETH", // TODO: validate data
            "volumeUSD" => "volumeUSD", // TODO: validate data
            "feesUSD" => "feesUSD",     // TODO: validate data
            _ => continue,
        };

        // TODO: should this be done on all the updates?
        if delta.operation == store_delta::Operation::Delete {
            tables.delete_row(keyer::UNISWAP_DAY_DATA, &day_id).mark_final();
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
        .update_row("UniswapDayData", &id)
        .set("date", day_start_timestamp)
        .set("volumeETH", &bigdecimal0)
        .set("volumeUSD", &bigdecimal0)
        .set("volumeUSDUntracked", &bigdecimal0) // TODO: NEED TO SET THIS VALUE IN THE SUBSTREAMS
        .set("totalValueLockedUSD", &bigdecimal0)
        .set("feesUSD", &bigdecimal0)
        .set("txCount", &delta.new_value);
}

// --------------------
//  Map Pool Day Data Entities
// --------------------
pub fn pool_day_data_create_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in deltas.deltas.iter() {
        if key::first_segment(&delta.key) != "PoolDayData" || delta.new_value.ne(&BigInt::one()) {
            continue;
        }
        let day_id = utils::extract_last_item_time_id_as_i64(&delta.key);
        let day_start_timestamp = (day_id * 86400) as i32;
        let pool_address = utils::extract_at_position_pool_address_as_str(&delta.key, 1);

        let pool_day_data_id = utils::pool_time_data_id(pool_address, day_id.to_string().as_str()).to_string();

        create_pool_day_data(tables, &pool_day_data_id, day_start_timestamp, pool_address, &delta);
    }
}

pub fn tx_count_pool_day_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in key::filter_first_segment_eq(deltas, "PoolDayData") {
        utils::update_tx_count_pool_entity_change(tables, POOL_DAY_DATA, delta);
    }
    for delta in deltas
        .deltas
        .iter()
        .filter(|d| key::first_segment(&d.key) == "PoolDayData")
    {}
}

pub fn swap_volume_pool_day_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(POOL_DAY_DATA) {
            continue;
        }

        let day_id = utils::extract_at_position_time_id_as_i64(&delta.key, 2).to_string();
        let pool_address = utils::extract_at_position_pool_address_as_str(&delta.key, 1);

        if let Some(name) = utils::extract_swap_volume_pool_entity_change_name(&delta.key) {
            // TODO: should this be done on all the updates?
            if delta.operation == store_delta::Operation::Delete {
                tables.delete_row(POOL_DAY_DATA, &day_id).mark_final();
                continue;
            }

            tables
                .update_row(POOL_DAY_DATA, utils::pool_time_data_id(pool_address, &day_id).as_str())
                .set(name, &delta.new_value);
        }
    }
}

pub fn liquidities_pool_day_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(POOL_DAY_DATA) {
            continue;
        }

        utils::update_liquidities_pool_entity_change(tables, POOL_DAY_DATA, delta);
    }
}

pub fn sqrt_price_and_tick_pool_day_data_entity_change(
    tables: &mut Tables,
    deltas: &Deltas<DeltaProto<events::PoolSqrtPrice>>,
) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(POOL_DAY_DATA) {
            continue;
        }

        utils::update_sqrt_price_and_tick_pool_entity_change(tables, POOL_DAY_DATA, delta);
    }
}

pub fn token_prices_pool_day_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(POOL_DAY_DATA) {
            continue;
        }

        let day_id = key::last_segment(&delta.key);
        let pool_address = key::segment(&delta.key, 1);
        let name = match key::segment(&delta.key, 2) {
            "token0" => "token0Price",
            "token1" => "token1Price",
            _ => continue,
        };

        let old_value = delta.old_value.clone();
        let new_value = delta.new_value.clone();
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

        tables
            .update_row(POOL_DAY_DATA, utils::pool_time_data_id(pool_address, &day_id).as_str())
            .set(name, &delta.new_value);

        if !high.eq(&BigDecimal::zero()) {
            tables
                .update_row(POOL_DAY_DATA, utils::pool_time_data_id(pool_address, &day_id).as_str())
                .set("high", high);
        }

        if !low.eq(&BigDecimal::zero()) {
            tables
                .update_row(POOL_DAY_DATA, utils::pool_time_data_id(pool_address, &day_id).as_str())
                .set("low", low);
        }
    }
}

pub fn fee_growth_global_x128_pool_day_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(POOL_DAY_DATA) {
            continue;
        }

        utils::update_fee_growth_global_x128_pool_entity_change(tables, POOL_DAY_DATA, delta);
    }
}

pub fn total_value_locked_usd_pool_day_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(POOL_DAY_DATA) {
            continue;
        }

        utils::update_total_value_locked_usd_pool_entity_change(tables, POOL_DAY_DATA, delta);
    }
}

fn create_pool_day_data(
    tables: &mut Tables,
    pool_day_data_id: &String,
    day_start_timestamp: i32,
    pool_addr: &str,
    delta: &DeltaBigInt,
) {
    tables
        .update_row(POOL_DAY_DATA, pool_day_data_id)
        .set("date", day_start_timestamp)
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
}

// --------------------
//  Map Pool Hour Data Entities
// --------------------
pub fn pool_hour_data_create_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(POOL_HOUR_DATA) {
            continue;
        }

        if !delta.new_value.eq(&BigInt::one()) {
            continue;
        }

        let hour_id: i64 = utils::extract_last_item_time_id_as_i64(&delta.key);
        let hours_start_unix = (hour_id * 3600) as i32;
        let pool_address = utils::extract_at_position_pool_address_as_str(&delta.key, 1);

        let pool_hour_data_id = utils::pool_time_data_id(pool_address, hour_id.to_string().as_str()).to_string();

        create_pool_hour_data(tables, &pool_hour_data_id, hours_start_unix, pool_address, &delta);
    }
}

pub fn tx_count_pool_hour_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(POOL_HOUR_DATA) {
            continue;
        }

        utils::update_tx_count_pool_entity_change(tables, POOL_HOUR_DATA, delta);
    }
}

pub fn liquidities_pool_hour_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(POOL_HOUR_DATA) {
            continue;
        }

        utils::update_liquidities_pool_entity_change(tables, POOL_HOUR_DATA, delta);
    }
}

pub fn sqrt_price_and_tick_pool_hour_data_entity_change(
    tables: &mut Tables,
    deltas: &Deltas<DeltaProto<events::PoolSqrtPrice>>,
) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(POOL_HOUR_DATA) {
            continue;
        }

        utils::update_sqrt_price_and_tick_pool_entity_change(tables, keyer::POOL_HOUR_DATA, delta);
    }
}

pub fn swap_volume_pool_hour_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(POOL_HOUR_DATA) {
            continue;
        }

        let hour_id = utils::extract_at_position_time_id_as_i64(&delta.key, 2).to_string();
        let pool_address = utils::extract_at_position_pool_address_as_str(&delta.key, 1);

        if let Some(name) = utils::extract_swap_volume_pool_entity_change_name(&delta.key) {
            // TODO: should this be done on all update operations
            if delta.operation == store_delta::Operation::Delete {
                tables.delete_row(POOL_HOUR_DATA, &hour_id).mark_final();
                continue;
            }

            tables
                .update_row(
                    POOL_HOUR_DATA,
                    utils::pool_time_data_id(pool_address, &hour_id).as_str(),
                )
                .set(name, &delta.new_value);
        }
    }
}

pub fn token_prices_pool_hour_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(keyer::POOL_HOUR_DATA) {
            continue;
        }

        let hour_id = key::last_segment(&delta.key);
        let pool_address = key::segment(&delta.key, 1);
        let name = match key::segment(&delta.key, 2) {
            "token0" => "token0Price",
            "token1" => "token1Price",
            _ => continue,
        };

        let old_value = delta.old_value.clone();
        let new_value = delta.new_value.clone();
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

        let pool_hour_id = utils::pool_time_data_id(pool_address, &hour_id);

        tables
            .update_row(POOL_HOUR_DATA, &pool_hour_id)
            .set(name, &delta.new_value);

        if !high.eq(&BigDecimal::zero()) {
            tables.update_row(POOL_HOUR_DATA, &pool_hour_id).set("high", high);
        }

        if !low.eq(&BigDecimal::zero()) {
            tables.update_row(POOL_HOUR_DATA, &pool_hour_id).set("low", low);
        }
    }
}

pub fn fee_growth_global_x128_pool_hour_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(POOL_HOUR_DATA) {
            continue;
        }

        utils::update_fee_growth_global_x128_pool_entity_change(tables, POOL_HOUR_DATA, delta);
    }
}

pub fn total_value_locked_usd_pool_hour_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(POOL_HOUR_DATA) {
            continue;
        }

        utils::update_total_value_locked_usd_pool_entity_change(tables, POOL_HOUR_DATA, delta);
    }
}

fn create_pool_hour_data(
    tables: &mut Tables,
    pool_day_data_id: &String,
    hours_start_unix: i32,
    pool_addr: &str,
    delta: &DeltaBigInt,
) {
    tables
        .update_row(POOL_HOUR_DATA, pool_day_data_id)
        .set("periodStartUnix", hours_start_unix)
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
}

// --------------------
//  Map Token Day Data Entities
// --------------------
pub fn token_day_data_create_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(TOKEN_DAY_DATA) {
            continue;
        }

        if !delta.new_value.eq(&BigInt::one()) {
            continue;
        }

        let day_id = utils::extract_last_item_time_id_as_i64(&delta.key);
        let day_start_timestamp = (day_id * 86400) as i32;
        let token_address = utils::extract_at_position_token_address_as_str(&delta.key, 1);

        let token_day_data_id = utils::token_time_data_id(token_address, &day_id.to_string()).to_string();

        create_token_day_data(tables, &token_day_data_id, day_start_timestamp, token_address);
    }
}

pub fn swap_volume_token_day_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(TOKEN_DAY_DATA) {
            continue;
        }

        let day_id = utils::extract_at_position_time_id_as_i64(&delta.key, 2).to_string();
        let token_address = utils::extract_at_position_token_address_as_str(&delta.key, 1);

        //TODO: need to add the :volume key
        if let Some(name) = utils::extract_swap_volume_token_entity_change_name(&delta.key) {
            if delta.operation == store_delta::Operation::Delete {
                tables.delete_row(TOKEN_DAY_DATA, &day_id).mark_final();
                continue;
            }

            tables
                .update_row(
                    TOKEN_DAY_DATA,
                    utils::token_time_data_id(token_address, &day_id).as_str(),
                )
                .set(name, &delta.new_value);
        }
    }
}

pub fn total_value_locked_usd_token_day_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(TOKEN_DAY_DATA) {
            continue;
        }

        utils::update_total_value_locked_usd_token_entity_change(tables, TOKEN_DAY_DATA, delta);
    }
}

pub fn total_value_locked_token_day_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(TOKEN_DAY_DATA) {
            continue;
        }

        utils::update_total_value_locked_token_entity_change(tables, TOKEN_DAY_DATA, delta);
    }
}

pub fn token_prices_token_day_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(TOKEN_DAY_DATA) {
            continue;
        }

        utils::update_token_prices_token_entity_change(tables, TOKEN_DAY_DATA, delta);
    }
}

fn create_token_day_data(tables: &mut Tables, token_day_data_id: &String, day_start_timestamp: i32, token_addr: &str) {
    tables
        .update_row(TOKEN_DAY_DATA, token_day_data_id)
        .set("date", day_start_timestamp)
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
}

// --------------------
//  Map Token Hour Data Entities
// --------------------
pub fn token_hour_data_create_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(TOKEN_HOUR_DATA) {
            continue;
        }

        if !delta.new_value.eq(&BigInt::one()) {
            continue;
        }

        let hour_id = utils::extract_last_item_time_id_as_i64(&delta.key);
        let hour_start_timestamp = (hour_id * 3600) as i32;
        let token_address = utils::extract_at_position_token_address_as_str(&delta.key, 1);

        let token_day_data_id = utils::token_time_data_id(token_address, &hour_id.to_string()).to_string();

        create_token_hour_data(tables, &token_day_data_id, hour_start_timestamp, token_address);
    }
}

pub fn swap_volume_token_hour_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(TOKEN_HOUR_DATA) {
            continue;
        }

        let hour_id = utils::extract_at_position_time_id_as_i64(&delta.key, 2).to_string();
        let token_address = utils::extract_at_position_token_address_as_str(&delta.key, 1);

        if let Some(name) = utils::extract_swap_volume_token_entity_change_name(&delta.key) {
            if delta.operation == store_delta::Operation::Delete {
                tables.delete_row(TOKEN_HOUR_DATA, &hour_id).mark_final();
                continue;
            }

            tables
                .update_row(
                    TOKEN_HOUR_DATA,
                    utils::token_time_data_id(token_address, &hour_id).as_str(),
                )
                .set(name, &delta.new_value);
        }
    }
}

pub fn total_value_locked_usd_token_hour_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(TOKEN_HOUR_DATA) {
            continue;
        }

        utils::update_total_value_locked_usd_token_entity_change(tables, TOKEN_HOUR_DATA, delta);
    }
}

pub fn total_value_locked_token_hour_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(TOKEN_HOUR_DATA) {
            continue;
        }

        let hour_id = utils::extract_last_item_time_id_as_i64(&delta.key).to_string();
        let token_address = utils::extract_at_position_token_address_as_str(&delta.key, 1);

        tables
            .update_row(
                TOKEN_HOUR_DATA,
                utils::token_time_data_id(token_address, &hour_id).as_str(),
            )
            .set("totalValueLocked", &delta.new_value);
    }
}

pub fn token_prices_token_hour_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(TOKEN_HOUR_DATA) {
            continue;
        }

        utils::update_token_prices_token_entity_change(tables, TOKEN_HOUR_DATA, delta);
    }
}

fn create_token_hour_data(tables: &mut Tables, token_hour_data_id: &String, hours_start_unix: i32, token_addr: &str) {
    tables
        .update_row(TOKEN_HOUR_DATA, token_hour_data_id)
        .set("periodStartUnix", hours_start_unix)
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
}
