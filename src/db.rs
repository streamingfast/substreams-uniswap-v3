use std::ops::{Add, Div, Mul};
use std::str::FromStr;

use substreams::pb::substreams::store_delta;
use substreams::prelude::StoreGetInt64;
use substreams::scalar::{BigDecimal, BigInt};
use substreams::store::{
    DeltaArray, DeltaBigDecimal, DeltaBigInt, DeltaInt64, DeltaProto, Deltas, StoreGet,
    StoreGetBigDecimal, StoreGetBigInt,
};
use substreams::{log, Hex};

use crate::keyer::{POOL_HOUR_DATA, TOKEN_DAY_DATA, TOKEN_HOUR_DATA};
use crate::pb::uniswap::events;
use crate::pb::uniswap::events::pool_event::Type::{
    Burn as BurnEvent, Mint as MintEvent, Swap as SwapEvent,
};
use crate::pb::uniswap::events::position::PositionType;
use crate::pb::uniswap::events::PoolSqrtPrice;
use crate::tables::Tables;
use crate::uniswap::{Erc20Token, Pools, SnapshotPositions};
use crate::{keyer, utils};

// -------------------
//  Map Bundle Entities
// -------------------
pub fn created_bundle_entity_change(tables: &mut Tables) {
    let bd = BigDecimal::from(0 as i32);
    tables.update_row("Bundle", "1").set("ethPriceUSD", bd);
}

pub fn bundle_store_eth_price_usd_bundle_entity_change(
    tables: &mut Tables,
    deltas: &Deltas<DeltaBigDecimal>,
) {
    for delta in &deltas.deltas {
        if !delta.key.starts_with("bundle") {
            continue;
        }

        tables.update_row("Bundle", "1").set("ethPriceUSD", delta);
    }
}

// -------------------
//  Map Factory Entities
// -------------------
pub fn factory_created_factory_entity_change(tables: &mut Tables) {
    tables
        .update_row(
            "Factory",
            Hex(utils::UNISWAP_V3_FACTORY).to_string().as_str(),
        )
        .set("id", Hex(utils::UNISWAP_V3_FACTORY).to_string())
        .set("poolCount", BigInt::zero())
        .set("txCount", BigInt::zero())
        .set("totalVolumeUSD", BigDecimal::zero())
        .set("totalVolumeETH", BigDecimal::zero())
        .set("totalFeesUSD", BigDecimal::zero())
        .set("totalFeesETH", BigDecimal::zero())
        .set("untrackedVolumeUSD", BigDecimal::zero())
        .set("totalValueLockedUSD", BigDecimal::zero())
        .set("totalValueLockedETH", BigDecimal::zero())
        .set("totalValueLockedUSDUntracked", BigDecimal::zero())
        .set("totalValueLockedETHUntracked", BigDecimal::zero())
        .set("owner", Hex(utils::ZERO_ADDRESS).to_string());
}

pub fn pool_created_factory_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in deltas.deltas.iter() {
        tables
            .update_row(
                "Factory",
                Hex(utils::UNISWAP_V3_FACTORY).to_string().as_str(),
            )
            .set("poolCount", delta);
    }
}

pub fn tx_count_factory_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in &deltas.deltas {
        if !delta.key.starts_with("factory:") {
            continue;
        }
        tables
            .update_row(
                "Factory",
                Hex(utils::UNISWAP_V3_FACTORY).to_string().as_str(),
            )
            .set("txCount", delta);
    }
}

pub fn swap_volume_factory_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in &deltas.deltas {
        if !delta.key.as_str().starts_with("factory:") {
            continue;
        }
        let name = match delta.key.as_str().split(":").last().unwrap() {
            "totalVolumeUSD" => "totalVolumeUSD",
            "untrackedVolumeUSD" => "untrackedVolumeUSD",
            "totalFeesUSD" => "totalFeesUSD",
            "totalVolumeETH" => "totalVolumeETH",
            "totalFeesETH" => "totalFeesETH",
            _ => continue,
        };

        tables
            .update_row(
                "Factory",
                Hex(utils::UNISWAP_V3_FACTORY).to_string().as_str(),
            )
            .set(name, delta);
    }
}

pub fn total_value_locked_factory_entity_change(
    tables: &mut Tables,
    deltas: &Deltas<DeltaBigDecimal>,
) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with("factory:") {
            continue;
        }
        let name = match delta.key.as_str().split(":").last().unwrap() {
            "totalValueLockedUSD" => "totalValueLockedUSD",
            "totalValueLockedETH" => "totalValueLockedETH",
            _ => continue,
        };

        tables
            .update_row(
                "Factory",
                Hex(utils::UNISWAP_V3_FACTORY).to_string().as_str(),
            )
            .set(name, delta);
    }
}

// -------------------
//  Map Pool Entities
// -------------------
pub fn pools_created_pool_entity_change(tables: &mut Tables, pools: &Pools) {
    for pool in &pools.pools {
        tables
            .update_row("Pool", &format!("0x{}", pool.address.clone().as_str()))
            .set("id", &pool.address)
            .set(
                "createdAtTimestamp",
                BigInt::from(pool.created_at_timestamp),
            )
            .set(
                "createdAtBlockNumber",
                BigInt::from(pool.created_at_block_number),
            )
            .set("token0", &pool.token0.as_ref().unwrap().address)
            .set("token1", &pool.token1.as_ref().unwrap().address)
            .set("feeTier", BigInt::from(pool.fee_tier.as_ref().unwrap()))
            .set("liquidity", BigInt::zero())
            .set("sqrtPrice", BigInt::zero())
            .set("feeGrowthGlobal0X128", BigInt::zero())
            .set("feeGrowthGlobal1X128", BigInt::zero())
            .set("token0Price", BigDecimal::zero())
            .set("token1Price", BigDecimal::zero())
            .set("tick", BigInt::zero())
            .set("observationIndex", BigInt::zero())
            .set("volumeToken0", BigDecimal::zero())
            .set("volumeToken1", BigDecimal::zero())
            .set("volumeUSD", BigDecimal::zero())
            .set("untrackedVolumeUSD", BigDecimal::zero())
            .set("feesUSD", BigDecimal::zero())
            .set("txCount", BigInt::zero())
            .set("collectedFeesToken0", BigDecimal::zero())
            .set("collectedFeesToken1", BigDecimal::zero())
            .set("collectedFeesUSD", BigDecimal::zero())
            .set("totalValueLockedToken0", BigDecimal::zero())
            .set("totalValueLockedToken1", BigDecimal::zero())
            .set("totalValueLockedETH", BigDecimal::zero())
            .set("totalValueLockedUSD", BigDecimal::zero())
            .set("totalValueLockedUSDUntracked", BigDecimal::zero())
            .set("liquidityProviderCount", BigInt::zero());
    }
}

pub fn pool_sqrt_price_entity_change(
    tables: &mut Tables,
    deltas: &Deltas<DeltaProto<PoolSqrtPrice>>,
) {
    for delta in deltas.deltas.iter() {
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();

        tables
            .update_row("Pool", &format!("0x{}", pool_address.as_str()))
            .set(
                "sqrtPrice",
                DeltaBigInt {
                    operation: delta.operation,
                    ordinal: 0,
                    key: "".to_string(),
                    old_value: delta.old_value.sqrt_price(),
                    new_value: delta.new_value.sqrt_price(),
                },
            )
            .set(
                "tick",
                DeltaBigInt {
                    operation: delta.operation,
                    ordinal: 0,
                    key: "".to_string(),
                    old_value: delta.old_value.tick(),
                    new_value: delta.new_value.tick(),
                },
            );
    }
}

pub fn pool_liquidities_pool_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in deltas.deltas.iter() {
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        tables
            .update_row("Pool", &format!("0x{}", pool_address.as_str()))
            .set("liquidity", delta);
    }
}

pub fn pool_fee_growth_global_entity_change(
    tables: &mut Tables,
    updates: Vec<events::FeeGrowthGlobal>,
) {
    for update in updates {
        let row = tables.update_row("Pool", &format!("0x{}", update.pool_address.as_str()));
        if update.token_idx == 0 {
            row.set(
                "feeGrowthGlobal0X128",
                BigInt::from(&update.new_value.unwrap()),
            );
        } else if update.token_idx == 1 {
            row.set(
                "feeGrowthGlobal1X128",
                BigInt::from(&update.new_value.unwrap()),
            );
        }
    }
}

pub fn total_value_locked_pool_entity_change(tables: &mut Tables, deltas: Deltas<DeltaBigDecimal>) {
    for delta in deltas.deltas {
        if !delta.key.starts_with("pool:") {
            continue;
        }
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();

        let name = match delta.key.as_str().split(":").last().unwrap() {
            "usd" => "totalValueLockedUSD",
            "eth" => "totalValueLockedETH",
            _ => continue,
        };

        tables
            .update_row("Pool", &format!("0x{}", pool_address.as_str()))
            .set(name, delta);
    }
}

pub fn pool_total_value_locked_by_token_entity_change(
    tables: &mut Tables,
    deltas: &Deltas<DeltaBigDecimal>,
) {
    for delta in &deltas.deltas {
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        let name = match delta.key.as_str().split(":").last().unwrap() {
            "token0" => "totalValueLockedToken0",
            "token1" => "totalValueLockedToken1",
            _ => continue,
        };

        tables
            .update_row("Pool", &format!("0x{}", pool_address.as_str()))
            .set(name, delta);
    }
}

pub fn pool_fee_growth_global_x128_entity_change(
    tables: &mut Tables,
    deltas: &Deltas<DeltaBigInt>,
) {
    for delta in deltas.deltas.iter() {
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        let name = match delta.key.as_str().split(":").last().unwrap() {
            "token0" => "feeGrowthGlobal0X128",
            "token1" => "feeGrowthGlobal1X128",
            _ => continue,
        };

        tables
            .update_row("Pool", &format!("0x{}", pool_address.as_str()))
            .set(name, delta);
    }
}

pub fn price_pool_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.as_str().starts_with("pool:") {
            continue;
        }

        let mut key_parts = delta.key.as_str().split(":");
        let pool_address = key_parts.nth(1).unwrap().to_string();
        let name: &str = match key_parts.last().unwrap() {
            "token0" => "token0Price",
            "token1" => "token1Price",
            _ => continue,
        };

        tables
            .update_row("Pool", &format!("0x{}", pool_address.as_str()))
            .set(name, delta);
    }
}

pub fn tx_count_pool_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in &deltas.deltas {
        if !delta.key.starts_with("pool:") {
            continue;
        }

        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string(); // TODO: put in keyer
        tables
            .update_row("Pool", &format!("0x{}", pool_address.as_str()))
            .set("txCount", delta);
    }
}

pub fn swap_volume_pool_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in &deltas.deltas {
        if !delta.key.as_str().starts_with("pool:") {
            continue;
        }

        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        if delta
            .key
            .as_str()
            .split(":")
            .last()
            .unwrap()
            .eq("volumeUSD")
        {
            log::info!("delta value {:?}", delta);
        }

        let name = match delta.key.as_str().split(":").last().unwrap() {
            "volumeToken0" => "volumeToken0",
            "volumeToken1" => "volumeToken1",
            "volumeUSD" => "volumeUSD",
            "untrackedVolumeUSD" => "untrackedVolumeUSD",
            "feesUSD" => "feesUSD",
            _ => continue,
        };

        tables
            .update_row("Pool", &format!("0x{}", pool_address.as_str()))
            .set(name, delta);
    }
}

// --------------------
//  Map Token Entities
// --------------------
pub fn tokens_created_token_entity_change(
    tables: &mut Tables,
    pools: &Pools,
    tokens_store: StoreGetInt64,
) {
    for pool in &pools.pools {
        match tokens_store.get_at(
            pool.log_ordinal,
            &keyer::token_key(pool.token0_ref().address()),
        ) {
            Some(value) => {
                if value.eq(&1) {
                    add_token_entity_change(tables, pool.token0_ref());
                }
            }
            None => {
                panic!("pool contains token that doesn't exist {}", pool.address)
            }
        }

        match tokens_store.get_at(
            pool.log_ordinal,
            &keyer::token_key(pool.token1_ref().address()),
        ) {
            Some(value) => {
                if value.eq(&1) {
                    add_token_entity_change(tables, pool.token1_ref());
                }
            }
            None => {
                panic!("pool contains token that doesn't exist {}", pool.address)
            }
        }
    }
}

pub fn swap_volume_token_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigDecimal>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.as_str().starts_with("token:") {
            continue;
        }

        let token_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        let name: &str = match delta.key.as_str().split(":").last().unwrap() {
            "token0" | "token1" => "volume",
            "usd" => "volumeUSD",
            "untrackedUSD" => "untrackedVolumeUSD",
            "feesUSD" => "feesUSD",
            _ => continue,
        };

        tables
            .update_row("Token", token_address.as_str())
            .set(name, delta);
    }
}

pub fn tx_count_token_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with("token:") {
            continue;
        }

        log::info!("delta key {}", delta.key);
        let token_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        tables
            .update_row("Token", token_address.as_str())
            .set("txCount", delta);
    }
}

pub fn total_value_locked_by_token_token_entity_change(
    tables: &mut Tables,
    deltas: &Deltas<DeltaBigDecimal>,
) {
    for delta in &deltas.deltas {
        if !delta.key.starts_with("token:") {
            continue;
        }

        let token_address = delta.key.as_str().split(":").last().unwrap().to_string();
        log::info!("delta key {:?}", delta.key);
        log::info!("delta old value {:?}", delta.old_value);
        log::info!("delta new value {:?}", delta.new_value);

        tables
            .update_row("Token", token_address.as_str())
            .set("totalValueLocked", delta);
    }
}

pub fn total_value_locked_usd_token_entity_change(
    tables: &mut Tables,
    deltas: Deltas<DeltaBigDecimal>,
) {
    for delta in deltas.deltas {
        if !delta.key.starts_with("token:") {
            continue;
        }

        let token_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();

        let name: &str = match delta.key.as_str().split(":").last().unwrap() {
            "usd" => "totalValueLockedUSD",
            _ => continue,
        };

        tables
            .update_row("Token", token_address.as_str())
            .set(name, delta);
    }
}

pub fn derived_eth_prices_token_entity_change(
    tables: &mut Tables,
    deltas: &Deltas<DeltaBigDecimal>,
) {
    for delta in &deltas.deltas {
        log::info!("delta.key {:?}", delta.key);
        log::info!("delta.old_value {:?}", delta.old_value);
        log::info!("delta.new_value {:?}", delta.new_value);
        if !delta.key.starts_with("token:") {
            continue;
        }

        let token_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        let name: &str = match delta.key.as_str().split(":").last().unwrap() {
            "eth" => "derivedETH",
            _ => continue,
        };

        tables
            .update_row("Token", token_address.as_str())
            .set(name, delta);
    }
}

pub fn whitelist_token_entity_change(tables: &mut Tables, deltas: Deltas<DeltaArray<String>>) {
    for delta in deltas.deltas {
        let token_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        tables
            .update_row("Token", token_address.as_str())
            .set("whitelistPools", delta);
    }
}

fn add_token_entity_change(tables: &mut Tables, token: &Erc20Token) {
    tables
        .update_row("Token", &format!("0x{}", token.address.clone().as_str()))
        .set("id", token.address.clone())
        .set("symbol", token.symbol.clone())
        .set("name", token.name.clone())
        .set("decimals", BigInt::from(token.decimals))
        .set(
            "totalSupply",
            BigInt::from_str(token.total_supply.clone().as_str()).unwrap(),
        )
        .set("volume", BigDecimal::zero())
        .set("volumeUSD", BigDecimal::zero())
        .set("untrackedVolumeUSD", BigDecimal::zero())
        .set("feesUSD", BigDecimal::zero())
        .set("txCount", BigInt::zero())
        .set("poolCount", BigInt::zero())
        .set("totalValueLocked", BigDecimal::zero())
        .set("totalValueLockedUSD", BigDecimal::zero())
        .set("totalValueLockedUSDUntracked", BigDecimal::zero())
        .set("derivedETH", BigDecimal::zero())
        .set("whitelistPools", token.whitelist_pools.clone());
}

// --------------------
//  Map Tick Entities
// --------------------

pub fn ticks_liquidities_tick_entity_change(tables: &mut Tables, deltas: Deltas<DeltaBigInt>) {
    for delta in deltas.deltas {
        let pool_id = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        let tick_idx = delta.key.as_str().split(":").nth(2).unwrap().to_string();

        let name = match delta.key.as_str().split(":").last().unwrap() {
            "liquidityNet" => "liquidityNet",
            "liquidityGross" => "liquidityGross",
            _ => continue,
        };

        tables
            .update_row("Tick", &format!("0x{}#{}", pool_id, tick_idx))
            .set(name, delta);
    }
}

pub fn create_tick_entity_change(tables: &mut Tables, ticks: Vec<events::TickCreated>) {
    for tick in ticks {
        let id = format!(
            "0x{}#{}",
            tick.pool_address,
            BigInt::from(tick.idx.as_ref().unwrap())
        );
        tables
            .update_row("Tick", id.clone().as_str())
            .set("id", &id.clone())
            .set("poolAddress", tick.pool_address.clone())
            .set("tickIdx", BigInt::from(tick.idx.as_ref().unwrap()))
            .set("pool", &format!("0x{}", tick.pool_address))
            .set("liquidityGross", BigInt::zero())
            .set("liquidityNet", BigInt::zero())
            .set("price0", BigDecimal::from(tick.price0.unwrap()))
            .set("price1", BigDecimal::from(tick.price1.unwrap()))
            .set("volumeToken0", BigDecimal::zero())
            .set("volumeToken1", BigDecimal::zero())
            .set("volumeUSD", BigDecimal::zero())
            .set("untrackedVolumeUSD", BigDecimal::zero())
            .set("feesUSD", BigDecimal::zero())
            .set("collectedFeesToken0", BigDecimal::zero())
            .set("collectedFeesToken1", BigDecimal::zero())
            .set("collectedFeesUSD", BigDecimal::zero())
            .set(
                "createdAtTimestamp",
                BigInt::from(tick.created_at_timestamp),
            )
            .set(
                "createdAtBlockNumber",
                BigInt::from(tick.created_at_block_number),
            )
            .set("liquidityProviderCount", BigInt::zero())
            .set("feeGrowthOutside0X128", BigInt::zero())
            .set("feeGrowthOutside1X128", BigInt::zero());
    }
}

pub fn update_tick_entity_change(tables: &mut Tables, ticks: Vec<events::TickUpdated>) {
    for tick in ticks {
        let id = format!(
            "0x{}#{}",
            tick.pool_address,
            BigInt::from(tick.idx.unwrap())
        );
        let row = tables.update_row("Tick", id.clone().as_str());
        if let Some(fee) = tick.fee_growth_outside_0x_128 {
            row.set("feeGrowthOutside0X128", BigInt::from(fee));
        }
        if let Some(fee) = tick.fee_growth_outside_1x_128 {
            row.set("feeGrowthOutside1X128", BigInt::from(fee));
        }
    }
}

// --------------------
//  Map Position Entities
// --------------------
pub fn position_create_entity_change(
    tables: &mut Tables,
    positions: Vec<events::Position>,
    positions_store: StoreGetInt64,
) {
    for position in positions {
        match position.convert_position_type() {
            //TODO: Check https://github.com/streamingfast/substreams-uniswap-v3/issues/6
            // to merge positions of the same id. Probably gonna need a map[string][Position]
            // which will have the id as a key and simply loop over the map and merge the
            // exclusive data types. Good example is getting a Transfer then an
            // IncreaseLiquidity where the IncreaseLiquidity position sets owner (in this case) to
            // 0x000...000 but the Transfer will set a specific owner of the position. We
            // want the owner set by the Transfer as an end result.
            PositionType::IncreaseLiquidity => {
                add_or_skip_position_entity_change(
                    PositionType::IncreaseLiquidity,
                    &positions_store,
                    tables,
                    position,
                );
            }
            PositionType::DecreaseLiquidity => {
                add_or_skip_position_entity_change(
                    PositionType::DecreaseLiquidity,
                    &positions_store,
                    tables,
                    position,
                );
            }
            PositionType::Collect => {
                add_or_skip_position_entity_change(
                    PositionType::Collect,
                    &positions_store,
                    tables,
                    position,
                );
            }
            PositionType::Transfer => add_or_skip_position_entity_change(
                PositionType::Transfer,
                &positions_store,
                tables,
                position,
            ),
            _ => {}
        }
    }
}

pub fn positions_changes_entity_change(tables: &mut Tables, deltas: Deltas<DeltaBigDecimal>) {
    for delta in deltas.deltas {
        let position_id = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        let name = match delta.key.as_str().split(":").last().unwrap() {
            "liquidity" => "liquidity",
            "depositedToken0" => "depositedToken0",
            "depositedToken1" => "depositedToken1",
            "withdrawnToken0" => "withdrawnToken0",
            "withdrawnToken1" => "withdrawnToken1",
            "collectedFeesToken0" => "collectedFeesToken0",
            "collectedFeesToken1" => "collectedFeesToken1",
            _ => continue,
        };

        tables
            .update_row("Position", position_id.as_str())
            .set(name, delta);
    }
}

fn add_or_skip_position_entity_change(
    position_type: PositionType,
    positions_store: &StoreGetInt64,
    tables: &mut Tables,
    position: events::Position,
) {
    match positions_store.get_at(
        position.log_ordinal,
        keyer::position(&position.token_id, &position_type.to_string()),
    ) {
        None => {}
        Some(value) => {
            if value.eq(&1) {
                add_position_entity_change(tables, position);
            }
        }
    }
}

fn add_position_entity_change(tables: &mut Tables, position: events::Position) {
    tables
        .update_row("Position", position.token_id.clone().as_str())
        .set("id", position.token_id)
        .set("owner", position.owner.into_bytes())
        .set("pool", format!("0x{}", &position.pool))
        .set("token0", position.token0)
        .set("token1", position.token1)
        .set(
            "tickLower",
            format!("0x{}#{}", &position.pool, &position.tick_lower),
        )
        .set(
            "tickUpper",
            format!("0x{}#{}", &position.pool, &position.tick_upper),
        )
        .set("liquidity", BigDecimal::zero())
        .set("depositedToken0", BigDecimal::zero())
        .set("depositedToken1", BigDecimal::zero())
        .set("withdrawnToken0", BigDecimal::zero())
        .set("withdrawnToken1", BigDecimal::zero())
        .set("collectedFeesToken0", BigDecimal::zero())
        .set("collectedFeesToken1", BigDecimal::zero())
        .set("transaction", format!("0x{}", position.transaction))
        .set(
            "feeGrowthInside0LastX128",
            BigInt::from(position.fee_growth_inside_0_last_x_128.unwrap()),
        )
        .set(
            "feeGrowthInside1LastX128",
            BigInt::from(position.fee_growth_inside_1_last_x_128.unwrap()),
        );
}

// --------------------
//  Map Snapshot Position Entities
// --------------------
pub fn snapshot_position_entity_change(tables: &mut Tables, snapshot_positions: SnapshotPositions) {
    for snapshot_position in snapshot_positions.snapshot_positions {
        let id = format!(
            "{}#{}",
            snapshot_position.position, snapshot_position.block_number
        );
        tables
            .update_row("PositionSnapshot", id.as_str())
            .set("id", id.clone())
            .set("owner", snapshot_position.owner.into_bytes())
            .set("pool", snapshot_position.pool)
            .set("position", snapshot_position.position)
            .set("blockNumber", BigInt::from(snapshot_position.block_number))
            .set("timestamp", BigInt::from(snapshot_position.timestamp))
            .set(
                "liquidity",
                BigDecimal::from(snapshot_position.liquidity.unwrap()),
            )
            .set(
                "depositedToken0",
                BigDecimal::from(snapshot_position.deposited_token0.unwrap()),
            )
            .set(
                "depositedToken1",
                BigDecimal::from(snapshot_position.deposited_token1.unwrap()),
            )
            .set(
                "withdrawnToken0",
                BigDecimal::from(snapshot_position.withdrawn_token0.unwrap()),
            )
            .set(
                "withdrawnToken1",
                BigDecimal::from(snapshot_position.withdrawn_token1.unwrap()),
            )
            .set(
                "collectedFeesToken0",
                BigDecimal::from(snapshot_position.collected_fees_token0.unwrap()),
            )
            .set(
                "collectedFeesToken1",
                BigDecimal::from(snapshot_position.collected_fees_token1.unwrap()),
            )
            .set(
                "transaction",
                format!("0x{}", &snapshot_position.transaction),
            )
            .set(
                "feeGrowthInside0LastX128", // TODO: use the same SnapshotPosition updates stream as TickUpdates
                BigInt::from(snapshot_position.fee_growth_inside_0_last_x_128.unwrap()),
            )
            .set(
                "feeGrowthInside1LastX128",
                BigInt::from(snapshot_position.fee_growth_inside_1_last_x_128.unwrap()),
            );
    }
}

// --------------------
//  Map Transaction Entities
// --------------------
pub fn transaction_entity_change(tables: &mut Tables, transactions: Vec<events::Transaction>) {
    for transaction in transactions {
        let gas_price = match transaction.gas_price {
            None => BigInt::zero(),
            Some(price) => BigInt::from(price),
        };

        tables
            .update_row("Transaction", transaction.id.clone().as_str())
            .set("id", transaction.id)
            .set("blockNumber", BigInt::from(transaction.block_number))
            .set("timestamp", BigInt::from(transaction.timestamp))
            .set("gasUsed", BigInt::from(transaction.gas_used))
            .set("gasPrice", gas_price);
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

        if event.r#type.is_some() {
            let transaction_count: i32 = match tx_count_store.get_at(
                event.log_ordinal,
                keyer::pool_total_tx_count(&event.pool_address),
            ) {
                Some(data) => data.to_u64() as i32,
                None => 0,
            };

            let transaction_id: String = format!("{}#{}", event.transaction_id, transaction_count);

            let token0_derived_eth_price = match store_eth_prices
                .get_at(event.log_ordinal, keyer::token_eth_price(&event.token0))
            {
                // initializePool has occurred beforehand so there should always be a price
                // maybe just ? instead of returning 1 and bubble up the error if there is one
                None => BigDecimal::zero(),
                Some(price) => price,
            };

            let token1_derived_eth_price: BigDecimal = match store_eth_prices
                .get_at(event.log_ordinal, keyer::token_eth_price(&event.token1))
            {
                // initializePool has occurred beforehand so there should always be a price
                // maybe just ? instead of returning 1 and bubble up the error if there is one
                None => BigDecimal::zero(),
                Some(price) => price,
            };

            let bundle_eth_price: BigDecimal =
                match store_eth_prices.get_at(event.log_ordinal, keyer::bundle_eth_price()) {
                    // initializePool has occurred beforehand so there should always be a price
                    // maybe just ? instead of returning 1 and bubble up the error if there is one
                    None => BigDecimal::zero(),
                    Some(price) => price,
                };

            return match event.r#type.unwrap() {
                SwapEvent(swap) => {
                    let amount0: BigDecimal = BigDecimal::from(swap.amount_0.unwrap());
                    let amount1: BigDecimal = BigDecimal::from(swap.amount_1.unwrap());

                    let mut amount0_abs: BigDecimal = amount0.clone();
                    if amount0_abs.lt(&BigDecimal::from(0 as u64)) {
                        amount0_abs = amount0_abs.mul(BigDecimal::from(-1 as i64))
                    }

                    let mut amount1_abs: BigDecimal = amount1.clone();
                    if amount1_abs.lt(&BigDecimal::from(0 as u64)) {
                        amount1_abs = amount1_abs.mul(BigDecimal::from(-1 as i64))
                    }

                    let amount_total_usd_tracked: BigDecimal = utils::get_tracked_amount_usd(
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
                        .update_row("Swap", transaction_id.clone().as_str())
                        .set("id", transaction_id)
                        .set("transaction", event.transaction_id)
                        .set("timestamp", BigInt::from(event.timestamp))
                        .set("pool", event.pool_address)
                        .set("token0", event.token0)
                        .set("token1", event.token1)
                        .set("sender", swap.sender.into_bytes())
                        .set("recipient", swap.recipient.into_bytes())
                        .set("origin", swap.origin.into_bytes())
                        .set("amount0", amount0)
                        .set("amount1", amount1)
                        .set("amountUSD", amount_total_usd_tracked)
                        .set("sqrtPriceX96", BigInt::from(swap.sqrt_price.unwrap()))
                        .set("tick", BigInt::from(swap.tick.unwrap()))
                        .set("logIndex", BigInt::from(event.log_index));
                }
                MintEvent(mint) => {
                    let amount0: BigDecimal = BigDecimal::from(mint.amount_0.unwrap());
                    log::info!("mint amount 0 {}", amount0);
                    let amount1: BigDecimal = BigDecimal::from(mint.amount_1.unwrap());

                    let amount_usd: BigDecimal = utils::calculate_amount_usd(
                        &amount0,
                        &amount1,
                        &token0_derived_eth_price,
                        &token1_derived_eth_price,
                        &bundle_eth_price,
                    );

                    tables
                        .update_row("Mint", transaction_id.clone().as_str())
                        .set("id", transaction_id)
                        .set("transaction", event.transaction_id)
                        .set("timestamp", BigInt::from(event.timestamp))
                        .set("pool", event.pool_address)
                        .set("token0", event.token0)
                        .set("token1", event.token1)
                        .set("owner", mint.owner.into_bytes())
                        .set("sender", mint.sender.into_bytes())
                        .set("origin", mint.origin.into_bytes())
                        .set("amount", BigInt::from(mint.amount.unwrap()))
                        .set("amount0", amount0)
                        .set("amount1", amount1)
                        .set("amountUSD", amount_usd)
                        .set("tickLower", BigInt::from(mint.tick_lower.unwrap()))
                        .set("tickUpper", BigInt::from(mint.tick_upper.unwrap()))
                        .set("logIndex", BigInt::from(event.log_index));
                }
                BurnEvent(burn) => {
                    let amount0: BigDecimal = BigDecimal::from(burn.amount_0.unwrap());
                    let amount1: BigDecimal = BigDecimal::from(burn.amount_1.unwrap());

                    let amount_usd: BigDecimal = utils::calculate_amount_usd(
                        &amount0,
                        &amount1,
                        &token0_derived_eth_price,
                        &token1_derived_eth_price,
                        &bundle_eth_price,
                    );

                    tables
                        .update_row("Burn", transaction_id.clone().as_str())
                        .set("id", transaction_id)
                        .set("transaction", event.transaction_id)
                        .set("pool", event.pool_address)
                        .set("token0", event.token0)
                        .set("token1", event.token1)
                        .set("timestamp", BigInt::from(event.timestamp))
                        .set("owner", burn.owner.into_bytes())
                        .set("origin", burn.origin.into_bytes())
                        .set("amount", BigInt::from(burn.amount.unwrap()))
                        .set("amount0", amount0)
                        .set("amount1", amount1)
                        .set("amountUSD", amount_usd)
                        .set("tickLower", BigInt::from(burn.tick_lower.unwrap()))
                        .set("tickUpper", BigInt::from(burn.tick_upper.unwrap()))
                        .set("logIndex", BigInt::from(event.log_index));
                }
            };
        }
    }
}

// --------------------
//  Map Flashes Entities
// --------------------
pub fn flashes_update_pool_fee_entity_change(tables: &mut Tables, flashes: Vec<events::Flash>) {
    for flash in flashes {
        tables
            .update_row("Pool", flash.pool_address.as_str())
            .set(
                "feeGrowthGlobal0X128",
                BigInt::from(flash.fee_growth_global_0x_128.unwrap()),
            )
            .set(
                "feeGrowthGlobal1X128",
                BigInt::from(flash.fee_growth_global_1x_128.unwrap()),
            );
    }
}

// --------------------
//  Map Uniswap Day Data Entities
// --------------------
pub fn uniswap_day_data_create_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(keyer::UNISWAP_DAY_DATA) {
            continue;
        }

        if !delta.new_value.eq(&BigInt::one()) {
            return;
        }

        let day_id: i64 = delta
            .key
            .as_str()
            .split(":")
            .last()
            .unwrap()
            .parse::<i64>()
            .unwrap();
        let day_start_timestamp = (day_id * 86400) as i32;
        create_uniswap_day_data(tables, day_id, day_start_timestamp, &delta);
    }
}

pub fn tx_count_uniswap_day_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(keyer::UNISWAP_DAY_DATA) {
            continue;
        }

        let day_id: i64 = delta
            .key
            .as_str()
            .split(":")
            .last()
            .unwrap()
            .parse::<i64>()
            .unwrap();

        tables
            .update_row(keyer::UNISWAP_DAY_DATA, day_id.to_string().as_str())
            .set("txCount", delta);
    }
}

pub fn totals_uniswap_day_data_entity_change(
    tables: &mut Tables,
    deltas: &Deltas<DeltaBigDecimal>,
) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(keyer::UNISWAP_DAY_DATA) {
            continue;
        }

        let day_id: i64 = delta
            .key
            .as_str()
            .split(":")
            .last()
            .unwrap()
            .parse::<i64>()
            .unwrap();

        tables
            .update_row(keyer::UNISWAP_DAY_DATA, day_id.to_string().as_str())
            .set("totalValueLockedUSD", delta);
    }
}

pub fn volumes_uniswap_day_data_entity_change(
    tables: &mut Tables,
    deltas: &Deltas<DeltaBigDecimal>,
) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(keyer::UNISWAP_DAY_DATA) {
            continue;
        }

        let day_id = delta.key.as_str().split(":").nth(1).unwrap();

        let name = match delta.key.as_str().split(":").last().unwrap() {
            "volumeETH" => "volumeETH", // TODO: validate data
            "volumeUSD" => "volumeUSD", // TODO: validate data
            "feesUSD" => "feesUSD",     // TODO: validate data
            _ => continue,
        };

        if delta.operation == store_delta::Operation::Delete {
            tables
                .delete_row(keyer::UNISWAP_DAY_DATA, day_id)
                .mark_final();
            return;
        }

        tables
            .update_row(keyer::UNISWAP_DAY_DATA, day_id)
            .set(name, delta);
    }
}

fn create_uniswap_day_data(
    tables: &mut Tables,
    day_id: i64,
    day_start_timestamp: i32,
    delta: &DeltaBigInt,
) {
    tables
        .update_row(keyer::UNISWAP_DAY_DATA, day_id.to_string().as_str())
        .set("id", day_id.to_string())
        .set("date", day_start_timestamp)
        .set("volumeETH", BigDecimal::zero())
        .set("volumeUSD", BigDecimal::zero())
        .set("volumeUSDUntracked", BigDecimal::zero()) // TODO: NEED TO SET THIS VALUE IN THE SUBSTREAMS
        .set("totalValueLockedUSD", BigDecimal::zero())
        .set("feesUSD", BigDecimal::zero())
        .set("txCount", delta);
}

// --------------------
//  Map Pool Day Data Entities
// --------------------
pub fn pool_day_data_create_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(keyer::POOL_DAY_DATA) {
            continue;
        }

        if !delta.new_value.eq(&BigInt::one()) {
            return;
        }

        let day_id: i64 = delta
            .key
            .as_str()
            .split(":")
            .last()
            .unwrap()
            .parse::<i64>()
            .unwrap();
        let day_start_timestamp = (day_id * 86400) as i32;
        let pool_addr = delta.key.as_str().split(":").nth(1).unwrap();

        let pool_day_data_id = pool_day_data_id(pool_addr, day_id.to_string().as_str()).to_string();

        create_pool_day_data(
            tables,
            &pool_day_data_id,
            day_start_timestamp,
            pool_addr,
            &delta,
        );
    }
}

pub fn tx_count_pool_day_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(keyer::POOL_DAY_DATA) {
            continue;
        }

        let day_id = delta.key.as_str().split(":").last().unwrap();
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap();

        tables
            .update_row(
                keyer::POOL_DAY_DATA,
                pool_day_data_id(pool_address, day_id).as_str(),
            )
            .set("txCount", delta);
    }
}

pub fn swap_volume_pool_day_data_entity_change(
    tables: &mut Tables,
    deltas: &Deltas<DeltaBigDecimal>,
) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(keyer::POOL_DAY_DATA) {
            continue;
        }

        let day_id = delta.key.as_str().split(":").nth(2).unwrap();
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap();

        let name = match delta.key.as_str().split(":").last().unwrap() {
            "volumeToken0" => "volumeToken0", // TODO: validate data
            "volumeToken1" => "volumeToken1", // TODO: validate data
            "volumeUSD" => "volumeUSD",       // TODO: validate data
            "feesUSD" => "feesUSD",           // TODO: validate data
            _ => continue,
        };

        if delta.operation == store_delta::Operation::Delete {
            tables.delete_row(keyer::POOL_DAY_DATA, day_id).mark_final();
            return;
        }

        tables
            .update_row(
                keyer::POOL_DAY_DATA,
                pool_day_data_id(pool_address, day_id).as_str(),
            )
            .set(name, delta);
    }
}

pub fn liquidities_pool_day_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(keyer::POOL_DAY_DATA) {
            continue;
        }

        let day_id = delta.key.as_str().split(":").last().unwrap();
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap();
        tables
            .update_row(
                keyer::POOL_DAY_DATA,
                pool_day_data_id(pool_address, day_id).as_str(),
            )
            .set("liquidity", delta);
    }
}

pub fn sqrt_price_and_tick_pool_day_data_entity_change(
    tables: &mut Tables,
    deltas: &Deltas<DeltaProto<PoolSqrtPrice>>,
) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(keyer::POOL_DAY_DATA) {
            continue;
        }

        let day_id = delta.key.as_str().split(":").last().unwrap();
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap();
        let sqrt_price: BigInt =
            BigInt::from_str(delta.new_value.sqrt_price.as_ref().unwrap().value.as_str()).unwrap();
        let tick: BigInt =
            BigInt::from_str(delta.new_value.tick.as_ref().unwrap().value.as_str()).unwrap();

        tables
            .update_row(
                keyer::POOL_DAY_DATA,
                pool_day_data_id(pool_address, day_id).as_str(),
            )
            .set("sqrtPrice", sqrt_price)
            .set("tick", tick);
    }
}

pub fn token_prices_pool_day_data_entity_change(
    tables: &mut Tables,
    deltas: &Deltas<DeltaBigDecimal>,
) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(keyer::POOL_DAY_DATA) {
            continue;
        }

        let day_id = delta.key.as_str().split(":").last().unwrap();
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap();
        let name = match delta.key.as_str().split(":").nth(2).unwrap() {
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
            .update_row(
                keyer::POOL_DAY_DATA,
                pool_day_data_id(pool_address, day_id).as_str(),
            )
            .set(name, delta);

        if !high.eq(&BigDecimal::zero()) {
            tables
                .update_row(
                    keyer::POOL_DAY_DATA,
                    pool_day_data_id(pool_address, day_id).as_str(),
                )
                .set("high", high);
        }

        if !low.eq(&BigDecimal::zero()) {
            tables
                .update_row(
                    keyer::POOL_DAY_DATA,
                    pool_day_data_id(pool_address, day_id).as_str(),
                )
                .set("low", low);
        }
    }
}

pub fn fee_growth_global_x128_pool_day_data_entity_change(
    tables: &mut Tables,
    deltas: &Deltas<DeltaBigInt>,
) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(keyer::POOL_DAY_DATA) {
            continue;
        }

        let day_id = delta.key.as_str().split(":").last().unwrap();
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap();
        let name = match delta.key.as_str().split(":").nth(2).unwrap() {
            "token0" => "feeGrowthGlobal0X128",
            "token1" => "feeGrowthGlobal1X128",
            _ => continue,
        };

        tables
            .update_row(
                keyer::POOL_DAY_DATA,
                pool_day_data_id(pool_address, day_id).as_str(),
            )
            .set(name, delta);
    }
}

pub fn total_value_locked_usd_pool_day_data_entity_change(
    tables: &mut Tables,
    deltas: &Deltas<DeltaBigDecimal>,
) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(keyer::POOL_DAY_DATA) {
            continue;
        }

        let day_id = delta.key.as_str().split(":").last().unwrap();
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap();

        tables
            .update_row(
                keyer::POOL_DAY_DATA,
                pool_day_data_id(pool_address, day_id).as_str(),
            )
            .set("totalValueLockedUSD", delta);
    }
}

fn pool_day_data_id(pool_address: &str, day_id: &str) -> String {
    format!("{}-{}", pool_address, day_id)
}

fn create_pool_day_data(
    tables: &mut Tables,
    pool_day_data_id: &String,
    day_start_timestamp: i32,
    pool_addr: &str,
    delta: &DeltaBigInt,
) {
    tables
        .update_row(keyer::POOL_DAY_DATA, pool_day_data_id)
        .set("id", pool_day_data_id)
        .set("date", day_start_timestamp)
        .set("pool", pool_addr.to_string())
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
        .set("txCount", delta)
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
            return;
        }

        let hour_id: i64 = delta
            .key
            .as_str()
            .split(":")
            .last()
            .unwrap()
            .parse::<i64>()
            .unwrap();

        log::info!("hour_id {}", hour_id);
        let hours_start_unix = hour_id * 3600;
        let pool_addr = delta.key.as_str().split(":").nth(1).unwrap();

        let pool_hour_data_id =
            pool_hour_data_id(pool_addr, hour_id.to_string().as_str()).to_string();

        create_pool_hour_data(
            tables,
            &pool_hour_data_id,
            hours_start_unix,
            pool_addr,
            &delta,
        );
    }
}

pub fn tx_count_pool_hour_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(POOL_HOUR_DATA) {
            continue;
        }

        let hour_id = delta.key.as_str().split(":").last().unwrap();
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap();

        tables
            .update_row(
                POOL_HOUR_DATA,
                pool_hour_data_id(pool_address, hour_id).as_str(),
            )
            .set("txCount", delta);
    }
}

pub fn liquidities_pool_hour_data_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaBigInt>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(POOL_HOUR_DATA) {
            continue;
        }

        let hour_id = delta.key.as_str().split(":").last().unwrap();
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap();

        tables
            .update_row(
                POOL_HOUR_DATA,
                pool_hour_data_id(pool_address, hour_id).as_str(),
            )
            .set("liquidity", delta);
    }
}

pub fn sqrt_price_and_tick_pool_hour_data_entity_change(
    tables: &mut Tables,
    deltas: &Deltas<DeltaProto<PoolSqrtPrice>>,
) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(POOL_HOUR_DATA) {
            continue;
        }

        let hour_id = delta.key.as_str().split(":").last().unwrap();
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap();
        let sqrt_price: BigInt =
            BigInt::from_str(delta.new_value.sqrt_price.as_ref().unwrap().value.as_str()).unwrap();
        let tick: BigInt =
            BigInt::from_str(delta.new_value.tick.as_ref().unwrap().value.as_str()).unwrap();

        tables
            .update_row(
                POOL_HOUR_DATA,
                pool_hour_data_id(pool_address, hour_id).as_str(),
            )
            .set("sqrtPrice", sqrt_price)
            .set("tick", tick);
    }
}

pub fn swap_volume_pool_hour_data_entity_change(
    tables: &mut Tables,
    deltas: &Deltas<DeltaBigDecimal>,
) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(POOL_HOUR_DATA) {
            continue;
        }

        let hour_id = delta.key.as_str().split(":").nth(2).unwrap();
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap();

        let name = match delta.key.as_str().split(":").last().unwrap() {
            "volumeToken0" => "volumeToken0", // TODO: validate data
            "volumeToken1" => "volumeToken1", // TODO: validate data
            "volumeUSD" => "volumeUSD",       // TODO: validate data
            "feesUSD" => "feesUSD",           // TODO: validate data
            _ => continue,
        };

        if delta.operation == store_delta::Operation::Delete {
            tables.delete_row(POOL_HOUR_DATA, hour_id).mark_final();
            return;
        }

        tables
            .update_row(
                POOL_HOUR_DATA,
                pool_hour_data_id(pool_address, hour_id).as_str(),
            )
            .set(name, delta);
    }
}

pub fn token_prices_pool_hour_data_entity_change(
    tables: &mut Tables,
    deltas: &Deltas<DeltaBigDecimal>,
) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(keyer::POOL_HOUR_DATA) {
            continue;
        }

        let hour_id = delta.key.as_str().split(":").last().unwrap();
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap();
        let name = match delta.key.as_str().split(":").nth(2).unwrap() {
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

        let pool_hour_id = pool_hour_data_id(pool_address, hour_id);

        tables
            .update_row(POOL_HOUR_DATA, &pool_hour_id)
            .set(name, delta);

        if !high.eq(&BigDecimal::zero()) {
            tables
                .update_row(POOL_HOUR_DATA, &pool_hour_id)
                .set("high", high);
        }

        if !low.eq(&BigDecimal::zero()) {
            tables
                .update_row(POOL_HOUR_DATA, &pool_hour_id)
                .set("low", low);
        }
    }
}

pub fn fee_growth_global_x128_pool_hour_data_entity_change(
    tables: &mut Tables,
    deltas: &Deltas<DeltaBigInt>,
) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(POOL_HOUR_DATA) {
            continue;
        }

        let hour_id = delta.key.as_str().split(":").last().unwrap();
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap();
        let name = match delta.key.as_str().split(":").nth(2).unwrap() {
            "token0" => "feeGrowthGlobal0X128",
            "token1" => "feeGrowthGlobal1X128",
            _ => continue,
        };

        tables
            .update_row(
                POOL_HOUR_DATA,
                pool_hour_data_id(pool_address, hour_id).as_str(),
            )
            .set(name, delta);
    }
}

pub fn total_value_locked_usd_pool_hour_data_entity_change(
    tables: &mut Tables,
    deltas: &Deltas<DeltaBigDecimal>,
) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(POOL_HOUR_DATA) {
            continue;
        }

        let hour_id = delta.key.as_str().split(":").last().unwrap();
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap();

        tables
            .update_row(
                POOL_HOUR_DATA,
                pool_day_data_id(pool_address, hour_id).as_str(),
            )
            .set("totalValueLockedUSD", delta);
    }
}

fn pool_hour_data_id(pool_addr: &str, hour_id: &str) -> String {
    format!("{}-{}", pool_addr, hour_id)
}

fn create_pool_hour_data(
    tables: &mut Tables,
    pool_day_data_id: &String,
    hours_start_unix: i64,
    pool_addr: &str,
    delta: &DeltaBigInt,
) {
    tables
        .update_row(POOL_HOUR_DATA, pool_day_data_id)
        .set("id", pool_day_data_id)
        .set("periodStartUnix", hours_start_unix.to_string())
        .set("pool", pool_addr.to_string())
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
        .set("txCount", delta)
        .set("open", BigDecimal::zero())
        .set("high", BigDecimal::zero())
        .set("low", BigDecimal::zero())
        .set("close", BigDecimal::zero());
}

// --------------------
//  Map Token Day Data Entities
// --------------------
pub fn token_hour_data_create_entity_change(tables: &mut Tables, deltas: &Deltas<DeltaInt64>) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(TOKEN_DAY_DATA) {
            continue;
        }

        if !delta.new_value.eq(&(1 as i64)) {
            return;
        }

        let day_id: i64 = delta
            .key
            .as_str()
            .split(":")
            .last()
            .unwrap()
            .parse::<i64>()
            .unwrap();
        let day_start_timestamp = (day_id * 86400) as i32;
        let token_addr = delta.key.as_str().split(":").nth(1).unwrap();

        let token_day_data_id =
            token_day_data_id(token_addr, day_id.to_string().as_str()).to_string();

        create_token_day_data(tables, &token_day_data_id, day_start_timestamp, token_addr);
    }
}

pub fn swap_volume_token_day_data_entity_change(
    tables: &mut Tables,
    deltas: &Deltas<DeltaBigDecimal>,
) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(TOKEN_DAY_DATA) {
            continue;
        }

        let day_id = delta.key.as_str().split(":").nth(2).unwrap();
        let token_addr = delta.key.as_str().split(":").nth(1).unwrap();

        //TODO: need to add the :volume key
        let name = match delta.key.as_str().split(":").last().unwrap() {
            "volumeToken0" => "volumeToken0", // TODO: validate data
            "volumeToken1" => "volumeToken1", // TODO: validate data
            "volumeUSD" => "volumeUSD",       // TODO: validate data
            "feesUSD" => "feesUSD",           // TODO: validate data
            _ => continue,
        };

        if delta.operation == store_delta::Operation::Delete {
            tables.delete_row(TOKEN_DAY_DATA, day_id).mark_final();
            return;
        }

        tables
            .update_row(
                TOKEN_DAY_DATA,
                token_day_data_id(token_addr, day_id).as_str(),
            )
            .set(name, delta);
    }
}

pub fn total_value_locked_usd_token_day_data_entity_change(
    tables: &mut Tables,
    deltas: &Deltas<DeltaBigDecimal>,
) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(TOKEN_DAY_DATA) {
            continue;
        }

        let day_id = delta.key.as_str().split(":").last().unwrap();
        let token_address = delta.key.as_str().split(":").nth(1).unwrap();

        tables
            .update_row(
                TOKEN_DAY_DATA,
                pool_day_data_id(token_address, day_id).as_str(),
            )
            .set("totalValueLockedUSD", delta);
    }
}

pub fn total_value_locked_token_day_data_entity_change(
    tables: &mut Tables,
    deltas: &Deltas<DeltaBigDecimal>,
) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(TOKEN_DAY_DATA) {
            continue;
        }

        let day_id = delta.key.as_str().split(":").last().unwrap();
        let token_address = delta.key.as_str().split(":").nth(1).unwrap();

        tables
            .update_row(
                TOKEN_DAY_DATA,
                pool_day_data_id(token_address, day_id).as_str(),
            )
            .set("totalValueLocked", delta);
    }
}

pub fn token_prices_token_day_data_entity_change(
    tables: &mut Tables,
    deltas: &Deltas<DeltaBigDecimal>,
) {
    for delta in deltas.deltas.iter() {
        if !delta.key.starts_with(TOKEN_DAY_DATA) {
            continue;
        }

        let day_id = delta.key.as_str().split(":").last().unwrap();
        let token_address = delta.key.as_str().split(":").nth(1).unwrap();

        tables
            .update_row(
                TOKEN_DAY_DATA,
                pool_day_data_id(token_address, day_id).as_str(),
            )
            .set("tokenPrice", delta);
    }
}

fn token_day_data_id(token_addr: &str, hour_id: &str) -> String {
    format!("{}-{}", token_addr, hour_id)
}

fn create_token_day_data(
    tables: &mut Tables,
    token_day_data_id: &String,
    day_start_timestamp: i32,
    token_addr: &str,
) {
    tables
        .update_row(TOKEN_DAY_DATA, token_day_data_id)
        .set("id", token_day_data_id)
        .set("date", day_start_timestamp)
        .set("token", token_addr.to_string())
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
