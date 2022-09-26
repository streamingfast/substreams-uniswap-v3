use crate::pb::entity::EntityChange;

use crate::uniswap::event::Type::Mint;
use crate::utils::decode_bytes_to_big_decimal;
use crate::{
    keyer, pb, utils, BurnEvent, EntityChanges, Erc20Token, Events, Flashes, MintEvent,
    PoolSqrtPrice, Pools, Positions, SnapshotPositions, SwapEvent, Tick, Transactions,
};
use bigdecimal::{BigDecimal, Zero};
use num_bigint::BigInt;
use pb::entity::entity_change::Operation;
use std::str::FromStr;
use substreams::store::{Deltas, StoreGet};
use substreams::{proto, Hex};

// -------------------
//  Map Bundle Entities
// -------------------
pub fn created_bundle_entity_change(entity_changes: &mut EntityChanges) {
    entity_changes
        .push_change("Bundle", "1".to_string(), 1, Operation::Create)
        .new_string_field_change("id", "1".to_string())
        .new_bigdecimal_field_change("ethPriceUSD", "0".to_string());
}

pub fn bundle_store_eth_price_usd_bundle_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas,
) {
    for delta in deltas {
        if !delta.key.starts_with("bundle") {
            continue;
        }

        entity_changes
            .push_change("Bundle", "1".to_string(), delta.ordinal, Operation::Update)
            .update_bigdecimal("ethPriceUSD", delta);
    }
}

// -------------------
//  Map Factory Entities
// -------------------
pub fn factory_created_factory_entity_change(entity_changes: &mut EntityChanges) {
    entity_changes
        .push_change(
            "Factory",
            Hex(utils::UNISWAP_V3_FACTORY).to_string(),
            1,
            Operation::Create,
        )
        .new_string_field_change("id", Hex(utils::UNISWAP_V3_FACTORY).to_string())
        .new_bigint_field_change("poolCount", BigInt::zero().to_string())
        .new_bigint_field_change("txCount", BigInt::zero().to_string())
        .new_bigdecimal_field_change("totalVolumeUSD", BigDecimal::zero().to_string())
        .new_bigdecimal_field_change("totalVolumeETH", BigDecimal::zero().to_string())
        .new_bigdecimal_field_change("totalFeesUSD", BigDecimal::zero().to_string())
        .new_bigdecimal_field_change("totalFeesETH", BigDecimal::zero().to_string())
        .new_bigdecimal_field_change("untrackedVolumeUSD", BigDecimal::zero().to_string())
        .new_bigdecimal_field_change("totalValueLockedUSD", BigDecimal::zero().to_string())
        .new_bigdecimal_field_change("totalValueLockedETH", BigDecimal::zero().to_string())
        .new_bigdecimal_field_change(
            "totalValueLockedUSDUntracked",
            BigDecimal::zero().to_string(),
        )
        .new_bigdecimal_field_change(
            "totalValueLockedETHUntracked",
            BigDecimal::zero().to_string(),
        )
        .new_string_field_change("owner", Hex(utils::ZERO_ADDRESS).to_string());
}

pub fn pool_created_factory_entity_change(entity_changes: &mut EntityChanges, deltas: Deltas) {
    for delta in deltas {
        entity_changes
            .push_change(
                "Factory",
                Hex(utils::UNISWAP_V3_FACTORY).to_string(),
                delta.ordinal,
                Operation::Update,
            )
            .update_bigint("poolCount", delta);
    }
}

pub fn tx_count_factory_entity_change(entity_changes: &mut EntityChanges, deltas: Deltas) {
    for delta in deltas {
        if !delta.key.starts_with("factory:") {
            continue;
        }
        entity_changes
            .push_change(
                "Factory",
                Hex(utils::UNISWAP_V3_FACTORY).to_string(),
                delta.ordinal,
                Operation::Update,
            )
            .update_bigint("txCount", delta);
    }
}

pub fn swap_volume_factory_entity_change(entity_changes: &mut EntityChanges, deltas: Deltas) {
    for delta in deltas {
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

        entity_changes
            .push_change(
                "Factory",
                Hex(utils::UNISWAP_V3_FACTORY).to_string(),
                delta.ordinal,
                Operation::Update,
            )
            .update_bigdecimal(name, delta);
    }
}

pub fn total_value_locked_factory_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas,
) {
    for delta in deltas {
        if !delta.key.starts_with("factory:") {
            continue;
        }
        let name = match delta.key.as_str().split(":").last().unwrap() {
            "totalValueLockedUSD" => "totalValueLockedUSD",
            "totalValueLockedETH" => "totalValueLockedETH",
            _ => continue,
        };

        entity_changes
            .push_change(
                "Factory",
                Hex(utils::UNISWAP_V3_FACTORY).to_string(),
                delta.ordinal,
                Operation::Update,
            )
            .update_bigdecimal(name, delta);
    }
}

// -------------------
//  Map Pool Entities
// -------------------
pub fn pools_created_pool_entity_change(pools: Pools, entity_changes: &mut EntityChanges) {
    for pool in pools.pools {
        entity_changes
            .push_change(
                "Pool",
                pool.address.clone(),
                pool.log_ordinal,
                Operation::Create,
            )
            .new_string_field_change("id", pool.address)
            .new_bigint_field_change("createdAtTimestamp", pool.created_at_timestamp)
            .new_bigint_field_change("createdAtBlockNumber", pool.created_at_block_number)
            .new_string_field_change("token0", pool.token0.unwrap().address)
            .new_string_field_change("token1", pool.token1.unwrap().address)
            .new_bigint_field_change("feeTier", pool.fee_tier.to_string())
            .new_bigint_field_change("liquidity", BigInt::zero().to_string())
            .new_bigint_field_change("sqrtPrice", BigInt::zero().to_string())
            .new_bigint_field_change("feeGrowthGlobal0X128", BigInt::zero().to_string())
            .new_bigint_field_change("feeGrowthGlobal1X128", BigInt::zero().to_string())
            .new_bigdecimal_field_change("token0Price", BigDecimal::zero().to_string())
            .new_bigdecimal_field_change("token1Price", BigDecimal::zero().to_string())
            .new_bigint_field_change("tick", BigInt::zero().to_string())
            .new_bigint_field_change("observationIndex", BigInt::zero().to_string())
            .new_bigdecimal_field_change("volumeToken0", BigDecimal::zero().to_string())
            .new_bigdecimal_field_change("volumeToken1", BigDecimal::zero().to_string())
            .new_bigdecimal_field_change("volumeUSD", BigDecimal::zero().to_string())
            .new_bigdecimal_field_change("untrackedVolumeUSD", BigDecimal::zero().to_string())
            .new_bigdecimal_field_change("feesUSD", BigDecimal::zero().to_string())
            .new_bigint_field_change("txCount", BigInt::zero().to_string())
            .new_bigdecimal_field_change("collectedFeesToken0", BigDecimal::zero().to_string())
            .new_bigdecimal_field_change("collectedFeesToken1", BigDecimal::zero().to_string())
            .new_bigdecimal_field_change("collectedFeesUSD", BigDecimal::zero().to_string())
            .new_bigdecimal_field_change("totalValueLockedToken0", BigDecimal::zero().to_string())
            .new_bigdecimal_field_change("totalValueLockedToken1", BigDecimal::zero().to_string())
            .new_bigdecimal_field_change("totalValueLockedETH", BigDecimal::zero().to_string())
            .new_bigdecimal_field_change("totalValueLockedUSD", BigDecimal::zero().to_string())
            .new_bigdecimal_field_change(
                "totalValueLockedUSDUntracked",
                BigDecimal::zero().to_string(),
            )
            .new_bigint_field_change("liquidityProviderCount", BigInt::zero().to_string());
    }
}

pub fn pool_sqrt_price_entity_change(entity_changes: &mut EntityChanges, deltas: Deltas) {
    for delta in deltas {
        let new_value: PoolSqrtPrice = proto::decode(&delta.new_value).unwrap();
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();

        let mut entity_change = EntityChange {
            entity: "Pool".to_string(),
            id: pool_address,
            ordinal: delta.ordinal,
            operation: Operation::Update as i32,
            fields: vec![],
        };

        match delta.operation {
            1 => {
                entity_change
                    .new_bigint_field_change("sqrtPrice", new_value.sqrt_price)
                    .new_bigint_field_change("tick", new_value.tick);
            }
            2 => {
                let old_value: PoolSqrtPrice = proto::decode(&delta.new_value).unwrap();
                entity_change
                    .update_bigint_from_values(
                        "sqrtPrice",
                        old_value.sqrt_price,
                        new_value.sqrt_price,
                    )
                    .update_bigint_from_values("tick", old_value.tick, new_value.tick);
            }
            _ => continue,
        }

        entity_changes.entity_changes.push(entity_change);
    }
}

pub fn pool_liquidities_pool_entity_change(entity_changes: &mut EntityChanges, deltas: Deltas) {
    for delta in deltas {
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        entity_changes
            .push_change("Pool", pool_address, delta.ordinal, Operation::Update)
            .update_bigint("liquidity", delta);
    }
}

pub fn total_value_locked_pool_entity_change(entity_changes: &mut EntityChanges, deltas: Deltas) {
    for delta in deltas {
        if !delta.key.starts_with("pool:") {
            continue;
        }
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();

        let name = match delta.key.as_str().split(":").last().unwrap() {
            "usd" => "totalValueLockedUSD",
            "eth" => "totalValueLockedETH",
            _ => continue,
        };

        entity_changes
            .push_change("Pool", pool_address, delta.ordinal, Operation::Update)
            .update_bigdecimal(name, delta);
    }
}

pub fn total_value_locked_by_token_pool_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas,
) {
    for delta in deltas {
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        let name = match delta.key.as_str().split(":").last().unwrap() {
            "token0" => "totalValueLockedToken0",
            "token1" => "totalValueLockedToken1",
            _ => continue,
        };

        entity_changes
            .push_change("Pool", pool_address, delta.ordinal, Operation::Update)
            .update_bigdecimal(name, delta);
    }
}

pub fn pool_fee_growth_global_x128_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas,
) {
    for delta in deltas {
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        let name = match delta.key.as_str().split(":").last().unwrap() {
            "token0" => "feeGrowthGlobal0X128",
            "token1" => "feeGrowthGlobal1X128",
            _ => continue,
        };

        entity_changes
            .push_change("Pool", pool_address, delta.ordinal, Operation::Update)
            .update_bigint(name, delta);
    }
}

pub fn price_pool_entity_change(entity_changes: &mut EntityChanges, deltas: Deltas) {
    for delta in deltas {
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

        entity_changes
            .push_change("Pool", pool_address, delta.ordinal, Operation::Update)
            .update_bigdecimal(name, delta);
    }
}

pub fn tx_count_pool_entity_change(entity_changes: &mut EntityChanges, deltas: Deltas) {
    for delta in deltas {
        if !delta.key.starts_with("pool:") {
            continue;
        }

        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        entity_changes
            .push_change("Pool", pool_address, delta.ordinal, Operation::Update)
            .update_bigint("txCount", delta);
    }
}

pub fn swap_volume_pool_entity_change(entity_changes: &mut EntityChanges, deltas: Deltas) {
    for delta in deltas {
        if !delta.key.as_str().starts_with("swap") {
            continue;
        }

        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();

        let name = match delta.key.as_str().split(":").last().unwrap() {
            "token0" => "volumeToken0",
            "token1" => "volumeToken1",
            "usd" => "volumeUSD",
            "untrackedUSD" => "untrackedVolumeUSD",
            "feesUSD" => "feesUSD",
            _ => continue,
        };

        entity_changes
            .push_change("Pool", pool_address, delta.ordinal, Operation::Update)
            .update_bigdecimal(name, delta);
    }
}

// --------------------
//  Map Token Entities
// --------------------
pub fn tokens_created_token_entity_change(entity_changes: &mut EntityChanges, pools: Pools) {
    for pool in pools.pools {
        let token0: &Erc20Token = pool.token0.as_ref().unwrap();
        let token1: &Erc20Token = pool.token1.as_ref().unwrap();

        add_token_entity_change(entity_changes, token0, pool.log_ordinal);
        add_token_entity_change(entity_changes, token1, pool.log_ordinal);
    }
}

pub fn swap_volume_token_entity_change(entity_changes: &mut EntityChanges, deltas: Deltas) {
    for delta in deltas {
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

        entity_changes
            .push_change("Token", token_address, delta.ordinal, Operation::Update)
            .update_bigdecimal(name, delta);
    }
}

pub fn tx_count_token_entity_change(entity_changes: &mut EntityChanges, deltas: Deltas) {
    for delta in deltas {
        if !delta.key.starts_with("token:") {
            continue;
        }

        let token_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        entity_changes
            .push_change("Token", token_address, delta.ordinal, Operation::Update)
            .update_bigint("txCount", delta);
    }
}

pub fn total_value_locked_by_token_token_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas,
) {
    for delta in deltas {
        let token_address = delta.key.as_str().split(":").nth(2).unwrap().to_string();

        entity_changes
            .push_change("Token", token_address, delta.ordinal, Operation::Update)
            .update_bigdecimal("totalValueLocked", delta);
    }
}

pub fn total_value_locked_usd_token_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas,
) {
    for delta in deltas {
        if !delta.key.starts_with("token:") {
            continue;
        }

        let token_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();

        let name: &str = match delta.key.as_str().split(":").last().unwrap() {
            "usd" => "totalValueLockedUSD",
            _ => continue,
        };

        entity_changes
            .push_change("Token", token_address, delta.ordinal, Operation::Update)
            .update_bigdecimal(name, delta);
    }
}

pub fn derived_eth_prices_token_entity_change(entity_changes: &mut EntityChanges, deltas: Deltas) {
    for delta in deltas {
        if !delta.key.starts_with("token:") {
            continue;
        }

        let token_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        let name: &str = match delta.key.as_str().split(":").last().unwrap() {
            "eth" => "derivedETH",
            _ => continue,
        };

        entity_changes
            .push_change("Token", token_address, delta.ordinal, Operation::Update)
            .update_bigdecimal(name, delta);
    }
}

fn add_token_entity_change(
    entity_changes: &mut EntityChanges,
    token: &Erc20Token,
    log_ordinal: u64,
) {
    entity_changes
        .push_change(
            "Token",
            token.address.clone(),
            log_ordinal,
            Operation::Create,
        )
        .new_string_field_change("id", token.address.clone())
        .new_string_field_change("symbol", token.symbol.clone())
        .new_string_field_change("name", token.name.clone())
        .new_bigint_field_change("decimals", token.decimals.to_string())
        .new_bigint_field_change("totalSupply", token.total_supply.clone())
        .new_bigdecimal_field_change("volume", BigDecimal::zero().to_string())
        .new_bigdecimal_field_change("volumeUSD", BigDecimal::zero().to_string())
        .new_bigdecimal_field_change("untrackedVolumeUSD", BigDecimal::zero().to_string())
        .new_bigdecimal_field_change("feesUSD", BigDecimal::zero().to_string())
        .new_bigint_field_change("txCount", BigInt::zero().to_string())
        .new_bigint_field_change("poolCount", BigInt::zero().to_string())
        .new_bigdecimal_field_change("totalValueLocked", BigDecimal::zero().to_string())
        .new_bigdecimal_field_change("totalValueLockedUSD", BigDecimal::zero().to_string())
        .new_bigdecimal_field_change(
            "totalValueLockedUSDUntracked",
            BigDecimal::zero().to_string(),
        )
        .new_bigdecimal_field_change("derivedETH", BigDecimal::zero().to_string());
}

// --------------------
//  Map Tick Entities
// --------------------
pub fn create_or_update_ticks_entity_change(entity_changes: &mut EntityChanges, deltas: Deltas) {
    for delta in deltas {
        let new_tick: Tick = proto::decode(&delta.new_value).unwrap();

        if delta.old_value.len() == 0 {
            if new_tick.origin == Mint as i32 {
                create_tick_entity_change(entity_changes, new_tick)
            }
        } else {
            let old_tick: Tick = proto::decode(&delta.old_value).unwrap();
            update_tick_entity_change(entity_changes, old_tick, new_tick);
        }
    }
}

pub fn ticks_liquidities_tick_entity_change(entity_changes: &mut EntityChanges, deltas: Deltas) {
    for delta in deltas {
        let tick_id = delta.key.as_str().split(":").nth(1).unwrap().to_string();

        let name = match delta.key.as_str().split(":").last().unwrap() {
            "liquidityNet" => "liquidityNet",
            "liquidityGross" => "liquidityGross",
            _ => continue,
        };

        entity_changes
            .push_change("Tick", tick_id, delta.ordinal, Operation::Update)
            .update_bigint(name, delta);
    }
}

fn create_tick_entity_change(entity_changes: &mut EntityChanges, tick: Tick) {
    entity_changes
        .push_change("Tick", tick.id.clone(), tick.log_ordinal, Operation::Create)
        .new_string_field_change("id", tick.id)
        .new_string_field_change("poolAddress", tick.pool_address.clone())
        .new_bigint_field_change("tickIdx", tick.idx)
        .new_string_field_change("pool", tick.pool_address)
        .new_bigint_field_change("liquidityGross", BigInt::zero().to_string())
        .new_bigint_field_change("liquidityNet", BigInt::zero().to_string())
        .new_bigdecimal_field_change("price0", tick.price0)
        .new_bigdecimal_field_change("price1", tick.price1)
        .new_bigdecimal_field_change("volumeToken0", BigDecimal::zero().to_string())
        .new_bigdecimal_field_change("volumeToken1", BigDecimal::zero().to_string())
        .new_bigdecimal_field_change("volumeUSD", BigDecimal::zero().to_string())
        .new_bigdecimal_field_change("untrackedVolumeUSD", BigDecimal::zero().to_string())
        .new_bigdecimal_field_change("feesUSD", BigDecimal::zero().to_string())
        .new_bigdecimal_field_change("collectedFeesToken0", BigDecimal::zero().to_string())
        .new_bigdecimal_field_change("collectedFeesToken1", BigDecimal::zero().to_string())
        .new_bigdecimal_field_change("collectedFeesUSD", BigDecimal::zero().to_string())
        .new_bigint_field_change("createdAtTimestamp", tick.created_at_timestamp.to_string())
        .new_bigint_field_change(
            "createdAtBlockNumber",
            tick.created_at_block_number.to_string(),
        )
        .new_bigint_field_change(
            "liquidityProviderCount",
            tick.created_at_block_number.to_string(),
        )
        .new_bigint_field_change(
            "feeGrowthOutside0X128",
            tick.created_at_block_number.to_string(),
        )
        .new_bigint_field_change(
            "feeGrowthOutside1X128",
            tick.created_at_block_number.to_string(),
        );
}

fn update_tick_entity_change(entity_changes: &mut EntityChanges, old_tick: Tick, new_tick: Tick) {
    entity_changes
        .push_change("Tick", new_tick.id, new_tick.log_ordinal, Operation::Update)
        .update_bigint_from_values(
            "feeGrowthOutside0X128",
            old_tick.fee_growth_outside_0x_128,
            new_tick.fee_growth_outside_0x_128,
        )
        .update_bigint_from_values(
            "feeGrowthOutside1X128",
            old_tick.fee_growth_outside_1x_128,
            new_tick.fee_growth_outside_1x_128,
        );
}

// --------------------
//  Map Position Entities
// --------------------
pub fn position_create_entity_change(positions: Positions, entity_changes: &mut EntityChanges) {
    for position in positions.positions {
        entity_changes
            .push_change(
                "Positions",
                position.id.clone(),
                position.log_ordinal,
                Operation::Create,
            )
            .new_string_field_change("id", position.id)
            .new_string_field_change("owner", position.owner)
            .new_string_field_change("pool", position.pool)
            .new_string_field_change("token0", position.token0)
            .new_string_field_change("token1", position.token1)
            .new_string_field_change("tickLower", position.tick_lower)
            .new_string_field_change("tickUpper", position.tick_upper)
            .new_bigint_field_change("liquidity", BigInt::zero().to_string())
            .new_bigdecimal_field_change("depositedToken0", BigDecimal::zero().to_string())
            .new_bigdecimal_field_change("depositedToken1", BigDecimal::zero().to_string())
            .new_bigdecimal_field_change("withdrawnToken0", BigDecimal::zero().to_string())
            .new_bigdecimal_field_change("withdrawnToken1", BigDecimal::zero().to_string())
            .new_bigdecimal_field_change("collectedFeesToken0", BigDecimal::zero().to_string())
            .new_bigdecimal_field_change("collectedFeesToken1", BigDecimal::zero().to_string())
            .new_string_field_change("transaction", position.transaction)
            .new_bigint_field_change(
                "feeGrowthInside0LastX128",
                position.fee_growth_inside_0_last_x_128,
            )
            .new_bigint_field_change(
                "feeGrowthInside1LastX128",
                position.fee_growth_inside_1_last_x_128,
            );
    }
}

pub fn positions_changes_entity_change(entity_changes: &mut EntityChanges, deltas: Deltas) {
    for delta in deltas {
        let position_id = delta.key.as_str().split(":").nth(1).unwrap().to_string();

        let mut entity_change =
            EntityChange::new("Position", position_id, delta.ordinal, Operation::Update);

        let name = match delta.key.as_str().split(":").last().unwrap() {
            "liquidity" => {
                entity_change.update_bigint("liquidity", delta);
                continue;
            }
            "depositedToken0" => "depositedToken0",
            "depositedToken1" => "depositedToken1",
            "withdrawnToken0" => "withdrawnToken0",
            "withdrawnToken1" => "withdrawnToken1",
            "collectedFeesToken0" => "collectedFeesToken0",
            "collectedFeesToken1" => "collectedFeesToken1",
            _ => continue,
        };

        entity_changes
            .entity_changes
            .push(entity_change.update_bigdecimal(name, delta).to_owned());
    }
}

// --------------------
//  Map Snapshot Position Entities
// --------------------
pub fn snapshot_position_entity_change(
    snapshot_positions: SnapshotPositions,
    entity_changes: &mut EntityChanges,
) {
    for snapshot_position in snapshot_positions.snapshot_positions {
        entity_changes
            .push_change(
                "PositionSnapshot",
                snapshot_position.id.clone(),
                snapshot_position.log_ordinal,
                Operation::Create,
            )
            .new_string_field_change("id", snapshot_position.id)
            .new_string_field_change("owner", snapshot_position.owner)
            .new_string_field_change("pool", snapshot_position.pool)
            .new_string_field_change("position", snapshot_position.position)
            .new_bigint_field_change("blockNumber", snapshot_position.block_number.to_string())
            .new_bigint_field_change("timestamp", snapshot_position.timestamp.to_string())
            .new_bigint_field_change("liquidity", snapshot_position.liquidity)
            .new_bigdecimal_field_change("depositedToken0", snapshot_position.deposited_token0)
            .new_bigdecimal_field_change("depositedToken1", snapshot_position.deposited_token1)
            .new_bigdecimal_field_change("withdrawnToken0", snapshot_position.withdrawn_token0)
            .new_bigdecimal_field_change("withdrawnToken1", snapshot_position.withdrawn_token1)
            .new_bigdecimal_field_change(
                "collectedFeesToken0",
                snapshot_position.collected_fees_token0,
            )
            .new_bigdecimal_field_change(
                "collectedFeesToken1",
                snapshot_position.collected_fees_token1,
            )
            .new_string_field_change("transaction", snapshot_position.transaction)
            .new_bigint_field_change(
                "feeGrowthInside0LastX128",
                snapshot_position.fee_growth_inside_0_last_x_128,
            )
            .new_bigint_field_change(
                "feeGrowthInside1LastX128",
                snapshot_position.fee_growth_inside_1_last_x_128,
            );
    }
}

// --------------------
//  Map Transaction Entities
// --------------------
pub fn transaction_entity_change(transactions: Transactions, entity_changes: &mut EntityChanges) {
    for transaction in transactions.transactions {
        entity_changes
            .push_change(
                "Transaction",
                transaction.id.clone(),
                transaction.log_ordinal,
                Operation::Create,
            )
            .new_string_field_change("id", transaction.id)
            .new_bigint_field_change("blockNumber", transaction.block_number.to_string())
            .new_bigint_field_change("timestamp", transaction.timestamp.to_string())
            .new_bigint_field_change("gasUsed", transaction.gas_used.to_string())
            .new_bigint_field_change("gasPrice", transaction.gas_price);
    }
}

// --------------------
//  Map Swaps Mints Burns Entities
// --------------------
pub fn swaps_mints_burns_created_entity_change(
    events: Events,
    tx_count_store: StoreGet,
    store_eth_prices: StoreGet,
    entity_changes: &mut EntityChanges,
) {
    for event in events.events {
        if event.r#type.is_none() {
            continue;
        }

        if event.r#type.is_some() {
            let transaction_count: i32 =
                match tx_count_store.get_last(keyer::factory_total_tx_count()) {
                    Some(data) => String::from_utf8_lossy(data.as_slice())
                        .to_string()
                        .parse::<i32>()
                        .unwrap(),
                    None => 0,
                };

            let transaction_id: String = format!("{}#{}", event.transaction_id, transaction_count);

            let token0_derived_eth_price =
                match store_eth_prices.get_last(keyer::token_eth_price(&event.token0)) {
                    None => {
                        // initializePool has occurred beforehand so there should always be a price
                        // maybe just ? instead of returning 1 and bubble up the error if there is one
                        BigDecimal::from(0 as u64)
                    }
                    Some(derived_eth_price_bytes) => {
                        decode_bytes_to_big_decimal(derived_eth_price_bytes)
                    }
                };

            let token1_derived_eth_price: BigDecimal =
                match store_eth_prices.get_last(keyer::token_eth_price(&event.token1)) {
                    None => {
                        // initializePool has occurred beforehand so there should always be a price
                        // maybe just ? instead of returning 1 and bubble up the error if there is one
                        BigDecimal::from(0 as u64)
                    }
                    Some(derived_eth_price_bytes) => {
                        decode_bytes_to_big_decimal(derived_eth_price_bytes)
                    }
                };

            let bundle_eth_price: BigDecimal = match store_eth_prices
                .get_last(keyer::bundle_eth_price())
            {
                None => {
                    // initializePool has occurred beforehand so there should always be a price
                    // maybe just ? instead of returning 1 and bubble up the error if there is one
                    BigDecimal::from(1 as u64)
                }
                Some(bundle_eth_price_bytes) => decode_bytes_to_big_decimal(bundle_eth_price_bytes),
            };

            return match event.r#type.unwrap() {
                SwapEvent(swap) => {
                    let amount0: BigDecimal = BigDecimal::from_str(swap.amount_0.as_str()).unwrap();
                    let amount1: BigDecimal = BigDecimal::from_str(swap.amount_1.as_str()).unwrap();

                    let amount_usd: BigDecimal = utils::calculate_amount_usd(
                        &amount0,
                        &amount1,
                        &token0_derived_eth_price,
                        &token1_derived_eth_price,
                        &bundle_eth_price,
                    );

                    entity_changes
                        .push_change(
                            "Swap",
                            transaction_id.clone(),
                            event.log_ordinal,
                            Operation::Create,
                        )
                        .new_string_field_change("id", transaction_id)
                        .new_string_field_change("transaction", event.transaction_id)
                        .new_bigint_field_change("timestamp", event.timestamp.to_string())
                        .new_string_field_change("pool", event.pool_address)
                        .new_string_field_change("token0", event.token0)
                        .new_string_field_change("token0", event.token1)
                        .new_string_field_change("sender", swap.sender) // should this be bytes ?
                        .new_string_field_change("recipient", swap.recipient) // should this be bytes ?
                        .new_string_field_change("origin", swap.origin) // should this be bytes ?
                        .new_bigdecimal_field_change("amount0", swap.amount_0)
                        .new_bigdecimal_field_change("amount1", swap.amount_1)
                        .new_bigdecimal_field_change("amountUSD", amount_usd.to_string())
                        .new_bigint_field_change("sqrtPriceX96", swap.sqrt_price)
                        .new_bigint_field_change("tick", swap.tick.to_string())
                        .new_bigint_field_change("logIndex", event.log_ordinal.to_string());
                    // not sure if log index is good
                }
                MintEvent(mint) => {
                    let amount0: BigDecimal = BigDecimal::from_str(mint.amount_0.as_str()).unwrap();
                    let amount1: BigDecimal = BigDecimal::from_str(mint.amount_1.as_str()).unwrap();

                    let amount_usd: BigDecimal = utils::calculate_amount_usd(
                        &amount0,
                        &amount1,
                        &token0_derived_eth_price,
                        &token1_derived_eth_price,
                        &bundle_eth_price,
                    );

                    entity_changes
                        .push_change(
                            "Mint",
                            transaction_id.clone(),
                            event.log_ordinal,
                            Operation::Create,
                        )
                        .new_string_field_change("id", transaction_id)
                        .new_string_field_change("transaction", event.transaction_id)
                        .new_bigint_field_change("timestamp", event.timestamp.to_string())
                        .new_string_field_change("pool", event.pool_address)
                        .new_string_field_change("token0", event.token0)
                        .new_string_field_change("token0", event.token1)
                        .new_string_field_change("owner", mint.owner) // should this be bytes ?
                        .new_string_field_change("sender", mint.sender) // should this be bytes ?
                        .new_string_field_change("origin", mint.origin) // should this be bytes ?
                        .new_bigint_field_change("amount", mint.amount)
                        .new_bigdecimal_field_change("amount0", mint.amount_0)
                        .new_bigdecimal_field_change("amount1", mint.amount_1)
                        .new_bigdecimal_field_change("amountUSD", amount_usd.to_string())
                        .new_bigint_field_change("tickLower", mint.tick_lower.to_string())
                        .new_bigint_field_change("tickUpper", mint.tick_upper.to_string())
                        .new_bigint_field_change("logIndex", event.log_ordinal.to_string());
                    // not sure if log index is good
                }
                BurnEvent(burn) => {
                    let amount0: BigDecimal = BigDecimal::from_str(burn.amount_0.as_str()).unwrap();
                    let amount1: BigDecimal = BigDecimal::from_str(burn.amount_1.as_str()).unwrap();

                    let amount_usd: BigDecimal = utils::calculate_amount_usd(
                        &amount0,
                        &amount1,
                        &token0_derived_eth_price,
                        &token1_derived_eth_price,
                        &bundle_eth_price,
                    );

                    entity_changes
                        .push_change(
                            "Burn",
                            transaction_id.clone(),
                            event.log_ordinal,
                            Operation::Create,
                        )
                        .new_string_field_change("id", transaction_id)
                        .new_string_field_change("transaction", event.transaction_id)
                        .new_bigint_field_change("timestamp", event.timestamp.to_string())
                        .new_string_field_change("pool", event.pool_address)
                        .new_string_field_change("token0", event.token0)
                        .new_string_field_change("token0", event.token1)
                        .new_string_field_change("owner", burn.owner) // should this be bytes ?
                        .new_string_field_change("origin", burn.origin) // should this be bytes ?
                        .new_bigint_field_change("amount", burn.amount)
                        .new_bigdecimal_field_change("amount0", burn.amount_0)
                        .new_bigdecimal_field_change("amount1", burn.amount_1)
                        .new_bigdecimal_field_change("amountUSD", amount_usd.to_string())
                        .new_bigint_field_change("tickLower", burn.tick_lower.to_string())
                        .new_bigint_field_change("tickUpper", burn.tick_upper.to_string())
                        .new_bigint_field_change("logIndex", event.log_ordinal.to_string());
                    // not sure if log index is good
                }
            };
        }
    }
}

// --------------------
//  Map Flashes Entities
// --------------------
pub fn flashes_update_pool_fee_entity_change(flashes: Flashes, entity_changes: &mut EntityChanges) {
    for flash in flashes.flashes {
        entity_changes
            .push_change(
                "Pool",
                flash.pool_address,
                flash.log_ordinal,
                Operation::Update,
            )
            .new_bigint_field_change("feeGrowthGlobal0X128", flash.fee_growth_global_0x_128)
            .new_bigint_field_change("feeGrowthGlobal1X128", flash.fee_growth_global_1x_128);
    }
}

// --------------------
//  Map Uniswap Day Data Entities
// --------------------
pub fn uniswap_day_data_tx_count_entity_change(entity_changes: &mut EntityChanges, deltas: Deltas) {
    for delta in deltas {
        if !delta.key.starts_with("uniswap_day_data") {
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
        let day_start_timestamp = (day_id * 86400) as i32;

        entity_changes
            .push_change(
                "UniswapDayData",
                day_id.to_string(),
                delta.ordinal,
                Operation::Update,
            )
            .new_string_field_change("id", day_id.to_string())
            .new_int32_field_change("date", day_start_timestamp)
            .new_bigdecimal_field_change("volumeUSDUntracked", BigDecimal::zero().to_string())
            .update_bigint("txCount", delta);
    }
}

pub fn uniswap_day_data_totals_entity_change(entity_changes: &mut EntityChanges, deltas: Deltas) {
    for delta in deltas {
        if !delta.key.starts_with("uniswap_day_data") {
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

        entity_changes
            .push_change(
                "UniswapDayData",
                day_id.to_string(),
                delta.ordinal,
                Operation::Update,
            )
            .update_bigdecimal("tvlUSD", delta);
    }
}

pub fn uniswap_day_data_volumes_entity_change(entity_changes: &mut EntityChanges, deltas: Deltas) {
    for delta in deltas {
        if !delta.key.starts_with("uniswap_day_data") {
            continue;
        }

        let day_id: i64 = delta
            .key
            .as_str()
            .split(":")
            .nth(1)
            .unwrap()
            .parse::<i64>()
            .unwrap();

        let name = match delta.key.as_str().split(":").last().unwrap() {
            "volumeETH" => "volumeETH",
            "volumeUSD" => "volumeUSD",
            "feesUSD" => "feesUSD",
            _ => continue,
        };

        entity_changes
            .push_change(
                "UniswapDayData",
                day_id.to_string(),
                delta.ordinal,
                Operation::Update,
            )
            .update_bigdecimal(name, delta);
    }
}
