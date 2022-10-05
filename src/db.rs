use crate::pb::entity::EntityChange;

use crate::pb::change::BigIntChange;
use crate::pb::helpers::convert_i32_to_operation;
use crate::uniswap::tick::Origin;
use crate::{
    keyer, pb, utils, BurnEvent, EntityChanges, Erc20Token, Events, Flashes, MintEvent,
    PoolSqrtPrice, Pools, Positions, SnapshotPositions, SwapEvent, Tick, Transactions,
};
use pb::entity::entity_change::Operation;
use std::str::FromStr;
use substreams::scalar::{BigDecimal, BigInt};
use substreams::store::{
    ArrayDelta, BigDecimalDelta, BigDecimalStoreGet, BigIntDelta, BigIntStoreGet, Deltas,
    ProtoDelta, StoreGet,
};
use substreams::Hex;

// -------------------
//  Map Bundle Entities
// -------------------
pub fn created_bundle_entity_change(entity_changes: &mut EntityChanges) {
    entity_changes
        .push_change("Bundle", "1".to_string(), 1, Operation::Create)
        // .change_string("id", "1".into());
        .change_bigdecimal("ethPriceUSD", BigDecimal::zero().into());
}

pub fn bundle_store_eth_price_usd_bundle_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<BigDecimalDelta>,
) {
    for delta in deltas.deltas {
        if !delta.key.starts_with("bundle") {
            continue;
        }

        entity_changes
            .push_change("Bundle", "1".to_string(), delta.ordinal, Operation::Update)
            .change_bigdecimal("ethPriceUSD", delta.into());
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
        .change_string("id", Hex(utils::UNISWAP_V3_FACTORY).into())
        .change_bigint("poolCount", BigInt::zero().into())
        .change_bigint("txCount", BigInt::zero().into())
        .change_bigdecimal("totalVolumeUSD", BigDecimal::zero().into())
        .change_bigdecimal("totalVolumeETH", BigDecimal::zero().into())
        .change_bigdecimal("totalFeesUSD", BigDecimal::zero().into())
        .change_bigdecimal("totalFeesETH", BigDecimal::zero().into())
        .change_bigdecimal("untrackedVolumeUSD", BigDecimal::zero().into())
        .change_bigdecimal("totalValueLockedUSD", BigDecimal::zero().into())
        .change_bigdecimal("totalValueLockedETH", BigDecimal::zero().into())
        .change_bigdecimal("totalValueLockedUSDUntracked", BigDecimal::zero().into())
        .change_bigdecimal("totalValueLockedETHUntracked", BigDecimal::zero().into())
        .change_string("owner", Hex(utils::ZERO_ADDRESS).into());
}

pub fn pool_created_factory_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<BigIntDelta>,
) {
    for delta in deltas.deltas {
        entity_changes
            .push_change(
                "Factory",
                Hex(utils::UNISWAP_V3_FACTORY).to_string(),
                delta.ordinal,
                Operation::Update,
            )
            .change_bigint("poolCount", delta.into());
    }
}

pub fn tx_count_factory_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<BigIntDelta>,
) {
    for delta in deltas.deltas {
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
            .change_bigint("txCount", delta.into());
    }
}

pub fn swap_volume_factory_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<BigDecimalDelta>,
) {
    for delta in deltas.deltas {
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
            .change_bigdecimal(name, delta.into());
    }
}

pub fn total_value_locked_factory_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<BigDecimalDelta>,
) {
    for delta in deltas.deltas {
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
            .change_bigdecimal(name, delta.into());
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
            .change_string("id", pool.address.into())
            .change_bigint("createdAtTimestamp", pool.created_at_timestamp.into())
            .change_bigint("createdAtBlockNumber", pool.created_at_block_number.into())
            .change_string("token0", pool.token0.unwrap().address.into())
            .change_string("token1", pool.token1.unwrap().address.into())
            .change_bigint("feeTier", pool.fee_tier.into())
            .change_bigint("liquidity", BigInt::zero().into())
            .change_bigint("sqrtPrice", BigInt::zero().into())
            .change_bigint("feeGrowthGlobal0X128", BigInt::zero().into())
            .change_bigint("feeGrowthGlobal1X128", BigInt::zero().into())
            .change_bigdecimal("token0Price", BigDecimal::zero().into())
            .change_bigdecimal("token1Price", BigDecimal::zero().into())
            .change_bigint("tick", BigInt::zero().into())
            .change_bigint("observationIndex", BigInt::zero().into())
            .change_bigdecimal("volumeToken0", BigDecimal::zero().into())
            .change_bigdecimal("volumeToken1", BigDecimal::zero().into())
            .change_bigdecimal("volumeUSD", BigDecimal::zero().into())
            .change_bigdecimal("untrackedVolumeUSD", BigDecimal::zero().into())
            .change_bigdecimal("feesUSD", BigDecimal::zero().into())
            .change_bigint("txCount", BigInt::zero().into())
            .change_bigdecimal("collectedFeesToken0", BigDecimal::zero().into())
            .change_bigdecimal("collectedFeesToken1", BigDecimal::zero().into())
            .change_bigdecimal("collectedFeesUSD", BigDecimal::zero().into())
            .change_bigdecimal("totalValueLockedToken0", BigDecimal::zero().into())
            .change_bigdecimal("totalValueLockedToken1", BigDecimal::zero().into())
            .change_bigdecimal("totalValueLockedETH", BigDecimal::zero().into())
            .change_bigdecimal("totalValueLockedUSD", BigDecimal::zero().into())
            .change_bigdecimal("totalValueLockedUSDUntracked", BigDecimal::zero().into())
            .change_bigint("liquidityProviderCount", BigInt::zero().into());
    }
}

pub fn pool_sqrt_price_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<ProtoDelta<PoolSqrtPrice>>,
) {
    for delta in deltas.deltas {
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        let old_value = delta.old_value;
        let new_value = delta.new_value;

        entity_changes
            .push_change(
                "Pool",
                pool_address,
                delta.ordinal,
                convert_i32_to_operation(delta.operation as i32),
            )
            .change_bigint(
                "sqrtPrice",
                (old_value.sqrt_price, new_value.sqrt_price).into(),
            )
            .change_bigint("tick", (old_value.tick, new_value.tick).into());
    }
}

pub fn pool_liquidities_pool_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<BigIntDelta>,
) {
    for delta in deltas.deltas {
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        entity_changes
            .push_change("Pool", pool_address, delta.ordinal, Operation::Update)
            .change_bigint("liquidity", delta.into());
    }
}

pub fn total_value_locked_pool_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<BigDecimalDelta>,
) {
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

        entity_changes
            .push_change("Pool", pool_address, delta.ordinal, Operation::Update)
            .change_bigdecimal(name, delta.into());
    }
}

pub fn total_value_locked_by_token_pool_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<BigDecimalDelta>,
) {
    for delta in deltas.deltas {
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        let name = match delta.key.as_str().split(":").last().unwrap() {
            "token0" => "totalValueLockedToken0",
            "token1" => "totalValueLockedToken1",
            _ => continue,
        };

        entity_changes
            .push_change("Pool", pool_address, delta.ordinal, Operation::Update)
            .change_bigdecimal(name, delta.into());
    }
}

pub fn pool_fee_growth_global_x128_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<BigIntDelta>,
) {
    for delta in deltas.deltas {
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        let name = match delta.key.as_str().split(":").last().unwrap() {
            "token0" => "feeGrowthGlobal0X128",
            "token1" => "feeGrowthGlobal1X128",
            _ => continue,
        };

        entity_changes
            .push_change("Pool", pool_address, delta.ordinal, Operation::Update)
            .change_bigint(name, delta.into());
    }
}

pub fn price_pool_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<BigDecimalDelta>,
) {
    for delta in deltas.deltas {
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
            .change_bigdecimal(name, delta.into());
    }
}

pub fn tx_count_pool_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<BigIntDelta>,
) {
    for delta in deltas.deltas {
        if !delta.key.starts_with("pool:") {
            continue;
        }

        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string(); // TODO: put in keyer
        entity_changes
            .push_change("Pool", pool_address, delta.ordinal, Operation::Update)
            .change_bigint("txCount", delta.into());
    }
}

pub fn swap_volume_pool_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<BigDecimalDelta>,
) {
    for delta in deltas.deltas {
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
            .change_bigdecimal(name, delta.into());
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

pub fn swap_volume_token_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<BigDecimalDelta>,
) {
    for delta in deltas.deltas {
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
            .change_bigdecimal(name, delta.into());
    }
}

pub fn tx_count_token_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<BigIntDelta>,
) {
    for delta in deltas.deltas {
        if !delta.key.starts_with("token:") {
            continue;
        }

        let token_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        entity_changes
            .push_change("Token", token_address, delta.ordinal, Operation::Update)
            .change_bigint("txCount", delta.into());
    }
}

pub fn total_value_locked_by_token_token_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<BigDecimalDelta>,
) {
    for delta in deltas.deltas {
        let token_address = delta.key.as_str().split(":").nth(2).unwrap().to_string();

        entity_changes
            .push_change("Token", token_address, delta.ordinal, Operation::Update)
            .change_bigdecimal("totalValueLocked", delta.into());
    }
}

pub fn total_value_locked_usd_token_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<BigDecimalDelta>,
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

        entity_changes
            .push_change("Token", token_address, delta.ordinal, Operation::Update)
            .change_bigdecimal(name, delta.into());
    }
}

pub fn derived_eth_prices_token_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<BigDecimalDelta>,
) {
    for delta in deltas.deltas {
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
            .change_bigdecimal(name, delta.into());
    }
}

pub fn whitelist_token_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<ArrayDelta<String>>,
) {
    for delta in deltas.deltas {
        let token_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        entity_changes
            .push_change("Token", token_address, delta.ordinal, Operation::Update)
            .change_string_array("whitelistPools", delta.into());
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
        .change_string("id", token.address.clone().into())
        .change_string("symbol", token.symbol.clone().into())
        .change_string("name", token.name.clone().into())
        .change_bigint("decimals", token.decimals.into())
        .change_bigint("totalSupply", token.total_supply.clone().into())
        .change_bigdecimal("volume", BigDecimal::zero().into())
        .change_bigdecimal("volumeUSD", BigDecimal::zero().into())
        .change_bigdecimal("untrackedVolumeUSD", BigDecimal::zero().into())
        .change_bigdecimal("feesUSD", BigDecimal::zero().into())
        .change_bigint("txCount", BigInt::zero().into())
        .change_bigint("poolCount", BigInt::zero().into())
        .change_bigdecimal("totalValueLocked", BigDecimal::zero().into())
        .change_bigdecimal("totalValueLockedUSD", BigDecimal::zero().into())
        .change_bigdecimal("totalValueLockedUSDUntracked", BigDecimal::zero().into())
        .change_bigdecimal("derivedETH", BigDecimal::zero().into())
        .change_string_array("whitelistPools", token.whitelist_pools.clone().into());
}

// --------------------
//  Map Tick Entities
// --------------------
pub fn create_or_update_ticks_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<ProtoDelta<Tick>>,
) {
    for delta in deltas.deltas {
        let new_tick: Tick = delta.new_value;
        let old_tick: Tick = delta.old_value;

        if old_tick.id.eq("") {
            // does this makes sense?
            if new_tick.origin == Origin::Mint as i32 {
                create_tick_entity_change(entity_changes, new_tick)
            }
        } else {
            update_tick_entity_change(entity_changes, old_tick, new_tick);
        }
    }
}

pub fn ticks_liquidities_tick_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<BigIntDelta>,
) {
    for delta in deltas.deltas {
        let tick_id = delta.key.as_str().split(":").nth(1).unwrap().to_string();

        let name = match delta.key.as_str().split(":").last().unwrap() {
            "liquidityNet" => "liquidityNet",
            "liquidityGross" => "liquidityGross",
            _ => continue,
        };

        entity_changes
            .push_change("Tick", tick_id, delta.ordinal, Operation::Update)
            .change_bigint(name, delta.into());
    }
}

fn create_tick_entity_change(entity_changes: &mut EntityChanges, tick: Tick) {
    entity_changes
        .push_change("Tick", tick.id.clone(), tick.log_ordinal, Operation::Create)
        .change_string("id", tick.id.into())
        .change_string("poolAddress", tick.pool_address.clone().into())
        .change_bigint("tickIdx", tick.idx.into())
        .change_string("pool", tick.pool_address.into())
        .change_bigint("liquidityGross", BigInt::zero().into())
        .change_bigint("liquidityNet", BigInt::zero().into())
        .change_bigdecimal("price0", tick.price0.into())
        .change_bigdecimal("price1", tick.price1.into())
        .change_bigdecimal("volumeToken0", BigDecimal::zero().into())
        .change_bigdecimal("volumeToken1", BigDecimal::zero().into())
        .change_bigdecimal("volumeUSD", BigDecimal::zero().into())
        .change_bigdecimal("untrackedVolumeUSD", BigDecimal::zero().into())
        .change_bigdecimal("feesUSD", BigDecimal::zero().into())
        .change_bigdecimal("collectedFeesToken0", BigDecimal::zero().into())
        .change_bigdecimal("collectedFeesToken1", BigDecimal::zero().into())
        .change_bigdecimal("collectedFeesUSD", BigDecimal::zero().into())
        .change_bigint("createdAtTimestamp", tick.created_at_timestamp.into())
        .change_bigint("createdAtBlockNumber", tick.created_at_block_number.into())
        .change_bigint(
            "liquidityProviderCount",
            tick.created_at_block_number.into(),
        )
        .change_bigint("feeGrowthOutside0X128", tick.created_at_block_number.into())
        .change_bigint("feeGrowthOutside1X128", tick.created_at_block_number.into());
}

fn update_tick_entity_change(entity_changes: &mut EntityChanges, old_tick: Tick, new_tick: Tick) {
    entity_changes
        .push_change("Tick", new_tick.id, new_tick.log_ordinal, Operation::Update)
        .change_bigint(
            "feeGrowthOutside0X128",
            BigIntChange {
                old_value: old_tick.fee_growth_outside_0x_128,
                new_value: new_tick.fee_growth_outside_0x_128,
            },
        )
        .change_bigint(
            "feeGrowthOutside1X128",
            BigIntChange {
                old_value: old_tick.fee_growth_outside_1x_128,
                new_value: new_tick.fee_growth_outside_1x_128,
            },
        );
}

// --------------------
//  Map Position Entities
// --------------------
pub fn position_create_entity_change(positions: Positions, entity_changes: &mut EntityChanges) {
    for position in positions.positions {
        entity_changes
            .push_change(
                "Position",
                position.id.clone(),
                position.log_ordinal,
                Operation::Create,
            )
            .change_string("id", position.id.into())
            .change_string("owner", position.owner.into()) // string works
            .change_string("pool", position.pool.into())
            .change_string("token0", position.token0.into())
            .change_string("token1", position.token1.into())
            .change_string("tickLower", position.tick_lower.into())
            .change_string("tickUpper", position.tick_upper.into())
            .change_bigint("liquidity", BigInt::zero().into())
            .change_bigdecimal("depositedToken0", BigDecimal::zero().into())
            .change_bigdecimal("depositedToken1", BigDecimal::zero().into())
            .change_bigdecimal("withdrawnToken0", BigDecimal::zero().into())
            .change_bigdecimal("withdrawnToken1", BigDecimal::zero().into())
            .change_bigdecimal("collectedFeesToken0", BigDecimal::zero().into())
            .change_bigdecimal("collectedFeesToken1", BigDecimal::zero().into())
            .change_string("transaction", position.transaction.into())
            .change_bigint(
                "feeGrowthInside0LastX128",
                position.fee_growth_inside_0_last_x_128.into(),
            )
            .change_bigint(
                "feeGrowthInside1LastX128",
                position.fee_growth_inside_1_last_x_128.into(),
            );
    }
}

pub fn positions_changes_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<BigDecimalDelta>,
) {
    for delta in deltas.deltas {
        let position_id = delta.key.as_str().split(":").nth(1).unwrap().to_string();

        let mut entity_change =
            EntityChange::new("Position", position_id, delta.ordinal, Operation::Update);

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

        entity_changes.entity_changes.push(
            entity_change
                .change_bigdecimal(name, delta.into())
                .to_owned(),
        );
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
            .change_string("id", snapshot_position.id.into())
            .change_string("owner", snapshot_position.owner.into()) // string works
            .change_string("pool", snapshot_position.pool.into())
            .change_string("position", snapshot_position.position.into())
            .change_bigint("blockNumber", snapshot_position.block_number.into())
            .change_bigint("timestamp", snapshot_position.timestamp.into())
            .change_bigint("liquidity", snapshot_position.liquidity.into())
            .change_bigdecimal("depositedToken0", snapshot_position.deposited_token0.into())
            .change_bigdecimal("depositedToken1", snapshot_position.deposited_token1.into())
            .change_bigdecimal("withdrawnToken0", snapshot_position.withdrawn_token0.into())
            .change_bigdecimal("withdrawnToken1", snapshot_position.withdrawn_token1.into())
            .change_bigdecimal(
                "collectedFeesToken0",
                snapshot_position.collected_fees_token0.into(),
            )
            .change_bigdecimal(
                "collectedFeesToken1",
                snapshot_position.collected_fees_token1.into(),
            )
            .change_string("transaction", snapshot_position.transaction.into())
            .change_bigint(
                "feeGrowthInside0LastX128",
                snapshot_position.fee_growth_inside_0_last_x_128.into(),
            )
            .change_bigint(
                "feeGrowthInside1LastX128",
                snapshot_position.fee_growth_inside_1_last_x_128.into(),
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
            .change_string("id", transaction.id.into())
            .change_bigint("blockNumber", transaction.block_number.into())
            .change_bigint("timestamp", transaction.timestamp.into())
            .change_bigint("gasUsed", transaction.gas_used.into())
            .change_bigint("gasPrice", transaction.gas_price.into());
    }
}

// --------------------
//  Map Swaps Mints Burns Entities
// --------------------
pub fn swaps_mints_burns_created_entity_change(
    events: Events,
    tx_count_store: BigIntStoreGet,
    store_eth_prices: BigDecimalStoreGet,
    entity_changes: &mut EntityChanges,
) {
    for event in events.events {
        if event.r#type.is_none() {
            continue;
        }

        if event.r#type.is_some() {
            let transaction_count: i32 =
                match tx_count_store.get_last(keyer::factory_total_tx_count()) {
                    Some(data) => data.into(),
                    None => 0,
                };

            let transaction_id: String = format!("{}#{}", event.transaction_id, transaction_count);

            let token0_derived_eth_price =
                match store_eth_prices.get_last(keyer::token_eth_price(&event.token0)) {
                    // initializePool has occurred beforehand so there should always be a price
                    // maybe just ? instead of returning 1 and bubble up the error if there is one
                    None => BigDecimal::zero(),
                    Some(price) => price,
                };

            let token1_derived_eth_price: BigDecimal =
                match store_eth_prices.get_last(keyer::token_eth_price(&event.token1)) {
                    // initializePool has occurred beforehand so there should always be a price
                    // maybe just ? instead of returning 1 and bubble up the error if there is one
                    None => BigDecimal::zero(),
                    Some(price) => price,
                };

            let bundle_eth_price: BigDecimal =
                match store_eth_prices.get_last(keyer::bundle_eth_price()) {
                    // initializePool has occurred beforehand so there should always be a price
                    // maybe just ? instead of returning 1 and bubble up the error if there is one
                    None => BigDecimal::zero(),
                    Some(price) => price,
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
                        .change_string("id", transaction_id.into())
                        .change_string("transaction", event.transaction_id.into())
                        .change_bigint("timestamp", event.timestamp.into())
                        .change_string("pool", event.pool_address.into())
                        .change_string("token0", event.token0.into())
                        .change_string("token0", event.token1.into())
                        .change_string("sender", swap.sender.into()) // this as change_string()
                        .change_string("recipient", swap.recipient.into()) // this as change_string()
                        .change_string("origin", swap.origin.into()) // this as change_string()
                        .change_bigdecimal("amount0", swap.amount_0.into())
                        .change_bigdecimal("amount1", swap.amount_1.into())
                        .change_bigdecimal("amountUSD", amount_usd.into())
                        .change_bigint("sqrtPriceX96", swap.sqrt_price.into())
                        .change_bigint("tick", swap.tick.into())
                        .change_bigint("logIndex", event.log_ordinal.into());
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
                        .change_string("id", transaction_id.into())
                        .change_string("transaction", event.transaction_id.into())
                        .change_bigint("timestamp", event.timestamp.into())
                        .change_string("pool", event.pool_address.into())
                        .change_string("token0", event.token0.into())
                        .change_string("token0", event.token1.into())
                        .change_string("owner", mint.owner.into()) // this as change_string()
                        .change_string("sender", mint.sender.into()) // this as change_string()
                        .change_string("origin", mint.origin.into()) // this as change_string()
                        .change_bigint("amount", mint.amount.into())
                        .change_bigdecimal("amount0", mint.amount_0.into())
                        .change_bigdecimal("amount1", mint.amount_1.into())
                        .change_bigdecimal("amountUSD", amount_usd.into())
                        .change_bigint("tickLower", mint.tick_lower.into())
                        .change_bigint("tickUpper", mint.tick_upper.into())
                        .change_bigint("logIndex", event.log_ordinal.into());
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
                        .change_string("id", transaction_id.into())
                        .change_string("transaction", event.transaction_id.into())
                        .change_bigint("timestamp", event.timestamp.into())
                        .change_string("pool", event.pool_address.into())
                        .change_string("token0", event.token0.into())
                        .change_string("token0", event.token1.into())
                        .change_string("owner", burn.owner.into()) // this as change_string()
                        .change_string("origin", burn.origin.into()) // this as change_string()
                        .change_bigint("amount", burn.amount.into())
                        .change_bigdecimal("amount0", burn.amount_0.into())
                        .change_bigdecimal("amount1", burn.amount_1.into())
                        .change_bigdecimal("amountUSD", amount_usd.into())
                        .change_bigint("tickLower", burn.tick_lower.into())
                        .change_bigint("tickUpper", burn.tick_upper.into())
                        .change_bigint("logIndex", event.log_ordinal.into());
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
            .change_bigint(
                "feeGrowthGlobal0X128",
                flash.fee_growth_global_0x_128.into(),
            )
            .change_bigint(
                "feeGrowthGlobal1X128",
                flash.fee_growth_global_1x_128.into(),
            );
    }
}

// --------------------
//  Map Uniswap Day Data Entities
// --------------------
pub fn uniswap_day_data_tx_count_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<BigIntDelta>,
) {
    for delta in deltas.deltas {
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
            .change_string("id", day_id.into())
            .change_int32("date", day_start_timestamp.into())
            .change_bigdecimal("volumeUSDUntracked", BigDecimal::zero().into())
            .change_bigint("txCount", delta.into());
    }
}

pub fn uniswap_day_data_totals_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<BigDecimalDelta>,
) {
    for delta in deltas.deltas {
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
            .change_bigdecimal("tvlUSD", delta.into());
    }
}

pub fn uniswap_day_data_volumes_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<BigDecimalDelta>,
) {
    for delta in deltas.deltas {
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
            .change_bigdecimal(name, delta.into());
    }
}
