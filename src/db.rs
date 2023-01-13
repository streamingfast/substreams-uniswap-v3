use crate::uniswap::tick::Origin;
use crate::{
    keyer, utils, BurnEvent, Erc20Token, Flashes, MintEvent, PoolSqrtPrice, Pools, Positions,
    SnapshotPositions, SwapEvent, Tick, TokenEvents, Transactions,
};
use std::str::FromStr;
use substreams::scalar::{BigDecimal, BigInt};
use substreams::store::{
    DeltaArray, DeltaBigDecimal, DeltaBigInt, DeltaProto, Deltas, StoreGet, StoreGetBigDecimal,
    StoreGetBigInt,
};
use substreams::Hex;
use substreams_entity_change::pb::entity::{entity_change::Operation, EntityChange, EntityChanges};

// -------------------
//  Map Bundle Entities
// -------------------
pub fn created_bundle_entity_change(entity_changes: &mut EntityChanges) {
    let bd = BigDecimal::from(10 as i32);
    entity_changes
        .push_change("Bundle", "1", 1, Operation::Create)
        .change("ethPriceUSD", bd);
}

pub fn bundle_store_eth_price_usd_bundle_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<DeltaBigDecimal>,
) {
    for delta in deltas.deltas {
        if !delta.key.starts_with("bundle") {
            continue;
        }

        entity_changes
            .push_change("Bundle", "1", delta.ordinal, Operation::Update)
            .change("ethPriceUSD", delta);
    }
}

// -------------------
//  Map Factory Entities
// -------------------
pub fn factory_created_factory_entity_change(entity_changes: &mut EntityChanges) {
    entity_changes
        .push_change(
            "Factory",
            Hex(utils::UNISWAP_V3_FACTORY).to_string().as_str(),
            1,
            Operation::Create,
        )
        .change("id", Hex(utils::UNISWAP_V3_FACTORY).to_string())
        .change("poolCount", BigInt::zero())
        .change("txCount", BigInt::zero())
        .change("totalVolumeUSD", BigDecimal::zero())
        .change("totalVolumeETH", BigDecimal::zero())
        .change("totalFeesUSD", BigDecimal::zero())
        .change("totalFeesETH", BigDecimal::zero())
        .change("untrackedVolumeUSD", BigDecimal::zero())
        .change("totalValueLockedUSD", BigDecimal::zero())
        .change("totalValueLockedETH", BigDecimal::zero())
        .change("totalValueLockedUSDUntracked", BigDecimal::zero())
        .change("totalValueLockedETHUntracked", BigDecimal::zero())
        .change("owner", Hex(utils::ZERO_ADDRESS).to_string());
}

pub fn pool_created_factory_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<DeltaBigInt>,
) {
    for delta in deltas.deltas {
        entity_changes
            .push_change(
                "Factory",
                Hex(utils::UNISWAP_V3_FACTORY).to_string().as_str(),
                delta.ordinal,
                Operation::Update,
            )
            .change("poolCount", delta);
    }
}

pub fn tx_count_factory_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<DeltaBigInt>,
) {
    for delta in deltas.deltas {
        if !delta.key.starts_with("factory:") {
            continue;
        }
        entity_changes
            .push_change(
                "Factory",
                Hex(utils::UNISWAP_V3_FACTORY).to_string().as_str(),
                delta.ordinal,
                Operation::Update,
            )
            .change("txCount", delta);
    }
}

pub fn swap_volume_factory_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<DeltaBigDecimal>,
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
                Hex(utils::UNISWAP_V3_FACTORY).to_string().as_str(),
                delta.ordinal,
                Operation::Update,
            )
            .change(name, delta);
    }
}

pub fn total_value_locked_factory_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<DeltaBigDecimal>,
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
                Hex(utils::UNISWAP_V3_FACTORY).to_string().as_str(),
                delta.ordinal,
                Operation::Update,
            )
            .change(name, delta);
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
                pool.address.clone().as_str(),
                pool.log_ordinal,
                Operation::Create,
            )
            .change("id", pool.address)
            .change(
                "createdAtTimestamp",
                BigInt::from(pool.created_at_timestamp),
            )
            .change(
                "createdAtBlockNumber",
                BigInt::from(pool.created_at_block_number),
            )
            .change("token0", pool.token0.unwrap().address)
            .change("token1", pool.token1.unwrap().address)
            .change("feeTier", BigInt::from(pool.fee_tier.unwrap()))
            .change("liquidity", BigInt::zero())
            .change("sqrtPrice", BigInt::zero())
            .change("feeGrowthGlobal0X128", BigInt::zero())
            .change("feeGrowthGlobal1X128", BigInt::zero())
            .change("token0Price", BigDecimal::zero())
            .change("token1Price", BigDecimal::zero())
            .change("tick", BigInt::zero())
            .change("observationIndex", BigInt::zero())
            .change("volumeToken0", BigDecimal::zero())
            .change("volumeToken1", BigDecimal::zero())
            .change("volumeUSD", BigDecimal::zero())
            .change("untrackedVolumeUSD", BigDecimal::zero())
            .change("feesUSD", BigDecimal::zero())
            .change("txCount", BigInt::zero())
            .change("collectedFeesToken0", BigDecimal::zero())
            .change("collectedFeesToken1", BigDecimal::zero())
            .change("collectedFeesUSD", BigDecimal::zero())
            .change("totalValueLockedToken0", BigDecimal::zero())
            .change("totalValueLockedToken1", BigDecimal::zero())
            .change("totalValueLockedETH", BigDecimal::zero())
            .change("totalValueLockedUSD", BigDecimal::zero())
            .change("totalValueLockedUSDUntracked", BigDecimal::zero())
            .change("liquidityProviderCount", BigInt::zero());
    }
}

pub fn pool_sqrt_price_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<DeltaProto<PoolSqrtPrice>>,
) {
    for delta in deltas.deltas {
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();

        entity_changes
            .push_change(
                "Pool",
                pool_address.as_str(),
                delta.ordinal,
                Operation::from_i32(delta.operation as i32).unwrap(),
            )
            .change(
                "sqrtPrice",
                DeltaBigInt {
                    operation: delta.operation,
                    ordinal: 0,
                    key: "".to_string(),
                    old_value: delta.old_value.sqrt_price(),
                    new_value: delta.new_value.sqrt_price(),
                },
            )
            .change(
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

pub fn pool_liquidities_pool_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<DeltaBigInt>,
) {
    for delta in deltas.deltas {
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        entity_changes
            .push_change(
                "Pool",
                pool_address.as_str(),
                delta.ordinal,
                Operation::Update,
            )
            .change("liquidity", delta);
    }
}

pub fn total_value_locked_pool_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<DeltaBigDecimal>,
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
            .push_change(
                "Pool",
                pool_address.as_str(),
                delta.ordinal,
                Operation::Update,
            )
            .change(name, delta);
    }
}

pub fn total_value_locked_by_token_pool_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<DeltaBigDecimal>,
) {
    for delta in deltas.deltas {
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        let name = match delta.key.as_str().split(":").last().unwrap() {
            "token0" => "totalValueLockedToken0",
            "token1" => "totalValueLockedToken1",
            _ => continue,
        };

        entity_changes
            .push_change(
                "Pool",
                pool_address.as_str(),
                delta.ordinal,
                Operation::Update,
            )
            .change(name, delta);
    }
}

pub fn pool_fee_growth_global_x128_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<DeltaBigInt>,
) {
    for delta in deltas.deltas {
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        let name = match delta.key.as_str().split(":").last().unwrap() {
            "token0" => "feeGrowthGlobal0X128",
            "token1" => "feeGrowthGlobal1X128",
            _ => continue,
        };

        entity_changes
            .push_change(
                "Pool",
                pool_address.as_str(),
                delta.ordinal,
                Operation::Update,
            )
            .change(name, delta);
    }
}

pub fn price_pool_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<DeltaBigDecimal>,
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
            .push_change(
                "Pool",
                pool_address.as_str(),
                delta.ordinal,
                Operation::Update,
            )
            .change(name, delta);
    }
}

pub fn tx_count_pool_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<DeltaBigInt>,
) {
    for delta in deltas.deltas {
        if !delta.key.starts_with("pool:") {
            continue;
        }

        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string(); // TODO: put in keyer
        entity_changes
            .push_change(
                "Pool",
                pool_address.as_str(),
                delta.ordinal,
                Operation::Update,
            )
            .change("txCount", delta);
    }
}

pub fn swap_volume_pool_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<DeltaBigDecimal>,
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
            .push_change(
                "Pool",
                pool_address.as_str(),
                delta.ordinal,
                Operation::Update,
            )
            .change(name, delta);
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
    deltas: Deltas<DeltaBigDecimal>,
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
            .push_change(
                "Token",
                token_address.as_str(),
                delta.ordinal,
                Operation::Update,
            )
            .change(name, delta);
    }
}

pub fn tx_count_token_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<DeltaBigInt>,
) {
    for delta in deltas.deltas {
        if !delta.key.starts_with("token:") {
            continue;
        }

        let token_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        entity_changes
            .push_change(
                "Token",
                token_address.as_str(),
                delta.ordinal,
                Operation::Update,
            )
            .change("txCount", delta);
    }
}

pub fn total_value_locked_by_token_token_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<DeltaBigDecimal>,
) {
    for delta in deltas.deltas {
        let token_address = delta.key.as_str().split(":").nth(2).unwrap().to_string();

        entity_changes
            .push_change(
                "Token",
                token_address.as_str(),
                delta.ordinal,
                Operation::Update,
            )
            .change("totalValueLocked", delta);
    }
}

pub fn total_value_locked_usd_token_entity_change(
    entity_changes: &mut EntityChanges,
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

        entity_changes
            .push_change(
                "Token",
                token_address.as_str(),
                delta.ordinal,
                Operation::Update,
            )
            .change(name, delta);
    }
}

pub fn derived_eth_prices_token_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<DeltaBigDecimal>,
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
            .push_change(
                "Token",
                token_address.as_str(),
                delta.ordinal,
                Operation::Update,
            )
            .change(name, delta);
    }
}

pub fn whitelist_token_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<DeltaArray<String>>,
) {
    for delta in deltas.deltas {
        let token_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        entity_changes
            .push_change(
                "Token",
                token_address.as_str(),
                delta.ordinal,
                Operation::Update,
            )
            .change("whitelistPools", delta);
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
            token.address.clone().as_str(),
            log_ordinal,
            Operation::Create,
        )
        .change("id", token.address.clone())
        .change("symbol", token.symbol.clone())
        .change("name", token.name.clone())
        .change("decimals", BigInt::from(token.decimals))
        .change(
            "totalSupply",
            BigInt::from_str(token.total_supply.clone().as_str()).unwrap(),
        )
        .change("volume", BigDecimal::zero())
        .change("volumeUSD", BigDecimal::zero())
        .change("untrackedVolumeUSD", BigDecimal::zero())
        .change("feesUSD", BigDecimal::zero())
        .change("txCount", BigInt::zero())
        .change("poolCount", BigInt::zero())
        .change("totalValueLocked", BigDecimal::zero())
        .change("totalValueLockedUSD", BigDecimal::zero())
        .change("totalValueLockedUSDUntracked", BigDecimal::zero())
        .change("derivedETH", BigDecimal::zero())
        .change("whitelistPools", token.whitelist_pools.clone());
}

// --------------------
//  Map Tick Entities
// --------------------
pub fn create_or_update_ticks_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<DeltaProto<Tick>>,
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
    deltas: Deltas<DeltaBigInt>,
) {
    for delta in deltas.deltas {
        let tick_id = delta.key.as_str().split(":").nth(1).unwrap().to_string();

        let name = match delta.key.as_str().split(":").last().unwrap() {
            "liquidityNet" => "liquidityNet",
            "liquidityGross" => "liquidityGross",
            _ => continue,
        };

        entity_changes
            .push_change("Tick", tick_id.as_str(), delta.ordinal, Operation::Update)
            .change(name, delta);
    }
}

fn create_tick_entity_change(entity_changes: &mut EntityChanges, tick: Tick) {
    entity_changes
        .push_change(
            "Tick",
            tick.id.clone().as_str(),
            tick.log_ordinal,
            Operation::Create,
        )
        .change("id", tick.id)
        .change("poolAddress", tick.pool_address.clone())
        .change("tickIdx", BigInt::from(tick.idx.unwrap()))
        .change("pool", tick.pool_address)
        .change("liquidityGross", BigInt::zero())
        .change("liquidityNet", BigInt::zero())
        .change("price0", BigDecimal::from(tick.price0.unwrap()))
        .change("price1", BigDecimal::from(tick.price1.unwrap()))
        .change("volumeToken0", BigDecimal::zero())
        .change("volumeToken1", BigDecimal::zero())
        .change("volumeUSD", BigDecimal::zero())
        .change("untrackedVolumeUSD", BigDecimal::zero())
        .change("feesUSD", BigDecimal::zero())
        .change("collectedFeesToken0", BigDecimal::zero())
        .change("collectedFeesToken1", BigDecimal::zero())
        .change("collectedFeesUSD", BigDecimal::zero())
        .change(
            "createdAtTimestamp",
            BigInt::from(tick.created_at_timestamp),
        )
        .change(
            "createdAtBlockNumber",
            BigInt::from(tick.created_at_block_number),
        )
        .change("liquidityProviderCount", BigInt::zero())
        .change(
            "feeGrowthOutside0X128",
            BigInt::from(tick.created_at_block_number),
        )
        .change(
            "feeGrowthOutside1X128",
            BigInt::from(tick.created_at_block_number),
        );
}

fn update_tick_entity_change(entity_changes: &mut EntityChanges, old_tick: Tick, new_tick: Tick) {
    entity_changes
        .push_change(
            "Tick",
            new_tick.id.as_str(),
            new_tick.log_ordinal,
            Operation::Update,
        )
        .change(
            "feeGrowthOutside0X128",
            DeltaBigInt {
                operation: substreams::pb::substreams::store_delta::Operation::Update,
                ordinal: new_tick.log_ordinal,
                key: "".to_string(),
                old_value: BigInt::from(old_tick.fee_growth_outside_0x_128.unwrap()),
                new_value: BigInt::from(new_tick.fee_growth_outside_0x_128.unwrap()),
            },
        )
        .change(
            "feeGrowthOutside1X128",
            DeltaBigInt {
                operation: substreams::pb::substreams::store_delta::Operation::Update,
                ordinal: new_tick.log_ordinal,
                key: "".to_string(),
                old_value: BigInt::from(old_tick.fee_growth_outside_1x_128.unwrap()),
                new_value: BigInt::from(new_tick.fee_growth_outside_1x_128.unwrap()),
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
                position.id.clone().as_str(),
                position.log_ordinal,
                Operation::Create,
            )
            .change("id", position.id)
            .change("owner", position.owner.into_bytes())
            .change("pool", position.pool)
            .change("token0", position.token0)
            .change("token1", position.token1)
            .change("tickLower", position.tick_lower)
            .change("tickUpper", position.tick_upper)
            .change("liquidity", BigDecimal::zero())
            .change("depositedToken0", BigDecimal::zero())
            .change("depositedToken1", BigDecimal::zero())
            .change("withdrawnToken0", BigDecimal::zero())
            .change("withdrawnToken1", BigDecimal::zero())
            .change("collectedFeesToken0", BigDecimal::zero())
            .change("collectedFeesToken1", BigDecimal::zero())
            .change("transaction", position.transaction)
            .change(
                "feeGrowthInside0LastX128",
                BigInt::from(position.fee_growth_inside_0_last_x_128.unwrap()),
            )
            .change(
                "feeGrowthInside1LastX128",
                BigInt::from(position.fee_growth_inside_1_last_x_128.unwrap()),
            );
    }
}

pub fn positions_changes_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<DeltaBigDecimal>,
) {
    for delta in deltas.deltas {
        let position_id = delta.key.as_str().split(":").nth(1).unwrap().to_string();

        let mut entity_change = EntityChange::new(
            "Position",
            position_id.as_str(),
            delta.ordinal,
            Operation::Update,
        );

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

        entity_changes
            .entity_changes
            .push(entity_change.change(name, delta).to_owned());
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
                snapshot_position.id.clone().as_str(),
                snapshot_position.log_ordinal,
                Operation::Create,
            )
            .change("id", snapshot_position.id)
            .change("owner", snapshot_position.owner.into_bytes())
            .change("pool", snapshot_position.pool)
            .change("position", snapshot_position.position)
            .change("blockNumber", BigInt::from(snapshot_position.block_number))
            .change("timestamp", BigInt::from(snapshot_position.timestamp))
            .change(
                "liquidity",
                BigDecimal::from(snapshot_position.liquidity.unwrap()),
            )
            .change(
                "depositedToken0",
                BigDecimal::from(snapshot_position.deposited_token0.unwrap()),
            )
            .change(
                "depositedToken1",
                BigDecimal::from(snapshot_position.deposited_token1.unwrap()),
            )
            .change(
                "withdrawnToken0",
                BigDecimal::from(snapshot_position.withdrawn_token0.unwrap()),
            )
            .change(
                "withdrawnToken1",
                BigDecimal::from(snapshot_position.withdrawn_token1.unwrap()),
            )
            .change(
                "collectedFeesToken0",
                BigDecimal::from(snapshot_position.collected_fees_token0.unwrap()),
            )
            .change(
                "collectedFeesToken1",
                BigDecimal::from(snapshot_position.collected_fees_token1.unwrap()),
            )
            .change("transaction", snapshot_position.transaction)
            .change(
                "feeGrowthInside0LastX128",
                BigInt::from(snapshot_position.fee_growth_inside_0_last_x_128.unwrap()),
            )
            .change(
                "feeGrowthInside1LastX128",
                BigInt::from(snapshot_position.fee_growth_inside_1_last_x_128.unwrap()),
            );
    }
}

// --------------------
//  Map Transaction Entities
// --------------------
pub fn transaction_entity_change(transactions: Transactions, entity_changes: &mut EntityChanges) {
    for transaction in transactions.transactions {
        let gas_price = match transaction.gas_price {
            None => BigInt::zero(),
            Some(price) => BigInt::from(price),
        };

        entity_changes
            .push_change(
                "Transaction",
                transaction.id.clone().as_str(),
                transaction.log_ordinal,
                Operation::Create,
            )
            .change("id", transaction.id)
            .change("blockNumber", BigInt::from(transaction.block_number))
            .change("timestamp", BigInt::from(transaction.timestamp))
            .change("gasUsed", BigInt::from(transaction.gas_used))
            .change("gasPrice", gas_price);
    }
}

// --------------------
//  Map Swaps Mints Burns Entities
// --------------------
pub fn swaps_mints_burns_created_entity_change(
    events: TokenEvents,
    tx_count_store: StoreGetBigInt,
    store_eth_prices: StoreGetBigDecimal,
    entity_changes: &mut EntityChanges,
) {
    for event in events.events {
        if event.r#type.is_none() {
            continue;
        }

        if event.r#type.is_some() {
            let transaction_count: i32 =
                match tx_count_store.get_last(keyer::factory_total_tx_count()) {
                    Some(data) => data.to_u64() as i32,
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
                    let amount0: BigDecimal = BigDecimal::from(swap.amount_0.unwrap());
                    let amount1: BigDecimal = BigDecimal::from(swap.amount_1.unwrap());

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
                            transaction_id.clone().as_str(),
                            event.log_ordinal,
                            Operation::Create,
                        )
                        .change("id", transaction_id)
                        .change("transaction", event.transaction_id)
                        .change("timestamp", BigInt::from(event.timestamp))
                        .change("pool", event.pool_address)
                        .change("token0", event.token0)
                        .change("token1", event.token1)
                        .change("sender", swap.sender.into_bytes())
                        .change("recipient", swap.recipient.into_bytes())
                        .change("origin", swap.origin.into_bytes())
                        .change("amount0", amount0)
                        .change("amount1", amount1)
                        .change("amountUSD", amount_usd)
                        .change("sqrtPriceX96", BigInt::from(swap.sqrt_price.unwrap()))
                        .change("tick", BigInt::from(swap.tick.unwrap()))
                        .change("logIndex", BigInt::from(event.log_ordinal));
                    // not sure if log index is good
                }
                MintEvent(mint) => {
                    let amount0: BigDecimal = BigDecimal::from(mint.amount_0.unwrap());
                    let amount1: BigDecimal = BigDecimal::from(mint.amount_1.unwrap());

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
                            transaction_id.clone().as_str(),
                            event.log_ordinal,
                            Operation::Create,
                        )
                        .change("id", transaction_id)
                        .change("transaction", event.transaction_id)
                        .change("timestamp", BigInt::from(event.timestamp))
                        .change("pool", event.pool_address)
                        .change("token0", event.token0)
                        .change("token1", event.token1)
                        .change("owner", mint.owner.into_bytes())
                        .change("sender", mint.sender.into_bytes())
                        .change("origin", mint.origin.into_bytes())
                        .change("amount", BigInt::from(mint.amount.unwrap()))
                        .change("amount0", amount0)
                        .change("amount1", amount1)
                        .change("amountUSD", amount_usd)
                        .change("tickLower", BigInt::from(mint.tick_lower.unwrap()))
                        .change("tickUpper", BigInt::from(mint.tick_upper.unwrap()))
                        .change("logIndex", BigInt::from(event.log_ordinal));
                    // not sure if log index is good
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

                    entity_changes
                        .push_change(
                            "Burn",
                            transaction_id.clone().as_str(),
                            event.log_ordinal,
                            Operation::Create,
                        )
                        .change("id", transaction_id)
                        .change("transaction", event.transaction_id)
                        .change("pool", event.pool_address)
                        .change("token0", event.token0)
                        .change("token1", event.token1)
                        .change("timestamp", BigInt::from(event.timestamp))
                        .change("owner", burn.owner.into_bytes())
                        .change("origin", burn.origin.into_bytes())
                        .change("amount", BigInt::from(burn.amount.unwrap()))
                        .change("amount0", amount0)
                        .change("amount1", amount1)
                        .change("amountUSD", amount_usd)
                        .change("tickLower", BigInt::from(burn.tick_lower.unwrap()))
                        .change("tickUpper", BigInt::from(burn.tick_upper.unwrap()))
                        .change("logIndex", BigInt::from(event.log_ordinal));
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
                flash.pool_address.as_str(),
                flash.log_ordinal,
                Operation::Update,
            )
            .change(
                "feeGrowthGlobal0X128",
                BigInt::from(flash.fee_growth_global_0x_128.unwrap()),
            )
            .change(
                "feeGrowthGlobal1X128",
                BigInt::from(flash.fee_growth_global_1x_128.unwrap()),
            );
    }
}

// --------------------
//  Map Uniswap Day Data Entities
// --------------------
pub fn uniswap_day_data_tx_count_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<DeltaBigInt>,
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
                day_id.to_string().as_str(),
                delta.ordinal,
                Operation::Update,
            )
            .change("id", day_id.to_string())
            .change("date", day_start_timestamp)
            .change("volumeUSDUntracked", BigDecimal::zero())
            .change("txCount", delta);
    }
}

pub fn uniswap_day_data_totals_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<DeltaBigDecimal>,
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
                day_id.to_string().as_str(),
                delta.ordinal,
                Operation::Update,
            )
            .change("tvlUSD", delta);
    }
}

pub fn uniswap_day_data_volumes_entity_change(
    entity_changes: &mut EntityChanges,
    deltas: Deltas<DeltaBigDecimal>,
) {
    for delta in deltas.deltas {
        if !delta.key.starts_with("uniswap_day_data") {
            continue;
        }

        let day_id = delta.key.as_str().split(":").nth(1).unwrap();

        let name = match delta.key.as_str().split(":").last().unwrap() {
            "volumeETH" => "volumeETH",
            "volumeUSD" => "volumeUSD",
            "feesUSD" => "feesUSD",
            _ => continue,
        };

        entity_changes
            .push_change("UniswapDayData", day_id, delta.ordinal, Operation::Update)
            .change(name, delta);
    }
}
