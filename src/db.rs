use crate::pb::entity::field::Type as FieldType;
use crate::pb::entity::EntityChange;
use crate::pb::entity::Field;
use crate::utils::decode_bytes_to_big_decimal;
use crate::{
    big_decimal_string_field_value, big_decimal_vec_field_value, big_int_field_value,
    int_field_value, keyer, new_field, string_field_value, update_field, utils, BurnEvent,
    Erc20Token, Event, Flash, MintEvent, Pool, PoolSqrtPrice, Position, SnapshotPosition,
    SnapshotPositions, SwapEvent, Tick, Transaction,
};
use bigdecimal::{BigDecimal, Zero};
use num_bigint::{BigInt, ToBigInt};
use std::str::FromStr;
use substreams::pb::substreams::store_delta::Operation;
use substreams::pb::substreams::StoreDelta;
use substreams::store::StoreGet;
use substreams::{log, proto, Hex};

// -------------------
//  Map Bundle Entities
// -------------------
pub fn bundle_created_bundle_entity_change() -> EntityChange {
    return EntityChange {
        entity: "Bundle".to_string(),
        id: "1".to_string(),
        ordinal: 1,
        operation: Operation::Create as i32,
        fields: vec![
            new_field!("id", FieldType::String, "1".to_string()),
            new_field!(
                "ethPriceUSD",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
        ],
    };
}

pub fn bundle_store_eth_price_usd_bundle_entity_change(delta: StoreDelta) -> Option<EntityChange> {
    if !delta.key.starts_with("bundle") {
        return None;
    }

    Some(EntityChange {
        entity: "Bundle".to_string(),
        id: "1".to_string(),
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![update_field!(
            "ethPriceUSD",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
        )],
    })
}

// -------------------
//  Map Factory Entities
// -------------------
pub fn factory_created_factory_entity_change() -> EntityChange {
    return EntityChange {
        entity: "Factory".to_string(),
        id: Hex(utils::UNISWAP_V3_FACTORY).to_string(),
        ordinal: 1, //  factory is created only once at the deployment of the contract
        operation: Operation::Create as i32,
        fields: vec![
            new_field!(
                "id",
                FieldType::String,
                Hex(utils::UNISWAP_V3_FACTORY).to_string()
            ),
            new_field!("poolCount", FieldType::Bigint, BigInt::zero().to_string()),
            new_field!("txCount", FieldType::Bigint, BigInt::zero().to_string()),
            new_field!(
                "totalVolumeUSD",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "totalVolumeETH",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "totalFeesUSD",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "totalFeesETH",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "untrackedVolumeUSD",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "totalValueLockedUSD",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "totalValueLockedETH",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "totalValueLockedUSDUntracked",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "totalValueLockedETHUntracked",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "owner",
                FieldType::String,
                Hex(utils::ZERO_ADDRESS).to_string()
            ),
        ],
    };
}

pub fn pool_created_factory_entity_change(pool_count_delta: StoreDelta) -> EntityChange {
    return EntityChange {
        entity: "Factory".to_string(),
        id: Hex(utils::UNISWAP_V3_FACTORY).to_string(),
        ordinal: pool_count_delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![update_field!(
            "poolCount",
            FieldType::Bigint,
            utils::decode_bytes_to_big_int(pool_count_delta.old_value).to_string(),
            utils::decode_bytes_to_big_int(pool_count_delta.new_value).to_string()
        )],
    };
}

pub fn tx_count_factory_entity_change(tx_count_delta: StoreDelta) -> Option<EntityChange> {
    if !tx_count_delta.key.starts_with("factory:") {
        return None;
    }

    return Some(EntityChange {
        entity: "Factory".to_string(),
        id: Hex(utils::UNISWAP_V3_FACTORY).to_string(),
        ordinal: tx_count_delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![update_field!(
            "txCount",
            FieldType::Bigint,
            utils::decode_bytes_to_big_int(tx_count_delta.old_value).to_string(),
            utils::decode_bytes_to_big_int(tx_count_delta.new_value).to_string()
        )],
    });
}

pub fn swap_volume_factory_entity_change(delta: StoreDelta) -> Option<EntityChange> {
    if !delta.key.as_str().starts_with("factory:") {
        return None;
    }
    let mut change: EntityChange = EntityChange {
        entity: "Factory".to_string(),
        id: Hex(utils::UNISWAP_V3_FACTORY).to_string(),
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };
    match delta.key.as_str().split(":").last().unwrap() {
        "totalVolumeUSD" => change.fields.push(update_field!(
            "totalVolumeUSD",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
        )),
        "untrackedVolumeUSD" => change.fields.push(update_field!(
            "untrackedVolumeUSD",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
        )),
        "totalFeesUSD" => change.fields.push(update_field!(
            "totalFeesUSD",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
        )),
        "totalVolumeETH" => change.fields.push(update_field!(
            "totalVolumeETH",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
        )),
        "totalFeesETH" => change.fields.push(update_field!(
            "totalFeesETH",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
        )),
        _ => {
            return None;
        }
    }
    Some(change)
}

pub fn total_value_locked_factory_entity_change(
    total_value_locked_delta: StoreDelta,
) -> Option<EntityChange> {
    if !total_value_locked_delta.key.starts_with("factory:") {
        return None;
    }

    let mut change: EntityChange = EntityChange {
        entity: "Factory".to_string(),
        id: Hex(utils::UNISWAP_V3_FACTORY).to_string(),
        ordinal: total_value_locked_delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };

    match total_value_locked_delta
        .key
        .as_str()
        .split(":")
        .last()
        .unwrap()
    {
        "totalValueLockedUSD" => change.fields.push(update_field!(
            "totalValueLockedUSD",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(total_value_locked_delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(total_value_locked_delta.new_value).to_string()
        )),
        "totalValueLockedETH" => change.fields.push(update_field!(
            "totalValueLockedETH",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(total_value_locked_delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(total_value_locked_delta.new_value).to_string()
        )),
        _ => {}
    }

    Some(change)
}

// -------------------
//  Map Pool Entities
// -------------------
pub fn pools_created_pool_entity_change(pool: Pool) -> EntityChange {
    return EntityChange {
        entity: "Pool".to_string(),
        id: pool.address.clone(),
        ordinal: pool.log_ordinal,
        operation: Operation::Create as i32,
        fields: vec![
            new_field!("id", FieldType::String, pool.address),
            new_field!(
                "createdAtTimestamp",
                FieldType::Bigint,
                pool.created_at_timestamp
            ),
            new_field!(
                "createdAtBlockNumber",
                FieldType::Bigint,
                pool.created_at_block_number
            ),
            new_field!("token0", FieldType::String, pool.token0.unwrap().address),
            new_field!("token1", FieldType::String, pool.token1.unwrap().address),
            new_field!("feeTier", FieldType::Bigint, pool.fee_tier.to_string()),
            new_field!("liquidity", FieldType::Bigint, BigInt::zero().to_string()),
            new_field!("sqrtPrice", FieldType::Bigint, BigInt::zero().to_string()),
            new_field!(
                "feeGrowthGlobal0X128",
                FieldType::Bigint,
                BigInt::zero().to_string()
            ),
            new_field!(
                "feeGrowthGlobal1X128",
                FieldType::Bigint,
                BigInt::zero().to_string()
            ),
            new_field!(
                "token0Price",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "token1Price",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!("tick", FieldType::Bigint, BigInt::zero().to_string()),
            new_field!(
                "observationIndex",
                FieldType::Bigint,
                BigInt::zero().to_string()
            ),
            new_field!(
                "volumeToken0",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "volumeToken1",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "volumeUSD",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "untrackedVolumeUSD",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "feesUSD",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!("txCount", FieldType::Bigint, BigInt::zero().to_string()),
            new_field!(
                "collectedFeesToken0",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "collectedFeesToken1",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "collectedFeesUSD",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "totalValueLockedToken0",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "totalValueLockedToken1",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "totalValueLockedETH",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "totalValueLockedUSD",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "totalValueLockedUSDUntracked",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "liquidityProviderCount",
                FieldType::Bigint,
                BigInt::zero().to_string()
            ),
        ],
    };
}

pub fn pool_sqrt_price_entity_change(delta: StoreDelta) -> Option<EntityChange> {
    let new_value: PoolSqrtPrice = proto::decode(&delta.new_value).unwrap();
    let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();

    let mut change = EntityChange {
        entity: "Pool".to_string(),
        id: pool_address,
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };

    match delta.operation {
        1 => {
            change.fields.push(update_field!(
                "sqrtPrice",
                FieldType::Bigint,
                BigInt::zero().to_string(),
                new_value.sqrt_price
            ));
            change.fields.push(update_field!(
                "tick",
                FieldType::Bigint,
                BigInt::zero().to_string(),
                new_value.tick
            ));
        }
        2 => {
            let old_value: PoolSqrtPrice = proto::decode(&delta.new_value).unwrap();
            change.fields.push(update_field!(
                "sqrtPrice",
                FieldType::Bigint,
                old_value.sqrt_price,
                new_value.sqrt_price
            ));
            change.fields.push(update_field!(
                "tick",
                FieldType::Bigint,
                old_value.tick,
                new_value.tick
            ));
        }
        _ => return None,
    }

    Some(change)
}

pub fn pool_liquidities_pool_entity_change(delta: StoreDelta) -> EntityChange {
    let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
    return EntityChange {
        entity: "Pool".to_string(),
        id: pool_address,
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![update_field!(
            "liquidity",
            FieldType::Bigint,
            utils::decode_bytes_to_big_int(delta.old_value).to_string(),
            utils::decode_bytes_to_big_int(delta.new_value).to_string()
        )],
    };
}

pub fn total_value_locked_pool_entity_change(delta: StoreDelta) -> Option<EntityChange> {
    if !delta.key.starts_with("pool:") {
        return None;
    }
    let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
    let mut change: EntityChange = EntityChange {
        entity: "Pool".to_string(),
        id: pool_address,
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };

    match delta.key.as_str().split(":").last().unwrap() {
        "usd" => change.fields.push(update_field!(
            "totalValueLockedUSD",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
        )),
        "eth" => change.fields.push(update_field!(
            "totalValueLockedETH",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
        )),
        _ => {}
    }

    Some(change)
}

pub fn total_value_locked_by_token_pool_entity_change(delta: StoreDelta) -> Option<EntityChange> {
    let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
    let mut change: EntityChange = EntityChange {
        entity: "Pool".to_string(),
        id: pool_address,
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };

    match delta.key.as_str().split(":").last().unwrap() {
        "token0" => change.fields.push(update_field!(
            "totalValueLockedToken0",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
        )),
        "token1" => change.fields.push(update_field!(
            "totalValueLockedToken1",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
        )),
        _ => return None,
    }

    Some(change)
}

pub fn pool_fee_growth_global_x128_entity_change(delta: StoreDelta) -> Option<EntityChange> {
    let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
    let mut change: EntityChange = EntityChange {
        entity: "Pool".to_string(),
        id: pool_address,
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };

    match delta.key.as_str().split(":").last().unwrap() {
        "token0" => change.fields.push(update_field!(
            "feeGrowthGlobal0X128",
            FieldType::Bigint,
            utils::decode_bytes_to_big_int(delta.old_value).to_string(),
            utils::decode_bytes_to_big_int(delta.new_value).to_string()
        )),
        "token1" => change.fields.push(update_field!(
            "feeGrowthGlobal1X128",
            FieldType::Bigint,
            utils::decode_bytes_to_big_int(delta.old_value).to_string(),
            utils::decode_bytes_to_big_int(delta.new_value).to_string()
        )),
        _ => return None,
    }

    Some(change)
}

pub fn price_pool_entity_change(delta: StoreDelta) -> Option<EntityChange> {
    if !delta.key.as_str().starts_with("pool:") {
        return None;
    }

    let mut key_parts = delta.key.as_str().split(":");
    let pool_address = key_parts.nth(1).unwrap();
    let field_name: &str;
    match key_parts.last().unwrap() {
        "token0" => {
            field_name = "token0Price";
        }
        "token1" => {
            field_name = "token1Price";
        }
        _ => {
            return None;
        }
    }

    let mut change = EntityChange {
        entity: "Pool".to_string(),
        id: pool_address.to_string(),
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };

    match delta.operation {
        1 => {
            change.fields.push(update_field!(
                field_name,
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string(),
                utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
            ));
        }
        2 => {
            change.fields.push(update_field!(
                field_name,
                FieldType::Bigdecimal,
                utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
                utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
            ));
        }
        _ => return None,
    }

    Some(change)
}

pub fn tx_count_pool_entity_change(delta: StoreDelta) -> Option<EntityChange> {
    if !delta.key.starts_with("pool:") {
        return None;
    }

    let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();

    return Some(EntityChange {
        entity: "Pool".to_string(),
        id: pool_address,
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![update_field!(
            "txCount",
            FieldType::Bigint,
            utils::decode_bytes_to_big_int(delta.old_value).to_string(),
            utils::decode_bytes_to_big_int(delta.new_value).to_string()
        )],
    });
}

pub fn swap_volume_pool_entity_change(delta: StoreDelta) -> Option<EntityChange> {
    if !delta.key.as_str().starts_with("swap") {
        return None;
    }

    let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();

    let mut change: EntityChange = EntityChange {
        entity: "Pool".to_string(),
        id: pool_address,
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };
    match delta.key.as_str().split(":").last().unwrap() {
        "token0" => change.fields.push(update_field!(
            "volumeToken0",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
        )),
        "token1" => change.fields.push(update_field!(
            "volumeToken1",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
        )),
        "usd" => change.fields.push(update_field!(
            "volumeUSD",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
        )),
        "untrackedUSD" => change.fields.push(update_field!(
            "untrackedVolumeUSD",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
        )),
        "feesUSD" => change.fields.push(update_field!(
            "feesUSD",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
        )),
        _ => {
            return None;
        }
    }
    Some(change)
}

// --------------------
//  Map Token Entities
// --------------------
pub fn tokens_created_token_entity_change(pool: Pool) -> Vec<EntityChange> {
    let token0: &Erc20Token = pool.token0.as_ref().unwrap();
    let token1: &Erc20Token = pool.token1.as_ref().unwrap();

    return vec![
        EntityChange {
            entity: "Token".to_string(),
            id: token0.address.clone(),
            ordinal: pool.log_ordinal,
            operation: Operation::Create as i32,
            fields: vec![
                new_field!("id", FieldType::String, token0.address.clone()),
                new_field!("symbol", FieldType::String, token0.symbol.clone()),
                new_field!("name", FieldType::String, token0.name.clone()),
                new_field!("decimals", FieldType::Bigint, token0.decimals.to_string()),
                new_field!(
                    "totalSupply",
                    FieldType::Bigint,
                    token0.total_supply.clone()
                ),
                new_field!(
                    "volume",
                    FieldType::Bigdecimal,
                    BigDecimal::zero().to_string()
                ),
                new_field!(
                    "volumeUSD",
                    FieldType::Bigdecimal,
                    BigDecimal::zero().to_string()
                ),
                new_field!(
                    "untrackedVolumeUSD",
                    FieldType::Bigdecimal,
                    BigDecimal::zero().to_string()
                ),
                new_field!(
                    "feesUSD",
                    FieldType::Bigdecimal,
                    BigDecimal::zero().to_string()
                ),
                new_field!("txCount", FieldType::Bigint, BigInt::zero().to_string()),
                new_field!("poolCount", FieldType::Bigint, BigInt::zero().to_string()),
                new_field!(
                    "totalValueLocked",
                    FieldType::Bigdecimal,
                    BigDecimal::zero().to_string()
                ),
                new_field!(
                    "totalValueLockedUSD",
                    FieldType::Bigdecimal,
                    BigDecimal::zero().to_string()
                ),
                new_field!(
                    "totalValueLockedUSDUntracked",
                    FieldType::Bigdecimal,
                    BigDecimal::zero().to_string()
                ),
                new_field!(
                    "derivedETH",
                    FieldType::Bigdecimal,
                    BigDecimal::zero().to_string()
                ),
                //todo: should the field type be stored as bytes or
                // should we create a new type called array?

                // new_field!(
                //     "whitelistPools",
                //     FieldType::Array,
                //     big_decimal_string_field_value!("0".to_string())
                // ),
            ],
        },
        EntityChange {
            entity: "Token".to_string(),
            id: token1.address.clone(),
            ordinal: pool.log_ordinal,
            operation: Operation::Create as i32,
            fields: vec![
                new_field!("id", FieldType::String, token1.address.clone()),
                new_field!("symbol", FieldType::String, token1.symbol.clone()),
                new_field!("name", FieldType::String, token1.name.clone()),
                new_field!("decimals", FieldType::Bigint, token1.decimals.to_string()),
                new_field!(
                    "totalSupply",
                    FieldType::Bigint,
                    token1.total_supply.clone()
                ),
                new_field!(
                    "volume",
                    FieldType::Bigdecimal,
                    BigDecimal::zero().to_string()
                ),
                new_field!(
                    "volumeUSD",
                    FieldType::Bigdecimal,
                    BigDecimal::zero().to_string()
                ),
                new_field!(
                    "untrackedVolumeUSD",
                    FieldType::Bigdecimal,
                    BigDecimal::zero().to_string()
                ),
                new_field!(
                    "feesUSD",
                    FieldType::Bigdecimal,
                    BigDecimal::zero().to_string()
                ),
                new_field!("txCount", FieldType::Bigint, BigInt::zero().to_string()),
                new_field!("poolCount", FieldType::Bigint, BigInt::zero().to_string()),
                new_field!(
                    "totalValueLocked",
                    FieldType::Bigdecimal,
                    BigDecimal::zero().to_string()
                ),
                new_field!(
                    "totalValueLockedUSD",
                    FieldType::Bigdecimal,
                    BigDecimal::zero().to_string()
                ),
                new_field!(
                    "totalValueLockedUSDUntracked",
                    FieldType::Bigdecimal,
                    BigDecimal::zero().to_string()
                ),
                new_field!(
                    "derivedETH",
                    FieldType::Bigdecimal,
                    BigDecimal::zero().to_string()
                ),
                //todo: should the field type be stored as bytes or
                // should we create a new type called array?

                // new_field!(
                //     "whitelistPools",
                //     FieldType::Array,
                //     big_decimal_string_field_value!("0".to_string())
                // ),
            ],
        },
    ];
}

pub fn swap_volume_token_entity_change(delta: StoreDelta) -> Option<EntityChange> {
    if !delta.key.as_str().starts_with("token:") {
        return None;
    }

    let token_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
    let mut change: EntityChange = EntityChange {
        entity: "Token".to_string(),
        id: token_address,
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };
    match delta.key.as_str().split(":").last().unwrap() {
        "token0" | "token1" => change.fields.push(update_field!(
            "volume",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
        )),
        "usd" => change.fields.push(update_field!(
            "volumeUSD",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
        )),
        "untrackedUSD" => change.fields.push(update_field!(
            "untrackedVolumeUSD",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
        )),
        "feesUSD" => change.fields.push(update_field!(
            "feesUSD",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
        )),
        _ => {
            return None;
        }
    }

    Some(change)
}

pub fn tx_count_token_entity_change(delta: StoreDelta) -> Option<EntityChange> {
    if !delta.key.starts_with("token:") {
        return None;
    }

    let token_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();

    return Some(EntityChange {
        entity: "Token".to_string(),
        id: token_address,
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![update_field!(
            "txCount",
            FieldType::Bigint,
            utils::decode_bytes_to_big_int(delta.old_value).to_string(),
            utils::decode_bytes_to_big_int(delta.new_value).to_string()
        )],
    });
}

pub fn total_value_locked_by_token_token_entity_change(delta: StoreDelta) -> EntityChange {
    let token_address = delta.key.as_str().split(":").nth(2).unwrap().to_string();
    EntityChange {
        entity: "Token".to_string(),
        id: token_address,
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![update_field!(
            "totalValueLocked",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
        )],
    }
}

pub fn total_value_locked_usd_token_entity_change(delta: StoreDelta) -> Option<EntityChange> {
    if !delta.key.starts_with("token:") {
        return None;
    }

    let token_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();

    let mut change: EntityChange = EntityChange {
        entity: "Token".to_string(),
        id: token_address,
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };

    match delta.key.as_str().split(":").last().unwrap() {
        "usd" => change.fields.push(update_field!(
            "totalValueLockedUSD",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
        )),
        _ => return None,
    }

    Some(change)
}

pub fn derived_eth_prices_token_entity_change(delta: StoreDelta) -> Option<EntityChange> {
    if !delta.key.starts_with("token:") {
        return None;
    }

    let token_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();

    let mut change: EntityChange = EntityChange {
        entity: "Token".to_string(),
        id: token_address,
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };

    match delta.key.as_str().split(":").last().unwrap() {
        "eth" => change.fields.push(update_field!(
            "derivedETH",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
        )),
        _ => return None,
    }

    Some(change)
}

// --------------------
//  Map Tick Entities
// --------------------
pub fn ticks_created_tick_entity_change(tick: Tick) -> EntityChange {
    return EntityChange {
        entity: "Tick".to_string(),
        id: tick.id.clone(),
        ordinal: tick.log_ordinal,
        operation: Operation::Create as i32,
        fields: vec![
            new_field!("id", FieldType::String, tick.id),
            new_field!("poolAddress", FieldType::String, tick.pool_address.clone()),
            new_field!("tickIdx", FieldType::Bigint, tick.idx),
            new_field!("pool", FieldType::String, tick.pool_address),
            new_field!(
                "liquidityGross",
                FieldType::Bigint,
                BigInt::zero().to_string()
            ),
            new_field!(
                "liquidityNet",
                FieldType::Bigint,
                BigInt::zero().to_string()
            ),
            new_field!("price0", FieldType::Bigdecimal, tick.price0),
            new_field!("price1", FieldType::Bigdecimal, tick.price1),
            new_field!(
                "volumeToken0",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "volumeToken1",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "volumeUSD",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "untrackedVolumeUSD",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "feesUSD",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "collectedFeesToken0",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "collectedFeesToken1",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "collectedFeesUSD",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "createdAtTimestamp",
                FieldType::Bigint,
                tick.created_at_timestamp.to_string()
            ),
            new_field!(
                "createdAtBlockNumber",
                FieldType::Bigint,
                tick.created_at_block_number.to_string()
            ),
            new_field!(
                "liquidityProviderCount",
                FieldType::Bigint,
                BigInt::zero().to_string()
            ),
            new_field!(
                "feeGrowthOutside0X128",
                FieldType::Bigint,
                tick.fee_growth_outside_0x_128
            ),
            new_field!(
                "feeGrowthOutside1X128",
                FieldType::Bigint,
                tick.fee_growth_outside_1x_128
            ),
        ],
    };
}

pub fn ticks_updated_tick_entity_change(old_tick: Tick, new_tick: Tick) -> EntityChange {
    return EntityChange {
        entity: "Tick".to_string(),
        id: new_tick.id,
        ordinal: new_tick.log_ordinal,
        operation: Operation::Update as i32,
        fields: vec![
            update_field!(
                "feeGrowthOutside0X128",
                FieldType::Bigint,
                old_tick.fee_growth_outside_0x_128,
                new_tick.fee_growth_outside_0x_128
            ),
            update_field!(
                "feeGrowthOutside1X128",
                FieldType::Bigint,
                old_tick.fee_growth_outside_1x_128,
                new_tick.fee_growth_outside_1x_128
            ),
        ],
    };
}

pub fn ticks_liquidities_tick_entity_change(delta: StoreDelta) -> Option<EntityChange> {
    let tick_id = delta.key.as_str().split(":").nth(1).unwrap().to_string();
    let mut change: EntityChange = EntityChange {
        entity: "Tick".to_string(),
        id: tick_id,
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };

    match delta.key.as_str().split(":").last().unwrap() {
        "liquidityNet" => change.fields.push(update_field!(
            "liquidityNet",
            FieldType::Bigint,
            utils::decode_bytes_to_big_int(delta.old_value).to_string(),
            utils::decode_bytes_to_big_int(delta.new_value).to_string()
        )),
        "liquidityGross" => change.fields.push(update_field!(
            "liquidityGross",
            FieldType::Bigint,
            utils::decode_bytes_to_big_int(delta.old_value).to_string(),
            utils::decode_bytes_to_big_int(delta.new_value).to_string()
        )),
        _ => return None,
    }

    Some(change)
}

// --------------------
//  Map Position Entities
// --------------------
pub fn position_create_entity_change(position: Position) -> EntityChange {
    return EntityChange {
        entity: "Position".to_string(),
        id: position.id.clone(),
        ordinal: position.log_ordinal,
        operation: Operation::Create as i32,
        fields: vec![
            new_field!("id", FieldType::String, position.id),
            new_field!("owner", FieldType::String, position.owner),
            new_field!("pool", FieldType::String, position.pool),
            new_field!("token0", FieldType::String, position.token0),
            new_field!("token1", FieldType::String, position.token1),
            new_field!("tickLower", FieldType::String, position.tick_lower),
            new_field!("tickUpper", FieldType::String, position.tick_upper),
            new_field!("liquidity", FieldType::Bigint, BigInt::zero().to_string()),
            new_field!(
                "depositedToken0",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "depositedToken1",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "withdrawnToken0",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "withdrawnToken1",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "collectedFeesToken0",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!(
                "collectedFeesToken1",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            new_field!("transaction", FieldType::String, position.transaction),
            new_field!(
                "feeGrowthInside0LastX128",
                FieldType::Bigint,
                position.fee_growth_inside_0_last_x_128
            ),
            new_field!(
                "feeGrowthInside1LastX128",
                FieldType::Bigint,
                position.fee_growth_inside_1_last_x_128
            ),
        ],
    };
}

pub fn positions_changes_entity_change(delta: StoreDelta) -> Option<EntityChange> {
    let position_id = delta.key.as_str().split(":").nth(1).unwrap().to_string();
    let mut change: EntityChange = EntityChange {
        entity: "Position".to_string(),
        id: position_id,
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };

    let mut name: &str = "";

    match delta.key.as_str().split(":").last().unwrap() {
        "liquidity" => {
            change.fields.push(update_field!(
                "liquidity",
                FieldType::Bigint,
                utils::decode_bytes_to_big_int(delta.old_value).to_string(),
                utils::decode_bytes_to_big_int(delta.new_value).to_string()
            ));
            return Some(change);
        }
        "depositedToken0" => name = "depositedToken0",
        "depositedToken1" => name = "depositedToken1",
        "withdrawnToken0" => name = "withdrawnToken0",
        "withdrawnToken1" => name = "withdrawnToken1",
        "collectedFeesToken0" => name = "collectedFeesToken0",
        "collectedFeesToken1" => name = "collectedFeesToken1",
        _ => return None,
    }

    change.fields.push(update_field!(
        name,
        FieldType::Bigdecimal,
        utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
        utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
    ));

    return Some(change);
}

// --------------------
//  Map Snapshot Position Entities
// --------------------
pub fn snapshot_position_entity_change(snapshot_position: SnapshotPosition) -> EntityChange {
    return EntityChange {
        entity: "PositionSnapshot".to_string(),
        id: snapshot_position.id.clone(),
        ordinal: snapshot_position.log_ordinal,
        operation: Operation::Create as i32,
        fields: vec![
            new_field!("id", FieldType::String, snapshot_position.id),
            new_field!("owner", FieldType::String, snapshot_position.owner),
            new_field!("pool", FieldType::String, snapshot_position.pool),
            new_field!("position", FieldType::String, snapshot_position.position),
            new_field!(
                "blockNumber",
                FieldType::Bigint,
                snapshot_position.block_number.to_string()
            ),
            new_field!(
                "timestamp",
                FieldType::Bigint,
                snapshot_position.timestamp.to_string()
            ),
            new_field!("liquidity", FieldType::Bigint, snapshot_position.liquidity),
            new_field!(
                "depositedToken0",
                FieldType::Bigdecimal,
                snapshot_position.deposited_token0
            ),
            new_field!(
                "depositedToken1",
                FieldType::Bigdecimal,
                snapshot_position.deposited_token1
            ),
            new_field!(
                "withdrawnToken0",
                FieldType::Bigdecimal,
                snapshot_position.withdrawn_token0
            ),
            new_field!(
                "withdrawnToken1",
                FieldType::Bigdecimal,
                snapshot_position.withdrawn_token1
            ),
            new_field!(
                "collectedFeesToken0",
                FieldType::Bigdecimal,
                snapshot_position.collected_fees_token0
            ),
            new_field!(
                "collectedFeesToken1",
                FieldType::Bigdecimal,
                snapshot_position.collected_fees_token1
            ),
            new_field!(
                "transaction",
                FieldType::String,
                snapshot_position.transaction
            ),
            new_field!(
                "feeGrowthInside0LastX128",
                FieldType::Bigint,
                snapshot_position.fee_growth_inside_0_last_x_128
            ),
            new_field!(
                "feeGrowthInside1LastX128",
                FieldType::Bigint,
                snapshot_position.fee_growth_inside_1_last_x_128
            ),
        ],
    };
}

// --------------------
//  Map Transaction Entities
// --------------------
pub fn transaction_entity_change(transaction: Transaction) -> EntityChange {
    return EntityChange {
        entity: "Transaction".to_string(),
        id: transaction.id.clone(),
        ordinal: transaction.log_ordinal,
        operation: Operation::Create as i32,
        fields: vec![
            new_field!("id", FieldType::String, transaction.id),
            new_field!(
                "blockNumber",
                FieldType::Bigint,
                transaction.block_number.to_string()
            ),
            new_field!(
                "timestamp",
                FieldType::Bigint,
                transaction.timestamp.to_string()
            ),
            new_field!(
                "gasUsed",
                FieldType::Bigint,
                transaction.gas_used.to_string()
            ),
            new_field!("gasPrice", FieldType::Bigint, transaction.gas_price),
        ],
    };
}

// --------------------
//  Map Swaps Mints Burns Entities
// --------------------
pub fn swaps_mints_burns_created_entity_change(
    event: Event,
    tx_count_store: &StoreGet,
    store_eth_prices: &StoreGet,
) -> Option<EntityChange> {
    if event.r#type.is_none() {
        return None;
    }

    if event.r#type.is_some() {
        let transaction_count: i32 = match tx_count_store.get_last(keyer::factory_total_tx_count())
        {
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
                    utils::decode_bytes_to_big_decimal(derived_eth_price_bytes)
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
                    utils::decode_bytes_to_big_decimal(derived_eth_price_bytes)
                }
            };

        let bundle_eth_price: BigDecimal =
            match store_eth_prices.get_last(keyer::bundle_eth_price()) {
                None => {
                    // initializePool has occurred beforehand so there should always be a price
                    // maybe just ? instead of returning 1 and bubble up the error if there is one
                    BigDecimal::from(1 as u64)
                }
                Some(bundle_eth_price_bytes) => {
                    utils::decode_bytes_to_big_decimal(bundle_eth_price_bytes)
                }
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

                Some(EntityChange {
                    entity: "Swap".to_string(),
                    id: transaction_id.clone(),
                    ordinal: event.log_ordinal,
                    operation: Operation::Create as i32,
                    fields: vec![
                        new_field!("id", FieldType::String, transaction_id),
                        new_field!("transaction", FieldType::String, event.transaction_id),
                        new_field!("timestamp", FieldType::Bigint, event.timestamp.to_string()),
                        new_field!("pool", FieldType::String, event.pool_address),
                        new_field!("token0", FieldType::String, event.token0),
                        new_field!("token1", FieldType::String, event.token1),
                        new_field!("sender", FieldType::String, swap.sender), // should this be bytes ?
                        new_field!("recipient", FieldType::String, swap.recipient), // should this be bytes ?
                        new_field!("origin", FieldType::String, swap.origin), // should this be bytes ?
                        new_field!("amount0", FieldType::Bigdecimal, swap.amount_0),
                        new_field!("amount1", FieldType::Bigdecimal, swap.amount_1),
                        new_field!("amountUSD", FieldType::Bigdecimal, amount_usd.to_string()),
                        new_field!("sqrtPriceX96", FieldType::Int32, swap.sqrt_price),
                        new_field!("tick", FieldType::Bigint, swap.tick.to_string()),
                        new_field!("logIndex", FieldType::Bigint, event.log_ordinal.to_string()), // not sure if this is good
                    ],
                })
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

                Some(EntityChange {
                    entity: "Mint".to_string(),
                    id: transaction_id.clone(),
                    ordinal: event.log_ordinal,
                    operation: Operation::Create as i32,
                    fields: vec![
                        new_field!("id", FieldType::String, transaction_id),
                        new_field!("transaction", FieldType::String, event.transaction_id),
                        new_field!("timestamp", FieldType::Bigint, event.timestamp.to_string()),
                        new_field!("pool", FieldType::String, event.pool_address),
                        new_field!("token0", FieldType::String, event.token0),
                        new_field!("token1", FieldType::String, event.token1),
                        new_field!("owner", FieldType::String, mint.owner), // should this be bytes ?
                        new_field!("sender", FieldType::String, mint.sender), // should this be bytes ?
                        new_field!("origin", FieldType::String, mint.origin), // should this be bytes ?
                        new_field!("amount", FieldType::Bigint, mint.amount),
                        new_field!("amount0", FieldType::Bigdecimal, mint.amount_0),
                        new_field!("amount1", FieldType::Bigdecimal, mint.amount_1),
                        new_field!("amountUSD", FieldType::Bigdecimal, amount_usd.to_string()),
                        new_field!("tickLower", FieldType::Bigint, mint.tick_lower.to_string()),
                        new_field!("tickUpper", FieldType::Bigint, mint.tick_upper.to_string()),
                        new_field!("logIndex", FieldType::Bigint, event.log_ordinal.to_string()), // not sure if this is good
                    ],
                })
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

                Some(EntityChange {
                    entity: "Burn".to_string(),
                    id: transaction_id.clone(),
                    ordinal: event.log_ordinal,
                    operation: Operation::Create as i32,
                    fields: vec![
                        new_field!("id", FieldType::String, transaction_id),
                        new_field!("transaction", FieldType::String, event.transaction_id),
                        new_field!("timestamp", FieldType::Bigint, event.timestamp.to_string()),
                        new_field!("pool", FieldType::String, event.pool_address),
                        new_field!("token0", FieldType::String, event.token0),
                        new_field!("token1", FieldType::String, event.token1),
                        new_field!("owner", FieldType::String, burn.owner), // should this be bytes ?
                        new_field!("origin", FieldType::String, burn.origin), // should this be bytes ?
                        new_field!("amount", FieldType::Bigint, burn.amount),
                        new_field!("amount0", FieldType::Bigdecimal, burn.amount_0),
                        new_field!("amount1", FieldType::Bigdecimal, burn.amount_1),
                        new_field!("amountUSD", FieldType::Bigdecimal, amount_usd.to_string()),
                        new_field!("tickLower", FieldType::Bigint, burn.tick_lower.to_string()),
                        new_field!("tickUpper", FieldType::Bigint, burn.tick_upper.to_string()),
                        new_field!("logIndex", FieldType::Bigint, event.log_ordinal.to_string()), // not sure if this is good
                    ],
                })
            }
        };
    }
    return None;
}

// --------------------
//  Map Flashes Entities
// --------------------
pub fn flashes_update_pool_fee_entity_change(flash: Flash) -> EntityChange {
    return EntityChange {
        entity: "Pool".to_string(),
        id: flash.pool_address,
        ordinal: flash.log_ordinal,
        operation: Operation::Update as i32,
        fields: vec![
            new_field!(
                "feeGrowthGlobal0X128",
                FieldType::Bigint,
                flash.fee_growth_global_0x_128
            ),
            new_field!(
                "feeGrowthGlobal1X128",
                FieldType::Bigint,
                flash.fee_growth_global_1x_128
            ),
        ],
    };
}

// --------------------
//  Map Uniswap Day Data Entities
// --------------------
pub fn uniswap_day_data_tx_count_entity_change(delta: StoreDelta) -> Option<EntityChange> {
    if !delta.key.starts_with("uniswap_day_data") {
        return None;
    }

    let day_id: i64 = delta
        .key
        .as_str()
        .split(":")
        .last()
        .unwrap()
        .parse::<i64>()
        .unwrap();
    let day_start_timestamp = day_id * 86400;

    return Some(EntityChange {
        entity: "UniswapDayData".to_string(),
        id: day_id.to_string(),
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![
            new_field!("id", FieldType::String, day_id.to_string()),
            new_field!("date", FieldType::Int32, day_start_timestamp.to_string()),
            new_field!(
                "volumeUSDUntracked",
                FieldType::Bigdecimal,
                BigDecimal::zero().to_string()
            ),
            update_field!(
                "txCount",
                FieldType::Bigint,
                utils::decode_bytes_to_big_int(delta.old_value).to_string(),
                utils::decode_bytes_to_big_int(delta.new_value).to_string()
            ),
        ],
    });
}

pub fn uniswap_day_data_totals_entity_change(delta: StoreDelta) -> Option<EntityChange> {
    if !delta.key.starts_with("uniswap_day_data") {
        return None;
    }

    let day_id: i64 = delta
        .key
        .as_str()
        .split(":")
        .last()
        .unwrap()
        .parse::<i64>()
        .unwrap();

    return Some(EntityChange {
        entity: "UniswapDayData".to_string(),
        id: day_id.to_string(),
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![update_field!(
            "tvlUSD",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
        )],
    });
}

pub fn uniswap_day_data_volumes_entity_change(delta: StoreDelta) -> Option<EntityChange> {
    if !delta.key.starts_with("uniswap_day_data") {
        return None;
    }

    let day_id: i64 = delta
        .key
        .as_str()
        .split(":")
        .nth(1)
        .unwrap()
        .parse::<i64>()
        .unwrap();

    let mut change = EntityChange {
        entity: "UniswapDayData".to_string(),
        id: day_id.to_string(),
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };

    match delta.key.as_str().split(":").last().unwrap() {
        "volumeETH" => change.fields.push(update_field!(
            "volumeETH",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
        )),
        "volumeUSD" => change.fields.push(update_field!(
            "volumeUSD",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
        )),
        "feesUSD" => change.fields.push(update_field!(
            "feesUSD",
            FieldType::Bigdecimal,
            utils::decode_bytes_to_big_decimal(delta.old_value).to_string(),
            utils::decode_bytes_to_big_decimal(delta.new_value).to_string()
        )),
        _ => return None,
    }

    Some(change)
}
