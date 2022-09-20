use crate::pb::uniswap::field::Type as FieldType;
use crate::pb::uniswap::Field;
use crate::{
    big_decimal_string_field_value, big_decimal_vec_field_value, big_int_field_value,
    int_field_value, keyer, new_field, string_field_value, update_field, utils, BurnEvent,
    EntityChange, Erc20Token, Event, Flash, MintEvent, Pool, PoolSqrtPrice, Position,
    SnapshotPosition, SnapshotPositions, SwapEvent, Tick, Transaction,
};
use bigdecimal::BigDecimal;
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
        id: string_field_value!("1"),
        ordinal: 1,
        operation: Operation::Create as i32,
        fields: vec![
            new_field!("id", FieldType::String, string_field_value!("1")),
            new_field!(
                "ethPriceUSD",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
        ],
    };
}

pub fn bundle_store_eth_price_usd_bundle_entity_change(
    derived_eth_prices_delta: StoreDelta,
) -> Option<EntityChange> {
    if !derived_eth_prices_delta.key.starts_with("bundle") {
        return None;
    }

    Some(EntityChange {
        entity: "Bundle".to_string(),
        id: string_field_value!("1"),
        ordinal: derived_eth_prices_delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![update_field!(
            "ethPriceUSD",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(derived_eth_prices_delta.old_value),
            big_decimal_vec_field_value!(derived_eth_prices_delta.new_value)
        )],
    })
}

// -------------------
//  Map Factory Entities
// -------------------
pub fn factory_created_factory_entity_change() -> EntityChange {
    return EntityChange {
        entity: "Factory".to_string(),
        id: string_field_value!(Hex(utils::UNISWAP_V3_FACTORY).to_string()),
        ordinal: 1,
        operation: Operation::Create as i32,
        fields: vec![
            new_field!(
                "id",
                FieldType::String,
                string_field_value!(Hex(utils::UNISWAP_V3_FACTORY).to_string())
            ),
            new_field!(
                "poolCount",
                FieldType::Bigint,
                big_int_field_value!(BigInt::from(0 as i32).to_string())
            ),
            new_field!(
                "txCount",
                FieldType::Bigint,
                big_int_field_value!(BigInt::from(0 as i32).to_string())
            ),
            new_field!(
                "totalVolumeUSD",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "totalVolumeETH",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "totalFeesUSD",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "totalFeesETH",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "untrackedVolumeUSD",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "totalValueLockedUSD",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "totalValueLockedETH",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "totalValueLockedUSDUntracked",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "totalValueLockedETHUntracked",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "owner",
                FieldType::String,
                string_field_value!(Hex(utils::ZERO_ADDRESS).to_string())
            ),
        ],
    };
}

pub fn pool_created_factory_entity_change(pool_count_delta: StoreDelta) -> EntityChange {
    return EntityChange {
        entity: "Factory".to_string(),
        id: string_field_value!(Hex(utils::UNISWAP_V3_FACTORY).to_string()),
        ordinal: pool_count_delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![update_field!(
            "poolCount",
            FieldType::Bigint,
            big_int_field_value!(
                BigInt::from_signed_bytes_be(pool_count_delta.old_value.as_ref()).to_string()
            ),
            big_int_field_value!(
                BigInt::from_signed_bytes_be(pool_count_delta.new_value.as_ref()).to_string()
            )
        )],
    };
}

pub fn tx_count_factory_entity_change(tx_count_delta: StoreDelta) -> Option<EntityChange> {
    if !tx_count_delta.key.starts_with("factory:") {
        return None;
    }

    return Some(EntityChange {
        entity: "Factory".to_string(),
        id: string_field_value!(Hex(utils::UNISWAP_V3_FACTORY).to_string()),
        ordinal: tx_count_delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![update_field!(
            "txCount",
            FieldType::Bigint,
            big_int_field_value!(
                BigInt::from_signed_bytes_be(tx_count_delta.old_value.as_ref()).to_string()
            ),
            big_int_field_value!(
                BigInt::from_signed_bytes_be(tx_count_delta.new_value.as_ref()).to_string()
            )
        )],
    });
}

pub fn swap_volume_factory_entity_change(swap_volume_delta: StoreDelta) -> Option<EntityChange> {
    let mut change: EntityChange = EntityChange {
        entity: "Factory".to_string(),
        id: string_field_value!(Hex(utils::UNISWAP_V3_FACTORY).to_string()),
        ordinal: swap_volume_delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };
    match swap_volume_delta.key.as_str().split(":").last().unwrap() {
        "usd" => change.fields.push(update_field!(
            "totalVolumeUSD",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(swap_volume_delta.old_value),
            big_decimal_vec_field_value!(swap_volume_delta.new_value)
        )),
        "untrackedUSD" => change.fields.push(update_field!(
            "untrackedVolumeUSD",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(swap_volume_delta.old_value),
            big_decimal_vec_field_value!(swap_volume_delta.new_value)
        )),
        "feesUSD" => change.fields.push(update_field!(
            "totalFeesUSD",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(swap_volume_delta.old_value),
            big_decimal_vec_field_value!(swap_volume_delta.new_value)
        )),
        "totalVolumeETH" => change.fields.push(update_field!(
            "totalVolumeETH",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(swap_volume_delta.old_value),
            big_decimal_vec_field_value!(swap_volume_delta.new_value)
        )),
        "totalFeesETH" => change.fields.push(update_field!(
            "totalFeesETH",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(swap_volume_delta.old_value),
            big_decimal_vec_field_value!(swap_volume_delta.new_value)
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
        id: string_field_value!(Hex(utils::UNISWAP_V3_FACTORY).to_string()),
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
            big_decimal_vec_field_value!(total_value_locked_delta.old_value),
            big_decimal_vec_field_value!(total_value_locked_delta.new_value)
        )),
        "totalValueLockedETH" => change.fields.push(update_field!(
            "totalValueLockedETH",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(total_value_locked_delta.old_value),
            big_decimal_vec_field_value!(total_value_locked_delta.new_value)
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
        id: string_field_value!(pool.address),
        ordinal: pool.log_ordinal,
        operation: Operation::Create as i32,
        fields: vec![
            new_field!("id", FieldType::String, string_field_value!(pool.address)),
            new_field!(
                "createdAtTimestamp",
                FieldType::Bigint,
                big_int_field_value!(pool.created_at_timestamp)
            ),
            new_field!(
                "createdAtBlockNumber",
                FieldType::Bigint,
                big_int_field_value!(pool.created_at_block_number)
            ),
            new_field!(
                "token0",
                FieldType::String,
                string_field_value!(pool.token0.unwrap().address)
            ),
            new_field!(
                "token1",
                FieldType::String,
                string_field_value!(pool.token1.unwrap().address)
            ),
            new_field!(
                "feeTier",
                FieldType::Bigint,
                big_int_field_value!(pool.fee_tier.to_string())
            ),
            new_field!(
                "liquidity",
                FieldType::Bigint,
                big_int_field_value!(BigInt::from(0 as i32).to_string())
            ),
            new_field!(
                "sqrtPrice",
                FieldType::Bigint,
                big_int_field_value!(BigInt::from(0 as i32).to_string())
            ),
            new_field!(
                "feeGrowthGlobal0X128",
                FieldType::Bigint,
                big_int_field_value!(BigInt::from(0 as i32).to_string())
            ),
            new_field!(
                "feeGrowthGlobal1X128",
                FieldType::Bigint,
                big_int_field_value!(BigInt::from(0 as i32).to_string())
            ),
            new_field!(
                "token0Price",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "token1Price",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "tick",
                FieldType::Bigint,
                big_int_field_value!(BigInt::from(0 as i32).to_string())
            ),
            new_field!(
                "observationIndex",
                FieldType::Bigint,
                big_int_field_value!(BigInt::from(0 as i32).to_string())
            ),
            new_field!(
                "volumeToken0",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "volumeToken1",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "volumeUSD",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "untrackedVolumeUSD",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "feesUSD",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "txCount",
                FieldType::Bigint,
                big_int_field_value!(BigInt::from(0 as i32).to_string())
            ),
            new_field!(
                "collectedFeesToken0",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "collectedFeesToken1",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "collectedFeesUSD",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "totalValueLockedToken0",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "totalValueLockedToken1",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "totalValueLockedETH",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "totalValueLockedUSD",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "totalValueLockedUSDUntracked",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "liquidityProviderCount",
                FieldType::Bigint,
                big_int_field_value!(BigInt::from(0 as i32).to_string())
            ),
        ],
    };
}

pub fn pool_sqrt_price_entity_change(delta: StoreDelta) -> Option<EntityChange> {
    let new_value: PoolSqrtPrice = proto::decode(&delta.new_value).unwrap();

    let mut change = EntityChange {
        entity: "Pool".to_string(),
        id: string_field_value!(delta.key.as_str().split(":").nth(1).unwrap()),
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };

    match delta.operation {
        1 => {
            change.fields.push(update_field!(
                "sqrtPrice",
                FieldType::Bigint,
                big_int_field_value!("0".to_string()),
                big_int_field_value!(new_value.sqrt_price)
            ));
            change.fields.push(update_field!(
                "tick",
                FieldType::Bigint,
                big_int_field_value!("0".to_string()),
                big_int_field_value!(new_value.tick)
            ));
        }
        2 => {
            let old_value: PoolSqrtPrice = proto::decode(&delta.new_value).unwrap();
            change.fields.push(update_field!(
                "sqrtPrice",
                FieldType::Bigint,
                big_int_field_value!(old_value.sqrt_price),
                big_int_field_value!(new_value.sqrt_price)
            ));
            change.fields.push(update_field!(
                "tick",
                FieldType::Bigint,
                big_int_field_value!(old_value.tick),
                big_int_field_value!(new_value.tick)
            ));
        }
        _ => return None,
    }

    Some(change)
}

pub fn pool_liquidities_pool_entity_change(delta: StoreDelta) -> EntityChange {
    return EntityChange {
        entity: "Pool".to_string(),
        id: string_field_value!(delta.key.as_str().split(":").nth(1).unwrap()),
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![update_field!(
            "liquidity",
            FieldType::Bigint,
            big_int_field_value!(BigInt::from_signed_bytes_be(delta.old_value.as_ref()).to_string()),
            big_int_field_value!(BigInt::from_signed_bytes_be(delta.new_value.as_ref()).to_string())
        )],
    };
}

pub fn total_value_locked_pool_entity_change(delta: StoreDelta) -> Option<EntityChange> {
    if !delta.key.starts_with("pool:") {
        return None;
    }

    let mut change: EntityChange = EntityChange {
        entity: "Pool".to_string(),
        id: string_field_value!(delta.key.as_str().split(":").nth(1).unwrap()),
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };

    match delta.key.as_str().split(":").last().unwrap() {
        "usd" => change.fields.push(update_field!(
            "totalValueLockedUSD",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(delta.old_value),
            big_decimal_vec_field_value!(delta.new_value)
        )),
        "eth" => change.fields.push(update_field!(
            "totalValueLockedETH",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(delta.old_value),
            big_decimal_vec_field_value!(delta.new_value)
        )),
        _ => {}
    }

    Some(change)
}

pub fn total_value_locked_by_token_pool_entity_change(delta: StoreDelta) -> Option<EntityChange> {
    let mut change: EntityChange = EntityChange {
        entity: "Pool".to_string(),
        id: string_field_value!(delta.key.as_str().split(":").nth(1).unwrap()),
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };

    match delta.key.as_str().split(":").last().unwrap() {
        "token0" => change.fields.push(update_field!(
            "totalValueLockedToken0",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(delta.old_value),
            big_decimal_vec_field_value!(delta.new_value)
        )),
        "token1" => change.fields.push(update_field!(
            "totalValueLockedToken1",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(delta.old_value),
            big_decimal_vec_field_value!(delta.new_value)
        )),
        _ => return None,
    }

    Some(change)
}

pub fn pool_fee_growth_global_x128_entity_change(delta: StoreDelta) -> Option<EntityChange> {
    let mut change: EntityChange = EntityChange {
        entity: "Pool".to_string(),
        id: string_field_value!(delta.key.as_str().split(":").nth(1).unwrap()),
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };

    match delta.key.as_str().split(":").last().unwrap() {
        "token0" => {
            change.fields.push(update_field!(
                "feeGrowthGlobal0X128",
                FieldType::Bigint,
                big_int_field_value!(
                    BigInt::from_signed_bytes_be(delta.old_value.as_ref()).to_string()
                ),
                big_int_field_value!(
                    BigInt::from_signed_bytes_be(delta.new_value.as_ref()).to_string()
                )
            ))
        }
        "token1" => {
            change.fields.push(update_field!(
                "feeGrowthGlobal1X128",
                FieldType::Bigint,
                big_int_field_value!(
                    BigInt::from_signed_bytes_be(delta.old_value.as_ref()).to_string()
                ),
                big_int_field_value!(
                    BigInt::from_signed_bytes_be(delta.new_value.as_ref()).to_string()
                )
            ))
        }
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
        id: string_field_value!(pool_address),
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };

    match delta.operation {
        1 => {
            change.fields.push(update_field!(
                field_name,
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string()),
                big_decimal_vec_field_value!(delta.new_value)
            ));
        }
        2 => {
            change.fields.push(update_field!(
                field_name,
                FieldType::Bigdecimal,
                big_decimal_vec_field_value!(delta.old_value),
                big_decimal_vec_field_value!(delta.new_value)
            ));
        }
        _ => return None,
    }

    Some(change)
}

pub fn tx_count_pool_entity_change(tx_count_delta: StoreDelta) -> Option<EntityChange> {
    if !tx_count_delta.key.starts_with("pool:") {
        return None;
    }

    return Some(EntityChange {
        entity: "Pool".to_string(),
        id: string_field_value!(tx_count_delta.key.as_str().split(":").nth(1).unwrap()),
        ordinal: tx_count_delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![update_field!(
            "txCount",
            FieldType::Bigint,
            big_int_field_value!(
                BigInt::from_signed_bytes_be(tx_count_delta.old_value.as_ref()).to_string()
            ),
            big_int_field_value!(
                BigInt::from_signed_bytes_be(tx_count_delta.new_value.as_ref()).to_string()
            )
        )],
    });
}

pub fn swap_volume_pool_entity_change(delta: StoreDelta) -> Option<EntityChange> {
    if !delta.key.as_str().starts_with("swap") {
        return None;
    }

    let mut change: EntityChange = EntityChange {
        entity: "Pool".to_string(),
        id: string_field_value!(delta.key.as_str().split(":").nth(1).unwrap()),
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };
    match delta.key.as_str().split(":").last().unwrap() {
        "token0" => change.fields.push(update_field!(
            "volumeToken0",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(delta.old_value),
            big_decimal_vec_field_value!(delta.new_value)
        )),
        "token1" => change.fields.push(update_field!(
            "volumeToken1",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(delta.old_value),
            big_decimal_vec_field_value!(delta.new_value)
        )),
        "usd" => change.fields.push(update_field!(
            "volumeUSD",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(delta.old_value),
            big_decimal_vec_field_value!(delta.new_value)
        )),
        "untrackedUSD" => change.fields.push(update_field!(
            "untrackedVolumeUSD",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(delta.old_value),
            big_decimal_vec_field_value!(delta.new_value)
        )),
        "feesUSD" => change.fields.push(update_field!(
            "feesUSD",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(delta.old_value),
            big_decimal_vec_field_value!(delta.new_value)
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
    // create 2 entity changes
    return vec![
        EntityChange {
            entity: "Token".to_string(),
            id: string_field_value!(pool.address),
            ordinal: pool.log_ordinal,
            operation: Operation::Create as i32,
            fields: vec![
                new_field!("id", FieldType::String, string_field_value!(token0.address)),
                new_field!(
                    "symbol",
                    FieldType::String,
                    string_field_value!(token0.symbol)
                ),
                new_field!("name", FieldType::String, string_field_value!(token0.name)),
                new_field!(
                    "decimals",
                    FieldType::Bigint,
                    big_int_field_value!(token0.decimals.to_string())
                ),
                new_field!(
                    "totalSupply",
                    FieldType::Bigint,
                    big_int_field_value!(token0.total_supply)
                ),
                new_field!(
                    "volume",
                    FieldType::Bigdecimal,
                    big_decimal_string_field_value!("0".to_string())
                ),
                new_field!(
                    "volumeUSD",
                    FieldType::Bigdecimal,
                    big_decimal_string_field_value!("0".to_string())
                ),
                new_field!(
                    "untrackedVolumeUSD",
                    FieldType::Bigdecimal,
                    big_decimal_string_field_value!("0".to_string())
                ),
                new_field!(
                    "feesUSD",
                    FieldType::Bigdecimal,
                    big_decimal_string_field_value!("0".to_string())
                ),
                new_field!(
                    "txCount",
                    FieldType::Bigint,
                    big_int_field_value!(BigInt::from(0 as i32).to_string())
                ),
                new_field!(
                    "poolCount",
                    FieldType::Bigint,
                    big_int_field_value!(BigInt::from(0 as i32).to_string())
                ),
                new_field!(
                    "totalValueLocked",
                    FieldType::Bigdecimal,
                    big_decimal_string_field_value!("0".to_string())
                ),
                new_field!(
                    "totalValueLockedUSD",
                    FieldType::Bigdecimal,
                    big_decimal_string_field_value!("0".to_string())
                ),
                new_field!(
                    "totalValueLockedUSDUntracked",
                    FieldType::Bigdecimal,
                    big_decimal_string_field_value!("0".to_string())
                ),
                new_field!(
                    "derivedETH",
                    FieldType::Bigdecimal,
                    big_decimal_string_field_value!("0".to_string())
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
            id: string_field_value!(pool.address),
            ordinal: pool.log_ordinal,
            operation: Operation::Create as i32,
            fields: vec![
                new_field!("id", FieldType::String, string_field_value!(token1.address)),
                new_field!(
                    "symbol",
                    FieldType::String,
                    string_field_value!(token1.symbol)
                ),
                new_field!("name", FieldType::String, string_field_value!(token1.name)),
                new_field!(
                    "decimals",
                    FieldType::Bigint,
                    big_int_field_value!(token1.decimals.to_string())
                ),
                new_field!(
                    "totalSupply",
                    FieldType::Bigint,
                    big_int_field_value!(token1.total_supply)
                ),
                new_field!(
                    "volume",
                    FieldType::Bigdecimal,
                    big_decimal_string_field_value!("0".to_string())
                ),
                new_field!(
                    "volumeUSD",
                    FieldType::Bigdecimal,
                    big_decimal_string_field_value!("0".to_string())
                ),
                new_field!(
                    "untrackedVolumeUSD",
                    FieldType::Bigdecimal,
                    big_decimal_string_field_value!("0".to_string())
                ),
                new_field!(
                    "feesUSD",
                    FieldType::Bigdecimal,
                    big_decimal_string_field_value!("0".to_string())
                ),
                new_field!(
                    "txCount",
                    FieldType::Bigint,
                    big_int_field_value!(BigInt::from(0 as i32).to_string())
                ),
                new_field!(
                    "poolCount",
                    FieldType::Bigint,
                    big_int_field_value!(BigInt::from(0 as i32).to_string())
                ),
                new_field!(
                    "totalValueLocked",
                    FieldType::Bigdecimal,
                    big_decimal_string_field_value!("0".to_string())
                ),
                new_field!(
                    "totalValueLockedUSD",
                    FieldType::Bigdecimal,
                    big_decimal_string_field_value!("0".to_string())
                ),
                new_field!(
                    "totalValueLockedUSDUntracked",
                    FieldType::Bigdecimal,
                    big_decimal_string_field_value!("0".to_string())
                ),
                new_field!(
                    "derivedETH",
                    FieldType::Bigdecimal,
                    big_decimal_string_field_value!("0".to_string())
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

/// key -> swap:{pool_id}:volume:{token_id}:(token0/token1)
pub fn swap_volume_token_entity_change(swap_volume_delta: StoreDelta) -> Option<EntityChange> {
    let mut change: EntityChange = EntityChange {
        entity: "Token".to_string(),
        id: string_field_value!(swap_volume_delta.key.as_str().split(":").nth(0).unwrap()),
        ordinal: swap_volume_delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };
    match swap_volume_delta.key.as_str().split(":").last().unwrap() {
        "token0" | "token1" => change.fields.push(update_field!(
            "volume",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(swap_volume_delta.old_value),
            big_decimal_vec_field_value!(swap_volume_delta.new_value)
        )),
        "usd" => change.fields.push(update_field!(
            "volumeUSD",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(swap_volume_delta.old_value),
            big_decimal_vec_field_value!(swap_volume_delta.new_value)
        )),
        "untrackedUSD" => change.fields.push(update_field!(
            "untrackedVolumeUSD",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(swap_volume_delta.old_value),
            big_decimal_vec_field_value!(swap_volume_delta.new_value)
        )),
        "feesUSD" => change.fields.push(update_field!(
            "feesUSD",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(swap_volume_delta.old_value),
            big_decimal_vec_field_value!(swap_volume_delta.new_value)
        )),
        _ => {
            return None;
        }
    }

    Some(change)
}

pub fn tx_count_token_entity_change(tx_count_delta: StoreDelta) -> Option<EntityChange> {
    if !tx_count_delta.key.starts_with("token:") {
        return None;
    }

    return Some(EntityChange {
        entity: "Token".to_string(),
        id: string_field_value!(tx_count_delta.key.as_str().split(":").nth(1).unwrap()),
        ordinal: tx_count_delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![update_field!(
            "txCount",
            FieldType::Bigint,
            big_int_field_value!(
                BigInt::from_signed_bytes_be(tx_count_delta.old_value.as_ref()).to_string()
            ),
            big_int_field_value!(
                BigInt::from_signed_bytes_be(tx_count_delta.new_value.as_ref()).to_string()
            )
        )],
    });
}

pub fn total_value_locked_by_token_token_entity_change(
    total_value_locked_by_tokens_delta: StoreDelta,
) -> EntityChange {
    EntityChange {
        entity: "Token".to_string(),
        id: string_field_value!(total_value_locked_by_tokens_delta
            .key
            .as_str()
            .split(":")
            .nth(2)
            .unwrap()),
        ordinal: total_value_locked_by_tokens_delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![update_field!(
            "totalValueLocked",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(total_value_locked_by_tokens_delta.old_value),
            big_decimal_vec_field_value!(total_value_locked_by_tokens_delta.new_value)
        )],
    }
}

pub fn total_value_locked_usd_token_entity_change(
    total_value_locked_usd_delta: StoreDelta,
) -> Option<EntityChange> {
    if !total_value_locked_usd_delta.key.starts_with("token:") {
        return None;
    }

    let mut change: EntityChange = EntityChange {
        entity: "Token".to_string(),
        id: string_field_value!(total_value_locked_usd_delta
            .key
            .as_str()
            .split(":")
            .nth(1)
            .unwrap()),
        ordinal: total_value_locked_usd_delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };

    match total_value_locked_usd_delta
        .key
        .as_str()
        .split(":")
        .last()
        .unwrap()
    {
        "usd" => change.fields.push(update_field!(
            "totalValueLockedUSD",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(total_value_locked_usd_delta.old_value),
            big_decimal_vec_field_value!(total_value_locked_usd_delta.new_value)
        )),
        _ => return None,
    }

    Some(change)
}

pub fn derived_eth_prices_token_entity_change(
    derived_eth_prices_delta: StoreDelta,
) -> Option<EntityChange> {
    if !derived_eth_prices_delta.key.starts_with("token:") {
        return None;
    }

    let mut change: EntityChange = EntityChange {
        entity: "Token".to_string(),
        id: string_field_value!(derived_eth_prices_delta
            .key
            .as_str()
            .split(":")
            .nth(1)
            .unwrap()),
        ordinal: derived_eth_prices_delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };

    match derived_eth_prices_delta
        .key
        .as_str()
        .split(":")
        .last()
        .unwrap()
    {
        "eth" => change.fields.push(update_field!(
            "derivedETH",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(derived_eth_prices_delta.old_value),
            big_decimal_vec_field_value!(derived_eth_prices_delta.new_value)
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
        id: string_field_value!(tick.id),
        ordinal: tick.log_ordinal,
        operation: Operation::Create as i32,
        fields: vec![
            new_field!("id", FieldType::String, string_field_value!(tick.id)),
            new_field!(
                "poolAddress",
                FieldType::String,
                string_field_value!(tick.pool_address)
            ),
            new_field!("tickIdx", FieldType::Bigint, big_int_field_value!(tick.idx)),
            new_field!(
                "pool",
                FieldType::String,
                string_field_value!(tick.pool_address)
            ),
            new_field!(
                "liquidityGross",
                FieldType::Bigint,
                big_int_field_value!(BigInt::from(0 as i32).to_string())
            ),
            new_field!(
                "liquidityNet",
                FieldType::Bigint,
                big_int_field_value!(BigInt::from(0 as i32).to_string())
            ),
            new_field!(
                "price0",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!(tick.price0)
            ),
            new_field!(
                "price1",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!(tick.price1)
            ),
            new_field!(
                "volumeToken0",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "volumeToken1",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "volumeUSD",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "untrackedVolumeUSD",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "feesUSD",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "collectedFeesToken0",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "collectedFeesToken1",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "collectedFeesUSD",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            new_field!(
                "createdAtTimestamp",
                FieldType::Bigint,
                big_int_field_value!(tick.created_at_timestamp.to_string())
            ),
            new_field!(
                "createdAtBlockNumber",
                FieldType::Bigint,
                big_int_field_value!(tick.created_at_block_number.to_string())
            ),
            new_field!(
                "liquidityProviderCount",
                FieldType::Bigint,
                big_int_field_value!(BigInt::from(0 as i32).to_string())
            ),
            new_field!(
                "feeGrowthOutside0X128",
                FieldType::Bigint,
                big_int_field_value!(tick.fee_growth_outside_0x_128)
            ),
            new_field!(
                "feeGrowthOutside1X128",
                FieldType::Bigint,
                big_int_field_value!(tick.fee_growth_outside_1x_128)
            ),
        ],
    };
}

pub fn ticks_updated_tick_entity_change(old_tick: Tick, new_tick: Tick) -> EntityChange {
    return EntityChange {
        entity: "Tick".to_string(),
        id: string_field_value!(new_tick.id),
        ordinal: new_tick.log_ordinal,
        operation: Operation::Update as i32,
        fields: vec![
            update_field!(
                "feeGrowthOutside0X128",
                FieldType::Bigint,
                big_int_field_value!(old_tick.fee_growth_outside_0x_128),
                big_int_field_value!(new_tick.fee_growth_outside_0x_128)
            ),
            update_field!(
                "feeGrowthOutside1X128",
                FieldType::Bigint,
                big_int_field_value!(old_tick.fee_growth_outside_1x_128),
                big_int_field_value!(new_tick.fee_growth_outside_1x_128)
            ),
        ],
    };
}

pub fn ticks_liquidities_tick_entity_change(delta: StoreDelta) -> Option<EntityChange> {
    let mut change: EntityChange = EntityChange {
        entity: "Tick".to_string(),
        id: string_field_value!(delta.key.as_str().split(":").nth(1).unwrap()),
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };

    match delta.key.as_str().split(":").last().unwrap() {
        "liquidityNet" => {
            change.fields.push(update_field!(
                "liquidityNet",
                FieldType::Bigint,
                big_int_field_value!(
                    BigInt::from_signed_bytes_be(delta.old_value.as_ref()).to_string()
                ),
                big_int_field_value!(
                    BigInt::from_signed_bytes_be(delta.new_value.as_ref()).to_string()
                )
            ))
        }
        "liquidityGross" => {
            change.fields.push(update_field!(
                "liquidityGross",
                FieldType::Bigint,
                big_int_field_value!(
                    BigInt::from_signed_bytes_be(delta.old_value.as_ref()).to_string()
                ),
                big_int_field_value!(
                    BigInt::from_signed_bytes_be(delta.new_value.as_ref()).to_string()
                )
            ))
        }
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
        id: string_field_value!(position.id),
        ordinal: position.log_ordinal,
        operation: Operation::Create as i32,
        fields: vec![
            new_field!("id", FieldType::String, string_field_value!(position.id)),
            new_field!(
                "owner",
                FieldType::String,
                string_field_value!(position.owner)
            ),
            new_field!(
                "pool",
                FieldType::String,
                string_field_value!(position.pool)
            ),
            new_field!(
                "token0",
                FieldType::String,
                string_field_value!(position.token0)
            ),
            new_field!(
                "token1",
                FieldType::String,
                string_field_value!(position.token1)
            ),
            new_field!(
                "tickLower",
                FieldType::String,
                string_field_value!(position.tick_lower)
            ),
            new_field!(
                "tickUpper",
                FieldType::String,
                string_field_value!(position.tick_upper)
            ),
            new_field!(
                "transaction",
                FieldType::String,
                string_field_value!(position.transaction)
            ),
            new_field!(
                "feeGrowthInside0LastX128",
                FieldType::Bigint,
                big_int_field_value!(position.fee_growth_inside_0_last_x_128)
            ),
            new_field!(
                "feeGrowthInside1LastX128",
                FieldType::Bigint,
                big_int_field_value!(position.fee_growth_inside_1_last_x_128)
            ),
        ],
    };
}

pub fn positions_changes_entity_change(delta: StoreDelta) -> Option<EntityChange> {
    let mut change: EntityChange = EntityChange {
        entity: "Position".to_string(),
        id: string_field_value!(delta.key.as_str().split(":").nth(1).unwrap()),
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
                big_int_field_value!(
                    BigInt::from_signed_bytes_be(delta.old_value.as_ref()).to_string()
                ),
                big_int_field_value!(
                    BigInt::from_signed_bytes_be(delta.new_value.as_ref()).to_string()
                )
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
        big_decimal_vec_field_value!(delta.old_value),
        big_decimal_vec_field_value!(delta.new_value)
    ));

    return Some(change);
}

// --------------------
//  Map Snapshot Position Entities
// --------------------
pub fn snapshot_position_entity_change(snapshot_position: SnapshotPosition) -> EntityChange {
    return EntityChange {
        entity: "".to_string(),
        id: vec![],
        ordinal: 0,
        operation: 0,
        fields: vec![
            new_field!(
                "id",
                FieldType::String,
                string_field_value!(snapshot_position.id)
            ),
            new_field!(
                "owner",
                FieldType::String,
                string_field_value!(snapshot_position.owner)
            ),
            new_field!(
                "pool",
                FieldType::String,
                string_field_value!(snapshot_position.pool)
            ),
            new_field!(
                "position",
                FieldType::String,
                string_field_value!(snapshot_position.position)
            ),
            new_field!(
                "blockNumber",
                FieldType::Bigint,
                big_int_field_value!(snapshot_position.block_number.to_string())
            ),
            new_field!(
                "timestamp",
                FieldType::Bigint,
                big_int_field_value!(snapshot_position.timestamp.to_string())
            ),
            new_field!(
                "liquidity",
                FieldType::Bigint,
                big_int_field_value!(BigDecimal::from_str(snapshot_position.liquidity.as_str())
                    .unwrap()
                    .to_bigint()
                    .unwrap()
                    .to_string())
            ),
            new_field!(
                "depositedToken0",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!(snapshot_position.deposited_token0)
            ),
            new_field!(
                "depositedToken1",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!(snapshot_position.deposited_token1)
            ),
            new_field!(
                "withdrawnToken0",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!(snapshot_position.withdrawn_token0)
            ),
            new_field!(
                "withdrawnToken1",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!(snapshot_position.withdrawn_token1)
            ),
            new_field!(
                "collectedFeesToken0",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!(snapshot_position.collected_fees_token0)
            ),
            new_field!(
                "collectedFeesToken1",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!(snapshot_position.collected_fees_token1)
            ),
            new_field!(
                "transaction",
                FieldType::String,
                string_field_value!(snapshot_position.transaction)
            ),
            new_field!(
                "feeGrowthInside0LastX128",
                FieldType::Bigint,
                big_int_field_value!(snapshot_position.fee_growth_inside_0_last_x_128)
            ),
            new_field!(
                "feeGrowthInside1LastX128",
                FieldType::Bigint,
                big_int_field_value!(snapshot_position.fee_growth_inside_1_last_x_128)
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
        id: string_field_value!(transaction.id),
        ordinal: transaction.log_ordinal,
        operation: Operation::Create as i32,
        fields: vec![
            new_field!("id", FieldType::String, string_field_value!(transaction.id)),
            new_field!(
                "blockNumber",
                FieldType::Bigint,
                big_int_field_value!(BigInt::from(transaction.block_number).to_string())
            ),
            new_field!(
                "timestamp",
                FieldType::Bigint,
                big_int_field_value!(BigInt::from(transaction.timestamp).to_string())
            ),
            new_field!(
                "gasUsed",
                FieldType::Bigint,
                big_int_field_value!(BigInt::from(transaction.gas_used).to_string())
            ),
            new_field!(
                "gasPrice",
                FieldType::Bigint,
                big_int_field_value!(transaction.gas_price)
            ),
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
                    id: string_field_value!(transaction_id),
                    ordinal: event.log_ordinal,
                    operation: Operation::Create as i32,
                    fields: vec![
                        new_field!("id", FieldType::String, string_field_value!(transaction_id)),
                        new_field!(
                            "transaction",
                            FieldType::String,
                            string_field_value!(event.transaction_id)
                        ),
                        new_field!(
                            "timestamp",
                            FieldType::Bigint,
                            big_int_field_value!(event.timestamp.to_string())
                        ),
                        new_field!(
                            "pool",
                            FieldType::String,
                            string_field_value!(event.pool_address)
                        ),
                        new_field!(
                            "token0",
                            FieldType::String,
                            string_field_value!(event.token0)
                        ),
                        new_field!(
                            "token1",
                            FieldType::String,
                            string_field_value!(event.token1)
                        ),
                        new_field!(
                            "sender",
                            FieldType::String,
                            string_field_value!(swap.sender)
                        ),
                        new_field!(
                            "recipient",
                            FieldType::String,
                            string_field_value!(swap.recipient)
                        ),
                        new_field!(
                            "origin",
                            FieldType::String,
                            string_field_value!(swap.origin)
                        ),
                        new_field!(
                            "amount0",
                            FieldType::String,
                            string_field_value!(swap.amount_0)
                        ),
                        new_field!(
                            "amount1",
                            FieldType::String,
                            string_field_value!(swap.amount_1)
                        ),
                        new_field!(
                            "amountUSD",
                            FieldType::String,
                            string_field_value!(amount_usd.to_string())
                        ),
                        new_field!(
                            "sqrtPriceX96",
                            FieldType::Int,
                            string_field_value!(swap.sqrt_price)
                        ),
                        new_field!("tick", FieldType::Int, int_field_value!(swap.tick)),
                        new_field!(
                            "logIndex",
                            FieldType::String,
                            string_field_value!(event.log_ordinal.to_string())
                        ),
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
                    id: string_field_value!(transaction_id),
                    ordinal: event.log_ordinal,
                    operation: Operation::Create as i32,
                    fields: vec![
                        new_field!("id", FieldType::String, string_field_value!(transaction_id)),
                        new_field!(
                            "transaction",
                            FieldType::String,
                            string_field_value!(event.transaction_id)
                        ),
                        new_field!(
                            "timestamp",
                            FieldType::Bigint,
                            big_int_field_value!(event.timestamp.to_string())
                        ),
                        new_field!(
                            "pool",
                            FieldType::String,
                            string_field_value!(event.pool_address)
                        ),
                        new_field!(
                            "token0",
                            FieldType::String,
                            string_field_value!(event.token0)
                        ),
                        new_field!(
                            "token1",
                            FieldType::String,
                            string_field_value!(event.token1)
                        ),
                        new_field!("owner", FieldType::String, string_field_value!(mint.owner)),
                        new_field!(
                            "sender",
                            FieldType::String,
                            string_field_value!(mint.sender)
                        ),
                        new_field!(
                            "origin",
                            FieldType::String,
                            string_field_value!(mint.origin)
                        ),
                        new_field!(
                            "amount",
                            FieldType::String,
                            string_field_value!(mint.amount)
                        ),
                        new_field!(
                            "amount0",
                            FieldType::String,
                            string_field_value!(mint.amount_0)
                        ),
                        new_field!(
                            "amount1",
                            FieldType::String,
                            string_field_value!(mint.amount_1)
                        ),
                        new_field!(
                            "amountUSD",
                            FieldType::String,
                            string_field_value!(amount_usd.to_string())
                        ),
                        new_field!(
                            "tickLower",
                            FieldType::String,
                            string_field_value!(mint.tick_lower.to_string())
                        ),
                        new_field!(
                            "tickUpper",
                            FieldType::String,
                            string_field_value!(mint.tick_upper.to_string())
                        ),
                        new_field!(
                            "logIndex",
                            FieldType::String,
                            string_field_value!(event.log_ordinal.to_string())
                        ),
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
                    id: string_field_value!(transaction_id),
                    ordinal: event.log_ordinal,
                    operation: Operation::Create as i32,
                    fields: vec![
                        new_field!("id", FieldType::String, string_field_value!(transaction_id)),
                        new_field!(
                            "transaction",
                            FieldType::String,
                            string_field_value!(event.transaction_id)
                        ),
                        new_field!(
                            "timestamp",
                            FieldType::Bigint,
                            big_int_field_value!(event.timestamp.to_string())
                        ),
                        new_field!(
                            "pool",
                            FieldType::String,
                            string_field_value!(event.pool_address)
                        ),
                        new_field!(
                            "token0",
                            FieldType::String,
                            string_field_value!(event.token0)
                        ),
                        new_field!(
                            "token1",
                            FieldType::String,
                            string_field_value!(event.token1)
                        ),
                        new_field!("owner", FieldType::String, string_field_value!(burn.owner)),
                        new_field!(
                            "origin",
                            FieldType::String,
                            string_field_value!(burn.origin)
                        ),
                        new_field!(
                            "amount",
                            FieldType::String,
                            string_field_value!(burn.amount_0)
                        ),
                        new_field!(
                            "amount0",
                            FieldType::String,
                            string_field_value!(burn.amount_0)
                        ),
                        new_field!(
                            "amount1",
                            FieldType::String,
                            string_field_value!(burn.amount_1)
                        ),
                        new_field!(
                            "amountUSD",
                            FieldType::String,
                            string_field_value!(amount_usd.to_string())
                        ),
                        new_field!(
                            "tickLower",
                            FieldType::String,
                            string_field_value!(burn.tick_lower.to_string())
                        ),
                        new_field!(
                            "tickUpper",
                            FieldType::String,
                            string_field_value!(burn.tick_upper.to_string())
                        ),
                        new_field!(
                            "logIndex",
                            FieldType::String,
                            string_field_value!(event.log_ordinal.to_string())
                        ),
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
        id: string_field_value!(flash.pool_address),
        ordinal: flash.log_ordinal,
        operation: Operation::Update as i32,
        fields: vec![
            new_field!(
                "feeGrowthGlobal0X128",
                FieldType::Bigint,
                big_int_field_value!(flash.fee_growth_global_0x_128)
            ),
            new_field!(
                "feeGrowthGlobal1X128",
                FieldType::Bigint,
                big_int_field_value!(flash.fee_growth_global_1x_128)
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
        id: string_field_value!(day_id.to_string()),
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![
            new_field!(
                "id",
                FieldType::String,
                string_field_value!(day_id.to_string())
            ),
            new_field!(
                "date",
                FieldType::Int,
                int_field_value!(day_start_timestamp)
            ),
            new_field!(
                "volumeUSDUntracked",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string())
            ),
            update_field!("txCount", FieldType::Int, delta.old_value, delta.new_value),
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
        id: string_field_value!(day_id.to_string()),
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![update_field!(
            "tvlUSD",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(delta.old_value),
            big_decimal_vec_field_value!(delta.new_value)
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
        id: string_field_value!(day_id.to_string()),
        ordinal: delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };

    match delta.key.as_str().split(":").last().unwrap() {
        "volumeETH" => change.fields.push(update_field!(
            "volumeETH",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(delta.old_value),
            big_decimal_vec_field_value!(delta.new_value)
        )),
        "volumeUSD" => change.fields.push(update_field!(
            "volumeUSD",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(delta.old_value),
            big_decimal_vec_field_value!(delta.new_value)
        )),
        "feesUSD" => change.fields.push(update_field!(
            "feesUSD",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(delta.old_value),
            big_decimal_vec_field_value!(delta.new_value)
        )),
        _ => return None,
    }

    Some(change)
}
