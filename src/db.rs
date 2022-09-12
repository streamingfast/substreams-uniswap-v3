use crate::pb::uniswap::field::Type as FieldType;
use crate::pb::uniswap::Field;
use crate::{
    big_decimal_string_field_value, big_decimal_vec_field_value, big_int_field_value, new_field,
    string_field_value, update_field, utils, EntityChange, Erc20Token, Pool, PoolSqrtPrice,
};
use num_bigint::BigInt;
use std::str::FromStr;
use substreams::pb::substreams::store_delta::Operation;
use substreams::pb::substreams::StoreDelta;
use substreams::{proto, Hex};

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
                "sqrt_price",
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

pub fn pool_sqrt_price_entity_change(pool_sqrt_price_delta: StoreDelta) -> Option<EntityChange> {
    let new_value: PoolSqrtPrice = proto::decode(&pool_sqrt_price_delta.new_value).unwrap();

    let mut change = EntityChange {
        entity: "Pool".to_string(),
        id: string_field_value!(pool_sqrt_price_delta
            .key
            .as_str()
            .split(":")
            .nth(1)
            .unwrap()),
        ordinal: pool_sqrt_price_delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };

    match pool_sqrt_price_delta.operation {
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
                big_decimal_string_field_value!("0".to_string()),
                big_int_field_value!(new_value.tick)
            ));
        }
        2 => {
            let old_value: PoolSqrtPrice = proto::decode(&pool_sqrt_price_delta.new_value).unwrap();
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

pub fn pool_liquidities_pool_entity_change(
    pool_liquidities_store_delta: StoreDelta,
) -> EntityChange {
    let mut change = EntityChange {
        entity: "Pool".to_string(),
        id: string_field_value!(pool_liquidities_store_delta
            .key
            .as_str()
            .split(":")
            .nth(1)
            .unwrap()),
        ordinal: pool_liquidities_store_delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };
    match pool_liquidities_store_delta.operation {
        1 => {
            change.fields.push(update_field!(
                "liquidity",
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string()),
                pool_liquidities_store_delta.new_value
            ));
        }
        2 => {
            change.fields.push(update_field!(
                "liquidity",
                FieldType::Bigdecimal,
                pool_liquidities_store_delta.old_value,
                pool_liquidities_store_delta.new_value
            ));
        }
        _ => {}
    }

    change
}

pub fn price_pool_entity_change(price_delta: StoreDelta) -> Option<EntityChange> {
    let mut key_parts = price_delta.key.as_str().split(":");
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
        ordinal: price_delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };

    match price_delta.operation {
        1 => {
            change.fields.push(update_field!(
                field_name,
                FieldType::Bigdecimal,
                big_decimal_string_field_value!("0".to_string()),
                big_decimal_vec_field_value!(price_delta.new_value)
            ));
        }
        2 => {
            change.fields.push(update_field!(
                field_name,
                FieldType::Bigdecimal,
                big_decimal_vec_field_value!(price_delta.old_value),
                big_decimal_vec_field_value!(price_delta.new_value)
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

pub fn total_value_locked_pool_entity_change(
    total_value_locked_delta: StoreDelta,
) -> Option<EntityChange> {
    if !total_value_locked_delta.key.starts_with("pool:") {
        return None;
    }

    let mut change: EntityChange = EntityChange {
        entity: "Pool".to_string(),
        id: string_field_value!(total_value_locked_delta
            .key
            .as_str()
            .split(":")
            .nth(1)
            .unwrap()),
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
        "usd" => change.fields.push(update_field!(
            "totalValueLockedUSD",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(total_value_locked_delta.old_value),
            big_decimal_vec_field_value!(total_value_locked_delta.new_value)
        )),
        "eth" => change.fields.push(update_field!(
            "totalValueLockedETH",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(total_value_locked_delta.old_value),
            big_decimal_vec_field_value!(total_value_locked_delta.new_value)
        )),
        _ => {}
    }

    Some(change)
}

pub fn total_value_locked_by_token_pool_entity_change(
    total_value_locked_by_tokens_delta: StoreDelta,
) -> Option<EntityChange> {
    let mut change: EntityChange = EntityChange {
        entity: "Pool".to_string(),
        id: string_field_value!(total_value_locked_by_tokens_delta
            .key
            .as_str()
            .split(":")
            .nth(1)
            .unwrap()),
        ordinal: total_value_locked_by_tokens_delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };

    match total_value_locked_by_tokens_delta
        .key
        .as_str()
        .split(":")
        .last()
        .unwrap()
    {
        "token0" => change.fields.push(update_field!(
            "totalValueLockedToken0",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(total_value_locked_by_tokens_delta.old_value),
            big_decimal_vec_field_value!(total_value_locked_by_tokens_delta.new_value)
        )),
        "token1" => change.fields.push(update_field!(
            "totalValueLockedToken1",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(total_value_locked_by_tokens_delta.old_value),
            big_decimal_vec_field_value!(total_value_locked_by_tokens_delta.new_value)
        )),
        _ => return None,
    }

    Some(change)
}

pub fn swap_volume_pool_entity_change(swap_volume_delta: StoreDelta) -> Option<EntityChange> {
    let mut change: EntityChange = EntityChange {
        entity: "Pool".to_string(),
        id: string_field_value!(swap_volume_delta.key.as_str().split(":").nth(1).unwrap()),
        ordinal: swap_volume_delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };
    match swap_volume_delta.key.as_str().split(":").last().unwrap() {
        "token0" => change.fields.push(update_field!(
            "volumeToken0",
            FieldType::Bigdecimal,
            big_decimal_vec_field_value!(swap_volume_delta.old_value),
            big_decimal_vec_field_value!(swap_volume_delta.new_value)
        )),
        "token1" => change.fields.push(update_field!(
            "volumeToken1",
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

pub fn pool_fee_growth_global_x128_entity_change(
    pool_fee_growth_global_x128_delta: StoreDelta,
) -> Option<EntityChange> {
    let mut change: EntityChange = EntityChange {
        entity: "Pool".to_string(),
        id: string_field_value!(pool_fee_growth_global_x128_delta
            .key
            .as_str()
            .split(":")
            .nth(1)
            .unwrap()),
        ordinal: pool_fee_growth_global_x128_delta.ordinal,
        operation: Operation::Update as i32,
        fields: vec![],
    };

    match pool_fee_growth_global_x128_delta
        .key
        .as_str()
        .split(":")
        .nth(1)
        .unwrap()
    {
        "token0" => change.fields.push(update_field!(
            "feeGrowthGlobal0X128",
            FieldType::Bigint,
            big_int_field_value!(BigInt::from_signed_bytes_be(
                pool_fee_growth_global_x128_delta.old_value.as_ref()
            )
            .to_string()),
            big_int_field_value!(BigInt::from_signed_bytes_be(
                pool_fee_growth_global_x128_delta.new_value.as_ref()
            )
            .to_string())
        )),
        "token1" => change.fields.push(update_field!(
            "feeGrowthGlobal1X128",
            FieldType::Bigint,
            big_int_field_value!(BigInt::from_signed_bytes_be(
                pool_fee_growth_global_x128_delta.old_value.as_ref()
            )
            .to_string()),
            big_int_field_value!(BigInt::from_signed_bytes_be(
                pool_fee_growth_global_x128_delta.new_value.as_ref()
            )
            .to_string())
        )),
        _ => return None,
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
