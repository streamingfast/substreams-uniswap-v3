use std::collections::HashMap;
use std::ops::{Div, Mul};
use std::str::FromStr;

use substreams::prelude::StoreGetInt64;
use substreams::scalar::{BigDecimal, BigInt};
use substreams::store::{
    DeltaArray, DeltaBigDecimal, DeltaBigInt, DeltaProto, Deltas, StoreGet, StoreGetBigDecimal,
    StoreGetBigInt, StoreGetProto,
};
use substreams::{log, Hex};
use substreams_entity_change::change::ToField;
use substreams_entity_change::pb::entity::{
    entity_change::Operation, EntityChange, EntityChanges, Field, Value,
};

use crate::pb::position_event::PositionEventType;
use crate::pb::uniswap::position::PositionType;
use crate::pb::uniswap::{Pool, Position};
use crate::uniswap::tick::Origin;
use crate::{
    keyer, utils, BurnEvent, Erc20Token, Flashes, MintEvent, PoolSqrtPrice, Pools, Positions,
    SnapshotPositions, SwapEvent, Tick, TokenEvents, Transactions,
};

pub struct Tables {
    // Map from table name to the primary keys within that table
    pub tables: HashMap<String, Rows>,
}

impl Tables {
    pub fn new() -> Self {
        Tables {
            tables: HashMap::new(),
        }
    }

    pub fn update_row(&mut self, table: &str, key: &str) -> &mut Row {
        let rows = self.tables.entry(table.to_string()).or_insert(Rows::new());
        let row = rows.pks.entry(key.to_string()).or_insert(Row::new());
        row.operation = Operation::Update;
        row
    }

    pub fn delete_row(&mut self, table: &str, key: &str) {
        let rows = self.tables.entry(table.to_string()).or_insert(Rows::new());
        let row = rows.pks.entry(key.to_string()).or_insert(Row::new());
        row.operation = Operation::Delete;
        row.columns = HashMap::new();
    }

    // Convert Tables into an EntityChanges protobuf object
    pub fn to_entity_changes(mut self) -> EntityChanges {
        let mut entities = EntityChanges::default();
        for (table, rows) in self.tables.iter_mut() {
            for (pk, row) in rows.pks.iter_mut() {
                // Map the row.operation into an EntityChange.Operation
                let mut change = EntityChange::new(table, pk, 0, row.operation);
                for (field, value) in row.columns.iter_mut() {
                    change.fields.push(Field {
                        name: field.clone(),
                        new_value: Some(value.clone()),
                        old_value: None,
                    });
                }
                entities.entity_changes.push(change.clone());
            }
        }
        entities
    }
}

pub struct Rows {
    // Map of primary keys within this table, to the fields within
    pub pks: HashMap<String, Row>,
}

impl Rows {
    pub fn new() -> Self {
        Rows {
            pks: HashMap::new(),
        }
    }
}

pub struct Row {
    // Verify that we don't try to delete the same row as we're creating it
    pub operation: Operation,
    // Map of field name to its last change
    pub columns: HashMap<String, Value>,
}

impl Row {
    pub fn new() -> Self {
        Row {
            operation: Operation::Unset,
            columns: HashMap::new(),
        }
    }

    pub fn set<N: AsRef<str>, T: ToField>(&mut self, name: N, change: T) -> &mut Self {
        let field = change.to_field(name);
        self.columns.insert(field.name, field.new_value.unwrap());
        self
    }
}

// -------------------
//  Map Bundle Entities
// -------------------
pub fn created_bundle_entity_change(tables: &mut Tables) {
    let bd = BigDecimal::from(10 as i32);
    tables.update_row("Bundle", "1").set("ethPriceUSD", bd);
}

pub fn bundle_store_eth_price_usd_bundle_entity_change(
    tables: &mut Tables,
    deltas: Deltas<DeltaBigDecimal>,
) {
    for delta in deltas.deltas {
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

pub fn pool_created_factory_entity_change(tables: &mut Tables, deltas: Deltas<DeltaBigInt>) {
    for delta in deltas.deltas {
        tables
            .update_row(
                "Factory",
                Hex(utils::UNISWAP_V3_FACTORY).to_string().as_str(),
            )
            .set("poolCount", delta);
    }
}

pub fn tx_count_factory_entity_change(tables: &mut Tables, deltas: Deltas<DeltaBigInt>) {
    for delta in deltas.deltas {
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

pub fn swap_volume_factory_entity_change(tables: &mut Tables, deltas: Deltas<DeltaBigDecimal>) {
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
pub fn pools_created_pool_entity_change(pools: Pools, tables: &mut Tables) {
    for pool in pools.pools {
        tables
            .update_row("Pool", &format!("0x{}", pool.address.clone().as_str()))
            .set("id", pool.address)
            .set(
                "createdAtTimestamp",
                BigInt::from(pool.created_at_timestamp),
            )
            .set(
                "createdAtBlockNumber",
                BigInt::from(pool.created_at_block_number),
            )
            .set("token0", pool.token0.unwrap().address)
            .set("token1", pool.token1.unwrap().address)
            .set("feeTier", BigInt::from(pool.fee_tier.unwrap()))
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
    deltas: Deltas<DeltaProto<PoolSqrtPrice>>,
) {
    for delta in deltas.deltas {
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

pub fn pool_liquidities_pool_entity_change(tables: &mut Tables, deltas: Deltas<DeltaBigInt>) {
    for delta in deltas.deltas {
        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        tables
            .update_row("Pool", &format!("0x{}", pool_address.as_str()))
            .set("liquidity", delta);
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

pub fn total_value_locked_by_token_pool_entity_change(
    tables: &mut Tables,
    deltas: Deltas<DeltaBigDecimal>,
) {
    for delta in deltas.deltas {
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

pub fn pool_fee_growth_global_x128_entity_change(tables: &mut Tables, deltas: Deltas<DeltaBigInt>) {
    for delta in deltas.deltas {
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

pub fn price_pool_entity_change(tables: &mut Tables, deltas: Deltas<DeltaBigDecimal>) {
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

        tables
            .update_row("Pool", &format!("0x{}", pool_address.as_str()))
            .set(name, delta);
    }
}

pub fn tx_count_pool_entity_change(tables: &mut Tables, deltas: Deltas<DeltaBigInt>) {
    for delta in deltas.deltas {
        if !delta.key.starts_with("pool:") {
            continue;
        }

        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string(); // TODO: put in keyer
        tables
            .update_row("Pool", &format!("0x{}", pool_address.as_str()))
            .set("txCount", delta);
    }
}

pub fn swap_volume_pool_entity_change(tables: &mut Tables, deltas: Deltas<DeltaBigDecimal>) {
    for delta in deltas.deltas {
        if !delta.key.as_str().starts_with("swap") {
            continue;
        }

        let pool_address = delta.key.as_str().split(":").nth(1).unwrap().to_string();
        if delta.key.as_str().split(":").last().unwrap().eq("usd") {
            log::info!("delta value {:?}", delta);
        }

        let name = match delta.key.as_str().split(":").last().unwrap() {
            "token0" => "volumeToken0",
            "token1" => "volumeToken1",
            "usd" => "volumeUSD",
            "untrackedUSD" => "untrackedVolumeUSD",
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
    pools: Pools,
    tokens_store: StoreGetInt64,
) {
    for pool in pools.pools {
        match tokens_store.get_last(&keyer::token_key(pool.token0_ref().address())) {
            Some(value) => {
                if value.eq(&1) {
                    add_token_entity_change(tables, pool.token0_ref(), pool.log_ordinal);
                }
            }
            None => {
                panic!("pool contains token that doesn't exist {}", pool.address)
            }
        }

        match tokens_store.get_last(&keyer::token_key(pool.token1_ref().address())) {
            Some(value) => {
                if value.eq(&1) {
                    add_token_entity_change(tables, pool.token1_ref(), pool.log_ordinal);
                }
            }
            None => {
                panic!("pool contains token that doesn't exist {}", pool.address)
            }
        }
    }
}

pub fn swap_volume_token_entity_change(tables: &mut Tables, deltas: Deltas<DeltaBigDecimal>) {
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

        tables
            .update_row("Token", token_address.as_str())
            .set(name, delta);
    }
}

pub fn tx_count_token_entity_change(tables: &mut Tables, deltas: Deltas<DeltaBigInt>) {
    for delta in deltas.deltas {
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
    deltas: Deltas<DeltaBigDecimal>,
) {
    for delta in deltas.deltas {
        if !delta.key.starts_with("token:") {
            continue;
        }

        let token_address = delta.key.as_str().split(":").last().unwrap().to_string();
        log::info!("printing delta {:?}", delta);

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
    deltas: Deltas<DeltaBigDecimal>,
) {
    for delta in deltas.deltas {
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

fn add_token_entity_change(tables: &mut Tables, token: &Erc20Token, log_ordinal: u64) {
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
pub fn create_or_update_ticks_entity_change(tables: &mut Tables, deltas: Deltas<DeltaProto<Tick>>) {
    for delta in deltas.deltas {
        let new_tick: Tick = delta.new_value;
        let old_tick: Tick = delta.old_value;

        if old_tick.id.eq("") {
            // does this makes sense?
            if new_tick.origin == Origin::Mint as i32 {
                create_tick_entity_change(tables, new_tick)
            }
        } else {
            update_tick_entity_change(tables, old_tick, new_tick);
        }
    }
}

pub fn ticks_liquidities_tick_entity_change(tables: &mut Tables, deltas: Deltas<DeltaBigInt>) {
    for delta in deltas.deltas {
        let tick_id = delta.key.as_str().split(":").nth(1).unwrap().to_string();

        let name = match delta.key.as_str().split(":").last().unwrap() {
            "liquidityNet" => "liquidityNet",
            "liquidityGross" => "liquidityGross",
            _ => continue,
        };

        tables.update_row("Tick", tick_id.as_str()).set(name, delta);
    }
}

fn create_tick_entity_change(tables: &mut Tables, tick: Tick) {
    tables
        .update_row("Tick", tick.id.clone().as_str())
        .set("id", tick.id)
        .set("poolAddress", tick.pool_address.clone())
        .set("tickIdx", BigInt::from(tick.idx.unwrap()))
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
        .set(
            "feeGrowthOutside0X128",
            BigInt::from(tick.fee_growth_outside_0x_128.unwrap()),
        )
        .set(
            "feeGrowthOutside1X128",
            BigInt::from(tick.fee_growth_outside_1x_128.unwrap()),
        );
}

fn update_tick_entity_change(tables: &mut Tables, old_tick: Tick, new_tick: Tick) {
    tables
        .update_row("Tick", new_tick.id.as_str())
        .set(
            "feeGrowthOutside0X128",
            DeltaBigInt {
                operation: substreams::pb::substreams::store_delta::Operation::Update,
                ordinal: new_tick.log_ordinal,
                key: "".to_string(),
                old_value: BigInt::from(old_tick.fee_growth_outside_0x_128.unwrap()),
                new_value: BigInt::from(new_tick.fee_growth_outside_0x_128.unwrap()),
            },
        )
        .set(
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
pub fn position_create_entity_change(
    positions: Positions,
    positions_store: StoreGetInt64,
    tables: &mut Tables,
) {
    for position in positions.positions {
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
    position: Position,
) {
    match positions_store.get_last(&keyer::position(&position.id, &position_type.to_string())) {
        None => {}
        Some(value) => {
            if value.eq(&1) {
                add_position_entity_change(tables, position);
            }
        }
    }
}

fn add_position_entity_change(tables: &mut Tables, position: Position) {
    tables
        .update_row("Position", position.id.clone().as_str())
        .set("id", position.id)
        .set("owner", position.owner.into_bytes())
        .set("pool", position.pool)
        .set("token0", position.token0)
        .set("token1", position.token1)
        .set("tickLower", position.tick_lower)
        .set("tickUpper", position.tick_upper)
        .set("liquidity", BigDecimal::zero())
        .set("depositedToken0", BigDecimal::zero())
        .set("depositedToken1", BigDecimal::zero())
        .set("withdrawnToken0", BigDecimal::zero())
        .set("withdrawnToken1", BigDecimal::zero())
        .set("collectedFeesToken0", BigDecimal::zero())
        .set("collectedFeesToken1", BigDecimal::zero())
        .set("transaction", position.transaction)
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
pub fn snapshot_position_entity_change(snapshot_positions: SnapshotPositions, tables: &mut Tables) {
    for snapshot_position in snapshot_positions.snapshot_positions {
        tables
            .update_row("PositionSnapshot", snapshot_position.id.clone().as_str())
            .set("id", snapshot_position.id)
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
            .set("transaction", snapshot_position.transaction)
            .set(
                "feeGrowthInside0LastX128",
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
pub fn transaction_entity_change(transactions: Transactions, tables: &mut Tables) {
    for transaction in transactions.transactions {
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
    events: TokenEvents,
    tx_count_store: StoreGetBigInt,
    store_eth_prices: StoreGetBigDecimal,
    tables: &mut Tables,
) {
    for event in events.events {
        if event.r#type.is_none() {
            continue;
        }

        if event.r#type.is_some() {
            let transaction_count: i32 =
                match tx_count_store.get_last(keyer::pool_total_tx_count(&event.pool_address)) {
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
pub fn flashes_update_pool_fee_entity_change(flashes: Flashes, tables: &mut Tables) {
    for flash in flashes.flashes {
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
pub fn uniswap_day_data_tx_count_entity_change(tables: &mut Tables, deltas: Deltas<DeltaBigInt>) {
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

        tables
            .update_row("UniswapDayData", day_id.to_string().as_str())
            .set("id", day_id.to_string())
            .set("date", day_start_timestamp)
            .set("volumeUSDUntracked", BigDecimal::zero())
            .set("txCount", delta);
    }
}

pub fn uniswap_day_data_totals_entity_change(tables: &mut Tables, deltas: Deltas<DeltaBigDecimal>) {
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

        tables
            .update_row("UniswapDayData", day_id.to_string().as_str())
            .set("tvlUSD", delta);
    }
}

pub fn uniswap_day_data_volumes_entity_change(
    tables: &mut Tables,
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

        tables.update_row("UniswapDayData", day_id).set(name, delta);
    }
}
