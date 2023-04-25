extern crate core;

pub mod abi;
mod ast;
mod db;
mod eth;
mod filtering;
mod keyer;
mod math;
mod pb;
mod price;
mod rpc;
mod storage;
mod tables;
mod utils;

use crate::ethpb::v2::{Block, StorageChange};
use crate::pb::uniswap;
use crate::pb::uniswap::events::pool_event::Type;
use crate::pb::uniswap::events::pool_event::Type::{Burn as BurnEvent, Mint as MintEvent, Swap as SwapEvent};
use crate::pb::uniswap::events::PoolSqrtPrice;
use crate::pb::uniswap::{events, Events, SnapshotPosition, SnapshotPositions};
use crate::pb::uniswap::{Erc20Token, Erc20Tokens, Pool, Pools};
use crate::price::WHITELIST_TOKENS;
use crate::tables::Tables;
use crate::uniswap::events::position::PositionType::{Collect, DecreaseLiquidity, IncreaseLiquidity, Transfer};
use crate::utils::UNISWAP_V3_FACTORY;
use std::ops::{Div, Mul, Sub};
use substreams::errors::Error;
use substreams::hex;
use substreams::pb::substreams::Clock;
use substreams::prelude::*;
use substreams::scalar::{BigDecimal, BigInt};
use substreams::store::{
    DeltaArray, DeltaBigDecimal, DeltaBigInt, DeltaProto, StoreAddBigDecimal, StoreAddBigInt, StoreAppend,
    StoreGetBigDecimal, StoreGetBigInt, StoreGetProto, StoreGetRaw, StoreSetBigDecimal, StoreSetBigInt, StoreSetProto,
};
use substreams::{log, Hex};
use substreams_entity_change::pb::entity::EntityChanges;
use substreams_ethereum::{pb::eth as ethpb, Event as EventTrait};

#[substreams::handlers::map]
pub fn map_pools_created(block: Block) -> Result<Pools, Error> {
    use abi::factory::events::PoolCreated;

    Ok(Pools {
        pools: block
            .events::<PoolCreated>(&[&UNISWAP_V3_FACTORY])
            .filter_map(|(event, log)| {
                log::info!("pool addr: {}", Hex(&event.pool));

                let token0_address = Hex(&event.token0).to_string();
                let token1_address = Hex(&event.token1).to_string();

                //todo: question regarding the ignore_pool line. In the
                // uniswap-v3 subgraph, they seem to bail out when they
                // match the addr, should we do the same ?
                Some(Pool {
                    address: Hex(&log.data()[44..64]).to_string(),
                    transaction_id: Hex(&log.receipt.transaction.hash).to_string(),
                    created_at_block_number: block.number,
                    created_at_timestamp: block.timestamp_seconds(),
                    fee_tier: event.fee.to_string(),
                    tick_spacing: event.tick_spacing.into(),
                    log_ordinal: log.ordinal(),
                    ignore_pool: event.pool == hex!("8fe8d9bb8eeba3ed688069c3d6b556c9ca258248"),
                    token0: Some(match rpc::create_uniswap_token(&token0_address) {
                        Some(mut token) => {
                            token.total_supply = rpc::token_total_supply_call(&token0_address)
                                .unwrap_or(BigInt::zero())
                                .to_string();
                            token
                        }
                        None => {
                            // We were unable to create the uniswap token, so we discard this event entirely
                            return None;
                        }
                    }),
                    token1: Some(match rpc::create_uniswap_token(&token1_address) {
                        Some(mut token) => {
                            token.total_supply = rpc::token_total_supply_call(&token1_address)
                                .unwrap_or(BigInt::zero())
                                .to_string();
                            token
                        }
                        None => {
                            // We were unable to create the uniswap token, so we discard this event entirely
                            return None;
                        }
                    }),
                    ..Default::default()
                })
            })
            .collect(),
    })
}

#[substreams::handlers::store]
pub fn store_pools(pools: Pools, store: StoreSetProto<Pool>) {
    for pool in pools.pools {
        store.set_many(
            pool.log_ordinal,
            &vec![
                keyer::pool_key(&pool.address),
                keyer::pool_token_index_key(
                    &pool.token0_ref().address(),
                    &pool.token1_ref().address(),
                    &pool.fee_tier,
                ),
            ],
            &pool,
        );
    }
}

#[substreams::handlers::store]
pub fn store_tokens(clock: Clock, pools: Pools, store: StoreAddInt64) {
    let timestamp_seconds = clock.timestamp.unwrap().seconds;
    let day_id: i64 = timestamp_seconds / 86400;
    let hour_id: i64 = timestamp_seconds / 3600;

    store.delete_prefix(0, &format!("{}:{}:", keyer::TOKEN_DAY_DATA, day_id - 1));
    store.delete_prefix(0, &format!("{}:{}:", keyer::TOKEN_HOUR_DATA, hour_id - 1));

    for pool in pools.pools {
        store.add_many(
            pool.log_ordinal,
            &vec![
                keyer::token_key(&pool.token0_ref().address()),
                keyer::token_key(&pool.token1_ref().address()),
                keyer::token_day_data_token_key(&pool.token0_ref().address(), day_id.to_string()),
                keyer::token_day_data_token_key(&pool.token1_ref().address(), day_id.to_string()),
                keyer::token_hour_data_token_key(&pool.token0_ref().address(), hour_id.to_string()),
                keyer::token_hour_data_token_key(&pool.token1_ref().address(), hour_id.to_string()),
            ],
            1,
        );
    }
}

#[substreams::handlers::store]
pub fn store_pool_count(pools: Pools, store: StoreAddBigInt) {
    for pool in pools.pools {
        store.add(pool.log_ordinal, keyer::factory_pool_count_key(), &BigInt::one())
    }
}

#[substreams::handlers::map]
pub fn map_tokens_whitelist_pools(pools: Pools) -> Result<Erc20Tokens, Error> {
    let mut tokens = vec![];

    for pool in pools.pools {
        let mut token0 = pool.token0();
        let mut token1 = pool.token1();

        let token0_whitelisted = WHITELIST_TOKENS.contains(&token0.address.as_str());
        let token1_whitelisted = WHITELIST_TOKENS.contains(&token1.address.as_str());

        if token0_whitelisted {
            log::info!("adding pool: {} to token: {}", pool.address, token1.address);
            token1.whitelist_pools.push(pool.address.to_string());
            tokens.push(token1);
        }

        if token1_whitelisted {
            log::info!("adding pool: {} to token: {}", pool.address, token0.address);
            token0.whitelist_pools.push(pool.address.to_string());
            tokens.push(token0);
        }
    }

    Ok(Erc20Tokens { tokens })
}

#[substreams::handlers::store]
pub fn store_tokens_whitelist_pools(tokens: Erc20Tokens, output_append: StoreAppend<String>) {
    for token in tokens.tokens {
        output_append.append_all(1, keyer::token_pool_whitelist(&token.address), token.whitelist_pools);
    }
}

#[substreams::handlers::map]
pub fn map_extract_data_types(block: Block, pools_store: StoreGetProto<Pool>) -> Result<Events, Error> {
    let mut events = Events::default();

    let mut pool_sqrt_prices: Vec<events::PoolSqrtPrice> = vec![];
    let mut pool_liquidities: Vec<events::PoolLiquidity> = vec![];
    let mut fee_growth_global_updates: Vec<events::FeeGrowthGlobal> = vec![];
    let mut pool_events: Vec<events::PoolEvent> = vec![];
    let mut transactions: Vec<events::Transaction> = vec![];
    let mut flashes: Vec<events::Flash> = vec![];
    let mut ticks_created: Vec<events::TickCreated> = vec![];
    let mut ticks_updated: Vec<events::TickUpdated> = vec![];

    let mut positions_created: Vec<events::CreatedPosition> = vec![];
    let mut positions_increase_liquidity: Vec<events::IncreaseLiquidityPosition> = vec![];
    let mut positions_decrease_liquidity: Vec<events::DecreaseLiquidityPosition> = vec![];
    let mut positions_collect: Vec<events::CollectPosition> = vec![];
    let mut positions_transfer: Vec<events::TransferPosition> = vec![];

    let timestamp = block.timestamp_seconds();

    for trx in block.transactions() {
        for (log, call_view) in trx.logs_with_calls() {
            let pool_address = &Hex(log.clone().address).to_string();
            let pool_key = &keyer::pool_key(&pool_address);
            let transactions_id = Hex(&trx.hash).to_string();

            match pools_store.get_last(pool_key) {
                Some(pool) => {
                    filtering::extract_pool_sqrt_prices(&mut pool_sqrt_prices, log, pool_address);
                    filtering::extract_pool_liquidities(
                        &mut pool_liquidities,
                        log,
                        &call_view.call.storage_changes,
                        &pool,
                    );
                    filtering::extract_fee_growth_update(
                        &mut fee_growth_global_updates,
                        log,
                        &call_view.call.storage_changes,
                        &pool,
                    );

                    filtering::extract_pool_events_and_positions(
                        &mut pool_events,
                        &mut ticks_created,
                        &mut ticks_updated,
                        &mut positions_created,
                        &mut positions_increase_liquidity,
                        &mut positions_decrease_liquidity,
                        &mut positions_collect,
                        &mut positions_transfer,
                        &transactions_id,
                        &Hex(&trx.from).to_string(),
                        log,
                        &call_view,
                        &pool,
                        timestamp,
                        block.number,
                    );

                    filtering::extract_transactions(&mut transactions, log, &trx, timestamp, block.number);
                    filtering::extract_flashes(&mut flashes, &log, &pools_store, pool_key);
                }
                _ => (),
            }
        }
    }

    events.pool_sqrt_prices = pool_sqrt_prices;
    events.pool_liquidities = pool_liquidities;
    events.fee_growth_global_updates = fee_growth_global_updates;
    events.pool_events = pool_events;
    events.transactions = transactions;
    events.created_positions = positions_created;
    events.increase_liquidity_positions = positions_increase_liquidity;
    events.decrease_liquidity_positions = positions_decrease_liquidity;
    events.collect_positions = positions_collect;
    events.transfer_positions = positions_transfer;
    events.flashes = flashes;
    events.ticks_created = ticks_created;
    events.ticks_updated = ticks_updated;

    Ok(events)
}

#[substreams::handlers::store]
pub fn store_pool_sqrt_price(clock: Clock, events: Events, store: StoreSetProto<PoolSqrtPrice>) {
    let timestamp_seconds = clock.timestamp.unwrap().seconds;
    let day_id: i64 = timestamp_seconds / 86400;
    let hour_id: i64 = timestamp_seconds / 3600;

    store.delete_prefix(0, &format!("{}:{}:", keyer::POOL_DAY_DATA, day_id - 1));
    store.delete_prefix(0, &format!("{}:{}:", keyer::POOL_HOUR_DATA, hour_id - 1));

    for sqrt_price in events.pool_sqrt_prices {
        store.set_many(
            sqrt_price.ordinal,
            &vec![
                keyer::pool_sqrt_price_key(&sqrt_price.pool_address),
                keyer::pool_day_data_sqrt_price(&sqrt_price.pool_address, day_id.to_string()),
                keyer::pool_hour_data_sqrt_price(&sqrt_price.pool_address, hour_id.to_string()),
            ],
            &sqrt_price,
        )
    }
}

#[substreams::handlers::store]
pub fn store_prices(clock: Clock, events: Events, pools_store: StoreGetProto<Pool>, store: StoreSetBigDecimal) {
    let timestamp_seconds = clock.timestamp.unwrap().seconds;
    let day_id: i64 = timestamp_seconds / 86400;
    let hour_id: i64 = timestamp_seconds / 3600;

    store.delete_prefix(0, &format!("{}:{}:", keyer::POOL_DAY_DATA, day_id - 1));
    store.delete_prefix(0, &format!("{}:{}:", keyer::POOL_HOUR_DATA, hour_id - 1));
    store.delete_prefix(0, &format!("{}:{}:", keyer::TOKEN_DAY_DATA, day_id - 1));
    store.delete_prefix(0, &format!("{}:{}:", keyer::TOKEN_HOUR_DATA, hour_id - 1));

    for sqrt_price_update in events.pool_sqrt_prices {
        match pools_store.get_last(keyer::pool_key(&sqrt_price_update.pool_address)) {
            None => {
                log::info!("skipping pool {}", &sqrt_price_update.pool_address,);
                continue;
            }
            Some(pool) => {
                let token0 = pool.token0.as_ref().unwrap();
                let token1 = pool.token1.as_ref().unwrap();
                log::debug!(
                    "pool addr: {}, pool trx_id: {}, token 0 addr: {}, token 1 addr: {}",
                    pool.address,
                    pool.transaction_id,
                    token0.address,
                    token1.address
                );

                let sqrt_price = BigDecimal::try_from(sqrt_price_update.sqrt_price).unwrap();
                log::debug!("sqrtPrice: {}", sqrt_price.to_string());

                let tokens_price: (BigDecimal, BigDecimal) =
                    price::sqrt_price_x96_to_token_prices(sqrt_price, &token0, &token1);
                log::debug!("token prices: {} {}", tokens_price.0, tokens_price.1);

                store.set_many(
                    sqrt_price_update.ordinal,
                    &vec![
                        keyer::prices_pool_token_key(&pool.address, &token0.address, "token0".to_string()),
                        keyer::prices_token_pair(
                            &pool.token0.as_ref().unwrap().address,
                            &pool.token1.as_ref().unwrap().address,
                        ),
                        keyer::pool_day_data_token_price(&pool.address, "token0".to_string(), day_id.to_string()),
                        keyer::pool_hour_data_token_price(&pool.address, "token0".to_string(), hour_id.to_string()),
                        // TODO: validate this data
                        keyer::token_day_data_token_price(&pool.token0.as_ref().unwrap().address, day_id.to_string()),
                        keyer::token_hour_data_token_price(&pool.token0.as_ref().unwrap().address, hour_id.to_string()),
                    ],
                    &tokens_price.0,
                );

                store.set_many(
                    sqrt_price_update.ordinal,
                    &vec![
                        keyer::prices_pool_token_key(&pool.address, &token1.address, "token1".to_string()),
                        keyer::prices_token_pair(
                            &pool.token1.as_ref().unwrap().address,
                            &pool.token0.as_ref().unwrap().address,
                        ),
                        keyer::pool_day_data_token_price(&pool.address, "token1".to_string(), day_id.to_string()),
                        keyer::pool_hour_data_token_price(&pool.address, "token1".to_string(), hour_id.to_string()),
                        // TODO: validate this data
                        keyer::token_day_data_token_price(&pool.token1.as_ref().unwrap().address, day_id.to_string()),
                        keyer::token_hour_data_token_price(&pool.token1.as_ref().unwrap().address, hour_id.to_string()),
                    ],
                    &tokens_price.1,
                );
            }
        }
    }
}

#[substreams::handlers::store]
pub fn store_pool_liquidities(clock: Clock, events: Events, store: StoreSetBigInt) {
    let timestamp_seconds = clock.timestamp.unwrap().seconds;
    let day_id: i64 = timestamp_seconds / 86400;
    let hour_id: i64 = timestamp_seconds / 3600;

    store.delete_prefix(0, &format!("{}:{}:", keyer::POOL_DAY_DATA, day_id - 1));
    store.delete_prefix(0, &format!("{}:{}:", keyer::POOL_HOUR_DATA, hour_id - 1));

    for pool_liquidity in events.pool_liquidities {
        store.set_many(
            0,
            &vec![
                keyer::pool_liquidity(&pool_liquidity.pool_address),
                keyer::pool_day_data_liquidity(&pool_liquidity.pool_address, day_id.to_string()),
                keyer::pool_hour_data_liquidity(&pool_liquidity.pool_address, hour_id.to_string()),
            ],
            &BigInt::try_from(pool_liquidity.liquidity).unwrap(),
        )
    }
}

// Stores all the total value locked
#[substreams::handlers::store]
pub fn store_totals(
    clock: Clock,
    store_eth_prices: StoreGetBigDecimal,
    derived_total_value_locked_deltas: Deltas<DeltaBigDecimal>, /* store_derived_total_value_locked */
    output: StoreAddBigDecimal,
) {
    let timestamp_seconds = clock.timestamp.unwrap().seconds;
    let day_id: i64 = timestamp_seconds / 86400;
    let hour_id: i64 = timestamp_seconds / 3600;

    output.delete_prefix(0, &format!("{}:{}:", keyer::UNISWAP_DAY_DATA, day_id - 1));
    output.delete_prefix(0, &format!("{}:{}:", keyer::POOL_DAY_DATA, day_id - 1));
    output.delete_prefix(0, &format!("{}:{}:", keyer::POOL_HOUR_DATA, hour_id - 1));
    output.delete_prefix(0, &format!("{}:{}:", keyer::TOKEN_DAY_DATA, day_id - 1));
    output.delete_prefix(0, &format!("{}:{}:", keyer::TOKEN_HOUR_DATA, hour_id - 1));

    let mut pool_total_value_locked_eth_new_value: BigDecimal = BigDecimal::zero();
    for delta in derived_total_value_locked_deltas.deltas {
        // here need to split on a couple of things
        // split on the first key to match the entity type
        // split on the last key to match the column

        log::info!("delta key {:?}", delta.key);

        match delta.key.as_str().split(":").nth(0).unwrap() {
            "pool" => {
                //todo: here we need to take the difference (newValue - oldValue) and
                // set the value for eth, usd, ethUntracked and userUntracked
            }
            "token" => {
                // nothing to do here for the moment
            }
            "factory" => {
                //todo: subtract the oldValue and
            }
            _ => {}
        }
        match delta.key.as_str().split(":").last().unwrap() {
            "eth" => {
                let pool_total_value_locked_eth_old_value = delta.old_value;
                pool_total_value_locked_eth_new_value = delta.new_value;

                let pool_total_value_locked_eth_diff: BigDecimal = pool_total_value_locked_eth_new_value
                    .clone()
                    .sub(pool_total_value_locked_eth_old_value.clone());

                log::info!("total value locked eth old: {}", pool_total_value_locked_eth_old_value);
                log::info!("total value locked eth new: {}", pool_total_value_locked_eth_new_value);
                log::info!("diff: {}", pool_total_value_locked_eth_diff);

                output.add(
                    delta.ordinal,
                    keyer::factory_total_value_locked_eth(),
                    &pool_total_value_locked_eth_diff,
                )
            }
            "usd" => {
                let bundle_eth_price: BigDecimal = match store_eth_prices.get_at(delta.ordinal, "bundle") {
                    None => continue, // FIXME(abourget): should we return zero?
                    Some(price) => price,
                };
                log::debug!("eth_price_usd: {}", bundle_eth_price);

                let total_value_locked_usd: BigDecimal =
                    pool_total_value_locked_eth_new_value.clone().mul(bundle_eth_price);

                log::info!("total value locked usd {}", total_value_locked_usd);

                // here we have to do a hackish way to set the value, to not have to
                // create a new store which would do the same but that would set the
                // value instead of summing it, what we do is calculate the difference
                // and simply add/sub the difference and that mimics the same as setting
                // the value
                let total_value_locked_usd_old_value: BigDecimal = delta.old_value;
                let diff: BigDecimal = total_value_locked_usd
                    .clone()
                    .sub(total_value_locked_usd_old_value.clone());

                log::info!("total value locked usd old {}", total_value_locked_usd_old_value);
                log::info!("diff {}", diff);

                // TODO: in the store_total_value_locked we will need store the pool address and the token address
                let pool_address = "TODO".to_string();
                let token_address = "TODO".to_string();

                output.add_many(
                    delta.ordinal,
                    &vec![
                        keyer::factory_total_value_locked_usd(),
                        keyer::pool_day_data_total_value_locked_usd(&pool_address, day_id.to_string()),
                        keyer::pool_hour_data_total_value_locked_usd(&pool_address, hour_id.to_string()),
                        keyer::token_day_data_total_value_locked_usd(&token_address, day_id.to_string()),
                        keyer::token_hour_data_total_value_locked_usd(&token_address, day_id.to_string()),
                    ],
                    &diff,
                );
                // same as the pool day data here, we need to add the old and the new in the other place
                output.add(
                    delta.ordinal,
                    keyer::uniswap_total_value_locked_usd(day_id.to_string()),
                    &total_value_locked_usd,
                )
            }
            _ => continue,
        }
    }
}

#[substreams::handlers::store]
pub fn store_total_tx_counts(clock: Clock, events: Events, output: StoreAddBigInt) {
    let timestamp_seconds = clock.timestamp.unwrap().seconds;
    let day_id: i64 = timestamp_seconds / 86400;
    let hour_id: i64 = timestamp_seconds / 3600;

    output.delete_prefix(0, &format!("{}:{}:", keyer::UNISWAP_DAY_DATA, day_id - 1));
    output.delete_prefix(0, &format!("{}:{}:", keyer::POOL_DAY_DATA, day_id - 1));
    output.delete_prefix(0, &format!("{}:{}:", keyer::POOL_HOUR_DATA, hour_id - 1));
    output.delete_prefix(0, &format!("{}:{}:", keyer::TOKEN_DAY_DATA, day_id - 1));
    output.delete_prefix(0, &format!("{}:{}:", keyer::TOKEN_HOUR_DATA, hour_id - 1));

    for event in events.pool_events {
        let keys: Vec<String> = vec![
            keyer::pool_total_tx_count(&event.pool_address),
            keyer::token_total_tx_count(&event.token0),
            keyer::token_total_tx_count(&event.token1),
            keyer::factory_total_tx_count(),
            keyer::uniswap_day_data_tx_count(day_id.to_string()),
            keyer::pool_day_data_tx_count(&event.pool_address, day_id.to_string()),
            keyer::pool_hour_data_tx_count(&event.pool_address, hour_id.to_string()),
            keyer::token_day_data_tx_count(&event.token0, day_id.to_string()),
            keyer::token_day_data_tx_count(&event.token1, day_id.to_string()),
            keyer::token_hour_data_tx_count(&event.token0, hour_id.to_string()),
            keyer::token_hour_data_tx_count(&event.token1, hour_id.to_string()),
        ];
        output.add_many(event.log_ordinal, &keys, &BigInt::from(1 as i32));
    }
}

#[substreams::handlers::store]
pub fn store_pool_fee_growth_global_x128(clock: Clock, events: Events, output: StoreSetBigInt) {
    let timestamp_seconds = clock.timestamp.unwrap().seconds;
    let day_id: i64 = timestamp_seconds / 86400;
    let hour_id: i64 = timestamp_seconds / 3600;

    output.delete_prefix(0, &format!("{}:{}:", keyer::POOL_DAY_DATA, day_id - 1));
    output.delete_prefix(0, &format!("{}:{}:", keyer::POOL_HOUR_DATA, hour_id - 1));

    for event in events.pool_events {
        let pool_address = event.pool_address;
        log::info!("pool address: {} trx_id:{}", pool_address, event.transaction_id);
        let (big_int_0, big_int_1) = rpc::fee_growth_global_x128_call(&pool_address);

        output.set_many(
            event.log_ordinal,
            &vec![
                keyer::pool_fee_growth_global_x128(&pool_address, "token0".to_string()),
                keyer::pool_day_data_fee_growth_global_x128(&pool_address, "token0".to_string(), day_id.to_string()),
                keyer::pool_hour_data_fee_growth_global_x128(&pool_address, "token0".to_string(), hour_id.to_string()),
            ],
            &big_int_0,
        );
        output.set_many(
            event.log_ordinal,
            &vec![
                keyer::pool_fee_growth_global_x128(&pool_address, "token1".to_string()),
                keyer::pool_day_data_fee_growth_global_x128(&pool_address, "token1".to_string(), day_id.to_string()),
                keyer::pool_hour_data_fee_growth_global_x128(&pool_address, "token1".to_string(), hour_id.to_string()),
            ],
            &big_int_1,
        );
    }
}

/**
 * STORE NATIVE AMOUNTS -> spits out any mint, swap and burn amounts
 */
#[substreams::handlers::store]
pub fn store_native_amounts(events: Events, store: StoreSetBigDecimal) {
    for pool_event in events.pool_events {
        log::info!(
            "transaction_id: {} and type of pool event {:?}",
            pool_event.transaction_id,
            pool_event.r#type.as_ref().unwrap(),
        );
        if let Some(token_amounts) = pool_event.get_amounts() {
            let amount0: BigDecimal = token_amounts.amount0;
            let amount1: BigDecimal = token_amounts.amount1;
            log::info!("amount 0: {} amount 1: {}", amount0, amount1);
            store.set_many(
                pool_event.log_ordinal,
                &vec![
                    keyer::token_native_total_value_locked(&token_amounts.token0_addr),
                    keyer::pool_native_total_value_locked_token(
                        &pool_event.pool_address.clone(),
                        &token_amounts.token0_addr,
                    ),
                ],
                &amount0,
            );
            store.set_many(
                pool_event.log_ordinal,
                &vec![
                    keyer::token_native_total_value_locked(&token_amounts.token1_addr),
                    keyer::pool_native_total_value_locked_token(
                        &pool_event.pool_address.clone(),
                        &token_amounts.token1_addr,
                    ),
                ],
                &amount1,
            );
        }
    }
}

#[substreams::handlers::store]
pub fn store_eth_prices(
    events: Events,                                /* map_extract_data_types */
    pools_store: StoreGetProto<Pool>,              /* store_pools */
    prices_store: StoreGetBigDecimal,              /* store_prices */
    tokens_whitelist_pools_store: StoreGetRaw,     /* store_tokens_whitelist_pools */
    total_native_amount_store: StoreGetBigDecimal, /* store_native_amounts */
    pool_liquidities_store: StoreGetBigInt,        /* store_pool_liquidities */
    store: StoreSetBigDecimal,
) {
    for pool_sqrt_price in events.pool_sqrt_prices {
        let ord = pool_sqrt_price.ordinal;
        log::debug!(
            "handling pool price update - addr: {} price: {}",
            pool_sqrt_price.pool_address,
            pool_sqrt_price.sqrt_price
        );
        let pool = pools_store.must_get_last(&keyer::pool_key(&pool_sqrt_price.pool_address));
        let token_0 = pool.token0.as_ref().unwrap();
        let token_1 = pool.token1.as_ref().unwrap();

        token_0.log();
        token_1.log();

        let bundle_eth_price_usd = price::get_eth_price_in_usd(&prices_store, ord);
        log::info!("bundle_eth_price_usd: {}", bundle_eth_price_usd);

        let token0_derived_eth_price: BigDecimal = price::find_eth_per_token(
            ord,
            &pool.address,
            &token_0.address,
            &pools_store,
            &pool_liquidities_store,
            &tokens_whitelist_pools_store,
            &total_native_amount_store,
            &prices_store,
        );
        log::info!(
            "token 0 {} derived eth price: {}",
            token_0.address,
            token0_derived_eth_price
        );

        let token1_derived_eth_price: BigDecimal = price::find_eth_per_token(
            ord,
            &pool.address,
            &token_1.address,
            &pools_store,
            &pool_liquidities_store,
            &tokens_whitelist_pools_store,
            &total_native_amount_store,
            &prices_store,
        );
        log::info!(
            "token 1 {} derived eth price: {}",
            token_1.address,
            token1_derived_eth_price
        );

        store.set(ord, "bundle", &bundle_eth_price_usd);

        store.set(ord, keyer::token_eth_price(&token_0.address), &token0_derived_eth_price);

        store.set(ord, keyer::token_eth_price(&token_1.address), &token1_derived_eth_price);
    }
}

#[substreams::handlers::store]
pub fn store_swaps_volume(
    clock: Clock,
    events: Events,
    store_pool: StoreGetProto<Pool>,
    store_total_tx_counts: StoreGetBigInt,
    store_eth_prices: StoreGetBigDecimal,
    output: StoreAddBigDecimal,
) {
    let timestamp_seconds = clock.timestamp.unwrap().seconds;
    let day_id: i64 = timestamp_seconds / 86400;
    let hour_id: i64 = timestamp_seconds / 3600;

    output.delete_prefix(0, &format!("{}:{}:", keyer::UNISWAP_DAY_DATA, day_id - 1));
    output.delete_prefix(0, &format!("{}:{}:", keyer::POOL_DAY_DATA, day_id - 1));
    output.delete_prefix(0, &format!("{}:{}:", keyer::POOL_HOUR_DATA, hour_id - 1));
    output.delete_prefix(0, &format!("{}:{}:", keyer::TOKEN_DAY_DATA, day_id - 1));
    output.delete_prefix(0, &format!("{}:{}:", keyer::TOKEN_HOUR_DATA, hour_id - 1));

    for event in events.pool_events {
        let ord = event.log_ordinal;
        let pool: Pool = match store_pool.get_last(keyer::pool_key(&event.pool_address)) {
            None => continue,
            Some(pool) => pool,
        };
        match store_total_tx_counts.has_last(keyer::pool_total_tx_count(&event.pool_address)) {
            false => {}
            true => {
                log::info!("type of pool event {:?}", event);
                match event.r#type.unwrap() {
                    MintEvent(_) => output.add(
                        ord,
                        format!("pool:{}:liquidityProviderCount", pool.address),
                        &BigDecimal::one(),
                    ),
                    SwapEvent(swap) => {
                        log::info!("transaction: {}", pool.transaction_id);
                        let eth_price_in_usd: BigDecimal = match store_eth_prices.get_at(ord, "bundle") {
                            None => {
                                panic!("bundle eth price not found")
                            }
                            Some(price) => price,
                        };

                        let token0_derived_eth_price: BigDecimal =
                            match store_eth_prices.get_at(ord, keyer::token_eth_price(&event.token0)) {
                                None => continue,
                                Some(price) => price,
                            };

                        let token1_derived_eth_price: BigDecimal =
                            match store_eth_prices.get_at(ord, keyer::token_eth_price(&event.token1)) {
                                None => continue,
                                Some(price) => price,
                            };

                        let amount0_abs = BigDecimal::try_from(swap.amount_0).unwrap().absolute();
                        let amount1_abs = BigDecimal::try_from(swap.amount_1).unwrap().absolute();

                        let volume_amounts = utils::get_adjusted_amounts(
                            &event.token0,
                            &event.token1,
                            &amount0_abs,
                            &amount1_abs,
                            &token0_derived_eth_price,
                            &token1_derived_eth_price,
                            &eth_price_in_usd,
                        );
                        let volume_eth = volume_amounts.stable_eth.clone().div(BigDecimal::from(2 as i32));
                        let volume_usd = volume_amounts.stable_usd.clone().div(BigDecimal::from(2 as i32));
                        let volume_usd_untracked = volume_amounts
                            .stable_usd_untracked
                            .clone()
                            .div(BigDecimal::from(2 as i32));

                        let fee_tier = BigDecimal::try_from(pool.fee_tier).unwrap();
                        let fee_eth: BigDecimal = volume_eth
                            .clone()
                            .mul(fee_tier.clone())
                            .div(BigDecimal::from(1000000 as u64));
                        let fee_usd: BigDecimal = volume_usd
                            .clone()
                            .mul(fee_tier.clone())
                            .div(BigDecimal::from(1000000 as u64));

                        output.add_many(
                            ord,
                            &vec![
                                keyer::swap_pool_volume_token_0(&event.pool_address),
                                keyer::swap_token_volume(&event.token0, "token0".to_string()),
                                keyer::swap_pool_day_data_volume_token(
                                    &event.pool_address,
                                    day_id.to_string(),
                                    &event.token0,
                                    "volumeToken0".to_string(),
                                ),
                                keyer::swap_token_day_data_volume_token(&event.token0, day_id.to_string()),
                                keyer::swap_pool_hour_data_volume_token(
                                    &event.pool_address,
                                    hour_id.to_string(),
                                    &event.token1,
                                    "volumeToken0".to_string(),
                                ),
                                keyer::swap_token_hour_data_volume_token(&event.token0, hour_id.to_string()),
                            ],
                            &amount0_abs,
                        );
                        output.add_many(
                            ord,
                            &vec![
                                keyer::swap_pool_volume_token_1(&event.pool_address),
                                keyer::swap_token_volume(&event.token1, "token1".to_string()),
                                keyer::swap_pool_day_data_volume_token(
                                    &event.pool_address,
                                    day_id.to_string(),
                                    &event.token1,
                                    "volumeToken1".to_string(),
                                ),
                                keyer::swap_token_day_data_volume_token(&event.token1, day_id.to_string()),
                                keyer::swap_pool_hour_data_volume_token(
                                    &event.pool_address,
                                    hour_id.to_string(),
                                    &event.token1,
                                    "volumeToken1".to_string(),
                                ),
                                keyer::swap_token_hour_data_volume_token(&event.token1, hour_id.to_string()),
                            ],
                            &amount1_abs,
                        );
                        output.add_many(
                            ord,
                            &vec![
                                keyer::swap_pool_volume_usd(&event.pool_address),
                                keyer::swap_token_volume_usd(&event.token0),
                                keyer::swap_token_volume_usd(&event.token1),
                                keyer::swap_factory_total_volume_usd(),
                                keyer::swap_uniswap_day_data_volume_usd(day_id.to_string()),
                                keyer::swap_pool_day_data_volume_usd(&event.pool_address, day_id.to_string()),
                                keyer::swap_token_day_data_volume_usd(&event.token0, day_id.to_string()),
                                keyer::swap_token_day_data_volume_usd(&event.token1, day_id.to_string()),
                                keyer::swap_pool_hour_data_volume_usd(&event.pool_address, hour_id.to_string()),
                                keyer::swap_token_hour_data_volume_usd(&event.token0, hour_id.to_string()),
                                keyer::swap_token_hour_data_volume_usd(&event.token1, hour_id.to_string()),
                            ],
                            //TODO: CONFIRM EQUALS -> IN THE SUBGRAPH THIS IS THE VOLUME USD
                            &volume_usd,
                        );
                        output.add_many(
                            ord,
                            &vec![
                                keyer::swap_factory_untracked_volume_usd(),
                                keyer::swap_pool_untracked_volume_usd(&event.pool_address),
                                keyer::swap_token_volume_untracked_volume_usd(&event.token0),
                                keyer::swap_token_volume_untracked_volume_usd(&event.token1),
                            ],
                            &volume_usd_untracked,
                        );
                        output.add_many(
                            ord,
                            &vec![
                                keyer::swap_factory_total_volume_eth(),
                                keyer::swap_uniswap_day_data_volume_eth(day_id.to_string()),
                            ],
                            &volume_eth.clone(),
                        );
                        output.add_many(
                            ord,
                            &vec![
                                keyer::swap_pool_fee_usd(&event.pool_address),
                                keyer::swap_token_fee_usd(&event.token0),
                                keyer::swap_token_fee_usd(&event.token1),
                                keyer::swap_factory_total_fees_usd(),
                                keyer::swap_uniswap_day_data_fees_usd(day_id.to_string()),
                                keyer::swap_pool_day_data_fees_usd(&event.pool_address, day_id.to_string()),
                                keyer::swap_token_day_data_fees_usd(&event.token0, day_id.to_string()),
                                keyer::swap_token_day_data_fees_usd(&event.token1, day_id.to_string()),
                                keyer::swap_pool_hour_data_fees_usd(&event.pool_address, hour_id.to_string()),
                                keyer::swap_token_hour_data_fees_usd(&event.token0, hour_id.to_string()),
                                keyer::swap_token_hour_data_fees_usd(&event.token1, hour_id.to_string()),
                            ],
                            &fee_usd,
                        );
                        output.add(ord, keyer::swap_factory_total_fees_eth(), &fee_eth);
                    }
                    _ => {}
                }
            }
        }
    }
}

#[substreams::handlers::store]
pub fn store_token_tvl(clock: Clock, events: Events, output: StoreAddBigDecimal) {
    let timestamp_seconds = clock.timestamp.unwrap().seconds;
    let day_id: i64 = timestamp_seconds / 86400;
    let hour_id: i64 = timestamp_seconds / 3600;
    let prev_day_id = day_id - 1;
    let prev_hour_id = hour_id - 1;

    log::info!("hour_id {}", hour_id);

    output.delete_prefix(0, &format!("TokenDayData:{prev_day_id}:"));
    output.delete_prefix(0, &format!("TokenHourData:{prev_hour_id}:"));

    for pool_event in events.pool_events {
        let token_amounts = pool_event.get_amounts().unwrap();
        let pool_address = pool_event.pool_address.to_string();
        let token0_addr = pool_event.token0.to_string();
        let token1_addr = pool_event.token1.to_string();
        let ord = pool_event.log_ordinal;

        output.add_many(
            ord,
            &vec![
                &format!("pool:{pool_address}:{token0_addr}:token0"),
                &format!("token:{token0_addr}"),
                &format!("TokenDayData:{token0_addr}:{day_id}"),
                &format!("TokenHourData:{token0_addr}:{hour_id}"),
            ],
            &token_amounts.amount0,
        );
        output.add_many(
            ord,
            &vec![
                &format!("pool:{pool_address}:{token1_addr}:token1"),
                &format!("token:{token1_addr}"),
                &format!("TokenDayData:{token1_addr}:{day_id}"),
                &format!("TokenHourData:{token1_addr}:{hour_id}"),
            ],
            &token_amounts.amount1,
        );
    }
}

#[substreams::handlers::store]
pub fn store_derived_tvl(
    clock: Clock,
    events: Events,
    token_total_value_locked: StoreGetBigDecimal,
    pools_store: StoreGetProto<Pool>,
    eth_prices_store: StoreGetBigDecimal,
    output: StoreSetBigDecimal,
) {
    let timestamp_seconds = clock.timestamp.unwrap().seconds;
    let day_id: i64 = timestamp_seconds / 86400;
    let hour_id: i64 = timestamp_seconds / 3600;

    output.delete_prefix(0, &format!("{}:{}:", keyer::TOKEN_DAY_DATA, day_id - 1));
    output.delete_prefix(0, &format!("{}:{}:", keyer::TOKEN_HOUR_DATA, hour_id - 1));
    output.delete_prefix(0, &format!("{}:{}:", keyer::POOL_DAY_DATA, day_id - 1));
    output.delete_prefix(0, &format!("{}:{}:", keyer::POOL_HOUR_DATA, hour_id - 1));

    for pool_event in events.pool_events {
        let ord = pool_event.log_ordinal;
        let eth_price_usd: BigDecimal = match &eth_prices_store.get_at(ord, "bundle") {
            None => continue,
            Some(price) => price.with_prec(100),
        };

        let pool = pools_store.must_get_last(keyer::pool_key(&pool_event.pool_address));

        let token0_derive_eth = utils::get_derived_eth_price(
            pool_event.log_ordinal,
            &pool.token0.as_ref().unwrap().address(),
            &eth_prices_store,
        );

        let token1_derive_eth = utils::get_derived_eth_price(
            pool_event.log_ordinal,
            &pool.token1.as_ref().unwrap().address(),
            &eth_prices_store,
        );

        let total_value_locked_token0 = utils::get_total_value_locked_token(
            pool_event.log_ordinal,
            &pool.token0.as_ref().unwrap().address(),
            &token_total_value_locked,
        );

        let total_value_locked_token1 = utils::get_total_value_locked_token(
            pool_event.log_ordinal,
            &pool.token1.as_ref().unwrap().address(),
            &token_total_value_locked,
        );

        log::info!("total_value_locked_token0: {}", total_value_locked_token0);
        log::info!("total_value_locked_token1: {}", total_value_locked_token1);

        // not sure about this part
        let derived_token0_eth = total_value_locked_token0.clone().mul(token0_derive_eth.clone());
        let derived_token1_eth = total_value_locked_token1.clone().mul(token1_derive_eth.clone());
        log::info!("derived_token0_eth: {}", derived_token0_eth);
        log::info!("derived_token1_eth: {}", derived_token1_eth);

        let amounts = utils::get_adjusted_amounts(
            pool.token0.as_ref().unwrap().address(),
            pool.token1.as_ref().unwrap().address(),
            &total_value_locked_token0,
            &total_value_locked_token1,
            &token0_derive_eth,
            &token1_derive_eth,
            &eth_price_usd,
        );

        let derived_token0_usd = total_value_locked_token0
            .clone()
            .mul(token0_derive_eth.clone().mul(eth_price_usd.clone()));
        let derived_token1_usd = total_value_locked_token1
            .clone()
            .mul(token1_derive_eth.clone().mul(eth_price_usd.clone()));

        // token0.totalValueLockedUSD
        output.set_many(
            pool_event.log_ordinal,
            &vec![
                &keyer::token_derived_total_value_locked_usd(pool.token0.as_ref().unwrap().address(), &"0".to_string()),
                &keyer::token_day_data_derived_total_value_locked_usd(
                    pool.token0.as_ref().unwrap().address(),
                    &"0".to_string(),
                    &day_id.to_string(),
                ),
                &keyer::token_hour_data_derived_total_value_locked_usd(
                    pool.token0.as_ref().unwrap().address(),
                    &"0".to_string(),
                    &hour_id.to_string(),
                ),
            ],
            &derived_token0_usd,
        );

        // token1.totalValueLockedUSD
        output.set_many(
            pool_event.log_ordinal,
            &vec![
                &keyer::token_derived_total_value_locked_usd(pool.token1.as_ref().unwrap().address(), &"1".to_string()),
                &keyer::token_day_data_derived_total_value_locked_usd(
                    pool.token1.as_ref().unwrap().address(),
                    &"1".to_string(),
                    &day_id.to_string(),
                ),
                &keyer::token_hour_data_derived_total_value_locked_usd(
                    pool.token1.as_ref().unwrap().address(),
                    &"1".to_string(),
                    &hour_id.to_string(),
                ),
            ],
            &derived_token1_usd,
        );

        // pool.totalValueLockedETH
        output.set(
            pool_event.log_ordinal,
            &keyer::pool_derived_total_value_locked_eth(
                &pool.address,
                pool.token0.as_ref().unwrap().address(),
                &"0".to_string(),
            ),
            &amounts.stable_eth,
        );
        output.set(
            pool_event.log_ordinal,
            &keyer::pool_derived_total_value_locked_eth(
                &pool.address,
                pool.token1.as_ref().unwrap().address(),
                &"1".to_string(),
            ),
            &amounts.stable_eth,
        );

        // pool.totalValueLockedUSD
        output.set_many(
            pool_event.log_ordinal,
            &vec![
                keyer::pool_derived_total_value_locked_usd(
                    &pool.address,
                    pool.token0.as_ref().unwrap().address(),
                    &"0".to_string(),
                ),
                keyer::pool_day_derived_total_value_locked_usd(
                    &pool.address,
                    pool.token0.as_ref().unwrap().address(),
                    &"0".to_string(),
                    &day_id.to_string(),
                ),
                keyer::pool_hour_derived_total_value_locked_usd(
                    &pool.address,
                    pool.token0.as_ref().unwrap().address(),
                    &"0".to_string(),
                    &hour_id.to_string(),
                ),
            ],
            &amounts.stable_usd,
        );
        output.set_many(
            pool_event.log_ordinal,
            &vec![
                keyer::pool_derived_total_value_locked_usd(
                    &pool.address,
                    pool.token1.as_ref().unwrap().address(),
                    &"1".to_string(),
                ),
                keyer::pool_day_derived_total_value_locked_usd(
                    &pool.address,
                    pool.token1.as_ref().unwrap().address(),
                    &"1".to_string(),
                    &day_id.to_string(),
                ),
                keyer::pool_hour_derived_total_value_locked_usd(
                    &pool.address,
                    pool.token1.as_ref().unwrap().address(),
                    &"1".to_string(),
                    &hour_id.to_string(),
                ),
            ],
            &amounts.stable_usd,
        );

        // pool.totalValueLockedETHUntracked
        output.set(
            pool_event.log_ordinal,
            &keyer::pool_derived_total_value_locked_eth_untracked(
                &pool.address,
                pool.token0.as_ref().unwrap().address(),
                &"0".to_string(),
            ),
            &amounts.stable_eth_untracked,
        );
        output.set(
            pool_event.log_ordinal,
            &keyer::pool_derived_total_value_locked_eth_untracked(
                &pool.address,
                pool.token1.as_ref().unwrap().address(),
                &"1".to_string(),
            ),
            &amounts.stable_eth_untracked,
        );

        // pool.totalValueLockedUSDUntracked
        output.set(
            pool_event.log_ordinal,
            &keyer::pool_derived_total_value_locked_usd_untracked(
                &pool.address,
                pool.token1.as_ref().unwrap().address(),
                &"1".to_string(),
            ),
            &amounts.stable_usd_untracked,
        );
        output.set(
            pool_event.log_ordinal,
            &keyer::pool_derived_total_value_locked_usd_untracked(
                &pool.address,
                pool.token1.as_ref().unwrap().address(),
                &"1".to_string(),
            ),
            &amounts.stable_usd_untracked,
        )
    }
}

#[substreams::handlers::store]
pub fn store_derived_factory_tvl(
    clock: Clock,
    derived_tvl_deltas: Deltas<DeltaBigDecimal>,
    output: StoreAddBigDecimal,
) {
    let timestamp_seconds = clock.timestamp.unwrap().seconds;
    let day_id: i64 = timestamp_seconds / 86400;
    output.delete_prefix(0, &format!("{}:{}:", keyer::UNISWAP_DAY_DATA, day_id - 1));

    for delta in derived_tvl_deltas.deltas {
        if utils::extract_item_from_key_at_position(&delta.key, 0).eq("pool") {
            log::info!("stats with pool:");
            if utils::extract_item_from_key_last_item(&delta.key).eq("eth") {
                output.add(
                    delta.ordinal,
                    &format!("factory:totalValueLockedETH"),
                    calculate_diff(&delta),
                );
            }

            if utils::extract_item_from_key_last_item(&delta.key).eq("ethUntracked") {
                output.add(
                    delta.ordinal,
                    &format!("factory:totalValueLockedETHUntracked"),
                    calculate_diff(&delta),
                );
            }

            if utils::extract_item_from_key_last_item(&delta.key).eq("usd") {
                output.add_many(
                    delta.ordinal,
                    &vec![
                        format!("factory:totalValueLockedUSD"),
                        format!("{}:totalValueLockedUSD:{}", keyer::UNISWAP_DAY_DATA, day_id.to_string()),
                    ],
                    calculate_diff(&delta),
                );
            }

            if utils::extract_item_from_key_last_item(&delta.key).eq("usdUntracked") {
                output.add(
                    delta.ordinal,
                    &format!("factory:totalValueLockedUSDUntracked"),
                    calculate_diff(&delta),
                );
            }
        }
    }
}

fn calculate_diff(delta: &DeltaBigDecimal) -> BigDecimal {
    let old_value = delta.old_value.clone();
    let new_value = delta.new_value.clone();
    return new_value.clone().sub(old_value);
}

#[substreams::handlers::store]
pub fn store_ticks_liquidities(events: Events, output: StoreAddBigInt) {
    for event in events.pool_events {
        let pool = event.pool_address;
        match event.r#type.unwrap() {
            Type::Mint(mint) => {
                output.add_many(
                    event.log_ordinal,
                    &vec![
                        keyer::tick_liquidities_gross(&pool, &mint.tick_lower),
                        keyer::tick_liquidities_net(&pool, &mint.tick_lower),
                        keyer::tick_liquidities_gross(&pool, &mint.tick_upper),
                    ],
                    &BigInt::try_from(mint.amount.clone()).unwrap(),
                );
                output.add(
                    event.log_ordinal,
                    keyer::tick_liquidities_net(&pool, &mint.tick_upper),
                    &BigInt::try_from(mint.amount.clone()).unwrap().neg(),
                );
            }
            Type::Burn(burn) => {
                output.add_many(
                    event.log_ordinal,
                    &vec![
                        keyer::tick_liquidities_gross(&pool, &burn.tick_lower),
                        keyer::tick_liquidities_net(&pool, &burn.tick_lower),
                        keyer::tick_liquidities_gross(&pool, &burn.tick_upper),
                    ],
                    &BigInt::try_from(&burn.amount).unwrap().neg(),
                );
                output.add(
                    event.log_ordinal,
                    keyer::tick_liquidities_net(&pool, &burn.tick_upper),
                    &BigInt::try_from(&burn.amount).unwrap(),
                );
            }
            _ => {}
        }
    }
}

#[substreams::handlers::map]
pub fn graph_out(
    clock: Clock,
    pool_count_deltas: Deltas<DeltaBigInt>,              /* store_pool_count */
    tx_count_deltas: Deltas<DeltaBigInt>,                /* store_total_tx_counts deltas */
    swaps_volume_deltas: Deltas<DeltaBigDecimal>,        /* store_swaps_volume */
    derived_factory_tvl_deltas: Deltas<DeltaBigDecimal>, /* store_derived_factory_tvl */
    derived_eth_prices_deltas: Deltas<DeltaBigDecimal>,  /* store_eth_prices */
    events: Events,                                      /* map_extract_data_types */
    pools_created: Pools,                                /* map_pools_created */
    pool_sqrt_price_deltas: Deltas<DeltaProto<PoolSqrtPrice>>, /* store_pool_sqrt_price */
    pool_liquidities_store_deltas: Deltas<DeltaBigInt>,  /* store_pool_liquidities */
    token_tvl_deltas: Deltas<DeltaBigDecimal>,           /* store_token_tvl */
    pool_fee_growth_global_x128_deltas: Deltas<DeltaBigInt>, /* store_pool_fee_growth_global_x128 */
    price_deltas: Deltas<DeltaBigDecimal>,               /* store_prices */
    tokens_store: StoreGetInt64,                         /* store_tokens */
    tokens_whitelist_pools: Deltas<DeltaArray<String>>,  /* store_tokens_whitelist_pools */
    derived_tvl_deltas: Deltas<DeltaBigDecimal>,         /* store_derived_tvl */
    ticks_liquidities_deltas: Deltas<DeltaBigInt>,       /* store_ticks_liquidities */
    tx_count_store: StoreGetBigInt,                      /* store_total_tx_counts */
    store_eth_prices: StoreGetBigDecimal,                /* store_eth_prices */
) -> Result<EntityChanges, Error> {
    let mut tables = Tables::new();

    if clock.number == 12369621 {
        // FIXME: Hard-coded start block, how could we pull that from the manifest?
        // FIXME: ideally taken from the params of the module
        db::factory_created_factory_entity_change(&mut tables);
        db::created_bundle_entity_change(&mut tables);
    }
    // Bundle
    db::bundle_store_eth_price_usd_bundle_entity_change(&mut tables, &derived_eth_prices_deltas);

    // Factory:
    db::pool_created_factory_entity_change(&mut tables, &pool_count_deltas);
    db::tx_count_factory_entity_change(&mut tables, &tx_count_deltas);
    db::swap_volume_factory_entity_change(&mut tables, &swaps_volume_deltas);
    db::tvl_factory_entity_change(&mut tables, &derived_factory_tvl_deltas);

    // Pool:
    db::pools_created_pool_entity_change(&mut tables, &pools_created);
    db::sqrt_price_and_tick_pool_entity_change(&mut tables, &pool_sqrt_price_deltas);
    db::liquidities_pool_entity_change(&mut tables, &pool_liquidities_store_deltas);
    db::fee_growth_global_pool_entity_change(&mut tables, events.fee_growth_global_updates);
    db::total_value_locked_pool_entity_change(&mut tables, &derived_tvl_deltas);
    db::total_value_locked_by_token_pool_entity_change(&mut tables, &token_tvl_deltas);
    db::fee_growth_global_x128_pool_entity_change(&mut tables, &pool_fee_growth_global_x128_deltas);
    db::price_pool_entity_change(&mut tables, &price_deltas);
    db::tx_count_pool_entity_change(&mut tables, &tx_count_deltas);
    db::swap_volume_pool_entity_change(&mut tables, &swaps_volume_deltas);

    // Tokens:
    db::tokens_created_token_entity_change(&mut tables, &pools_created, tokens_store);
    db::swap_volume_token_entity_change(&mut tables, &swaps_volume_deltas);
    db::tx_count_token_entity_change(&mut tables, &tx_count_deltas);
    db::total_value_locked_by_token_token_entity_change(&mut tables, &token_tvl_deltas);
    db::total_value_locked_usd_token_entity_change(&mut tables, &derived_tvl_deltas);
    db::derived_eth_prices_token_entity_change(&mut tables, &derived_eth_prices_deltas);
    db::whitelist_token_entity_change(&mut tables, tokens_whitelist_pools);

    // Tick:
    db::create_tick_entity_change(&mut tables, events.ticks_created);
    db::update_tick_entity_change(&mut tables, events.ticks_updated);
    db::liquidities_tick_entity_change(&mut tables, ticks_liquidities_deltas);

    // Position:
    // TODO: validate all the positions here
    db::position_create_entity_change(&mut tables, &events.created_positions);
    db::increase_liquidity_position_entity_change(&mut tables, &events.increase_liquidity_positions);
    db::decrease_liquidity_position_entity_change(&mut tables, &events.decrease_liquidity_positions);
    db::collect_position_entity_change(&mut tables, &events.collect_positions);
    db::transfer_position_entity_change(&mut tables, &events.transfer_positions);

    // PositionSnapshot:
    // TODO: validate all the snapshot positions here
    db::snapshot_positions_create_entity_change(&mut tables, &events.created_positions);
    db::increase_liquidity_snapshot_position_entity_change(
        &mut tables,
        clock.number,
        &events.increase_liquidity_positions,
    );
    db::decrease_liquidity_snapshot_position_entity_change(
        &mut tables,
        clock.number,
        &events.decrease_liquidity_positions,
    );
    db::collect_snapshot_position_entity_change(&mut tables, clock.number, &events.collect_positions);
    db::transfer_snapshot_position_entity_change(&mut tables, clock.number, &events.transfer_positions);

    // Transaction:
    db::transaction_entity_change(&mut tables, events.transactions);

    // Swap, Mint, Burn:
    db::swaps_mints_burns_created_entity_change(&mut tables, events.pool_events, tx_count_store, store_eth_prices);

    // Flashes:
    // TODO: implement flashes entity change - UNISWAP has not done this part
    db::flashes_update_pool_fee_entity_change(&mut tables, events.flashes);

    // Uniswap day data:
    db::uniswap_day_data_create_entity_change(&mut tables, &tx_count_deltas);
    db::tx_count_uniswap_day_data_entity_change(&mut tables, &tx_count_deltas);
    db::totals_uniswap_day_data_entity_change(&mut tables, &derived_factory_tvl_deltas);
    db::volumes_uniswap_day_data_entity_change(&mut tables, &swaps_volume_deltas);

    // Pool Day data:
    db::pool_day_data_create_entity_change(&mut tables, &tx_count_deltas);
    db::tx_count_pool_day_data_entity_change(&mut tables, &tx_count_deltas);
    db::liquidities_pool_day_data_entity_change(&mut tables, &pool_liquidities_store_deltas);
    db::sqrt_price_and_tick_pool_day_data_entity_change(&mut tables, &pool_sqrt_price_deltas);
    db::swap_volume_pool_day_data_entity_change(&mut tables, &swaps_volume_deltas);
    db::token_prices_pool_day_data_entity_change(&mut tables, &price_deltas);
    db::fee_growth_global_x128_pool_day_data_entity_change(&mut tables, &pool_fee_growth_global_x128_deltas);
    db::total_value_locked_usd_pool_day_data_entity_change(&mut tables, &derived_tvl_deltas);

    // Pool Hour data:
    db::pool_hour_data_create_entity_change(&mut tables, &tx_count_deltas);
    db::tx_count_pool_hour_data_entity_change(&mut tables, &tx_count_deltas);
    db::liquidities_pool_hour_data_entity_change(&mut tables, &pool_liquidities_store_deltas);
    db::sqrt_price_and_tick_pool_hour_data_entity_change(&mut tables, &pool_sqrt_price_deltas);
    db::swap_volume_pool_hour_data_entity_change(&mut tables, &swaps_volume_deltas);
    db::token_prices_pool_hour_data_entity_change(&mut tables, &price_deltas);
    db::fee_growth_global_x128_pool_hour_data_entity_change(&mut tables, &pool_fee_growth_global_x128_deltas);
    db::total_value_locked_usd_pool_hour_data_entity_change(&mut tables, &derived_tvl_deltas);

    // Token Day data:
    db::token_day_data_create_entity_change(&mut tables, &tx_count_deltas);
    db::swap_volume_token_day_data_entity_change(&mut tables, &swaps_volume_deltas);
    db::total_value_locked_usd_token_day_data_entity_change(&mut tables, &derived_tvl_deltas);
    db::total_value_locked_token_day_data_entity_change(&mut tables, &token_tvl_deltas);
    db::token_prices_token_day_data_entity_change(&mut tables, &price_deltas);

    // Token Hour data:
    db::token_hour_data_create_entity_change(&mut tables, &tx_count_deltas);
    db::swap_volume_token_hour_data_entity_change(&mut tables, &swaps_volume_deltas);
    db::total_value_locked_usd_token_hour_data_entity_change(&mut tables, &derived_tvl_deltas);
    db::total_value_locked_token_hour_data_entity_change(&mut tables, &token_tvl_deltas);
    db::token_prices_token_hour_data_entity_change(&mut tables, &price_deltas);

    Ok(tables.to_entity_changes())
}
