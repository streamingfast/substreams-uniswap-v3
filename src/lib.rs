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

use crate::abi::pool::events::Swap;
use crate::ethpb::v2::{Block, StorageChange};
use crate::pb::uniswap;
use crate::pb::uniswap::events::pool_event::Type;
use crate::pb::uniswap::events::pool_event::Type::{
    Burn as BurnEvent, Mint as MintEvent, Swap as SwapEvent,
};
use crate::pb::uniswap::events::PoolSqrtPrice;
use crate::pb::uniswap::{events, Events, SnapshotPosition, SnapshotPositions};
use crate::pb::uniswap::{Erc20Token, Erc20Tokens, Pool, Pools};
use crate::price::WHITELIST_TOKENS;
use crate::tables::Tables;
use crate::uniswap::events::position::PositionType::{
    Collect, DecreaseLiquidity, IncreaseLiquidity, Transfer,
};
use crate::utils::UNISWAP_V3_FACTORY;
use std::collections::HashMap;
use std::ops::{Add, Div, Mul, Sub};
use substreams::errors::Error;
use substreams::hex;
use substreams::pb::substreams::Clock;
use substreams::prelude::*;
use substreams::scalar::{BigDecimal, BigInt};
use substreams::store::{
    DeltaArray, DeltaBigDecimal, DeltaBigInt, DeltaProto, StoreAddBigDecimal, StoreAddBigInt,
    StoreAppend, StoreGetBigDecimal, StoreGetBigInt, StoreGetProto, StoreGetRaw,
    StoreSetBigDecimal, StoreSetBigInt, StoreSetProto,
};
use substreams::{log, Hex};
use substreams_entity_change::pb::entity::EntityChanges;
use substreams_ethereum::pb::eth::v2;
use substreams_ethereum::pb::eth::v2::TransactionTrace;
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
                    fee_tier: Some(event.fee.into()),
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
                    pool.fee_tier.as_ref().unwrap().into(),
                ),
            ],
            &pool,
        );
    }
}

#[substreams::handlers::store]
pub fn store_tokens(pools: Pools, store: StoreAddInt64) {
    for pool in pools.pools {
        store.add(
            pool.log_ordinal,
            keyer::token_key(&pool.token0_ref().address()),
            1,
        );
        store.add(
            pool.log_ordinal,
            keyer::token_key(&pool.token1_ref().address()),
            1,
        );
    }
}

#[substreams::handlers::store]
pub fn store_pool_count(pools: Pools, store: StoreAddBigInt) {
    for pool in pools.pools {
        store.add(
            pool.log_ordinal,
            keyer::factory_pool_count_key(),
            &BigInt::one(),
        )
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
        output_append.append_all(
            1,
            keyer::token_pool_whitelist(&token.address),
            token.whitelist_pools,
        );
    }
}

#[substreams::handlers::map]
pub fn map_extract_data_types(
    block: Block,
    pools_store: StoreGetProto<Pool>,
) -> Result<Events, Error> {
    let mut events = Events::default();

    let mut pool_sqrt_prices: Vec<events::PoolSqrtPrice> = vec![];
    let mut pool_liquidities: Vec<events::PoolLiquidity> = vec![];
    let mut fee_growth_global_updates: Vec<events::FeeGrowthGlobal> = vec![];
    let mut pool_events: Vec<events::PoolEvent> = vec![];
    let mut transactions: Vec<events::Transaction> = vec![];
    let mut positions: Vec<events::Position> = vec![];
    let mut flashes: Vec<events::Flash> = vec![];
    let mut ticks_created: Vec<events::TickCreated> = vec![];
    let mut ticks_updated: Vec<events::TickUpdated> = vec![];

    let timestamp = block.timestamp_seconds();

    for trx in block.transactions() {
        for (log, call) in logs_with_calls(trx) {
            let pool_address = &Hex(log.clone().address).to_string();
            let pool_key = &keyer::pool_key(&pool_address);
            let transactions_id = Hex(&trx.hash).to_string();

            match pools_store.get_last(pool_key) {
                Some(pool) => {
                    filtering::extract_pool_sqrt_prices(&mut pool_sqrt_prices, log, pool_address);

                    filtering::extract_pool_liquidities(
                        &mut pool_liquidities,
                        log,
                        &call.storage_changes,
                        &pool,
                    );

                    // FeeGrowthUpdate
                    filtering::extract_fee_growth_update(
                        &mut fee_growth_global_updates,
                        log,
                        &call.storage_changes,
                        &pool,
                    );

                    // TokenEvents
                    filtering::extract_pool_events(
                        &mut pool_events,
                        &transactions_id,
                        &Hex(&trx.from).to_string(),
                        log,
                        &pool,
                        timestamp,
                        block.number,
                        &call.storage_changes,
                    );

                    filtering::extract_transactions(
                        &mut transactions,
                        log,
                        &trx,
                        timestamp,
                        block.number,
                    );

                    filtering::extract_flashes(&mut flashes, &log, &pools_store, pool_key);
                }
                _ => (), // do nothing
            }

            //todo: pools_store needed to check if the pool exists in the store
            // by checking the index:token1:token2 instead of the address...
            // could this be done smarter and checked with the log_address ?

            // Positions
            filtering::extract_positions(
                &mut positions,
                log,
                &transactions_id,
                &pools_store,
                timestamp,
                block.number,
            );
            // Positions
        }
    }

    Ok(events)
}

fn logs_with_calls(trx: &TransactionTrace) -> Vec<(&v2::Log, &v2::Call)> {
    let mut res: Vec<(&v2::Log, &v2::Call)> = vec![];

    for call in trx.calls.iter() {
        if call.state_reverted {
            continue;
        }
        for log in call.logs.iter() {
            res.push((&log, &call));
        }
    }

    res.sort_by(|x, y| x.0.ordinal.cmp(&y.0.ordinal));
    res
}

#[substreams::handlers::store]
pub fn store_pool_sqrt_price(events: Events, store: StoreSetProto<events::PoolSqrtPrice>) {
    for sqrt_price in events.pool_sqrt_prices {
        store.set(
            sqrt_price.ordinal,
            keyer::pool_sqrt_price_key(&sqrt_price.pool_address),
            &sqrt_price,
        )
    }
}

#[substreams::handlers::store]
pub fn store_prices(events: Events, pools_store: StoreGetProto<Pool>, store: StoreSetBigDecimal) {
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

                let sqrt_price = BigDecimal::from(sqrt_price_update.sqrt_price.unwrap());
                log::debug!("sqrtPrice: {}", sqrt_price.to_string());

                let tokens_price: (BigDecimal, BigDecimal) =
                    price::sqrt_price_x96_to_token_prices(sqrt_price, &token0, &token1);
                log::debug!("token prices: {} {}", tokens_price.0, tokens_price.1);

                store.set_many(
                    sqrt_price_update.ordinal,
                    &vec![
                        keyer::prices_pool_token_key(
                            &pool.address,
                            &token0.address,
                            "token0".to_string(),
                        ),
                        keyer::prices_token_pair(
                            &pool.token0.as_ref().unwrap().address,
                            &pool.token1.as_ref().unwrap().address,
                        ),
                    ],
                    &tokens_price.0,
                );

                store.set_many(
                    sqrt_price_update.ordinal,
                    &vec![
                        keyer::prices_pool_token_key(
                            &pool.address,
                            &token1.address,
                            "token1".to_string(),
                        ),
                        keyer::prices_token_pair(
                            &pool.token1.as_ref().unwrap().address,
                            &pool.token0.as_ref().unwrap().address,
                        ),
                    ],
                    &tokens_price.1,
                );
            }
        }
    }
}

#[substreams::handlers::store]
pub fn store_pool_liquidities(events: Events, store: StoreSetBigInt) {
    for pool_liquidity in events.pool_liquidities {
        let big_int: BigInt = pool_liquidity.liquidity.unwrap().into();
        store.set(
            0,
            keyer::pool_liquidity(&pool_liquidity.pool_address),
            &big_int,
        )
    }
}

#[substreams::handlers::store]
pub fn store_totals(
    clock: Clock,
    store_eth_prices: StoreGetBigDecimal,
    total_value_locked_deltas: Deltas<DeltaBigDecimal>,
    store: StoreAddBigDecimal,
) {
    let timestamp_seconds = clock.timestamp.unwrap().seconds;
    let day_id: i64 = timestamp_seconds / 86400;
    store.delete_prefix(0, &format!("uniswap_day_data:{}:", day_id - 1));

    let mut pool_total_value_locked_eth_new_value: BigDecimal = BigDecimal::zero();
    for delta in total_value_locked_deltas.deltas {
        log::info!("delta key {:?}", delta.key);
        if !delta.key.starts_with("factory:") {
            continue;
        }
        match delta.key.as_str().split(":").last().unwrap() {
            "eth" => {
                let pool_total_value_locked_eth_old_value = delta.old_value;
                pool_total_value_locked_eth_new_value = delta.new_value;

                let pool_total_value_locked_eth_diff: BigDecimal =
                    pool_total_value_locked_eth_new_value
                        .clone()
                        .sub(pool_total_value_locked_eth_old_value.clone());

                log::info!(
                    "total value locked eth old: {}",
                    pool_total_value_locked_eth_old_value
                );
                log::info!(
                    "total value locked eth new: {}",
                    pool_total_value_locked_eth_new_value
                );
                log::info!("diff: {}", pool_total_value_locked_eth_diff);

                store.add(
                    delta.ordinal,
                    keyer::factory_total_value_locked_eth(),
                    &pool_total_value_locked_eth_diff,
                )
            }
            "usd" => {
                let bundle_eth_price: BigDecimal =
                    match store_eth_prices.get_at(delta.ordinal, "bundle") {
                        None => continue, // FIXME(abourget): should we return zero?
                        Some(price) => price,
                    };
                log::debug!("eth_price_usd: {}", bundle_eth_price);

                let total_value_locked_usd: BigDecimal = pool_total_value_locked_eth_new_value
                    .clone()
                    .mul(bundle_eth_price);

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

                log::info!(
                    "total value locked usd old {}",
                    total_value_locked_usd_old_value
                );
                log::info!("diff {}", diff);

                store.add(
                    delta.ordinal,
                    keyer::factory_total_value_locked_usd(),
                    &diff,
                );
                store.add(
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
    output.delete_prefix(0, &format!("uniswap_day_data:{}:", day_id - 1));

    for event in events.pool_events {
        let keys: Vec<String> = vec![
            keyer::pool_total_tx_count(&event.pool_address),
            keyer::token_total_tx_count(&event.token0),
            keyer::token_total_tx_count(&event.token1),
            keyer::factory_total_tx_count(),
            keyer::uniswap_data_data_tx_count(day_id.to_string()),
            // here if the id is 1, we will need to send a create
        ];
        output.add_many(event.log_ordinal, &keys, &BigInt::from(1 as i32));
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
    output.delete_prefix(0, &format!("uniswap_day_data:{}:", day_id - 1));

    for event in events.pool_events {
        let pool: Pool = match store_pool.get_last(keyer::pool_key(&event.pool_address)) {
            None => continue,
            Some(pool) => pool,
        };
        match store_total_tx_counts.has_last(keyer::pool_total_tx_count(&event.pool_address)) {
            false => {}
            true => match event.r#type.unwrap() {
                SwapEvent(swap) => {
                    let eth_price_in_usd: BigDecimal = match store_eth_prices
                        .get_at(event.log_ordinal, &keyer::bundle_eth_price())
                    {
                        None => {
                            panic!("bundle eth price not found")
                        }
                        Some(price) => price,
                    };

                    let token0_derived_eth_price: BigDecimal = match store_eth_prices
                        .get_at(event.log_ordinal, keyer::token_eth_price(&event.token0))
                    {
                        None => continue,
                        Some(price) => price,
                    };

                    let token1_derived_eth_price: BigDecimal = match store_eth_prices
                        .get_at(event.log_ordinal, keyer::token_eth_price(&event.token1))
                    {
                        None => continue,
                        Some(price) => price,
                    };

                    let mut amount0_abs: BigDecimal = BigDecimal::from(swap.amount_0.unwrap());
                    if amount0_abs.lt(&BigDecimal::from(0 as u64)) {
                        amount0_abs = amount0_abs.mul(BigDecimal::from(-1 as i64))
                    }

                    let mut amount1_abs: BigDecimal = BigDecimal::from(swap.amount_1.unwrap());
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
                        &eth_price_in_usd,
                    )
                    .div(BigDecimal::from(2 as i32));

                    let amount_total_eth_tracked =
                        math::safe_div(&amount_total_usd_tracked, &eth_price_in_usd);

                    let amount0_eth = amount0_abs.clone().mul(token0_derived_eth_price);
                    let amount1_eth = amount1_abs.clone().mul(token1_derived_eth_price);
                    let amount0_usd = amount0_eth.mul(eth_price_in_usd.clone());
                    let amount1_usd = amount1_eth.mul(eth_price_in_usd);

                    let amount_total_usd_untracked: BigDecimal = amount0_usd
                        .clone()
                        .add(amount1_usd)
                        .div(BigDecimal::from(2 as i32));

                    let fee_tier: BigDecimal = BigDecimal::from(pool.fee_tier.unwrap());
                    let fee_usd: BigDecimal = amount_total_usd_tracked
                        .clone()
                        .mul(fee_tier.clone())
                        .div(BigDecimal::from(1000000 as u64));
                    let fee_eth: BigDecimal = amount_total_eth_tracked
                        .clone()
                        .mul(fee_tier)
                        .div(BigDecimal::from(1000000 as u64));

                    output.add_many(
                        event.log_ordinal,
                        &vec![
                            keyer::swap_volume_token_0(&event.pool_address),
                            keyer::swap_token_volume(&event.token0, "token0".to_string()),
                        ],
                        &amount0_abs,
                    );
                    output.add_many(
                        event.log_ordinal,
                        &vec![
                            keyer::swap_volume_token_1(&event.pool_address),
                            keyer::swap_token_volume(&event.token1, "token1".to_string()),
                        ],
                        &amount1_abs,
                    );
                    output.add_many(
                        event.log_ordinal,
                        &vec![
                            keyer::swap_volume_usd(&event.pool_address),
                            keyer::swap_token_volume_usd(&event.token0),
                            keyer::swap_token_volume_usd(&event.token1),
                            keyer::swap_factory_total_volume_usd(),
                            keyer::swap_uniswap_day_data_volume_usd(day_id.to_string()),
                        ],
                        &amount_total_usd_tracked,
                    );
                    output.add_many(
                        event.log_ordinal,
                        &vec![
                            keyer::swap_untracked_volume_usd(&event.pool_address),
                            keyer::swap_token_volume_untracked_volume_usd(&event.token0),
                            keyer::swap_token_volume_untracked_volume_usd(&event.token1),
                            keyer::swap_factory_untracked_volume_usd(),
                        ],
                        &amount_total_usd_untracked,
                    );
                    output.add(
                        event.log_ordinal,
                        keyer::swap_factory_total_volume_eth(),
                        &amount_total_eth_tracked.clone(),
                    );
                    output.add_many(
                        event.log_ordinal,
                        &vec![
                            keyer::swap_fee_usd(&event.pool_address),
                            keyer::swap_token_fee_usd(&event.token0),
                            keyer::swap_token_fee_usd(&event.token1),
                            keyer::swap_factory_total_fees_usd(),
                            keyer::swap_uniswap_day_data_fees_usd(day_id.to_string()),
                        ],
                        &fee_usd,
                    );
                    output.add(
                        event.log_ordinal,
                        keyer::swap_factory_total_fees_eth(),
                        &fee_eth,
                    );
                }
                _ => {}
            },
        }
    }
}

#[substreams::handlers::store]
pub fn store_pool_fee_growth_global_x128(events: Events, store: StoreSetBigInt) {
    for event in events.pool_events {
        let pool_address = event.pool_address;
        log::info!(
            "pool address: {} trx_id:{}",
            pool_address,
            event.transaction_id
        );
        let (big_int_1, big_int_2) = rpc::fee_growth_global_x128_call(&pool_address);
        log::info!("big decimal0: {}", big_int_1);
        log::info!("big decimal1: {}", big_int_2);

        store.set(
            event.log_ordinal,
            keyer::pool_fee_growth_global_x128(&pool_address, "token0".to_string()),
            &big_int_1,
        );
        store.set(
            event.log_ordinal,
            keyer::pool_fee_growth_global_x128(&pool_address, "token1".to_string()),
            &big_int_2,
        );
    }
}

#[substreams::handlers::store]
// Find a better solution to the early exit in the loop
pub fn store_native_total_value_locked(
    events: Events,
    store: StoreSetBigDecimal, // fixme: why is this an add ???
) {
    for pool_event in events.pool_events {
        let token_amounts_wrapped = pool_event.get_amounts();
        if let None = token_amounts_wrapped {
            continue;
        }
        let token_amounts = token_amounts_wrapped.unwrap();
        // check whether get_amount0() is None
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

#[substreams::handlers::store]
pub fn store_eth_prices(
    events: Events,                                      /* map_extract_data_types */
    pools_store: StoreGetProto<Pool>,                    /* store_pools */
    prices_store: StoreGetBigDecimal,                    /* store_prices */
    tokens_whitelist_pools_store: StoreGetRaw,           /* store_tokens_whitelist_pools */
    total_native_value_locked_store: StoreGetBigDecimal, /* store_native_total_value_locked */
    pool_liquidities_store: StoreGetBigInt,              /* store_pool_liquidities */
    store: StoreSetBigDecimal,
) {
    for pool_sqrt_price in events.pool_sqrt_prices {
        log::debug!(
            "handling pool price update - addr: {} price: {}",
            pool_sqrt_price.pool_address,
            pool_sqrt_price.sqrt_price.unwrap().value
        );
        let pool = pools_store.must_get_last(&keyer::pool_key(&pool_sqrt_price.pool_address));
        let token_0 = pool.token0.as_ref().unwrap();
        let token_1 = pool.token1.as_ref().unwrap();

        token_0.log();
        token_1.log();

        let bundle_eth_price_usd =
            price::get_eth_price_in_usd(&prices_store, pool_sqrt_price.ordinal);
        log::info!("bundle_eth_price_usd: {}", bundle_eth_price_usd);

        let token0_derived_eth_price: BigDecimal = price::find_eth_per_token(
            pool_sqrt_price.ordinal,
            &pool.address,
            &token_0.address,
            &pools_store,
            &pool_liquidities_store,
            &tokens_whitelist_pools_store,
            &total_native_value_locked_store,
            &prices_store,
        );
        log::info!(
            "token 0 {} derived eth price: {}",
            token_0.address,
            token0_derived_eth_price
        );

        let token1_derived_eth_price: BigDecimal = price::find_eth_per_token(
            pool_sqrt_price.ordinal,
            &pool.address,
            &token_1.address,
            &pools_store,
            &pool_liquidities_store,
            &tokens_whitelist_pools_store,
            &total_native_value_locked_store,
            &prices_store,
        );
        log::info!(
            "token 1 {} derived eth price: {}",
            token_1.address,
            token1_derived_eth_price
        );

        store.set(
            pool_sqrt_price.ordinal,
            keyer::bundle_eth_price(),
            &bundle_eth_price_usd,
        );

        store.set(
            pool_sqrt_price.ordinal,
            keyer::token_eth_price(&token_0.address),
            &token0_derived_eth_price,
        );

        store.set(
            pool_sqrt_price.ordinal,
            keyer::token_eth_price(&token_1.address),
            &token1_derived_eth_price,
        );
    }
}

#[substreams::handlers::store]
pub fn store_total_value_locked_by_tokens(events: Events, store: StoreAddBigDecimal) {
    for event in events.pool_events {
        log::debug!("trx_id: {}", event.transaction_id);
        let mut amount0: BigDecimal;
        let mut amount1: BigDecimal;

        match event.r#type.unwrap() {
            BurnEvent(burn) => {
                amount0 = burn.amount_0.unwrap().into();
                amount0 = amount0.neg();
                amount1 = burn.amount_1.unwrap().into();
                amount1 = amount1.neg();
            }
            MintEvent(mint) => {
                amount0 = mint.amount_0.unwrap().into();
                amount1 = mint.amount_1.unwrap().into();
            }
            SwapEvent(swap) => {
                amount0 = swap.amount_0.unwrap().into();
                amount1 = swap.amount_1.unwrap().into();
            }
        }

        store.add_many(
            event.log_ordinal,
            &vec![
                keyer::total_value_locked_by_pool(
                    &event.pool_address,
                    &event.token0,
                    "token0".to_string(),
                ),
                keyer::total_value_locked_by_token(&event.token0),
            ],
            &amount0,
        );

        store.add_many(
            event.log_ordinal,
            &vec![
                keyer::total_value_locked_by_pool(
                    &event.pool_address,
                    &event.token1,
                    "token1".to_string(),
                ),
                keyer::total_value_locked_by_token(&event.token1),
            ],
            &amount1,
        );
    }
}

#[substreams::handlers::store]
pub fn store_total_value_locked(
    native_total_value_locked_deltas: Deltas<DeltaBigDecimal>,
    total_value_locked_by_tokens_store: StoreGetBigDecimal,
    pools_store: StoreGetProto<Pool>,
    eth_prices_store: StoreGetBigDecimal,
    store: StoreAddBigDecimal,
) {
    let mut pool_aggregator: HashMap<String, (u64, BigDecimal)> = HashMap::from([]);

    // the deltas will contain the amount0 and amount1 needed
    // to compute the totalValueLockedToken0 and totalValueLockedToken0
    for delta in native_total_value_locked_deltas.deltas {
        log::info!("\n");
        log::info!("!!!DELTA KEY: {}", delta.key);
        log::info!("!!!DELTA oldValue: {}", delta.old_value);
        log::info!("!!!DELTA newValue: {}", delta.new_value);
        log::info!("\n");

        let eth_price_usd: BigDecimal =
            match &eth_prices_store.get_at(delta.ordinal, &keyer::bundle_eth_price()) {
                None => continue,
                Some(price) => price.with_prec(100),
            };
        log::debug!("eth_price_usd: {}", eth_price_usd);

        // ----- TOKEN ----- //
        if let Some(token_addr) = keyer::native_token_from_key(&delta.key) {
            let value = &delta.new_value;
            let token_derive_eth: BigDecimal = match eth_prices_store
                .get_at(delta.ordinal, &keyer::token_eth_price(&token_addr))
            {
                None => panic!("token eth price not found for token {}", token_addr),
                Some(price) => price,
            };
            log::info!("token_derive_eth {}", token_derive_eth);

            //todo: here I think that value is incorrect and not what we think it is
            // the value should be total_value_locked_usd = (totalValueLockedETH * ethPriceUSD)
            let total_value_locked_usd = value
                .clone()
                .mul(token_derive_eth)
                .mul(eth_price_usd.clone());

            log::info!(
                "token {} total value locked usd: {}",
                token_addr,
                total_value_locked_usd
            );
            //FIXME: we changed this substreams to become an add, BUT this causes an issue with this
            // store key because it

            // store.set(
            //     native_total_value_locked.ordinal,
            //     keyer::token_usd_total_value_locked(&token_addr),
            //     &total_value_locked_usd,
            // );

            // ----- POOL ----- //
        } else if let Some((pool_addr, token_addr)) = keyer::native_pool_from_key(&delta.key) {
            let pool = pools_store.must_get_last(keyer::pool_key(&pool_addr));
            log::info!("pool addr {}", pool.address);
            log::info!("token0 {}", pool.token0_ref().address);
            log::info!("token1 {}", pool.token1_ref().address);
            // fixme: why did we only check the token0??
            // if pool.token0.as_ref().unwrap().address != token_addr {
            //     continue;
            // }
            let value: BigDecimal = delta.new_value;
            let token0_derive_eth: BigDecimal = match eth_prices_store.get_at(
                delta.ordinal,
                &keyer::token_eth_price(&pool.token0_ref().address),
            ) {
                None => panic!(
                    "token eth price not found for token {}",
                    pool.token0_ref().address
                ),
                Some(price) => price,
            };

            log::info!("token0_derive_eth: {}", token0_derive_eth);

            let token1_derive_eth: BigDecimal = match eth_prices_store.get_at(
                delta.ordinal,
                &keyer::token_eth_price(&pool.token1_ref().address),
            ) {
                None => panic!(
                    "token eth price not found for token {}",
                    pool.token1_ref().address
                ),
                Some(price) => price,
            };

            log::info!("token1_derive_eth: {}", token1_derive_eth);

            // log::info!("token_derive_eth {}", token_derive_eth);
            // let partial_pool_total_value_locked_eth = value.mul(token_derive_eth);
            // log::info!(
            //     "partial pool {} token {} \n partial total value locked eth: {}",
            //     pool_addr,
            //     token_addr,
            //     partial_pool_total_value_locked_eth,
            // );
            let aggregate_key = pool_addr.clone();

            // todo: fetch totalValueLockedToken0 and total
            let total_value_locked_token0: BigDecimal = match total_value_locked_by_tokens_store
                .get_at(
                    delta.ordinal,
                    &keyer::total_value_locked_by_token(pool.token0_ref().address()),
                ) {
                None => {
                    panic!("impossible")
                }
                Some(val) => val,
            };

            let total_value_locked_token1: BigDecimal = match total_value_locked_by_tokens_store
                .get_at(
                    delta.ordinal,
                    &keyer::total_value_locked_by_token(pool.token1_ref().address()),
                ) {
                None => {
                    panic!("impossible")
                }
                Some(val) => val,
            };

            log::info!("total_value_locked_token0: {}", total_value_locked_token0);
            log::info!("total_value_locked_token1: {}", total_value_locked_token1);

            let try_total_value_locked_eth = total_value_locked_token0
                .clone()
                .mul(token0_derive_eth.clone())
                .add(
                    total_value_locked_token1
                        .clone()
                        .mul(token1_derive_eth.clone()),
                );

            log::info!("try_total_value_locked_eth {}", try_total_value_locked_eth);

            store.add(
                delta.ordinal,
                &keyer::factory_native_total_value_locked_eth(),
                try_total_value_locked_eth,
            );

            // fixme: check this: but I think that we can reduce this a lot
            // if let Some(pool_agg) = pool_aggregator.get(&aggregate_key) {
            //     let count = &pool_agg.0;
            //     let rolling_sum = &pool_agg.1;
            //     log::info!("found another partial pool value {} token {} count {} \n partial total value locked eth: {}",
            //         pool_addr,
            //         token_addr,
            //         count,
            //         rolling_sum,
            //     );
            //     if count >= &(2 as u64) {
            //         panic!(
            //             "{}",
            //             format!("this is unexpected should only see 2 pool keys")
            //         )
            //     }
            //
            //     log::info!(
            //         "partial_pool_total_value_locked_eth: {} \n and rolling_sum: {}",
            //         partial_pool_total_value_locked_eth,
            //         rolling_sum,
            //     );
            //     let pool_total_value_locked_eth =
            //         partial_pool_total_value_locked_eth.add(rolling_sum.clone());
            //     let pool_total_value_locked_usd = pool_total_value_locked_eth
            //         .clone()
            //         .mul(eth_price_usd.clone());
            //     log::info!(
            //         "pool_total_value_locked_eth {}",
            //         pool_total_value_locked_eth
            //     );
            //     log::info!(
            //         "pool_total_value_locked_usd {}",
            //         pool_total_value_locked_usd
            //     );
            //     store.set(
            //         native_total_value_locked.ordinal,
            //         keyer::pool_eth_total_value_locked(&pool_addr),
            //         &pool_total_value_locked_eth,
            //     );
            //     store.set(
            //         native_total_value_locked.ordinal,
            //         keyer::pool_usd_total_value_locked(&pool_addr),
            //         &pool_total_value_locked_usd,
            //     );
            //
            //     // todo: here we should remove the pool from the pool_aggregator no ?

            continue;
        }
        // pool_aggregator.insert(
        //     aggregate_key.clone(),
        //     (1, partial_pool_total_value_locked_eth.clone()),
        // );
        // // todo: should we not loop over the pools here after ?
        // log::info!(
        //     "partial inserted, partial_pool_total_value_locked_eth {}",
        //     partial_pool_total_value_locked_eth
        // );
    }
}

#[substreams::handlers::store]
pub fn store_total_value_locked_usd(
    native_total_value_locked_deltas: Deltas<DeltaBigDecimal>,
    total_value_locked_by_tokens_store: StoreGetBigDecimal,
    pools_store: StoreGetProto<Pool>,
    eth_prices_store: StoreGetBigDecimal,
    store: StoreSetBigDecimal,
) {
    for delta in native_total_value_locked_deltas.deltas {
        let eth_price_usd: BigDecimal =
            match &eth_prices_store.get_at(delta.ordinal, &keyer::bundle_eth_price()) {
                None => continue,
                Some(price) => price.with_prec(100),
            };
        log::debug!("eth_price_usd: {}", eth_price_usd);
        if let Some((pool_addr, token_addr)) = keyer::native_pool_from_key(&delta.key) {
            let pool = pools_store.must_get_last(keyer::pool_key(&pool_addr));

            let token0_derive_eth: BigDecimal = match eth_prices_store.get_at(
                delta.ordinal,
                &keyer::token_eth_price(&pool.token0_ref().address),
            ) {
                None => panic!(
                    "token eth price not found for token {}",
                    pool.token0_ref().address
                ),
                Some(price) => price,
            };

            log::info!("token0_derive_eth: {}", token0_derive_eth);

            let token1_derive_eth: BigDecimal = match eth_prices_store.get_at(
                delta.ordinal,
                &keyer::token_eth_price(&pool.token1_ref().address),
            ) {
                None => panic!(
                    "token eth price not found for token {}",
                    pool.token1_ref().address
                ),
                Some(price) => price,
            };

            log::info!("token1_derive_eth: {}", token1_derive_eth);

            let total_value_locked_token0: BigDecimal = match total_value_locked_by_tokens_store
                .get_at(
                    delta.ordinal,
                    &keyer::total_value_locked_by_token(pool.token0_ref().address()),
                ) {
                None => {
                    panic!("impossible")
                }
                Some(val) => val,
            };

            let total_value_locked_token1: BigDecimal = match total_value_locked_by_tokens_store
                .get_at(
                    delta.ordinal,
                    &keyer::total_value_locked_by_token(pool.token1_ref().address()),
                ) {
                None => {
                    panic!("impossible")
                }
                Some(val) => val,
            };

            // todo: this is how we calculate the total_value_locked_usd per token, need to set this
            // in another substreams
            let total_value_locked_usd_token0 =
                total_value_locked_token0.mul(token0_derive_eth.mul(eth_price_usd.clone()));
            log::info!(
                "total_value_locked_usd token0 {}",
                total_value_locked_usd_token0
            );

            let total_value_locked_usd_token1 =
                total_value_locked_token1.mul(token1_derive_eth.mul(eth_price_usd));
            log::info!(
                "total_value_locked_usd token1 {}",
                total_value_locked_usd_token1
            );

            store.set(
                delta.ordinal,
                keyer::token_usd_total_value_locked(pool.token0_ref().address()),
                &total_value_locked_usd_token0,
            );

            store.set(
                delta.ordinal,
                keyer::token_usd_total_value_locked(pool.token1_ref().address()),
                &total_value_locked_usd_token1,
            );
        }
    }
}

// TODO: PORT THIS CODE.
// #[substreams::handlers::map]
// pub fn map_ticks(events: Events) -> Result<Vec<events::TickCreated>, Error> {
//     let mut out: Vec<events::TickCreated> = vec![];
//     for event in events.pool_events {
//         log::info!("event: {:?}", event);
//         match event.r#type.unwrap() {
//             BurnEvent(burn) => {
//                 log::debug!("burn event transaction_id: {}", event.transaction_id);
//                 let lower_tick_id = format!(
//                     "{}#{}",
//                     &event.pool_address,
//                     burn.tick_lower.as_ref().unwrap().value
//                 );
//                 let lower_tick_idx: BigInt = burn.tick_lower.unwrap().into();
//                 let lower_tick_price0 = math::big_decimal_exponated(
//                     BigDecimal::try_from(1.0001).unwrap().with_prec(100),
//                     lower_tick_idx.clone(),
//                 );
//                 let lower_tick_price1 =
//                     math::safe_div(&BigDecimal::from(1 as i32), &lower_tick_price0);
//
//                 //todo: implement the fee_growth_outside_x128_call mimicked from the smart contract
//                 // to reduce the number of rpc calls to do
//                 let lower_tick_result =
//                     rpc::fee_growth_outside_x128_call(&event.pool_address, &lower_tick_idx);
//
//                 let tick_lower = events::TickCreated {
//                     pool_address: event.pool_address.to_string(),
//                     idx: Some(lower_tick_idx.into()),
//                     price0: Some(lower_tick_price0.into()),
//                     price1: Some(lower_tick_price1.into()),
//                     created_at_timestamp: event.timestamp,
//                     created_at_block_number: event.created_at_block_number,
//                     log_ordinal: event.log_ordinal,
//                     amount: burn.amount.clone(),
//                     r#type: TickType::Lower as i32,
//                     origin: TickOrigin::Burn as i32,
//                 };
//
//                 let upper_tick_id: String = format!(
//                     "{}#{}",
//                     &event.pool_address,
//                     burn.tick_upper.as_ref().unwrap().value
//                 );
//                 let upper_tick_idx: BigInt = burn.tick_upper.unwrap().into();
//                 let upper_tick_price0 = math::big_decimal_exponated(
//                     BigDecimal::try_from(1.0001).unwrap().with_prec(100),
//                     upper_tick_idx.clone(),
//                 );
//                 let upper_upper_price1 =
//                     math::safe_div(&BigDecimal::from(1 as i32), &upper_tick_price0);
//
//                 let upper_tick_result =
//                     rpc::fee_growth_outside_x128_call(&event.pool_address, &upper_tick_idx);
//
//                 let tick_upper = events::TickCreated {
//                     pool_address: event.pool_address.to_string(),
//                     idx: Some(upper_tick_idx.into()),
//                     price0: Some(upper_tick_price0.into()),
//                     price1: Some(upper_upper_price1.into()),
//                     created_at_timestamp: event.timestamp,
//                     created_at_block_number: event.created_at_block_number,
//                     log_ordinal: event.log_ordinal,
//                     amount: burn.amount,
//                     r#type: TickType::Upper as i32,
//                     origin: TickOrigin::Burn as i32,
//                 };
//
//                 out.push(tick_lower);
//                 out.push(tick_upper);
//             }
//             MintEvent(mint) => {
//                 log::debug!("mint event transaction_id: {}", event.transaction_id);
//                 let lower_tick_id: String = format!(
//                     "{}#{}",
//                     &event.pool_address,
//                     mint.tick_lower.as_ref().unwrap().value
//                 );
//                 let lower_tick_idx: BigInt = mint.tick_lower.unwrap().into();
//                 let lower_tick_price0 = math::big_decimal_exponated(
//                     BigDecimal::try_from(1.0001).unwrap().with_prec(100),
//                     lower_tick_idx.clone(),
//                 );
//                 let lower_tick_price1 =
//                     math::safe_div(&BigDecimal::from(1 as i32), &lower_tick_price0);
//
//                 let lower_tick_result =
//                     rpc::fee_growth_outside_x128_call(&event.pool_address, &lower_tick_idx);
//
//                 // in the subgraph, there is a `load` which is done to see if the tick
//                 // exists and if it doesn't exist, createTick()
//                 let tick_lower = events::TickCreated {
//                     pool_address: event.pool_address.to_string(),
//                     idx: Some(lower_tick_idx.into()),
//                     price0: Some(lower_tick_price0.into()),
//                     price1: Some(lower_tick_price1.into()),
//                     created_at_timestamp: event.timestamp,
//                     created_at_block_number: event.created_at_block_number,
//                     log_ordinal: event.log_ordinal,
//                     amount: mint.amount.clone(),
//                     r#type: TickType::Lower as i32,
//                     origin: TickOrigin::Mint as i32,
//                 };
//
//                 let upper_tick_id: String = format!(
//                     "{}#{}",
//                     &event.pool_address,
//                     mint.tick_upper.as_ref().unwrap().value
//                 );
//                 let upper_tick_idx: BigInt = mint.tick_upper.unwrap().into();
//                 let upper_tick_price0 = math::big_decimal_exponated(
//                     BigDecimal::try_from(1.0001).unwrap().with_prec(100),
//                     upper_tick_idx.clone(),
//                 );
//                 let upper_tick_price1 =
//                     math::safe_div(&BigDecimal::from(1 as i32), &upper_tick_price0);
//
//                 let upper_tick_result =
//                     rpc::fee_growth_outside_x128_call(&event.pool_address, &upper_tick_idx);
//
//                 let tick_upper = events::TickCreated {
//                     pool_address: event.pool_address.to_string(),
//                     idx: Some(upper_tick_idx.into()),
//                     price0: Some(upper_tick_price0.into()),
//                     price1: Some(upper_tick_price1.into()),
//                     created_at_timestamp: event.timestamp,
//                     created_at_block_number: event.created_at_block_number,
//                     log_ordinal: event.log_ordinal,
//                     amount: mint.amount,
//                     r#type: TickType::Upper as i32,
//                     origin: TickOrigin::Mint as i32,
//                 };
//
//                 out.push(tick_lower);
//                 out.push(tick_upper);
//             }
//             _ => {}
//         }
//     }
//     Ok(out)
// }

//
// #[substreams::handlers::store]
// pub fn store_ticks(ticks: Ticks /* input map_tick_entities */, output: StoreSetProto<Tick>) {
//     for tick in ticks.ticks {
//         output.set(tick.log_ordinal, keyer::ticks(&tick.id), &tick);
//     }
// }

#[substreams::handlers::store]
pub fn store_ticks_liquidities(events: Events, output: StoreAddBigInt) {
    for event in events.pool_events {
        let pool = event.pool_address;
        match event.r#type.unwrap() {
            Type::Mint(mint) => {
                output.add_many(
                    event.log_ordinal,
                    &vec![
                        keyer::tick_liquidities_gross(&pool, &mint.tick_lower.as_ref().unwrap()),
                        keyer::tick_liquidities_net(&pool, &mint.tick_lower.as_ref().unwrap()),
                        keyer::tick_liquidities_gross(&pool, &mint.tick_upper.as_ref().unwrap()),
                    ],
                    &BigInt::from(mint.amount.as_ref().unwrap()),
                );
                output.add(
                    event.log_ordinal,
                    keyer::tick_liquidities_net(&pool, &mint.tick_upper.as_ref().unwrap()),
                    &BigInt::from(mint.amount.as_ref().unwrap()).neg(),
                );
            }
            Type::Burn(burn) => {
                output.add_many(
                    event.log_ordinal,
                    &vec![
                        keyer::tick_liquidities_gross(&pool, &burn.tick_lower.as_ref().unwrap()),
                        keyer::tick_liquidities_net(&pool, &burn.tick_lower.as_ref().unwrap()),
                        keyer::tick_liquidities_gross(&pool, &burn.tick_upper.as_ref().unwrap()),
                    ],
                    &BigInt::from(burn.amount.as_ref().unwrap()).neg(),
                );
                output.add(
                    event.log_ordinal,
                    keyer::tick_liquidities_net(&pool, &burn.tick_upper.as_ref().unwrap()),
                    &BigInt::from(burn.amount.as_ref().unwrap()),
                );
            }
            _ => {}
        }
    }
}

//
// #[substreams::handlers::map]
// pub fn map_positions(
//     block: Block,
//     all_positions_store: StoreGetProto<events::Position>,
// ) -> Result<Vec<events::Position>, Error> {
//     let mut positions: Vec<events::Position> = vec![];
//     let mut ordered_positions: Vec<String> = vec![];
//     let mut enriched_positions: HashMap<String, events::Position> = HashMap::new();
//
//     for log in block.logs() {
//         if log.address() != NON_FUNGIBLE_POSITION_MANAGER {
//             continue;
//         }
//
//         if let Some(event) = abi::positionmanager::events::IncreaseLiquidity::match_and_decode(log)
//         {
//             let token_id: String = event.token_id.to_string();
//             if !enriched_positions.contains_key(&token_id) {
//                 match all_positions_store.get_at(
//                     log.ordinal(),
//                     keyer::all_position(&token_id, &IncreaseLiquidity.to_string()),
//                 ) {
//                     None => {
//                         log::debug!("increase liquidity for id {} doesn't exist", token_id);
//                         continue;
//                     }
//                     Some(pos) => {
//                         enriched_positions.insert(token_id.clone(), pos);
//                         if !ordered_positions.contains(&String::from(token_id.clone())) {
//                             ordered_positions.push(String::from(token_id))
//                         }
//                     }
//                 }
//             }
//         } else if let Some(event) = abi::positionmanager::events::Collect::match_and_decode(log) {
//             let token_id: String = event.token_id.to_string();
//             let mut position = if !enriched_positions.contains_key(&token_id) {
//                 match all_positions_store.get_at(
//                     log.ordinal(),
//                     keyer::all_position(&token_id, &Collect.to_string()),
//                 ) {
//                     None => {
//                         log::debug!("increase liquidity for id {} doesn't exist", token_id);
//                         continue;
//                     }
//                     Some(pos) => pos,
//                 }
//             } else {
//                 enriched_positions
//                     .remove(&event.token_id.to_string())
//                     .unwrap()
//             };
//
//             if let Some(position_call_result) =
//                 rpc::positions_call(&Hex(log.address()).to_string(), event.token_id)
//             {
//                 position.fee_growth_inside_0_last_x_128 = Some(position_call_result.5.into());
//                 position.fee_growth_inside_1_last_x_128 = Some(position_call_result.6.into());
//                 enriched_positions.insert(token_id.clone(), position);
//                 if !ordered_positions.contains(&String::from(token_id.clone())) {
//                     ordered_positions.push(String::from(token_id))
//                 }
//             }
//         } else if let Some(event) =
//             abi::positionmanager::events::DecreaseLiquidity::match_and_decode(log)
//         {
//             let token_id: String = event.token_id.to_string();
//             if !enriched_positions.contains_key(&token_id) {
//                 match all_positions_store.get_at(
//                     log.ordinal(),
//                     keyer::all_position(
//                         &event.token_id.to_string(),
//                         &DecreaseLiquidity.to_string(),
//                     ),
//                 ) {
//                     None => {
//                         log::debug!("increase liquidity for id {} doesn't exist", token_id);
//                         continue;
//                     }
//                     Some(pos) => {
//                         enriched_positions.insert(token_id.clone(), pos);
//                         if !ordered_positions.contains(&String::from(token_id.clone())) {
//                             ordered_positions.push(String::from(token_id))
//                         }
//                     }
//                 }
//             }
//         } else if let Some(event) = abi::positionmanager::events::Transfer::match_and_decode(log) {
//             let token_id: String = event.token_id.to_string();
//             let mut position = if !enriched_positions.contains_key(&token_id) {
//                 match all_positions_store.get_at(
//                     log.ordinal(),
//                     keyer::all_position(&token_id, &Transfer.to_string()),
//                 ) {
//                     None => {
//                         log::debug!("increase liquidity for id {} doesn't exist", token_id);
//                         continue;
//                     }
//                     Some(pos) => pos,
//                 }
//             } else {
//                 enriched_positions.remove(&token_id).unwrap()
//             };
//
//             position.owner = Hex(event.to.as_slice()).to_string();
//             enriched_positions.insert(token_id.clone(), position);
//             if !ordered_positions.contains(&String::from(token_id.clone())) {
//                 ordered_positions.push(String::from(token_id))
//             }
//         }
//     }
//
//     log::debug!("len of map: {}", enriched_positions.len());
//     for element in ordered_positions.iter() {
//         let pos = enriched_positions.remove(element);
//         if pos.is_some() {
//             positions.push(pos.unwrap());
//         }
//     }
//
//     Ok(positions)
// }

#[substreams::handlers::store]
pub fn store_positions(events: Events, store: StoreAddInt64) {
    for position in events.positions {
        match position.convert_position_type() {
            IncreaseLiquidity => {
                store.add(
                    position.log_ordinal,
                    keyer::position(&position.id, &IncreaseLiquidity.to_string()),
                    1,
                );
            }
            DecreaseLiquidity => {
                store.add(
                    position.log_ordinal,
                    keyer::position(&position.id, &DecreaseLiquidity.to_string()),
                    1,
                );
            }
            Collect => {
                store.add(
                    position.log_ordinal,
                    keyer::position(&position.id, &Collect.to_string()),
                    1,
                );
            }
            Transfer => {
                store.add(
                    position.log_ordinal,
                    keyer::position(&position.id, &Transfer.to_string()),
                    1,
                );
            }
            _ => {}
        }
    }
}

#[substreams::handlers::store]
pub fn store_position_changes(events: Events, store: StoreAddBigDecimal) {
    for position in events.positions {
        match position.convert_position_type() {
            IncreaseLiquidity => {
                store.add(
                    position.log_ordinal,
                    keyer::position_liquidity(&position.id),
                    &BigDecimal::from(position.liquidity.unwrap()),
                );
                store.add(
                    position.log_ordinal,
                    keyer::position_deposited_token(&position.id, "Token0"),
                    &BigDecimal::from(position.amount0.unwrap()),
                );
                store.add(
                    position.log_ordinal,
                    keyer::position_deposited_token(&position.id, "Token1"),
                    &BigDecimal::from(position.amount1.unwrap()),
                );
            }
            DecreaseLiquidity => {
                store.add(
                    position.log_ordinal,
                    keyer::position_liquidity(&position.id),
                    &BigDecimal::from(position.liquidity.unwrap()).neg(),
                );
                store.add(
                    position.log_ordinal,
                    keyer::position_withdrawn_token(&position.id, "Token0"),
                    &BigDecimal::from(position.amount0.unwrap()),
                );
                store.add(
                    position.log_ordinal,
                    keyer::position_withdrawn_token(&position.id, "Token1"),
                    &BigDecimal::from(position.amount1.unwrap()),
                );
            }
            Collect => {
                store.add(
                    position.log_ordinal,
                    keyer::position_collected_fees_token(&position.id, "Token0"),
                    &BigDecimal::from(position.amount0.unwrap()),
                );
                store.add(
                    position.log_ordinal,
                    keyer::position_collected_fees_token(&position.id, "Token1"),
                    &BigDecimal::from(position.amount1.unwrap()),
                );
            }
            _ => {}
        }
    }
}

#[substreams::handlers::map]
pub fn map_position_snapshots(
    events: Events,
    position_changes_store: StoreGetBigDecimal,
) -> Result<SnapshotPositions, Error> {
    let mut snapshot_positions: SnapshotPositions = SnapshotPositions {
        snapshot_positions: vec![],
    };

    for position in events.positions {
        let mut snapshot_position: SnapshotPosition = SnapshotPosition {
            id: format!("{}#{}", position.id, position.block_number),
            owner: position.owner,
            pool: position.pool,
            position: position.id.clone(),
            block_number: position.block_number,
            timestamp: position.timestamp,
            transaction: position.transaction,
            fee_growth_inside_0_last_x_128: position.fee_growth_inside_0_last_x_128,
            fee_growth_inside_1_last_x_128: position.fee_growth_inside_1_last_x_128,
            log_ordinal: position.log_ordinal,
            ..Default::default()
        };

        //TODO: when the value is not found, do we really want to set the liquidity, deposited_token0, etc.
        // to 0? We could simply not touch the data point...
        match position_changes_store.get_at(
            position.log_ordinal,
            keyer::position_liquidity(&position.id),
        ) {
            Some(liquidity) => snapshot_position.liquidity = Some(liquidity.into()),
            _ => snapshot_position.liquidity = Some(BigDecimal::zero().into()),
        }

        match position_changes_store.get_at(
            position.log_ordinal,
            keyer::position_deposited_token(&position.id, "Token0"),
        ) {
            Some(deposited_token0) => {
                snapshot_position.deposited_token0 = Some(deposited_token0.into());
            }
            _ => snapshot_position.deposited_token0 = Some(BigDecimal::zero().into()),
        }

        match position_changes_store.get_at(
            position.log_ordinal,
            keyer::position_deposited_token(&position.id, "Token1"),
        ) {
            Some(deposited_token1) => {
                snapshot_position.deposited_token1 = Some(deposited_token1.into());
            }
            _ => snapshot_position.deposited_token1 = Some(BigDecimal::zero().into()),
        }

        match position_changes_store.get_at(
            position.log_ordinal,
            keyer::position_withdrawn_token(&position.id, "Token0"),
        ) {
            Some(withdrawn_token0) => {
                snapshot_position.withdrawn_token0 = Some(withdrawn_token0.into());
            }
            _ => snapshot_position.withdrawn_token0 = Some(BigDecimal::zero().into()),
        }

        match position_changes_store.get_at(
            position.log_ordinal,
            keyer::position_withdrawn_token(&position.id, "Token1"),
        ) {
            Some(withdrawn_token1) => {
                snapshot_position.withdrawn_token1 = Some(withdrawn_token1.into());
            }
            _ => snapshot_position.withdrawn_token1 = Some(BigDecimal::zero().into()),
        }

        match position_changes_store.get_at(
            position.log_ordinal,
            keyer::position_collected_fees_token(&position.id, "Token0"),
        ) {
            Some(collected_fees_token0) => {
                snapshot_position.collected_fees_token0 = Some(collected_fees_token0.into());
            }
            _ => snapshot_position.collected_fees_token0 = Some(BigDecimal::zero().into()),
        }

        match position_changes_store.get_at(
            position.log_ordinal,
            keyer::position_collected_fees_token(&position.id, "Token1"),
        ) {
            Some(collected_fees_token1) => {
                snapshot_position.collected_fees_token1 = Some(collected_fees_token1.into());
            }
            _ => snapshot_position.collected_fees_token1 = Some(BigDecimal::zero().into()),
        }

        snapshot_positions
            .snapshot_positions
            .push(snapshot_position);
    }

    Ok(snapshot_positions)
}

#[substreams::handlers::map]
pub fn graph_out(
    clock: Clock,
    pool_count_deltas: Deltas<DeltaBigInt>, /* store_pool_count */
    tx_count_deltas: Deltas<DeltaBigInt>,   /* store_total_tx_counts deltas */
    swaps_volume_deltas: Deltas<DeltaBigDecimal>, /* store_swaps_volume */
    totals_deltas: Deltas<DeltaBigDecimal>, /* store_totals */
    derived_eth_prices_deltas: Deltas<DeltaBigDecimal>, /* store_eth_prices */
    events: Events,                         /* map_extract_data_types */
    pools_created: Pools,                   /* map_pools_created */
    pool_sqrt_price_deltas: Deltas<DeltaProto<PoolSqrtPrice>>, /* store_pool_sqrt_price */
    pool_liquidities_store_deltas: Deltas<DeltaBigInt>, /* store_pool_liquidities */
    total_value_locked_deltas: Deltas<DeltaBigDecimal>, /* store_total_value_locked */
    total_value_locked_by_tokens_deltas: Deltas<DeltaBigDecimal>, /* store_total_value_locked_by_tokens */
    pool_fee_growth_global_x128_deltas: Deltas<DeltaBigInt>, /* store_pool_fee_growth_global_x128 */
    price_deltas: Deltas<DeltaBigDecimal>,                   /* store_prices */
    tokens_store: StoreGetInt64,                             /* store_tokens */
    total_value_locked_usd_deltas: Deltas<DeltaBigDecimal>,  /* store_total_value_locked_usd */
    tokens_whitelist_pools: Deltas<DeltaArray<String>>,      /* store_tokens_whitelist_pools */
    ticks_liquidities_deltas: Deltas<DeltaBigInt>,           /* store_ticks_liquidities */
    positions_store: StoreGetInt64,                          /* store_positions */
    positions_changes_deltas: Deltas<DeltaBigDecimal>,       /* store_position_changes */
    snapshot_positions: SnapshotPositions,                   /* map_position_snapshots */
    tx_count_store: StoreGetBigInt,                          /* store_total_tx_counts */
    store_eth_prices: StoreGetBigDecimal,                    /* store_eth_prices */
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
    db::total_value_locked_factory_entity_change(&mut tables, &totals_deltas);

    // Pool:
    db::pools_created_pool_entity_change(&mut tables, &pools_created);
    db::pool_sqrt_price_entity_change(&mut tables, pool_sqrt_price_deltas);
    db::pool_liquidities_pool_entity_change(&mut tables, pool_liquidities_store_deltas);
    db::pool_fee_growth_global_entity_change(&mut tables, events.fee_growth_global_updates);
    db::total_value_locked_pool_entity_change(&mut tables, total_value_locked_deltas);
    db::total_value_locked_by_token_pool_entity_change(
        &mut tables,
        &total_value_locked_by_tokens_deltas,
    );
    db::pool_fee_growth_global_x128_entity_change(&mut tables, pool_fee_growth_global_x128_deltas);
    db::price_pool_entity_change(&mut tables, price_deltas);
    db::tx_count_pool_entity_change(&mut tables, &tx_count_deltas);
    db::swap_volume_pool_entity_change(&mut tables, &swaps_volume_deltas);

    // Tokens:
    db::tokens_created_token_entity_change(&mut tables, &pools_created, tokens_store);
    db::swap_volume_token_entity_change(&mut tables, &swaps_volume_deltas);
    db::tx_count_token_entity_change(&mut tables, &tx_count_deltas);
    db::total_value_locked_by_token_token_entity_change(
        &mut tables,
        &total_value_locked_by_tokens_deltas,
    );
    db::total_value_locked_usd_token_entity_change(&mut tables, total_value_locked_usd_deltas);
    db::derived_eth_prices_token_entity_change(&mut tables, &derived_eth_prices_deltas);
    db::whitelist_token_entity_change(&mut tables, tokens_whitelist_pools);

    // Tick:
    //create_ticks_entity_change(&mut tables, events);
    db::create_tick_entity_change(&mut tables, events.ticks_created);
    db::update_tick_entity_change(&mut tables, events.ticks_updated);
    // create_update_ticks_entity_change(&mut tables, );
    // update_ticks_entity_change(&mut table, );

    db::ticks_liquidities_tick_entity_change(&mut tables, ticks_liquidities_deltas);

    // Position:
    db::position_create_entity_change(&mut tables, events.positions, positions_store);
    db::positions_changes_entity_change(&mut tables, positions_changes_deltas);

    // PositionSnapshot:
    db::snapshot_position_entity_change(&mut tables, snapshot_positions);

    // Transaction:
    db::transaction_entity_change(&mut tables, events.transactions);

    // Swap, Mint, Burn:
    db::swaps_mints_burns_created_entity_change(
        &mut tables,
        events.pool_events,
        tx_count_store,
        store_eth_prices,
    );

    // Flashes:
    db::flashes_update_pool_fee_entity_change(&mut tables, events.flashes);

    // Uniswap day data:
    db::uniswap_day_data_create_entity_change(&mut tables, &tx_count_deltas);
    db::uniswap_day_data_tx_count_entity_change(&mut tables, &tx_count_deltas);
    db::uniswap_day_data_totals_entity_change(&mut tables, &totals_deltas);
    db::uniswap_day_data_volumes_entity_change(&mut tables, &swaps_volume_deltas);

    Ok(tables.to_entity_changes())
}

#[substreams::handlers::map]
pub fn map_uniswap_day_data_entities(
    tx_count_deltas: Deltas<DeltaBigInt>, /* store_total_tx_counts deltas */
    swaps_volume_deltas: Deltas<DeltaBigDecimal>, /* store_swaps_volume */
    totals_deltas: Deltas<DeltaBigDecimal>, /* store_totals */
) -> Result<EntityChanges, Error> {
    let mut tables = Tables::new();

    // todo: need to create the first uniswap day data for given day
    db::uniswap_day_data_create_entity_change(&mut tables, &tx_count_deltas);
    db::uniswap_day_data_tx_count_entity_change(&mut tables, &tx_count_deltas);
    db::uniswap_day_data_totals_entity_change(&mut tables, &totals_deltas);
    db::uniswap_day_data_volumes_entity_change(&mut tables, &swaps_volume_deltas);

    Ok(tables.to_entity_changes())
}
