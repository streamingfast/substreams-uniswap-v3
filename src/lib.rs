extern crate core;

pub mod abi;
mod ast;
mod db;
mod eth;
mod filtering;
mod math;
mod pb;
mod price;
mod rpc;
mod storage;
mod ticks_idx;
mod utils;

use crate::ethpb::v2::{Block, StorageChange};
use crate::pb::uniswap;
use crate::pb::uniswap::events::pool_event::Type;
use crate::pb::uniswap::events::pool_event::Type::{Burn as BurnEvent, Mint as MintEvent, Swap as SwapEvent};
use crate::pb::uniswap::events::position_event::Type::{
    CollectPosition, CreatedPosition, DecreaseLiquidityPosition, IncreaseLiquidityPosition, TransferPosition,
};
use crate::pb::uniswap::events::{PoolSqrtPrice, PositionEvent};
use crate::pb::uniswap::{events, Events};
use crate::pb::uniswap::{Erc20Token, Erc20Tokens, Pool, Pools};
use crate::price::WHITELIST_TOKENS;
use crate::utils::{ERROR_POOL, UNISWAP_V3_FACTORY};
use std::ops::{Div, Mul, Sub};
use substreams::errors::Error;
use substreams::key;
use substreams::pb::substreams::{store_delta, Clock};
use substreams::prelude::*;
use substreams::scalar::{BigDecimal, BigInt};
use substreams::store::{
    DeltaArray, DeltaBigDecimal, DeltaBigInt, DeltaExt, DeltaProto, StoreAddBigDecimal, StoreAddBigInt, StoreAppend,
    StoreGetBigDecimal, StoreGetBigInt, StoreGetProto, StoreGetRaw, StoreSetBigDecimal, StoreSetBigInt, StoreSetProto,
};
use substreams::{log, Hex};
use substreams_entity_change::pb::entity::EntityChanges;
use substreams_entity_change::tables::Tables;
use substreams_ethereum::{pb::eth as ethpb, Event as EventTrait};

#[substreams::handlers::map]
pub fn map_pools_created(block: Block) -> Result<Pools, Error> {
    use abi::factory::events::PoolCreated;

    Ok(Pools {
        pools: block
            .events::<PoolCreated>(&[&UNISWAP_V3_FACTORY])
            .filter_map(|(event, log)| {
                log::info!("pool addr: {}", Hex(&event.pool));

                if event.pool == ERROR_POOL {
                    return None;
                }

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
                    ignore_pool: event.pool == ERROR_POOL,
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
pub fn store_pools_created(pools: Pools, store: StoreSetProto<Pool>) {
    for pool in pools.pools {
        let pool_address = &pool.address;
        store.set(pool.log_ordinal, format!("pool:{pool_address}"), &pool);
    }
}

#[substreams::handlers::store]
pub fn store_tokens(pools: Pools, store: StoreAddInt64) {
    for pool in pools.pools {
        let token0_addr = pool.token0_ref().address();
        let token1_addr = pool.token1_ref().address();

        store.add_many(
            pool.log_ordinal,
            &vec![format!("token:{token0_addr}"), format!("token:{token1_addr}")],
            1,
        );
    }
}

#[substreams::handlers::store]
pub fn store_pool_count(pools: Pools, store: StoreAddBigInt) {
    for pool in pools.pools {
        store.add(pool.log_ordinal, format!("factory:poolCount"), &BigInt::one())
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
        output_append.append_all(1, format!("token:{}", token.address), token.whitelist_pools);
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
    // let mut flashes: Vec<events::Flash> = vec![];
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
            let transactions_id = Hex(&trx.hash).to_string();

            let pool_opt = pools_store.get_last(format!("pool:{pool_address}"));
            if pool_opt.is_none() {
                continue;
            }
            let pool = pool_opt.unwrap();
            filtering::extract_pool_sqrt_prices(&mut pool_sqrt_prices, log, pool_address);
            filtering::extract_pool_liquidities(&mut pool_liquidities, log, &call_view.call.storage_changes, &pool);
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

            // filtering::extract_flashes(&mut flashes, &log);
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
    // events.flashes = flashes;
    events.ticks_created = ticks_created;
    events.ticks_updated = ticks_updated;

    Ok(events)
}

#[substreams::handlers::store]
pub fn store_pool_sqrt_price(events: Events, store: StoreSetProto<PoolSqrtPrice>) {
    for sqrt_price in events.pool_sqrt_prices {
        let pool_address = &sqrt_price.pool_address;
        store.set(sqrt_price.ordinal, format!("pool:{pool_address}"), &sqrt_price)
    }
}

#[substreams::handlers::store]
pub fn store_prices(clock: Clock, events: Events, pools_store: StoreGetProto<Pool>, store: StoreSetBigDecimal) {
    let timestamp_seconds = clock.timestamp.unwrap().seconds;
    let day_id: i64 = timestamp_seconds / 86400;
    let hour_id: i64 = timestamp_seconds / 3600;
    let prev_day_id = day_id - 1;
    let prev_hour_id = hour_id - 1;

    store.delete_prefix(0, &format!("PoolDayData:{prev_day_id}:"));
    store.delete_prefix(0, &format!("PoolHourData:{prev_hour_id}:"));

    for sqrt_price_update in events.pool_sqrt_prices {
        let pool_address = &sqrt_price_update.pool_address;
        match pools_store.get_last(format!("pool:{pool_address}")) {
            None => {
                log::info!("skipping pool {}", &pool_address);
                continue;
            }
            Some(pool) => {
                // This sqrt price has this value when there is no liquidity in the pool
                if sqrt_price_update.sqrt_price == "1461446703485210103287273052203988822378723970341" {
                    continue;
                }
                // maybe check for this sqrt price also : 4295128739 -> on the other side

                let token0 = pool.token0.as_ref().unwrap();
                let token1 = pool.token1.as_ref().unwrap();
                log::debug!(
                    "pool addr: {}, token 0 addr: {}, token 1 addr: {}",
                    pool.address,
                    token0.address,
                    token1.address
                );

                let sqrt_price = BigDecimal::try_from(sqrt_price_update.sqrt_price).unwrap();
                log::debug!("sqrtPrice: {}", sqrt_price.to_string());

                let tokens_price: (BigDecimal, BigDecimal) =
                    price::sqrt_price_x96_to_token_prices(sqrt_price, &token0, &token1);
                log::debug!("token prices: {} {}", tokens_price.0, tokens_price.1);

                let token0_addr = &token0.address;
                let token1_addr = &token1.address;
                store.set_many(
                    sqrt_price_update.ordinal,
                    &vec![
                        format!("pool:{pool_address}:{token0_addr}:token0"),
                        format!("pair:{token0_addr}:{token1_addr}"), // used for find_eth_per_token
                    ],
                    &tokens_price.0,
                );

                store.set_many(
                    sqrt_price_update.ordinal,
                    &vec![
                        format!("pool:{pool_address}:{token1_addr}:token1"),
                        format!("pair:{token1_addr}:{token0_addr}"), // used for find_eth_per_token
                    ],
                    &tokens_price.1,
                );

                // We only want to set the prices of PoolDayData and PoolHourData when
                // the pool is post-initialized, not on the initialized event.
                if sqrt_price_update.initialized {
                    continue;
                }

                store.set_many(
                    sqrt_price_update.ordinal,
                    &vec![
                        // We only need the token0Prices to compute the open, high, low and close
                        format!("PoolDayData:{day_id}:{pool_address}:token0"),
                        format!("PoolHourData:{hour_id}:{pool_address}:token0"),
                    ],
                    &tokens_price.0,
                );

                store.set_many(
                    sqrt_price_update.ordinal,
                    &vec![
                        format!("PoolDayData:{day_id}:{pool_address}:token1"),
                        format!("PoolHourData:{hour_id}:{pool_address}:token1"),
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
    let prev_day_id = day_id - 1;
    let prev_hour_id = hour_id - 1;

    store.delete_prefix(0, &format!("PoolDayData:{prev_day_id}:"));
    store.delete_prefix(0, &format!("PoolHourData:{prev_hour_id}:"));

    for pool_liquidity in events.pool_liquidities {
        let pool_address = &pool_liquidity.pool_address;
        let token0_address = &pool_liquidity.token0;
        let token1_address = &pool_liquidity.token1;
        store.set_many(
            pool_liquidity.log_ordinal,
            &vec![
                format!("pool:{pool_address}"),
                format!("pair:{token0_address}:{token1_address}"),
                format!("pair:{token1_address}:{token0_address}"),
                format!("PoolDayData:{day_id}:{pool_address}"),
                format!("PoolHourData:{hour_id}:{pool_address}"),
            ],
            &BigInt::try_from(pool_liquidity.liquidity).unwrap(),
        )
    }
}

#[substreams::handlers::store]
pub fn store_total_tx_counts(clock: Clock, events: Events, output: StoreAddBigInt) {
    let timestamp_seconds = clock.timestamp.unwrap().seconds;
    let day_id = timestamp_seconds / 86400;
    let hour_id = timestamp_seconds / 3600;
    let prev_day_id = day_id - 1;
    let prev_hour_id = hour_id - 1;
    let factory_addr = Hex(UNISWAP_V3_FACTORY);

    output.delete_prefix(0, &format!("UniswapDayData:{prev_day_id}:"));
    output.delete_prefix(0, &format!("PoolDayData:{prev_day_id}:"));
    output.delete_prefix(0, &format!("PoolHourData:{prev_hour_id}:"));
    output.delete_prefix(0, &format!("TokenDayData:{prev_day_id}:"));
    output.delete_prefix(0, &format!("TokenHourData:{prev_hour_id}:"));

    for event in events.pool_events {
        let pool_address = &event.pool_address;
        let token0_addr = &event.token0;
        let token1_addr = &event.token1;

        output.add_many(
            event.log_ordinal,
            &vec![
                format!("pool:{pool_address}"),
                format!("token:{token0_addr}"),
                format!("token:{token1_addr}"),
                format!("factory:{factory_addr}"),
                format!("UniswapDayData:{day_id}"),
                format!("PoolDayData:{day_id}:{pool_address}"),
                format!("PoolHourData:{hour_id}:{pool_address}"),
                format!("TokenDayData:{day_id}:{token0_addr}"),
                format!("TokenDayData:{day_id}:{token1_addr}"),
                format!("TokenHourData:{hour_id}:{token0_addr}"),
                format!("TokenHourData:{hour_id}:{token1_addr}"),
            ],
            &BigInt::from(1 as i32),
        );
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
    let day_id = timestamp_seconds / 86400;
    let hour_id = timestamp_seconds / 3600;
    let prev_day_id = day_id - 1;
    let prev_hour_id = hour_id - 1;

    output.delete_prefix(0, &format!("UniswapDayData:{prev_day_id}:"));
    output.delete_prefix(0, &format!("PoolDayData:{prev_day_id}:"));
    output.delete_prefix(0, &format!("PoolHourData:{prev_hour_id}:"));
    output.delete_prefix(0, &format!("TokenDayData:{prev_day_id}:"));
    output.delete_prefix(0, &format!("TokenHourData:{prev_hour_id}:"));

    for event in events.pool_events {
        let ord = event.log_ordinal;
        let pool_address = &event.pool_address;
        let pool = store_pool.must_get_last(format!("pool:{pool_address}"));
        if !store_total_tx_counts.has_last(format!("pool:{pool_address}")) {
            continue;
        }

        let token0_addr = &event.token0;
        let token1_addr = &event.token1;
        match event.r#type.unwrap() {
            MintEvent(_) => output.add(
                ord,
                format!("pool:{pool_address}:liquidityProviderCount"),
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

                let token0_derived_eth_price =
                    match store_eth_prices.get_at(ord, format!("token:{token0_addr}:dprice:eth")) {
                        None => continue,
                        Some(price) => price,
                    };

                let token1_derived_eth_price =
                    match store_eth_prices.get_at(ord, format!("token:{token1_addr}:dprice:eth")) {
                        None => continue,
                        Some(price) => price,
                    };

                log::info!("token0_derived_eth_price {}", token0_derived_eth_price);
                log::info!("token1_derived_eth_price {}", token1_derived_eth_price);

                let amount0_abs = BigDecimal::try_from(swap.amount_0).unwrap().absolute();
                let amount1_abs = BigDecimal::try_from(swap.amount_1).unwrap().absolute();

                log::info!("amount0_abs {}", amount0_abs);
                log::info!("amount1_abs {}", amount1_abs);

                let volume_amounts = utils::get_adjusted_amounts(
                    token0_addr,
                    token1_addr,
                    &amount0_abs,
                    &amount1_abs,
                    &token0_derived_eth_price,
                    &token1_derived_eth_price,
                    &eth_price_in_usd,
                );

                log::info!("volumeAmounts.eth {}", volume_amounts.delta_tvl_eth);
                log::info!("volumeAmounts.usd {}", volume_amounts.delta_tvl_usd);
                log::info!("volumeAmounts.untrackedETH {}", volume_amounts.stable_eth_untracked);
                log::info!("volumeAmounts.untrackedUSD {}", volume_amounts.stable_usd_untracked);

                let volume_eth = volume_amounts.delta_tvl_eth.clone().div(BigDecimal::from(2 as i32));
                let volume_usd = volume_amounts.delta_tvl_usd.clone().div(BigDecimal::from(2 as i32));
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

                log::info!("volume_eth {}", volume_eth);
                log::info!("volume_usd {}", volume_usd);
                log::info!("volume_usd_untracked {}", volume_usd_untracked);
                log::info!("fee_eth {}", fee_eth);
                log::info!("fee_usd {}", fee_usd);
                log::info!("fee_tier {}", fee_tier);

                output.add_many(
                    ord,
                    &vec![
                        format!("pool:{pool_address}:volumeToken0"),
                        // FIXME: why compute volumes only for one side of the tokens?!  We should compute them for both sides no?
                        //  Does it really matter which side the volume comes from?
                        format!("token:{token0_addr}:volume"),
                        format!("PoolDayData:{day_id}:{pool_address}:{token0_addr}:volumeToken0"),
                        format!("TokenDayData:{day_id}:{token0_addr}:volume"),
                        format!("PoolHourData:{hour_id}:{pool_address}:{token0_addr}:volumeToken0"),
                        format!("TokenHourData:{hour_id}:{token0_addr}:volume"),
                    ],
                    &amount0_abs,
                );
                output.add_many(
                    ord,
                    &vec![
                        format!("pool:{pool_address}:volumeToken1"),
                        format!("token:{token1_addr}:volume"),
                        format!("PoolDayData:{day_id}:{pool_address}:{token1_addr}:volumeToken1"),
                        format!("TokenDayData:{day_id}:{token1_addr}:volume"),
                        format!("PoolHourData:{hour_id}:{pool_address}:{token1_addr}:volumeToken1"),
                        format!("TokenHourData:{hour_id}:{token1_addr}:volume"),
                    ],
                    &amount1_abs,
                );
                output.add_many(
                    ord,
                    &vec![
                        format!("pool:{pool_address}:volumeUSD"),
                        format!("token:{token0_addr}:volume:usd"), // TODO: does this make sens that the volume usd is the same
                        format!("token:{token1_addr}:volume:usd"), // TODO: does this make sens that the volume usd is the same
                        format!("factory:totalVolumeUSD"),
                        format!("UniswapDayData:{day_id}:volumeUSD"),
                        format!("PoolDayData:{day_id}:{pool_address}:volumeUSD"),
                        format!("TokenDayData:{day_id}:{token0_addr}:volumeUSD"),
                        format!("TokenDayData:{day_id}:{token1_addr}:volumeUSD"),
                        format!("PoolHourData:{hour_id}:{pool_address}:volumeUSD"),
                        format!("TokenHourData:{hour_id}:{token0_addr}:volumeUSD"),
                        format!("TokenHourData:{hour_id}:{token1_addr}:volumeUSD"),
                    ],
                    //TODO: CONFIRM EQUALS -> IN THE SUBGRAPH THIS IS THE VOLUME USD
                    &volume_usd,
                );
                output.add_many(
                    ord,
                    &vec![
                        format!("factory:untrackedVolumeUSD"),
                        format!("pool:{pool_address}:volumeUntrackedUSD"),
                        format!("token:{token0_addr}:volume:untrackedUSD"),
                        format!("token:{token1_addr}:volume:untrackedUSD"),
                        format!("TokenDayData:{day_id}:{token0_addr}:volume:untrackedUSD"),
                        format!("TokenDayData:{day_id}:{token1_addr}:volume:untrackedUSD"),
                        format!("TokenHourData:{hour_id}:{token0_addr}:volume:untrackedUSD"),
                        format!("TokenHourData:{hour_id}:{token1_addr}:volume:untrackedUSD"),
                    ],
                    &volume_usd_untracked,
                );
                output.add_many(
                    ord,
                    &vec![
                        format!("factory:totalVolumeETH"),
                        format!("UniswapDayData:{day_id}:volumeETH"),
                    ],
                    &volume_eth.clone(),
                );
                output.add_many(
                    ord,
                    &vec![
                        format!("pool:{pool_address}:feesUSD"),
                        format!("token:{token0_addr}:feesUSD"),
                        format!("token:{token1_addr}:feesUSD"),
                        format!("factory:totalFeesUSD"),
                        format!("UniswapDayData:{day_id}:feesUSD"),
                        format!("PoolDayData:{day_id}:{pool_address}:feesUSD"),
                        format!("TokenDayData:{day_id}:{token0_addr}:feesUSD"),
                        format!("TokenDayData:{day_id}:{token1_addr}:feesUSD"),
                        format!("PoolHourData:{hour_id}:{pool_address}:feesUSD"),
                        format!("TokenHourData:{hour_id}:{token0_addr}:feesUSD"),
                        format!("TokenHourData:{hour_id}:{token1_addr}:feesUSD"),
                    ],
                    &fee_usd,
                );
                output.add(ord, format!("factory:totalFeesETH"), &fee_eth);
            }
            _ => {}
        }
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
            let amount0 = token_amounts.amount0;
            let amount1 = token_amounts.amount1;
            log::info!("amount 0: {} amount 1: {}", amount0, amount1);

            let pool_address = &pool_event.pool_address;
            let token0_addr = token_amounts.token0_addr;
            let token1_addr = token_amounts.token1_addr;

            store.set_many(
                pool_event.log_ordinal,
                &vec![
                    format!("token:{token0_addr}:native"),
                    format!("pool:{pool_address}:{token0_addr}:native"),
                ],
                &amount0,
            );
            store.set_many(
                pool_event.log_ordinal,
                &vec![
                    format!("token:{token1_addr}:native"),
                    format!("pool:{pool_address}:{token1_addr}:native"),
                ],
                &amount1,
            );
        }
    }
}

#[substreams::handlers::store]
pub fn store_eth_prices(
    clock: Clock,
    events: Events,                                /* map_extract_data_types */
    pools_store: StoreGetProto<Pool>,              /* store_pools_created */
    prices_store: StoreGetBigDecimal,              /* store_prices */
    tokens_whitelist_pools_store: StoreGetRaw,     /* store_tokens_whitelist_pools */
    total_native_amount_store: StoreGetBigDecimal, /* store_native_amounts */
    pool_liquidities_store: StoreGetBigInt,        /* store_pool_liquidities */
    output: StoreSetBigDecimal,
) {
    let timestamp_seconds = clock.timestamp.unwrap().seconds;
    let day_id = timestamp_seconds / 86400;
    let hour_id = timestamp_seconds / 3600;
    let prev_day_id = day_id - 1;
    let prev_hour_id = hour_id - 1;

    output.delete_prefix(0, &format!("TokenDayData:{prev_day_id}:"));
    output.delete_prefix(0, &format!("TokenHourData:{prev_hour_id}:"));

    for pool_sqrt_price in events.pool_sqrt_prices {
        let ord = pool_sqrt_price.ordinal;
        log::debug!(
            "handling pool price update - addr: {} price: {}",
            pool_sqrt_price.pool_address,
            pool_sqrt_price.sqrt_price
        );
        let pool_address = &pool_sqrt_price.pool_address;
        let pool = pools_store.must_get_last(format!("pool:{pool_address}"));
        let token0 = pool.token0.as_ref().unwrap();
        let token1 = pool.token1.as_ref().unwrap();
        let token0_addr = &token0.address;
        let token1_addr = &token1.address;

        token0.log();
        token1.log();

        let bundle_eth_price_usd = price::get_eth_price_in_usd(&prices_store, ord);
        log::info!("bundle_eth_price_usd: {}", bundle_eth_price_usd);

        let token0_derived_eth_price: BigDecimal = price::find_eth_per_token(
            ord,
            &pool.address,
            token0_addr,
            &pools_store,
            &pool_liquidities_store,
            &tokens_whitelist_pools_store,
            &total_native_amount_store,
            &prices_store,
        );
        log::info!(format!(
            "token 0 {token0_addr} derived eth price: {token0_derived_eth_price}"
        ));

        let token1_derived_eth_price: BigDecimal = price::find_eth_per_token(
            ord,
            &pool.address,
            token1_addr,
            &pools_store,
            &pool_liquidities_store,
            &tokens_whitelist_pools_store,
            &total_native_amount_store,
            &prices_store,
        );
        log::info!(format!(
            "token 1 {token1_addr} derived eth price: {token1_derived_eth_price}"
        ));

        output.set(ord, "bundle", &bundle_eth_price_usd);
        output.set(
            ord,
            format!("token:{token0_addr}:dprice:eth"),
            &token0_derived_eth_price,
        );
        output.set(
            ord,
            format!("token:{token1_addr}:dprice:eth"),
            &token1_derived_eth_price,
        );

        let token0_price_usd = token0_derived_eth_price.clone().mul(bundle_eth_price_usd.clone());
        let token1_price_usd = token1_derived_eth_price.clone().mul(bundle_eth_price_usd);

        log::info!("token0 price usd: {}", token0_price_usd);
        log::info!("token1 price usd: {}", token1_price_usd);

        // We only want to set the prices of TokenDayData and TokenHourData when
        // the pool is post-initialized, not on the initialized event.
        if pool_sqrt_price.initialized {
            continue;
        }

        output.set_many(
            ord,
            &vec![
                format!("TokenDayData:{day_id}:{token0_addr}"),
                format!("TokenHourData:{hour_id}:{token0_addr}"),
            ],
            &token0_price_usd,
        );
        output.set_many(
            ord,
            &vec![
                format!("TokenDayData:{day_id}:{token1_addr}"),
                format!("TokenHourData:{hour_id}:{token1_addr}"),
            ],
            &token1_price_usd,
        );
    }
}

#[substreams::handlers::store]
pub fn store_token_tvl(events: Events, output: StoreAddBigDecimal) {
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
            ],
            &token_amounts.amount0,
        );

        output.add_many(
            ord,
            &vec![
                &format!("pool:{pool_address}:{token1_addr}:token1"),
                &format!("token:{token1_addr}"),
            ],
            &token_amounts.amount1,
        );
    }
}

#[substreams::handlers::store]
pub fn store_derived_tvl(
    clock: Clock,
    events: Events,
    token_total_value_locked: StoreGetBigDecimal, /* store_token_tvl  */
    pools_store: StoreGetProto<Pool>,
    eth_prices_store: StoreGetBigDecimal,
    output: StoreSetBigDecimal,
) {
    let timestamp_seconds = clock.timestamp.unwrap().seconds;
    let day_id: i64 = timestamp_seconds / 86400;
    let hour_id: i64 = timestamp_seconds / 3600;
    let prev_day_id = day_id - 1;
    let prev_hour_id = hour_id - 1;

    output.delete_prefix(0, &format!("PoolDayData:{prev_day_id}:"));
    output.delete_prefix(0, &format!("PoolHourData:{prev_hour_id}:"));
    output.delete_prefix(0, &format!("TokenDayData:{prev_day_id}:"));
    output.delete_prefix(0, &format!("TokenHourData:{prev_hour_id}:"));

    for pool_event in events.pool_events {
        let ord = pool_event.log_ordinal;
        let eth_price_usd = match &eth_prices_store.get_at(ord, "bundle") {
            None => continue,
            Some(price) => price.with_prec(100),
        };
        log::info!("eth_price_usd {}", eth_price_usd);

        let pool = pools_store.must_get_last(format!("pool:{}", pool_event.pool_address));
        let pool_address = &pool_event.pool_address;
        let token0_addr = &pool.token0.as_ref().unwrap().address();
        let token1_addr = &pool.token1.as_ref().unwrap().address();

        log::info!("pool address {}", pool_address);
        log::info!("token0 address {}", token0_addr);
        log::info!("token1 address {}", token1_addr);

        let token0_derive_eth = utils::get_derived_eth_price(ord, token0_addr, &eth_prices_store);
        let token1_derive_eth = utils::get_derived_eth_price(ord, token1_addr, &eth_prices_store);

        let tvl_token0_in_pool =
            utils::get_token_tvl_in_pool(ord, pool_address, token0_addr, "token0", &token_total_value_locked);
        let tvl_token1_in_pool =
            utils::get_token_tvl_in_pool(ord, pool_address, token1_addr, "token1", &token_total_value_locked);

        let tvl_for_token0 = utils::get_token_tvl(ord, token0_addr, &token_total_value_locked);
        let tvl_for_token1 = utils::get_token_tvl(ord, token1_addr, &token_total_value_locked);

        log::info!("total_value_locked_token0 in pool: {}", tvl_token0_in_pool);
        log::info!("total_value_locked_token1 in pool: {}", tvl_token1_in_pool);
        log::info!("total_value_locked_token0 for token: {}", tvl_for_token0);
        log::info!("total_value_locked_token1 for token: {}", tvl_for_token1);

        // // not sure about this part
        // let derived_token0_eth = tvl_token0_in_pool.clone().mul(token0_derive_eth.clone());
        // let derived_token1_eth = tvl_token1_in_pool.clone().mul(token1_derive_eth.clone());
        // log::info!("derived_token0_eth: {}", derived_token0_eth);
        // log::info!("derived_token1_eth: {}", derived_token1_eth);

        let amounts_in_pool = utils::get_adjusted_amounts(
            token0_addr,
            token1_addr,
            &tvl_token0_in_pool,
            &tvl_token1_in_pool,
            &token0_derive_eth,
            &token1_derive_eth,
            &eth_price_usd,
        );
        // let amounts_for_token = utils::get_adjusted_amounts(
        //     token0_addr,
        //     token1_addr,
        //     &tvl_for_token0,
        //     &tvl_for_token1,
        //     &token0_derive_eth,
        //     &token1_derive_eth,
        //     &eth_price_usd,
        // );

        let derived_tvl_usd_for_token0 = tvl_for_token0
            .clone()
            .mul(token0_derive_eth.clone().mul(eth_price_usd.clone()));
        let derived_tvl_usd_for_token1 = tvl_for_token1
            .clone()
            .mul(token1_derive_eth.clone().mul(eth_price_usd.clone()));

        output.set_many(
            ord,
            &vec![
                format!("token:{token0_addr}:totalValueLockedUSD"),
                format!("TokenDayData:{day_id}:{token0_addr}:totalValueLockedUSD"),
                format!("TokenHourData:{hour_id}:{token0_addr}:totalValueLockedUSD"),
            ],
            &derived_tvl_usd_for_token0, // token0.totalValueLockedUSD
        );
        output.set_many(
            ord,
            &vec![
                format!("token:{token1_addr}:totalValueLockedUSD"),
                format!("TokenDayData:{day_id}:{token1_addr}:totalValueLockedUSD"),
                format!("TokenHourData:{hour_id}:{token1_addr}:totalValueLockedUSD"),
            ],
            &derived_tvl_usd_for_token1, // token1.totalValueLockedUSD
        );

        output.set(
            ord,
            format!("pool:{pool_address}:totalValueLockedETH"),
            &amounts_in_pool.delta_tvl_eth, // pool.totalValueLockedETH
        );

        output.set_many(
            ord,
            &vec![
                format!("pool:{pool_address}:totalValueLockedUSD"),
                format!("PoolDayData:{day_id}:{pool_address}:totalValueLockedUSD"),
                format!("PoolHourData:{hour_id}:{pool_address}:totalValueLockedUSD"),
            ],
            &amounts_in_pool.delta_tvl_usd, // pool.totalValueLockedUSD
        );

        // pool.totalValueLockedETHUntracked
        output.set(
            pool_event.log_ordinal,
            format!("pool:{pool_address}:totalValueLockedETHUntracked"),
            &amounts_in_pool.stable_eth_untracked,
        );

        // pool.totalValueLockedUSDUntracked
        output.set(
            ord,
            format!("pool:{pool_address}:totalValueLockedUSDUntracked"),
            &amounts_in_pool.stable_usd_untracked,
        );
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
    let prev_day_id = day_id - 1;
    output.delete_prefix(0, &format!("UniswapDayData:{prev_day_id}:"));

    for delta in derived_tvl_deltas.into_iter().key_first_segment_eq("pool") {
        log::info!("delta key {}", delta.key);
        log::info!("delta old {}", delta.old_value);
        log::info!("delta new {}", delta.new_value);
        let delta_diff = &calculate_diff(&delta);
        let ord = delta.ordinal;

        log::info!("delta diff {}", delta_diff);

        // why do we calculate the diff
        match key::last_segment(&delta.key) {
            "totalValueLockedETH" => {
                log::info!("adding factory:totalValueLockedETH {}", delta_diff);
                output.add(ord, &format!("factory:totalValueLockedETH"), delta_diff)
            }
            "totalValueLockedETHUntracked" => {
                output.add(ord, &format!("factory:totalValueLockedETHUntracked"), delta_diff)
            }
            "totalValueLockedUSD" => output.add_many(
                ord,
                &vec![
                    format!("factory:totalValueLockedUSD"),
                    format!("UniswapDayData:{day_id}:totalValueLockedUSD"),
                ],
                delta_diff,
            ),
            "totalValueLockedUSDUntracked" => {
                output.add(ord, &format!("factory:totalValueLockedUSDUntracked"), delta_diff)
            }
            _ => {}
        }
    }
}

fn calculate_diff(delta: &DeltaBigDecimal) -> BigDecimal {
    let old_value = delta.old_value.clone();
    let new_value = delta.new_value.clone();
    return new_value.clone().sub(old_value);
}

#[substreams::handlers::store]
pub fn store_ticks_liquidities(clock: Clock, events: Events, output: StoreAddBigInt) {
    let timestamp_seconds = clock.timestamp.unwrap().seconds;
    let day_id = timestamp_seconds / 86400;
    let hour_id = timestamp_seconds / 3600;
    let prev_day_id = day_id - 1;
    let prev_hour_id = hour_id - 1;

    output.delete_prefix(0, &format!("TickDayData:{prev_day_id}:"));
    output.delete_prefix(0, &format!("TickHourData:{prev_hour_id}:"));

    for event in events.pool_events {
        let pool = event.pool_address;
        match event.r#type.unwrap() {
            Type::Mint(mint) => {
                let tick_lower = &mint.tick_lower;
                let tick_upper = &mint.tick_upper;
                output.add_many(
                    event.log_ordinal,
                    &vec![
                        format!("tick:{pool}:{tick_lower}:liquidityGross"),
                        format!("tick:{pool}:{tick_lower}:liquidityNet"),
                        format!("tick:{pool}:{tick_upper}:liquidityGross"),
                        format!("TickDayData:{day_id}:{pool}:{tick_lower}:liquidityGross"),
                        format!("TickDayData:{day_id}:{pool}:{tick_lower}:liquidityNet"),
                        format!("TickDayData:{day_id}:{pool}:{tick_upper}:liquidityGross"),
                        format!("TickHourData:{hour_id}:{pool}:{tick_lower}:liquidityGross"),
                        format!("TickHourData:{hour_id}:{pool}:{tick_lower}:liquidityNet"),
                        format!("TickHourData:{hour_id}:{pool}:{tick_upper}:liquidityGross"),
                    ],
                    &BigInt::try_from(mint.amount.clone()).unwrap(),
                );
                output.add_many(
                    event.log_ordinal,
                    &vec![
                        format!("tick:{pool}:{tick_upper}:liquidityNet"),
                        format!("TickDayData:{day_id}:{pool}:{tick_upper}:liquidityNet"),
                        format!("TickHourData:{hour_id}:{pool}:{tick_upper}:liquidityNet"),
                    ],
                    &BigInt::try_from(mint.amount.clone()).unwrap().neg(),
                );
            }
            Type::Burn(burn) => {
                let tick_lower = &burn.tick_lower;
                let tick_upper = &burn.tick_upper;
                output.add_many(
                    event.log_ordinal,
                    &vec![
                        format!("tick:{pool}:{tick_lower}:liquidityGross"),
                        format!("tick:{pool}:{tick_lower}:liquidityNet"),
                        format!("tick:{pool}:{tick_upper}:liquidityGross"),
                        format!("TickDayData:{day_id}:{pool}:{tick_lower}:liquidityGross"),
                        format!("TickDayData:{day_id}:{pool}:{tick_lower}:liquidityNet"),
                        format!("TickDayData:{day_id}:{pool}:{tick_upper}:liquidityGross"),
                        format!("TickHourData:{hour_id}:{pool}:{tick_lower}:liquidityGross"),
                        format!("TickHourData:{hour_id}:{pool}:{tick_lower}:liquidityNet"),
                        format!("TickHourData:{hour_id}:{pool}:{tick_upper}:liquidityGross"),
                    ],
                    &BigInt::try_from(&burn.amount).unwrap().neg(),
                );
                output.add_many(
                    event.log_ordinal,
                    &vec![
                        format!("tick:{pool}:{tick_upper}:liquidityNet"),
                        format!("TickDayData:{day_id}:{pool}:{tick_upper}:liquidityNet"),
                        format!("TickHourData:{hour_id}:{pool}:{tick_upper}:liquidityNet"),
                    ],
                    &BigInt::try_from(&burn.amount).unwrap(),
                );
            }
            _ => {}
        }
    }
}

#[substreams::handlers::store]
pub fn store_positions(events: Events, output: StoreSetProto<PositionEvent>) {
    let mut positions_events: Vec<PositionEvent> = vec![];
    for pos in events.created_positions {
        positions_events.push(PositionEvent {
            r#type: Some(CreatedPosition(pos)),
        });
    }

    for pos in events.increase_liquidity_positions {
        positions_events.push(PositionEvent {
            r#type: Some(IncreaseLiquidityPosition(pos)),
        });
    }

    for pos in events.decrease_liquidity_positions {
        positions_events.push(PositionEvent {
            r#type: Some(DecreaseLiquidityPosition(pos)),
        });
    }

    for pos in events.collect_positions {
        positions_events.push(PositionEvent {
            r#type: Some(CollectPosition(pos)),
        });
    }

    for pos in events.transfer_positions {
        positions_events.push(PositionEvent {
            r#type: Some(TransferPosition(pos)),
        });
    }

    positions_events.sort_by(|x, y| x.get_ordinal().cmp(&y.get_ordinal()));

    for position in positions_events {
        match position.r#type.as_ref().unwrap() {
            CreatedPosition(pos) => {
                output.set(pos.log_ordinal, format!("position_created:{}", pos.token_id), &position)
            }
            IncreaseLiquidityPosition(pos) => output.set(
                pos.log_ordinal,
                format!("position_increase_liquidity:{}", pos.token_id),
                &position,
            ),
            DecreaseLiquidityPosition(pos) => output.set(
                pos.log_ordinal,
                format!("position_decrease_liquidity:{}", pos.token_id),
                &position,
            ),
            CollectPosition(pos) => {
                output.set(pos.log_ordinal, format!("position_collect:{}", pos.token_id), &position)
            }
            TransferPosition(pos) => output.set(
                pos.log_ordinal,
                format!("position_transfer:{}", pos.token_id),
                &position,
            ),
        }
    }
}

#[substreams::handlers::store]
pub fn store_min_windows(
    clock: Clock,
    prices_deltas: Deltas<DeltaBigDecimal>,     /* store_prices */
    eth_prices_deltas: Deltas<DeltaBigDecimal>, /* store_eth_prices */
    output: StoreMinBigDecimal,
) {
    let mut deltas = prices_deltas.deltas;
    let mut eth_deltas = eth_prices_deltas.deltas;
    deltas.append(&mut eth_deltas);
    deltas.sort_by(|x, y| x.ordinal.cmp(&y.ordinal));

    let timestamp_seconds = clock.timestamp.unwrap().seconds;
    let day_id = timestamp_seconds / 86400;
    let hour_id = timestamp_seconds / 3600;
    let prev_day_id = day_id - 1;
    let prev_hour_id = hour_id - 1;

    output.delete_prefix(0, &format!("PoolDayData:{prev_day_id}:"));
    output.delete_prefix(0, &format!("PoolHourData:{prev_hour_id}:"));
    output.delete_prefix(0, &format!("TokenDayData:{prev_day_id}:"));
    output.delete_prefix(0, &format!("TokenHourData:{prev_hour_id}:"));

    for delta in deltas {
        if delta.operation == store_delta::Operation::Delete {
            continue;
        }

        let table_name = match key::first_segment(&delta.key) {
            "PoolDayData" => {
                if key::last_segment(&delta.key) != "token0" {
                    continue;
                }
                "PoolDayData"
            }
            "PoolHourData" => {
                if key::last_segment(&delta.key) != "token0" {
                    continue;
                }
                "PoolHourData"
            }
            "TokenDayData" => "TokenDayData",
            "TokenHourData" => "TokenHourData",
            _ => continue,
        };

        let time_id = key::segment_at(&delta.key, 1);
        let address = key::segment_at(&delta.key, 2);

        if delta.operation == store_delta::Operation::Create {
            output.min(
                delta.ordinal,
                format!("{table_name}:{time_id}:{address}:open"),
                &delta.new_value,
            );
        }

        output.min(
            delta.ordinal,
            format!("{table_name}:{time_id}:{address}:low"),
            &delta.new_value,
        );
    }
}

#[substreams::handlers::store]
pub fn store_max_windows(
    clock: Clock,
    prices_deltas: Deltas<DeltaBigDecimal>,     /* store_prices */
    eth_prices_deltas: Deltas<DeltaBigDecimal>, /* store_eth_prices */
    output: StoreMaxBigDecimal,
) {
    let mut deltas = prices_deltas.deltas;
    let mut eth_deltas = eth_prices_deltas.deltas;
    deltas.append(&mut eth_deltas);
    deltas.sort_by(|x, y| x.ordinal.cmp(&y.ordinal));

    let timestamp_seconds = clock.timestamp.unwrap().seconds;
    let day_id = timestamp_seconds / 86400;
    let hour_id = timestamp_seconds / 3600;
    let prev_day_id = day_id - 1;
    let prev_hour_id = hour_id - 1;

    output.delete_prefix(0, &format!("PoolDayData:{prev_day_id}:"));
    output.delete_prefix(0, &format!("PoolHourData:{prev_hour_id}:"));
    output.delete_prefix(0, &format!("TokenDayData:{prev_day_id}:"));
    output.delete_prefix(0, &format!("TokenHourData:{prev_hour_id}:"));

    for delta in deltas {
        if delta.operation == store_delta::Operation::Delete {
            continue;
        }

        let table_name = match key::first_segment(&delta.key) {
            "PoolDayData" => {
                if key::last_segment(&delta.key) != "token0" {
                    continue;
                }
                "PoolDayData"
            }
            "PoolHourData" => {
                if key::last_segment(&delta.key) != "token0" {
                    continue;
                }
                "PoolHourData"
            }
            "TokenDayData" => "TokenDayData",
            "TokenHourData" => "TokenHourData",
            _ => continue,
        };

        let day_id = key::segment_at(&delta.key, 1);
        let pool_address = key::segment_at(&delta.key, 2);

        output.max(
            delta.ordinal,
            format!("{table_name}:{day_id}:{pool_address}:high"),
            delta.new_value,
        );
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
    pool_sqrt_price_store: StoreGetProto<PoolSqrtPrice>, /* store_pool_sqrt_price */
    pool_liquidities_store_deltas: Deltas<DeltaBigInt>,  /* store_pool_liquidities */
    token_tvl_deltas: Deltas<DeltaBigDecimal>,           /* store_token_tvl */
    price_deltas: Deltas<DeltaBigDecimal>,               /* store_prices */
    store_prices: StoreGetBigDecimal,                    /* store_prices */
    tokens_store: StoreGetInt64,                         /* store_tokens */
    tokens_whitelist_pools_deltas: Deltas<DeltaArray<String>>, /* store_tokens_whitelist_pools */
    derived_tvl_deltas: Deltas<DeltaBigDecimal>,         /* store_derived_tvl */
    ticks_liquidities_deltas: Deltas<DeltaBigInt>,       /* store_ticks_liquidities */
    tx_count_store: StoreGetBigInt,                      /* store_total_tx_counts */
    store_eth_prices: StoreGetBigDecimal,                /* store_eth_prices */
    store_positions: StoreGetProto<PositionEvent>,       /* store_positions */
    min_windows_deltas: Deltas<DeltaBigDecimal>,         /* store_min_windows */
    max_windows_deltas: Deltas<DeltaBigDecimal>,         /* store_max_windows */
) -> Result<EntityChanges, Error> {
    let mut tables = Tables::new();
    let timestamp = clock.timestamp.unwrap().seconds;

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
    db::pools_created_pool_entity_changes(&mut tables, &pools_created);
    db::sqrt_price_and_tick_pool_entity_change(&mut tables, &pool_sqrt_price_deltas);
    db::liquidities_pool_entity_change(&mut tables, &pool_liquidities_store_deltas);
    db::fee_growth_global_pool_entity_change(&mut tables, &events.fee_growth_global_updates);
    db::total_value_locked_pool_entity_change(&mut tables, &derived_tvl_deltas);
    db::total_value_locked_by_token_pool_entity_change(&mut tables, &token_tvl_deltas);
    db::price_pool_entity_change(&mut tables, &price_deltas);
    db::tx_count_pool_entity_change(&mut tables, &tx_count_deltas);
    db::swap_volume_pool_entity_change(&mut tables, &swaps_volume_deltas);

    // Tokens:
    db::tokens_created_token_entity_changes(&mut tables, &pools_created, tokens_store);
    db::swap_volume_token_entity_change(&mut tables, &swaps_volume_deltas);
    db::tx_count_token_entity_change(&mut tables, &tx_count_deltas);
    db::total_value_locked_by_token_token_entity_change(&mut tables, &token_tvl_deltas);
    db::total_value_locked_usd_token_entity_change(&mut tables, &derived_tvl_deltas);
    db::derived_eth_prices_token_entity_change(&mut tables, &derived_eth_prices_deltas);
    db::whitelist_token_entity_change(&mut tables, tokens_whitelist_pools_deltas);

    // Tick:
    db::create_tick_entity_change(&mut tables, &events.ticks_created);
    db::update_tick_entity_change(&mut tables, &events.ticks_updated);
    db::liquidities_tick_entity_change(&mut tables, &ticks_liquidities_deltas);

    // Tick Day/Hour data
    // db::create_entity_tick_windows(&mut tables, &events.ticks_created);
    // db::update_tick_windows(&mut tables, &events.ticks_updated);
    // db::liquidities_tick_windows(&mut tables, &ticks_liquidities_deltas);

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
        &store_positions,
    );
    db::decrease_liquidity_snapshot_position_entity_change(
        &mut tables,
        clock.number,
        &events.decrease_liquidity_positions,
        &store_positions,
    );
    db::collect_snapshot_position_entity_change(&mut tables, clock.number, &events.collect_positions, &store_positions);
    db::transfer_snapshot_position_entity_change(
        &mut tables,
        clock.number,
        &events.transfer_positions,
        &store_positions,
    );

    // Transaction:
    db::transaction_entity_change(&mut tables, &events.transactions);

    // Swap, Mint, Burn:
    db::swaps_mints_burns_created_entity_change(&mut tables, &events.pool_events, tx_count_store, store_eth_prices);

    // Flashes:
    // TODO: should we implement flashes entity change - UNISWAP has not done this part
    // db::flashes_update_pool_fee_entity_change(&mut tables, events.flashes);

    // Uniswap day data:
    db::uniswap_day_data_create(&mut tables, &tx_count_deltas);
    db::uniswap_day_data_update(
        &mut tables,
        &swaps_volume_deltas,
        &derived_factory_tvl_deltas,
        &tx_count_deltas,
    );

    // Pool Day/Hour data:
    db::pool_windows_create(&mut tables, &tx_count_deltas);
    db::pool_windows_update(
        &mut tables,
        timestamp,
        &tx_count_deltas,
        &swaps_volume_deltas,
        &events,
        &pool_sqrt_price_store,
        &pool_liquidities_store_deltas,
        &price_deltas,
        &store_prices,
        &derived_tvl_deltas,
        &min_windows_deltas,
        &max_windows_deltas,
    );

    // Token Day/Hour data:
    db::token_windows_create(&mut tables, &tx_count_deltas);
    db::token_windows_update(
        &mut tables,
        timestamp,
        &swaps_volume_deltas,
        &derived_tvl_deltas,
        &min_windows_deltas,
        &max_windows_deltas,
        &derived_eth_prices_deltas,
        &token_tvl_deltas,
    );

    Ok(tables.to_entity_changes())
}
