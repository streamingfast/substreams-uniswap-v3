extern crate core;

mod abi;
mod eth;
mod helper;
mod keyer;
mod macros;
mod math;
mod pb;
mod price;
mod rpc;
mod utils;

use crate::abi::pool::events::Swap;
use crate::ethpb::v1::{Block, StorageChange};
use crate::keyer::{native_pool_from_key, native_token_from_key};
use crate::pb::uniswap::entity_change::Operation;
use crate::pb::uniswap::event::Type::{Burn as BurnEvent, Mint as MintEvent, Swap as SwapEvent};
use crate::pb::uniswap::field::Type as FieldType;
use crate::pb::uniswap::{
    Burn, EntitiesChanges, EntityChange, Erc20Token, Erc20Tokens, Event, EventAmount, Events,
    Field, Mint, Pool, PoolLiquidities, PoolLiquidity, PoolSqrtPrice, PoolSqrtPrices, Pools, Tick,
};
use crate::price::WHITELIST_TOKENS;
use crate::utils::UNISWAP_V3_FACTORY;
use bigdecimal::ToPrimitive;
use bigdecimal::{BigDecimal, FromPrimitive};
use num_bigint::BigInt;
use std::collections::HashMap;
use std::ops::{Add, Div, Mul, Neg};
use std::str::FromStr;
use substreams::errors::Error;
use substreams::store;
use substreams::store::{StoreAddBigFloat, StoreAddBigInt, StoreAppend, StoreGet, StoreSet};
use substreams::{log, proto, Hex};
use substreams_ethereum::{pb::eth as ethpb, Event as EventTrait};

#[substreams::handlers::map]
pub fn map_pools_created(block: ethpb::v1::Block) -> Result<Pools, Error> {
    let mut pools = vec![];
    for log in block.logs() {
        if let Some(event) = abi::factory::events::PoolCreated::match_and_decode(log) {
            log::info!("pool addr: {}", Hex(&event.pool));

            let mut ignore = false;
            if log.address() != UNISWAP_V3_FACTORY
                || Hex(&event.pool)
                    .to_string()
                    .eq("8fe8d9bb8eeba3ed688069c3d6b556c9ca258248")
            {
                ignore = true;
            }

            let mut pool: Pool = Pool {
                address: Hex(&log.data()[44..64]).to_string(),
                transaction_id: Hex(&log.receipt.transaction.hash).to_string(),
                created_at_block_number: block.number.to_string(),
                created_at_timestamp: block
                    .header
                    .as_ref()
                    .unwrap()
                    .timestamp
                    .as_ref()
                    .unwrap()
                    .seconds
                    .to_string(),
                fee_tier: event.fee.as_u32(),
                tick_spacing: event.tick_spacing.to_i32().unwrap(),
                log_ordinal: log.ordinal(),
                ignore_pool: ignore,
                ..Default::default()
            };
            // check the validity of the token0 and token1
            let mut token0 = Erc20Token {
                address: "".to_string(),
                name: "".to_string(),
                symbol: "".to_string(),
                decimals: 0,
                whitelist_pools: vec![],
            };
            let mut token1 = Erc20Token {
                address: "".to_string(),
                name: "".to_string(),
                symbol: "".to_string(),
                decimals: 0,
                whitelist_pools: vec![],
            };

            let token0_address: String = Hex(&event.token0).to_string();
            match rpc::create_uniswap_token(&token0_address) {
                None => {
                    continue;
                }
                Some(token) => {
                    token0 = token;
                }
            }

            let token1_address: String = Hex(&event.token1).to_string();
            match rpc::create_uniswap_token(&token1_address) {
                None => {
                    continue;
                }
                Some(token) => {
                    token1 = token;
                }
            }

            pool.token0 = Some(token0.clone());
            pool.token1 = Some(token1.clone());
            pools.push(pool);
        }
    }
    Ok(Pools { pools })
}

#[substreams::handlers::store]
pub fn store_pools(pools: Pools, output: store::StoreSet) {
    for pool in pools.pools {
        output.set(
            pool.log_ordinal,
            keyer::pool_key(&pool.address),
            &proto::encode(&pool).unwrap(),
        );
        output.set(
            pool.log_ordinal,
            keyer::pool_token_index_key(
                &pool.token0.as_ref().unwrap().address,
                &pool.token1.as_ref().unwrap().address,
            ),
            &proto::encode(&pool).unwrap(),
        )
    }
}

#[substreams::handlers::map]
pub fn map_tokens_whitelist_pools(pools: Pools) -> Result<Erc20Tokens, Error> {
    let mut erc20_tokens = Erc20Tokens { tokens: vec![] };

    for pool in pools.pools {
        let mut token0 = pool.token0.unwrap();
        let mut token1 = pool.token1.unwrap();

        if WHITELIST_TOKENS.contains(&token0.address.as_str()) {
            log::info!("adding pool: {} to token: {}", pool.address, token1.address);
            token1.whitelist_pools.push(pool.address.to_string());
            erc20_tokens.tokens.push(token1.clone());
        }

        if WHITELIST_TOKENS.contains(&token1.address.as_str()) {
            log::info!("adding pool: {} to token: {}", pool.address, token0.address);
            token0.whitelist_pools.push(pool.address.to_string());
            erc20_tokens.tokens.push(token0.clone());
        }
    }

    Ok(erc20_tokens)
}

#[substreams::handlers::store]
pub fn store_tokens_whitelist_pools(tokens: Erc20Tokens, output_append: StoreAppend) {
    for token in tokens.tokens {
        for pools in token.whitelist_pools {
            output_append.append(
                1,
                keyer::token_pool_whitelist(&token.address),
                &format!("{};", pools.to_string()),
            )
        }
    }
}

#[substreams::handlers::map]
pub fn map_pool_sqrt_price(block: Block, pools_store: StoreGet) -> Result<PoolSqrtPrices, Error> {
    let mut pool_sqrt_prices = vec![];
    for log in block.logs() {
        let pool_address = &Hex(log.address()).to_string();
        if let Some(event) = abi::pool::events::Initialize::match_and_decode(log) {
            match helper::get_pool(&pools_store, pool_address) {
                Err(err) => {
                    log::info!("skipping pool {}: {:?}", &pool_address, err);
                }
                Ok(pool) => {
                    pool_sqrt_prices.push(PoolSqrtPrice {
                        pool_address: pool.address,
                        ordinal: log.ordinal(),
                        sqrt_price: event.sqrt_price_x96.to_string(),
                        tick: event.tick.to_string(),
                    });
                }
            }
        } else if let Some(event) = Swap::match_and_decode(log) {
            match helper::get_pool(&pools_store, &pool_address) {
                Err(err) => {
                    log::info!("skipping pool {}: {:?}", &pool_address, err);
                }
                Ok(pool) => {
                    pool_sqrt_prices.push(PoolSqrtPrice {
                        pool_address: pool.address,
                        ordinal: log.ordinal(),
                        sqrt_price: event.sqrt_price_x96.to_string(),
                        tick: event.tick.to_string(),
                    });
                }
            }
        }
    }
    Ok(PoolSqrtPrices { pool_sqrt_prices })
}

#[substreams::handlers::store]
pub fn store_pool_sqrt_price(sqrt_prices: PoolSqrtPrices, output: StoreSet) {
    for sqrt_price in sqrt_prices.pool_sqrt_prices {
        log::info!("storing sqrt price {}", &sqrt_price.pool_address);
        // fixme: probably need to have a similar key for like we have for a swap
        output.set(
            sqrt_price.ordinal,
            keyer::pool_sqrt_price_key(&sqrt_price.pool_address),
            &proto::encode(&sqrt_price).unwrap(),
        )
    }
}

#[substreams::handlers::map]
pub fn map_pool_liquidities(block: Block, pools_store: StoreGet) -> Result<PoolLiquidities, Error> {
    let mut pool_liquidities = vec![];
    let mut debug = false;
    for trx in block.transaction_traces {
        if trx.status != 1 {
            continue;
        }
        for call in trx.calls {
            let _call_index = call.index;
            if call.state_reverted {
                continue;
            }
            for log in call.logs {
                let pool_key = keyer::pool_key(&Hex(&log.address).to_string());
                if let Some(_) = Swap::match_and_decode(&log) {
                    match pools_store.get_last(&pool_key) {
                        None => continue,
                        Some(pool_bytes) => {
                            let pool: Pool = proto::decode(&pool_bytes).unwrap();
                            if !utils::should_handle_swap(&pool) {
                                continue;
                            }
                            if let Some(pl) = utils::extract_pool_liquidity(
                                log.ordinal,
                                &log.address,
                                &call.storage_changes,
                            ) {
                                pool_liquidities.push(pl)
                            }
                        }
                    }
                } else if let Some(_) = abi::pool::events::Mint::match_and_decode(&log) {
                    match pools_store.get_last(&pool_key) {
                        None => {
                            log::info!("unknown pool");
                            continue;
                        }
                        Some(pool_bytes) => {
                            let pool: Pool = proto::decode(&pool_bytes).unwrap();
                            if !utils::should_handle_mint_and_burn(&pool) {
                                continue;
                            }
                            if let Some(pl) = utils::extract_pool_liquidity(
                                log.ordinal,
                                &log.address,
                                &call.storage_changes,
                            ) {
                                pool_liquidities.push(pl)
                            }
                        }
                    }
                } else if let Some(_) = abi::pool::events::Burn::match_and_decode(&log) {
                    match pools_store.get_last(&pool_key) {
                        None => continue,
                        Some(pool_bytes) => {
                            let pool: Pool = proto::decode(&pool_bytes).unwrap();
                            if !utils::should_handle_mint_and_burn(&pool) {
                                continue;
                            }
                            if let Some(pl) = utils::extract_pool_liquidity(
                                log.ordinal,
                                &log.address,
                                &call.storage_changes,
                            ) {
                                pool_liquidities.push(pl)
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(PoolLiquidities { pool_liquidities })
}

#[substreams::handlers::store]
pub fn store_pool_liquidities(pool_liquidities: PoolLiquidities, output: store::StoreSet) {
    for pool_liquidity in pool_liquidities.pool_liquidities {
        // fixme: probably need to have a similar key for like we have for a swap
        output.set(
            0,
            keyer::pool_liquidity(&pool_liquidity.pool_address),
            &Vec::from(pool_liquidity.liquidity),
        )
    }
}

#[substreams::handlers::store]
pub fn store_prices(
    pool_sqrt_prices: PoolSqrtPrices,
    pools_store: store::StoreGet,
    output: store::StoreSet,
) {
    for sqrt_price_update in pool_sqrt_prices.pool_sqrt_prices {
        match helper::get_pool(&pools_store, &sqrt_price_update.pool_address) {
            Err(err) => {
                log::info!(
                    "skipping pool {}: {:?}",
                    &sqrt_price_update.pool_address,
                    err
                );
                continue;
            }
            Ok(pool) => {
                let token0 = pool.token0.as_ref().unwrap();
                let token1 = pool.token1.as_ref().unwrap();
                log::info!(
                    "pool addr: {}, token 0 addr: {}, token 1 addr: {}",
                    pool.address,
                    token0.address,
                    token1.address
                );

                let sqrt_price =
                    BigDecimal::from_str(sqrt_price_update.sqrt_price.as_str()).unwrap();
                log::info!("sqrtPrice: {}", sqrt_price.to_string());

                let tokens_price: (BigDecimal, BigDecimal) =
                    price::sqrt_price_x96_to_token_prices(&sqrt_price, &token0, &token1);
                log::debug!("token prices: {} {}", tokens_price.0, tokens_price.1);

                output.set(
                    sqrt_price_update.ordinal,
                    keyer::prices_pool_token_key(&pool.address, &token0.address),
                    &Vec::from(tokens_price.0.to_string()),
                );
                output.set(
                    sqrt_price_update.ordinal,
                    keyer::prices_pool_token_key(&pool.address, &token1.address),
                    &Vec::from(tokens_price.1.to_string()),
                );

                output.set(
                    sqrt_price_update.ordinal,
                    keyer::prices_token_pair(
                        &pool.token0.as_ref().unwrap().address,
                        &pool.token1.as_ref().unwrap().address,
                    ),
                    &Vec::from(tokens_price.0.to_string()),
                );
                output.set(
                    sqrt_price_update.ordinal,
                    keyer::prices_token_pair(
                        &pool.token1.as_ref().unwrap().address,
                        &pool.token0.as_ref().unwrap().address,
                    ),
                    &Vec::from(tokens_price.1.to_string()),
                );
            }
        }
    }
}

#[substreams::handlers::map]
pub fn map_swaps_mints_burns(
    block: ethpb::v1::Block,
    pools_store: StoreGet,
) -> Result<Events, Error> {
    let mut events = vec![];
    for log in block.logs() {
        let pool_key = &format!("pool:{}", Hex(&log.address()).to_string());

        if let Some(swap) = Swap::match_and_decode(log) {
            match pools_store.get_last(pool_key) {
                None => {
                    log::info!(
                        "invalid swap. pool does not exist. pool address {} transaction {}",
                        Hex(&log.address()).to_string(),
                        Hex(&log.receipt.transaction.hash).to_string()
                    );
                    continue;
                }
                Some(pool_bytes) => {
                    let pool: Pool = proto::decode(&pool_bytes).unwrap();
                    if !utils::should_handle_swap(&pool) {
                        continue;
                    }

                    let token0 = pool.token0.as_ref().unwrap();
                    let token1 = pool.token1.as_ref().unwrap();

                    let amount0 = utils::convert_token_to_decimal(&swap.amount0, token0.decimals);
                    let amount1 = utils::convert_token_to_decimal(&swap.amount1, token1.decimals);
                    log::debug!("amount0: {}, amount1:{}", amount0, amount1);

                    events.push(Event {
                        log_ordinal: log.ordinal(),
                        log_index: log.block_index() as u64,
                        pool_address: pool.address.to_string(),
                        token0: pool.token0.as_ref().unwrap().address.to_string(),
                        token1: pool.token1.as_ref().unwrap().address.to_string(),
                        fee: pool.fee_tier.to_string(),
                        transaction_id: Hex(&log.receipt.transaction.hash).to_string(), // todo: need to add #tx_count at the end
                        timestamp: block
                            .header
                            .as_ref()
                            .unwrap()
                            .timestamp
                            .as_ref()
                            .unwrap()
                            .seconds as u64,
                        r#type: Some(SwapEvent(pb::uniswap::Swap {
                            sender: Hex(&swap.sender).to_string(),
                            recipient: Hex(&swap.recipient).to_string(),
                            origin: Hex(&log.receipt.transaction.from).to_string(),
                            amount_0: amount0.to_string(),
                            amount_1: amount1.to_string(),
                            sqrt_price: swap.sqrt_price_x96.to_string(),
                            liquidity: swap.liquidity.to_string(),
                            tick: swap.tick.to_i32().unwrap(),
                        })),
                    });
                }
            }
        } else if let Some(mint) = abi::pool::events::Mint::match_and_decode(log) {
            match pools_store.get_last(pool_key) {
                None => {
                    log::info!(
                        "invalid mint. pool does not exist. pool address {} transaction {}",
                        Hex(&log.address()).to_string(),
                        Hex(&log.receipt.transaction.hash).to_string()
                    );
                    continue;
                }
                Some(pool_bytes) => {
                    let pool: Pool = proto::decode(&pool_bytes).unwrap();
                    if !utils::should_handle_mint_and_burn(&pool) {
                        continue;
                    }

                    let token0 = pool.token0.as_ref().unwrap();
                    let token1 = pool.token1.as_ref().unwrap();

                    let amount0_bi = BigInt::from_str(mint.amount0.to_string().as_str()).unwrap();
                    let amount1_bi = BigInt::from_str(mint.amount1.to_string().as_str()).unwrap();
                    let amount0 = utils::convert_token_to_decimal(&amount0_bi, token0.decimals);
                    let amount1 = utils::convert_token_to_decimal(&amount1_bi, token1.decimals);
                    log::debug!(
                        "logOrdinal: {}, amount0: {}, amount1:{}",
                        log.ordinal(),
                        amount0,
                        amount1
                    );

                    events.push(Event {
                        log_ordinal: log.ordinal(),
                        log_index: log.block_index() as u64,
                        pool_address: pool.address.to_string(),
                        token0: pool.token0.unwrap().address,
                        token1: pool.token1.unwrap().address,
                        fee: pool.fee_tier.to_string(),
                        transaction_id: Hex(&log.receipt.transaction.hash).to_string(),
                        timestamp: block
                            .header
                            .as_ref()
                            .unwrap()
                            .timestamp
                            .as_ref()
                            .unwrap()
                            .seconds as u64,
                        r#type: Some(MintEvent(Mint {
                            owner: Hex(&mint.owner).to_string(),
                            sender: Hex(&mint.sender).to_string(),
                            origin: Hex(&log.receipt.transaction.from).to_string(),
                            amount: mint.amount.to_string(),
                            amount_0: amount0.to_string(),
                            amount_1: amount1.to_string(),
                            tick_lower: mint.tick_lower.to_i32().unwrap(),
                            tick_upper: mint.tick_upper.to_i32().unwrap(),
                        })),
                    });
                }
            }
        } else if let Some(burn) = abi::pool::events::Burn::match_and_decode(log) {
            match pools_store.get_last(pool_key) {
                None => {
                    log::info!(
                        "invalid burn. pool does not exist. pool address {} transaction {}",
                        Hex(&log.address()).to_string(),
                        Hex(&log.receipt.transaction.hash).to_string()
                    );
                    continue;
                }
                Some(pool_bytes) => {
                    let pool: Pool = proto::decode(&pool_bytes).unwrap();
                    if !utils::should_handle_mint_and_burn(&pool) {
                        continue;
                    }

                    let token0 = pool.token0.as_ref().unwrap();
                    let token1 = pool.token1.as_ref().unwrap();

                    let amount0_bi = BigInt::from_str(burn.amount0.to_string().as_str()).unwrap();
                    let amount1_bi = BigInt::from_str(burn.amount1.to_string().as_str()).unwrap();
                    let amount0 = utils::convert_token_to_decimal(&amount0_bi, token0.decimals);
                    let amount1 = utils::convert_token_to_decimal(&amount1_bi, token1.decimals);
                    log::debug!("amount0: {}, amount1:{}", amount0, amount1);

                    events.push(Event {
                        log_ordinal: log.ordinal(),
                        log_index: log.block_index() as u64,
                        pool_address: pool.address.to_string(),
                        token0: pool.token0.as_ref().unwrap().address.to_string(),
                        token1: pool.token1.as_ref().unwrap().address.to_string(),
                        fee: pool.fee_tier.to_string(),
                        transaction_id: Hex(&log.receipt.transaction.hash).to_string(),
                        timestamp: block
                            .header
                            .as_ref()
                            .unwrap()
                            .timestamp
                            .as_ref()
                            .unwrap()
                            .seconds as u64,
                        r#type: Some(BurnEvent(Burn {
                            owner: Hex(&burn.owner).to_string(),
                            origin: Hex(&log.receipt.transaction.from).to_string(),
                            amount: burn.amount.to_string(),
                            amount_0: amount0.to_string(),
                            amount_1: amount1.to_string(),
                            tick_lower: burn.tick_lower.to_i32().unwrap(),
                            tick_upper: burn.tick_upper.to_i32().unwrap(),
                        })),
                    });
                }
            }
        }
    }
    Ok(Events { events })
}

#[substreams::handlers::map]
pub fn map_event_amounts(events: Events) -> Result<pb::uniswap::EventAmounts, Error> {
    let mut event_amounts = vec![];
    for event in events.events {
        log::debug!("transaction id: {}", event.transaction_id);
        if event.r#type.is_none() {
            continue;
        }

        if event.r#type.is_some() {
            match event.r#type.unwrap() {
                BurnEvent(burn) => {
                    log::debug!("handling burn for pool {}", event.pool_address);
                    let amount0 = BigDecimal::from_str(burn.amount_0.as_str()).unwrap();
                    let amount1 = BigDecimal::from_str(burn.amount_1.as_str()).unwrap();
                    let mut ea = EventAmount {
                        pool_address: event.pool_address,
                        log_ordinal: event.log_ordinal,
                        token0_addr: event.token0,
                        amount0_value: amount0.neg().to_string(),
                        token1_addr: event.token1,
                        amount1_value: amount1.neg().to_string(),
                        ..Default::default()
                    };
                    event_amounts.push(ea);
                }
                MintEvent(mint) => {
                    log::debug!("handling mint for pool {}", event.pool_address);
                    let amount0 = BigDecimal::from_str(mint.amount_0.as_str()).unwrap();
                    let amount1 = BigDecimal::from_str(mint.amount_1.as_str()).unwrap();
                    let mut ea = EventAmount {
                        pool_address: event.pool_address,
                        log_ordinal: event.log_ordinal,
                        token0_addr: event.token0,
                        amount0_value: amount0.to_string(),
                        token1_addr: event.token1,
                        amount1_value: amount1.to_string(),
                        ..Default::default()
                    };
                    event_amounts.push(ea);
                }
                SwapEvent(swap) => {
                    log::debug!("handling swap for pool {}", event.pool_address);
                    let amount0 = BigDecimal::from_str(swap.amount_0.as_str()).unwrap();
                    let amount1 = BigDecimal::from_str(swap.amount_1.as_str()).unwrap();
                    event_amounts.push(EventAmount {
                        pool_address: event.pool_address,
                        log_ordinal: event.log_ordinal,
                        token0_addr: event.token0,
                        amount0_value: amount0.to_string(),
                        token1_addr: event.token1,
                        amount1_value: amount1.to_string(),
                        ..Default::default()
                    });
                }
            }
        }
    }
    Ok(pb::uniswap::EventAmounts { event_amounts })
}

#[substreams::handlers::store]
pub fn store_total_tx_counts(events: Events, output: store::StoreAddBigInt) {
    for event in events.events {
        output.add(
            event.log_ordinal,
            keyer::pool_total_tx_count(&event.pool_address),
            &BigInt::from(1 as i32),
        );
        output.add(
            event.log_ordinal,
            keyer::token_total_tx_count(&event.token0),
            &BigInt::from(1 as i32),
        );
        output.add(
            event.log_ordinal,
            keyer::token_total_tx_count(&event.token1),
            &BigInt::from(1 as i32),
        );
        output.add(
            event.log_ordinal,
            keyer::factory_total_tx_count(),
            &BigInt::from(1 as i32),
        );
    }
}

//todo: maybe change the name of this substreams and actually have something related to the events
// overall and not just the swaps ??
#[substreams::handlers::store]
pub fn store_swaps_volume(
    events: Events,
    store_pool: StoreGet,
    store_total_tx_counts: StoreGet,
    store_eth_prices: StoreGet,
    output: StoreAddBigFloat,
) {
    for event in events.events {
        let pool: Pool = match store_pool.get_last(keyer::pool_key(&event.pool_address)) {
            None => continue,
            Some(bytes) => proto::decode(&bytes).unwrap(),
        };
        match store_total_tx_counts.get_last(keyer::pool_total_tx_count(&event.pool_address)) {
            None => {}
            Some(_) => match event.r#type.unwrap() {
                SwapEvent(swap) => {
                    let mut eth_price_in_usd = helper::get_eth_price(&store_eth_prices).unwrap();

                    let mut token0_derived_eth_price: BigDecimal = BigDecimal::from(0 as i32);
                    match store_eth_prices.get_last(keyer::token_eth_price(&event.token0)) {
                        None => continue,
                        Some(bytes) => token0_derived_eth_price = math::decimal_from_bytes(&bytes),
                    }

                    let mut token1_derived_eth_price: BigDecimal = BigDecimal::from(0 as i32);
                    match store_eth_prices.get_last(keyer::token_eth_price(&event.token1)) {
                        None => continue,
                        Some(bytes) => token1_derived_eth_price = math::decimal_from_bytes(&bytes),
                    }

                    let mut amount0_abs: BigDecimal =
                        BigDecimal::from_str(swap.amount_0.as_str()).unwrap();
                    if amount0_abs.lt(&BigDecimal::from(0 as u64)) {
                        amount0_abs = amount0_abs.mul(BigDecimal::from(-1 as i64))
                    }

                    let mut amount1_abs: BigDecimal =
                        BigDecimal::from_str(swap.amount_1.as_str()).unwrap();
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

                    let amount_total_usd_untracked: BigDecimal = amount0_abs
                        .clone()
                        .add(amount1_abs.clone())
                        .div(BigDecimal::from(2 as i32));

                    let fee_tier: BigDecimal = BigDecimal::from(pool.fee_tier);
                    let fee_usd: BigDecimal = amount_total_usd_tracked
                        .clone()
                        .mul(fee_tier)
                        .div(BigDecimal::from(1000000 as u64));

                    output.add(
                        event.log_ordinal,
                        keyer::swap_volume_token_0(&event.pool_address),
                        &amount0_abs,
                    );
                    output.add(
                        event.log_ordinal,
                        keyer::swap_volume_token_1(&event.pool_address),
                        &amount1_abs,
                    );
                    output.add(
                        event.log_ordinal,
                        keyer::swap_volume_usd(&event.pool_address),
                        &amount_total_usd_tracked,
                    );
                    output.add(
                        event.log_ordinal,
                        keyer::swap_untracked_volume_usd(&event.pool_address),
                        &amount_total_usd_untracked,
                    );
                    output.add(
                        event.log_ordinal,
                        keyer::swap_fee_usd(&event.pool_address),
                        &fee_usd,
                    )
                }
                _ => {}
            },
        }
    }
}

#[substreams::handlers::store]
pub fn store_pool_fee_growth_global_x128(pools: Pools, output: StoreSet) {
    for pool in pools.pools {
        log::info!(
            "pool address: {} trx_id:{}",
            pool.address,
            pool.transaction_id
        );
        let (bd1, bd2) = rpc::fee_growth_global_x128_call(&pool.address);
        log::debug!("big decimal1: {}", bd1);
        log::debug!("big decimal2: {}", bd2);

        output.set(
            pool.log_ordinal,
            keyer::pool_fee_growth_global_x128(&pool.address, "token0".to_string()),
            &Vec::from(bd1.to_string().as_str()),
        );
        output.set(
            pool.log_ordinal,
            keyer::pool_fee_growth_global_x128(&pool.address, "token1".to_string()),
            &Vec::from(bd1.to_string().as_str()),
        );
    }
}

#[substreams::handlers::store]
pub fn store_native_total_value_locked(
    event_amounts: pb::uniswap::EventAmounts,
    output: StoreAddBigFloat,
) {
    for event_amount in event_amounts.event_amounts {
        output.add(
            event_amount.log_ordinal,
            keyer::token_native_total_value_locked(&event_amount.token0_addr),
            &BigDecimal::from_str(event_amount.amount0_value.as_str()).unwrap(),
        );
        output.add(
            event_amount.log_ordinal,
            keyer::pool_native_total_value_locked_token(
                &event_amount.pool_address,
                &event_amount.token0_addr,
            ),
            &BigDecimal::from_str(event_amount.amount0_value.as_str()).unwrap(),
        );
        output.add(
            event_amount.log_ordinal,
            keyer::token_native_total_value_locked(&event_amount.token1_addr),
            &BigDecimal::from_str(event_amount.amount1_value.as_str()).unwrap(),
        );
        output.add(
            event_amount.log_ordinal,
            keyer::pool_native_total_value_locked_token(
                &event_amount.pool_address,
                &event_amount.token1_addr,
            ),
            &BigDecimal::from_str(event_amount.amount1_value.as_str()).unwrap(),
        );
    }
}

#[substreams::handlers::store]
pub fn store_eth_prices(
    pool_sqrt_prices: PoolSqrtPrices,
    pools_store: StoreGet,
    prices_store: StoreGet,
    tokens_whitelist_pools_store: StoreGet,
    total_native_value_locked_store: StoreGet,
    pool_liquidities_store: store::StoreGet,
    output: StoreSet,
) {
    for pool_sqrt_price in pool_sqrt_prices.pool_sqrt_prices {
        log::debug!(
            "handling pool price update - addr: {} price: {}",
            pool_sqrt_price.pool_address,
            pool_sqrt_price.sqrt_price
        );
        let pool = helper::get_pool(&pools_store, &pool_sqrt_price.pool_address).unwrap();
        let token_0 = pool.token0.as_ref().unwrap();
        let token_1 = pool.token1.as_ref().unwrap();

        utils::log_token(token_0, 0);
        utils::log_token(token_1, 1);

        let bundle_eth_price_usd =
            price::get_eth_price_in_usd(&prices_store, pool_sqrt_price.ordinal);
        log::info!("bundle_eth_price_usd: {}", bundle_eth_price_usd);

        let token0_derived_eth_price = price::find_eth_per_token(
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

        let token1_derived_eth_price = price::find_eth_per_token(
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

        output.set(
            pool_sqrt_price.ordinal,
            keyer::bundle_eth_price(),
            &Vec::from(bundle_eth_price_usd.to_string()),
        );

        output.set(
            pool_sqrt_price.ordinal,
            keyer::token_eth_price(&token_0.address),
            &Vec::from(token0_derived_eth_price.to_string()),
        );

        output.set(
            pool_sqrt_price.ordinal,
            keyer::token_eth_price(&token_1.address),
            &Vec::from(token1_derived_eth_price.to_string()),
        );
    }
}

#[substreams::handlers::store]
pub fn store_total_value_locked_by_tokens(events: Events, output: StoreAddBigFloat) {
    for event in events.events {
        let mut amount0: BigDecimal = BigDecimal::from(0 as i32);
        let mut amount1: BigDecimal = BigDecimal::from(0 as i32);

        match event.r#type.unwrap() {
            BurnEvent(burn) => {
                amount0 = BigDecimal::from_str(burn.amount_0.as_str()).unwrap();
                amount1 = BigDecimal::from_str(burn.amount_1.as_str()).unwrap();
            }
            MintEvent(mint) => {
                amount0 = BigDecimal::from_str(mint.amount_0.as_str()).unwrap();
                amount1 = BigDecimal::from_str(mint.amount_1.as_str()).unwrap();
            }
            SwapEvent(swap) => {
                amount0 = BigDecimal::from_str(swap.amount_0.as_str()).unwrap();
                amount1 = BigDecimal::from_str(swap.amount_1.as_str()).unwrap();
            }
        }

        output.add(
            event.log_ordinal,
            keyer::total_value_locked_tokens(&event.pool_address, "token0".to_string()),
            &amount0,
        );
        output.add(
            event.log_ordinal,
            keyer::total_value_locked_tokens(&event.pool_address, "token1".to_string()),
            &amount1,
        );
    }
}

#[substreams::handlers::store]
pub fn store_total_value_locked(
    native_total_value_locked_deltas: store::Deltas,
    pools_store: StoreGet,
    eth_prices_store: StoreGet,
    output: StoreSet,
) {
    // fixme: @julien: what is the use for the pool aggregator here ?
    let mut pool_aggregator: HashMap<String, (u64, BigDecimal)> = HashMap::from([]);
    let eth_price_usd = helper::get_eth_price(&eth_prices_store).unwrap();
    for native_total_value_locked in native_total_value_locked_deltas {
        if let Some(token_addr) = native_token_from_key(&native_total_value_locked.key) {
            let value = math::decimal_from_bytes(&native_total_value_locked.new_value);
            let token_derive_eth =
                helper::get_token_eth_price(&eth_prices_store, &token_addr).unwrap();
            let total_value_locked_usd = value.mul(token_derive_eth).mul(&eth_price_usd);
            log::info!(
                "token {} total value locked usd: {}",
                token_addr,
                total_value_locked_usd
            );
            output.set(
                native_total_value_locked.ordinal,
                keyer::token_usd_total_value_locked(&token_addr),
                &Vec::from(total_value_locked_usd.to_string()),
            );
        } else if let Some((pool_addr, token_addr)) =
            native_pool_from_key(&native_total_value_locked.key)
        {
            let pool = helper::get_pool(&pools_store, &pool_addr).unwrap();
            // we only want to use the token0
            if pool.token0.as_ref().unwrap().address != token_addr {
                continue;
            }
            let value = math::decimal_from_bytes(&native_total_value_locked.new_value);
            let token_derive_eth =
                helper::get_token_eth_price(&eth_prices_store, &token_addr).unwrap();
            let partial_pool_total_value_locked_eth = value.mul(token_derive_eth);
            log::info!(
                "partial pool {} token {} partial total value locked usd: {}",
                pool_addr,
                token_addr,
                partial_pool_total_value_locked_eth,
            );
            let aggregate_key = pool_addr.clone();
            if let Some(pool_agg) = pool_aggregator.get(&aggregate_key) {
                let count = &pool_agg.0;
                let rolling_sum = &pool_agg.1;
                log::info!("found another partial pool value {} token {} count {} partial total value locked usd: {}",
                    pool_addr,
                    token_addr,
                    count,
                    rolling_sum,
                );
                if count.to_i32().unwrap() >= 2 {
                    panic!(
                        "{}",
                        format!("this is unexpected should only see 2 pool keys")
                    )
                }
                let pool_total_value_locked_eth =
                    partial_pool_total_value_locked_eth.add(rolling_sum);
                let pool_total_value_locked_usd =
                    pool_total_value_locked_eth.clone().mul(&eth_price_usd);
                output.set(
                    native_total_value_locked.ordinal,
                    keyer::pool_eth_total_value_locked(&pool_addr),
                    &Vec::from(pool_total_value_locked_eth.to_string()),
                );
                output.set(
                    native_total_value_locked.ordinal,
                    keyer::pool_usd_total_value_locked(&pool_addr),
                    &Vec::from(pool_total_value_locked_usd.to_string()),
                );

                continue;
            }
            pool_aggregator.insert(
                aggregate_key.clone(),
                (1, partial_pool_total_value_locked_eth),
            );
            log::info!("partial inserted");
        }
    }
}

#[substreams::handlers::store]
pub fn store_ticks(events: Events, output_set: StoreSet) {
    for event in events.events {
        match event.r#type.unwrap() {
            SwapEvent(_) => {}
            BurnEvent(_) => {
                // todo
            }
            MintEvent(mint) => {
                let tick_lower_big_int = BigInt::from_str(&mint.tick_lower.to_string()).unwrap();
                let tick_lower_price0 = math::big_decimal_exponated(
                    BigDecimal::from_f64(1.0001).unwrap().with_prec(100),
                    tick_lower_big_int,
                );
                let tick_lower_price1 =
                    math::safe_div(&BigDecimal::from(1 as i32), &tick_lower_price0);

                let tick_lower: Tick = Tick {
                    pool_address: event.pool_address.to_string(),
                    idx: mint.tick_lower.to_string(),
                    price0: tick_lower_price0.to_string(),
                    price1: tick_lower_price1.to_string(),
                };

                output_set.set(
                    event.log_ordinal,
                    format!(
                        "tick:{}:pool:{}",
                        mint.tick_lower.to_string(),
                        event.pool_address.to_string()
                    ),
                    &proto::encode(&tick_lower).unwrap(),
                );

                let tick_upper_big_int = BigInt::from_str(&mint.tick_upper.to_string()).unwrap();
                let tick_upper_price0 = math::big_decimal_exponated(
                    BigDecimal::from_f64(1.0001).unwrap().with_prec(100),
                    tick_upper_big_int,
                );
                let tick_upper_price1 =
                    math::safe_div(&BigDecimal::from(1 as i32), &tick_upper_price0);
                let tick_upper: Tick = Tick {
                    pool_address: event.pool_address.to_string(),
                    idx: mint.tick_upper.to_string(),
                    price0: tick_upper_price0.to_string(),
                    price1: tick_upper_price1.to_string(),
                };

                output_set.set(
                    event.log_ordinal,
                    format!(
                        "tick:{}:pool:{}",
                        mint.tick_upper.to_string(),
                        event.pool_address.to_string()
                    ),
                    &proto::encode(&tick_upper).unwrap(),
                );
            }
        }
    }
}

// #[substreams::handlers::map]
// pub fn map_fees(block: ethpb::v1::Block) -> Result<pb::uniswap::Fees, Error> {
//     let mut out = pb::uniswap::Fees { fees: vec![] };
//
//     for trx in block.transaction_traces {
//         for call in trx.calls.iter() {
//             if call.state_reverted {
//                 continue;
//             }
//
//             for log in call.logs.iter() {
//                 if !abi::factory::events::FeeAmountEnabled::match_log(&log) {
//                     continue;
//                 }
//
//                 let ev = abi::factory::events::FeeAmountEnabled::decode(&log).unwrap();
//
//                 out.fees.push(pb::uniswap::Fee {
//                     fee: ev.fee.as_u32(),
//                     tick_spacing: ev.tick_spacing.to_i32().unwrap(),
//                 });
//             }
//         }
//     }
//
//     Ok(out)
// }
//
// #[substreams::handlers::store]
// pub fn store_fees(block: ethpb::v1::Block, output: store::StoreSet) {
//     for trx in block.transaction_traces {
//         for call in trx.calls.iter() {
//             if call.state_reverted {
//                 continue;
//             }
//             for log in call.logs.iter() {
//                 if !abi::factory::events::FeeAmountEnabled::match_log(&log) {
//                     continue;
//                 }
//
//                 let event = abi::factory::events::FeeAmountEnabled::decode(&log).unwrap();
//
//                 let fee = pb::uniswap::Fee {
//                     fee: event.fee.as_u32(),
//                     tick_spacing: event.tick_spacing.to_i32().unwrap(),
//                 };
//
//                 output.set(
//                     log.ordinal,
//                     format!("fee:{}:{}", fee.fee, fee.tick_spacing),
//                     &proto::encode(&fee).unwrap(),
//                 );
//             }
//         }
//     }
// }
//
// #[substreams::handlers::map]
// pub fn map_flashes(block: ethpb::v1::Block) -> Result<pb::uniswap::Flashes, Error> {
//     let mut out = pb::uniswap::Flashes { flashes: vec![] };
//
//     for trx in block.transaction_traces {
//         for call in trx.calls.iter() {
//             if call.state_reverted {
//                 continue;
//             }
//             for log in call.logs.iter() {
//                 if abi::pool::events::Swap::match_log(&log) {
//                     log::debug!("log ordinal: {}", log.ordinal);
//                 }
//                 if !abi::pool::events::Flash::match_log(&log) {
//                     continue;
//                 }
//
//                 let flash = abi::pool::events::Flash::decode(&log).unwrap();
//
//                 out.flashes.push(Flash {
//                     sender: Hex(&flash.sender).to_string(),
//                     recipient: Hex(&flash.recipient).to_string(),
//                     amount_0: flash.amount0.as_u64(),
//                     amount_1: flash.amount1.as_u64(),
//                     paid_0: flash.paid0.as_u64(),
//                     paid_1: flash.paid1.as_u64(),
//                     transaction_id: Hex(&trx.hash).to_string(),
//                     log_ordinal: log.ordinal,
//                 });
//             }
//         }
//     }
//
//     Ok(out)
// }

// NOTE:
//  pool liquidity is a 2 part process, for swaps is a set
//  and for mint and burns is an add process so it can't be
//  done in the same substreams
// todo: break this substreams in different files
#[substreams::handlers::map]
pub fn map_pool_entities(
    pools_created: Pools,
    pool_sqrt_price_deltas: store::Deltas,
    pool_liquidities_store_deltas: store::Deltas,
    total_value_locked_deltas: store::Deltas,
    total_value_locked_by_tokens_deltas: store::Deltas,
    pool_fee_growth_global_x128_deltas: store::Deltas,
    price_deltas: store::Deltas,
    tx_count_deltas: store::Deltas,
    swaps_volume_deltas: store::Deltas,
) -> Result<EntitiesChanges, Error> {
    let mut out = EntitiesChanges {
        ..Default::default()
    };

    for pool in pools_created.pools {
        let change = EntityChange {
            entity: "Pool".to_string(),
            id: string_field_value!(pool.address.as_str()),
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
        out.entity_changes.push(change);
    }

    // SqrtPrice changes
    // Note: All changes from the sqrt_price state are updates
    for pool_sqrt_price_delta in pool_sqrt_price_deltas {
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
                let old_value: PoolSqrtPrice =
                    proto::decode(&pool_sqrt_price_delta.new_value).unwrap();
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
            _ => {}
        }
        out.entity_changes.push(change)
    }

    for pool_liquidities_store_delta in pool_liquidities_store_deltas {
        let mut change = EntityChange {
            entity: "pool".to_string(),
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
        out.entity_changes.push(change)
    }

    for delta in price_deltas {
        let mut key_parts = delta.key.as_str().split(":");
        let pool_address = key_parts.nth(1).unwrap();
        let field_name: &str;
        match key_parts.next().unwrap() {
            "token0" => {
                field_name = "token0Price";
            }
            "token1" => {
                field_name = "token1Price";
            }
            _ => {
                continue;
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
            _ => {}
        }
        out.entity_changes.push(change);
    }

    for delta in tx_count_deltas {
        if !delta.key.starts_with("pool:") {
            continue;
        }

        out.entity_changes.push(EntityChange {
            entity: "Pool".to_string(),
            id: string_field_value!(delta.key.as_str().split(":").nth(1).unwrap()),
            ordinal: delta.ordinal,
            operation: Operation::Update as i32,
            fields: vec![update_field!(
                "txCount",
                FieldType::Bigint,
                big_int_field_value!(
                    BigInt::from_signed_bytes_be(delta.old_value.as_ref()).to_string()
                ),
                big_int_field_value!(
                    BigInt::from_signed_bytes_be(delta.new_value.as_ref()).to_string()
                )
            )],
        })
    }

    for delta in total_value_locked_deltas {
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
    }

    for delta in total_value_locked_by_tokens_deltas {
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
            _ => {}
        }

        out.entity_changes.push(change);
    }

    for delta in swaps_volume_deltas {
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
            _ => {}
        }
        out.entity_changes.push(change);
    }

    for delta in pool_fee_growth_global_x128_deltas {
        let mut change: EntityChange = EntityChange {
            entity: "Pool".to_string(),
            id: string_field_value!(delta.key.as_str().split(":").nth(1).unwrap()),
            ordinal: delta.ordinal,
            operation: Operation::Update as i32,
            fields: vec![],
        };

        match delta.key.as_str().split(":").nth(1).unwrap() {
            "token0" => change.fields.push(update_field!(
                "feeGrowthGlobal0X128",
                FieldType::Bigint,
                big_int_field_value!(
                    BigInt::from_signed_bytes_be(delta.old_value.as_ref()).to_string()
                ),
                big_int_field_value!(
                    BigInt::from_signed_bytes_be(delta.new_value.as_ref()).to_string()
                )
            )),
            "token1" => change.fields.push(update_field!(
                "feeGrowthGlobal1X128",
                FieldType::Bigint,
                big_int_field_value!(
                    BigInt::from_signed_bytes_be(delta.old_value.as_ref()).to_string()
                ),
                big_int_field_value!(
                    BigInt::from_signed_bytes_be(delta.new_value.as_ref()).to_string()
                )
            )),
            _ => {}
        }
    }

    Ok(out)
}

//todo: check the tickLower, tickUpper, amount, amount0, amount1 and amountUSD, for the moment
// they are stored as String values, but shouldn't it be int instead or BigInt in some cases?
#[substreams::handlers::map]
pub fn map_swaps_mints_burns_entities(
    events: Events,
    tx_count_store: StoreGet,
    store_eth_prices: StoreGet,
) -> Result<EntitiesChanges, Error> {
    let mut out = EntitiesChanges {
        block_id: vec![],
        block_number: 0,
        prev_block_id: vec![],
        prev_block_number: 0,
        entity_changes: vec![],
    };

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

            match event.r#type.unwrap() {
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

                    out.entity_changes.push(EntityChange {
                        entity: "Swap".to_string(),
                        id: string_field_value!(transaction_id),
                        ordinal: event.log_ordinal,
                        operation: Operation::Create as i32,
                        fields: vec![
                            new_field!(
                                "id",
                                FieldType::String,
                                string_field_value!(transaction_id)
                            ),
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

                    out.entity_changes.push(EntityChange {
                        entity: "Mint".to_string(),
                        id: string_field_value!(transaction_id),
                        ordinal: event.log_ordinal,
                        operation: Operation::Create as i32,
                        fields: vec![
                            new_field!(
                                "id",
                                FieldType::String,
                                string_field_value!(transaction_id)
                            ),
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
                    });
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

                    out.entity_changes.push(EntityChange {
                        entity: "Burn".to_string(),
                        id: string_field_value!(transaction_id),
                        ordinal: event.log_ordinal,
                        operation: Operation::Create as i32,
                        fields: vec![
                            new_field!(
                                "id",
                                FieldType::String,
                                string_field_value!(transaction_id)
                            ),
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
            }
        }
    }

    Ok(out)
}

#[substreams::handlers::map]
pub fn graph_out(
    block: ethpb::v1::Block,
    pool_entities: EntitiesChanges,
    swaps_mints_burns_entities: EntitiesChanges,
) -> Result<EntitiesChanges, Error> {
    let mut out = EntitiesChanges {
        block_id: block.hash,
        block_number: block.number,
        prev_block_id: block.header.unwrap().parent_hash,
        prev_block_number: block.number - 1 as u64,
        entity_changes: vec![],
    };

    //todo: check if we wand to check the block ordinal here and sort by the ordinal
    // or simply stream out all the entity changes

    for change in pool_entities.entity_changes {
        out.entity_changes.push(change);
    }

    for change in swaps_mints_burns_entities.entity_changes {
        out.entity_changes.push(change);
    }

    Ok(out)
}
