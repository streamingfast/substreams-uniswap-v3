extern crate core;

mod abi;
mod db;
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
use crate::ethpb::v2::{Block, StorageChange};
use crate::keyer::{native_pool_from_key, position};
use crate::pb::position_event::PositionEventType;
use crate::pb::uniswap::entity_change::Operation;
use crate::pb::uniswap::event::Type::{Burn as BurnEvent, Mint as MintEvent, Swap as SwapEvent};
use crate::pb::uniswap::field::Type as FieldType;
use crate::pb::uniswap::tick::Origin::{Burn, Mint};
use crate::pb::uniswap::tick::Type::{Lower, Upper};
use crate::pb::uniswap::{
    EntitiesChanges, EntityChange, Erc20Token, Erc20Tokens, Event, EventAmount, Events, Field,
    Pool, PoolLiquidities, PoolLiquidity, PoolSqrtPrice, PoolSqrtPrices, Pools, Tick, Ticks,
};
use crate::pb::{uniswap, PositionEvent};
use crate::price::WHITELIST_TOKENS;
use crate::uniswap::position::PositionType;
use crate::uniswap::position::PositionType::{
    Collect, DecreaseLiquidity, IncreaseLiquidity, Transfer,
};
use crate::uniswap::{
    Position, Positions, SnapshotPosition, SnapshotPositions, Transaction, Transactions,
};
use crate::utils::{NON_FUNGIBLE_POSITION_MANAGER, UNISWAP_V3_FACTORY, ZERO_ADDRESS};
use bigdecimal::ToPrimitive;
use bigdecimal::{BigDecimal, FromPrimitive};
use ethabi::Token::Uint;
use num_bigint::BigInt;
use std::collections::HashMap;
use std::ops::{Add, Div, Mul, Neg, Sub};
use std::str::FromStr;
use substreams::errors::Error;
use substreams::pb::substreams::StoreDeltas;
use substreams::store;
use substreams::store::{StoreAddBigFloat, StoreAddBigInt, StoreAppend, StoreGet, StoreSet};
use substreams::{log, proto, Hex};
use substreams_ethereum::{pb::eth as ethpb, Event as EventTrait};

#[substreams::handlers::map]
pub fn map_pools_created(block: Block) -> Result<Pools, Error> {
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
                total_supply: "".to_string(),
                whitelist_pools: vec![],
            };
            let mut token1 = Erc20Token {
                address: "".to_string(),
                name: "".to_string(),
                symbol: "".to_string(),
                decimals: 0,
                total_supply: "".to_string(),
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

            let token0_total_supply: BigInt = rpc::token_total_supply_call(&token0_address);
            token0.total_supply = token0_total_supply.to_string();

            let token1_total_supply: BigInt = rpc::token_total_supply_call(&token1_address);
            token1.total_supply = token1_total_supply.to_string();

            pool.token0 = Some(token0.clone());
            pool.token1 = Some(token1.clone());
            pools.push(pool);
        }
    }
    Ok(Pools { pools })
}

#[substreams::handlers::store]
pub fn store_pools(pools: Pools, output: StoreSet) {
    for pool in pools.pools {
        output.set(
            pool.log_ordinal,
            keyer::pool_key(&pool.address),
            &proto::encode(&pool).unwrap(),
        );

        // and issue occurs here, should we not set the key to index:token0:token1:fee?
        // so we have all the pools
        output.set(
            pool.log_ordinal,
            keyer::pool_token_index_key(
                &pool.token0.as_ref().unwrap().address,
                &pool.token1.as_ref().unwrap().address,
                &pool.fee_tier.to_string(),
            ),
            &proto::encode(&pool).unwrap(),
        );
    }
}

#[substreams::handlers::store]
pub fn store_pool_count(pools: Pools, output: StoreAddBigInt) {
    for pool in pools.pools {
        output.add(
            pool.log_ordinal,
            keyer::factory_pool_count_key(),
            &BigInt::from(1 as i32),
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
            log::info!(
                "log addr: {}",
                Hex(&log.receipt.transaction.hash.as_slice()).to_string()
            );
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
            log::info!(
                "log addr: {}",
                Hex(&log.receipt.transaction.hash.as_slice()).to_string()
            );
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
                    log::debug!("swap - trx_id: {}", Hex(&trx.hash).to_string());
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
                    log::debug!("mint - trx_id: {}", Hex(&trx.hash).to_string());
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
                    log::debug!("burn - trx_id: {}", Hex(&trx.hash).to_string());
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
pub fn store_pool_liquidities(pool_liquidities: PoolLiquidities, output: StoreSet) {
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
pub fn store_prices(pool_sqrt_prices: PoolSqrtPrices, pools_store: StoreGet, output: StoreSet) {
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
                    "pool addr: {}, pool trx_id: {}, token 0 addr: {}, token 1 addr: {}",
                    pool.address,
                    pool.transaction_id,
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
                    keyer::prices_pool_token_key(
                        &pool.address,
                        &token0.address,
                        "token0".to_string(),
                    ),
                    &Vec::from(tokens_price.0.to_string()),
                );
                output.set(
                    sqrt_price_update.ordinal,
                    keyer::prices_pool_token_key(
                        &pool.address,
                        &token1.address,
                        "token1".to_string(),
                    ),
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
pub fn map_swaps_mints_burns(block: Block, pools_store: StoreGet) -> Result<Events, Error> {
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
                        transaction_id: Hex(&log.receipt.transaction.hash).to_string(),
                        timestamp: block
                            .header
                            .as_ref()
                            .unwrap()
                            .timestamp
                            .as_ref()
                            .unwrap()
                            .seconds as u64,
                        created_at_block_number: block.number,
                        r#type: Some(SwapEvent(uniswap::Swap {
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
                        created_at_block_number: block.number,
                        r#type: Some(MintEvent(uniswap::Mint {
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
                        created_at_block_number: block.number,
                        r#type: Some(BurnEvent(uniswap::Burn {
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
pub fn map_event_amounts(events: Events) -> Result<uniswap::EventAmounts, Error> {
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
                    event_amounts.push(EventAmount {
                        pool_address: event.pool_address,
                        log_ordinal: event.log_ordinal,
                        token0_addr: event.token0,
                        amount0_value: amount0.neg().to_string(),
                        token1_addr: event.token1,
                        amount1_value: amount1.neg().to_string(),
                        ..Default::default()
                    });
                }
                MintEvent(mint) => {
                    log::debug!("handling mint for pool {}", event.pool_address);
                    let amount0 = BigDecimal::from_str(mint.amount_0.as_str()).unwrap();
                    let amount1 = BigDecimal::from_str(mint.amount_1.as_str()).unwrap();
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

#[substreams::handlers::map]
pub fn map_transactions(block: Block, pools_store: StoreGet) -> Result<Transactions, Error> {
    let mut transactions: Transactions = Transactions {
        transactions: vec![],
    };

    for log in block.logs() {
        let mut add_transaction = false;
        let pool_key = &format!("pool:{}", Hex(&log.address()).to_string());

        if let Some(_) = abi::pool::events::Burn::match_and_decode(log) {
            match pools_store.get_last(pool_key) {
                None => {
                    log::info!(
                        "invalid burn. pool does not exist. pool address {} transaction {}",
                        Hex(&log.address()).to_string(),
                        Hex(&log.receipt.transaction.hash).to_string()
                    );
                    continue;
                }
                Some(_) => add_transaction = true,
            }
        } else if let Some(_) = abi::pool::events::Mint::match_and_decode(log) {
            match pools_store.get_last(pool_key) {
                None => {
                    log::info!(
                        "invalid mint. pool does not exist. pool address {} transaction {}",
                        Hex(&log.address()).to_string(),
                        Hex(&log.receipt.transaction.hash).to_string()
                    );
                    continue;
                }
                Some(_) => add_transaction = true,
            }
        } else if let Some(_) = abi::pool::events::Swap::match_and_decode(log) {
            match pools_store.get_last(pool_key) {
                None => {
                    log::info!(
                        "invalid swap. pool does not exist. pool address {} transaction {}",
                        Hex(&log.address()).to_string(),
                        Hex(&log.receipt.transaction.hash).to_string()
                    );
                    continue;
                }
                Some(_) => add_transaction = true,
            }
        } else if let Some(_) =
            abi::positionmanager::events::IncreaseLiquidity::match_and_decode(log)
        {
            if log.address() == NON_FUNGIBLE_POSITION_MANAGER {
                add_transaction = true;
            }
        } else if let Some(_) = abi::positionmanager::events::Collect::match_and_decode(log) {
            if log.address() == NON_FUNGIBLE_POSITION_MANAGER {
                add_transaction = true;
            }
        } else if let Some(_) =
            abi::positionmanager::events::DecreaseLiquidity::match_and_decode(log)
        {
            if log.address() == NON_FUNGIBLE_POSITION_MANAGER {
                add_transaction = true;
            }
        } else if let Some(_) = abi::positionmanager::events::Transfer::match_and_decode(log) {
            if log.address() == NON_FUNGIBLE_POSITION_MANAGER {
                add_transaction = true;
            }
        }

        if add_transaction {
            transactions.transactions.push(utils::load_transaction(
                block.number,
                block
                    .header
                    .as_ref()
                    .unwrap()
                    .timestamp
                    .as_ref()
                    .unwrap()
                    .seconds as u64,
                log.ordinal(),
                log.receipt.transaction,
            ));
        }
    }

    Ok(transactions)
}

#[substreams::handlers::store]
pub fn store_totals(
    store_eth_prices: StoreGet,
    total_value_locked_deltas: store::Deltas,
    output: StoreAddBigFloat,
) {
    let mut pool_total_value_locked_eth_new_value: BigDecimal = BigDecimal::from(0);
    for delta in total_value_locked_deltas {
        if !delta.key.starts_with("pool:") {
            continue;
        }
        match delta.key.as_str().split(":").last().unwrap() {
            "eth" => {
                let pool_total_value_locked_eth_old_value: BigDecimal =
                    math::decimal_from_bytes(&delta.old_value);
                pool_total_value_locked_eth_new_value = math::decimal_from_bytes(&delta.new_value);

                let pool_total_value_locked_eth_diff: BigDecimal =
                    pool_total_value_locked_eth_old_value
                        .sub(pool_total_value_locked_eth_new_value.clone());

                output.add(
                    delta.ordinal,
                    keyer::factory_total_value_locked_eth(),
                    &pool_total_value_locked_eth_diff,
                )
            }
            "usd" => {
                let bundle_eth_price: BigDecimal = match store_eth_prices.get_last("bundle") {
                    Some(price) => math::decimal_from_bytes(&price),
                    None => continue,
                };
                log::debug!("eth_price_usd: {}", bundle_eth_price);

                let total_value_locked_usd: BigDecimal = pool_total_value_locked_eth_new_value
                    .clone()
                    .mul(bundle_eth_price);

                // here we have to do a hackish way to set the value, to not have to
                // create a new store which would do the same but that would set the
                // value instead of summing it, what we do is calculate the difference
                // and simply add/sub the difference and that mimics the same as setting
                // the value
                let total_value_locked_usd_old_value: BigDecimal =
                    math::decimal_from_bytes(&delta.old_value);
                let diff: BigDecimal = total_value_locked_usd.sub(total_value_locked_usd_old_value);

                output.add(
                    delta.ordinal,
                    keyer::factory_total_value_locked_usd(),
                    &diff,
                );
            }
            _ => continue,
        }
    }
}

#[substreams::handlers::store]
pub fn store_total_tx_counts(events: Events, output: StoreAddBigInt) {
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
                    let eth_price_in_usd = helper::get_eth_price(&store_eth_prices).unwrap();

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

                    log::info!("trx_id: {}", event.transaction_id);
                    log::info!("bundle.ethPriceUSD: {}", eth_price_in_usd);
                    log::info!("token0_derived_eth_price: {}", token0_derived_eth_price);
                    log::info!("token1_derived_eth_price: {}", token1_derived_eth_price);
                    log::info!("amount0_abs: {}", amount0_abs);
                    log::info!("amount1_abs: {}", amount1_abs);

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

                    let amount_total_usd_untracked: BigDecimal = amount0_abs
                        .clone()
                        .add(amount1_abs.clone())
                        .div(BigDecimal::from(2 as i32));

                    let fee_tier: BigDecimal = BigDecimal::from(pool.fee_tier);
                    let fee_usd: BigDecimal = amount_total_usd_tracked
                        .clone()
                        .mul(fee_tier.clone())
                        .div(BigDecimal::from(1000000 as u64));
                    let fee_eth: BigDecimal = amount_total_eth_tracked
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
                    );
                    output.add(
                        event.log_ordinal,
                        keyer::swap_token_volume(&event.token0, "token0".to_string()),
                        &amount0_abs,
                    );
                    output.add(
                        event.log_ordinal,
                        keyer::swap_token_volume(&event.token1, "token1".to_string()),
                        &amount1_abs,
                    );
                    output.add(
                        event.log_ordinal,
                        keyer::swap_token_volume_usd(&event.token0),
                        &amount_total_usd_tracked,
                    );
                    output.add(
                        event.log_ordinal,
                        keyer::swap_token_volume_usd(&event.token1),
                        &amount_total_usd_tracked,
                    );
                    output.add(
                        event.log_ordinal,
                        keyer::swap_token_volume_untracked_volume_usd(&event.token0),
                        &amount_total_usd_untracked,
                    );
                    output.add(
                        event.log_ordinal,
                        keyer::swap_token_volume_untracked_volume_usd(&event.token1),
                        &amount_total_usd_untracked,
                    );
                    output.add(
                        event.log_ordinal,
                        keyer::swap_token_fee_usd(&event.token0),
                        &fee_usd,
                    );
                    output.add(
                        event.log_ordinal,
                        keyer::swap_token_fee_usd(&event.token1),
                        &fee_usd,
                    );
                    output.add(
                        event.log_ordinal,
                        keyer::swap_factory_total_volume_eth(),
                        &amount_total_eth_tracked,
                    );
                    output.add(
                        event.log_ordinal,
                        keyer::swap_factory_total_fees_eth(),
                        &fee_eth,
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
        let (bd0, bd1) = rpc::fee_growth_global_x128_call(&pool.address);
        log::debug!("big decimal0: {}", bd0);
        log::debug!("big decimal1: {}", bd1);

        output.set(
            pool.log_ordinal,
            keyer::pool_fee_growth_global_x128(&pool.address, "token0".to_string()),
            &Vec::from(bd0.to_string().as_str()),
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
    pool_liquidities_store: StoreGet,
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
        log::info!("trx_id: {}", event.transaction_id);
        let mut amount0: BigDecimal = BigDecimal::from(0 as i32);
        let mut amount1: BigDecimal = BigDecimal::from(0 as i32);

        match event.r#type.unwrap() {
            BurnEvent(burn) => {
                amount0 = BigDecimal::from_str(burn.amount_0.as_str()).unwrap().neg();
                amount1 = BigDecimal::from_str(burn.amount_1.as_str()).unwrap().neg();
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
            keyer::total_value_locked_by_tokens(
                &event.pool_address,
                &event.token0,
                "token0".to_string(),
            ),
            &amount0,
        );
        output.add(
            event.log_ordinal,
            keyer::total_value_locked_by_tokens(
                &event.pool_address,
                &event.token1,
                "token1".to_string(),
            ),
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

    // fixme: are we sure we want to unwrap and fail here ? we can't even go over the first block..
    // let eth_price_usd = helper::get_eth_price(&eth_prices_store).unwrap();

    for native_total_value_locked in native_total_value_locked_deltas {
        let eth_price_usd: BigDecimal = match &eth_prices_store.get_last(&keyer::bundle_eth_price())
        {
            None => continue,
            Some(bytes) => math::decimal_from_bytes(&bytes),
        };
        log::debug!(
            "eth_price_usd: {}, native_total_value_locked.key: {}",
            eth_price_usd,
            native_total_value_locked.key
        );
        if let Some(token_addr) = keyer::native_token_from_key(&native_total_value_locked.key) {
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
            let value: BigDecimal = math::decimal_from_bytes(&native_total_value_locked.new_value);
            let token_derive_eth: BigDecimal =
                helper::get_token_eth_price(&eth_prices_store, &token_addr).unwrap();
            let partial_pool_total_value_locked_eth = value.mul(token_derive_eth);
            log::info!(
                "partial pool {} token {} partial total value locked usd: {}",
                pool_addr,
                token_addr,
                partial_pool_total_value_locked_eth,
            );
            let aggregate_key = pool_addr.clone();

            //fixme: @julien: it seems we never actually enter here... as it would only be valid if we have
            // twice a valid event on the same pool
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

                log::info!(
                    "partial_pool_total_value_locked_eth: {} and rolling_sum: {}",
                    partial_pool_total_value_locked_eth,
                    rolling_sum,
                );
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

#[substreams::handlers::map]
pub fn map_ticks(events: Events) -> Result<Ticks, Error> {
    let mut out: Ticks = Ticks { ticks: vec![] };
    for event in events.events {
        match event.r#type.unwrap() {
            BurnEvent(burn) => {
                log::debug!("burn event transaction_id: {}", event.transaction_id);
                let lower_tick_id: String =
                    format!("{}#{}", &event.pool_address, burn.tick_lower.to_string());
                let lower_tick_idx = BigInt::from_str(&burn.tick_lower.to_string()).unwrap();
                let lower_tick_price0 = math::big_decimal_exponated(
                    BigDecimal::from_f64(1.0001).unwrap().with_prec(100),
                    lower_tick_idx.clone(),
                );
                let lower_tick_price1 =
                    math::safe_div(&BigDecimal::from(1 as i32), &lower_tick_price0);

                let tick_result = rpc::fee_growth_outside_x128_call(
                    &event.pool_address,
                    &lower_tick_idx.to_string(),
                );

                let tick_lower: Tick = Tick {
                    id: lower_tick_id,
                    pool_address: event.pool_address.to_string(),
                    idx: burn.tick_lower.to_string(),
                    price0: lower_tick_price0.to_string(),
                    price1: lower_tick_price1.to_string(),
                    created_at_timestamp: event.timestamp,
                    created_at_block_number: event.created_at_block_number,
                    fee_growth_outside_0x_128: tick_result.0.to_string(),
                    fee_growth_outside_1x_128: tick_result.1.to_string(),
                    log_ordinal: event.log_ordinal,
                    amount: burn.amount.clone(),
                    r#type: Lower as i32,
                    origin: Burn as i32,
                };

                let upper_tick_id: String =
                    format!("{}#{}", &event.pool_address, burn.tick_upper.to_string());
                let upper_tick_idx = BigInt::from_str(&burn.tick_upper.to_string()).unwrap();
                let upper_tick_price0 = math::big_decimal_exponated(
                    BigDecimal::from_f64(1.0001).unwrap().with_prec(100),
                    upper_tick_idx.clone(),
                );
                let upper_upper_price1 =
                    math::safe_div(&BigDecimal::from(1 as i32), &upper_tick_price0);

                let tick_result = rpc::fee_growth_outside_x128_call(
                    &event.pool_address,
                    &upper_tick_idx.to_string(),
                );

                let tick_upper: Tick = Tick {
                    id: upper_tick_id,
                    pool_address: event.pool_address.to_string(),
                    idx: burn.tick_upper.to_string(),
                    price0: upper_tick_price0.to_string(),
                    price1: upper_upper_price1.to_string(),
                    created_at_timestamp: event.timestamp,
                    created_at_block_number: event.created_at_block_number,
                    fee_growth_outside_0x_128: tick_result.0.to_string(),
                    fee_growth_outside_1x_128: tick_result.1.to_string(),
                    log_ordinal: event.log_ordinal,
                    amount: burn.amount,
                    r#type: Upper as i32,
                    origin: Burn as i32,
                };

                out.ticks.push(tick_lower);
                out.ticks.push(tick_upper);
            }
            MintEvent(mint) => {
                log::debug!("mint event transaction_id: {}", event.transaction_id);
                let lower_tick_id: String =
                    format!("{}#{}", &event.pool_address, mint.tick_lower.to_string());
                let lower_tick_idx = BigInt::from_str(&mint.tick_lower.to_string()).unwrap();
                let lower_tick_price0 = math::big_decimal_exponated(
                    BigDecimal::from_f64(1.0001).unwrap().with_prec(100),
                    lower_tick_idx.clone(),
                );
                let lower_tick_price1 =
                    math::safe_div(&BigDecimal::from(1 as i32), &lower_tick_price0);

                let tick_result = rpc::fee_growth_outside_x128_call(
                    &event.pool_address,
                    &lower_tick_idx.to_string(),
                );

                // in the subgraph, there is a `load` which is done to see if the tick
                // exists and if it doesn't exist, createTick()
                let tick_lower: Tick = Tick {
                    id: lower_tick_id,
                    pool_address: event.pool_address.to_string(),
                    idx: mint.tick_lower.to_string(),
                    price0: lower_tick_price0.to_string(),
                    price1: lower_tick_price1.to_string(),
                    created_at_timestamp: event.timestamp,
                    created_at_block_number: event.created_at_block_number,
                    fee_growth_outside_0x_128: tick_result.0.to_string(),
                    fee_growth_outside_1x_128: tick_result.1.to_string(),
                    log_ordinal: event.log_ordinal,
                    amount: mint.amount.clone(),
                    r#type: Lower as i32,
                    origin: Mint as i32,
                };

                let upper_tick_id: String =
                    format!("{}#{}", &event.pool_address, mint.tick_upper.to_string());
                let upper_tick_idx = BigInt::from_str(&mint.tick_upper.to_string()).unwrap();
                let upper_tick_price0 = math::big_decimal_exponated(
                    BigDecimal::from_f64(1.0001).unwrap().with_prec(100),
                    upper_tick_idx.clone(),
                );
                let upper_tick_price1 =
                    math::safe_div(&BigDecimal::from(1 as i32), &upper_tick_price0);

                let tick_result = rpc::fee_growth_outside_x128_call(
                    &event.pool_address,
                    &upper_tick_idx.to_string(),
                );

                let tick_upper: Tick = Tick {
                    id: upper_tick_id,
                    pool_address: event.pool_address.to_string(),
                    idx: mint.tick_upper.to_string(),
                    price0: upper_tick_price0.to_string(),
                    price1: upper_tick_price1.to_string(),
                    created_at_timestamp: event.timestamp,
                    created_at_block_number: event.created_at_block_number,
                    fee_growth_outside_0x_128: tick_result.0.to_string(),
                    fee_growth_outside_1x_128: tick_result.1.to_string(),
                    log_ordinal: event.log_ordinal,
                    amount: mint.amount,
                    r#type: Upper as i32,
                    origin: Mint as i32,
                };

                out.ticks.push(tick_lower);
                out.ticks.push(tick_upper);
            }
            _ => {}
        }
    }
    Ok(out)
}

#[substreams::handlers::store]
pub fn store_ticks(ticks: Ticks, output: StoreSet) {
    for tick in ticks.ticks {
        output.set(
            tick.log_ordinal,
            keyer::ticks(&tick.id),
            &proto::encode(&tick).unwrap(),
        );
    }
}

#[substreams::handlers::store]
pub fn store_ticks_liquidities(ticks: Ticks, output: StoreAddBigInt) {
    for tick in ticks.ticks {
        log::info!("tick id: {}", tick.id);
        if tick.origin == Mint as i32 {
            if tick.r#type == Lower as i32 {
                output.add(
                    tick.log_ordinal,
                    keyer::tick_liquidities_net(&tick.id),
                    &BigInt::from_str(tick.amount.as_str()).unwrap(),
                );
            } else {
                // upper
                output.add(
                    tick.log_ordinal,
                    keyer::tick_liquidities_net(&tick.id),
                    &BigInt::from_str(tick.amount.as_str()).unwrap().neg(),
                );
            }

            output.add(
                tick.log_ordinal,
                keyer::tick_liquidities_gross(&tick.id),
                &BigInt::from_str(tick.amount.as_str()).unwrap(),
            );
        } else if tick.origin == Burn as i32 {
            if tick.r#type == Lower as i32 {
                output.add(
                    tick.log_ordinal,
                    keyer::tick_liquidities_net(&tick.id),
                    &BigInt::from_str(tick.amount.as_str()).unwrap().neg(),
                );
            } else {
                // upper
                output.add(
                    tick.log_ordinal,
                    keyer::tick_liquidities_net(&tick.id),
                    &BigInt::from_str(tick.amount.as_str()).unwrap(),
                );
            }

            output.add(
                tick.log_ordinal,
                keyer::tick_liquidities_gross(&tick.id),
                &BigInt::from_str(tick.amount.as_str()).unwrap().neg(),
            );
        }
    }
}

#[substreams::handlers::map]
pub fn map_all_positions(block: Block, store_pool: StoreGet) -> Result<Positions, Error> {
    let mut positions: Positions = Positions { positions: vec![] };

    for log in block.logs() {
        if log.address() != NON_FUNGIBLE_POSITION_MANAGER {
            continue;
        }

        if let Some(event) = abi::positionmanager::events::IncreaseLiquidity::match_and_decode(log)
        {
            if let Some(position) = utils::get_position(
                &store_pool,
                &Hex(log.address()).to_string(),
                &log.receipt.transaction.hash,
                IncreaseLiquidity,
                log.ordinal(),
                block
                    .header
                    .as_ref()
                    .unwrap()
                    .timestamp
                    .as_ref()
                    .unwrap()
                    .seconds as u64,
                block.number,
                PositionEvent {
                    event: PositionEventType::IncreaseLiquidity(event),
                },
            ) {
                positions.positions.push(position);
            }
        } else if let Some(event) = abi::positionmanager::events::Collect::match_and_decode(log) {
            if let Some(position) = utils::get_position(
                &store_pool,
                &Hex(log.address()).to_string(),
                &log.receipt.transaction.hash,
                Collect,
                log.ordinal(),
                block
                    .header
                    .as_ref()
                    .unwrap()
                    .timestamp
                    .as_ref()
                    .unwrap()
                    .seconds as u64,
                block.number,
                PositionEvent {
                    event: PositionEventType::Collect(event),
                },
            ) {
                positions.positions.push(position);
            }
        } else if let Some(event) =
            abi::positionmanager::events::DecreaseLiquidity::match_and_decode(log)
        {
            if let Some(position) = utils::get_position(
                &store_pool,
                &Hex(log.address()).to_string(),
                &log.receipt.transaction.hash,
                DecreaseLiquidity,
                log.ordinal(),
                block
                    .header
                    .as_ref()
                    .unwrap()
                    .timestamp
                    .as_ref()
                    .unwrap()
                    .seconds as u64,
                block.number,
                PositionEvent {
                    event: PositionEventType::DecreaseLiquidity(event),
                },
            ) {
                positions.positions.push(position);
            }
        } else if let Some(event) = abi::positionmanager::events::Transfer::match_and_decode(log) {
            if let Some(position) = utils::get_position(
                &store_pool,
                &Hex(log.address()).to_string(),
                &log.receipt.transaction.hash,
                Transfer,
                log.ordinal(),
                block
                    .header
                    .as_ref()
                    .unwrap()
                    .timestamp
                    .as_ref()
                    .unwrap()
                    .seconds as u64,
                block.number,
                PositionEvent {
                    event: PositionEventType::Transfer(event.clone()),
                },
            ) {
                positions.positions.push(position);
            }
        }
    }

    Ok(positions)
}

#[substreams::handlers::store]
pub fn store_all_positions(positions: Positions, output: StoreSet) {
    for position in positions.positions {
        output.set(
            position.log_ordinal,
            keyer::all_position(
                &position.id,
                &PositionType::get_position_type(position.position_type).to_string(),
            ),
            &proto::encode(&position).unwrap(),
        )
    }
}

#[substreams::handlers::map]
pub fn map_positions(block: Block, all_positions_store: StoreGet) -> Result<Positions, Error> {
    let mut positions: Positions = Positions { positions: vec![] };
    let mut ordered_positions: Vec<String> = vec![];
    let mut enriched_positions: HashMap<String, Position> = HashMap::new();

    for log in block.logs() {
        let mut position: Position = Position {
            ..Default::default()
        };
        if log.address() != NON_FUNGIBLE_POSITION_MANAGER {
            continue;
        }

        if let Some(event) = abi::positionmanager::events::IncreaseLiquidity::match_and_decode(log)
        {
            let token_id: String = event.token_id.to_string();
            if !enriched_positions.contains_key(&token_id) {
                match all_positions_store.get_last(keyer::all_position(
                    &token_id,
                    &IncreaseLiquidity.to_string(),
                )) {
                    None => {
                        log::info!("increase liquidity for id {} doesn't exist", token_id);
                        continue;
                    }
                    Some(bytes) => {
                        position = proto::decode(&bytes).unwrap();
                        enriched_positions.insert(token_id.clone(), position);
                        if !ordered_positions.contains(&String::from(token_id.clone())) {
                            ordered_positions.push(String::from(token_id))
                        }
                    }
                }
            }
        } else if let Some(event) = abi::positionmanager::events::Collect::match_and_decode(log) {
            let token_id: String = event.token_id.to_string();
            if !enriched_positions.contains_key(&token_id) {
                match all_positions_store
                    .get_last(keyer::all_position(&token_id, &Collect.to_string()))
                {
                    None => {
                        log::info!("increase liquidity for id {} doesn't exist", token_id);
                        continue;
                    }
                    Some(bytes) => {
                        position = proto::decode(&bytes).unwrap();
                    }
                }
            } else {
                position = enriched_positions
                    .remove(&event.token_id.to_string())
                    .unwrap()
            }

            if let Some(position_call_result) =
                rpc::positions_call(&Hex(log.address()).to_string(), event.token_id)
            {
                position.fee_growth_inside_0_last_x_128 = position_call_result.5.to_string();
                position.fee_growth_inside_1_last_x_128 = position_call_result.6.to_string();
                enriched_positions.insert(token_id.clone(), position);
                if !ordered_positions.contains(&String::from(token_id.clone())) {
                    ordered_positions.push(String::from(token_id))
                }
            }
        } else if let Some(event) =
            abi::positionmanager::events::DecreaseLiquidity::match_and_decode(log)
        {
            let token_id: String = event.token_id.to_string();
            if !enriched_positions.contains_key(&token_id) {
                match all_positions_store.get_last(keyer::all_position(
                    &event.token_id.to_string(),
                    &DecreaseLiquidity.to_string(),
                )) {
                    None => {
                        log::info!("increase liquidity for id {} doesn't exist", token_id);
                        continue;
                    }
                    Some(bytes) => {
                        position = proto::decode(&bytes).unwrap();
                        enriched_positions.insert(token_id.clone(), position);
                        if !ordered_positions.contains(&String::from(token_id.clone())) {
                            ordered_positions.push(String::from(token_id))
                        }
                    }
                }
            }
        } else if let Some(event) = abi::positionmanager::events::Transfer::match_and_decode(log) {
            let token_id: String = event.token_id.to_string();
            if !enriched_positions.contains_key(&token_id) {
                match all_positions_store
                    .get_last(keyer::all_position(&token_id, &Transfer.to_string()))
                {
                    None => {
                        log::info!("increase liquidity for id {} doesn't exist", token_id);
                        continue;
                    }
                    Some(bytes) => {
                        position = proto::decode(&bytes).unwrap();
                    }
                }
            } else {
                position = enriched_positions.remove(&token_id).unwrap();
            }
            position.owner = Hex(event.to.as_slice()).to_string();
            enriched_positions.insert(token_id.clone(), position);
            if !ordered_positions.contains(&String::from(token_id.clone())) {
                ordered_positions.push(String::from(token_id))
            }
        }
    }

    log::info!("len of map: {}", enriched_positions.len());
    for element in ordered_positions.iter() {
        let pos = enriched_positions.remove(element);
        if pos.is_some() {
            positions.positions.push(pos.unwrap());
        }
    }

    Ok(positions)
}

// todo: maybe this can be omitted, if not used anywhere else
#[substreams::handlers::store]
pub fn store_positions(positions: Positions, output: StoreSet) {
    for position in positions.positions {
        output.set(
            position.log_ordinal,
            keyer::position(&position.id),
            &proto::encode(&position).unwrap(),
        )
    }
}

#[substreams::handlers::store]
pub fn store_position_changes(all_positions: Positions, output: StoreAddBigFloat) {
    for position in all_positions.positions {
        match position.position_type {
            x if x == IncreaseLiquidity as i32 => {
                output.add(
                    position.log_ordinal,
                    keyer::position_liquidity(&position.id),
                    &BigDecimal::from_str(position.liquidity.as_str()).unwrap(),
                );
                output.add(
                    position.log_ordinal,
                    keyer::position_deposited_token(&position.id, "Token0"),
                    &BigDecimal::from_str(position.amount0.as_str()).unwrap(),
                );
                output.add(
                    position.log_ordinal,
                    keyer::position_deposited_token(&position.id, "Token1"),
                    &BigDecimal::from_str(position.amount1.as_str()).unwrap(),
                );
            }
            x if x == DecreaseLiquidity as i32 => {
                output.add(
                    position.log_ordinal,
                    keyer::position_liquidity(&position.id),
                    &BigDecimal::from_str(position.liquidity.as_str())
                        .unwrap()
                        .neg(),
                );
                output.add(
                    position.log_ordinal,
                    keyer::position_withdrawn_token(&position.id, "Token0"),
                    &BigDecimal::from_str(position.amount0.as_str()).unwrap(),
                );
                output.add(
                    position.log_ordinal,
                    keyer::position_withdrawn_token(&position.id, "Token1"),
                    &BigDecimal::from_str(position.amount1.as_str()).unwrap(),
                );
            }
            x if x == Collect as i32 => {
                output.add(
                    position.log_ordinal,
                    keyer::position_collected_fees_token(&position.id, "Token0"),
                    &BigDecimal::from_str(position.amount0.as_str()).unwrap(),
                );
                output.add(
                    position.log_ordinal,
                    keyer::position_collected_fees_token(&position.id, "Token1"),
                    &BigDecimal::from_str(position.amount1.as_str()).unwrap(),
                );
            }
            _ => {}
        }
    }
}

#[substreams::handlers::map]
pub fn map_snapshot_positions(
    positions: Positions,
    position_changes_store: StoreGet,
) -> Result<SnapshotPositions, Error> {
    let mut snapshot_positions: SnapshotPositions = SnapshotPositions {
        snapshot_positions: vec![],
    };

    for position in positions.positions {
        let mut snapshot_position: SnapshotPosition = SnapshotPosition {
            id: format!("{}#{}", position.id, position.block_number),
            owner: position.owner,
            pool: position.pool,
            position: position.id.clone(),
            block_number: position.block_number,
            timestamp: position.timestamp,
            transaction: position.transaction,
            liquidity: "".to_string(),
            deposited_token0: "".to_string(),
            deposited_token1: "".to_string(),
            withdrawn_token0: "".to_string(),
            withdrawn_token1: "".to_string(),
            collected_fees_token0: "".to_string(),
            collected_fees_token1: "".to_string(),
            fee_growth_inside_0_last_x_128: position.fee_growth_inside_0_last_x_128,
            fee_growth_inside_1_last_x_128: position.fee_growth_inside_1_last_x_128,
        };

        match position_changes_store.get_last(keyer::position_liquidity(&position.id)) {
            Some(bytes) => {
                snapshot_position.liquidity = utils::decode_bytes_to_big_decimal(bytes).to_string();
            }
            _ => {}
        }

        match position_changes_store
            .get_last(keyer::position_deposited_token(&position.id, "Token0"))
        {
            Some(bytes) => {
                snapshot_position.deposited_token0 =
                    utils::decode_bytes_to_big_decimal(bytes).to_string();
            }
            _ => {}
        }

        match position_changes_store
            .get_last(keyer::position_deposited_token(&position.id, "Token1"))
        {
            Some(bytes) => {
                snapshot_position.deposited_token1 =
                    utils::decode_bytes_to_big_decimal(bytes).to_string();
            }
            _ => {}
        }

        match position_changes_store
            .get_last(keyer::position_withdrawn_token(&position.id, "Token0"))
        {
            Some(bytes) => {
                snapshot_position.withdrawn_token0 =
                    utils::decode_bytes_to_big_decimal(bytes).to_string();
            }
            _ => {}
        }

        match position_changes_store
            .get_last(keyer::position_withdrawn_token(&position.id, "Token1"))
        {
            Some(bytes) => {
                snapshot_position.withdrawn_token1 =
                    utils::decode_bytes_to_big_decimal(bytes).to_string();
            }
            _ => {}
        }

        match position_changes_store
            .get_last(keyer::position_collected_fees_token(&position.id, "Token0"))
        {
            Some(bytes) => {
                snapshot_position.collected_fees_token0 =
                    utils::decode_bytes_to_big_decimal(bytes).to_string();
            }
            _ => {}
        }

        match position_changes_store
            .get_last(keyer::position_collected_fees_token(&position.id, "Token1"))
        {
            Some(bytes) => {
                snapshot_position.collected_fees_token1 =
                    utils::decode_bytes_to_big_decimal(bytes).to_string();
            }
            _ => {}
        }

        snapshot_positions
            .snapshot_positions
            .push(snapshot_position);
    }

    Ok(snapshot_positions)
}

// #[substreams::handlers::map]
// pub fn map_fees(block: ethpb::v2::Block) -> Result<pb::uniswap::Fees, Error> {
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
// pub fn store_fees(block: ethpb::v2::Block, output: store::StoreSet) {
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
// pub fn map_flashes(block: ethpb::v2::Block) -> Result<pb::uniswap::Flashes, Error> {
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

#[substreams::handlers::map]
pub fn map_bundle_entities(
    block: Block,
    derived_eth_prices_deltas: store::Deltas,
) -> Result<EntitiesChanges, Error> {
    let mut out = EntitiesChanges {
        ..Default::default()
    };

    if block.number == 12369621 {
        out.entity_changes
            .push(db::bundle_created_bundle_entity_change())
    }

    for delta in derived_eth_prices_deltas {
        if let Some(change) = db::bundle_store_eth_price_usd_bundle_entity_change(delta) {
            out.entity_changes.push(change);
        }
    }

    Ok(out)
}

#[substreams::handlers::map]
pub fn map_factory_entities(
    block: Block,
    pool_count_deltas: store::Deltas,
    tx_count_deltas: store::Deltas,
    swaps_volume_deltas: store::Deltas,
    totals_deltas: store::Deltas,
) -> Result<EntitiesChanges, Error> {
    let mut out = EntitiesChanges {
        ..Default::default()
    };

    if block.number == 12369621 {
        out.entity_changes
            .push(db::factory_created_factory_entity_change());
    }

    for delta in pool_count_deltas {
        out.entity_changes
            .push(db::pool_created_factory_entity_change(delta))
    }

    for delta in tx_count_deltas {
        if let Some(change) = db::tx_count_factory_entity_change(delta) {
            out.entity_changes.push(change);
        }
    }

    for delta in swaps_volume_deltas {
        if let Some(change) = db::swap_volume_factory_entity_change(delta) {
            out.entity_changes.push(change);
        }
    }

    for delta in totals_deltas {
        if let Some(change) = db::total_value_locked_factory_entity_change(delta) {
            out.entity_changes.push(change);
        }
    }

    Ok(out)
}

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
        out.entity_changes
            .push(db::pools_created_pool_entity_change(pool));
    }

    for delta in pool_sqrt_price_deltas {
        if let Some(change) = db::pool_sqrt_price_entity_change(delta) {
            out.entity_changes.push(change);
        }
    }

    for delta in pool_liquidities_store_deltas {
        out.entity_changes
            .push(db::pool_liquidities_pool_entity_change(delta))
    }

    for delta in total_value_locked_deltas {
        if let Some(change) = db::total_value_locked_pool_entity_change(delta) {
            out.entity_changes.push(change);
        }
    }

    for delta in total_value_locked_by_tokens_deltas {
        if let Some(change) = db::total_value_locked_by_token_pool_entity_change(delta) {
            out.entity_changes.push(change);
        }
    }

    for delta in pool_fee_growth_global_x128_deltas {
        if let Some(change) = db::pool_fee_growth_global_x128_entity_change(delta) {
            out.entity_changes.push(change);
        }
    }

    for delta in price_deltas {
        if let Some(change) = db::price_pool_entity_change(delta) {
            out.entity_changes.push(change);
        }
    }

    for delta in tx_count_deltas {
        if let Some(change) = db::tx_count_pool_entity_change(delta) {
            out.entity_changes.push(change);
        }
    }

    for delta in swaps_volume_deltas {
        if let Some(change) = db::swap_volume_pool_entity_change(delta) {
            out.entity_changes.push(change);
        }
    }

    Ok(out)
}

#[substreams::handlers::map]
pub fn map_tokens_entities(
    pools_created: Pools,
    swaps_volume_deltas: store::Deltas,
    tx_count_deltas: store::Deltas,
    total_value_locked_by_deltas: store::Deltas,
    total_value_locked_deltas: store::Deltas,
    derived_eth_prices_deltas: store::Deltas,
) -> Result<EntitiesChanges, Error> {
    let mut out = EntitiesChanges {
        block_id: vec![],
        block_number: 0,
        prev_block_id: vec![],
        prev_block_number: 0,
        entity_changes: vec![],
    };

    //todo: when a pool is created, we also save the token
    // (id, name, symbol, decimals and total supply)
    // issue here is what if we have multiple pools with t1-t2, t1-t3, t1-t4, etc.
    // we will have t1 generate multiple entity changes for nothings since it has
    // already been emitted -- subgraph doesn't solve this either
    for pool in pools_created.pools {
        out.entity_changes
            .append(&mut db::tokens_created_token_entity_change(pool));
    }

    for delta in swaps_volume_deltas {
        if let Some(change) = db::swap_volume_token_entity_change(delta) {
            out.entity_changes.push(change);
        }
    }

    for delta in tx_count_deltas {
        if let Some(change) = db::tx_count_token_entity_change(delta) {
            out.entity_changes.push(change);
        }
    }

    for delta in total_value_locked_by_deltas {
        out.entity_changes
            .push(db::total_value_locked_by_token_token_entity_change(delta))
    }

    for delta in total_value_locked_deltas {
        if let Some(change) = db::total_value_locked_usd_token_entity_change(delta) {
            out.entity_changes.push(change);
        }
    }

    for delta in derived_eth_prices_deltas {
        if let Some(change) = db::derived_eth_prices_token_entity_change(delta) {
            out.entity_changes.push(change);
        }
    }

    Ok(out)
}

#[substreams::handlers::map]
pub fn map_tick_entities(
    ticks_deltas: store::Deltas,
    ticks_liquidities_deltas: store::Deltas,
) -> Result<EntitiesChanges, Error> {
    let mut out: EntitiesChanges = EntitiesChanges {
        ..Default::default()
    };

    for tick in ticks_deltas {
        let mut old_tick = Tick {
            ..Default::default()
        };
        let new_tick: Tick = proto::decode(&tick.new_value).unwrap();

        if tick.old_value.len() != 0 {
            old_tick = proto::decode(&tick.old_value).unwrap();
            out.entity_changes
                .push(db::ticks_updated_tick_entity_change(old_tick, new_tick));
        } else {
            // no old tick, so we have a new tick
            if new_tick.origin == Mint as i32 {
                out.entity_changes
                    .push(db::ticks_created_tick_entity_change(new_tick));
            }
        }
    }

    for delta in ticks_liquidities_deltas {
        if let Some(change) = db::ticks_liquidities_tick_entity_change(delta) {
            out.entity_changes.push(change);
        }
    }

    Ok(out)
}

#[substreams::handlers::map]
pub fn map_positions_entities(
    positions: Positions,
    positions_changes_deltas: store::Deltas,
) -> Result<EntitiesChanges, Error> {
    let mut out = EntitiesChanges {
        block_id: vec![],
        block_number: 0,
        prev_block_id: vec![],
        prev_block_number: 0,
        entity_changes: vec![],
    };

    for position in positions.positions {
        out.entity_changes
            .push(db::position_create_entity_change(position));
    }

    for delta in positions_changes_deltas {
        if let Some(change) = db::positions_changes_entity_change(delta) {
            out.entity_changes.push(change);
        }
    }

    Ok(out)
}

#[substreams::handlers::map]
pub fn map_snapshot_positions_entities(
    snapshot_positions: SnapshotPositions,
) -> Result<EntitiesChanges, Error> {
    let mut out = EntitiesChanges {
        block_id: vec![],
        block_number: 0,
        prev_block_id: vec![],
        prev_block_number: 0,
        entity_changes: vec![],
    };

    for snapshot_position in snapshot_positions.snapshot_positions {
        out.entity_changes
            .push(db::snapshot_position_entity_change(snapshot_position));
    }

    Ok(out)
}

#[substreams::handlers::map]
pub fn map_transaction_entities(transactions: Transactions) -> Result<EntitiesChanges, Error> {
    let mut out = EntitiesChanges {
        block_id: vec![],
        block_number: 0,
        prev_block_id: vec![],
        prev_block_number: 0,
        entity_changes: vec![],
    };

    for transaction in transactions.transactions {
        out.entity_changes
            .push(db::transaction_entity_change(transaction))
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
        if let Some(change) =
            db::swaps_mints_burns_created_entity_change(event, &tx_count_store, &store_eth_prices)
        {
            out.entity_changes.push(change);
        }
    }

    Ok(out)
}

#[substreams::handlers::map]
pub fn graph_out(
    block: Block,
    pool_entities: EntitiesChanges,
    token_entities: EntitiesChanges,
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

    for change in token_entities.entity_changes {
        out.entity_changes.push(change);
    }

    for change in swaps_mints_burns_entities.entity_changes {
        out.entity_changes.push(change);
    }

    Ok(out)
}
