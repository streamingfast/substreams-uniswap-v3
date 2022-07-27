extern crate core;

mod abi;
mod eth;
mod macros;
mod pb;
mod rpc;
mod utils;

use bigdecimal::ToPrimitive;
use bigdecimal::{BigDecimal, FromPrimitive};
use num_bigint::BigInt;
use std::collections::HashMap;
use std::ops::Neg;
use std::str::FromStr;
use substreams::errors::Error;
use substreams::store;
use substreams::{log, proto, Hex};
use substreams_ethereum::{pb::eth as ethpb,Event as EventTrait};

use crate::pb::uniswap::entity_change::Operation;
use crate::pb::uniswap::event::Type::{Burn as BurnEvent, Mint as MintEvent, Swap as SwapEvent};
use crate::pb::uniswap::field::Type as FieldType;
use crate::pb::uniswap::{
    Burn, EntitiesChanges, EntityChange, Erc20Token, Event, Field, Flash, Mint, Pool,
    PoolInitialization, PoolInitializations, Pools, SqrtPriceUpdate, SqrtPriceUpdates, Tick,
};
use crate::utils::{big_decimal_exponated, get_last_pool_tick, safe_div};

#[substreams::handlers::map]
pub fn map_pools_created(block: ethpb::v1::Block) -> Result<Pools, Error> {
    // optimization and make sure to not add the same token twice
    // it is possible to have multiple pools created with the same
    // tokens (USDC, WETH, etc.)
    let mut cached_tokens = HashMap::new();
    let pools = block
        .events::<abi::factory::events::PoolCreated>(&[&utils::UNISWAP_V3_FACTORY])
        .filter_map(|(event, log)| {
            log::info!("pool addr: {}", Hex(event.pool));

            let mut pool: Pool = Pool {
                address: Hex(&log.data()[44..64]).to_string(),
                token0: None,
                token1: None,
                creation_transaction_id: Hex(&log.receipt.transaction.hash).to_string(),
                fee: event.fee.as_u32(),
                block_num: block.number.to_string(),
                log_ordinal: log.ordinal(),
                tick_spacing: event.tick_spacing.to_i32().unwrap(),
            };
            // check the validity of the token0 and token1
            let mut token0 = Erc20Token {
                address: "".to_string(),
                name: "".to_string(),
                symbol: "".to_string(),
                decimals: 0,
            };
            let mut token1 = Erc20Token {
                address: "".to_string(),
                name: "".to_string(),
                symbol: "".to_string(),
                decimals: 0,
            };

            let token0_address: String = Hex(&event.token0).to_string();
            if !cached_tokens.contains_key(&token0_address) {
                match rpc::create_uniswap_token(&token0_address) {
                    None => {
                        return None;
                    }
                    Some(token) => {
                        token0 = token;
                        cached_tokens.insert(String::from(&token0_address), true);
                    }
                }
            }

            let token1_address: String = Hex(&event.token1).to_string();
            if !cached_tokens.contains_key(&token1_address) {
                match rpc::create_uniswap_token(&token1_address) {
                    None => {
                        return None;
                    }
                    Some(token) => {
                        token1 = token;
                        cached_tokens.insert(String::from(&token1_address), true);
                    }
                }
            }
            pool.token0 = Some(token0.clone());
            pool.token1 = Some(token1.clone());
            Some(pool)
        }).collect();

    Ok(Pools{pools})
}

#[substreams::handlers::map]
pub fn map_pools_initialized(
    block: ethpb::v1::Block,
) -> Result<pb::uniswap::PoolInitializations, Error> {
    let mut output = pb::uniswap::PoolInitializations {
        pool_initializations: vec![],
    };
    for trx in block.transaction_traces {
        for call in trx.calls.iter() {
            if call.state_reverted {
                continue;
            }
            for log in call.logs.iter() {
                if !abi::pool::events::Initialize::match_log(&log) {
                    continue;
                }

                let event = abi::pool::events::Initialize::decode(&log).unwrap();
                output.pool_initializations.push(PoolInitialization {
                    address: Hex(&log.address).to_string(),
                    initialization_transaction_id: Hex(&trx.hash).to_string(),
                    log_ordinal: log.ordinal,
                    tick: event.tick.to_string(),
                    sqrt_price: event.sqrt_price_x96.to_string(),
                });
            }
        }
    }

    Ok(output)
}

// map will take the block and the init mapper for the pools
// mapper -> sqrt price de init et swap
#[substreams::handlers::map]
pub fn map_sqrt_price(block: ethpb::v1::Block) -> Result<SqrtPriceUpdates, Error> {
    let mut output = SqrtPriceUpdates {
        sqrt_prices: vec![],
    };

    for trx in block.transaction_traces {
        for call in trx.calls.iter() {
            if call.state_reverted {
                continue;
            }
            for log in call.logs.iter() {
                if abi::pool::events::Initialize::match_log(&log) {
                    let event = abi::pool::events::Initialize::decode(&log).unwrap();
                    output.sqrt_prices.push(SqrtPriceUpdate {
                        pool_address: Hex(&log.address).to_string(),
                        ordinal: log.ordinal,
                        sqrt_price: event.sqrt_price_x96.to_string(),
                        tick: event.tick.to_string(),
                    })
                } else if abi::pool::events::Swap::match_log(&log) {
                    let event = abi::pool::events::Swap::decode(&log).unwrap();
                    output.sqrt_prices.push(SqrtPriceUpdate {
                        pool_address: Hex(&log.address).to_string(),
                        ordinal: log.ordinal,
                        sqrt_price: event.sqrt_price_x96.to_string(),
                        tick: event.tick.to_string(),
                    })
                }
            }
        }
    }

    output.sqrt_prices.sort_by(|a, b| a.ordinal.cmp(&b.ordinal));

    Ok(output)
}

#[substreams::handlers::store]
pub fn store_sqrt_price(mut sqrt_prices: SqrtPriceUpdates, output: store::StoreSet) {
    for sqrt_price in sqrt_prices.sqrt_prices {
        // fixme: probably need to have a similar key for like we have for a swap
        output.set(
            0,
            format!("sqrt_price:{}", sqrt_price.pool_address),
            &proto::encode(&sqrt_price).unwrap(),
        )
    }
}

/// Keyspace
///     pool_init:{pool_init.address} -> stores an encoded value of the pool_init
#[substreams::handlers::store]
pub fn store_pools_initialization(
    pools: pb::uniswap::PoolInitializations,
    output_set: store::StoreSet,
) {
    for init in pools.pool_initializations {
        output_set.set(
            1,
            format!("pool_init:{}", init.address),
            &proto::encode(&init).unwrap(),
        );
    }
}

/// Keyspace
///     pool:{pool.address} -> stores an encoded value of the pool
///     tokens:{}:{} (token0:token1 or token1:token0) -> stores an encoded value of the pool
#[substreams::handlers::store]
pub fn store_pools(pools: pb::uniswap::Pools, output: store::StoreSet) {
    for pool in pools.pools {
        output.set(
            pool.log_ordinal,
            format!("pool:{}", pool.address),
            &proto::encode(&pool).unwrap(),
        );
        output.set(
            pool.log_ordinal,
            format!(
                "tokens:{}",
                utils::generate_tokens_key(
                    pool.token0.as_ref().unwrap().address.as_str(),
                    pool.token1.as_ref().unwrap().address.as_str(),
                )
            ),
            &proto::encode(&pool).unwrap(),
        )
    }
}

#[substreams::handlers::map]
pub fn map_burns_swaps_mints(
    block: ethpb::v1::Block,
    pools_store: store::StoreGet,
) -> Result<pb::uniswap::Events, Error> {
    let mut output = pb::uniswap::Events { events: vec![] };
    for trx in block.transaction_traces {
        for call in trx.calls.iter() {
            if call.state_reverted {
                continue;
            }
            for log in call.logs.iter() {
                let pool_key = &format!("pool:{}", Hex(&log.address).to_string());

                if abi::pool::events::Swap::match_log(log) {
                    let swap = abi::pool::events::Swap::decode(log).unwrap();
                    match pools_store.get_last(pool_key) {
                        None => {
                            panic!(
                                "invalid swap. pool does not exist. pool address {} transaction {}",
                                Hex(&log.address).to_string(),
                                Hex(&trx.hash).to_string()
                            );
                        }
                        Some(pool_bytes) => {
                            let pool: Pool = proto::decode(&pool_bytes).unwrap();
                            let token0 = pool.token0.as_ref().unwrap();
                            let token1 = pool.token1.as_ref().unwrap();

                            let amount0 =
                                utils::convert_token_to_decimal(&swap.amount0, token0.decimals);
                            let amount1 =
                                utils::convert_token_to_decimal(&swap.amount1, token1.decimals);
                            log::debug!("amount0: {}, amount1:{}", amount0, amount1);

                            output.events.push(Event {
                                log_ordinal: log.ordinal,
                                pool_address: pool.address.to_string(),
                                token0: pool.token0.as_ref().unwrap().address.to_string(),
                                token1: pool.token1.as_ref().unwrap().address.to_string(),
                                fee: pool.fee.to_string(),
                                transaction_id: Hex(&trx.hash).to_string(),
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
                                    amount_0: amount0.to_string(), // big_decimal?
                                    amount_1: amount1.to_string(), // big_decimal?
                                    sqrt_price: swap.sqrt_price_x96.to_string(),
                                    liquidity: swap.liquidity.to_string(),
                                    tick: swap.tick.to_i32().unwrap(),
                                })),
                            });
                        }
                    }
                }

                if abi::pool::events::Burn::match_log(log) {
                    let burn = abi::pool::events::Burn::decode(log).unwrap();

                    match pools_store.get_last(pool_key) {
                        None => {
                            panic!(
                                "invalid burn. pool does not exist. pool address {} transaction {}",
                                Hex(&log.address).to_string(),
                                Hex(&trx.hash).to_string()
                            );
                        }
                        Some(pool_bytes) => {
                            let pool: Pool = proto::decode(&pool_bytes).unwrap();
                            let token0 = pool.token0.as_ref().unwrap();
                            let token1 = pool.token1.as_ref().unwrap();

                            let amount0_bi =
                                BigInt::from_str(burn.amount0.to_string().as_str()).unwrap();
                            let amount1_bi =
                                BigInt::from_str(burn.amount1.to_string().as_str()).unwrap();
                            let amount0 =
                                utils::convert_token_to_decimal(&amount0_bi, token0.decimals);
                            let amount1 =
                                utils::convert_token_to_decimal(&amount1_bi, token1.decimals);
                            log::debug!("amount0: {}, amount1:{}", amount0, amount1);

                            output.events.push(Event {
                                log_ordinal: log.ordinal,
                                pool_address: pool.address.to_string(),
                                token0: pool.token0.as_ref().unwrap().address.to_string(),
                                token1: pool.token1.as_ref().unwrap().address.to_string(),
                                fee: pool.fee.to_string(),
                                transaction_id: Hex(&trx.hash).to_string(),
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
                                    amount_0: amount0.to_string(),
                                    amount_1: amount1.to_string(),
                                    tick_lower: burn.tick_lower.to_i32().unwrap(),
                                    tick_upper: burn.tick_upper.to_i32().unwrap(),
                                    amount: burn.amount.to_string(),
                                })),
                            });
                        }
                    }
                }

                if abi::pool::events::Mint::match_log(log) {
                    let mint = abi::pool::events::Mint::decode(log).unwrap();

                    match pools_store.get_last(pool_key) {
                        None => {
                            panic!(
                                "invalid mint. pool does not exist. pool address {} transaction {}",
                                Hex(&log.address).to_string(),
                                Hex(&trx.hash).to_string()
                            );
                        }
                        Some(pool_bytes) => {
                            let pool: Pool = proto::decode(&pool_bytes).unwrap();
                            let token0 = pool.token0.as_ref().unwrap();
                            let token1 = pool.token1.as_ref().unwrap();

                            let amount0_bi =
                                BigInt::from_str(mint.amount0.to_string().as_str()).unwrap();
                            let amount1_bi =
                                BigInt::from_str(mint.amount1.to_string().as_str()).unwrap();
                            let amount0 =
                                utils::convert_token_to_decimal(&amount0_bi, token0.decimals);
                            let amount1 =
                                utils::convert_token_to_decimal(&amount1_bi, token1.decimals);
                            log::debug!("amount0: {}, amount1:{}", amount0, amount1);

                            output.events.push(Event {
                                log_ordinal: log.ordinal,
                                pool_address: pool.address.to_string(),
                                token0: pool.token0.unwrap().address,
                                token1: pool.token1.unwrap().address,
                                fee: pool.fee.to_string(),
                                transaction_id: Hex(&trx.hash).to_string(),
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
                                    amount: mint.amount.to_string(), // big_decimal?
                                    amount_0: amount0.to_string(),
                                    amount_1: amount1.to_string(),
                                    tick_lower: mint.tick_lower.to_i32().unwrap(),
                                    tick_upper: mint.tick_upper.to_i32().unwrap(),
                                })),
                            });
                        }
                    }
                }
            }
        }
    }

    Ok(output)
}

#[substreams::handlers::store]
pub fn store_swaps(events: pb::uniswap::Events, output: store::StoreSet) {
    for event in events.events {
        match event.r#type.unwrap() {
            SwapEvent(swap) => {
                output.set(
                    0,
                    format!("swap:{}:{}", event.transaction_id, event.log_ordinal),
                    &proto::encode(&swap).unwrap(),
                );
            }
            _ => {}
        }
    }
}

#[substreams::handlers::store]
pub fn store_ticks(events: pb::uniswap::Events, output_set: store::StoreSet) {
    for event in events.events {
        match event.r#type.unwrap() {
            SwapEvent(_) => {}
            BurnEvent(_) => {
                // todo
            }
            MintEvent(mint) => {
                let tick_lower_big_int = BigInt::from_str(&mint.tick_lower.to_string()).unwrap();
                let tick_lower_price0 = big_decimal_exponated(
                    BigDecimal::from_f64(1.0001).unwrap().with_prec(100),
                    tick_lower_big_int,
                );
                let tick_lower_price1 = safe_div(&BigDecimal::from(1 as i32), &tick_lower_price0);

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
                let tick_upper_price0 = big_decimal_exponated(
                    BigDecimal::from_f64(1.0001).unwrap().with_prec(100),
                    tick_upper_big_int,
                );
                let tick_upper_price1 = safe_div(&BigDecimal::from(1 as i32), &tick_upper_price0);
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

/// Keyspace:
///
///    liquidity:{pool_address} =>
///    total_value_locked:{tokenA}:{tokenB} => 0.1231 (tokenA total value locked, summed for all pools dealing with tokenA:tokenB, in floating point decimal taking tokenA's decimals in consideration).
///
#[substreams::handlers::store]
pub fn store_liquidity(
    events: pb::uniswap::Events,
    swap_store: store::StoreGet,
    pool_init_store: store::StoreGet,
    output: store::StoreAddBigFloat,
) {
    for event in events.events {
        log::debug!("transaction id: {}", event.transaction_id);
        if event.r#type.is_none() {
            continue;
        }

        if event.r#type.is_some() {
            match event.r#type.unwrap() {
                BurnEvent(burn) => {
                    let amount = BigDecimal::from_str(burn.amount.as_str()).unwrap();
                    let amount0 = BigDecimal::from_str(burn.amount_0.as_str()).unwrap();
                    let amount1 = BigDecimal::from_str(burn.amount_1.as_str()).unwrap();
                    let tick_lower =
                        BigDecimal::from_str(burn.tick_lower.to_string().as_str()).unwrap();
                    let tick_upper =
                        BigDecimal::from_str(burn.tick_upper.to_string().as_str()).unwrap();
                    let tick = get_last_pool_tick(
                        &pool_init_store,
                        &swap_store,
                        &event.pool_address,
                        &event.transaction_id,
                        event.log_ordinal,
                    )
                    .unwrap();

                    if tick_lower <= tick && tick <= tick_upper {
                        output.add(
                            event.log_ordinal,
                            format!("liquidity:{}", event.pool_address),
                            &amount.neg(),
                        );
                    }

                    output.add(
                        event.log_ordinal,
                        format!("total_value_locked:{}:{}", event.token0, event.token1),
                        &amount0.neg(),
                    );
                    output.add(
                        event.log_ordinal,
                        format!(
                            "total_value_locked:{}:{}",
                            event.token1, event.token0 /* FIXME: triple check that */
                        ),
                        &amount1.neg(),
                    );
                }
                MintEvent(mint) => {
                    let amount = BigDecimal::from_str(mint.amount.as_str()).unwrap();
                    let amount0 = BigDecimal::from_str(mint.amount_0.as_str()).unwrap();
                    let amount1 = BigDecimal::from_str(mint.amount_1.as_str()).unwrap();
                    let tick_lower =
                        BigDecimal::from_str(mint.tick_lower.to_string().as_str()).unwrap();
                    let tick_upper =
                        BigDecimal::from_str(mint.tick_upper.to_string().as_str()).unwrap();
                    let tick = get_last_pool_tick(
                        &pool_init_store,
                        &swap_store,
                        &event.pool_address,
                        &event.transaction_id,
                        event.log_ordinal,
                    )
                    .unwrap();

                    if tick_lower <= tick && tick <= tick_upper {
                        output.add(
                            event.log_ordinal,
                            format!("liquidity:{}", event.pool_address),
                            &amount,
                        );
                    }

                    output.add(
                        event.log_ordinal,
                        format!("total_value_locked:{}:{}", event.pool_address, event.token0),
                        &amount0,
                    );
                    output.add(
                        event.log_ordinal,
                        format!("total_value_locked:{}:{}", event.pool_address, event.token1),
                        &amount1,
                    );
                }
                SwapEvent(_) => {}
            }
        }
    }
}

/// Keyspace
///     price:{token0_addr}:{token1_addr} -> stores the tokens price 0 for token 1
///     price:{token1_addr}:{token0_addr} -> stores the tokens price 1 for token 0
#[substreams::handlers::store]
pub fn store_prices(
    sqrt_price_updates: SqrtPriceUpdates,
    pools_store: store::StoreGet,
    output: store::StoreSet,
) {
    for sqrt_price_update in sqrt_price_updates.sqrt_prices {
        let pool =
            utils::get_last_pool(&pools_store, sqrt_price_update.pool_address.as_str()).unwrap();

        let token0 = pool.token0.as_ref().unwrap();
        let token1 = pool.token1.as_ref().unwrap();
        log::info!(
            "pool addr: {}, token 0 addr: {}, token 1 addr: {}",
            pool.address,
            token0.address,
            token1.address
        );

        let sqrt_price = BigDecimal::from_str(sqrt_price_update.sqrt_price.as_str()).unwrap();
        log::info!("sqrtPrice: {}", sqrt_price.to_string());

        let tokens_price: (BigDecimal, BigDecimal) =
            utils::sqrt_price_x96_to_token_prices(&sqrt_price, &token0, &token1);
        log::debug!("token prices: {} {}", tokens_price.0, tokens_price.1);

        output.set(
            sqrt_price_update.ordinal,
            format!("pool:{}:token0:{}:price", pool.address, token0.address),
            &Vec::from(tokens_price.0.to_string()),
        );
        output.set(
            sqrt_price_update.ordinal,
            format!("pool:{}:token1:{}:price", pool.address, token1.address),
            &Vec::from(tokens_price.1.to_string()),
        );

        output.set(
            sqrt_price_update.ordinal,
            format!(
                "price:{}:{}",
                pool.token0.as_ref().unwrap().address,
                pool.token1.as_ref().unwrap().address
            ),
            &Vec::from(tokens_price.0.to_string()),
        );
        output.set(
            sqrt_price_update.ordinal,
            format!(
                "price:{}:{}",
                pool.token1.as_ref().unwrap().address,
                pool.token0.as_ref().unwrap().address
            ),
            &Vec::from(tokens_price.1.to_string()),
        );
        // perhaps? set `sqrt_price:{pair}:{tokenA} => 0.123`
        // We did that in Uniswap v2, we'll see if useful in the future.
    }
}

/// Keyspace
///     token:{token0_addr}:dprice:eth -> stores the derived eth price per token0 price
///     token:{token1_addr}:dprice:eth -> stores the derived eth price per token1 price
#[substreams::handlers::store]
pub fn store_derived_eth_prices(
    sqrt_price_updates: SqrtPriceUpdates,
    pools_store: store::StoreGet,
    prices_store: store::StoreGet,
    liquidity_store: store::StoreGet,
    output: store::StoreSet,
) {
    for sqrt_price_update in sqrt_price_updates.sqrt_prices {
        log::debug!("fetching pool: {}", sqrt_price_update.pool_address);
        log::debug!("sqrt_price: {}", sqrt_price_update.sqrt_price);
        let pool =
            utils::get_last_pool(&pools_store, sqrt_price_update.pool_address.as_str()).unwrap();
        let token_0 = pool.token0.as_ref().unwrap();
        let token_1 = pool.token1.as_ref().unwrap();

        log::info!(
            "token 0 addr: {}, token 0 decimals: {}, tokens 0 symbol: {}, token 0 name: {}",
            token_0.address,
            token_0.decimals,
            token_0.symbol,
            token_0.name
        );
        log::info!(
            "token 1 addr: {}, token 1 decimals: {}, tokens 1 symbol: {}, token 1 name: {}",
            token_1.address,
            token_1.decimals,
            token_1.symbol,
            token_1.name
        );

        let token0_derived_eth_price = utils::find_eth_per_token(
            sqrt_price_update.ordinal,
            &pool.address,
            &token_0.address.as_str(),
            &liquidity_store,
            &prices_store,
        );
        log::info!("token0_derived_eth_price: {}", token0_derived_eth_price);

        let token1_derived_eth_price = utils::find_eth_per_token(
            sqrt_price_update.ordinal,
            &pool.address,
            &token_1.address.as_str(),
            &liquidity_store,
            &prices_store,
        );
        log::info!("token1_derived_eth_price: {}", token1_derived_eth_price);

        output.set(
            sqrt_price_update.ordinal,
            format!("token:{}:dprice:eth", token_0.address),
            &Vec::from(token0_derived_eth_price.to_string()),
        );
        output.set(
            sqrt_price_update.ordinal,
            format!("token:{}:dprice:eth", token_1.address),
            &Vec::from(token1_derived_eth_price.to_string()),
        );
    }
}

#[substreams::handlers::map]
pub fn map_fees(block: ethpb::v1::Block) -> Result<pb::uniswap::Fees, Error> {
    let mut out = pb::uniswap::Fees { fees: vec![] };

    for trx in block.transaction_traces {
        for call in trx.calls.iter() {
            if call.state_reverted {
                continue;
            }

            for log in call.logs.iter() {
                if !abi::factory::events::FeeAmountEnabled::match_log(&log) {
                    continue;
                }

                let ev = abi::factory::events::FeeAmountEnabled::decode(&log).unwrap();

                out.fees.push(pb::uniswap::Fee {
                    fee: ev.fee.as_u32(),
                    tick_spacing: ev.tick_spacing.to_i32().unwrap(),
                });
            }
        }
    }

    Ok(out)
}

#[substreams::handlers::store]
pub fn store_fees(block: ethpb::v1::Block, output: store::StoreSet) {
    for trx in block.transaction_traces {
        for call in trx.calls.iter() {
            if call.state_reverted {
                continue;
            }
            for log in call.logs.iter() {
                if !abi::factory::events::FeeAmountEnabled::match_log(&log) {
                    continue;
                }

                let event = abi::factory::events::FeeAmountEnabled::decode(&log).unwrap();

                let fee = pb::uniswap::Fee {
                    fee: event.fee.as_u32(),
                    tick_spacing: event.tick_spacing.to_i32().unwrap(),
                };

                output.set(
                    log.ordinal,
                    format!("fee:{}:{}", fee.fee, fee.tick_spacing),
                    &proto::encode(&fee).unwrap(),
                );
            }
        }
    }
}

#[substreams::handlers::map]
pub fn map_flashes(block: ethpb::v1::Block) -> Result<pb::uniswap::Flashes, Error> {
    let mut out = pb::uniswap::Flashes { flashes: vec![] };

    for trx in block.transaction_traces {
        for call in trx.calls.iter() {
            if call.state_reverted {
                continue;
            }
            for log in call.logs.iter() {
                if abi::pool::events::Swap::match_log(&log) {
                    log::debug!("log ordinal: {}", log.ordinal);
                }
                if !abi::pool::events::Flash::match_log(&log) {
                    continue;
                }

                let flash = abi::pool::events::Flash::decode(&log).unwrap();

                out.flashes.push(Flash {
                    sender: Hex(&flash.sender).to_string(),
                    recipient: Hex(&flash.recipient).to_string(),
                    amount_0: flash.amount0.as_u64(),
                    amount_1: flash.amount1.as_u64(),
                    paid_0: flash.paid0.as_u64(),
                    paid_1: flash.paid1.as_u64(),
                    transaction_id: Hex(&trx.hash).to_string(),
                    log_ordinal: log.ordinal,
                });
            }
        }
    }

    Ok(out)
}

#[substreams::handlers::map]
fn map_pool_entities(
    pools_created: Pools,
    pool_inits: PoolInitializations,
    sqrt_price_deltas: store::Deltas,
    liquidity_deltas: store::Deltas,
    price_deltas: store::Deltas,
) -> Result<EntitiesChanges, Error> {
    let mut out = EntitiesChanges {
        entity_changes: vec![],
    };

    for pool in pools_created.pools {
        let change = EntityChange {
            entity: "pool".to_string(),
            id: string_field_value!(pool.address.as_str()),
            ordinal: pool.log_ordinal,
            operation: Operation::Create as i32,
            fields: vec![
                new_field!(
                    "address",
                    FieldType::String,
                    string_field_value!(pool.address)
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
                    "creation_transaction_id",
                    FieldType::String,
                    string_field_value!(pool.creation_transaction_id)
                ),
                new_field!("fee_tier", FieldType::Int, int_field_value!(pool.fee)),
                new_field!(
                    "block_num",
                    FieldType::String,
                    string_field_value!(pool.block_num)
                ),
                new_field!(
                    "log_ordinal",
                    FieldType::Int,
                    int_field_value!(pool.log_ordinal)
                ),
                new_field!(
                    "tick_spacing",
                    FieldType::Int,
                    int_field_value!(pool.tick_spacing)
                ),
            ],
        };
        out.entity_changes.push(change);
    }

    for pool_init in pool_inits.pool_initializations {
        let change = EntityChange {
            entity: "pool".to_string(),
            id: string_field_value!(pool_init.address.as_str()),
            ordinal: pool_init.log_ordinal,
            operation: Operation::Update as i32,
            fields: vec![
                new_field!(
                    "sqrt_price",
                    FieldType::Bigdecimal,
                    big_decimal_string_field_value!(pool_init.sqrt_price)
                ),
                new_field!(
                    "tick",
                    FieldType::Bigdecimal,
                    big_int_field_value!(pool_init.tick)
                ),
            ],
        };
        out.entity_changes.push(change);
    }

    // SqrtPrice changes
    // Note: All changes from the sqrt_price state are updates
    for delta in sqrt_price_deltas {
        if !delta.key.starts_with("pool:") {
            continue;
        }

        let new_value: SqrtPriceUpdate = proto::decode(&delta.new_value).unwrap();

        let mut change = EntityChange {
            entity: "pool".to_string(),
            id: string_field_value!(delta.key.as_str().split(":").nth(1).unwrap()),
            ordinal: delta.ordinal,
            operation: Operation::Update as i32,
            fields: vec![],
        };
        match delta.operation {
            1 => {
                change.fields.push(update_field!(
                    "sqrt_price",
                    FieldType::Bigdecimal,
                    big_decimal_string_field_value!("0".to_string()),
                    big_decimal_string_field_value!(new_value.sqrt_price)
                ));
                change.fields.push(update_field!(
                    "tick",
                    FieldType::Bigint,
                    big_decimal_string_field_value!("0".to_string()),
                    big_decimal_string_field_value!(new_value.sqrt_price)
                ));
            }
            2 => {
                let old_value: SqrtPriceUpdate = proto::decode(&delta.new_value).unwrap();
                change.fields.push(update_field!(
                    "sqrt_price",
                    FieldType::Bigdecimal,
                    big_decimal_string_field_value!(old_value.sqrt_price),
                    big_decimal_string_field_value!(new_value.sqrt_price)
                ));
                change.fields.push(update_field!(
                    "tick",
                    FieldType::Bigint,
                    big_int_field_value!(old_value.tick),
                    big_decimal_string_field_value!(new_value.tick)
                ));
            }
            _ => {}
        }
        out.entity_changes.push(change)
    }

    // Liquidity changes
    // Note: All changes from the liquidity state are updates.
    for delta in liquidity_deltas {
        if !delta.key.starts_with("liquidity:") {
            continue;
        }

        let mut change = EntityChange {
            entity: "pool".to_string(),
            id: string_field_value!(delta.key.as_str().split(":").nth(1).unwrap()),
            ordinal: delta.ordinal,
            operation: Operation::Update as i32,
            fields: vec![],
        };
        match delta.operation {
            1 => {
                change.fields.push(update_field!(
                    "liquidity",
                    FieldType::Bigdecimal,
                    big_decimal_string_field_value!("0".to_string()),
                    big_decimal_vec_field_value!(delta.new_value)
                ));
            }
            2 => {
                change.fields.push(update_field!(
                    "liquidity",
                    FieldType::Bigdecimal,
                    big_decimal_vec_field_value!(delta.old_value),
                    big_decimal_vec_field_value!(delta.new_value)
                ));
            }
            _ => {}
        }
        out.entity_changes.push(change);
    }

    // pool token price changes from the price state.
    // Note: All changes from the price state are updates
    for delta in price_deltas {
        //get the token prices from the price state
        if !delta.key.starts_with("pool:") {
            continue;
        }

        let mut key_parts = delta.key.as_str().split(":");
        let pool_address = key_parts.nth(1).unwrap();
        let field_name: &str;
        match key_parts.next().unwrap() {
            "token0" => {
                field_name = "token0_price";
            }
            "token1" => {
                field_name = "token1_price";
            }
            _ => {
                continue;
            }
        }

        let mut change = EntityChange {
            entity: "pool".to_string(),
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

    Ok(out)
}

#[substreams::handlers::map]
pub fn graph_out(pool_entities: EntitiesChanges) -> Result<EntitiesChanges, Error> {
    let mut out = EntitiesChanges {
        entity_changes: vec![],
    };

    for change in pool_entities.entity_changes {
        out.entity_changes.push(change);
    }

    Ok(out)
}
