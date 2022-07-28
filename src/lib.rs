extern crate core;

mod abi;
mod eth;
mod macros;
mod pb;
mod rpc;
mod utils;
mod keyer;

use bigdecimal::{Num, ToPrimitive};
use bigdecimal::{BigDecimal, FromPrimitive};
use num_bigint::BigInt;
use std::collections::HashMap;
use std::ops::Neg;
use std::str::FromStr;
use prost::length_delimiter_len;
use substreams::errors::Error;
use substreams::{log_debug, store};
use substreams::{log, proto, Hex};
use substreams_ethereum::{pb::eth as ethpb,Event as EventTrait};
use crate::ethpb::v1::BlockHeader;

use crate::pb::uniswap::entity_change::Operation;
use crate::pb::uniswap::event::Type::{Burn as BurnEvent, Mint as MintEvent, Swap as SwapEvent};
use crate::pb::uniswap::field::Type as FieldType;
use crate::pb::uniswap::{Burn, EntitiesChanges, EntityChange, Erc20Token, Event, Field, Flash, Liquidity, Mint, Pool, Pools, PoolSqrtPrice, PoolSqrtPrices, Tick};
use crate::utils::{big_decimal_exponated, get_last_pool_sqrt_price, safe_div};

#[substreams::handlers::map]
pub fn map_pools_created(block: ethpb::v1::Block) -> Result<Pools, Error> {
    // optimization and make sure to not add the same token twice
    // it is possible to have multiple pools created with the same
    // tokens (USDC, WETH, etc.)
    let mut cached_tokens = HashMap::new();
    let pools = block
        .events::<abi::factory::events::PoolCreated>(&[&utils::UNISWAP_V3_FACTORY])
        .filter_map(|(event, log)| {
            log::info!("pool addr: {}", Hex(&event.pool));
            let mut pool: Pool = Pool {
                address: Hex(&log.data()[44..64]).to_string(),
                token0: None,
                token1: None,
                transaction_id: Hex(&log.receipt.transaction.hash).to_string(),
                created_at_block_number: block.number.to_string(),
                created_at_timestamp: block.header.as_ref().unwrap().timestamp.as_ref().unwrap().seconds.to_string(),
                fee_tier: event.fee.as_u32(),
                tick_spacing: event.tick_spacing.to_i32().unwrap(),
                log_ordinal: log.ordinal(),
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
pub fn map_pool_sqrt_price(block: ethpb::v1::Block) -> Result<PoolSqrtPrices, Error> {
    let mut pool_sqrt_prices= vec![];
    for log in block.logs() {
        if let Some(event) = abi::pool::events::Initialize::match_and_decode(log) {
            pool_sqrt_prices.push(PoolSqrtPrice {
                pool_address: Hex(log.address()).to_string(),
                ordinal: log.ordinal(),
                sqrt_price: event.sqrt_price_x96.to_string(),
                tick: event.tick.to_string(),
            });
        } else if  let Some(event) = abi::pool::events::Swap::match_and_decode(log) {
            pool_sqrt_prices.push(PoolSqrtPrice {
                pool_address: Hex(log.address()).to_string(),
                ordinal: log.ordinal(),
                sqrt_price: event.sqrt_price_x96.to_string(),
                tick: event.tick.to_string(),
            });
        }
    }
    Ok(pb::uniswap::PoolSqrtPrices{ pool_sqrt_prices })
}

/// Keyspace
///     sqrt_price:{pool.address} -> stores an encoded value of the pool
#[substreams::handlers::store]
pub fn store_pool_sqrt_price(mut sqrt_prices: PoolSqrtPrices, output: store::StoreSet) {
    for sqrt_price in sqrt_prices.pool_sqrt_prices {
        // fixme: probably need to have a similar key for like we have for a swap
        output.set(
            0,
            keyer::pool_sqrt_price_key(&sqrt_price.pool_address),
            &proto::encode(&sqrt_price).unwrap(),
        )
    }
}

/// Keyspace
///     price:{token0_addr}:{token1_addr} -> stores the tokens price 0 for token 1
///     price:{token1_addr}:{token0_addr} -> stores the tokens price 1 for token 0
#[substreams::handlers::store]
pub fn store_prices(
    pool_sqrt_prices: PoolSqrtPrices,
    pools_store: store::StoreGet,
    output: store::StoreSet,
) {
    for sqrt_price_update in pool_sqrt_prices.pool_sqrt_prices {
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

#[substreams::handlers::map]
pub fn map_swap_mints_burns(
    block: ethpb::v1::Block,
    pools_store: store::StoreGet,
) -> Result<pb::uniswap::Events, Error> {
    let mut events= vec![];
    for log in block.logs() {
        let pool_key = &format!("pool:{}", Hex(&log.address()).to_string());

        if let Some(swap) = abi::pool::events::Swap::match_and_decode(log) {
            match pools_store.get_last(pool_key) {
                None => {
                    panic!(
                        "invalid swap: pool does not exist, pool address {} transaction {}",
                        Hex(log.address()).to_string(),
                        Hex(&log.receipt.transaction.hash).to_string()
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

                    events.push(Event {
                        log_ordinal: log.ordinal(),
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
        } else if let Some(mint) = abi::pool::events::Mint::match_and_decode(log) {
            match pools_store.get_last(pool_key) {
                None => {
                    panic!(
                        "invalid mint: pool does not exist. pool address {} transaction {}",
                        Hex(log.address()).to_string(),
                        Hex(&log.receipt.transaction.hash).to_string()
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

                    events.push(Event {
                        log_ordinal: log.ordinal(),
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
                            amount: mint.amount.to_string(), // big_decimal?
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
                    panic!(
                        "invalid burn: pool does not exist. pool address {} transaction {}",
                        Hex(log.address()).to_string(),
                        Hex(&log.receipt.transaction.hash).to_string()
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

                    events.push(Event {
                        log_ordinal: log.ordinal(),
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
    }
    Ok(pb::uniswap::Events { events})
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


/// Keyspace:
///
///    liquidity:{pool_address} =>
///    total_value_locked:{tokenA}:{tokenB} => 0.1231 (tokenA total value locked, summed for all pools dealing with tokenA:tokenB, in floating point decimal taking tokenA's decimals in consideration).
///
#[substreams::handlers::map]
pub fn map_liquidity(
    events: pb::uniswap::Events,
    pool_sqrt_price_store: store::StoreGet,
) -> Result<pb::uniswap::Liquidities, Error> {
    let mut liquidities= vec![];
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

                    let pool_sqrt_price = get_last_pool_sqrt_price(&pool_sqrt_price_store, &event.pool_address)?;
                    let tick = BigDecimal::from_str_radix(pool_sqrt_price.tick.as_str(), 10).unwrap();


                    let mut li = Liquidity {
                        pool_address: event.pool_address,
                        log_ordinal: event.log_ordinal,
                        update_pool_value: false,
                        token0: event.token0,
                        amount0: amount0.neg().to_string(),
                        token1: event.token1,
                        amount1: amount1.neg().to_string(),
                        ..Default::default()
                    };
                    if tick_lower <= tick && tick <= tick_upper {
                        li.update_pool_value = true;
                        li.pool_value = amount.neg().to_string()
                    }

                    liquidities.push(li);
                }
                MintEvent(mint) => {
                    let amount = BigDecimal::from_str(mint.amount.as_str()).unwrap();
                    let amount0 = BigDecimal::from_str(mint.amount_0.as_str()).unwrap();
                    let amount1 = BigDecimal::from_str(mint.amount_1.as_str()).unwrap();
                    let tick_lower =
                        BigDecimal::from_str(mint.tick_lower.to_string().as_str()).unwrap();
                    let tick_upper =
                        BigDecimal::from_str(mint.tick_upper.to_string().as_str()).unwrap();

                    let pool_sqrt_price = get_last_pool_sqrt_price(&pool_sqrt_price_store, &event.pool_address)?;
                    let tick = BigDecimal::from_str_radix(pool_sqrt_price.tick.as_str(), 10).unwrap();


                    let mut li = Liquidity {
                        pool_address: event.pool_address,
                        log_ordinal: event.log_ordinal,
                        update_pool_value: false,
                        token0: event.token0,
                        amount0: amount0.to_string(),
                        token1: event.token1,
                        amount1: amount1.to_string(),
                        ..Default::default()
                    };
                    if tick_lower <= tick && tick <= tick_upper {
                        li.update_pool_value = true;
                        li.pool_value = amount.to_string()
                    }

                    liquidities.push(li);
                }
                SwapEvent(_) => {}
            }
        }
    }
    Ok(pb::uniswap::Liquidities{liquidities})
}

/// Keyspace
///     pool:{pool.address} -> stores an encoded value of the pool
///     tokens:{}:{} (token0:token1 or token1:token0) -> stores an encoded value of the pool
#[substreams::handlers::store]
pub fn store_liquidity(liquidities: pb::uniswap::Liquidities, output: store::StoreAddBigFloat) {
    for pool_liquidity in liquidities.liquidities {
        if pool_liquidity.update_pool_value {
            output.add(
                pool_liquidity.log_ordinal,
                keyer::liquidity_pool(&pool_liquidity.pool_address),
                &BigDecimal::from_str(pool_liquidity.pool_value.as_str()).unwrap(),
            );
        }
        output.add(
            pool_liquidity.log_ordinal,
            keyer::toal_value_locked_0(&pool_liquidity.token0, &pool_liquidity.token1),
            &BigDecimal::from_str(pool_liquidity.amount0.as_str()).unwrap(),
        );
        output.add(
            pool_liquidity.log_ordinal,
            keyer::toal_value_locked_1(&pool_liquidity.token1,&pool_liquidity.token0),
            &BigDecimal::from_str(pool_liquidity.amount1.as_str()).unwrap(),
        );
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









/// Keyspace
///     token:{token0_addr}:dprice:eth -> stores the derived eth price per token0 price
///     token:{token1_addr}:dprice:eth -> stores the derived eth price per token1 price
#[substreams::handlers::store]
pub fn store_derived_eth_prices(
    pool_sqrt_prices: PoolSqrtPrices,
    pools_store: store::StoreGet,
    prices_store: store::StoreGet,
    liquidity_store: store::StoreGet,
    output: store::StoreSet,
) {
    for pool_sqrt_price in pool_sqrt_prices.pool_sqrt_prices {
        log::debug!("fetching pool: {}", pool_sqrt_price.pool_address);
        log::debug!("sqrt_price: {}", pool_sqrt_price.sqrt_price);
        let pool =
            utils::get_last_pool(&pools_store, pool_sqrt_price.pool_address.as_str()).unwrap();
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
            pool_sqrt_price.ordinal,
            &pool.address,
            &token_0.address.as_str(),
            &liquidity_store,
            &prices_store,
        );
        log::info!("token0_derived_eth_price: {}", token0_derived_eth_price);

        let token1_derived_eth_price = utils::find_eth_per_token(
            pool_sqrt_price.ordinal,
            &pool.address,
            &token_1.address.as_str(),
            &liquidity_store,
            &prices_store,
        );
        log::info!("token1_derived_eth_price: {}", token1_derived_eth_price);

        output.set(
            pool_sqrt_price.ordinal,
            format!("token:{}:dprice:eth", token_0.address),
            &Vec::from(token0_derived_eth_price.to_string()),
        );
        output.set(
            pool_sqrt_price.ordinal,
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
    pool_sqrt_price_deltas: store::Deltas,
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
                new_field!("id", FieldType::String, string_field_value!(pool.address)),
                new_field!("created_at_timestamp", FieldType::Bigint, big_int_field_value!(pool.created_at_timestamp)),
                new_field!("created_at_block_number", FieldType::String, string_field_value!(pool.created_at_block_number)),
                new_field!("token0", FieldType::String, string_field_value!(pool.token0.unwrap().address)),
                new_field!("token1", FieldType::String,string_field_value!(pool.token1.unwrap().address)),
                new_field!("fee_tier", FieldType::Int, int_field_value!(pool.fee_tier)),
                new_field!(
                    "tick",
                    FieldType::Int,
                    int_field_value!(pool.tick_spacing)
                ),
            ],
        };
        out.entity_changes.push(change);
    }

    // for pool_init in pool_inits.pool_initializations {
    //     let change = EntityChange {
    //         entity: "pool".to_string(),
    //         id: string_field_value!(pool_init.address.as_str()),
    //         ordinal: pool_init.log_ordinal,
    //         operation: Operation::Update as i32,
    //         fields: vec![
    //             new_field!( "sqrt_price", FieldType::Bigdecimal, big_decimal_string_field_value!(pool_init.sqrt_price)),
    //             new_field!( "tick", FieldType::Bigint, big_int_field_value!(pool_init.tick)),
    //         ],
    //     };
    //     out.entity_changes.push(change);
    // }

    // SqrtPrice changes
    // Note: All changes from the sqrt_price state are updates
    for pool_sqrt_price_delta in pool_sqrt_price_deltas {
        if !pool_sqrt_price_delta.key.starts_with("pool:") {
            continue;
        }

        let new_value: PoolSqrtPrice = proto::decode(&pool_sqrt_price_delta.new_value).unwrap();

        let mut change = EntityChange {
            entity: "pool".to_string(),
            id: string_field_value!(pool_sqrt_price_delta.key.as_str().split(":").nth(1).unwrap()),
            ordinal: pool_sqrt_price_delta.ordinal,
            operation: Operation::Update as i32,
            fields: vec![],
        };
        match pool_sqrt_price_delta.operation {
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
                let old_value: PoolSqrtPrice = proto::decode(&pool_sqrt_price_delta.new_value).unwrap();
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
