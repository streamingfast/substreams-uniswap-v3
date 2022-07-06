extern crate core;

mod pb;
mod abi;
mod utils;
mod rpc;
mod eth;

use std::collections::HashMap;
use std::ops::Neg;
use std::str::FromStr;
use bigdecimal::BigDecimal;
use substreams::errors::Error;
use substreams::{Hex, log, proto, store};
use substreams::store::{StoreAddBigFloat, StoreAppend, StoreGet, StoreSet};
use substreams_ethereum::pb::eth as ethpb;
use crate::pb::uniswap::{Burn, Event, Mint, Pool, PoolData, UniswapToken, UniswapTokens};
use crate::pb::uniswap::event::Type;
use crate::pb::uniswap::event::Type::Swap as SwapEvent;
use crate::pb::uniswap::event::Type::Burn as BurnEvent;
use crate::pb::uniswap::event::Type::Mint as MintEvent;

const UNISWAP_V3_FACTORY: &str = "1f98431c8ad98523631ae4a59f267346ea31f984";

#[substreams::handlers::map]
pub fn map_uniswap_tokens(pools: pb::uniswap::Pools) -> Result<UniswapTokens, Error> {
    let mut output = UniswapTokens { uniswap_tokens: vec![] };

    //todo(colin): do we really care enough to cache tokens?
    let mut cached_tokens = HashMap::new();

    for pool in pools.pools {
        let token0_address: String = pool.token0_address;

        log::debug!("pool address: {}", pool.address);
        if !cached_tokens.contains_key(&token0_address) {
            let mut uniswap_token0 = rpc::create_uniswap_token(&token0_address);
            cached_tokens.insert(String::from(&token0_address), true);

            if !output.uniswap_tokens.contains(&uniswap_token0) {
                if utils::WHITELIST_TOKENS.contains(&token0_address.as_str()) {
                    uniswap_token0.whitelist_pools.push(String::from(&pool.address))
                }
                output.uniswap_tokens.push(uniswap_token0);
            }
        }

        let token1_address: String = pool.token1_address;
        if !cached_tokens.contains_key(&token1_address) {
            let mut uniswap_token1 = rpc::create_uniswap_token(&token1_address);
            cached_tokens.insert(String::from(&token1_address), true);

            if !output.uniswap_tokens.contains(&uniswap_token1) {
                if utils::WHITELIST_TOKENS.contains(&token1_address.as_str()) {
                    uniswap_token1.whitelist_pools.push(String::from(&pool.address))
                }
                output.uniswap_tokens.push(uniswap_token1);
            }
        }
    }

    Ok(output)
}

#[substreams::handlers::store]
pub fn store_uniswap_tokens(uniswap_tokens: UniswapTokens, output_set: StoreSet) {
    for uniswap_token in uniswap_tokens.uniswap_tokens {
        output_set.set(
            1,
            format!("token:{}", uniswap_token.address),
            &proto::encode(&uniswap_token).unwrap()
        );
    }
}

#[substreams::handlers::store]
pub fn store_uniswap_tokens_whitelist_pools(uniswap_tokens: UniswapTokens, output_append: StoreAppend) {
    for uniswap_token in uniswap_tokens.uniswap_tokens {
        for pools in uniswap_token.whitelist_pools {
            output_append.append(
                1,
                format!("token:{}", uniswap_token.address),
                &format!("{};", pools.to_string())
            )
        }
    }
}

// todo: create a blacklist list which contains invalid pools
#[substreams::handlers::map]
pub fn map_pools_created(block: ethpb::v1::Block) -> Result<pb::uniswap::Pools, Error> {
    let mut output = pb::uniswap::Pools { pools: vec![] };
    for trx in block.transaction_traces {
        for call in trx.calls.iter() {
            if hex::encode(&call.address) != UNISWAP_V3_FACTORY {
                continue;
            }

            for call_log in call.logs.iter() {
                if !abi::factory::events::PoolCreated::match_log(&call_log) {
                    continue
                }

                log::debug!("pool address: {}", &Hex(&call_log.data[44..64]).to_string());
                if utils::BLACKLISTED_POOLS.contains(&Hex(&call_log.data[44..64]).to_string().as_str()) {
                    continue;
                }

                let event = abi::factory::events::PoolCreated::must_decode(&call_log);
                output.pools.push(Pool {
                    address: Hex(&call_log.data[44..64]).to_string(),
                    token0_address: Hex(&event.token0).to_string(),
                    token1_address: Hex(&event.token1).to_string(),
                    creation_transaction_id: Hex(&trx.hash).to_string(),
                    fee: event.fee.as_u32(),
                    block_num: block.number.to_string(),
                    log_ordinal: call_log.block_index as u64,
                    tick_spacing: i32::try_from(event.tick_spacing).unwrap(),
                });
            }
        }
    }

    Ok(output)
}

#[substreams::handlers::map]
pub fn map_pools_initialized(block: ethpb::v1::Block) -> Result<pb::uniswap::PoolDatas, Error> {
    let mut output = pb::uniswap::PoolDatas { pool_data: vec![] };
    for trx in block.transaction_traces {
        for log in trx.receipt.unwrap().logs {
            if !abi::pool::events::Initialize::match_log(&log) {
                continue;
            }

            let event = abi::pool::events::Initialize::must_decode(&log);
            output.pool_data.push(PoolData{
                address: Hex(&log.address).to_string(),
                initialization_transaction_id: Hex(&trx.hash).to_string(),
                log_ordinal: log.block_index as u64,
                tick: event.tick.to_string(),
                sqrt_price: event.sqrt_price_x96.to_string(),
            });
        }
    }

    Ok(output)
}

#[substreams::handlers::store]
pub fn store_pools_data(pools: pb::uniswap::PoolDatas, output_set: StoreSet) {
    for data in pools.pool_data {
        output_set.set(
            data.log_ordinal,
            format!("pool_init:{}", data.address),
            &proto::encode(&data).unwrap()
        );
    }
}

#[substreams::handlers::store]
pub fn store_pools(pools: pb::uniswap::Pools, output: StoreSet) {
    for pool in pools.pools {
        output.set(
            pool.log_ordinal,
            format!("pool:{}", pool.address),
            &proto::encode(&pool).unwrap(),
        );
    }
}

#[substreams::handlers::map]
pub fn map_burns_swaps_mints(block: ethpb::v1::Block, store: StoreGet) -> Result<pb::uniswap::Events, Error> {
    let mut output = pb::uniswap::Events { events: vec![] };
    for trx in block.transaction_traces {
        for log in trx.receipt.unwrap().logs.iter() {
            if abi::pool::events::Swap::match_log(log) {
                let swap = abi::pool::events::Swap::must_decode(log);

                match store.get_last(&format!("pool:{}", Hex(&log.address).to_string())) {
                    None => {
                        panic!("invalid swap. pool does not exist. pool address {} transaction {}", Hex(&log.address).to_string(), Hex(&trx.hash).to_string());
                    }
                    Some(pool_bytes) => {
                        // log::info!("trx id: {}", &Hex(trx.hash.clone()).to_string());
                        let pool: Pool = proto::decode(&pool_bytes).unwrap();

                        output.events.push(Event{
                            log_ordinal: log.block_index as u64,
                            pool_address: pool.address.to_string(),
                            token0: pool.token0_address.to_string(),
                            token1: pool.token1_address.to_string(),
                            fee: pool.fee.to_string(),
                            transaction_id: Hex(&trx.hash).to_string(),
                            timestamp: block.header.as_ref().unwrap().timestamp.as_ref().unwrap().seconds as u64,
                            r#type: Some(SwapEvent(pb::uniswap::Swap{
                                sender: Hex(&swap.sender).to_string(),
                                recipient: Hex(&swap.recipient).to_string(),
                                amount_0: swap.amount0.to_string(), // big_decimal?
                                amount_1: swap.amount1.to_string(), // big_decimal?
                                sqrt_price: swap.sqrt_price_x96.to_string(), // big_decimal?
                                liquidity: swap.liquidity.to_string(),
                                tick: swap.tick.to_string(),
                            })),
                        });
                    }
                }
            }

            if abi::pool::events::Burn::match_log(log) {
                let burn = abi::pool::events::Burn::must_decode(log);

                match store.get_last(&format!("pool:{}", Hex(&log.address).to_string())) {
                    None => {
                        panic!("invalid burn. pool does not exist. pool address {} transaction {}", Hex(&log.address).to_string(), Hex(&trx.hash).to_string());
                    }
                    Some(pool_bytes) => {
                        let pool: Pool = proto::decode(&pool_bytes).unwrap();

                        output.events.push(Event{
                            log_ordinal: log.block_index as u64,
                            pool_address: pool.address.to_string(),
                            token0: pool.token0_address.to_string(),
                            token1: pool.token1_address.to_string(),
                            fee: pool.fee.to_string(),
                            transaction_id: Hex(&trx.hash).to_string(),
                            timestamp: block.header.as_ref().unwrap().timestamp.as_ref().unwrap().seconds as u64,
                            r#type: Some(BurnEvent(Burn{
                                owner: Hex(&burn.owner).to_string(),
                                amount_0: burn.amount0.to_string(), // big_decimal?
                                amount_1: burn.amount1.to_string(), // big_decimal?
                                tick_lower: burn.tick_lower.to_string(),
                                tick_upper: burn.tick_upper.to_string(),
                                amount: burn.amount.to_string(), // big_decimal?
                            })),
                        });
                    }
                }
            }

            if abi::pool::events::Mint::match_log(log) {
                let mint = abi::pool::events::Mint::must_decode(log);

                match store.get_last(&format!("pool:{}", Hex(&log.address).to_string())) {
                    None => {
                        panic!("invalid mint. pool does not exist. pool address {} transaction {}", Hex(&log.address).to_string(), Hex(&trx.hash).to_string());
                    }
                    Some(pool_bytes) => {
                        let pool: Pool = proto::decode(&pool_bytes).unwrap();

                        output.events.push(Event{
                            log_ordinal: log.block_index as u64,
                            pool_address: pool.address.to_string(),
                            token0: pool.token0_address.to_string(),
                            token1: pool.token1_address.to_string(),
                            fee: pool.fee.to_string(),
                            transaction_id: Hex(&trx.hash).to_string(),
                            timestamp: block.header.as_ref().unwrap().timestamp.as_ref().unwrap().seconds as u64,
                            r#type: Some(MintEvent(Mint{
                                owner: Hex(&mint.owner).to_string(),
                                sender: Hex(&mint.sender).to_string(),
                                amount_0: mint.amount0.to_string(), // big_decimal?
                                amount_1: mint.amount1.to_string(), // big_decimal?
                                tick_lower: mint.tick_lower.to_string(),
                                tick_upper: mint.tick_upper.to_string(),
                                amount: mint.amount.to_string(), // big_decimal?
                            })),
                        });
                    }
                }
            }
        }
    }

    Ok(output)
}

#[substreams::handlers::store]
pub fn store_liquidity(events: pb::uniswap::Events, output: StoreAddBigFloat) {
    for event in events.events {
        if event.r#type.is_some() {
            match event.r#type.unwrap() {
                Type::Swap(swap) => {
                    let amount0 = BigDecimal::from_str(swap.amount_0.as_str()).unwrap();
                    let amount1 = BigDecimal::from_str(swap.amount_1.as_str()).unwrap();
                    output.add(
                        event.log_ordinal,
                        format!("pool:{}:token:{}:total_value_locked", event.pool_address, event.token0),
                        &amount0
                    );
                    output.add(
                        event.log_ordinal,
                        format!("pool:{}:token:{}:total_value_locked", event.pool_address, event.token1),
                        &amount1
                    );
                }
                Type::Burn(burn) => {
                    let amount = BigDecimal::from_str(burn.amount.as_str()).unwrap();
                    let amount0 = BigDecimal::from_str(burn.amount_0.as_str()).unwrap();
                    let amount1 = BigDecimal::from_str(burn.amount_1.as_str()).unwrap();
                    output.add(
                        event.log_ordinal,
                        format!("pool:{}:liquidity", event.pool_address),
                        &amount.neg()
                    );
                    output.add(
                        event.log_ordinal,
                        format!("pool:{}:token:{}:total_value_locked", event.pool_address, event.token0),
                        &amount0.neg()
                    );
                    output.add(
                        event.log_ordinal,
                        format!("pool:{}:token:{}:total_value_locked", event.pool_address, event.token1),
                        &amount1.neg()
                    );
                }
                Type::Mint(mint) => {
                    let amount = BigDecimal::from_str(mint.amount.as_str()).unwrap();
                    let amount0 = BigDecimal::from_str(mint.amount_0.as_str()).unwrap();
                    let amount1 = BigDecimal::from_str(mint.amount_1.as_str()).unwrap();

                    output.add(
                        event.log_ordinal,
                        format!("pool:{}:liquidity", event.pool_address),
                        &amount
                    );
                    output.add(
                        event.log_ordinal,
                        format!("pool:{}:token:{}:total_value_locked", event.pool_address, event.token0),
                        &amount0
                    );
                    output.add(
                        event.log_ordinal,
                        format!("pool:{}:token:{}:total_value_locked", event.pool_address, event.token1),
                        &amount1
                    );
                }
            }
        }
    }
}

#[substreams::handlers::store]
pub fn store_prices(
    block: ethpb::v1::Block,
    swaps_burns_mints: pb::uniswap::Events,
    liquidity_store: StoreGet,
    pools_store: StoreGet,
    pools_data_store: StoreGet,
    tokens_store: StoreGet,
    whitelist_pools_store: StoreGet,
    output: StoreSet
) {
    let timestamp_seconds: i64 = block.header.unwrap().timestamp.unwrap().seconds;
    let hour_id: i64 = timestamp_seconds / 3600;
    let day_id: i64 = timestamp_seconds / 86400;

    // output.delete_prefix(0, &format!("pool_id:{}:", hour_id - 1));
    // output.delete_prefix(0, &format!("pool_id:{}:", day_id - 1));
    // output.delete_prefix(0, &format!("token_id:{}:", day_id - 1));

    for event in swaps_burns_mints.events {
        log::info!("looking for swap event");
        match event.r#type.unwrap() {
            Type::Swap(swap) => {
                let token_0 = utils::get_last_token(&tokens_store, event.token0.as_str()).unwrap();
                let token_1 = utils::get_last_token(&tokens_store, event.token1.as_str()).unwrap();

                let sqrt_price = BigDecimal::from_str(swap.sqrt_price.as_str()).unwrap();
                let tokens_price: (BigDecimal, BigDecimal) = utils::compute_prices(&sqrt_price, &token_0, &token_1);
                log::debug!("token prices: {} {}", tokens_price.0, tokens_price.1);

                //todo: is this interesting data to store?  these are the prices of tokens
                // in relation to the other token in this specific pool.
                // should we add the pool id to the key?
                output.set(
                    event.log_ordinal,
                    format!("token:{}:price", event.token0),
                    &Vec::from(tokens_price.0.to_string())
                );
                output.set(
                    event.log_ordinal,
                    format!("token:{}:price", event.token1),
                    &Vec::from(tokens_price.1.to_string())
                );

                let token0_derived_eth_price = utils::find_eth_per_token(
                    event.log_ordinal,
                    &token_0.address.as_str(),
                    &pools_store,
                    &pools_data_store,
                    &tokens_store,
                    &whitelist_pools_store,
                    &liquidity_store,
                    0,
                );
                log::info!("token0_derived_eth_price: {}", token0_derived_eth_price);

                let token1_derived_eth_price = utils::find_eth_per_token(
                    event.log_ordinal,
                    &token_1.address.as_str(),
                    &pools_store,
                    &pools_data_store,
                    &tokens_store,
                    &whitelist_pools_store,
                    &liquidity_store,
                    0,
                );
                log::info!("token1_derived_eth_price: {}", token1_derived_eth_price);

                output.set(
                    event.log_ordinal,
                    format!("token:{}:dprice:eth", event.token0),
                    &Vec::from(token0_derived_eth_price.to_string())
                );
                output.set(
                    event.log_ordinal,
                    format!("token:{}:dprice:eth", event.token1),
                    &Vec::from(token1_derived_eth_price.to_string())
                );
            }
            Type::Burn(_) => {
            }
            Type::Mint(_) => {
            }
        }
    }
}

#[substreams::handlers::map]
pub fn map_fees(block: ethpb::v1::Block) -> Result<pb::uniswap::Fees, Error> {
    let mut out = pb::uniswap::Fees { fees: vec![] };

    for trx in block.transaction_traces {
        for log in trx.receipt.unwrap().logs {
            if !abi::factory::events::FeeAmountEnabled::match_log(&log) {
                continue;
            }

            let ev = abi::factory::events::FeeAmountEnabled::must_decode(&log);

            out.fees.push(pb::uniswap::Fee {
                fee: ev.fee.as_u32(),
                tick_spacing: ev.tick_spacing.as_u32() as i32,
            });
        }
    }

    Ok(out)
}

#[substreams::handlers::store]
pub fn store_fees(block: ethpb::v1::Block, output: store::StoreSet) {
    for trx in block.transaction_traces {
        for log in trx.receipt.unwrap().logs {
            if !abi::factory::events::FeeAmountEnabled::match_log(&log) {
                continue;
            }

            let event = abi::factory::events::FeeAmountEnabled::must_decode(&log);

            let fee = pb::uniswap::Fee {
                fee: event.fee.as_u32(),
                tick_spacing: event.tick_spacing.as_u32() as i32
            };

            output.set(
                log.block_index as u64,
                format!("fee:{}:{}", fee.fee, fee.tick_spacing),
                &proto::encode(&fee).unwrap(),
            );
        }
    }
}

#[substreams::handlers::map]
pub fn map_flashes(block: ethpb::v1::Block) -> Result<pb::uniswap::Flashes, Error> {
    let mut out = pb::uniswap::Flashes { flashes: vec![] };

    for trx in block.transaction_traces {
        for call in trx.calls.iter() {
            log::debug!("call target: {}", Hex(&call.address).to_string());
            for call_log in call.logs.iter() {
                if !abi::pool::events::Flash::match_log(&call_log) {
                    continue;
                }

                let ev = abi::pool::events::Flash::must_decode(&call_log);
                log::debug!("{:?}", ev);

                log::debug!("trx id: {}", Hex(&trx.hash).to_string());
                for change in call.storage_changes.iter() {
                    log::debug!(
                        "storage change: {} {} {} {}",
                        Hex(&change.address).to_string(),
                        Hex(&change.key).to_string(),
                        Hex(&change.old_value).to_string(),
                        Hex(&change.new_value).to_string(),
                    );
                }
            }
        }
    }

    Ok(out)
}