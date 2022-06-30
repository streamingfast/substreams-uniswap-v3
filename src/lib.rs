extern crate core;

mod pb;
mod abi;
mod utils;
mod event;
mod rpc;
mod eth;

use std::collections::HashMap;
use std::ops::Neg;
use std::str::FromStr;
use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use substreams::errors::Error;
use substreams::{Hex, hex, log, proto, store};
use substreams::store::{StoreAddBigFloat, StoreAppend, StoreGet, StoreSet};
use substreams_ethereum::pb::eth as ethpb;
use crate::pb::uniswap::{Burn, Event, Mint, Pool, Swap, UniswapToken, UniswapTokens};
use crate::pb::uniswap::event::Type;
use crate::pb::uniswap::event::Type::Swap as SwapEvent;
use crate::pb::uniswap::event::Type::Burn as BurnEvent;
use crate::pb::uniswap::event::Type::Mint as MintEvent;

const UNISWAP_V3_FACTORY: &str = "1f98431c8ad98523631ae4a59f267346ea31f984";

// todo: create a blacklist list which contains invalid pools
#[substreams::handlers::map]
pub fn map_pools_created(block: ethpb::v1::Block) -> Result<pb::uniswap::Pools, Error> {
    let mut pools = pb::uniswap::Pools { pools: vec![] };

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

                pools.pools.push(Pool {
                    address: Hex(&call_log.data[44..64]).to_string(),
                    token0_address: Hex(&event.token0).to_string(),
                    token1_address: Hex(&event.token1).to_string(),
                    creation_transaction_id: Hex(&trx.hash).to_string(),
                    fee: event.fee.as_u32(),
                    block_num: block.number,
                    log_ordinal: call_log.block_index as u64,
                    tick: "".to_string(),
                    sqrt_price: "".to_string()
                });
            }
        }
    }

    Ok(pools)
}

#[substreams::handlers::map]
pub fn map_pools_initialized(block: ethpb::v1::Block) -> Result<pb::uniswap::Pools, Error> {
    let mut pools = pb::uniswap::Pools { pools: vec![] };

    for trx in block.transaction_traces {
        for log in trx.receipt.unwrap().logs {
            if !abi::pool::events::Initialize::match_log(&log) {
                continue;
            }

            let event = abi::pool::events::Initialize::must_decode(&log);
            pools.pools.push(Pool{
                address: Hex(&log.address).to_string(),
                token0_address: "".to_string(),
                token1_address: "".to_string(),
                creation_transaction_id: Hex(&trx.hash).to_string(),
                fee: 0,
                block_num: 0,
                log_ordinal: 0,
                tick: event.tick.to_string(),
                sqrt_price: event.sqrt_price_x96.to_string(),
            });
        }
    }

    Ok(pools)
}

#[substreams::handlers::store]
pub fn store_pools_created(pools: pb::uniswap::Pools, output: StoreSet) {
    for pool in pools.pools {
        output.set(
            pool.log_ordinal,
            format!("pool:{}", pool.address),
            &proto::encode(&pool).unwrap(),
        );
    }
}

#[substreams::handlers::store]
pub fn store_pools(pools_initialized: pb::uniswap::Pools, store_created: StoreGet, output: StoreSet) {
    for pool in pools_initialized.pools {
        match store_created.get_last(&format!("pool:{}", pool.address)) {
            None => {
                panic!("pool {} initialized but never created, tx id {}", pool.address, pool.creation_transaction_id)
            }
            Some(pool_bytes) => {
                let mut created_pool : Pool = proto::decode(&pool_bytes).unwrap();
                created_pool.tick = pool.tick;
                created_pool.sqrt_price = pool.sqrt_price;

                output.set(
                    created_pool.log_ordinal,
                    format!("pool:{}", created_pool.address),
                    &proto::encode(&created_pool).unwrap(),
                );
            }
        }
    }
}

//todo: is this the correct way to do? better substreams pattern to not fetch the information
// again when we have the information from the ethtoken
#[substreams::handlers::map]
pub fn map_uniswap_tokens(pools: pb::uniswap::Pools) -> Result<UniswapTokens, Error> {
    let mut uniswap_tokens = UniswapTokens { uniswap_tokens: vec![] };

    let mut cached_tokens = HashMap::new();

    for pool in pools.pools {
        let token0_address: String = pool.token0_address;

        log::debug!("pool address: {}", pool.address);
        if !cached_tokens.contains_key(&token0_address) {
            let mut uniswap_token0 = rpc::create_uniswap_token(&token0_address);
            cached_tokens.insert(String::from(&token0_address), true);

            if !uniswap_tokens.uniswap_tokens.contains(&uniswap_token0) {
                if utils::WHITELIST_TOKENS.contains(&token0_address.as_str()) {
                    uniswap_token0.whitelist_pools.push(String::from(&pool.address))
                }

                uniswap_tokens.uniswap_tokens.push(uniswap_token0);
            }
        }

        let token1_address: String = pool.token1_address;
        if !cached_tokens.contains_key(&token1_address) {
            let mut uniswap_token1 = rpc::create_uniswap_token(&token1_address);
            cached_tokens.insert(String::from(&token1_address), true);

            if !uniswap_tokens.uniswap_tokens.contains(&uniswap_token1) {
                if utils::WHITELIST_TOKENS.contains(&token1_address.as_str()) {
                    uniswap_token1.whitelist_pools.push(String::from(&pool.address))
                }

                uniswap_tokens.uniswap_tokens.push(uniswap_token1);
            }
        }
    }

    Ok(uniswap_tokens)
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
            log::info!("call target: {}", Hex(&call.address).to_string());
            for call_log in call.logs.iter() {
                if !abi::pool::events::Flash::match_log(&call_log) {
                    continue;
                }

                let ev = abi::pool::events::Flash::must_decode(&call_log);
                log::info!("{:?}", ev);

                log::info!("trx id: {}", Hex(&trx.hash).to_string());
                for change in call.storage_changes.iter() {
                    log::info!(
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

#[substreams::handlers::map]
pub fn map_burns_swaps_mints(block: ethpb::v1::Block, store: StoreGet) -> Result<pb::uniswap::Events, Error> {
    let mut out = pb::uniswap::Events { events: vec![] };

    for trx in block.transaction_traces {
        for log in trx.receipt.unwrap().logs.iter() {
            if abi::pool::events::Swap::match_log(log) {
                let swap = abi::pool::events::Swap::must_decode(log);

                match store.get_last(&format!("pool:{}", Hex(&log.address).to_string())) {
                    None => {
                        panic!("invalid swap. pool does not exist. pool address {} transaction {}", Hex(&log.address).to_string(), Hex(&trx.hash).to_string());
                    }
                    Some(pool_bytes) => {
                        let pool: Pool = proto::decode(&pool_bytes).unwrap();

                        out.events.push(Event{
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
                                amount_usd: "".to_string(),
                                sqrt_price: swap.sqrt_price_x96.to_string(), // big_decimal?
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

                        out.events.push(Event{
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
                                amount_usd: "".to_string(),
                                amount: burn.amount.to_string(), // big_decimal?
                                tick: "".to_string()
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

                        out.events.push(Event{
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
                                amount_usd: "".to_string(),
                                amount: mint.amount.to_string(), // big_decimal?
                                tick: "".to_string()
                            })),
                        });
                    }
                }
            }
        }
    }

    Ok(out)
}

#[substreams::handlers::map]
pub fn map_swaps(block: ethpb::v1::Block, store: StoreGet) -> Result<pb::uniswap::Events, Error> {
    let mut out = pb::uniswap::Events { events: vec![] };

    for trx in block.transaction_traces {
        for log in trx.receipt.unwrap().logs.iter() {
            if !abi::pool::events::Swap::match_log(log) {
                continue;
            }

            let swap = abi::pool::events::Swap::must_decode(log);

            match store.get_last(&format!("pool:{}", Hex(&log.address).to_string())) {
                None => {
                    panic!("invalid swap. pool does not exist. pool address {} transaction {}", Hex(&log.address).to_string(), Hex(&trx.hash).to_string());
                }
                Some(pool_bytes) => {
                    let pool: Pool = proto::decode(&pool_bytes).unwrap();

                    out.events.push(Event{
                        log_ordinal: log.block_index as u64,
                        pool_address: pool.address.to_string(),
                        token0: pool.token0_address.to_string(),
                        token1: pool.token1_address.to_string(),
                        fee: pool.fee.to_string(),
                        transaction_id: Hex(&trx.hash).to_string(),
                        timestamp: block.header.as_ref().unwrap().timestamp.as_ref().unwrap().seconds as u64,
                        r#type: Some(SwapEvent(Swap{
                            sender: Hex(&swap.sender).to_string(),
                            recipient: Hex(&swap.recipient).to_string(),
                            amount_0: swap.amount0.to_string(),
                            amount_1: swap.amount1.to_string(),
                            amount_usd: "".to_string(),
                            sqrt_price: swap.sqrt_price_x96.to_string(),
                            tick: swap.tick.to_string(),
                        })),
                    });
                }
            }
        }
    }

    Ok(out)
}

#[substreams::handlers::map]
pub fn map_burns(block: ethpb::v1::Block, store: StoreGet) -> Result<pb::uniswap::Events, Error> {
    let mut out = pb::uniswap::Events { events: vec![] };

    for trx in block.transaction_traces {
        for log in trx.receipt.unwrap().logs.iter() {
            if !abi::pool::events::Burn::match_log(log) {
                continue;
            }

            let burn = abi::pool::events::Burn::must_decode(log);

            match store.get_last(&format!("pool:{}", Hex(&log.address).to_string())) {
                None => {
                    panic!("invalid burn. pool does not exist. pool address {} transaction {}", Hex(&log.address).to_string(), Hex(&trx.hash).to_string());
                }
                Some(pool_bytes) => {
                    let pool: Pool = proto::decode(&pool_bytes).unwrap();

                    out.events.push(Event{
                        log_ordinal: log.block_index as u64,
                        pool_address: pool.address.to_string(),
                        token0: pool.token0_address.to_string(),
                        token1: pool.token1_address.to_string(),
                        fee: pool.fee.to_string(),
                        transaction_id: Hex(&trx.hash).to_string(),
                        timestamp: block.header.as_ref().unwrap().timestamp.as_ref().unwrap().seconds as u64,
                        r#type: Some(BurnEvent(pb::uniswap::Burn{
                            owner: Hex(&burn.owner).to_string(),
                            amount_0: burn.amount0.to_string(), // big_decimal?
                            amount_1: burn.amount1.to_string(), // big_decimal?
                            amount_usd: "".to_string(),
                            amount: burn.amount.to_string(), // big_decimal?
                            tick: "".to_string()
                        })),
                    });
                }
            }
        }
    }

    Ok(out)
}

#[substreams::handlers::map]
pub fn map_mints(block: ethpb::v1::Block, store: StoreGet) -> Result<pb::uniswap::Events, Error> {
    let mut out = pb::uniswap::Events { events: vec![] };

    for trx in block.transaction_traces {
        for log in trx.receipt.unwrap().logs.iter() {
            // match
            // decode
            // create swap
            if !abi::pool::events::Mint::match_log(log) {
                continue;
            }

            let mint = abi::pool::events::Mint::must_decode(log);

            match store.get_last(&format!("pool:{}", Hex(&log.address).to_string())) {
                None => {
                    panic!("invalid mint. pool does not exist. pool address {} transaction {}", Hex(&log.address).to_string(), Hex(&trx.hash).to_string());
                }
                Some(pool_bytes) => {
                    let pool: Pool = proto::decode(&pool_bytes).unwrap();


                    out.events.push(Event{
                        log_ordinal: log.block_index as u64,
                        pool_address: pool.address.to_string(),
                        token0: pool.token0_address.to_string(),
                        token1: pool.token1_address.to_string(),
                        fee: pool.fee.to_string(),
                        transaction_id: Hex(&trx.hash).to_string(),
                        timestamp: block.header.as_ref().unwrap().timestamp.as_ref().unwrap().seconds as u64,
                        r#type: Some(BurnEvent(pb::uniswap::Burn{
                            owner: Hex(&mint.owner).to_string(),
                            amount_0: mint.amount0.to_string(),
                            amount_1: mint.amount1.to_string(),
                            amount_usd: "".to_string(),
                            amount: mint.amount.to_string(),
                            tick: "".to_string()
                        })),
                    });
                }
            }
        }
    }

    Ok(out)
}

// todo: find a better name ftw
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

// need ethPriceUSD


// are we better off to do a similar pattern as we did for pcs and have a substreams that takes care of all 3 types of events ?


//
// // todo: swap store
// #[substreams::handlers::store]
// pub fn store_swaps(swap_events: pb::uniswap::Events, output: store::StoreSet) {
//     for swap_event in swap_events.events {
//         output.set(
//     }
// }

// #[substreams::handlers::map]
// pub fn map_reserves(
//     block: ethpb::v1::Block,
//     pairs: store::StoreGet,
//     tokens: store::StoreGet
// ) -> Result<pb::uniswap::Reserves, Error> {
//     let mut reserves = pb::uniswap::Reserves { reserves: vec![] };
//
//     for trx in block.transaction_traces {
//         for log in trx.receipt.unwrap().logs {
//             let addr: String = Hex(&log.address).to_string();
//             match pairs.get_last(&format!("pair:{}", addr)) {
//                 None => continue,
//                 Some(pair_bytes) => {
//                     let sig = hex::encode(&log.topics[0]);
//
//                     if !event::is_pair_sync_event(sig.as_str()) {
//                         continue
//                     }
//
//                 }
//             }
//         }
//     }
//
//     Ok(reserves)
// }
//
// #[substreams::handlers::store]
// pub fn store_reserves(
//     clock: substreams::pb::substreams::Clock,
//     reserves: pb::uniswap::Reserves,
//     pairs_store: store::StoreGet,
//     output: store::StoreSet
// ) {
//
//     // todo
//
// }
//


/*
- per block or not @alex ??? or both -> at each swap, not on each block for the moment

price store:
    - clock
    - map swaps
    - map pool
    - map pool initialize
    - store pool

    --> write ethPriceUSD:price:usd -> proto:encode ou wtv {} ||| future: to get price get_last
    --> write block_num:ethPriceUSD:price:ust -> proto:encode ou wtv {}
*/

#[substreams::handlers::store]
pub fn store_prices(
    block: ethpb::v1::Block,
    swaps: pb::uniswap::Events,
    pools_store: StoreGet, // todo: probably gonna need the pools here (mapper and the store)
    tokens_store: StoreGet,
    whitelist_pools_store: StoreGet,
    output: StoreSet
) {
    //todo -> price stream for usd
    // price seems to be in the event -> event.params.sqrtPriceX96
    let timestamp_seconds: i64 = block.header.unwrap().timestamp.unwrap().seconds;
    let hour_id: i64 = timestamp_seconds / 3600;
    let day_id: i64 = timestamp_seconds / 86400;

    // output.delete_prefix(0, &format!("pool_id:{}:", hour_id - 1));
    // output.delete_prefix(0, &format!("pool_id:{}:", day_id - 1));
    // output.delete_prefix(0, &format!("token_id:{}:", day_id - 1));

    for swap_event in swaps.events {
        let token_0 = utils::get_last_token(&tokens_store, swap_event.token0.as_str());
        let token_1 = utils::get_last_token(&tokens_store, swap_event.token1.as_str());

        match swap_event.r#type.unwrap() {
            Type::Swap(swap) => {
                let sqrt_price = BigInt::from_str(swap.sqrt_price.as_str()).unwrap();
                let tokens_price: (BigDecimal, BigDecimal) = utils::compute_prices(&sqrt_price, token_0, token_1);
                output.set(
                    swap_event.log_ordinal,
                    format!("token:{}:price", swap_event.token0),
                    &Vec::from(tokens_price.0.to_string())
                );
                output.set(
                    swap_event.log_ordinal,
                    format!("token:{}:price", swap_event.token1),
                    &Vec::from(tokens_price.1.to_string())
                );

                let token0_derived_eth_price = utils::find_eth_per_token(
                    swap_event.log_ordinal,
                    token_0.address.as_str(),
                    &pools_store,
                    &whitelist_pools_store
                );
                let token1_derived_eth_price = utils::find_eth_per_token(
                    swap_event.log_ordinal,
                    token_1.address.as_str(),
                    &pools_store,
                    &whitelist_pools_store
                );
                output.set(
                    swap_event.log_ordinal,
                    format!("token:{}:dprice:eth", swap_event.token0),
                    &Vec::from(token0_derived_eth_price.to_string())
                );
                output.set(
                    swap_event.log_ordinal,
                    format!("token:{}:dprice:eth", swap_event.token1),
                    &Vec::from(token1_derived_eth_price.to_string())
                );

            }
            Type::Burn(_) => {}
            Type::Mint(_) => {}
        }
    }

}

// @colin and @eduard
// need to scope out what we need to have a complete migration of
// uniswap v3 subgraph to a substream
