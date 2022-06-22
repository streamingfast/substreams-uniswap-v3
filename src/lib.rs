mod pb;
mod abi;

use std::fmt::format;
use bigdecimal::BigDecimal;
use bigdecimal::num_traits::pow;
use num_bigint::{BigInt, Sign};
use substreams::errors::Error;
use substreams::{Hex, log, proto, store};
use substreams_ethereum::pb::eth as ethpb;
use crate::abi::pool::events::Swap;
use crate::pb::uniswap::Pool;

const UNISWAP_V3_FACTORY: &str = "1f98431c8ad98523631ae4a59f267346ea31f984";

#[substreams::handlers::map]
pub fn map_pools_created(block: ethpb::v1::Block) -> Result<pb::uniswap::Pools, Error> {
    let mut pools = pb::uniswap::Pools { pools: vec![] };

    for trx in block.transaction_traces {

        for call in trx.calls.iter() {
            if hex::encode(&call.address) != UNISWAP_V3_FACTORY {
                continue;
            }
            for log in call.logs.iter() {
                if !abi::factory::events::PoolCreated::match_log(&log) {
                    continue
                }
                let event = abi::factory::events::PoolCreated::must_decode(&log);

                pools.pools.push(Pool {
                    address: Hex(&log.data[44..64]).to_string(),
                    token0_address: Hex(&event.token0).to_string(),
                    token1_address: Hex(&event.token1).to_string(),
                    creation_transaction_id: Hex(&trx.hash).to_string(),
                    fee: event.fee.as_u32(),
                    block_num: block.number,
                    log_ordinal: log.block_index as u64,
                    tick: "".to_string(),
                    sqrt_price: "".to_string()
                });
            }
        }
    }

    Ok(pools)
}

// map_pool_initialize
// here we will get the tick and sqrtprice
// and have the rest from the store_pool
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
pub fn store_pools_created(pools: pb::uniswap::Pools, output: store::StoreSet) {
    for pool in pools.pools {
        output.set(
            pool.log_ordinal,
            format!("pool:{}", pool.address),
            &proto::encode(&pool).unwrap(),
        );
    }
}

#[substreams::handlers::store]
pub fn store_pools(pools_initialized: pb::uniswap::Pools, store_created: store::StoreGet, output: store::StoreSet) {
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
pub fn fees(block: ethpb::v1::Block, output: store::StoreSet) {
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
            for l in call.logs.iter() {
                if !abi::pool::events::Flash::match_log(&l) {
                    continue;
                }

                let ev = abi::pool::events::Flash::must_decode(&l);
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
//
// #[substreams::handler::map]
// pub fn map_swaps(block: ethpb::v1::Block) -> Result<, Error> {
//
// }

// todo: swap store

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
- per block or not @alex ??? or both

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
    // pools_store: store::StoreGet, // todo: probably gonna need the pools here (mapper and the store)
    tokens_store: store::StoreGet,
    output: store::StoreSet
) {
    //todo -> price stream for usd
    // price seems to be in the event -> event.params.sqrtPriceX96
    let timestamp_seconds: i64 = block.header.unwrap().timestamp.unwrap().seconds;
    let hour_id: i64 = timestamp_seconds / 3600;
    let day_id: i64 = timestamp_seconds / 86400;
    let base: i32 = 2;

    output.delete_prefix(0, &format!("pool_id:{}:", hour_id - 1));
    output.delete_prefix(0, &format!("pool_id:{}:", day_id - 1));
    output.delete_prefix(0, &format!("token_id:{}:", day_id - 1));

    for trx in block.transaction_traces {
        for log in trx.receipt.unwrap().logs {
            if !Swap::match_log(&log) {
                continue;
            }
            let pool = Hex(&log.address).to_string();
            // blk 10: usdt-dai

            let event: Swap = Swap::must_decode(&log);
            let sqrt_price = BigInt::from(event.sqrt_price_x96.as_u128());
            let price = sqrt_price.pow(base as u32);

            log::info!("trx hash: {}, amount0: {}, amount1: {}, price: {}", Hex(trx.hash.as_slice()).to_string(), event.amount0, event.amount1, price);
            //
            // match tokens_store.get_last(&format!("token:{}", event.));
            // let amount0 = utils::convert_token_to_decimal(event.amount0, )

        }
    }

}
