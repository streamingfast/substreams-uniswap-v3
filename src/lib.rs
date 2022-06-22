mod pb;
mod abi;

use bigdecimal::BigDecimal;
use bigdecimal::num_traits::pow;
use num_bigint::{BigInt, Sign};
use substreams::errors::Error;
use substreams::{Hex, log, proto, store};
use substreams_ethereum::pb::eth as ethpb;
use crate::abi::pool::events::Swap;

const UNISWAP_V3_FACTORY: &str = "1f98431c8ad98523631ae4a59f267346ea31f984";

#[substreams::handlers::map]
pub fn map_pools_created(block: ethpb::v1::Block) -> Result<pb::uniswap::Pools, Error> {
    let mut pools = pb::uniswap::Pools { pools: vec![] };

    for trx in block.transaction_traces {
        /* Uniswap v3 Factory address 0x1f98431c8ad98523631ae4a59f267346ea31f984 */
        if hex::encode(&trx.to) != UNISWAP_V3_FACTORY {
            continue;
        }

        for log in trx.receipt.unwrap().logs {
            if !abi::factory::events::PoolCreated::match_log(&log) {
                continue;
            }

            let event = abi::factory::events::PoolCreated::must_decode(&log);

            pools.pools.push(pb::uniswap::Pool {
                address: Hex(&log.data[12..32]).to_string(),
                token0_address: Hex(&event.token0).to_string(),
                token1_address: Hex(&event.token1).to_string(),
                creation_transaction_id: Hex(&trx.hash).to_string(),
                fee: event.fee.as_u32(),
                block_num: block.number,
                log_ordinal: log.block_index as u64,
            })
        }
    }

    Ok(pools)
}

// map_pool_initialize
// here we will get the tick and sqrtprice
// and have the rest from the store_pool
#[substreams::handlers::map]
pub fn map_pools_initialized(block: ethpb::v1::Block, store: store::StoreGet) -> Result<pb::uniswap::Pools, Error> {
    let mut pools = pb::uniswap::Pools { pools: vec![] };

    for trx in block.transaction_traces {
        for log in trx.receipt.unwrap().logs {
            if !abi::pool::events::Initialize::match_log(&log) {
                continue;
            }


        }
    }









    Ok(pools)
}

#[substreams::handlers::store]
pub fn store_pools(pools: pb::uniswap::Pools, output: store::StoreSet) {
    log::info!("Building pool state");
    for pool in pools.pools {
        output.set(
            pool.log_ordinal,
            format!("pool:{}:fee:{}", pool.address, pool.fee),
            &proto::encode(&pool).unwrap(),
        );
    }

    // another loop over the initialized to add the other pools by getting the information from the store
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

#[substreams::handler::map]
pub fn map_swaps(block: ethpb::v1::Block) -> Result<, Error> {

}

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

            match tokens_store.get_last(&format!("token:{}", event.));
            let amount0 = utils::convert_token_to_decimal(event.amount0, )

        }
    }

}
