mod pb;
mod event;

use substreams::errors::Error;
use substreams::{Hex, log, proto, store};
use substreams_ethereum::pb::eth as ethpb;

const UNISWAP_V3_FACTORY: &str = "1f98431c8ad98523631ae4a59f267346ea31f984";

#[substreams::handlers::map]
pub fn map_pools(block: ethpb::v1::Block) -> Result<pb::uniswap::Pools, Error> {
    let mut pools = pb::uniswap::Pools { pools: vec![] };

    // todo: use abigen instead of manually checking the events
    for trx in block.transaction_traces {
        /* Uniswap v3 Factory address 0x1f98431c8ad98523631ae4a59f267346ea31f984 */
        if hex::encode(&trx.to) != UNISWAP_V3_FACTORY {
            continue;
        }

        for log in trx.receipt.unwrap().logs {
            let sig = hex::encode(&log.topics[0]);

            if !event::is_pool_created_event(sig.as_str()) {
                continue;
            }

            pools.pools.push(pb::uniswap::Pool {
                address: Hex(&log.data[12..32]).to_string(),
                token0_address: Hex(&log.topics[1][12..]).to_string(),
                token1_address: Hex(&log.topics[2][12..]).to_string(),
                creation_transaction_id: Hex(&trx.hash).to_string(),
                fee: Hex(&log.topics[3][12..]).to_string().parse().unwrap(),
                block_num: block.number,
                log_ordinal: log.block_index as u64,
            })
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
            format!("pool:{}", pool.address),
            &proto::encode(&pool).unwrap(),
        );
    }
}

#[substreams::handlers::store]
pub fn store_fee_amount_enabled(block: ethpb::v1::Block, output: store::StoreSet) {
    for trx in block.transaction_traces {
        for log in trx.receipt.unwrap().logs {
            let sig = hex::encode(&log.topics[0]);

            if !event::is_fee_amount_enabled(sig.as_str()) {
                continue;
            }

            let fee = pb::uniswap::Fee {
                fee: Hex(&log.topics[0][29..]).to_string().parse().unwrap(),
                tick_spacing: Hex(&log.topics[1][29..]).to_string().parse().unwrap(),
                log_ordinal: log.block_index as u64,
                creation_transaction_id: Hex(&trx.hash).to_string(),
            };

            output.set(
                fee.log_ordinal,
                format!("fee:{}", fee.creation_transaction_id),
                    &proto::encode(&fee).unwrap(),
            );
        }
    }
}

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
// #[substreams::handlers::store]
// pub fn store_prices(
//     clock: substreams::pb::substreams::Clock,
//     reserves: pb::uniswap::Reserves,
//     pairs_store: store::StoreGet,
//     reserves_store: store::StoreGet,
//     output: store::StoreSet
// ) {
//
//     // todo -> price stream for usd
//
// }
