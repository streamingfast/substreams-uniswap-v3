mod pb;
mod event;

use substreams::errors::Error;
use substreams::{Hex, log, proto, store};
use substreams_ethereum::pb::eth as ethpb;

#[substreams::handlers::map]
pub fn map_pairs(blk: ethpb::v1::Block) -> Result<pb::uniswap::Pairs, Error> {
    let mut pairs = pb::uniswap::Pairs { pairs: vec![] };

    for trx in blk.transaction_traces {
        /* PCS Factory address */
        //0x1f98431c8ad98523631ae4a59f267346ea31f984
        if hex::encode(&trx.to) != "1f98431c8ad98523631ae4a59f267346ea31f984" {
            continue;
        }

        for log in trx.receipt.unwrap().logs {
            let sig = hex::encode(&log.topics[0]);

            if !event::is_pair_created_event(sig.as_str()) {
                continue;
            }

            pairs.pairs.push(pb::uniswap::Pair {
                address: Hex(&log.data[12..32]).to_string(),
                token0_address: Hex(&log.topics[1][12..]).to_string(),
                token1_address: Hex(&log.topics[2][12..]).to_string(),
                creation_transaction_id: Hex(&trx.hash).to_string(),
                block_num: blk.number,
                log_ordinal: log.block_index as u64,
            })
        }

    }

    Ok(pairs)
}

#[substreams::handlers::store]
pub fn store_pairs(pairs: pb::uniswap::Pairs, output: store::StoreSet) {
    log::info!("Building pair state");
    for pair in pairs.pairs {
        output.set(
            pair.log_ordinal,
            format!("pair:{}", pair.address),
            &proto::encode(&pair).unwrap(),
        );
    }
}