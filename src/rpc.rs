use crate::{eth, utils, Erc20Token};
use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use substreams::log;
use substreams_ethereum::pb::eth as ethpb;

pub fn fee_growth_global_x128_call(pool_address: &String) -> (BigDecimal, BigDecimal) {
    let rpc_calls: ethpb::rpc::RpcCalls =
        create_fee_growth_global_x123_calls(&hex::decode(pool_address).unwrap());

    let rpc_responses_unmarshalled: ethpb::rpc::RpcResponses =
        substreams_ethereum::rpc::eth_call(&rpc_calls);
    let responses = rpc_responses_unmarshalled.responses;

    log::info!("bytes response.0: {:?}", responses[0].raw);
    log::info!("bytes response.1: {:?}", responses[1].raw);

    // todo: need to convert the data from bytes to bigdecimal
    let fee_growth_global_0_x128: BigDecimal =
        BigDecimal::from(BigInt::from_signed_bytes_be(responses[0].raw.as_ref()));
    let fee_growth_global_1_x128: BigDecimal =
        BigDecimal::from(BigInt::from_signed_bytes_be(responses[1].raw.as_ref()));

    return (fee_growth_global_0_x128, fee_growth_global_1_x128);
}

pub fn create_uniswap_token(token_address: &String) -> Option<Erc20Token> {
    let rpc_calls = create_rpc_calls(&hex::decode(token_address).unwrap());
    let rpc_responses_unmarshalled: ethpb::rpc::RpcResponses =
        substreams_ethereum::rpc::eth_call(&rpc_calls);
    let responses = rpc_responses_unmarshalled.responses;
    let mut decimals: u64 = 0;
    match eth::read_uint32(responses[0].raw.as_ref()) {
        Ok(decoded_decimals) => {
            decimals = decoded_decimals as u64;
        }
        Err(err) => match utils::get_static_uniswap_tokens(token_address.as_str()) {
            Some(token) => {
                decimals = token.decimals;
            }
            None => {
                log::debug!(
                    "{} is not a an ERC20 token contract decimal `eth_call` failed: {}",
                    &token_address,
                    err.msg,
                );
                return None;
            }
        },
    }
    log::debug!("decoded_decimals ok");

    let mut name = "unknown".to_string();
    match eth::read_string(responses[1].raw.as_ref()) {
        Ok(decoded_name) => {
            name = decoded_name;
        }
        Err(_) => match utils::get_static_uniswap_tokens(token_address.as_str()) {
            Some(token) => {
                name = token.name;
            }
            None => {
                name = eth::read_string_from_bytes(responses[1].raw.as_ref());
            }
        },
    }
    log::debug!("decoded_name ok");

    let mut symbol = "unknown".to_string();
    match eth::read_string(responses[2].raw.as_ref()) {
        Ok(s) => {
            symbol = s;
        }
        Err(_) => match utils::get_static_uniswap_tokens(token_address.as_str()) {
            Some(token) => {
                symbol = token.symbol;
            }
            None => {
                symbol = eth::read_string_from_bytes(responses[2].raw.as_ref());
            }
        },
    }
    log::debug!("decoded_symbol ok");

    return Some(Erc20Token {
        address: String::from(token_address),
        name,
        symbol,
        decimals,
        whitelist_pools: vec![],
    });
}

fn create_rpc_calls(addr: &Vec<u8>) -> ethpb::rpc::RpcCalls {
    let decimals = hex::decode("313ce567").unwrap();
    let name = hex::decode("06fdde03").unwrap();
    let symbol = hex::decode("95d89b41").unwrap();

    return ethpb::rpc::RpcCalls {
        calls: vec![
            ethpb::rpc::RpcCall {
                to_addr: Vec::from(addr.clone()),
                method_signature: decimals,
            },
            ethpb::rpc::RpcCall {
                to_addr: Vec::from(addr.clone()),
                method_signature: name,
            },
            ethpb::rpc::RpcCall {
                to_addr: Vec::from(addr.clone()),
                method_signature: symbol,
            },
        ],
    };
}

fn create_fee_growth_global_x123_calls(addr: &Vec<u8>) -> ethpb::rpc::RpcCalls {
    let fee_growth_global_0_x128_method_signature = hex::decode("f3058399").unwrap();
    let fee_growth_global_1_x128_method_signature = hex::decode("46141319").unwrap();

    return ethpb::rpc::RpcCalls {
        calls: vec![
            ethpb::rpc::RpcCall {
                to_addr: Vec::from(addr.clone()),
                method_signature: fee_growth_global_0_x128_method_signature,
            },
            ethpb::rpc::RpcCall {
                to_addr: Vec::from(addr.clone()),
                method_signature: fee_growth_global_1_x128_method_signature,
            },
        ],
    };
}
