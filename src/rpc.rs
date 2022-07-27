use crate::{eth, utils, Erc20Token};
use substreams::{log, Hex};
use substreams_ethereum::pb::eth as ethpb;

// NOTE: on uniswap-v3-subgraph, if the token decimal doesn't exist, they simply
//  ignore the token? and don't create the pool... is this something we want to do?
//  Also some tokens have a decimal value of 0... is this legit ? The subgraph will store
//  the token nonetheless. In our case, check substreams `map_uniswap_tokens`
//  we only decide to output/store the pool when both tokens are valid, if not
//  it makes no sense to store the pool

pub fn create_uniswap_token(token_address: &String) -> Option<Erc20Token> {
    let rpc_calls = create_rpc_calls(&hex::decode(token_address).unwrap());

    let rpc_responses_unmarshalled: ethpb::rpc::RpcResponses =
        substreams_ethereum::rpc::eth_call(&rpc_calls);
    let responses = rpc_responses_unmarshalled.responses;

    if responses[0].failed || responses[1].failed || responses[2].failed {
        let decimals_error = String::from_utf8_lossy(responses[0].raw.as_ref());
        let name_error = String::from_utf8_lossy(responses[1].raw.as_ref());
        let symbol_error = String::from_utf8_lossy(responses[2].raw.as_ref());
        log::debug!(
            "{} is not a an ERC20 token contract because of 'eth_call' failures [decimals: {}, name: {}, symbol: {}]",
            Hex(&token_address),
            decimals_error,
            name_error,
            symbol_error,
        );
        return None;
    };

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
    log::debug!("decoded_name ok");

    return Some(Erc20Token {
        address: String::from(token_address),
        name,
        symbol,
        decimals,
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
