use crate::{abi, eth, utils, Erc20Token};
use ethabi::Uint;
use substreams::log;
use substreams::scalar::BigInt;
use substreams_ethereum::pb::eth as ethpb;
use substreams_ethereum::scalar::EthBigInt;

pub fn token_total_supply_call(token_address: &String) -> BigInt {
    let rpc_calls: ethpb::rpc::RpcCalls =
        create_token_total_supply_calls(&hex::decode(token_address).unwrap());

    let rpc_responses_unmarshalled: ethpb::rpc::RpcResponses =
        substreams_ethereum::rpc::eth_call(&rpc_calls);
    let responses = rpc_responses_unmarshalled.responses;

    return BigInt::from_signed_bytes_be(responses[0].raw.as_slice());
}

pub fn fee_growth_global_x128_call(pool_address: &String) -> (BigInt, BigInt) {
    let rpc_calls: ethpb::rpc::RpcCalls =
        create_fee_growth_global_x128_calls(&hex::decode(pool_address).unwrap());

    let rpc_responses_unmarshalled: ethpb::rpc::RpcResponses =
        substreams_ethereum::rpc::eth_call(&rpc_calls);
    let responses = rpc_responses_unmarshalled.responses;

    log::info!("bytes response.0: {:?}", responses[0].raw);
    log::info!("bytes response.1: {:?}", responses[1].raw);

    let fee_0: BigInt = BigInt::from_signed_bytes_be(responses[0].raw.as_slice());
    let fee_1: BigInt = BigInt::from_signed_bytes_be(responses[1].raw.as_slice());

    return (fee_0, fee_1);
}

pub fn fee_growth_outside_x128_call(
    pool_address: &String,
    tick_idx: &String,
) -> (EthBigInt, EthBigInt) {
    let tick: EthBigInt = EthBigInt::new(tick_idx.try_into().unwrap());
    log::info!("pool address {} tick idx {}", pool_address, tick_idx);
    let ticks = abi::pool::functions::Ticks { tick };

    let tick_option = ticks.call(hex::decode(pool_address).unwrap());
    if tick_option.is_none() {
        panic!("ticks call failed");
    }
    let (_, _, fee_0, fee_1, _, _, _, _) = tick_option.unwrap();

    return (fee_0.try_into().unwrap(), fee_1.try_into().unwrap());
}

pub fn positions_call(
    pool_address: &String,
    token_id: Uint,
) -> Option<(
    Vec<u8>,
    Vec<u8>,
    EthBigInt,
    EthBigInt,
    EthBigInt,
    EthBigInt,
    EthBigInt,
)> {
    let positions = abi::positionmanager::functions::Positions { token_id };
    if let Some(positions_result) = positions.call(hex::decode(pool_address).unwrap()) {
        return Some((
            positions_result.2,
            positions_result.3,
            positions_result.4.try_into().unwrap(),
            positions_result.5,
            positions_result.6,
            positions_result.8.try_into().unwrap(),
            positions_result.9.try_into().unwrap(),
        ));
    };

    return None;
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
            Some(token) => decimals = token.decimals,
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
            Some(token) => name = token.name,
            None => name = eth::read_string_from_bytes(responses[1].raw.as_ref()),
        },
    }
    log::debug!("decoded_name ok");

    let mut symbol = "unknown".to_string();
    match eth::read_string(responses[2].raw.as_ref()) {
        Ok(s) => {
            symbol = s;
        }
        Err(_) => match utils::get_static_uniswap_tokens(token_address.as_str()) {
            Some(token) => symbol = token.symbol,
            None => symbol = eth::read_string_from_bytes(responses[2].raw.as_ref()),
        },
    }
    log::debug!("decoded_symbol ok");

    return Some(Erc20Token {
        address: String::from(token_address),
        name,
        symbol,
        decimals,
        total_supply: "".to_string(),
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
                data: decimals,
            },
            ethpb::rpc::RpcCall {
                to_addr: Vec::from(addr.clone()),
                data: name,
            },
            ethpb::rpc::RpcCall {
                to_addr: Vec::from(addr.clone()),
                data: symbol,
            },
        ],
    };
}

fn create_fee_growth_global_x128_calls(addr: &Vec<u8>) -> ethpb::rpc::RpcCalls {
    let fee_growth_global_0_x128_data = hex::decode("f3058399").unwrap();
    let fee_growth_global_1_x128_data = hex::decode("46141319").unwrap();

    return ethpb::rpc::RpcCalls {
        calls: vec![
            ethpb::rpc::RpcCall {
                to_addr: Vec::from(addr.clone()),
                data: fee_growth_global_0_x128_data,
            },
            ethpb::rpc::RpcCall {
                to_addr: Vec::from(addr.clone()),
                data: fee_growth_global_1_x128_data,
            },
        ],
    };
}

fn create_token_total_supply_calls(addr: &Vec<u8>) -> ethpb::rpc::RpcCalls {
    let token_total_supply_data = hex::decode("18160ddd").unwrap();

    return ethpb::rpc::RpcCalls {
        calls: vec![ethpb::rpc::RpcCall {
            to_addr: Vec::from(addr.clone()),
            data: token_total_supply_data,
        }],
    };
}
