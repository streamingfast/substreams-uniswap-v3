use crate::{abi, eth, utils, Erc20Token};
use prost::Message;
use substreams::log;
use substreams::scalar::BigInt;
use substreams::Hex;
use substreams_ethereum::rpc::RpcBatch;

pub fn create_uniswap_token(token_address: &String) -> Option<Erc20Token> {
    let batch = RpcBatch::new();
    let responses = batch
        .add(abi::erc20::functions::Decimals {}, hex::decode(token_address).unwrap())
        .add(abi::erc20::functions::Name {}, hex::decode(token_address).unwrap())
        .add(abi::erc20::functions::Symbol {}, hex::decode(token_address).unwrap())
        .execute()
        .unwrap()
        .responses;

    let decimals: u64;
    match RpcBatch::decode::<_, abi::erc20::functions::Decimals>(&responses[0]) {
        Some(decoded_decimals) => {
            // if the number of decimals are bigger than 255, we ignore the token
            if decoded_decimals >= BigInt::from(255) {
                log::info!(
                    "ignoring token address {} because the decimals are bigger than 255",
                    token_address,
                );
                return None;
            }
            decimals = decoded_decimals.to_u64();
        }
        None => match utils::get_static_uniswap_tokens(token_address.encode_to_vec().as_slice()) {
            Some(token) => decimals = token.decimals,
            None => {
                log::debug!(
                    "{} is not an ERC20 token contract decimal `eth_call` failed",
                    Hex(&token_address),
                );

                return None;
            }
        },
    };
    log::debug!("decoded_decimals ok");

    let name: String;
    match RpcBatch::decode::<_, abi::erc20::functions::Name>(&responses[1]) {
        Some(decoded_name) => {
            name = decoded_name;
        }
        None => match utils::get_static_uniswap_tokens(token_address.encode_to_vec().as_slice()) {
            Some(token) => name = token.name,
            None => {
                log::debug!(
                    "{} is not an ERC20 token contract name `eth_call` failed",
                    &token_address,
                );
                name = eth::read_string_from_bytes(responses[1].raw.as_ref());
            }
        },
    };
    log::debug!("decoded_name ok");

    let symbol: String;
    match RpcBatch::decode::<_, abi::erc20::functions::Symbol>(&responses[2]) {
        Some(decoded_symbol) => {
            symbol = decoded_symbol;
        }
        None => match utils::get_static_uniswap_tokens(token_address.encode_to_vec().as_slice()) {
            Some(token) => symbol = token.symbol,
            None => {
                log::debug!(
                    "{} is not a an ERC20 token contract symbol `eth_call` failed",
                    &token_address,
                );
                symbol = eth::read_string_from_bytes(responses[2].raw.as_ref());
            }
        },
    };
    log::debug!("decoded_symbol ok");

    return Some(Erc20Token {
        address: token_address.clone(),
        name,
        symbol,
        decimals,
        total_supply: "".to_string(),
        whitelist_pools: vec![],
    });
}

pub fn token_total_supply_call(token_address: &String) -> Option<BigInt> {
    let token_supply = abi::erc20::functions::TotalSupply {};
    if let Some(token_supply_result) = token_supply.call(hex::decode(token_address).unwrap()) {
        return Some(token_supply_result);
    };

    return None;
}
