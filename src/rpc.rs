use crate::{abi, eth, utils, Erc20Token};
use ethabi::Uint;
use substreams::log;
use substreams::scalar::BigInt;
use substreams_ethereum::pb::eth as ethpb;
use substreams_ethereum::rpc::RpcBatch;
use substreams_ethereum::scalar::EthBigInt;

pub fn fee_growth_global_x128_call(pool_address: &String) -> (BigInt, BigInt) {
    let responses = RpcBatch::new()
        .add(abi::pool::functions::FeeGrowthGlobal0X128{}, hex::decode(pool_address).unwrap())
        .add(abi::pool::functions::FeeGrowthGlobal1X128{}, hex::decode(pool_address).unwrap())
        .execute().unwrap().responses;

    log::info!("bytes response.0: {:?}", responses[0].raw);
    log::info!("bytes response.1: {:?}", responses[1].raw);

    let fee_0: BigInt = match RpcBatch::decode::<_, abi::pool::functions::FeeGrowthGlobal0X128>(&responses[0]) {
        Some(data) => {
            let mut v = [0u8; 256usize];
            data.to_big_endian(&mut v);
            BigInt::from_signed_bytes_be(&v)
        },
        None => {
            panic!("Failed to decode fee growth global 1x128");
        },
    };
    let fee_1: BigInt = match RpcBatch::decode::<_, abi::pool::functions::FeeGrowthGlobal1X128>(&responses[1]) {
        Some(data) => {
            let mut v = [0u8; 256usize];
            data.to_big_endian(&mut v);
            BigInt::from_signed_bytes_be(&v)
        },
        None => {
            panic!("Failed to decode fee growth global 1x128");
        },
    };

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
    let batch = substreams_ethereum::rpc::RpcBatch::new();
    let responses = batch
        .add(abi::erc20::functions::Decimals{}, hex::decode(token_address).unwrap())
        .add(abi::erc20::functions::Name{}, hex::decode(token_address).unwrap())
        .add(abi::erc20::functions::Symbol{}, hex::decode(token_address).unwrap())
        .execute().unwrap().responses;

    let mut decimals: u64 = 0;
    match RpcBatch::decode::<_, abi::erc20::functions::Decimals>(&responses[0]) {
        Some(decoded_decimals) => {
            decimals = decoded_decimals.as_u64();
        }
        None => match utils::get_static_uniswap_tokens(token_address.as_str()) {
            Some(token) => decimals = token.decimals,
            None => {
                log::debug!(
                    "{} is not a an ERC20 token contract decimal `eth_call` failed",
                    &token_address,
                );
                return None;
            }
        },
    }
    log::debug!("decoded_decimals ok");

    let mut name = "unknown".to_string();
    match RpcBatch::decode::<_, abi::erc20::functions::Name>(&responses[1]) {
        Some(decoded_name) => {
            name = decoded_name;
        }
        None => match utils::get_static_uniswap_tokens(token_address.as_str()) {
            Some(token) => name = token.name,
            None => {
                log::debug!(
                    "{} is not a an ERC20 token contract name `eth_call` failed",
                    &token_address,
                );
                name = eth::read_string_from_bytes(responses[1].raw.as_ref());
            }
        },
    }
    log::debug!("decoded_name ok");

    let mut symbol = "unknown".to_string();
    match RpcBatch::decode::<_, abi::erc20::functions::Symbol>(&responses[2]) {
        Some(decoded_symbol) => {
            symbol = decoded_symbol;
        }
        None => match utils::get_static_uniswap_tokens(token_address.as_str()) {
            Some(token) => symbol = token.symbol,
            None => {
                log::debug!(
                    "{} is not a an ERC20 token contract symbol `eth_call` failed",
                    &token_address,
                );
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
        total_supply: "".to_string(),
        whitelist_pools: vec![],
    });
}
