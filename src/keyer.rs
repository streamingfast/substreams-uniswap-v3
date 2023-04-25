use crate::utils;
use substreams::Hex;

pub const UNISWAP_DAY_DATA: &str = "UniswapDayData";
pub const POOL_DAY_DATA: &str = "PoolDayData";
pub const POOL_HOUR_DATA: &str = "PoolHourData";
pub const TOKEN_DAY_DATA: &str = "TokenDayData";
pub const TOKEN_HOUR_DATA: &str = "TokenHourData";

// ------------------------------------------------
//      store_pools
// ------------------------------------------------
pub fn pool_key(pool_address: &String) -> String {
    format!("pool:{}", pool_address)
}

pub fn pool_token_index_key<T>(token0_address: T, token1_address: T, fee: &String) -> String
where
    T: AsRef<str>,
{
    format!(
        "index:{}:{}",
        generate_tokens_key(token0_address.as_ref(), token1_address.as_ref()),
        fee
    )
}

pub fn token_key(token_address: &String) -> String {
    format!("token:{}", token_address)
}

pub fn token_day_data_token_key(token_address: &String, day_id: String) -> String {
    format!("{}:{}:{}", TOKEN_DAY_DATA, token_address, day_id)
}

pub fn token_hour_data_token_key(token_address: &String, hour_id: String) -> String {
    format!("{}:{}:{}", TOKEN_HOUR_DATA, token_address, hour_id)
}

// ------------------------------------------------
//      store_tokens_whitelist_pools
// ------------------------------------------------
pub fn token_pool_whitelist(token_address: &String) -> String {
    format!("token:{}", token_address)
}

// ------------------------------------------------
//      store_pool_sqrt_price
// ------------------------------------------------
pub fn pool_sqrt_price_key(pool_address: &String) -> String {
    format!("sqrt_price:{}", pool_address)
}

pub fn pool_day_data_sqrt_price(pool_address: &String, day_id: String) -> String {
    format!("{}:{}:{}", POOL_DAY_DATA, pool_address, day_id)
}

pub fn pool_hour_data_sqrt_price(pool_address: &String, hour_id: String) -> String {
    format!("{}:{}:{}", POOL_HOUR_DATA, pool_address, hour_id)
}

// ------------------------------------------------
//      store_prices
// ------------------------------------------------
pub fn prices_pool_token_key(pool_address: &String, token_address: &String, token: String) -> String {
    format!("pool:{}:{}:{}", pool_address, token_address, token)
}

// TODO: is the naming here correct?
pub fn prices_token_pair(token_numerator_address: &String, token_denominator_address: &String) -> String {
    format!("pair:{}:{}", token_numerator_address, token_denominator_address)
}

pub fn pool_day_data_token_price(pool_address: &String, token: String, day_id: String) -> String {
    format!("{}:{}:{}:{}", POOL_DAY_DATA, pool_address, token, day_id)
}

pub fn pool_hour_data_token_price(pool_address: &String, token: String, hour_id: String) -> String {
    format!("{}:{}:{}:{}", POOL_HOUR_DATA, pool_address, token, hour_id)
}

pub fn token_day_data_token_price(token_address: &String, day_id: String) -> String {
    format!("{}:{}:{}", TOKEN_DAY_DATA, token_address, day_id)
}

pub fn token_hour_data_token_price(token_address: &String, hour_id: String) -> String {
    format!("{}:{}:{}", TOKEN_HOUR_DATA, token_address, hour_id)
}

pub fn native_token_from_key(key: &String) -> Option<String> {
    let chunks: Vec<&str> = key.split(":").collect();
    if chunks.len() != 3 {
        return None;
    }
    if chunks[0] != "token" {
        return None;
    }
    return Some(chunks[1].to_string());
}

pub fn native_pool_from_key(key: &String) -> Option<(String, String)> {
    let chunks: Vec<&str> = key.split(":").collect();
    if chunks.len() != 4 {
        return None;
    }
    if chunks[0] != "pool" {
        return None;
    }
    return Some((chunks[1].to_string(), chunks[2].to_string()));
}

// ------------------------------------------------
//      store_pool_liquidities
// ------------------------------------------------
pub fn pool_liquidity(pool_address: &String) -> String {
    format!("liquidity:{}", pool_address)
}

pub fn pool_day_data_liquidity(pool_address: &String, day_id: String) -> String {
    format!("{}:{}:{}", POOL_DAY_DATA, pool_address, day_id)
}

pub fn pool_hour_data_liquidity(pool_address: &String, hour_id: String) -> String {
    format!("{}:{}:{}", POOL_HOUR_DATA, pool_address, hour_id)
}

// ------------------------------------------------
//      store_derived_eth_prices
// ------------------------------------------------
pub fn token_eth_price(token_address: &String) -> String {
    format!("token:{}:dprice:eth", token_address)
}

// ------------------------------------------------
//      store_ticks_liquidities
// ------------------------------------------------
pub fn tick_liquidities_net(pool: &String, tick_idx: &String) -> String {
    format!("tick:{}:{}:liquidityNet", pool, tick_idx)
}

pub fn tick_liquidities_gross(pool: &String, tick_idx: &String) -> String {
    format!("tick:{}:{}:liquidityGross", pool, tick_idx)
}

// ------------------------------------------------
//      store_positions_misc
// ------------------------------------------------
pub fn position(id: &String, position_type: &String) -> String {
    format!("position:{}:{}", id, position_type)
}

pub fn position_liquidity(id: &String) -> String {
    format!("position:{}:liquidity", id)
}

pub fn position_deposited_token(id: &String, token: &str) -> String {
    format!("position:{}:deposited{}", id, token)
}

pub fn position_withdrawn_token(id: &String, token: &str) -> String {
    format!("position:{}:withdrawn{}", id, token)
}

pub fn position_collected_fees_token(id: &String, token: &str) -> String {
    format!("position:{}:collectedFees{}", id, token)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigdecimal::BigDecimal;
    use prost::bytes::Buf;
    use std::str::FromStr;

    #[test]
    fn test_bigdecimal_from_string() {
        let bytes: [u8; 32] = [
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4,
        ];
        let bytes_str = Hex(&bytes).to_string();
        println!("{}", bytes_str);
        let eql = bytes_str == "0000000000000000000000000000000000000000000000000000000000000004";
        assert_eq!(true, eql)
    }

    #[test]
    fn test_invalid_token_key() {
        let input = "pool:bb:aa".to_string();
        assert_eq!(None, native_token_from_key(&input));
    }

    #[test]
    fn test_invalid_pool_key() {
        let input = "token:bb".to_string();
        assert_eq!(None, native_pool_from_key(&input));
    }
}
