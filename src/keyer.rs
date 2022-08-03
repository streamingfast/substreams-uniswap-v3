use crate::utils;
use substreams::Hex;

// ------------------------------------------------
//      store_pools
// ------------------------------------------------
pub fn pool_key(pool_address: &String) -> String {
    format!("pool:{}", pool_address)
}

pub fn pool_token_index_key(token0_address: &String, token1_address: &String) -> String {
    format!(
        "index:{}",
        generate_tokens_key(token0_address.as_str(), token1_address.as_str())
    )
}

pub fn generate_tokens_key(token0: &str, token1: &str) -> String {
    if token0 > token1 {
        return format!("{}:{}", token1, token0);
    }
    return format!("{}:{}", token0, token1);
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

// ------------------------------------------------
//      store_prices
// ------------------------------------------------
pub fn prices_pool_token_key(pool_address: &String, token_address: &String) -> String {
    format!("pool:{}:{}", pool_address, token_address)
}

pub fn prices_pool_token1_key(pool_address: &String) -> String {
    format!("pool:{}:token1", pool_address)
}

// TODO: is the naming here correct?
pub fn prices_token_pair(
    token_numerator_address: &String,
    token_denominator_address: &String,
) -> String {
    format!(
        "pair:{}:{}",
        token_numerator_address, token_denominator_address
    )
}

// ------------------------------------------------
//      store_native_total_value_locked && store_total_value_locked
// ------------------------------------------------
pub fn token_native_total_value_locked(token_address: &String) -> String {
    format!("token:{}:native", token_address)
}

pub fn pool_liquidity(pool_address: &String) -> String {
    format!("pool:{}:liquidity", pool_address)
}

pub fn token_usd_total_value_locked(token_address: &String) -> String {
    format!("token:{}:usd", token_address)
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

pub fn pool_native_total_value_locked_token(
    pool_address: &String,
    token_address: &String,
) -> String {
    format!("pool:{}:{}:native", pool_address, token_address)
}

pub fn pool_eth_total_value_locked(pool_address: &String) -> String {
    format!("pool:{}:eth", pool_address)
}
pub fn pool_usd_total_value_locked(pool_address: &String) -> String {
    format!("pool:{}:usd", pool_address)
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
//      store_derived_eth_prices
// ------------------------------------------------
pub fn token_eth_price(token_address: &String) -> String {
    format!("token:{}", token_address)
}

pub fn bundle_eth_price() -> String {
    format!("bundle")
}

// ------------------------------------------------
//      store_total_tx_counts
// ------------------------------------------------
pub fn pool_total_tx_count(pool_address: &String) -> String {
    format!("pool:{}", pool_address)
}

pub fn token_total_tx_count(token_address: &String) -> String {
    format!("token:{}", token_address)
}

pub fn factory_total_tx_count() -> String {
    format!("factory:{}", Hex(utils::UNISWAP_V3_FACTORY))
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::bytes::Buf;

    #[test]
    fn test_valid_token_key() {
        let input = "token:bb".to_string();
        assert_eq!(Some("bb".to_string()), native_token_from_key(&input));
    }

    #[test]
    fn test_invalid_token_key() {
        let input = "pool:bb:aa".to_string();
        assert_eq!(None, native_token_from_key(&input));
    }

    #[test]
    fn test_valid_pool_key() {
        let input = "pool:bb:aa".to_string();
        assert_eq!(
            Some(("bb".to_string(), "aa".to_string())),
            native_pool_from_key(&input)
        );
    }

    #[test]
    fn test_invalid_pool_key() {
        let input = "token:bb".to_string();
        assert_eq!(None, native_pool_from_key(&input));
    }
}
