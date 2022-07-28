use substreams::Hex;
use crate::utils;
use crate::utils::UNISWAP_V3_FACTORY;

// ------------------------------------------------
//      store_pools
// ------------------------------------------------
pub fn pool_key(pool_address: &String) -> String {
    format!("pool:{}", pool_address)
}

pub fn pool_token_index_key(token0_address: &String,token1_address: &String) -> String {
    format!("index:{}", generate_tokens_key(token0_address.as_str(),token1_address.as_str()))
}

pub fn generate_tokens_key(token0: &str, token1: &str) -> String {
    if token0 > token1 {
        return format!("{}:{}", token1, token0);
    }
    return format!("{}:{}", token0, token1);
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
pub fn prices_pool_token0_key(pool_address: &String) -> String {
    format!("pool:{}:token0", pool_address)
}

pub fn prices_pool_token1_key(pool_address: &String) -> String {
    format!("pool:{}:token1", pool_address)
}

// TODO: is the naming here correct?
pub fn prices_token_pair(token_numerator_address: &String, token_denominator_address: &String) -> String {
    format!("pair:{}:{}", token_numerator_address, token_denominator_address)
}

// ------------------------------------------------
//      store_total_value_locked
// ------------------------------------------------
pub fn token_total_value_locked(token_address: &String) -> String {
    format!("token:{}", token_address)
}

pub fn pool_total_value_locked_token(pool_address: &String, token_address: &String) -> String {
    format!("pool:{}:{}", pool_address, token_address)
}

// ------------------------------------------------
//      store_derived_eth_prices
// ------------------------------------------------
pub fn token_eth_price(token_address: &String) -> String {
    format!("token:{}:dprice:eth", token_address)
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


