use std::io::Sink;

pub fn pool_sqrt_price_key(pool_address: &String) -> String {
    format!("sqrt_price:{}", pool_address)
}

pub fn liquidity_pool(pool_address: &String) -> String {
    format!("liquidity:{}", pool_address)
}

pub fn token_total_value_locked(token_address: &String) -> String {
    format!("token:{}", token_address)
}

pub fn pool_total_value_locked_token0(pool_address: &String, token_index: &String) -> String {
    format!("pool:{}:token{}", pool_address, token_index)
}