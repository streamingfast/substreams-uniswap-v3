// ------------------------------------------------
//      store_pools_created
// ------------------------------------------------
pub fn pool_key(pool_address: &String) -> String {
    format!("pool:{}", pool_address)
}

pub fn token_key(token_address: &String) -> String {
    format!("token:{}", token_address)
}

pub fn token_day_data_token_key(token_address: &String, day_id: String) -> String {
    format!("TokenDayData:{token_address}:{day_id}")
}

pub fn token_hour_data_token_key(token_address: &String, hour_id: String) -> String {
    format!("TokenHourData:{token_address}:{hour_id}")
}

// ------------------------------------------------
//      store_tokens_whitelist_pools
// ------------------------------------------------
pub fn token_pool_whitelist(token_address: &String) -> String {
    format!("token:{}", token_address)
}

// ------------------------------------------------
//      store_prices
// ------------------------------------------------
pub fn prices_pool_token_key(pool_address: &String, token_address: &String, token: String) -> String {
    format!("pool:{}:{}:{}", pool_address, token_address, token)
}
