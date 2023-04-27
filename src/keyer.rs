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
//      store_prices
// ------------------------------------------------
pub fn prices_pool_token_key(pool_address: &String, token_address: &String, token: String) -> String {
    format!("pool:{}:{}:{}", pool_address, token_address, token)
}

// TODO: is the naming here correct?
pub fn prices_token_pair(token_numerator_address: &String, token_denominator_address: &String) -> String {
    format!("pair:{}:{}", token_numerator_address, token_denominator_address)
}

// ------------------------------------------------
//      store_derived_eth_prices
// ------------------------------------------------
pub fn token_eth_price(token_address: &String) -> String {
    format!("token:{}:dprice:eth", token_address)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigdecimal::BigDecimal;
    use prost::bytes::Buf;
    use std::str::FromStr;
    use substreams::Hex;

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
}
