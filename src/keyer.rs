use std::io::Sink;

pub fn pool_sqrt_price_key(pool_address: &String) -> String {
    format!("sqrt_price:{}", pool_address)
}

pub fn liquidity_pool(pool_address: &String) -> String {
    format!("liquidity:{}", pool_address)
}

pub fn toal_value_locked_0(token0_address: &String,token1_address: &String) -> String {
    format!("total_value_locked:{}:{}", token0_address, token1_address)
}

pub fn toal_value_locked_1(token0_address: &String,token1_address: &String) -> String {
    format!("total_value_locked:{}:{}", token0_address, token1_address)
}


