pub fn is_pool_created_event(signature: &str) -> bool {
    /* keccak value for PoolCreated(address,address,uint24,int24,address) */
    return signature == "783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118";
}

pub fn is_pool_sync_event(signature: &str) -> bool {
    return false
}

pub fn is_fee_amount_enabled(signature: &str) -> bool {
    /* keccak value for FeeAmountEnabled(uint24,int24) */
    return signature == "c66a3fdf07232cdd185febcc6579d408c241b47ae2f9907d84be655141eeaecc";
}