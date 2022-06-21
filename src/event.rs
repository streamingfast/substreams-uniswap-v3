pub fn is_pair_created_event(sig: &str) -> bool {
    /* keccak value for PoolCreated(address,address,uint24,int24,address) */
    return sig == "783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118";
}