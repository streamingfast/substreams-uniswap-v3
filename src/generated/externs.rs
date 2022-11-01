use substreams::prelude::*;
use substreams::errors::Error;
use crate::pb;
use crate::generated::substreams::{Substreams, SubstreamsTrait};


#[no_mangle]
pub extern "C" fn map_pools_created(
    block_ptr: *mut u8,
    block_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||-> Result<pb::uniswap_types_v1::Pools, Error>{
        
        let block: substreams_ethereum::pb::eth::v2::Block = substreams::proto::decode_ptr(block_ptr, block_len).unwrap();

        Substreams::map_pools_created(block,
            
        )
    };
    let result = func();
    if result.is_err() {
        panic!("{:?}", &result.err().unwrap());
    }
    substreams::output(result.unwrap());
}

#[no_mangle]
pub extern "C" fn store_pools(
    map_pools_created_ptr: *mut u8,
    map_pools_created_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||{
        
        let store: substreams::store::StoreSetProto<pb::uniswap_types_v1::Pool> = substreams::store::StoreSetProto::new();
        
        let map_pools_created: pb::uniswap_types_v1::Pools = substreams::proto::decode_ptr(map_pools_created_ptr, map_pools_created_len).unwrap();

        Substreams::store_pools(map_pools_created,
            store,
        )
    };
    func()
}

#[no_mangle]
pub extern "C" fn store_pool_count(
    map_pools_created_ptr: *mut u8,
    map_pools_created_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||{
        
        let store: substreams::store::StoreAddBigInt = substreams::store::StoreAddBigInt::new();
        
        let map_pools_created: pb::uniswap_types_v1::Pools = substreams::proto::decode_ptr(map_pools_created_ptr, map_pools_created_len).unwrap();

        Substreams::store_pool_count(map_pools_created,
            store,
        )
    };
    func()
}

#[no_mangle]
pub extern "C" fn map_tokens_whitelist_pools(
    map_pools_created_ptr: *mut u8,
    map_pools_created_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||-> Result<pb::uniswap_types_v1::Erc20Tokens, Error>{
        
        let map_pools_created: pb::uniswap_types_v1::Pools = substreams::proto::decode_ptr(map_pools_created_ptr, map_pools_created_len).unwrap();

        Substreams::map_tokens_whitelist_pools(map_pools_created,
            
        )
    };
    let result = func();
    if result.is_err() {
        panic!("{:?}", &result.err().unwrap());
    }
    substreams::output(result.unwrap());
}

#[no_mangle]
pub extern "C" fn store_tokens_whitelist_pools(
    map_tokens_whitelist_pools_ptr: *mut u8,
    map_tokens_whitelist_pools_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||{
        
        let store: substreams::store::StoreAppend<String> = substreams::store::StoreAppend::new();
        
        let map_tokens_whitelist_pools: pb::uniswap_types_v1::Erc20Tokens = substreams::proto::decode_ptr(map_tokens_whitelist_pools_ptr, map_tokens_whitelist_pools_len).unwrap();

        Substreams::store_tokens_whitelist_pools(map_tokens_whitelist_pools,
            store,
        )
    };
    func()
}

#[no_mangle]
pub extern "C" fn map_pool_sqrt_price(
    block_ptr: *mut u8,
    block_len: usize,
    store_pools_ptr: u32,
) {
    substreams::register_panic_hook();
    let func = ||-> Result<pb::uniswap_types_v1::PoolSqrtPrices, Error>{
        
        let block: substreams_ethereum::pb::eth::v2::Block = substreams::proto::decode_ptr(block_ptr, block_len).unwrap();
        let store_pools: substreams::store::StoreGetProto<pb::uniswap_types_v1::Pool>  = substreams::store::StoreGetProto::new(store_pools_ptr);

        Substreams::map_pool_sqrt_price(block,
            store_pools,
            
        )
    };
    let result = func();
    if result.is_err() {
        panic!("{:?}", &result.err().unwrap());
    }
    substreams::output(result.unwrap());
}

#[no_mangle]
pub extern "C" fn store_pool_sqrt_price(
    map_pool_sqrt_price_ptr: *mut u8,
    map_pool_sqrt_price_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||{
        
        let store: substreams::store::StoreSetProto<pb::uniswap_types_v1::PoolSqrtPrice> = substreams::store::StoreSetProto::new();
        
        let map_pool_sqrt_price: pb::uniswap_types_v1::PoolSqrtPrices = substreams::proto::decode_ptr(map_pool_sqrt_price_ptr, map_pool_sqrt_price_len).unwrap();

        Substreams::store_pool_sqrt_price(map_pool_sqrt_price,
            store,
        )
    };
    func()
}

#[no_mangle]
pub extern "C" fn store_prices(
    map_pool_sqrt_price_ptr: *mut u8,
    map_pool_sqrt_price_len: usize,
    store_pools_ptr: u32,
) {
    substreams::register_panic_hook();
    let func = ||{
        
        let store: substreams::store::StoreSetBigDecimal = substreams::store::StoreSetBigDecimal::new();
        
        let map_pool_sqrt_price: pb::uniswap_types_v1::PoolSqrtPrices = substreams::proto::decode_ptr(map_pool_sqrt_price_ptr, map_pool_sqrt_price_len).unwrap();
        let store_pools: substreams::store::StoreGetProto<pb::uniswap_types_v1::Pool>  = substreams::store::StoreGetProto::new(store_pools_ptr);

        Substreams::store_prices(map_pool_sqrt_price,
            store_pools,
            store,
        )
    };
    func()
}

#[no_mangle]
pub extern "C" fn map_pool_liquidities(
    block_ptr: *mut u8,
    block_len: usize,
    store_pools_ptr: u32,
) {
    substreams::register_panic_hook();
    let func = ||-> Result<pb::uniswap_types_v1::PoolLiquidities, Error>{
        
        let block: substreams_ethereum::pb::eth::v2::Block = substreams::proto::decode_ptr(block_ptr, block_len).unwrap();
        let store_pools: substreams::store::StoreGetProto<pb::uniswap_types_v1::Pool>  = substreams::store::StoreGetProto::new(store_pools_ptr);

        Substreams::map_pool_liquidities(block,
            store_pools,
            
        )
    };
    let result = func();
    if result.is_err() {
        panic!("{:?}", &result.err().unwrap());
    }
    substreams::output(result.unwrap());
}

#[no_mangle]
pub extern "C" fn store_pool_liquidities(
    map_pool_liquidities_ptr: *mut u8,
    map_pool_liquidities_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||{
        
        let store: substreams::store::StoreSetBigInt = substreams::store::StoreSetBigInt::new();
        
        let map_pool_liquidities: pb::uniswap_types_v1::PoolLiquidities = substreams::proto::decode_ptr(map_pool_liquidities_ptr, map_pool_liquidities_len).unwrap();

        Substreams::store_pool_liquidities(map_pool_liquidities,
            store,
        )
    };
    func()
}

#[no_mangle]
pub extern "C" fn map_swaps_mints_burns(
    block_ptr: *mut u8,
    block_len: usize,
    store_pools_ptr: u32,
) {
    substreams::register_panic_hook();
    let func = ||-> Result<pb::uniswap_types_v1::Events, Error>{
        
        let block: substreams_ethereum::pb::eth::v2::Block = substreams::proto::decode_ptr(block_ptr, block_len).unwrap();
        let store_pools: substreams::store::StoreGetProto<pb::uniswap_types_v1::Pool>  = substreams::store::StoreGetProto::new(store_pools_ptr);

        Substreams::map_swaps_mints_burns(block,
            store_pools,
            
        )
    };
    let result = func();
    if result.is_err() {
        panic!("{:?}", &result.err().unwrap());
    }
    substreams::output(result.unwrap());
}

#[no_mangle]
pub extern "C" fn map_event_amounts(
    map_swaps_mints_burns_ptr: *mut u8,
    map_swaps_mints_burns_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||-> Result<pb::uniswap_types_v1::EventAmounts, Error>{
        
        let map_swaps_mints_burns: pb::uniswap_types_v1::Events = substreams::proto::decode_ptr(map_swaps_mints_burns_ptr, map_swaps_mints_burns_len).unwrap();

        Substreams::map_event_amounts(map_swaps_mints_burns,
            
        )
    };
    let result = func();
    if result.is_err() {
        panic!("{:?}", &result.err().unwrap());
    }
    substreams::output(result.unwrap());
}

#[no_mangle]
pub extern "C" fn map_transactions(
    block_ptr: *mut u8,
    block_len: usize,
    store_pools_ptr: u32,
) {
    substreams::register_panic_hook();
    let func = ||-> Result<pb::uniswap_types_v1::Transactions, Error>{
        
        let block: substreams_ethereum::pb::eth::v2::Block = substreams::proto::decode_ptr(block_ptr, block_len).unwrap();
        let store_pools: substreams::store::StoreGetProto<pb::uniswap_types_v1::Pool>  = substreams::store::StoreGetProto::new(store_pools_ptr);

        Substreams::map_transactions(block,
            store_pools,
            
        )
    };
    let result = func();
    if result.is_err() {
        panic!("{:?}", &result.err().unwrap());
    }
    substreams::output(result.unwrap());
}

#[no_mangle]
pub extern "C" fn store_totals(
    clock_ptr: *mut u8,
    clock_len: usize,
    store_eth_prices_ptr: u32,
    store_total_value_locked_deltas_ptr: *mut u8,
    store_total_value_locked_deltas_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||{
        
        let store: substreams::store::StoreAddBigDecimal = substreams::store::StoreAddBigDecimal::new();
        
        let clock: substreams::pb::substreams::Clock = substreams::proto::decode_ptr(clock_ptr, clock_len).unwrap();
        let store_eth_prices: substreams::store::StoreGetBigDecimal = substreams::store::StoreGetBigDecimal::new(store_eth_prices_ptr);
        let raw_store_total_value_locked_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_total_value_locked_deltas_ptr, store_total_value_locked_deltas_len).unwrap().deltas;
		let store_total_value_locked_deltas: substreams::store::Deltas<substreams::store::DeltaBigDecimal> = substreams::store::Deltas::new(raw_store_total_value_locked_deltas);

        Substreams::store_totals(clock,
            store_eth_prices,
            store_total_value_locked_deltas,
            store,
        )
    };
    func()
}

#[no_mangle]
pub extern "C" fn store_total_tx_counts(
    clock_ptr: *mut u8,
    clock_len: usize,
    map_swaps_mints_burns_ptr: *mut u8,
    map_swaps_mints_burns_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||{
        
        let store: substreams::store::StoreAddBigInt = substreams::store::StoreAddBigInt::new();
        
        let clock: substreams::pb::substreams::Clock = substreams::proto::decode_ptr(clock_ptr, clock_len).unwrap();
        let map_swaps_mints_burns: pb::uniswap_types_v1::Events = substreams::proto::decode_ptr(map_swaps_mints_burns_ptr, map_swaps_mints_burns_len).unwrap();

        Substreams::store_total_tx_counts(clock,
            map_swaps_mints_burns,
            store,
        )
    };
    func()
}

#[no_mangle]
pub extern "C" fn store_swaps_volume(
    clock_ptr: *mut u8,
    clock_len: usize,
    map_swaps_mints_burns_ptr: *mut u8,
    map_swaps_mints_burns_len: usize,
    store_pools_ptr: u32,
    store_total_tx_counts_ptr: u32,
    store_eth_prices_ptr: u32,
) {
    substreams::register_panic_hook();
    let func = ||{
        
        let store: substreams::store::StoreAddBigDecimal = substreams::store::StoreAddBigDecimal::new();
        
        let clock: substreams::pb::substreams::Clock = substreams::proto::decode_ptr(clock_ptr, clock_len).unwrap();
        let map_swaps_mints_burns: pb::uniswap_types_v1::Events = substreams::proto::decode_ptr(map_swaps_mints_burns_ptr, map_swaps_mints_burns_len).unwrap();
        let store_pools: substreams::store::StoreGetProto<pb::uniswap_types_v1::Pool>  = substreams::store::StoreGetProto::new(store_pools_ptr);
        let store_total_tx_counts: substreams::store::StoreGetBigInt = substreams::store::StoreGetBigInt::new(store_total_tx_counts_ptr);
        let store_eth_prices: substreams::store::StoreGetBigDecimal = substreams::store::StoreGetBigDecimal::new(store_eth_prices_ptr);

        Substreams::store_swaps_volume(clock,
            map_swaps_mints_burns,
            store_pools,
            store_total_tx_counts,
            store_eth_prices,
            store,
        )
    };
    func()
}

#[no_mangle]
pub extern "C" fn store_pool_fee_growth_global_x128(
    map_pools_created_ptr: *mut u8,
    map_pools_created_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||{
        
        let store: substreams::store::StoreSetBigInt = substreams::store::StoreSetBigInt::new();
        
        let map_pools_created: pb::uniswap_types_v1::Pools = substreams::proto::decode_ptr(map_pools_created_ptr, map_pools_created_len).unwrap();

        Substreams::store_pool_fee_growth_global_x128(map_pools_created,
            store,
        )
    };
    func()
}

#[no_mangle]
pub extern "C" fn store_native_total_value_locked(
    map_event_amounts_ptr: *mut u8,
    map_event_amounts_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||{
        
        let store: substreams::store::StoreAddBigDecimal = substreams::store::StoreAddBigDecimal::new();
        
        let map_event_amounts: pb::uniswap_types_v1::EventAmounts = substreams::proto::decode_ptr(map_event_amounts_ptr, map_event_amounts_len).unwrap();

        Substreams::store_native_total_value_locked(map_event_amounts,
            store,
        )
    };
    func()
}

#[no_mangle]
pub extern "C" fn store_eth_prices(
    map_pool_sqrt_price_ptr: *mut u8,
    map_pool_sqrt_price_len: usize,
    store_pools_ptr: u32,
    store_prices_ptr: u32,
    store_tokens_whitelist_pools_ptr: u32,
    store_native_total_value_locked_ptr: u32,
    store_pool_liquidities_ptr: u32,
) {
    substreams::register_panic_hook();
    let func = ||{
        
        let store: substreams::store::StoreSetBigDecimal = substreams::store::StoreSetBigDecimal::new();
        
        let map_pool_sqrt_price: pb::uniswap_types_v1::PoolSqrtPrices = substreams::proto::decode_ptr(map_pool_sqrt_price_ptr, map_pool_sqrt_price_len).unwrap();
        let store_pools: substreams::store::StoreGetProto<pb::uniswap_types_v1::Pool>  = substreams::store::StoreGetProto::new(store_pools_ptr);
        let store_prices: substreams::store::StoreGetBigDecimal = substreams::store::StoreGetBigDecimal::new(store_prices_ptr);
        let store_tokens_whitelist_pools: substreams::store::StoreGetRaw = substreams::store::StoreGetRaw::new(store_tokens_whitelist_pools_ptr);
        let store_native_total_value_locked: substreams::store::StoreGetBigDecimal = substreams::store::StoreGetBigDecimal::new(store_native_total_value_locked_ptr);
        let store_pool_liquidities: substreams::store::StoreGetBigInt = substreams::store::StoreGetBigInt::new(store_pool_liquidities_ptr);

        Substreams::store_eth_prices(map_pool_sqrt_price,
            store_pools,
            store_prices,
            store_tokens_whitelist_pools,
            store_native_total_value_locked,
            store_pool_liquidities,
            store,
        )
    };
    func()
}

#[no_mangle]
pub extern "C" fn store_total_value_locked_by_tokens(
    map_swaps_mints_burns_ptr: *mut u8,
    map_swaps_mints_burns_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||{
        
        let store: substreams::store::StoreAddBigDecimal = substreams::store::StoreAddBigDecimal::new();
        
        let map_swaps_mints_burns: pb::uniswap_types_v1::Events = substreams::proto::decode_ptr(map_swaps_mints_burns_ptr, map_swaps_mints_burns_len).unwrap();

        Substreams::store_total_value_locked_by_tokens(map_swaps_mints_burns,
            store,
        )
    };
    func()
}

#[no_mangle]
pub extern "C" fn store_total_value_locked(
    store_native_total_value_locked_deltas_ptr: *mut u8,
    store_native_total_value_locked_deltas_len: usize,
    store_pools_ptr: u32,
    store_eth_prices_ptr: u32,
) {
    substreams::register_panic_hook();
    let func = ||{
        
        let store: substreams::store::StoreSetBigDecimal = substreams::store::StoreSetBigDecimal::new();
        
        let raw_store_native_total_value_locked_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_native_total_value_locked_deltas_ptr, store_native_total_value_locked_deltas_len).unwrap().deltas;
		let store_native_total_value_locked_deltas: substreams::store::Deltas<substreams::store::DeltaBigDecimal> = substreams::store::Deltas::new(raw_store_native_total_value_locked_deltas);
        let store_pools: substreams::store::StoreGetProto<pb::uniswap_types_v1::Pool>  = substreams::store::StoreGetProto::new(store_pools_ptr);
        let store_eth_prices: substreams::store::StoreGetBigDecimal = substreams::store::StoreGetBigDecimal::new(store_eth_prices_ptr);

        Substreams::store_total_value_locked(store_native_total_value_locked_deltas,
            store_pools,
            store_eth_prices,
            store,
        )
    };
    func()
}

#[no_mangle]
pub extern "C" fn map_ticks(
    map_swaps_mints_burns_ptr: *mut u8,
    map_swaps_mints_burns_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||-> Result<pb::uniswap_types_v1::Ticks, Error>{
        
        let map_swaps_mints_burns: pb::uniswap_types_v1::Events = substreams::proto::decode_ptr(map_swaps_mints_burns_ptr, map_swaps_mints_burns_len).unwrap();

        Substreams::map_ticks(map_swaps_mints_burns,
            
        )
    };
    let result = func();
    if result.is_err() {
        panic!("{:?}", &result.err().unwrap());
    }
    substreams::output(result.unwrap());
}

#[no_mangle]
pub extern "C" fn store_ticks(
    map_ticks_ptr: *mut u8,
    map_ticks_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||{
        
        let store: substreams::store::StoreSetProto<pb::uniswap_types_v1::Tick> = substreams::store::StoreSetProto::new();
        
        let map_ticks: pb::uniswap_types_v1::Ticks = substreams::proto::decode_ptr(map_ticks_ptr, map_ticks_len).unwrap();

        Substreams::store_ticks(map_ticks,
            store,
        )
    };
    func()
}

#[no_mangle]
pub extern "C" fn store_ticks_liquidities(
    map_ticks_ptr: *mut u8,
    map_ticks_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||{
        
        let store: substreams::store::StoreAddBigInt = substreams::store::StoreAddBigInt::new();
        
        let map_ticks: pb::uniswap_types_v1::Ticks = substreams::proto::decode_ptr(map_ticks_ptr, map_ticks_len).unwrap();

        Substreams::store_ticks_liquidities(map_ticks,
            store,
        )
    };
    func()
}

#[no_mangle]
pub extern "C" fn map_all_positions(
    block_ptr: *mut u8,
    block_len: usize,
    store_pools_ptr: u32,
) {
    substreams::register_panic_hook();
    let func = ||-> Result<pb::uniswap_types_v1::Positions, Error>{
        
        let block: substreams_ethereum::pb::eth::v2::Block = substreams::proto::decode_ptr(block_ptr, block_len).unwrap();
        let store_pools: substreams::store::StoreGetProto<pb::uniswap_types_v1::Pool>  = substreams::store::StoreGetProto::new(store_pools_ptr);

        Substreams::map_all_positions(block,
            store_pools,
            
        )
    };
    let result = func();
    if result.is_err() {
        panic!("{:?}", &result.err().unwrap());
    }
    substreams::output(result.unwrap());
}

#[no_mangle]
pub extern "C" fn store_all_positions(
    map_all_positions_ptr: *mut u8,
    map_all_positions_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||{
        
        let store: substreams::store::StoreSetProto<pb::uniswap_types_v1::Position> = substreams::store::StoreSetProto::new();
        
        let map_all_positions: pb::uniswap_types_v1::Positions = substreams::proto::decode_ptr(map_all_positions_ptr, map_all_positions_len).unwrap();

        Substreams::store_all_positions(map_all_positions,
            store,
        )
    };
    func()
}

#[no_mangle]
pub extern "C" fn map_positions(
    block_ptr: *mut u8,
    block_len: usize,
    store_all_positions_ptr: u32,
) {
    substreams::register_panic_hook();
    let func = ||-> Result<pb::uniswap_types_v1::Positions, Error>{
        
        let block: substreams_ethereum::pb::eth::v2::Block = substreams::proto::decode_ptr(block_ptr, block_len).unwrap();
        let store_all_positions: substreams::store::StoreGetProto<pb::uniswap_types_v1::Position>  = substreams::store::StoreGetProto::new(store_all_positions_ptr);

        Substreams::map_positions(block,
            store_all_positions,
            
        )
    };
    let result = func();
    if result.is_err() {
        panic!("{:?}", &result.err().unwrap());
    }
    substreams::output(result.unwrap());
}

#[no_mangle]
pub extern "C" fn store_position_changes(
    map_all_positions_ptr: *mut u8,
    map_all_positions_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||{
        
        let store: substreams::store::StoreAddBigDecimal = substreams::store::StoreAddBigDecimal::new();
        
        let map_all_positions: pb::uniswap_types_v1::Positions = substreams::proto::decode_ptr(map_all_positions_ptr, map_all_positions_len).unwrap();

        Substreams::store_position_changes(map_all_positions,
            store,
        )
    };
    func()
}

#[no_mangle]
pub extern "C" fn map_position_snapshots(
    map_positions_ptr: *mut u8,
    map_positions_len: usize,
    store_position_changes_ptr: u32,
) {
    substreams::register_panic_hook();
    let func = ||-> Result<pb::uniswap_types_v1::SnapshotPositions, Error>{
        
        let map_positions: pb::uniswap_types_v1::Positions = substreams::proto::decode_ptr(map_positions_ptr, map_positions_len).unwrap();
        let store_position_changes: substreams::store::StoreGetBigDecimal = substreams::store::StoreGetBigDecimal::new(store_position_changes_ptr);

        Substreams::map_position_snapshots(map_positions,
            store_position_changes,
            
        )
    };
    let result = func();
    if result.is_err() {
        panic!("{:?}", &result.err().unwrap());
    }
    substreams::output(result.unwrap());
}

#[no_mangle]
pub extern "C" fn store_swaps(
    map_swaps_mints_burns_ptr: *mut u8,
    map_swaps_mints_burns_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||{
        
        let store: substreams::store::StoreSetProto<pb::uniswap_types_v1::Swap> = substreams::store::StoreSetProto::new();
        
        let map_swaps_mints_burns: pb::uniswap_types_v1::Events = substreams::proto::decode_ptr(map_swaps_mints_burns_ptr, map_swaps_mints_burns_len).unwrap();

        Substreams::store_swaps(map_swaps_mints_burns,
            store,
        )
    };
    func()
}

#[no_mangle]
pub extern "C" fn map_fees(
    block_ptr: *mut u8,
    block_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||-> Result<pb::uniswap_types_v1::Fees, Error>{
        
        let block: substreams_ethereum::pb::eth::v2::Block = substreams::proto::decode_ptr(block_ptr, block_len).unwrap();

        Substreams::map_fees(block,
            
        )
    };
    let result = func();
    if result.is_err() {
        panic!("{:?}", &result.err().unwrap());
    }
    substreams::output(result.unwrap());
}

#[no_mangle]
pub extern "C" fn store_fees(
    block_ptr: *mut u8,
    block_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||{
        
        let store: substreams::store::StoreSetProto<pb::uniswap_types_v1::Fee> = substreams::store::StoreSetProto::new();
        
        let block: substreams_ethereum::pb::eth::v2::Block = substreams::proto::decode_ptr(block_ptr, block_len).unwrap();

        Substreams::store_fees(block,
            store,
        )
    };
    func()
}

#[no_mangle]
pub extern "C" fn map_flashes(
    block_ptr: *mut u8,
    block_len: usize,
    store_pools_ptr: u32,
) {
    substreams::register_panic_hook();
    let func = ||-> Result<pb::uniswap_types_v1::Flashes, Error>{
        
        let block: substreams_ethereum::pb::eth::v2::Block = substreams::proto::decode_ptr(block_ptr, block_len).unwrap();
        let store_pools: substreams::store::StoreGetProto<pb::uniswap_types_v1::Pool>  = substreams::store::StoreGetProto::new(store_pools_ptr);

        Substreams::map_flashes(block,
            store_pools,
            
        )
    };
    let result = func();
    if result.is_err() {
        panic!("{:?}", &result.err().unwrap());
    }
    substreams::output(result.unwrap());
}

#[no_mangle]
pub extern "C" fn map_bundle_entities(
    block_ptr: *mut u8,
    block_len: usize,
    store_eth_prices_deltas_ptr: *mut u8,
    store_eth_prices_deltas_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||-> Result<substreams_entity_change::pb::entity::EntityChanges, Error>{
        
        let block: substreams_ethereum::pb::eth::v2::Block = substreams::proto::decode_ptr(block_ptr, block_len).unwrap();
        let raw_store_eth_prices_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_eth_prices_deltas_ptr, store_eth_prices_deltas_len).unwrap().deltas;
		let store_eth_prices_deltas: substreams::store::Deltas<substreams::store::DeltaBigDecimal> = substreams::store::Deltas::new(raw_store_eth_prices_deltas);

        Substreams::map_bundle_entities(block,
            store_eth_prices_deltas,
            
        )
    };
    let result = func();
    if result.is_err() {
        panic!("{:?}", &result.err().unwrap());
    }
    substreams::output(result.unwrap());
}

#[no_mangle]
pub extern "C" fn map_factory_entities(
    block_ptr: *mut u8,
    block_len: usize,
    store_pool_count_deltas_ptr: *mut u8,
    store_pool_count_deltas_len: usize,
    store_total_tx_counts_deltas_ptr: *mut u8,
    store_total_tx_counts_deltas_len: usize,
    store_swaps_volume_deltas_ptr: *mut u8,
    store_swaps_volume_deltas_len: usize,
    store_totals_deltas_ptr: *mut u8,
    store_totals_deltas_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||-> Result<substreams_entity_change::pb::entity::EntityChanges, Error>{
        
        let block: substreams_ethereum::pb::eth::v2::Block = substreams::proto::decode_ptr(block_ptr, block_len).unwrap();
        let raw_store_pool_count_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_pool_count_deltas_ptr, store_pool_count_deltas_len).unwrap().deltas;
		let store_pool_count_deltas: substreams::store::Deltas<substreams::store::DeltaBigInt> = substreams::store::Deltas::new(raw_store_pool_count_deltas);
        let raw_store_total_tx_counts_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_total_tx_counts_deltas_ptr, store_total_tx_counts_deltas_len).unwrap().deltas;
		let store_total_tx_counts_deltas: substreams::store::Deltas<substreams::store::DeltaBigInt> = substreams::store::Deltas::new(raw_store_total_tx_counts_deltas);
        let raw_store_swaps_volume_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_swaps_volume_deltas_ptr, store_swaps_volume_deltas_len).unwrap().deltas;
		let store_swaps_volume_deltas: substreams::store::Deltas<substreams::store::DeltaBigDecimal> = substreams::store::Deltas::new(raw_store_swaps_volume_deltas);
        let raw_store_totals_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_totals_deltas_ptr, store_totals_deltas_len).unwrap().deltas;
		let store_totals_deltas: substreams::store::Deltas<substreams::store::DeltaBigDecimal> = substreams::store::Deltas::new(raw_store_totals_deltas);

        Substreams::map_factory_entities(block,
            store_pool_count_deltas,
            store_total_tx_counts_deltas,
            store_swaps_volume_deltas,
            store_totals_deltas,
            
        )
    };
    let result = func();
    if result.is_err() {
        panic!("{:?}", &result.err().unwrap());
    }
    substreams::output(result.unwrap());
}

#[no_mangle]
pub extern "C" fn map_pool_entities(
    map_pools_created_ptr: *mut u8,
    map_pools_created_len: usize,
    store_pool_sqrt_price_deltas_ptr: *mut u8,
    store_pool_sqrt_price_deltas_len: usize,
    store_pool_liquidities_deltas_ptr: *mut u8,
    store_pool_liquidities_deltas_len: usize,
    store_total_value_locked_deltas_ptr: *mut u8,
    store_total_value_locked_deltas_len: usize,
    store_total_value_locked_by_tokens_deltas_ptr: *mut u8,
    store_total_value_locked_by_tokens_deltas_len: usize,
    store_pool_fee_growth_global_x128_deltas_ptr: *mut u8,
    store_pool_fee_growth_global_x128_deltas_len: usize,
    store_prices_deltas_ptr: *mut u8,
    store_prices_deltas_len: usize,
    store_total_tx_counts_deltas_ptr: *mut u8,
    store_total_tx_counts_deltas_len: usize,
    store_swaps_volume_deltas_ptr: *mut u8,
    store_swaps_volume_deltas_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||-> Result<substreams_entity_change::pb::entity::EntityChanges, Error>{
        
        let map_pools_created: pb::uniswap_types_v1::Pools = substreams::proto::decode_ptr(map_pools_created_ptr, map_pools_created_len).unwrap();
        let raw_store_pool_sqrt_price_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_pool_sqrt_price_deltas_ptr, store_pool_sqrt_price_deltas_len).unwrap().deltas;
		let store_pool_sqrt_price_deltas: substreams::store::Deltas<substreams::store::DeltaProto<pb::uniswap_types_v1::PoolSqrtPrice>> = substreams::store::Deltas::new(raw_store_pool_sqrt_price_deltas);
        let raw_store_pool_liquidities_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_pool_liquidities_deltas_ptr, store_pool_liquidities_deltas_len).unwrap().deltas;
		let store_pool_liquidities_deltas: substreams::store::Deltas<substreams::store::DeltaBigInt> = substreams::store::Deltas::new(raw_store_pool_liquidities_deltas);
        let raw_store_total_value_locked_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_total_value_locked_deltas_ptr, store_total_value_locked_deltas_len).unwrap().deltas;
		let store_total_value_locked_deltas: substreams::store::Deltas<substreams::store::DeltaBigDecimal> = substreams::store::Deltas::new(raw_store_total_value_locked_deltas);
        let raw_store_total_value_locked_by_tokens_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_total_value_locked_by_tokens_deltas_ptr, store_total_value_locked_by_tokens_deltas_len).unwrap().deltas;
		let store_total_value_locked_by_tokens_deltas: substreams::store::Deltas<substreams::store::DeltaBigDecimal> = substreams::store::Deltas::new(raw_store_total_value_locked_by_tokens_deltas);
        let raw_store_pool_fee_growth_global_x128_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_pool_fee_growth_global_x128_deltas_ptr, store_pool_fee_growth_global_x128_deltas_len).unwrap().deltas;
		let store_pool_fee_growth_global_x128_deltas: substreams::store::Deltas<substreams::store::DeltaBigInt> = substreams::store::Deltas::new(raw_store_pool_fee_growth_global_x128_deltas);
        let raw_store_prices_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_prices_deltas_ptr, store_prices_deltas_len).unwrap().deltas;
		let store_prices_deltas: substreams::store::Deltas<substreams::store::DeltaBigDecimal> = substreams::store::Deltas::new(raw_store_prices_deltas);
        let raw_store_total_tx_counts_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_total_tx_counts_deltas_ptr, store_total_tx_counts_deltas_len).unwrap().deltas;
		let store_total_tx_counts_deltas: substreams::store::Deltas<substreams::store::DeltaBigInt> = substreams::store::Deltas::new(raw_store_total_tx_counts_deltas);
        let raw_store_swaps_volume_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_swaps_volume_deltas_ptr, store_swaps_volume_deltas_len).unwrap().deltas;
		let store_swaps_volume_deltas: substreams::store::Deltas<substreams::store::DeltaBigDecimal> = substreams::store::Deltas::new(raw_store_swaps_volume_deltas);

        Substreams::map_pool_entities(map_pools_created,
            store_pool_sqrt_price_deltas,
            store_pool_liquidities_deltas,
            store_total_value_locked_deltas,
            store_total_value_locked_by_tokens_deltas,
            store_pool_fee_growth_global_x128_deltas,
            store_prices_deltas,
            store_total_tx_counts_deltas,
            store_swaps_volume_deltas,
            
        )
    };
    let result = func();
    if result.is_err() {
        panic!("{:?}", &result.err().unwrap());
    }
    substreams::output(result.unwrap());
}

#[no_mangle]
pub extern "C" fn map_tokens_entities(
    map_pools_created_ptr: *mut u8,
    map_pools_created_len: usize,
    store_swaps_volume_deltas_ptr: *mut u8,
    store_swaps_volume_deltas_len: usize,
    store_total_tx_counts_deltas_ptr: *mut u8,
    store_total_tx_counts_deltas_len: usize,
    store_total_value_locked_by_tokens_deltas_ptr: *mut u8,
    store_total_value_locked_by_tokens_deltas_len: usize,
    store_total_value_locked_deltas_ptr: *mut u8,
    store_total_value_locked_deltas_len: usize,
    store_eth_prices_deltas_ptr: *mut u8,
    store_eth_prices_deltas_len: usize,
    store_tokens_whitelist_pools_deltas_ptr: *mut u8,
    store_tokens_whitelist_pools_deltas_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||-> Result<substreams_entity_change::pb::entity::EntityChanges, Error>{
        
        let map_pools_created: pb::uniswap_types_v1::Pools = substreams::proto::decode_ptr(map_pools_created_ptr, map_pools_created_len).unwrap();
        let raw_store_swaps_volume_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_swaps_volume_deltas_ptr, store_swaps_volume_deltas_len).unwrap().deltas;
		let store_swaps_volume_deltas: substreams::store::Deltas<substreams::store::DeltaBigDecimal> = substreams::store::Deltas::new(raw_store_swaps_volume_deltas);
        let raw_store_total_tx_counts_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_total_tx_counts_deltas_ptr, store_total_tx_counts_deltas_len).unwrap().deltas;
		let store_total_tx_counts_deltas: substreams::store::Deltas<substreams::store::DeltaBigInt> = substreams::store::Deltas::new(raw_store_total_tx_counts_deltas);
        let raw_store_total_value_locked_by_tokens_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_total_value_locked_by_tokens_deltas_ptr, store_total_value_locked_by_tokens_deltas_len).unwrap().deltas;
		let store_total_value_locked_by_tokens_deltas: substreams::store::Deltas<substreams::store::DeltaBigDecimal> = substreams::store::Deltas::new(raw_store_total_value_locked_by_tokens_deltas);
        let raw_store_total_value_locked_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_total_value_locked_deltas_ptr, store_total_value_locked_deltas_len).unwrap().deltas;
		let store_total_value_locked_deltas: substreams::store::Deltas<substreams::store::DeltaBigDecimal> = substreams::store::Deltas::new(raw_store_total_value_locked_deltas);
        let raw_store_eth_prices_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_eth_prices_deltas_ptr, store_eth_prices_deltas_len).unwrap().deltas;
		let store_eth_prices_deltas: substreams::store::Deltas<substreams::store::DeltaBigDecimal> = substreams::store::Deltas::new(raw_store_eth_prices_deltas);
        let raw_store_tokens_whitelist_pools_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_tokens_whitelist_pools_deltas_ptr, store_tokens_whitelist_pools_deltas_len).unwrap().deltas;
		let store_tokens_whitelist_pools_deltas: substreams::store::Deltas<substreams::store::DeltaArray<String>> = substreams::store::Deltas::new(raw_store_tokens_whitelist_pools_deltas);

        Substreams::map_tokens_entities(map_pools_created,
            store_swaps_volume_deltas,
            store_total_tx_counts_deltas,
            store_total_value_locked_by_tokens_deltas,
            store_total_value_locked_deltas,
            store_eth_prices_deltas,
            store_tokens_whitelist_pools_deltas,
            
        )
    };
    let result = func();
    if result.is_err() {
        panic!("{:?}", &result.err().unwrap());
    }
    substreams::output(result.unwrap());
}

#[no_mangle]
pub extern "C" fn map_tick_entities(
    store_ticks_deltas_ptr: *mut u8,
    store_ticks_deltas_len: usize,
    store_ticks_liquidities_deltas_ptr: *mut u8,
    store_ticks_liquidities_deltas_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||-> Result<substreams_entity_change::pb::entity::EntityChanges, Error>{
        
        let raw_store_ticks_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_ticks_deltas_ptr, store_ticks_deltas_len).unwrap().deltas;
		let store_ticks_deltas: substreams::store::Deltas<substreams::store::DeltaProto<pb::uniswap_types_v1::Tick>> = substreams::store::Deltas::new(raw_store_ticks_deltas);
        let raw_store_ticks_liquidities_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_ticks_liquidities_deltas_ptr, store_ticks_liquidities_deltas_len).unwrap().deltas;
		let store_ticks_liquidities_deltas: substreams::store::Deltas<substreams::store::DeltaBigInt> = substreams::store::Deltas::new(raw_store_ticks_liquidities_deltas);

        Substreams::map_tick_entities(store_ticks_deltas,
            store_ticks_liquidities_deltas,
            
        )
    };
    let result = func();
    if result.is_err() {
        panic!("{:?}", &result.err().unwrap());
    }
    substreams::output(result.unwrap());
}

#[no_mangle]
pub extern "C" fn map_position_entities(
    map_positions_ptr: *mut u8,
    map_positions_len: usize,
    store_position_changes_deltas_ptr: *mut u8,
    store_position_changes_deltas_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||-> Result<substreams_entity_change::pb::entity::EntityChanges, Error>{
        
        let map_positions: pb::uniswap_types_v1::Positions = substreams::proto::decode_ptr(map_positions_ptr, map_positions_len).unwrap();
        let raw_store_position_changes_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_position_changes_deltas_ptr, store_position_changes_deltas_len).unwrap().deltas;
		let store_position_changes_deltas: substreams::store::Deltas<substreams::store::DeltaBigDecimal> = substreams::store::Deltas::new(raw_store_position_changes_deltas);

        Substreams::map_position_entities(map_positions,
            store_position_changes_deltas,
            
        )
    };
    let result = func();
    if result.is_err() {
        panic!("{:?}", &result.err().unwrap());
    }
    substreams::output(result.unwrap());
}

#[no_mangle]
pub extern "C" fn map_position_snapshot_entities(
    map_position_snapshots_ptr: *mut u8,
    map_position_snapshots_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||-> Result<substreams_entity_change::pb::entity::EntityChanges, Error>{
        
        let map_position_snapshots: pb::uniswap_types_v1::SnapshotPositions = substreams::proto::decode_ptr(map_position_snapshots_ptr, map_position_snapshots_len).unwrap();

        Substreams::map_position_snapshot_entities(map_position_snapshots,
            
        )
    };
    let result = func();
    if result.is_err() {
        panic!("{:?}", &result.err().unwrap());
    }
    substreams::output(result.unwrap());
}

#[no_mangle]
pub extern "C" fn map_transaction_entities(
    map_transactions_ptr: *mut u8,
    map_transactions_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||-> Result<substreams_entity_change::pb::entity::EntityChanges, Error>{
        
        let map_transactions: pb::uniswap_types_v1::Transactions = substreams::proto::decode_ptr(map_transactions_ptr, map_transactions_len).unwrap();

        Substreams::map_transaction_entities(map_transactions,
            
        )
    };
    let result = func();
    if result.is_err() {
        panic!("{:?}", &result.err().unwrap());
    }
    substreams::output(result.unwrap());
}

#[no_mangle]
pub extern "C" fn map_swaps_mints_burns_entities(
    map_swaps_mints_burns_ptr: *mut u8,
    map_swaps_mints_burns_len: usize,
    store_total_tx_counts_ptr: u32,
    store_eth_prices_ptr: u32,
) {
    substreams::register_panic_hook();
    let func = ||-> Result<substreams_entity_change::pb::entity::EntityChanges, Error>{
        
        let map_swaps_mints_burns: pb::uniswap_types_v1::Events = substreams::proto::decode_ptr(map_swaps_mints_burns_ptr, map_swaps_mints_burns_len).unwrap();
        let store_total_tx_counts: substreams::store::StoreGetBigInt = substreams::store::StoreGetBigInt::new(store_total_tx_counts_ptr);
        let store_eth_prices: substreams::store::StoreGetBigDecimal = substreams::store::StoreGetBigDecimal::new(store_eth_prices_ptr);

        Substreams::map_swaps_mints_burns_entities(map_swaps_mints_burns,
            store_total_tx_counts,
            store_eth_prices,
            
        )
    };
    let result = func();
    if result.is_err() {
        panic!("{:?}", &result.err().unwrap());
    }
    substreams::output(result.unwrap());
}

#[no_mangle]
pub extern "C" fn map_flash_entities(
    map_flashes_ptr: *mut u8,
    map_flashes_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||-> Result<substreams_entity_change::pb::entity::EntityChanges, Error>{
        
        let map_flashes: pb::uniswap_types_v1::Flashes = substreams::proto::decode_ptr(map_flashes_ptr, map_flashes_len).unwrap();

        Substreams::map_flash_entities(map_flashes,
            
        )
    };
    let result = func();
    if result.is_err() {
        panic!("{:?}", &result.err().unwrap());
    }
    substreams::output(result.unwrap());
}

#[no_mangle]
pub extern "C" fn map_uniswap_day_data_entities(
    store_total_tx_counts_deltas_ptr: *mut u8,
    store_total_tx_counts_deltas_len: usize,
    store_totals_deltas_ptr: *mut u8,
    store_totals_deltas_len: usize,
    store_swaps_volume_deltas_ptr: *mut u8,
    store_swaps_volume_deltas_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||-> Result<substreams_entity_change::pb::entity::EntityChanges, Error>{
        
        let raw_store_total_tx_counts_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_total_tx_counts_deltas_ptr, store_total_tx_counts_deltas_len).unwrap().deltas;
		let store_total_tx_counts_deltas: substreams::store::Deltas<substreams::store::DeltaBigInt> = substreams::store::Deltas::new(raw_store_total_tx_counts_deltas);
        let raw_store_totals_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_totals_deltas_ptr, store_totals_deltas_len).unwrap().deltas;
		let store_totals_deltas: substreams::store::Deltas<substreams::store::DeltaBigDecimal> = substreams::store::Deltas::new(raw_store_totals_deltas);
        let raw_store_swaps_volume_deltas = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(store_swaps_volume_deltas_ptr, store_swaps_volume_deltas_len).unwrap().deltas;
		let store_swaps_volume_deltas: substreams::store::Deltas<substreams::store::DeltaBigDecimal> = substreams::store::Deltas::new(raw_store_swaps_volume_deltas);

        Substreams::map_uniswap_day_data_entities(store_total_tx_counts_deltas,
            store_totals_deltas,
            store_swaps_volume_deltas,
            
        )
    };
    let result = func();
    if result.is_err() {
        panic!("{:?}", &result.err().unwrap());
    }
    substreams::output(result.unwrap());
}

#[no_mangle]
pub extern "C" fn graph_out(
    map_factory_entities_ptr: *mut u8,
    map_factory_entities_len: usize,
    map_bundle_entities_ptr: *mut u8,
    map_bundle_entities_len: usize,
    map_transaction_entities_ptr: *mut u8,
    map_transaction_entities_len: usize,
    map_pool_entities_ptr: *mut u8,
    map_pool_entities_len: usize,
    map_tokens_entities_ptr: *mut u8,
    map_tokens_entities_len: usize,
    map_tick_entities_ptr: *mut u8,
    map_tick_entities_len: usize,
    map_position_entities_ptr: *mut u8,
    map_position_entities_len: usize,
    map_position_snapshot_entities_ptr: *mut u8,
    map_position_snapshot_entities_len: usize,
    map_flash_entities_ptr: *mut u8,
    map_flash_entities_len: usize,
    map_swaps_mints_burns_entities_ptr: *mut u8,
    map_swaps_mints_burns_entities_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||-> Result<substreams_entity_change::pb::entity::EntityChanges, Error>{
        
        let map_factory_entities: substreams_entity_change::pb::entity::EntityChanges = substreams::proto::decode_ptr(map_factory_entities_ptr, map_factory_entities_len).unwrap();
        let map_bundle_entities: substreams_entity_change::pb::entity::EntityChanges = substreams::proto::decode_ptr(map_bundle_entities_ptr, map_bundle_entities_len).unwrap();
        let map_transaction_entities: substreams_entity_change::pb::entity::EntityChanges = substreams::proto::decode_ptr(map_transaction_entities_ptr, map_transaction_entities_len).unwrap();
        let map_pool_entities: substreams_entity_change::pb::entity::EntityChanges = substreams::proto::decode_ptr(map_pool_entities_ptr, map_pool_entities_len).unwrap();
        let map_tokens_entities: substreams_entity_change::pb::entity::EntityChanges = substreams::proto::decode_ptr(map_tokens_entities_ptr, map_tokens_entities_len).unwrap();
        let map_tick_entities: substreams_entity_change::pb::entity::EntityChanges = substreams::proto::decode_ptr(map_tick_entities_ptr, map_tick_entities_len).unwrap();
        let map_position_entities: substreams_entity_change::pb::entity::EntityChanges = substreams::proto::decode_ptr(map_position_entities_ptr, map_position_entities_len).unwrap();
        let map_position_snapshot_entities: substreams_entity_change::pb::entity::EntityChanges = substreams::proto::decode_ptr(map_position_snapshot_entities_ptr, map_position_snapshot_entities_len).unwrap();
        let map_flash_entities: substreams_entity_change::pb::entity::EntityChanges = substreams::proto::decode_ptr(map_flash_entities_ptr, map_flash_entities_len).unwrap();
        let map_swaps_mints_burns_entities: substreams_entity_change::pb::entity::EntityChanges = substreams::proto::decode_ptr(map_swaps_mints_burns_entities_ptr, map_swaps_mints_burns_entities_len).unwrap();

        Substreams::graph_out(map_factory_entities,
            map_bundle_entities,
            map_transaction_entities,
            map_pool_entities,
            map_tokens_entities,
            map_tick_entities,
            map_position_entities,
            map_position_snapshot_entities,
            map_flash_entities,
            map_swaps_mints_burns_entities,
            
        )
    };
    let result = func();
    if result.is_err() {
        panic!("{:?}", &result.err().unwrap());
    }
    substreams::output(result.unwrap());
}

#[no_mangle]
pub extern "C" fn dummy_graph_out_store(
    graph_out_ptr: *mut u8,
    graph_out_len: usize,
) {
    substreams::register_panic_hook();
    let func = ||{
        
        let store: substreams::store::StoreSetString = substreams::store::StoreSetString::new();
        
        let graph_out: substreams_entity_change::pb::entity::EntityChanges = substreams::proto::decode_ptr(graph_out_ptr, graph_out_len).unwrap();

        Substreams::dummy_graph_out_store(graph_out,
            store,
        )
    };
    func()
}
