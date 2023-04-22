use crate::pb::position_event::PositionEventType;
use crate::pb::uniswap::events;
use crate::pb::uniswap::events::position::PositionType::{Collect, DecreaseLiquidity, IncreaseLiquidity, Transfer};
use crate::pb::uniswap::events::{
    CollectPosition, DecreaseLiquidityPosition, IncreaseLiquidityPosition, TransferPosition,
};
use crate::pb::PositionEvent;
use crate::storage::UniswapPoolStorage;
use crate::utils::NON_FUNGIBLE_POSITION_MANAGER;
use crate::{abi, math, rpc, utils, BurnEvent, EventTrait, MintEvent, Pool, SwapEvent};
use substreams::prelude::{BigDecimal, BigInt, StoreGet, StoreGetProto};
use substreams::{log, Hex};
use substreams_ethereum::pb::eth::v2::{Log, StorageChange, TransactionTrace};

pub fn extract_pool_events(
    pool_events: &mut Vec<events::PoolEvent>,
    ticks_created: &mut Vec<events::TickCreated>,
    ticks_updated: &mut Vec<events::TickUpdated>,
    transaction_id: &String,
    origin: &String,
    log: &Log,
    pool: &Pool,
    timestamp_seconds: u64,
    block_number: u64,
    storage_changes: &Vec<StorageChange>,
) {
    let common_tick_updated = events::TickUpdated {
        log_ordinal: log.ordinal,
        pool_address: pool.address.to_string(),
        ..Default::default()
    };
    if let Some(swap) = abi::pool::events::Swap::match_and_decode(log) {
        if !pool.should_handle_swap() {
            return;
        }

        let token0 = pool.token0.as_ref().unwrap();
        let token1 = pool.token1.as_ref().unwrap();

        log::info!("'swap amount 0 {}", swap.amount0);
        log::info!("'swap amount 1 {}", swap.amount1);
        let amount0 = swap.amount0.to_decimal(token0.decimals);
        let amount1 = swap.amount1.to_decimal(token1.decimals);

        pool_events.push(events::PoolEvent {
            log_ordinal: log.ordinal,
            log_index: log.block_index as u64,
            pool_address: pool.address.to_string(),
            token0: token0.address.clone(),
            token1: token1.address.clone(),
            fee: pool.fee_tier.clone(),
            transaction_id: transaction_id.to_string(),
            timestamp: timestamp_seconds,
            created_at_block_number: block_number,
            r#type: Some(SwapEvent(events::pool_event::Swap {
                sender: Hex(&swap.sender).to_string(),
                recipient: Hex(&swap.recipient).to_string(),
                origin: origin.to_string(),
                amount_0: amount0.into(),
                amount_1: amount1.into(),
                sqrt_price: swap.sqrt_price_x96.into(),
                liquidity: swap.liquidity.into(),
                tick: swap.tick.into(),
            })),
        })
    } else if let Some(mint) = abi::pool::events::Mint::match_and_decode(log) {
        log::info!("transaction: {}", transaction_id.to_string());
        if !pool.should_handle_mint_and_burn() {
            return;
        }

        let token0 = pool.token0.as_ref().unwrap();
        let token1 = pool.token1.as_ref().unwrap();
        let amount0 = mint.amount0.to_decimal(token0.decimals);
        let amount1 = mint.amount1.to_decimal(token1.decimals);

        pool_events.push(events::PoolEvent {
            log_ordinal: log.ordinal,
            log_index: log.block_index as u64,
            pool_address: pool.address.to_string(),
            token0: token0.address.clone(),
            token1: token1.address.clone(),
            fee: pool.fee_tier.clone(),
            transaction_id: transaction_id.to_string(),
            timestamp: timestamp_seconds,
            created_at_block_number: block_number,
            r#type: Some(MintEvent(events::pool_event::Mint {
                owner: Hex(&mint.owner).to_string(),
                sender: Hex(&mint.sender).to_string(),
                origin: origin.to_string(),
                amount: mint.amount.to_string(),
                amount_0: amount0.into(),
                amount_1: amount1.into(),
                tick_lower: mint.tick_lower.to_string(),
                tick_upper: mint.tick_upper.to_string(),
            })),
        });

        let storage = UniswapPoolStorage::new(storage_changes, &log.address);
        let create_lower_tick = initialized_changed(storage.get_ticks_initialized(&mint.tick_lower));
        let create_upper_tick = initialized_changed(storage.get_ticks_initialized(&mint.tick_upper));

        if create_lower_tick || create_upper_tick {
            let common_tick = events::TickCreated {
                pool_address: pool.address.to_string(),
                created_at_timestamp: timestamp_seconds,
                created_at_block_number: block_number,
                log_ordinal: log.ordinal,
                amount: mint.amount.into(),
                ..Default::default()
            };

            let mut lower_tick = common_tick.clone();
            let (price0, price1) = prices_from_tick_index(&mint.tick_lower);
            lower_tick.idx = mint.tick_lower.as_ref().into();
            lower_tick.price0 = price0.into();
            lower_tick.price1 = price1.into();
            ticks_created.push(lower_tick);

            let mut upper_tick = common_tick.clone();
            let (price0, price1) = prices_from_tick_index(&mint.tick_upper);
            upper_tick.idx = mint.tick_upper.as_ref().into();
            upper_tick.price0 = price0.into();
            upper_tick.price1 = price1.into();
            ticks_created.push(upper_tick);
        }

        ticks_updated.push(events::TickUpdated {
            idx: mint.tick_upper.as_ref().into(),
            fee_growth_outside_0x_128: bigint_if_some(storage.get_ticks_fee_growth_outside_0_x128(&mint.tick_upper)),
            fee_growth_outside_1x_128: bigint_if_some(storage.get_ticks_fee_growth_outside_1_x128(&mint.tick_upper)),
            ..common_tick_updated.clone()
        });
        ticks_updated.push(events::TickUpdated {
            idx: mint.tick_lower.as_ref().into(),
            fee_growth_outside_0x_128: bigint_if_some(storage.get_ticks_fee_growth_outside_0_x128(&mint.tick_lower)),
            fee_growth_outside_1x_128: bigint_if_some(storage.get_ticks_fee_growth_outside_1_x128(&mint.tick_lower)),
            ..common_tick_updated.clone()
        });
    } else if let Some(burn) = abi::pool::events::Burn::match_and_decode(log) {
        if !pool.should_handle_mint_and_burn() {
            return;
        }

        let token0 = pool.token0.as_ref().unwrap();
        let token1 = pool.token1.as_ref().unwrap();

        let amount0_bi: BigInt = burn.amount0;
        let amount1_bi: BigInt = burn.amount1;
        let amount0 = amount0_bi.to_decimal(token0.decimals);
        let amount1 = amount1_bi.to_decimal(token1.decimals);

        pool_events.push(events::PoolEvent {
            log_ordinal: log.ordinal,
            log_index: log.block_index as u64,
            pool_address: pool.address.to_string(),
            token0: token0.address.clone(),
            token1: token1.address.clone(),
            fee: pool.fee_tier.clone(),
            transaction_id: transaction_id.to_string(),
            timestamp: timestamp_seconds,
            created_at_block_number: block_number,
            r#type: Some(BurnEvent(events::pool_event::Burn {
                owner: Hex(&burn.owner).to_string(),
                origin: origin.to_string(),
                amount: burn.amount.into(),
                amount_0: amount0.into(),
                amount_1: amount1.into(),
                tick_lower: burn.tick_lower.as_ref().into(),
                tick_upper: burn.tick_upper.as_ref().into(),
            })),
        });

        let storage = UniswapPoolStorage::new(storage_changes, &log.address);

        ticks_updated.push(events::TickUpdated {
            idx: burn.tick_upper.as_ref().into(),
            fee_growth_outside_0x_128: bigint_if_some(storage.get_ticks_fee_growth_outside_0_x128(&burn.tick_upper)),
            fee_growth_outside_1x_128: bigint_if_some(storage.get_ticks_fee_growth_outside_1_x128(&burn.tick_upper)),
            ..common_tick_updated.clone()
        });
        ticks_updated.push(events::TickUpdated {
            idx: burn.tick_lower.as_ref().into(),
            fee_growth_outside_0x_128: bigint_if_some(storage.get_ticks_fee_growth_outside_0_x128(&burn.tick_lower)),
            fee_growth_outside_1x_128: bigint_if_some(storage.get_ticks_fee_growth_outside_1_x128(&burn.tick_lower)),
            ..common_tick_updated.clone()
        });
    }
}

fn bigint_if_some(input: Option<(BigInt, BigInt)>) -> String {
    if let Some(el) = input {
        el.1.into()
    } else {
        "".to_string()
    }
}

fn initialized_changed(input: Option<(bool, bool)>) -> bool {
    match input {
        Some((old, new)) => old != new,
        None => false,
    }
}

fn prices_from_tick_index(tick_idx: &BigInt) -> (BigDecimal, BigDecimal) {
    let price0 = math::big_decimal_exponated(BigDecimal::try_from(1.0001).unwrap().with_prec(100), tick_idx.clone());
    let price1 = math::safe_div(&BigDecimal::from(1 as i32), &price0);
    (price0, price1)
}

pub fn extract_pool_liquidities(
    pool_liquidities: &mut Vec<events::PoolLiquidity>,
    log: &Log,
    storage_changes: &Vec<StorageChange>,
    pool: &Pool,
) {
    if let Some(_) = abi::pool::events::Swap::match_and_decode(&log) {
        if !pool.should_handle_swap() {
            return;
        }
        if let Some(pl) = utils::extract_pool_liquidity(log.ordinal, &log.address, storage_changes) {
            pool_liquidities.push(pl)
        }
    } else if let Some(_) = abi::pool::events::Mint::match_and_decode(&log) {
        if !pool.should_handle_mint_and_burn() {
            return;
        }
        if let Some(pl) = utils::extract_pool_liquidity(log.ordinal, &log.address, storage_changes) {
            pool_liquidities.push(pl)
        }
    } else if let Some(_) = abi::pool::events::Burn::match_and_decode(&log) {
        if !pool.should_handle_mint_and_burn() {
            return;
        }
        if let Some(pl) = utils::extract_pool_liquidity(log.ordinal, &log.address, storage_changes) {
            pool_liquidities.push(pl)
        }
    }
}

pub fn extract_fee_growth_update(
    fee_growth_updates: &mut Vec<events::FeeGrowthGlobal>,
    log: &Log,
    storage_changes: &Vec<StorageChange>,
    pool: &Pool,
) {
    let mut do_extract = false;
    if let Some(_) = abi::pool::events::Swap::match_and_decode(&log) {
        if !pool.should_handle_swap() {
            return;
        }
        do_extract = true;
    } else if let Some(_) = abi::pool::events::Mint::match_and_decode(&log) {
        if !pool.should_handle_mint_and_burn() {
            return;
        }
        do_extract = true
    } else if let Some(_) = abi::pool::events::Burn::match_and_decode(&log) {
        if !pool.should_handle_mint_and_burn() {
            return;
        }
        do_extract = true;
    } else if let Some(_) = abi::pool::events::Flash::match_and_decode(&log) {
        do_extract = true;
    }
    if do_extract {
        fee_growth_updates.append(&mut utils::extract_pool_fee_growth_global_updates(
            log.ordinal,
            &log.address,
            storage_changes,
        ));
    }
}

pub fn extract_pool_sqrt_prices(pool_sqrt_prices: &mut Vec<events::PoolSqrtPrice>, log: &Log, pool_address: &String) {
    if let Some(event) = abi::pool::events::Initialize::match_and_decode(log) {
        pool_sqrt_prices.push(events::PoolSqrtPrice {
            pool_address: pool_address.to_string(),
            ordinal: log.ordinal,
            sqrt_price: event.sqrt_price_x96.into(),
            tick: event.tick.into(),
        });
    } else if let Some(event) = abi::pool::events::Swap::match_and_decode(log) {
        pool_sqrt_prices.push(events::PoolSqrtPrice {
            pool_address: pool_address.to_string(),
            ordinal: log.ordinal,
            sqrt_price: event.sqrt_price_x96.to_string(),
            tick: event.tick.to_string(),
        });
    }
}

pub fn extract_transactions(
    transactions: &mut Vec<events::Transaction>,
    log: &Log,
    transaction_trace: &TransactionTrace,
    timestamp_seconds: u64,
    block_number: u64,
) {
    let mut add_transaction = false;
    if abi::pool::events::Burn::match_and_decode(log).is_some()
        || abi::pool::events::Mint::match_and_decode(log).is_some()
        || abi::pool::events::Swap::match_and_decode(log).is_some()
        || abi::positionmanager::events::IncreaseLiquidity::match_and_decode(log).is_some()
        || abi::positionmanager::events::Collect::match_and_decode(log).is_some()
        || abi::positionmanager::events::DecreaseLiquidity::match_and_decode(log).is_some()
        || abi::positionmanager::events::Transfer::match_and_decode(log).is_some()
    {
        add_transaction = true
    }

    if add_transaction {
        transactions.push(utils::load_transaction(
            block_number,
            timestamp_seconds,
            log.ordinal,
            transaction_trace,
        ));
    }
}

pub fn extract_positions(
    created_positions: &mut Vec<events::CreatedPosition>,
    increase_liquidity_positions: &mut Vec<events::IncreaseLiquidityPosition>,
    decrease_liquidity_positions: &mut Vec<events::DecreaseLiquidityPosition>,
    collect_positions: &mut Vec<events::CollectPosition>,
    transfer_positions: &mut Vec<events::TransferPosition>,
    log: &Log,
    transaction_id: &String,
    pools_store: &StoreGetProto<Pool>,
    timestamp: u64,
    block_number: u64,
) {
    let log_address = log.clone().address;
    if log.address != NON_FUNGIBLE_POSITION_MANAGER {
        return;
    }

    //TODO: check the tokenId in the storage changes
    // then emit positions created

    //TODO: in the subgraph, on each of these 4 events: they will make an eth_call to fetch
    // the tick upper, tick lower, feeGrowthInside0LastX128 and feeGrowthInside1LastX128
    // should we check the SC for changes on these values, and if so, then we can
    // set them?? Maybe we can uncouple the feeGrowthInside*LastX128 and check the SC...
    if let Some(event) = abi::positionmanager::events::IncreaseLiquidity::match_and_decode(log) {
        increase_liquidity_positions.push(IncreaseLiquidityPosition {
            token_id: event.token_id.to_string(),
            liquidity: event.liquidity.to_string(),
            deposited_token0: event.amount0.to_string(),
            deposited_token1: event.amount1.to_string(),
        });
    } else if let Some(event) = abi::positionmanager::events::DecreaseLiquidity::match_and_decode(log) {
        decrease_liquidity_positions.push(DecreaseLiquidityPosition {
            token_id: event.token_id.to_string(),
            liquidity: event.liquidity.to_string(),
            withdrawn_token0: event.amount0.to_string(),
            withdrawn_token1: event.amount1.to_string(),
        });
    } else if let Some(event) = abi::positionmanager::events::Collect::match_and_decode(log) {
        collect_positions.push(CollectPosition {
            token_id: event.token_id.to_string(),
            collected_fees_token0: event.amount0.to_string(),
            collected_fees_token1: event.amount1.to_string(),
        });
    } else if let Some(event) = abi::positionmanager::events::Transfer::match_and_decode(log) {
        transfer_positions.push(TransferPosition {
            token_id: event.token_id.to_string(),
            owner: Hex(&event.to).to_string(),
        });
    }
}

pub fn extract_flashes(
    flashes: &mut Vec<events::Flash>,
    log: &Log,
    pools_store: &StoreGetProto<Pool>,
    pool_key: &String,
) {
    if abi::pool::events::Flash::match_log(&log) {
        let pool_address: String = Hex(&log.address).to_string();

        match pools_store.has_last(pool_key) {
            true => {
                log::info!("pool_address: {}", pool_address);
                let (fee_growth_global_0x_128, fee_growth_global_1x_128) =
                    rpc::fee_growth_global_x128_call(&pool_address);

                flashes.push(events::Flash {
                    pool_address,
                    fee_growth_global_0x_128: fee_growth_global_0x_128.to_string(),
                    fee_growth_global_1x_128: fee_growth_global_1x_128.to_string(),
                    log_ordinal: log.ordinal,
                });
            }
            false => {
                panic!("pool {} not found for flash", pool_address)
            }
        }
    }
}
