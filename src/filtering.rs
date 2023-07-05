use crate::math::compute_price_from_tick_idx;
use crate::pb::uniswap::events;
use crate::storage::position_manager::PositionManagerStorage;
use crate::storage::uniswap_v3_pool::UniswapPoolStorage;
use crate::utils::NON_FUNGIBLE_POSITION_MANAGER;
use crate::{abi, math, utils, BurnEvent, EventTrait, MintEvent, Pool, SwapEvent};
use substreams::prelude::{BigDecimal, BigInt};
use substreams::{log, Hex};
use substreams_ethereum::block_view::CallView;
use substreams_ethereum::pb::eth::v2::{Call, Log, StorageChange, TransactionTrace};

pub fn extract_pool_events_and_positions(
    pool_events: &mut Vec<events::PoolEvent>,
    ticks_created: &mut Vec<events::TickCreated>,
    ticks_updated: &mut Vec<events::TickUpdated>,
    created_positions: &mut Vec<events::CreatedPosition>,
    increase_liquidity_positions: &mut Vec<events::IncreaseLiquidityPosition>,
    decrease_liquidity_positions: &mut Vec<events::DecreaseLiquidityPosition>,
    collect_positions: &mut Vec<events::CollectPosition>,
    transfer_positions: &mut Vec<events::TransferPosition>,
    transaction_id: &String,
    origin: &String,
    log: &Log,
    call_view: &CallView,
    pool: &Pool,
    timestamp_seconds: u64,
    block_number: u64,
) {
    let common_tick_updated = events::TickUpdated {
        log_ordinal: log.ordinal,
        pool_address: pool.address.to_string(),
        timestamp: timestamp_seconds,
        ..Default::default()
    };

    if let Some(swap) = abi::pool::events::Swap::match_and_decode(log) {
        log::info!("SWAP: transaction: {}", transaction_id.to_string());
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
        });

        //TODO: verify if a swap changes the fee growth inside 0x128 and 1x128
        if let Some(position_manager_contract_call) = call_view.parent() {
            extract_positions(
                pool,
                increase_liquidity_positions,
                decrease_liquidity_positions,
                collect_positions,
                transfer_positions,
                &position_manager_contract_call,
            );
        }
    } else if let Some(mint) = abi::pool::events::Mint::match_and_decode(log) {
        log::info!("MINT: transaction: {}", transaction_id.to_string());
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

        let common_tick = events::TickCreated {
            pool_address: pool.address.to_string(),
            created_at_timestamp: timestamp_seconds,
            created_at_block_number: block_number,
            log_ordinal: log.ordinal,
            amount: mint.amount.into(),
            ..Default::default()
        };

        let mut lower_tick = common_tick.clone();
        let (price0, price1) = prices_from_tick_index(mint.tick_lower.to_i32());
        lower_tick.idx = mint.tick_lower.as_ref().into();
        lower_tick.price0 = price0.into();
        lower_tick.price1 = price1.into();
        ticks_created.push(lower_tick);

        let mut upper_tick = common_tick.clone();
        let (price0, price1) = prices_from_tick_index(mint.tick_upper.to_i32());
        upper_tick.idx = mint.tick_upper.as_ref().into();
        upper_tick.price0 = price0.into();
        upper_tick.price1 = price1.into();
        ticks_created.push(upper_tick);

        let storage = UniswapPoolStorage::new(&call_view.call.storage_changes, &log.address);

        ticks_updated.push(events::TickUpdated {
            idx: mint.tick_upper.as_ref().into(),
            fee_growth_outside_0x_128: bigint_if_some(storage.ticks(&mint.tick_upper).fee_growth_outside_0_x128()),
            fee_growth_outside_1x_128: bigint_if_some(storage.ticks(&mint.tick_upper).fee_growth_outside_1_x128()),
            ..common_tick_updated.clone()
        });
        ticks_updated.push(events::TickUpdated {
            idx: mint.tick_lower.as_ref().into(),
            fee_growth_outside_0x_128: bigint_if_some(storage.ticks(&mint.tick_lower).fee_growth_outside_0_x128()),
            fee_growth_outside_1x_128: bigint_if_some(storage.ticks(&mint.tick_lower).fee_growth_outside_1_x128()),
            ..common_tick_updated.clone()
        });

        if let Some(position_manager_contract_call) = call_view.parent() {
            if position_manager_contract_call.address != NON_FUNGIBLE_POSITION_MANAGER {
                return;
            }

            let uniswap_pool_manager_storage = PositionManagerStorage::new(
                &position_manager_contract_call.storage_changes,
                &position_manager_contract_call.address,
            );

            if let Some((old_value, _new_value)) = uniswap_pool_manager_storage.next_id() {
                let token_id = old_value;

                let mut fee_growth_inside0_last_x128 = None;
                let mut fee_growth_inside1_last_x128 = None;

                if let Some((_old_value, new_value)) = uniswap_pool_manager_storage
                    .positions(&token_id)
                    .fee_growth_inside0last_x128()
                {
                    fee_growth_inside0_last_x128 = Some(new_value.to_string());
                }

                if let Some((_old_value, new_value)) = uniswap_pool_manager_storage
                    .positions(&token_id)
                    .fee_growth_inside1last_x128()
                {
                    fee_growth_inside1_last_x128 = Some(new_value.to_string());
                }

                created_positions.push(events::CreatedPosition {
                    token_id: token_id.to_string(),
                    pool: pool.address.clone(),
                    token0: token0.address.clone(),
                    token1: token1.address.clone(),
                    tick_lower: mint.tick_lower.to_string(),
                    tick_upper: mint.tick_upper.to_string(),
                    transaction: transaction_id.to_string(),
                    log_ordinal: log.ordinal,
                    timestamp: timestamp_seconds,
                    block_number,
                    fee_growth_inside0_last_x128,
                    fee_growth_inside1_last_x128,
                });
            }

            extract_positions(
                pool,
                increase_liquidity_positions,
                decrease_liquidity_positions,
                collect_positions,
                transfer_positions,
                &position_manager_contract_call,
            );
        }
    } else if let Some(burn) = abi::pool::events::Burn::match_and_decode(log) {
        log::info!("BURN: transaction: {}", transaction_id.to_string());
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

        let storage = UniswapPoolStorage::new(&call_view.call.storage_changes, &log.address);

        ticks_updated.push(events::TickUpdated {
            idx: burn.tick_upper.as_ref().into(),
            fee_growth_outside_0x_128: bigint_if_some(storage.ticks(&burn.tick_upper).fee_growth_outside_0_x128()),
            fee_growth_outside_1x_128: bigint_if_some(storage.ticks(&burn.tick_upper).fee_growth_outside_1_x128()),
            ..common_tick_updated.clone()
        });
        ticks_updated.push(events::TickUpdated {
            idx: burn.tick_lower.as_ref().into(),
            fee_growth_outside_0x_128: bigint_if_some(storage.ticks(&burn.tick_lower).fee_growth_outside_0_x128()),
            fee_growth_outside_1x_128: bigint_if_some(storage.ticks(&burn.tick_lower).fee_growth_outside_1_x128()),
            ..common_tick_updated.clone()
        });

        if let Some(position_manager_contract_call) = call_view.parent() {
            extract_positions(
                pool,
                increase_liquidity_positions,
                decrease_liquidity_positions,
                collect_positions,
                transfer_positions,
                &position_manager_contract_call,
            );
        }
    } else if abi::pool::events::Collect::match_log(log) {
        if let Some(position_manager_contract_call) = call_view.parent() {
            extract_positions(
                pool,
                increase_liquidity_positions,
                decrease_liquidity_positions,
                collect_positions,
                transfer_positions,
                &position_manager_contract_call,
            );
        };
    }
}

fn bigint_if_some(input: Option<(BigInt, BigInt)>) -> String {
    if let Some(el) = input {
        el.1.into()
    } else {
        "".to_string()
    }
}

fn prices_from_tick_index(tick_idx: i32) -> (BigDecimal, BigDecimal) {
    let price0 = compute_price_from_tick_idx(tick_idx);
    let price1 = math::safe_div(&BigDecimal::from(1 as i32), &price0);
    (price0, price1)
}

pub fn extract_pool_liquidities(
    pool_liquidities: &mut Vec<events::PoolLiquidity>,
    log: &Log,
    storage_changes: &Vec<StorageChange>,
    pool: &Pool,
) {
    let pool_address = &pool.address;
    let token0 = &pool.token0().address;
    let token1 = &pool.token1().address;
    if Hex(&log.address).to_string() != pool.address {
        return;
    }

    let storage = UniswapPoolStorage::new(storage_changes, &log.address);

    if abi::pool::events::Swap::match_log(&log) {
        if !pool.should_handle_swap() {
            return;
        }
        let value = bigint_if_some(storage.liquidity());

        if value != "" {
            pool_liquidities.push(events::PoolLiquidity {
                pool_address: pool_address.to_string(),
                liquidity: value,
                token0: token0.to_string(),
                token1: token1.to_string(),
                log_ordinal: log.ordinal,
            });
        }
    } else if abi::pool::events::Mint::match_log(&log) {
        if !pool.should_handle_mint_and_burn() {
            return;
        }
        let value = bigint_if_some(storage.liquidity());

        if value != "" {
            pool_liquidities.push(events::PoolLiquidity {
                pool_address: pool_address.to_string(),
                liquidity: value,
                token0: token0.to_string(),
                token1: token1.to_string(),
                log_ordinal: log.ordinal,
            });
        }
    } else if abi::pool::events::Burn::match_log(&log) {
        if !pool.should_handle_mint_and_burn() {
            return;
        }
        let value = bigint_if_some(storage.liquidity());

        if value != "" {
            pool_liquidities.push(events::PoolLiquidity {
                pool_address: pool_address.to_string(),
                liquidity: value,
                token0: token0.to_string(),
                token1: token1.to_string(),
                log_ordinal: log.ordinal,
            });
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
    if abi::pool::events::Swap::match_log(&log) {
        if !pool.should_handle_swap() {
            return;
        }
        do_extract = true;
    } else if abi::pool::events::Mint::match_log(&log) {
        if !pool.should_handle_mint_and_burn() {
            return;
        }
        do_extract = true
    } else if abi::pool::events::Burn::match_log(&log) {
        if !pool.should_handle_mint_and_burn() {
            return;
        }
        do_extract = true;
    } else if abi::pool::events::Flash::match_log(&log) {
        do_extract = false; //TODO: handle this later when we process flashes
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
            initialized: true,
        });
    } else if let Some(event) = abi::pool::events::Swap::match_and_decode(log) {
        pool_sqrt_prices.push(events::PoolSqrtPrice {
            pool_address: pool_address.to_string(),
            ordinal: log.ordinal,
            sqrt_price: event.sqrt_price_x96.to_string(),
            tick: event.tick.to_string(),
            initialized: false,
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
    if abi::pool::events::Burn::match_log(log)
        || abi::pool::events::Mint::match_log(log)
        || abi::pool::events::Swap::match_log(log)
        || abi::positionmanager::events::IncreaseLiquidity::match_log(log)
        || abi::positionmanager::events::Collect::match_log(log)
        || abi::positionmanager::events::DecreaseLiquidity::match_log(log)
        || abi::positionmanager::events::Transfer::match_log(log)
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

fn extract_positions(
    pool: &Pool,
    increase_liquidity_positions: &mut Vec<events::IncreaseLiquidityPosition>,
    decrease_liquidity_positions: &mut Vec<events::DecreaseLiquidityPosition>,
    collect_positions: &mut Vec<events::CollectPosition>,
    transfer_positions: &mut Vec<events::TransferPosition>,
    call: &Call,
) {
    for log in call.logs.iter() {
        if log.address != NON_FUNGIBLE_POSITION_MANAGER {
            return;
        }

        let mut fee_growth_inside0_last_x128 = None;
        let mut fee_growth_inside1_last_x128 = None;

        let manager_storage = PositionManagerStorage::new(&call.storage_changes, &call.address);

        if let Some(event) = abi::positionmanager::events::IncreaseLiquidity::match_and_decode(log) {
            if let Some((_old_value, new_value)) =
                manager_storage.positions(&event.token_id).fee_growth_inside0last_x128()
            {
                fee_growth_inside0_last_x128 = Some(new_value.to_string());
            }

            if let Some((_old_value, new_value)) =
                manager_storage.positions(&event.token_id).fee_growth_inside1last_x128()
            {
                fee_growth_inside1_last_x128 = Some(new_value.to_string());
            }

            increase_liquidity_positions.push(events::IncreaseLiquidityPosition {
                token_id: event.token_id.to_string(),
                liquidity: event.liquidity.to_string(),
                deposited_token0: event.amount0.to_decimal(pool.token0().decimals).to_string(),
                deposited_token1: event.amount1.to_decimal(pool.token1().decimals).to_string(),
                fee_growth_inside0_last_x128,
                fee_growth_inside1_last_x128,
                log_ordinal: log.ordinal,
            });
        } else if let Some(event) = abi::positionmanager::events::DecreaseLiquidity::match_and_decode(log) {
            if let Some((_old_value, new_value)) =
                manager_storage.positions(&event.token_id).fee_growth_inside0last_x128()
            {
                fee_growth_inside0_last_x128 = Some(new_value.to_string());
            }

            if let Some((_old_value, new_value)) =
                manager_storage.positions(&event.token_id).fee_growth_inside1last_x128()
            {
                fee_growth_inside1_last_x128 = Some(new_value.to_string());
            }
            decrease_liquidity_positions.push(events::DecreaseLiquidityPosition {
                token_id: event.token_id.to_string(),
                liquidity: event.liquidity.to_string(),
                withdrawn_token0: event.amount0.to_decimal(pool.token0().decimals).to_string(),
                withdrawn_token1: event.amount1.to_decimal(pool.token1().decimals).to_string(),
                fee_growth_inside0_last_x128,
                fee_growth_inside1_last_x128,
                log_ordinal: log.ordinal,
            });
        } else if let Some(event) = abi::positionmanager::events::Collect::match_and_decode(log) {
            if let Some((_old_value, new_value)) =
                manager_storage.positions(&event.token_id).fee_growth_inside0last_x128()
            {
                fee_growth_inside0_last_x128 = Some(new_value.to_string());
            }

            if let Some((_old_value, new_value)) =
                manager_storage.positions(&event.token_id).fee_growth_inside1last_x128()
            {
                fee_growth_inside1_last_x128 = Some(new_value.to_string());
            }
            collect_positions.push(events::CollectPosition {
                token_id: event.token_id.to_string(),
                collected_fees_token0: event.amount0.to_decimal(pool.token0().decimals).to_string(),
                collected_fees_token1: event.amount1.to_decimal(pool.token1().decimals).to_string(),
                fee_growth_inside0_last_x128,
                fee_growth_inside1_last_x128,
                log_ordinal: log.ordinal,
            });
        } else if let Some(event) = abi::positionmanager::events::Transfer::match_and_decode(log) {
            transfer_positions.push(events::TransferPosition {
                token_id: event.token_id.to_string(),
                owner: Hex(&event.to).to_string(),
                log_ordinal: log.ordinal,
            });
        }
    }
}

// pub fn extract_flashes(flashes: &mut Vec<events::Flash>, log: &Log) {
//     if abi::pool::events::Flash::match_log(&log) {
//         let pool_address: String = Hex(&log.address).to_string();
//
//         // FIXME: kill those `rpc` calls here!
//         log::info!("pool_address: {}", pool_address);
//         let (fee_growth_global_0x_128, fee_growth_global_1x_128) = rpc::fee_growth_global_x128_call(&pool_address);
//
//         flashes.push(events::Flash {
//             pool_address,
//             fee_growth_global_0x_128: fee_growth_global_0x_128.to_string(),
//             fee_growth_global_1x_128: fee_growth_global_1x_128.to_string(),
//             log_ordinal: log.ordinal,
//         });
//     }
// }
