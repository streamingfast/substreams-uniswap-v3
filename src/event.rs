// use substreams::{Hex, pb, proto};
// use substreams::store::StoreGet;
// use crate::{abi, Event, Pool};
// use crate::abi::pool::events::{Burn, Mint, Swap};
// use crate::pb::uniswap::event;
//
// pub enum AbstractEvent {
//     Swap(Swap),
//     Burn(Burn),
//     Mint(Mint)
// }
//
// trait IEvent {
//     fn decode_event(log: &substreams_ethereum::pb::eth::v1::Log) -> Option<AbstractEvent>;
//     fn create_event(swap: AbstractEvent);
//     fn to_string() -> String;
// }
//
// impl IEvent for Swap {
//     fn decode_event(log: &substreams_ethereum::pb::eth::v1::Log) -> Option<AbstractEvent> {
//         if !Swap::match_log(log) {
//             return None;
//         }
//         return Some(AbstractEvent::Swap(Swap::must_decode(log)));
//     }
//
//     fn create_event(swap: AbstractEvent) {
//         // return event::Type::Swap {
//         //     0: Swap {
//         //         sender: vec![],
//         //         recipient: vec![],
//         //         amount0: Default::default(),
//         //         amount1: Default::default(),
//         //         sqrt_price_x96: Default::default(),
//         //         liquidity: Default::default(),
//         //         tick: Default::default()
//         //     }
//         // };
//     }
//
//     fn to_string() -> String {
//         return "swap".to_string()
//     }
// }
//
// impl IEvent for Burn {
//     fn decode_event(log: &substreams_ethereum::pb::eth::v1::Log) -> Option<AbstractEvent> {
//         if !Burn::match_log(log) {
//             return None;
//         }
//         return Some(AbstractEvent::Burn(Burn::must_decode(log)));
//     }
//
//     fn create_event(burn: AbstractEvent) {
//         todo!()
//     }
//
//     fn to_string() -> String {
//         return "burn".to_string()
//     }
// }
//
// impl IEvent for Mint {
//     fn decode_event(log: &substreams_ethereum::pb::eth::v1::Log) -> Option<AbstractEvent> {
//         if !Mint::match_log(log) {
//             return None;
//         }
//         return Some(AbstractEvent::Mint(Mint::must_decode(log)));
//     }
//
//     fn create_event(mint: AbstractEvent) {
//         todo!()
//     }
//
//     fn to_string() -> String {
//         return "mint".to_string()
//     }
// }
//
// pub fn process_event(log: &substreams_ethereum::pb::eth::v1::Log, store: &StoreGet) -> Option<Event> {
//     let event: Option<AbstractEvent> = IEvent::decode_event(log);
//     if event.is_none() {
//         return None;
//     }
//
//     match store.get_last(&format!("pool:{}", Hex(&log.address).to_string())) {
//         None => {
//             panic!("invalid {}. pool does not exist. pool address {} transaction {}", event.unwrap()., Hex(&log.address).to_string(), Hex(&trx.hash).to_string());
//         }
//         Some(pool_bytes) => {
//             let pool: Pool = proto::decode(&pool_bytes).unwrap();
//
//             return Some(Event{
//                 log_ordinal: log.block_index as u64,
//                 pool_address: pool.address.to_string(),
//                 token0: pool.token0_address.to_string(),
//                 token1: pool.token1_address.to_string(),
//                 fee: pool.fee.to_string(),
//                 transaction_id: Hex(&trx.hash).to_string(),
//                 timestamp: block.header.as_ref().unwrap().timestamp.as_ref().unwrap().seconds as u64,
//                 r#type: Some(IEvent::create_event(event.unwrap())),
//             });
//         }
//     }
// }
