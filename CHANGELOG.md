# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v0.2.10

* Fixed `total_supply` incorrectly sent as `String` to `graph-node` while it should have been `BigInt`.
* Building from `substreams-patch-over-0.2.9.yaml` to reuse the caches from v0.2.9

## v0.2.9

* ERRATUM: The outputs are the wrong type on `total_supply`, breaking the integration with graph-node.

* Fixed `decimals()` handling when token reports more than 255 decimals.

* Moved `tables.rs` abstraction into `substreams-database-change` crate

## v0.2.8

* Update q192 to use real computed q192 value instead of truncated js value to have more precise values and closer to the real value mathematically.
* Javascript Engine 2^192 computed value: 6277101735386681000000000000000000000000000000000000000000 Real value of 2^192: 6277101735386680763835789423207666416102355444464034512896

## v0.2.7

* Release v0.2.7

## v0.2.6

* Fixed issue with tvlUSD on `token`, `TokenDayData` and `TokenHourData` where the token_tvl value was wrongfully fetched from the store.

## v0.2.5

* Fix issue with "open" and "close" being reinitialized when a token was _used_ for a transaction in a pool.

## v0.2.4

* Fix liquidities when computing the derived eth  price of token 0 and token 1.

## v0.2.3

* Fix issue with liquidities check when looping over the storage changes.

## v0.2.2

* Fix issue for `store_pool_liquidities` which was setting all the liquidities for the pools at `ordinal` 0. This caused issues when computing the `derived_eth_prices` for `token0` and `token1` because we were wrongfully matching liquidities for pools which didn't really have any liquidities.

## v0.2.1

* Latest fix to the TokenDayData, TokenHourData, PoolDayData and PoolHourData properly created entities.

> **Note** The version in the `substreams.yaml` is still stated as 0.1.5-beta as it was forgotten to bump the version to 0.2.1.

## v0.2.0

* Stable release of Uniswap v3 Substreams containing fixes to PoolDayData, PoolHourData, TokenDayData and TokenHourData.

## v0.1.5-beta

* Fix tick mathematics

## v0.1.4-beta

* Major refactoring for better performance to remove a couple of substreams modules
* Fix some data discrepancies where human errors were introduced
* Add new usage of `has_*` store methods for better performance

## v0.1.2

* Adding beta release of v0.1.2

## v0.1.1

* Fix issue when `map_pool_sqrt_price` was panicking because of `must_get_last` call on `pool_store`
