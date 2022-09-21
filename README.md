# substreams-uniswap-v3

[Substreams](https://substreams.streamingfast.io)-based Uniswap-v3 Subgraph and Substreams. Still in beta.

The Subgraph-style output hinges on the latest `graph-node` to implement `substreams` data sources.

Substreams are consumable directly.

## Stream Entity changes

First, [authenticate](https://substreams.streamingfast.io/reference-and-specs/authentication), and run `sftoken` in your shell session.

This runs the `graph_out` module against a Substreams cluster:

```bash
$ substreams run https://github.com/streamingfast/substreams-uniswap-v3/releases/download/v0.1.0-beta/uniswap-v3-v0.1.0-beta.spkg \
  graph_out \
  -e api.streamingfast.io:443 \
  -t +150
[...]
{
  "@module": "graph_out",
  "@block": 12369811,
  "@type": "uniswap.types.v1.EntityChanges",
  "@data": {
    "blockId": "UtxZ8zz58lX7UzRF9mgiGpNhkUpib/7M/A9ioKEYkYE=",
    "blockNumber": "12369811",
    "prevBlockId": "2R2W2n9fQzYgarjF+gI43UUp7b9GSneu0mcZYgE9Jic=",
    "prevBlockNumber": "12369810",
    "entityChanges": [
      {
        "entity": "Pool",
        "id": "N2JlYTM5ODY3ZTQxNjlkYmUyMzdkNTVjODI0MmE4ZjJmY2RjYzM4Nw==",
        "ordinal": "4389",
        "operation": "CREATE",
        "fields": [
          {
            "name": "id",
            "valueType": "STRING",
            "newValue": "N2JlYTM5ODY3ZTQxNjlkYmUyMzdkNTVjODI0MmE4ZjJmY2RjYzM4Nw==",
            "oldValueNull": true
          },
          {
            "name": "createdAtTimestamp",
            "valueType": "BIGINT",
            "newValue": "YJGpkQ==",
            "oldValueNull": true
          },
[...]
      {
        "entity": "Pool",
        "id": "N2JlYTM5ODY3ZTQxNjlkYmUyMzdkNTVjODI0MmE4ZjJmY2RjYzM4Nw==",
        "ordinal": "4397",
        "operation": "UPDATE",
        "fields": [
          {
            "name": "sqrtPrice",
            "valueType": "BIGINT",
            "newValue": "QweXGFEipPui5q3xAQo=",
            "oldValue": "AA=="
          },
          {
            "name": "tick",
            "valueType": "BIGINT",
            "newValue": "AvnI",
            "oldValue": "AA=="
          }
        ]
      },
```


## Stream intermediate values and events

```bash
$ substreams run https://github.com/streamingfast/substreams-uniswap-v3/releases/download/v0.1.0-beta/uniswap-v3-v0.1.0-beta.spkg \
  map_pools_created \
  -e api-dev.streamingfast.io:443 \
  -t +150
[...]
{
  "@module": "map_pools_created",
  "@block": 12369739,
  "@type": "uniswap.types.v1.Pools",
  "@data": {
    "pools": [
      {
        "address": "1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801",
        "createdAtTimestamp": "1620157956",
        "createdAtBlockNumber": "12369739",
        "token0": {
          "address": "1f9840a85d5af5bf1d1762f925bdaddc4201f984",
          "name": "Uniswap",
          "symbol": "UNI",
          "decimals": "18",
          "totalSupply": "1000000000000000000000000000"
        },
        "token1": {
          "address": "c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
          "name": "Wrapped Ether",
          "symbol": "WETH",
          "decimals": "18",
          "totalSupply": "6776710776126719425230827"
        },
        "feeTier": 3000,
        "tickSpacing": 60,
        "logOrdinal": "835",
        "transactionId": "37d8f4b1b371fde9e4b1942588d16a1cbf424b7c66e731ec915aca785ca2efcf"
      }
    ]
  }
}
[...]
```

### Stream Entity changes

```


## Hack on it

### Build `substreams-uniswap-v3`

```bash
$ cargo build --target wasm32-unknown-unknown --release
[...]
$ substreams run  -e api-dev.streamingfast.io:443 substreams.yaml graph_out -t +150
[...]
try it :)
```


### Pack everything to release

```bash
substreams pack substreams.yaml
```
