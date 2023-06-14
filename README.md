# substreams-uniswap-v3

[Substreams](https://substreams.streamingfast.io)-based Uniswap-v3 Subgraph and Substreams. Still in beta.

The Subgraph-style output hinges on the latest `graph-node` to implement `substreams` data sources.

Substreams are consumable directly.

## Stream intermediate values and events

- Replace the spkg with the version you would like to used, the current release can be foubd on [Github](https://github.com/streamingfast/substreams-uniswap-v3/releases)

- Replace the streamingfast api endpoint with the preffered network, a list of chains and endpoints can be found [here](https://substreams.streamingfast.io/reference-and-specs/chains-and-endpoints)

```bash
$ substreams run https://github.com/streamingfast/substreams-uniswap-v3/releases/download/v0.2.8/substreams.spkg \
  map_pools_created \
  -e api.streamingfast.io:443 \
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



## Stream Entity changes

First, [authenticate](https://substreams.streamingfast.io/reference-and-specs/authentication), and run `sftoken` in your shell session.

This runs the `graph_out` module against a Substreams cluster:

```bash
$ substreams run https://github.com/streamingfast/substreams-uniswap-v3/releases/download/v0.2.8/substreams.spkg \
  graph_out \
  -e api.streamingfast.io:443 \
  -t +150
[...]
{
  "@module": "graph_out",
  "@block": 12369621,
  "@type": "sf.substreams.entity.v1.EntityChanges",
  "@data": {
    "entityChanges": [
      {
        "entity": "Factory",
        "id": "1f98431c8ad98523631ae4a59f267346ea31f984",
        "ordinal": "1",
        "operation": "CREATE",
        "fields": [
          {
            "name": "id",
            "newValue": {
              "string": "1f98431c8ad98523631ae4a59f267346ea31f984"
            }
          },
          {
            "name": "poolCount",
            "newValue": {
              "bigint": "0"
            }
          },
[...]
       ]
      },
     {
        "entity": "Pool",
        "id": "1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801",
        "ordinal": "927",
        "operation": "UPDATE",
        "fields": [
          {
            "name": "totalValueLockedToken0",
            "newValue": {
              "bigdecimal": "0.9999999999999999240000000000003427709097170609759698726797493006923644998096278868615627288818359375"
            },
            "oldValue": {
              "bigdecimal": "0"
            }
          }
        ]
      },
[...]
    ]
  }
}
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
