# substreams-uniswap-v3

[Substreams](https://substreams.streamingfast.io)-based Uniswap-v3 Subgraph and Substreams.

The Subgraph-style output hinges on the latest `graph-node` to implement `substreams` data sources.

Substreams are consumable directly.


## Run `map_pools_created` against Substreams cluster

First, [authenticate](https://substreams.streamingfast.io/reference-and-specs/authentication), and run `sftoken` in your shell session.

### Stream intermediate values and events

```bash
$ substreams run  -e api-dev.streamingfast.io:443 substreams.yaml map_pools_created -t +150
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

```bash
$ substreams run  -e api-dev.streamingfast.io:443 substreams.yaml graph_out -t +150
[...]
try it :)
```


## Hack on it

### Build `substreams-uniswap-v3`

```bash
cargo build --target wasm32-unknown-unknown --release
```


### Pack everything when you are satisfied and want to make a release

```bash
substreams pack substreams.yaml
```
