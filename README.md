# substreams-uniswap-v3

### Render protobuf structures and definitions
```bash
substreams protogen substreams.yaml
```

### Build substreams-uniswap-v3
```bash
cargo build --target wasm32-unknown-unknown --release
```

### Run map_pools_created against local firehose
```bash
substreams run substreams.yaml map_pools_created -p -e localhost:9000 -t +150
```

Output: 
```bash
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

### Run map_pools_created against remote firehose
```bash
sftoken
substreams run substreams.yaml map_pools_created -e api-dev.streamingfast.io:443 -t +150
```

### Pack everything when you are satistifed and want to make a release
```bash
substreams pack substreams.yaml
```
