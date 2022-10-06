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
  "@block": 12369621,
  "@type": "substreams.entity.v1.EntityChanges",
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
          {
            "name": "txCount",
            "newValue": {
              "bigint": "0"
            }
          },
          {
            "name": "totalVolumeUSD",
            "newValue": {
              "bigdecimal": "0"
            }
          },
          {
            "name": "totalVolumeETH",
            "newValue": {
              "bigdecimal": "0"
            }
          },
          {
            "name": "totalFeesUSD",
            "newValue": {
              "bigdecimal": "0"
            }
          },
          {
            "name": "totalFeesETH",
            "newValue": {
              "bigdecimal": "0"
            }
          },
          {
            "name": "untrackedVolumeUSD",
            "newValue": {
              "bigdecimal": "0"
            }
          },
          {
            "name": "totalValueLockedUSD",
            "newValue": {
              "bigdecimal": "0"
            }
          },
          {
            "name": "totalValueLockedETH",
            "newValue": {
              "bigdecimal": "0"
            }
          },
          {
            "name": "totalValueLockedUSDUntracked",
            "newValue": {
              "bigdecimal": "0"
            }
          },
          {
            "name": "totalValueLockedETHUntracked",
            "newValue": {
              "bigdecimal": "0"
            }
          },
          {
            "name": "owner",
            "newValue": {
              "string": "0000000000000000000000000000000000000000"
            }
          }
        ]
      },
      {
        "entity": "Bundle",
        "id": "1",
        "ordinal": "1",
        "operation": "CREATE",
        "fields": [
          {
            "name": "ethPriceUSD",
            "newValue": {
              "bigdecimal": "0"
            }
          }
        ]
      }
    ]
  }
}
[...]
{
  "@module": "graph_out",
  "@block": 12369739,
  "@type": "substreams.entity.v1.EntityChanges",
  "@data": {
    "entityChanges": [
      {
        "entity": "Factory",
        "id": "1f98431c8ad98523631ae4a59f267346ea31f984",
        "ordinal": "835",
        "operation": "UPDATE",
        "fields": [
          {
            "name": "poolCount",
            "newValue": {
              "bigint": "1"
            },
            "oldValue": {
              "bigint": "0"
            }
          }
        ]
      },
      {
        "entity": "Factory",
        "id": "1f98431c8ad98523631ae4a59f267346ea31f984",
        "ordinal": "927",
        "operation": "UPDATE",
        "fields": [
          {
            "name": "txCount",
            "newValue": {
              "bigint": "1"
            },
            "oldValue": {
              "bigint": "0"
            }
          }
        ]
      },
      {
        "entity": "Transaction",
        "id": "37d8f4b1b371fde9e4b1942588d16a1cbf424b7c66e731ec915aca785ca2efcf",
        "ordinal": "927",
        "operation": "CREATE",
        "fields": [
          {
            "name": "id",
            "newValue": {
              "string": "37d8f4b1b371fde9e4b1942588d16a1cbf424b7c66e731ec915aca785ca2efcf"
            }
          },
          {
            "name": "blockNumber",
            "newValue": {
              "bigint": "12369739"
            }
          },
          {
            "name": "timestamp",
            "newValue": {
              "bigint": "1620157956"
            }
          },
          {
            "name": "gasUsed",
            "newValue": {
              "bigint": "5203968"
            }
          },
          {
            "name": "gasPrice",
            "newValue": {
              "bigint": "100000000000"
            }
          }
        ]
      },
      {
        "entity": "Transaction",
        "id": "37d8f4b1b371fde9e4b1942588d16a1cbf424b7c66e731ec915aca785ca2efcf",
        "ordinal": "940",
        "operation": "CREATE",
        "fields": [
          {
            "name": "id",
            "newValue": {
              "string": "37d8f4b1b371fde9e4b1942588d16a1cbf424b7c66e731ec915aca785ca2efcf"
            }
          },
          {
            "name": "blockNumber",
            "newValue": {
              "bigint": "12369739"
            }
          },
          {
            "name": "timestamp",
            "newValue": {
              "bigint": "1620157956"
            }
          },
          {
            "name": "gasUsed",
            "newValue": {
              "bigint": "5203968"
            }
          },
          {
            "name": "gasPrice",
            "newValue": {
              "bigint": "100000000000"
            }
          }
        ]
      },
      {
        "entity": "Transaction",
        "id": "37d8f4b1b371fde9e4b1942588d16a1cbf424b7c66e731ec915aca785ca2efcf",
        "ordinal": "954",
        "operation": "CREATE",
        "fields": [
          {
            "name": "id",
            "newValue": {
              "string": "37d8f4b1b371fde9e4b1942588d16a1cbf424b7c66e731ec915aca785ca2efcf"
            }
          },
          {
            "name": "blockNumber",
            "newValue": {
              "bigint": "12369739"
            }
          },
          {
            "name": "timestamp",
            "newValue": {
              "bigint": "1620157956"
            }
          },
          {
            "name": "gasUsed",
            "newValue": {
              "bigint": "5203968"
            }
          },
          {
            "name": "gasPrice",
            "newValue": {
              "bigint": "100000000000"
            }
          }
        ]
      },
      {
        "entity": "Pool",
        "id": "1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801",
        "ordinal": "843",
        "operation": "CREATE",
        "fields": [
          {
            "name": "sqrtPrice",
            "newValue": {
              "bigint": "8927094545831003674704908909"
            }
          },
          {
            "name": "tick",
            "newValue": {
              "bigint": "-43667"
            }
          }
        ]
      },
      {
        "entity": "Pool",
        "id": "1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801",
        "operation": "UPDATE",
        "fields": [
          {
            "name": "liquidity",
            "newValue": {
              "bigint": "383995753785830744"
            },
            "oldValue": {
              "bigint": "0"
            }
          }
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
      {
        "entity": "Pool",
        "id": "1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801",
        "ordinal": "927",
        "operation": "UPDATE",
        "fields": [
          {
            "name": "totalValueLockedToken1",
            "newValue": {
              "bigdecimal": "0.01264381746197226000000000000000144162058914437447965098169986707804524073139873507898300886154174805"
            },
            "oldValue": {
              "bigdecimal": "0"
            }
          }
        ]
      },
      {
        "entity": "Pool",
        "id": "1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801",
        "ordinal": "835",
        "operation": "UPDATE",
        "fields": [
          {
            "name": "feeGrowthGlobal0X128",
            "newValue": {
              "bigint": "0"
            },
            "oldValue": {
              "bigint": "0"
            }
          }
        ]
      },
      {
        "entity": "Pool",
        "id": "1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801",
        "ordinal": "835",
        "operation": "UPDATE",
        "fields": [
          {
            "name": "feeGrowthGlobal1X128",
            "newValue": {
              "bigint": "0"
            },
            "oldValue": {
              "bigint": "0"
            }
          }
        ]
      },
      {
        "entity": "Pool",
        "id": "1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801",
        "ordinal": "843",
        "operation": "UPDATE",
        "fields": [
          {
            "name": "token0Price",
            "newValue": {
              "bigdecimal": "78.76601952474081448516162931189449750144335446771553281655550790474587287531348715556056299391062978"
            },
            "oldValue": {
              "bigdecimal": "0"
            }
          }
        ]
      },
      {
        "entity": "Pool",
        "id": "1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801",
        "ordinal": "843",
        "operation": "UPDATE",
        "fields": [
          {
            "name": "token1Price",
            "newValue": {
              "bigdecimal": "0.01269583008045613912537880652252357634826606549593750361690139278516628255672527307728256336238519793"
            },
            "oldValue": {
              "bigdecimal": "0"
            }
          }
        ]
      },
      {
        "entity": "Pool",
        "id": "1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801",
        "ordinal": "927",
        "operation": "UPDATE",
        "fields": [
          {
            "name": "txCount",
            "newValue": {
              "bigint": "1"
            },
            "oldValue": {
              "bigint": "0"
            }
          }
        ]
      },
      {
        "entity": "Token",
        "id": "1f9840a85d5af5bf1d1762f925bdaddc4201f984",
        "ordinal": "927",
        "operation": "UPDATE",
        "fields": [
          {
            "name": "txCount",
            "newValue": {
              "bigint": "1"
            },
            "oldValue": {
              "bigint": "0"
            }
          }
        ]
      },
      {
        "entity": "Token",
        "id": "c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        "ordinal": "927",
        "operation": "UPDATE",
        "fields": [
          {
            "name": "txCount",
            "newValue": {
              "bigint": "1"
            },
            "oldValue": {
              "bigint": "0"
            }
          }
        ]
      },
      {
        "entity": "Token",
        "id": "1f9840a85d5af5bf1d1762f925bdaddc4201f984",
        "ordinal": "927",
        "operation": "UPDATE",
        "fields": [
          {
            "name": "totalValueLocked",
            "newValue": {
              "bigdecimal": "0.9999999999999999240000000000003427709097170609759698726797493006923644998096278868615627288818359375"
            },
            "oldValue": {
              "bigdecimal": "0"
            }
          }
        ]
      },
      {
        "entity": "Token",
        "id": "c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        "ordinal": "927",
        "operation": "UPDATE",
        "fields": [
          {
            "name": "totalValueLocked",
            "newValue": {
              "bigdecimal": "0.01264381746197226000000000000000144162058914437447965098169986707804524073139873507898300886154174805"
            },
            "oldValue": {
              "bigdecimal": "0"
            }
          }
        ]
      },
      {
        "entity": "Token",
        "id": "1f9840a85d5af5bf1d1762f925bdaddc4201f984",
        "ordinal": "1",
        "operation": "UPDATE",
        "fields": [
          {
            "name": "whitelistPools",
            "newValue": {
              "array": {
                "value": [
                  {
                    "string": "1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801"
                  }
                ]
              }
            },
            "oldValue": {
              "array": {}
            }
          }
        ]
      },
      {
        "entity": "Tick",
        "id": "1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801#-50580",
        "ordinal": "927",
        "operation": "UPDATE",
        "fields": [
          {
            "name": "liquidityNet",
            "newValue": {
              "bigint": "383995753785830744"
            },
            "oldValue": {
              "bigint": "0"
            }
          }
        ]
      },
      {
        "entity": "Tick",
        "id": "1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801#-50580",
        "ordinal": "927",
        "operation": "UPDATE",
        "fields": [
          {
            "name": "liquidityGross",
            "newValue": {
              "bigint": "383995753785830744"
            },
            "oldValue": {
              "bigint": "0"
            }
          }
        ]
      },
      {
        "entity": "Tick",
        "id": "1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801#-36720",
        "ordinal": "927",
        "operation": "UPDATE",
        "fields": [
          {
            "name": "liquidityNet",
            "newValue": {
              "bigint": "-383995753785830744"
            },
            "oldValue": {
              "bigint": "0"
            }
          }
        ]
      },
      {
        "entity": "Tick",
        "id": "1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801#-36720",
        "ordinal": "927",
        "operation": "UPDATE",
        "fields": [
          {
            "name": "liquidityGross",
            "newValue": {
              "bigint": "383995753785830744"
            },
            "oldValue": {
              "bigint": "0"
            }
          }
        ]
      },
      {
        "entity": "Position",
        "id": "1",
        "ordinal": "954",
        "operation": "UPDATE",
        "fields": [
          {
            "name": "liquidity",
            "newValue": {
              "bigdecimal": "383995753785830744.0000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            },
            "oldValue": {
              "bigdecimal": "0"
            }
          }
        ]
      },
      {
        "entity": "Position",
        "id": "1",
        "ordinal": "954",
        "operation": "UPDATE",
        "fields": [
          {
            "name": "depositedToken0",
            "newValue": {
              "bigdecimal": "0.9999999999999999240000000000003427709097170609759698726797493006923644998096278868615627288818359375"
            },
            "oldValue": {
              "bigdecimal": "0"
            }
          }
        ]
      },
      {
        "entity": "Position",
        "id": "1",
        "ordinal": "954",
        "operation": "UPDATE",
        "fields": [
          {
            "name": "depositedToken1",
            "newValue": {
              "bigdecimal": "0.01264381746197226000000000000000144162058914437447965098169986707804524073139873507898300886154174805"
            },
            "oldValue": {
              "bigdecimal": "0"
            }
          }
        ]
      }
    ]
  }
}
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
