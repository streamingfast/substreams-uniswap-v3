# Contributing guide

You can update the metadata around the package with:

```bash
substreams tools extract-wasm uniswap-v3@latest --module graph_out target/wasm32-unknown-unknown/release/substreams_uniswap_v3.wasm
```

by bumping the release version in substreams.yaml, and then running:

```bash
substreams registry login  # if not already logged in
substreams publish
```


