ENDPOINT ?= api-unstable.streamingfast.io:443
GRAPH_CONFIG ?= ../graph-node-dev/config/graphman.toml
STOP_BLOCK ?= +1000

.PHONY: build
build:
	cargo build --target wasm32-unknown-unknown --release

.PHONY: stream
stream: build
	substreams run -e $(ENDPOINT) substreams.yaml map_extract_data_types -s 12369621 -t $(STOP_BLOCK)

.PHONY: graph_out
graph_out: build
	substreams run -e $(ENDPOINT) substreams.yaml graph_out -s 12369621 -t $(STOP_BLOCK)

.PHONY: protogen
protogen:
	substreams protogen ./substreams.yaml --exclude-paths="sf/substreams,google"

.PHONE: package
package: build
	substreams pack -o substreams.spkg substreams.yaml

.PHONE: deploy_local
deploy_local: package
	mkdir build 2> /dev/null || true
	graph build --ipfs http://localhost:5001 subgraph.yaml
	graph create uniswap_v3 --node http://127.0.0.1:8020
	graph deploy --node http://127.0.0.1:8020 --ipfs http://127.0.0.1:5001 --version-label v0.0.1 uniswap_v3 subgraph.yaml

.PHONE: undeploy_local
undeploy_local:
	graphman --config "$(GRAPH_CONFIG)" drop --force uniswap_v3

.PHONE: test
test:
	cargo test --target aarch64-apple-darwin