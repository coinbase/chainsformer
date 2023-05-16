#!/usr/bin/env bash

set -eo pipefail

OPTS="--proto_path=protos --go_out=paths=source_relative:protos"
protoc ${OPTS} --go-grpc_out=paths=source_relative,require_unimplemented_servers=false:protos protos/coinbase/chainsformer/*.proto
