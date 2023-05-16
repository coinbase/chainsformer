<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
  - [Environment Variables](#environment-variables)
  - [Service Configurations](#service-configurations)
  - [New Blockchain Configurations](#new-blockchain-configurations)
- [Development](#development)
  - [Running Chainsformer Server](#running-chainsformer-server)
  - [Run test client](#run-test-client)
  - [Use grpcurl](#use-grpcurl)
    - [Query Chainsformer for a range of blocks](#query-chainsformer-for-a-range-of-blocks)
    - [Query Chainsformer for a range of blocks events](#query-chainsformer-for-a-range-of-blocks-events)
- [Testing](#testing)
  - [Unit Test](#unit-test)
  - [Integration Test](#integration-test)
  - [Functional Test](#functional-test)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Overview

Chainsformer is an [Apache Arrow Flight](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/) service built on top of [ChainStorage](https://github.com/coinbase/chainstorage) as a stateless adaptor service. It currently supports batch data processing and micro batch data streaming from ChainStorage service to the Spark data processing platform.

It aims to provide a set of easy to use interfaces to support spark consumers to read and process ChainStorage Data on the Spark platform:
* It defines a set of standardized block and transaction data schema for each asset class (i.e EVM assets or bitcoin).
* It provides data transformation capability from protobuf to Arrow format.
* It can be easily scaled up to support higher data throughput.
* It can be easily integrated via the Chainsformer Spark Connector (LINK TO BE ADDED LATTER) for structured data streaming.

## Quick Start

Make sure your local go version is 1.18 by running the following commands:

```shell
brew install go@1.18
brew unlink go
brew link go@1.18

brew install protobuf@3.21.12
brew unlink protobuf
brew link protobuf
```

To set up for the first time (only done once):

```shell
make bootstrap
```

Rebuild everything:

```shell
make build
```

## Configuration

### Environment Variables

Chainsformer depends on the following environment variables to resolve the path of the configuration.
The directory structure is as follows: `config/chainsformer/{blockchain}/{network}/{environment}.yml`.

- `CHAINSFORMER_CONFIG`:
  This env var, in the format of `{blockchain}-{network}`, determines the blockchain and network managed by the service.
  The naming is defined in [chainstorage/protos/coinbase/c3/common/common.protp](https://github.com/coinbase/chainstorage/blob/master/protos/coinbase/c3/common/common.proto)
- `CHAINSFORMER_ENVIRONMENT`:
  This env var controls the `{environment}` in which the service is deployed. Possible values include `production`
  , `development`, and `local` (which is also the default value).

### Service Configurations

Asset specific configurations are stored in the `config` directory under the Chainsformer service repo. The config folder structure follows the following form `./config/chainsformer/{blockchain}/{network}/base.yml`

### New Blockchain Configurations
* Simply follow the config folder structure to add new configurations for any new blockchains or new networks of existing blockchains.
* Add new tests in the [config_test.go](/internal/config/config_test.go)
* Add new test configs in teh [testapp.go](/internal/utils/testapp/testapp.go)

## Development
  
### Running Chainsformer Server

Clone the Chainsformer service repo:
```shell
git clone https://github.com/coinbase/chainsformer.git
```

Change directory to the Chainsformer service repo:
```shell
cd chainsformer
```

Setup Chainstorage SDK credentials
```shell
export CHAINSTORAGE_SDK_AUTH_HEADER=cb-nft-api-token
export CHAINSTORAGE_SDK_AUTH_TOKEN=****
```

To set up Chainsformer for the first time (only done once):
```shell
make bootstrap
```

Rebuild Chainsformer:

```shell
make build
```

Start the Chainsformer service with default `CHAINSFORMER_CONFIG=ethereum-mainnet`:
```shell
make server
```

### Run test client

Query Chainsformer for a range of blocks
```shell
go run ./cmd/client --env local --blockchain ethereum --network mainnet --start 0 --end 10 --table blocks
```

Query Chainsformer for a range of block events
```shell
go run ./cmd/client --env local --blockchain ethereum --network mainnet --start 0 --end 10 --table streamed_blocks
```

### Use grpcurl

#### Query Chainsformer for a range of blocks
Calling the `GetSchema` API
```shell
cmd=$(echo -n '{"table": "blocks"}' | base64)
grpcurl --plaintext -d '{"cmd":'"\"$cmd\""',"type":2}' localhost:9090 arrow.flight.protocol.FlightService.GetSchema
```

Calling the `GetFlightInfo` API to partition the data
```shell
cmd=$(echo -n '{"batch_query": {"start_height": 0, "end_height": 10, "table": "blocks"}}' | base64)
grpcurl --plaintext -d '{"cmd":'"\"$cmd\""',"type":2}' localhost:9090 arrow.flight.protocol.FlightService.GetFlightInfo
```

Take one of the `ticket` returned by the above command
```
...
"endpoint": [
    {
      "ticket": {
        "ticket": "eyJiYXRjaF9xdWVyeSI6eyJlbmRfaGVpZ2h0IjoiMTAiLCJ0YWJsZSI6ImJsb2NrcyJ9fQ=="
      }
    }
  ]
...
```

Calling the `DoGet` API to get data for one of the partition
```shell
grpcurl --plaintext -d '{"ticket": "eyJiYXRjaF9xdWVyeSI6eyJlbmRfaGVpZ2h0IjoiMTAiLCJ0YWJsZSI6ImJsb2NrcyJ9fQ=="}' localhost:9090 arrow.flight.protocol.FlightService.DoGet
```

Calling the `DoGet` API to get data of a specific partition
```shell
cmd=$(echo -n '{"batch_query":{"start_height":"1", "end_height":"2", "table":"blocks"}}' | base64)
grpcurl --plaintext -d '{"ticket": '"\"$cmd\""'}' localhost:9090 arrow.flight.protocol.FlightService.DoGet
```

Calling the `DoAction` API to get the tip in ChainStorage via Chainsformer
```shell
grpcurl --plaintext -d '{"type": "TIP"}' localhost:9090 arrow.flight.protocol.FlightService.DoAction | jq '.body | @base64d'
```

#### Query Chainsformer for a range of blocks events
Calling the `GetSchema` API
```shell
cmd=$(echo -n '{"table": "streamed_blocks"}' | base64)
grpcurl --plaintext -d '{"cmd":'"\"$cmd\""',"type":2}' localhost:9090 arrow.flight.protocol.FlightService.GetSchema
```

Calling the `GetFlightInfo` API to partition the data
```shell
cmd=$(echo -n '{"stream_query": {"start_sequence": 0, "end_sequence": 10, "table": "streamed_blocks"}}' | base64)
grpcurl --plaintext -d '{"cmd":'"\"$cmd\""',"type":2}' localhost:9090 arrow.flight.protocol.FlightService.GetFlightInfo
```

Take one of the `ticket` returned by the above command
```
...
"endpoint": [
    {
      "ticket": {
        "ticket": "eyJzdHJlYW1fcXVlcnkiOnsic3RhcnRfc2VxdWVuY2UiOiIxIiwiZW5kX3NlcXVlbmNlIjoiMTAiLCJ0YWJsZSI6InN0cmVhbWVkX2Jsb2NrcyJ9fQ=="
      }
    }
  ]
...
```

Calling the `DoGet` API to get data for one of the partition
```shell
grpcurl --plaintext -d '{"ticket": "eyJzdHJlYW1fcXVlcnkiOnsic3RhcnRfc2VxdWVuY2UiOiIxIiwiZW5kX3NlcXVlbmNlIjoiMTAiLCJ0YWJsZSI6InN0cmVhbWVkX2Jsb2NrcyJ9fQ=="}' localhost:9090 arrow.flight.protocol.FlightService.DoGet
```

Calling the `DoGet` API to get data of a specific partition
```shell
cmd=$(echo -n '{"stream_query":{"start_sequence":"1", "end_sequence":"2", "table":"streamed_blocks"}}' | base64)
grpcurl --plaintext -d '{"ticket": '"\"$cmd\""'}' localhost:9090 arrow.flight.protocol.FlightService.DoGet
```

Calling the `DoAction` API to get the tip in ChainStorage via Chainsformer
```shell
grpcurl --plaintext -d '{"type": "STREAM_TIP"}' localhost:9090 arrow.flight.protocol.FlightService.DoAction | jq '.body | @base64d'
```

## Testing
### Unit Test

```shell
# Run everything
make test
```

### Integration Test
Under development

### Functional Test
Under development