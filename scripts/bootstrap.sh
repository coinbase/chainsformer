#!/usr/bin/env bash

set -eo pipefail

go install github.com/mikefarah/yq/v4@v4.27.5
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.32.0
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0
go install go.uber.org/mock/mockgen@v0.4.0
go install github.com/gordonklaus/ineffassign@v0.0.0-20230610083614-0e73809eb601
go install github.com/kisielk/errcheck@v1.6.3
go install golang.org/x/tools/cmd/goimports@v0.16.1
go mod download
go mod tidy
