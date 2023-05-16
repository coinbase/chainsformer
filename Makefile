TARGET ?= ...
TEST_FILTER ?= .
ifeq ($(TEST_FILTER),.)
	TEST_INTEGRATION_FILTER=TestIntegration
else
	TEST_INTEGRATION_FILTER=$(TEST_FILTER)
endif

SRCS=$(shell find . -name '*.go' -type f | grep -v -e ./protos -e /mocks -e '^./config/config.go')

.EXPORT_ALL_VARIABLES:
CHAINSFORMER_CONFIG ?= ethereum-mainnet
GO111MODULE ?= on
CLIENT_FLAGS ?= -env local -blockchain ethereum -network mainnet -blocks_per_partition 5 -start 14047030 -end 14047040 -blocks_per_record 0 -table transactions

ifeq ($(CI),)
define docker_compose_up
	docker-compose -f docker-compose-testing.yml up -d --force-recreate
	sleep 10
endef
define docker_compose_down
	docker-compose -f docker-compose-testing.yml down
endef
else
define docker_compose_up
endef
define docker_compose_down
endef
endif

.PHONY: build
build: codegen fmt build-go
	@echo "--- build"

.PHONY: bootstrap
bootstrap:
	@echo "--- bootstrap"
	scripts/bootstrap.sh

.PHONY: bin
bin:
	@echo "--- bin"
	mkdir -p bin
	go build -o bin ./$(TARGET)

.PHONY: build-go
build-go:
	@echo "--- build-go"
	mkdir -p bin
	go build -o bin ./$(TARGET)

.PHONY: test
test: fmt lint
	@echo "--- test"
	TEST_TYPE=unit go test ./$(TARGET) -run=$(TEST_FILTER)

.PHONY: lint
lint:
	@echo "--- lint"
	go vet -printfuncs=wrapf,statusf,warnf,infof,debugf,failf,equalf,containsf,fprintf,sprintf ./...
	errcheck -ignoretests -ignoregenerated ./...
	ineffassign ./...

.PHONY: integration
integration:
	@echo "--- integration"
	$(call docker_compose_up)
	TEST_TYPE=integration go test ./$(TARGET) -v -p=1 -parallel=1 -timeout=15m -failfast -run=$(TEST_INTEGRATION_FILTER)
	$(call docker_compose_down)

.PHONY: codegen
codegen:
	@echo "--- codegen"
	./scripts/protogen.sh
	./scripts/mockgen.sh
	@go generate ./...

.PHONY: fmt
fmt:
	@echo "--- fmt"
	@goimports -l -w -local github.com/coinbase/chainsformer $(SRCS)

.PHONY: server
server:
	go run ./cmd/server

.PHONY: client
client:
	go run ./cmd/client $(CLIENT_FLAGS)

.PHONY: docker-build
docker-build:
	@echo "--- docker-build"
	docker build -t coinbase/chainsformer .

.PHONY: docker-run
docker-run:
	docker run --rm --network host --name chainsformer coinbase/chainsformer
