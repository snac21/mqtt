.PHONY: all build clean test proto run deps lint

# Build settings
BINARY_NAME=mqtt-broker
GO=go
PROTOC=protoc

# Build flags
LDFLAGS=-ldflags "-s -w"

all: clean build

build:
	@echo "Building binary..."
	@mkdir -p bin
	$(GO) build $(LDFLAGS) -o bin/$(BINARY_NAME) cmd/main.go

clean:
	@echo "Cleaning up..."
	rm -rf bin/
	rm -f pkg/proto/*.pb.go

test:
	@echo "Running tests..."
	$(GO) test -v ./...

proto:
	@echo "Generating protobuf code..."
	$(PROTOC) --go_out=. --go_opt=paths=source_relative pkg/proto/message.proto

run: build
	@echo "Running broker..."
	./bin/$(BINARY_NAME)

deps:
	@echo "Installing dependencies..."
	$(GO) mod download
	$(GO) mod tidy

lint:
	@echo "Running linter..."
	golangci-lint run

.DEFAULT_GOAL := build 