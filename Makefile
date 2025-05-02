.PHONY: build clean proto deps test

# Variables
BINARY_NAME=mqtt-broker
PROTO_DIR=proto
GO=go
PROTOC=protoc

# Build flags
LDFLAGS=-ldflags "-w -s"

all: deps proto build

deps:
	$(GO) mod download
	$(GO) mod tidy

proto:
	@echo "Generating protobuf files..."
	$(PROTOC) --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/*.proto

build:
	@echo "Building binary..."
	$(GO) build $(LDFLAGS) -o bin/$(BINARY_NAME) cmd/main.go

clean:
	@echo "Cleaning..."
	rm -rf bin/
	rm -rf proto/*.pb.go

test:
	$(GO) test -v ./...

run: build
	./bin/$(BINARY_NAME)

docker-build:
	docker build -t mqtt-broker .

docker-run:
	docker run -p 1883:1883 -p 8080:8080 mqtt-broker 