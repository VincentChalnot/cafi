.PHONY: proto build lint test docker-build

# Generate protobuf code using buf
proto:
	buf generate

# Build all binaries
build:
	CGO_ENABLED=1 go build -o bin/cafi-client ./cmd/cafi-client/
	CGO_ENABLED=0 go build -o bin/cafi-server ./cmd/cafi-server/
	CGO_ENABLED=0 go build -o bin/cafi-search ./cmd/cafi-search/

# Run golangci-lint
lint:
	golangci-lint run ./...

# Run tests
test:
	CGO_ENABLED=1 go test ./...

# Build Docker image for cafi-server
docker-build:
	docker build -t cafi-server .
