# Cafi (Content Addressable File Indexer)

Cafi is a fast and efficient file indexing system designed to track and synchronize file metadata across multiple sources. It uses a gRPC-based bidirectional streaming protocol for real-time synchronization and BLAKE3 for high-performance content hashing.

## Features

- **Blazing Fast Hashing**: Uses [BLAKE3](https://github.com/zeebo/blake3) for high-performance file hashing.
- **Efficient Synchronization**: Bidirectional gRPC streaming for low-latency updates between clients and server.
- **Content-Addressable**: Files are indexed by their BLAKE3 hash, enabling efficient storage and deduplication tracking.
- **MIME Type Detection**: Automatically detects file types using magic bytes.
- **Multi-Source Support**: Track files from multiple machines or directories as independent "sources".
- **Searchable Index**: Includes a dedicated search tool to query the indexed file paths.
- **Stateful Client**: The client maintains a local state to only re-scan and sync modified or new files.

## Project Structure

- `cmd/cafi-server`: gRPC server that manages the database and handles synchronization.
- `cmd/cafi-client`: CLI tool to scan local directories and sync with the server.
- `cmd/cafi-search`: CLI tool to query the indexed files in the database.
- `internal/`: Core logic including database interactions, auth, gRPC server implementation, and scanner logic.
- `proto/`: Protocol Buffer definitions for the gRPC service.

## Prerequisites

- **Go 1.25+**
- **PostgreSQL 14+**
- **[mise-en-place](https://mise.jdx.dev/)** (optional, for task management)
- **Docker & Compose** (optional, for running dependencies)

## Getting Started

### 1. Start the Infrastructure

You can use the provided `compose.yaml` to start a PostgreSQL instance:

```bash
docker compose up -d
```

### 2. Build the Binaries

Using `mise`:
```bash
mise run build
```

Or manually:
```bash
go build -o bin/cafi-client ./cmd/cafi-client/
go build -o bin/cafi-server ./cmd/cafi-server/
go build -o bin/cafi-search ./cmd/cafi-search/
```

### 3. Run the Server

The server requires a PostgreSQL connection string.

```bash
export DATABASE_URL="postgres://cafi:cafi@localhost:5434/cafi"
./bin/cafi-server serve
```

You can also manage users and sources via the server CLI:

```bash
./bin/cafi-server user add myuser
./bin/cafi-server source add myuser my-laptop
```

### 4. Run the Client

The client needs the server address and a source token (generated when adding a source).

```bash
export CAFI_TOKEN="your_source_token"
./bin/cafi-client scan ~/Pictures --source=my-laptop
```

### 5. Search the Index

```bash
export CAFI_DATABASE_URL="postgres://cafi:cafi@localhost:5434/cafi"
./bin/cafi-search paths --filter='*.jpg'
```

## Configuration

### Server
- `DATABASE_URL`: PostgreSQL connection string.
- `PORT`: gRPC server port (default: 50051).

### Client
- `CAFI_TOKEN`: Authentication token for the source.
- `CAFI_SERVER`: Server address (default: `localhost:50051`).

## Development

The project uses `mise` for common tasks:

- `mise run proto`: Generate Go code from Protobuf definitions.
- `mise run lint`: Run `golangci-lint`.
- `mise run test`: Run all tests.
- `mise run docker-build`: Build the server Docker image.

## License

See [LICENSE](LICENSE) file for details.
