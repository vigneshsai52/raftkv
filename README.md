# RaftKV
Distributed key-value store with Raft consensus algorithm, built in Go.

## Features
- **Raft Consensus**: Leader election, log replication, automatic failover
- **HTTP API**: RESTful GET/POST/DELETE operations
- **WAL Persistence**: Write-ahead log for durability
- **Snapshotting**: Automatic log compaction
- **Metrics**: Built-in monitoring endpoint
- **Docker**: Ready for containerized deployment

## Quick Start
```bash
# Single node
go run main.go --http-addr=:8080

# Test it
curl -X POST http://localhost:8080/kv/hello -d '{"value":"world"}'
curl http://localhost:8080/kv/hello
```

## Architecture
```
Client → HTTP Server → RaftStore → Raft Node → WAL
                                      ↓
                                    Apply → In-Memory Store
```

## Tech Stack
- Go 1.21+
- Raft consensus (custom implementation)
- HTTP REST API
- Docker & Docker Compose

## Project Structure

| Directory | Purpose |
|-----------|---------|
| raft/     | Raft consensus engine |
| server/   | HTTP API handlers |
| store/    | In-memory KV + WAL |
| tools/    | Benchmark utilities |
| deploy/   | Production deployment scripts |

## Benchmarks
```bash
go run tools/bench.go --addr=http://localhost:8080 --workers=10 --ops=10000
```
Expected: ~10,000 ops/sec

## License
MIT
