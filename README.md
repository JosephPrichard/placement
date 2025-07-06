# Placement

A global canvas for the community to draw whatever they want.

## Setup

Create a .env file and put these values for local testing, or your own for a prod env.

```
SCYLLA_URI=127.0.0.1:9042`
REDIS_URL=redis://127.0.0.1:6380/
```

## Build & Execution

Run Docker Compose

`export PATH="$PATH:$(go env GOPATH)/bin"`

`protoc --go_out=. --go_opt=paths=source_relative pb/placement.proto`

`docker compose up`

`go run main.go`

## Scripts

Create Scylla Schema

`docker compose up`

`go run main.go --scripts`

## Testing

Run Docker Compose

`docker compose -p test up`

`echo "SELECT keyspace_name, table_name FROM system_schema.tables;" | docker exec -i test-scylla-1 cqlsh`

^This step is important to initialize the scylla instance

`go test ./...`