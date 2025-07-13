# Placement

A global canvas for the community to draw whatever they want.

## Development

Run Development Infra

`docker compose up`

Create .env File

```
CASSANDRA_CONTACT_POINTS=127.0.0.1:9042`
REDIS_URL=redis://127.0.0.1:6380/
```
Create Scylla Schema (if first time)

`type ./app/cql/create.cql | docker exec -i placement-scylla-1 cqlsh`

Generate Sources

`export PATH="$PATH:$(go env GOPATH)/bin"`

`protoc --go_out=. --go_opt=paths=source_relative pb/placement.proto`

Run App

`go run main.go`

## Testing
Tests are full e2e tests run against live databases with mock data. 
Docker compose is used to set up and teardown all infrastructure required for tests.

Run Test Infra

`docker compose -p test up`

Create Scylla Schema

`echo "DROP TABLE IF EXISTS pks.placements; DROP TABLE IF EXISTS pks.tiles;" | docker exec -i placement-scylla-test-1 cqlsh`

`type ./app/cql/create.cql | docker exec -i placement-scylla-test-1 cqlsh`

Verify Schema

`echo "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'pks';" | docker exec -i placement-scylla-test-1 cqlsh`

Run Tests

`go test ./...`