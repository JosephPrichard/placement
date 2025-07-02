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

`docker compose up`

`cargo run --package place --bin place`

## Scripts

Create Scylla Schema

`docker compose up`

`cargo run --package place --bin place`

## Testing

Run Docker Compose

`docker compose -p test up`

`echo "SELECT keyspace_name, table_name FROM system_schema.tables;" | docker exec -i test-scylla-1 cqlsh`

^This step is important to initialize the scylla instance

`cargo run --package place --bin test-place`