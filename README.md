# Placement

A global canvas for the community to draw whatever they want.

## Build & Execution

Run Docker Compose

`docker compose up`

`cargo run --package place --bin place`

## Testing

Run Docker Compose

`docker compose -p test up`

``
*this step is important to initialize the scylla instance

`cargo run --package place --bin test-place`