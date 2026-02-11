# signet-cold-sql

SQL backend for signet-cold storage.

## Testing

### SQLite (no external dependencies)

```sh
cargo t -p signet-cold-sql --features test-utils
```

### PostgreSQL (requires Docker)

A script is provided to start a Postgres container and run the full test
suite (SQLite + PostgreSQL):

```sh
./scripts/test-postgres.sh
```

This starts a Postgres 16 container via `docker compose`, runs the
conformance tests with `DATABASE_URL` set, and tears down the container
on exit.

**Manual steps** (if you prefer to manage Postgres yourself):

```sh
# 1. Start Postgres (any method)
docker compose up -d --wait postgres

# 2. Run tests with the connection string
DATABASE_URL="postgres://signet:signet@localhost:5432/signet_test" \
  cargo t -p signet-cold-sql --features test-utils

# 3. Tear down
docker compose down
```
