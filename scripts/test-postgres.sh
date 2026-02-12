#!/usr/bin/env bash
set -euo pipefail

docker compose up -d --wait postgres
trap 'docker compose down' EXIT

export DATABASE_URL="postgres://signet:signet@localhost:5432/signet_test"
cargo t -p signet-cold-sql --features test-utils
