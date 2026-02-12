-- Cold storage schema with fully decomposed columns (PostgreSQL).
--
-- Uses BYTEA and BIGINT for correct PostgreSQL types.

CREATE TABLE IF NOT EXISTS headers (
    block_number              BIGINT PRIMARY KEY NOT NULL,
    block_hash                BYTEA NOT NULL,
    parent_hash               BYTEA NOT NULL,
    ommers_hash               BYTEA NOT NULL,
    beneficiary               BYTEA NOT NULL,
    state_root                BYTEA NOT NULL,
    transactions_root         BYTEA NOT NULL,
    receipts_root             BYTEA NOT NULL,
    logs_bloom                BYTEA NOT NULL,
    difficulty                BYTEA NOT NULL,
    gas_limit                 BIGINT NOT NULL,
    gas_used                  BIGINT NOT NULL,
    timestamp                 BIGINT NOT NULL,
    extra_data                BYTEA NOT NULL,
    mix_hash                  BYTEA NOT NULL,
    nonce                     BYTEA NOT NULL,
    base_fee_per_gas          BIGINT,
    withdrawals_root          BYTEA,
    blob_gas_used             BIGINT,
    excess_blob_gas           BIGINT,
    parent_beacon_block_root  BYTEA,
    requests_hash             BYTEA
);

CREATE INDEX IF NOT EXISTS idx_headers_hash ON headers (block_hash);

CREATE TABLE IF NOT EXISTS transactions (
    block_number              BIGINT NOT NULL,
    tx_index                  BIGINT NOT NULL,
    tx_hash                   BYTEA NOT NULL,
    tx_type                   INTEGER NOT NULL,
    sig_y_parity              INTEGER NOT NULL,
    sig_r                     BYTEA NOT NULL,
    sig_s                     BYTEA NOT NULL,
    chain_id                  BIGINT,
    nonce                     BIGINT NOT NULL,
    gas_limit                 BIGINT NOT NULL,
    to_address                BYTEA,
    value                     BYTEA NOT NULL,
    input                     BYTEA NOT NULL,
    gas_price                 BYTEA,
    max_fee_per_gas           BYTEA,
    max_priority_fee_per_gas  BYTEA,
    max_fee_per_blob_gas      BYTEA,
    blob_versioned_hashes     BYTEA,
    access_list               BYTEA,
    authorization_list        BYTEA,
    PRIMARY KEY (block_number, tx_index)
);

CREATE INDEX IF NOT EXISTS idx_tx_hash ON transactions (tx_hash);

CREATE TABLE IF NOT EXISTS receipts (
    block_number              BIGINT NOT NULL,
    tx_index                  BIGINT NOT NULL,
    tx_type                   INTEGER NOT NULL,
    success                   INTEGER NOT NULL,
    cumulative_gas_used       BIGINT NOT NULL,
    PRIMARY KEY (block_number, tx_index)
);

CREATE TABLE IF NOT EXISTS logs (
    block_number              BIGINT NOT NULL,
    tx_index                  BIGINT NOT NULL,
    log_index                 BIGINT NOT NULL,
    address                   BYTEA NOT NULL,
    topic0                    BYTEA,
    topic1                    BYTEA,
    topic2                    BYTEA,
    topic3                    BYTEA,
    data                      BYTEA NOT NULL,
    PRIMARY KEY (block_number, tx_index, log_index)
);

CREATE INDEX IF NOT EXISTS idx_logs_address ON logs (address);
CREATE INDEX IF NOT EXISTS idx_logs_topic0 ON logs (topic0);

CREATE TABLE IF NOT EXISTS signet_events (
    block_number              BIGINT NOT NULL,
    event_index               BIGINT NOT NULL,
    event_type                INTEGER NOT NULL,
    order_index               BIGINT NOT NULL,
    rollup_chain_id           BYTEA NOT NULL,
    sender                    BYTEA,
    to_address                BYTEA,
    value                     BYTEA,
    gas                       BYTEA,
    max_fee_per_gas           BYTEA,
    data                      BYTEA,
    rollup_recipient          BYTEA,
    amount                    BYTEA,
    token                     BYTEA,
    PRIMARY KEY (block_number, event_index)
);

CREATE TABLE IF NOT EXISTS zenith_headers (
    block_number              BIGINT PRIMARY KEY NOT NULL,
    host_block_number         BYTEA NOT NULL,
    rollup_chain_id           BYTEA NOT NULL,
    gas_limit                 BYTEA NOT NULL,
    reward_address            BYTEA NOT NULL,
    block_data_hash           BYTEA NOT NULL
);
