-- Cold storage schema with fully decomposed columns.
--
-- Compatible with both PostgreSQL and SQLite.
-- BLOB is used instead of BYTEA for cross-dialect compatibility
-- (sqlx maps both transparently to Vec<u8> / &[u8]).

CREATE TABLE IF NOT EXISTS headers (
    block_number              INTEGER PRIMARY KEY NOT NULL,
    block_hash                BLOB NOT NULL,
    parent_hash               BLOB NOT NULL,
    ommers_hash               BLOB NOT NULL,
    beneficiary               BLOB NOT NULL,
    state_root                BLOB NOT NULL,
    transactions_root         BLOB NOT NULL,
    receipts_root             BLOB NOT NULL,
    logs_bloom                BLOB NOT NULL,
    difficulty                BLOB NOT NULL,
    gas_limit                 INTEGER NOT NULL,
    gas_used                  INTEGER NOT NULL,
    timestamp                 INTEGER NOT NULL,
    extra_data                BLOB NOT NULL,
    mix_hash                  BLOB NOT NULL,
    nonce                     BLOB NOT NULL,
    base_fee_per_gas          INTEGER,
    withdrawals_root          BLOB,
    blob_gas_used             INTEGER,
    excess_blob_gas           INTEGER,
    parent_beacon_block_root  BLOB,
    requests_hash             BLOB
);

CREATE INDEX IF NOT EXISTS idx_headers_hash ON headers (block_hash);

CREATE TABLE IF NOT EXISTS transactions (
    block_number              INTEGER NOT NULL,
    tx_index                  INTEGER NOT NULL,
    tx_hash                   BLOB NOT NULL,
    tx_type                   INTEGER NOT NULL,
    sig_y_parity              INTEGER NOT NULL,
    sig_r                     BLOB NOT NULL,
    sig_s                     BLOB NOT NULL,
    chain_id                  INTEGER,
    nonce                     INTEGER NOT NULL,
    gas_limit                 INTEGER NOT NULL,
    to_address                BLOB,
    value                     BLOB NOT NULL,
    input                     BLOB NOT NULL,
    gas_price                 BLOB,
    max_fee_per_gas           BLOB,
    max_priority_fee_per_gas  BLOB,
    max_fee_per_blob_gas      BLOB,
    blob_versioned_hashes     BLOB,
    access_list               BLOB,
    authorization_list        BLOB,
    PRIMARY KEY (block_number, tx_index)
);

CREATE INDEX IF NOT EXISTS idx_tx_hash ON transactions (tx_hash);

CREATE TABLE IF NOT EXISTS receipts (
    block_number              INTEGER NOT NULL,
    tx_index                  INTEGER NOT NULL,
    tx_type                   INTEGER NOT NULL,
    success                   INTEGER NOT NULL,
    cumulative_gas_used       INTEGER NOT NULL,
    PRIMARY KEY (block_number, tx_index)
);

CREATE TABLE IF NOT EXISTS logs (
    block_number              INTEGER NOT NULL,
    tx_index                  INTEGER NOT NULL,
    log_index                 INTEGER NOT NULL,
    address                   BLOB NOT NULL,
    topic0                    BLOB,
    topic1                    BLOB,
    topic2                    BLOB,
    topic3                    BLOB,
    data                      BLOB NOT NULL,
    PRIMARY KEY (block_number, tx_index, log_index)
);

CREATE INDEX IF NOT EXISTS idx_logs_address_block ON logs (address, block_number);
CREATE INDEX IF NOT EXISTS idx_logs_topic0 ON logs (topic0);

CREATE TABLE IF NOT EXISTS signet_events (
    block_number              INTEGER NOT NULL,
    event_index               INTEGER NOT NULL,
    event_type                INTEGER NOT NULL,
    order_index               INTEGER NOT NULL,
    rollup_chain_id           BLOB NOT NULL,
    sender                    BLOB,
    to_address                BLOB,
    value                     BLOB,
    gas                       BLOB,
    max_fee_per_gas           BLOB,
    data                      BLOB,
    rollup_recipient          BLOB,
    amount                    BLOB,
    token                     BLOB,
    PRIMARY KEY (block_number, event_index)
);

CREATE TABLE IF NOT EXISTS zenith_headers (
    block_number              INTEGER PRIMARY KEY NOT NULL,
    host_block_number         BLOB NOT NULL,
    rollup_chain_id           BLOB NOT NULL,
    gas_limit                 BLOB NOT NULL,
    reward_address            BLOB NOT NULL,
    block_data_hash           BLOB NOT NULL
);
