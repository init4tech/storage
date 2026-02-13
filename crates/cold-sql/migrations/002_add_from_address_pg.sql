-- Add sender address to transactions table (PostgreSQL).
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS from_address BYTEA;
