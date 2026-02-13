-- Add sender address to transactions table.
ALTER TABLE transactions ADD COLUMN from_address BLOB;
