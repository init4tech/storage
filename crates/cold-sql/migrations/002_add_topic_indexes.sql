-- Add indexes on topic1, topic2, topic3 for log filtering.
--
-- topic0 already has an index (001_initial.sql). Most ERC-20/721
-- Transfer lookups filter on topic1 (from) or topic2 (to), which
-- previously required sequential scans.

CREATE INDEX IF NOT EXISTS idx_logs_topic1 ON logs (topic1);
CREATE INDEX IF NOT EXISTS idx_logs_topic2 ON logs (topic2);
CREATE INDEX IF NOT EXISTS idx_logs_topic3 ON logs (topic3);
