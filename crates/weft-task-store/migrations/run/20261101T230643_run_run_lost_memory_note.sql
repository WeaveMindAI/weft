ALTER TABLE run ADD COLUMN lost_memory_full_at bigint;

ALTER TABLE run ADD COLUMN lost_memory_limit bigint;

ALTER TABLE run ADD COLUMN lost_memory_used bigint;
