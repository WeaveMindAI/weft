ALTER TABLE worker_lease ADD COLUMN memory_full_at bigint;

ALTER TABLE worker_lease ADD COLUMN memory_limit bigint;

ALTER TABLE worker_lease ADD COLUMN memory_used bigint;
