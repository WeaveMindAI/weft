ALTER TABLE worker_pod ADD COLUMN held_until_unix bigint DEFAULT 0 NOT NULL;

ALTER TABLE worker_pod ADD CONSTRAINT worker_pod_held_until_unix_not_null NOT NULL held_until_unix;
