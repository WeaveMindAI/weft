ALTER TABLE exec_event RENAME COLUMN instance TO replica;

ALTER TABLE signal RENAME COLUMN member_id TO instance_id;

ALTER TABLE signal_token RENAME COLUMN member_id TO instance_id;

ALTER TABLE trigger_bake RENAME COLUMN member_id TO instance_id;

ALTER TABLE execution RENAME COLUMN member_id TO instance_id;

ALTER TABLE execution RENAME COLUMN owner_instance TO owner_replica;

ALTER TABLE signal_token RENAME CONSTRAINT signal_token_member_expires TO signal_token_instance_expires;

ALTER TABLE signal_token RENAME CONSTRAINT signal_token_member_has_one_project TO signal_token_instance_has_one_project;

ALTER INDEX idx_execution_member RENAME TO idx_execution_instance;
