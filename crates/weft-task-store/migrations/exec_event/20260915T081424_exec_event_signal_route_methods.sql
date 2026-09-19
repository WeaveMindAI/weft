ALTER TABLE signal ADD COLUMN mount_methods text[] DEFAULT '{}'::text[] NOT NULL;

ALTER TABLE signal ADD CONSTRAINT signal_mount_methods_not_null NOT NULL mount_methods;

DROP INDEX idx_signal_mount_path;
CREATE UNIQUE INDEX idx_signal_mount_path ON signal USING btree (mount_path, mount_methods) WHERE (mount_path IS NOT NULL);
