ALTER TABLE project_version ADD COLUMN seq bigserial NOT NULL;

ALTER TABLE version_run ADD COLUMN seq bigserial NOT NULL;

ALTER TABLE project_version ADD CONSTRAINT project_version_seq_not_null NOT NULL seq;

ALTER TABLE version_run ADD CONSTRAINT version_run_seq_not_null NOT NULL seq;
