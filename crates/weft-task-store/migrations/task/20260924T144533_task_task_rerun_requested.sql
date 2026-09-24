ALTER TABLE task ADD COLUMN rerun_requested boolean DEFAULT false NOT NULL;

ALTER TABLE task ADD CONSTRAINT task_rerun_requested_not_null NOT NULL rerun_requested;
