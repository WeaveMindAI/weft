CREATE TABLE version_runs (
    lane text NOT NULL,
    last_run uuid NOT NULL,
    project_id uuid NOT NULL,
    runs bigint NOT NULL,
    source_version text NOT NULL
);

ALTER TABLE version_runs ADD CONSTRAINT version_runs_lane_not_null NOT NULL lane;

ALTER TABLE version_runs ADD CONSTRAINT version_runs_last_run_not_null NOT NULL last_run;

ALTER TABLE version_runs ADD CONSTRAINT version_runs_pkey PRIMARY KEY (project_id, source_version, lane);

ALTER TABLE version_runs ADD CONSTRAINT version_runs_project_id_not_null NOT NULL project_id;

ALTER TABLE version_runs ADD CONSTRAINT version_runs_runs_not_null NOT NULL runs;

ALTER TABLE version_runs ADD CONSTRAINT version_runs_source_version_not_null NOT NULL source_version;

-- Throws away every row in version_run.
DROP TABLE version_run;
