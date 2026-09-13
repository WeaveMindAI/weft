CREATE TABLE IF NOT EXISTS project_version (
            id           TEXT NOT NULL,
            project_id   UUID NOT NULL REFERENCES project(id) ON DELETE CASCADE,
            parent_id    TEXT,
            manifest     JSONB NOT NULL,
            label        TEXT,
            created_at   BIGINT NOT NULL,
            PRIMARY KEY (project_id, id)
        );
CREATE INDEX IF NOT EXISTS idx_project_version_parent ON project_version(project_id, parent_id);
CREATE TABLE IF NOT EXISTS version_run (
            color            UUID PRIMARY KEY,
            project_id       UUID NOT NULL,
            version_id       TEXT NOT NULL,
            seed_color       UUID,
            stale            TEXT[] NOT NULL DEFAULT '{}',
            spec             JSONB,
            definition_hash  TEXT NOT NULL,
            example          TEXT,
            -- What `weft check` concluded for the example this run
            -- checked: 'matched', 'drifted', or 'waiting'. NULL until
            -- the check reports, and on every other run.
            verdict          TEXT,
            created_at       BIGINT NOT NULL,
            FOREIGN KEY (project_id, version_id) REFERENCES project_version(project_id, id) ON DELETE CASCADE
        );
CREATE INDEX IF NOT EXISTS idx_version_run_version ON version_run(project_id, version_id);
