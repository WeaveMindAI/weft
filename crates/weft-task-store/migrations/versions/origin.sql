CREATE TABLE IF NOT EXISTS project_version (
            id           TEXT NOT NULL,
            -- Deliberately NO foreign key to `project`: the tree's life is
            -- the store's to decide, not the database's. A project's
            -- removal (`ProjectStore::remove`) drops the tree explicitly,
            -- so the same id registered again never inherits a tree it did
            -- not make and no version keeps naming stored files of a
            -- project that no longer exists. The journal's executions go
            -- the same way, at the same moment, so a removal leaves the
            -- person their files and nothing else.
            -- `retire_unused_versions` is the backstop for a removal that
            -- failed halfway, not a step of the ordinary one.
            project_id   UUID NOT NULL,
            parent_id    TEXT,
            manifest     JSONB NOT NULL,
            label        TEXT,
            created_at   BIGINT NOT NULL,
            -- Recording order. `created_at` is whole seconds, so two
            -- versions recorded in one second tie on it; `versions()`
            -- lists by this instead.
            seq          BIGSERIAL,
            PRIMARY KEY (project_id, id)
        );
CREATE INDEX IF NOT EXISTS idx_project_version_parent ON project_version(project_id, parent_id);
CREATE TABLE IF NOT EXISTS version_run (
            execution_id            UUID PRIMARY KEY,
            project_id       UUID NOT NULL,
            version_id       TEXT NOT NULL,
            seed_execution_id       UUID,
            stale            TEXT[] NOT NULL DEFAULT '{}',
            spec             JSONB,
            definition_hash  TEXT NOT NULL,
            example          TEXT,
            created_at       BIGINT NOT NULL,
            -- Recording order: what "newest run" means (the seed a
            -- bare head resolves to). Seconds tie, this never does.
            seq              BIGSERIAL,
            FOREIGN KEY (project_id, version_id) REFERENCES project_version(project_id, id) ON DELETE CASCADE
        );
CREATE INDEX IF NOT EXISTS idx_version_run_version ON version_run(project_id, version_id);
