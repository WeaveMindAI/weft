CREATE TABLE IF NOT EXISTS infra_event (
            id          BIGSERIAL PRIMARY KEY,
            tenant_id   TEXT NOT NULL,
            project_id  TEXT NOT NULL,
            node_id     TEXT,
            kind        TEXT NOT NULL,
            payload     JSONB NOT NULL,
            at_unix     BIGINT NOT NULL
        );
CREATE INDEX IF NOT EXISTS idx_infra_event_chrono ON infra_event(id);
CREATE INDEX IF NOT EXISTS idx_infra_event_project ON infra_event(project_id);
