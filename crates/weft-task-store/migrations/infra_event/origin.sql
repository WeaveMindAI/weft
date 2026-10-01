CREATE TABLE IF NOT EXISTS infra_event (
            id          BIGSERIAL PRIMARY KEY,
            tenant_id   TEXT NOT NULL,
            project_id  UUID NOT NULL,
            node_id     TEXT,
            -- Whose copy of the node the event is about: NULL for the
            -- shared copy, or for a project-wide event.
            member_id   TEXT,
            kind        TEXT NOT NULL,
            payload     JSONB NOT NULL,
            at_unix     BIGINT NOT NULL,
            -- The transaction that wrote the row, the order the
            -- bridge's cursor reads in (`crate::settled`).
            writer_xid  XID8 NOT NULL DEFAULT pg_current_xact_id()
        );
CREATE INDEX IF NOT EXISTS idx_infra_event_chrono ON infra_event(id);
CREATE INDEX IF NOT EXISTS idx_infra_event_settled ON infra_event(writer_xid, id);
CREATE INDEX IF NOT EXISTS idx_infra_event_project ON infra_event(project_id);
CREATE OR REPLACE FUNCTION infra_event_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify('weft_infra_event', NEW.id::text);
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS infra_event_notify_on_insert ON infra_event;
CREATE TRIGGER infra_event_notify_on_insert
            AFTER INSERT ON infra_event
            FOR EACH ROW
            EXECUTE FUNCTION infra_event_notify();
