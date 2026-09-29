-- Durable terminate-sweep queue: a row per terminated execution whose
        -- un-kept exec files still need sweeping. Inserted by the journal
        -- bridge (the durable observer of terminate), deleted by the sweep
        -- reaper once the broker confirmed the sweep.
        CREATE TABLE IF NOT EXISTS storage_sweep (
            execution_id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            enqueued_at_unix BIGINT NOT NULL
        );
CREATE OR REPLACE FUNCTION storage_sweep_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify('weft_storage_sweep', NEW.execution_id);
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS storage_sweep_notify_on_insert ON storage_sweep;
CREATE TRIGGER storage_sweep_notify_on_insert
            AFTER INSERT ON storage_sweep
            FOR EACH ROW
            EXECUTE FUNCTION storage_sweep_notify();
