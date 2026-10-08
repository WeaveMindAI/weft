CREATE TABLE IF NOT EXISTS parked_fire (
            -- The trigger's signal token, or the token a waiting run's
            -- answer resolves.
            token TEXT NOT NULL,
            seq BIGINT GENERATED ALWAYS AS IDENTITY,
            -- The event's identity: the run it starts is
            -- `weft_core::door_fire::run_of_fire(fire_id)`, so an event
            -- handed over twice is born once.
            fire_id UUID NOT NULL UNIQUE,
            -- What the trigger wakes with: every event waits here already
            -- processed by its kind (`/process`).
            payload JSONB NOT NULL,
            -- Who sent it, for a trigger somebody outside calls.
            caller TEXT,
            -- How many times it failed to become a run, and the moment it
            -- may be tried again (`park_backoff_secs`).
            attempts INTEGER NOT NULL DEFAULT 0,
            not_before BIGINT NOT NULL,
            -- Set while it waits on what its instance provides: the
            -- refusal, naming each field. Not retried on a timer: the
            -- instance's next change of values routes it again.
            instance_gap JSONB,
            -- An answer to a waiting run, rather than an event of an entry.
            is_resume BOOLEAN NOT NULL,
            -- Set on an answer already taken off its wait (the wait's
            -- signal is gone) for a run a worker drives: the run it is
            -- for. Its worker takes it; if the worker lets go of the run
            -- first, the letting go writes it into the run's record
            -- (`weft_journal::record::resolve_handed_in`).
            execution_id UUID,
            PRIMARY KEY (token, seq)
        );
CREATE INDEX IF NOT EXISTS parked_fire_handed ON parked_fire (execution_id) WHERE execution_id IS NOT NULL;
CREATE OR REPLACE FUNCTION parked_fire_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify('weft_parked_fire', COALESCE(
                    (SELECT s.project_id FROM signal s WHERE s.token = NEW.token),
                    (SELECT r.project_id FROM run r WHERE r.execution_id = NEW.execution_id))::text);
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS parked_fire_notify_on_insert ON parked_fire;
CREATE TRIGGER parked_fire_notify_on_insert
            AFTER INSERT ON parked_fire
            FOR EACH ROW
            EXECUTE FUNCTION parked_fire_notify();
CREATE OR REPLACE FUNCTION parked_fire_drop_with_signal() RETURNS trigger AS $$
            BEGIN
                DELETE FROM parked_fire WHERE token = OLD.token AND execution_id IS NULL;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS parked_fire_drop_on_signal_delete ON signal;
CREATE TRIGGER parked_fire_drop_on_signal_delete
            AFTER DELETE ON signal
            FOR EACH ROW
            EXECUTE FUNCTION parked_fire_drop_with_signal();
