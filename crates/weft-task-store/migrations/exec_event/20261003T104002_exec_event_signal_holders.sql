ALTER TABLE signal ADD COLUMN held_by text;

ALTER TABLE signal ADD COLUMN held_until bigint;

ALTER TABLE signal ADD COLUMN holds boolean DEFAULT false NOT NULL;

ALTER TABLE signal ADD COLUMN serving jsonb;

ALTER TABLE signal ADD CONSTRAINT signal_holds_not_null NOT NULL holds;

CREATE OR REPLACE FUNCTION signal_held_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                PERFORM pg_notify('weft_held_signals', '');
                RETURN NULL;
            END;
            $function$
;

CREATE INDEX idx_signal_held ON signal USING btree (held_until) WHERE holds;

CREATE TRIGGER signal_held_on_change AFTER UPDATE OF holds ON signal FOR EACH ROW WHEN ((new.holds IS DISTINCT FROM old.holds)) EXECUTE FUNCTION signal_held_notify();

CREATE TRIGGER signal_held_on_delete AFTER DELETE ON signal FOR EACH ROW WHEN (old.holds) EXECUTE FUNCTION signal_held_notify();

CREATE TRIGGER signal_held_on_insert AFTER INSERT ON signal FOR EACH ROW WHEN (new.holds) EXECUTE FUNCTION signal_held_notify();
