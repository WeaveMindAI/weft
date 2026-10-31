CREATE TABLE holder_load (
    cpu_percent integer NOT NULL,
    replica text NOT NULL,
    room bigint NOT NULL,
    settled boolean NOT NULL
);

ALTER TABLE holder_load ADD CONSTRAINT holder_load_cpu_percent_check CHECK ((cpu_percent >= 0));

ALTER TABLE holder_load ADD CONSTRAINT holder_load_pkey PRIMARY KEY (replica);

CREATE TRIGGER holder_load_on_change AFTER UPDATE OF cpu_percent, settled, room ON holder_load FOR EACH ROW WHEN (((new.cpu_percent IS DISTINCT FROM old.cpu_percent) OR (new.room IS DISTINCT FROM old.room) OR (new.settled AND (NOT old.settled)))) EXECUTE FUNCTION signal_held_notify();

CREATE TRIGGER holder_load_on_delete AFTER DELETE ON holder_load FOR EACH ROW EXECUTE FUNCTION signal_held_notify();

CREATE TRIGGER holder_load_on_insert AFTER INSERT ON holder_load FOR EACH ROW EXECUTE FUNCTION signal_held_notify();
