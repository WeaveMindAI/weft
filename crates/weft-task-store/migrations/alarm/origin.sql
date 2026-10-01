CREATE TABLE IF NOT EXISTS alarm (
            -- The wake's name (`Wake::name`): the same key and time is the
            -- same wake, set once however often it is asked for.
            name TEXT PRIMARY KEY,
            key TEXT NOT NULL,
            -- When it is due (unix milliseconds). A claimed wake has this
            -- pushed past its lease, so a delivery that never finishes
            -- comes due again.
            at_unix_ms BIGINT NOT NULL,
            -- The moment the wake was set for, what the receiver is told.
            set_for_unix_ms BIGINT NOT NULL,
            role TEXT NOT NULL,
            path TEXT NOT NULL,
            body JSONB NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0
        );
CREATE INDEX IF NOT EXISTS idx_alarm_due ON alarm(at_unix_ms);
