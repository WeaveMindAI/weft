CREATE TABLE IF NOT EXISTS unanswered_call (
            -- What does not answer (`Callee::key`).
            callee TEXT PRIMARY KEY,
            -- The last try's error, as the caller logged it.
            error TEXT NOT NULL,
            -- The first failure of this stretch and the last, in unix
            -- milliseconds on the database's clock.
            since_ms BIGINT NOT NULL,
            last_ms BIGINT NOT NULL
        );
