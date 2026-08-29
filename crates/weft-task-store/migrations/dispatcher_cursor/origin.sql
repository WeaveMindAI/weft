CREATE TABLE IF NOT EXISTS dispatcher_cursor (
            key TEXT PRIMARY KEY,
            last_id BIGINT NOT NULL
        );
