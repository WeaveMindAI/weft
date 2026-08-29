-- Durable terminate-sweep queue: a row per terminated color whose
        -- un-kept exec files still need sweeping. Inserted by the journal
        -- bridge (the durable observer of terminate), deleted by the sweep
        -- reaper once the broker confirmed the sweep.
        CREATE TABLE IF NOT EXISTS storage_sweep (
            color TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            enqueued_at_unix BIGINT NOT NULL
        );
