CREATE TABLE IF NOT EXISTS role_revision (
            -- The role (`CoreRole::as_str`): one row each.
            role TEXT PRIMARY KEY,
            -- The platform's name for the revision of the copy that
            -- started serving last (Cloud Run's `K_REVISION`).
            revision TEXT NOT NULL,
            -- When it did, in unix milliseconds on the database's clock.
            started_ms BIGINT NOT NULL
        );
