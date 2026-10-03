CREATE TABLE IF NOT EXISTS project_frontend (
            project_id UUID NOT NULL REFERENCES project(id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            -- Where it runs: 'cloud_run' (the install made its service) or
            -- 'external'.
            host TEXT NOT NULL CHECK (host IN ('cloud_run', 'external')),
            -- The GitHub repository that deploys a frontend the install
            -- hosts, its service there, and where visitors reach it.
            repo TEXT,
            service TEXT,
            url TEXT,
            -- The caller token it calls with (`signal_token.id`).
            token_id UUID NOT NULL,
            added_unix BIGINT NOT NULL,
            PRIMARY KEY (project_id, name),
            CHECK ((host = 'cloud_run') = (repo IS NOT NULL))
        );
