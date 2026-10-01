CREATE TABLE IF NOT EXISTS install_domain (
            -- Lower case, as `weft_core::install::normalize_domain_name`
            -- leaves it.
            name TEXT PRIMARY KEY,
            -- What it serves: 'install', 'frontend' or 'api'.
            serves TEXT NOT NULL CHECK (serves IN ('install', 'frontend', 'api')),
            -- The project a frontend or API domain belongs to.
            project_id UUID REFERENCES project(id) ON DELETE CASCADE,
            -- Where a frontend runs (its service's https address).
            upstream TEXT,
            added_unix BIGINT NOT NULL,
            CHECK ((serves = 'install') = (project_id IS NULL)),
            CHECK ((serves = 'frontend') = (upstream IS NOT NULL))
        );
