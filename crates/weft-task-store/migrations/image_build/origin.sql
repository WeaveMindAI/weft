CREATE TABLE IF NOT EXISTS image_build (
            -- The content-addressed ref the build pushes: one build per
            -- content, whichever project asked for it.
            image_ref TEXT PRIMARY KEY,
            project_id UUID NOT NULL,
            tenant_id TEXT NOT NULL,
            -- The build's instance, minted before it starts.
            build_name TEXT NOT NULL,
            -- The compile lane a worker build holds (see
            -- weft_compiler::worker_image::COMPILE_LANE_ARG).
            lane INTEGER NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'cancelled')),
            reason TEXT,
            -- The dispatcher instance driving the build, and until when its
            -- hold lasts without renewal.
            driver_instance TEXT NOT NULL,
            driver_until BIGINT NOT NULL,
            started_at BIGINT NOT NULL,
            finished_at BIGINT
        );
