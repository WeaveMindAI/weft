CREATE TABLE IF NOT EXISTS role_loop_due (
            -- The role (`CoreRole::as_str`) and one of its loops
            -- (`DrainLoop::name`).
            role TEXT NOT NULL,
            loop_name TEXT NOT NULL,
            -- When the loop next wants a look, in unix milliseconds on the
            -- database's clock, so instances whose own clocks disagree
            -- still agree on it.
            due_ms BIGINT NOT NULL,
            -- When this row was last written, on the same clock: how a
            -- pass tells whether a sibling booked a look since it read.
            written_ms BIGINT NOT NULL,
            PRIMARY KEY (role, loop_name)
        );
