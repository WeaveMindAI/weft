CREATE UNLOGGED TABLE IF NOT EXISTS entry_rate (
            key TEXT NOT NULL,
            window_start BIGINT NOT NULL,
            hits INTEGER NOT NULL,
            PRIMARY KEY (key, window_start)
        );
CREATE TABLE IF NOT EXISTS entry_slot (
            execution_id TEXT PRIMARY KEY,
            signal_token TEXT NOT NULL,
            unborn_until BIGINT NOT NULL
        );
CREATE INDEX IF NOT EXISTS idx_entry_slot_token ON entry_slot(signal_token);
