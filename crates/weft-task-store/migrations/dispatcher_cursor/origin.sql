CREATE TABLE IF NOT EXISTS dispatcher_cursor (
            key TEXT PRIMARY KEY,
            last_id BIGINT NOT NULL,
            -- With `last_id`, the last row passed, in the order a settled
            -- read goes (`crate::settled`). A cursor starts at xid 0,
            -- below every writer: the rows a database held before
            -- `writer_xid` existed were all given 0, so they keep their
            -- `last_id` order behind the cursor, and any row written
            -- since sorts after them.
            last_xid XID8 NOT NULL DEFAULT '0'::xid8
        );
