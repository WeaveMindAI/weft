ALTER TABLE dispatcher_cursor ADD COLUMN last_xid xid8 DEFAULT '0'::xid8 NOT NULL;

ALTER TABLE dispatcher_cursor ADD CONSTRAINT dispatcher_cursor_last_xid_not_null NOT NULL last_xid;
