ALTER TABLE exec_event ADD COLUMN writer_xid xid8 DEFAULT '0'::xid8 NOT NULL;

ALTER TABLE exec_event ADD CONSTRAINT exec_event_writer_xid_not_null NOT NULL writer_xid;

CREATE INDEX idx_exec_event_settled ON exec_event USING btree (writer_xid, id);
