ALTER TABLE infra_event ADD COLUMN writer_xid xid8 DEFAULT '0'::xid8 NOT NULL;

ALTER TABLE infra_event ADD CONSTRAINT infra_event_writer_xid_not_null NOT NULL writer_xid;

CREATE INDEX idx_infra_event_settled ON infra_event USING btree (writer_xid, id);
