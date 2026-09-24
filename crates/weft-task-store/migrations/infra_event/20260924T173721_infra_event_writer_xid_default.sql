ALTER TABLE infra_event ALTER COLUMN writer_xid SET DEFAULT pg_current_xact_id();
