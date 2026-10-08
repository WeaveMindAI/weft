-- storage_sweep.execution_id was text. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE storage_sweep ALTER COLUMN execution_id TYPE uuid USING execution_id::uuid;
