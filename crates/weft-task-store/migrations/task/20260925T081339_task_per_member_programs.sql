-- Fails if any row still holds a null here.
ALTER TABLE task ALTER COLUMN tenant_id SET NOT NULL;

ALTER TABLE task ADD CONSTRAINT task_tenant_id_not_null NOT NULL tenant_id;

DROP INDEX idx_task_tenant;
CREATE INDEX idx_task_tenant ON task USING btree (tenant_id);
