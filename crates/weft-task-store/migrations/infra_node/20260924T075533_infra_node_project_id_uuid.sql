-- infra_node.project_id was text. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE infra_node ALTER COLUMN project_id TYPE uuid USING project_id::uuid;
