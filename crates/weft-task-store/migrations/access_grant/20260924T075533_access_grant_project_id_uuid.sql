-- access_connect.project_id was text. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE access_connect ALTER COLUMN project_id TYPE uuid USING project_id::uuid;

-- access_grant.project_id was text. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE access_grant ALTER COLUMN project_id TYPE uuid USING project_id::uuid;
