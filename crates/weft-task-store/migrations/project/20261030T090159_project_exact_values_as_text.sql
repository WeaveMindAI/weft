-- project.api_address was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE project ALTER COLUMN api_address TYPE text USING api_address::text;

-- project.resync was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE project ALTER COLUMN resync TYPE text USING resync::text;

-- project.running_build_inputs was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE project ALTER COLUMN running_build_inputs TYPE text USING running_build_inputs::text;

-- project_code.left_out was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE project_code ALTER COLUMN left_out TYPE text USING left_out::text;

ALTER TABLE project_code ALTER COLUMN left_out SET DEFAULT '[]'::text;
