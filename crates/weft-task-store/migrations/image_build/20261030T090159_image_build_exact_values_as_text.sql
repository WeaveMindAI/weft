-- version_build.inputs was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE version_build ALTER COLUMN inputs TYPE text USING inputs::text;

-- version_build.program was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE version_build ALTER COLUMN program TYPE text USING program::text;

-- version_build.registered was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE version_build ALTER COLUMN registered TYPE text USING registered::text;
