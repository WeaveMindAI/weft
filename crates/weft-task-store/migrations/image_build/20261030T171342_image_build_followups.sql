-- version_build.waits_on was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE version_build ALTER COLUMN waits_on TYPE text USING waits_on::text;
