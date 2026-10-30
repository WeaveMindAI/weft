-- parked_fire.payload was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE parked_fire ALTER COLUMN payload TYPE text USING payload::text;
