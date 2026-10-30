-- alarm.body was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE alarm ALTER COLUMN body TYPE text USING body::text;
