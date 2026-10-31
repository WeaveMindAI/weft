-- access_connect.spec_json was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE access_connect ALTER COLUMN spec_json TYPE text USING spec_json::text;

-- Added nullable: fill it before the contract part makes it NOT NULL.
ALTER TABLE access_grant ADD COLUMN exclusive boolean;

-- Added nullable: fill it before the contract part makes it NOT NULL.
ALTER TABLE access_grant ADD COLUMN spec text;

-- The rows already here hold 1; the contract part drops the default.
ALTER TABLE access_grant ADD COLUMN spec_version integer DEFAULT 1 NOT NULL;

-- service_events_recipe.events_json was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE service_events_recipe ALTER COLUMN events_json TYPE text USING events_json::text;
