-- infra_node.doors_json was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE infra_node ALTER COLUMN doors_json TYPE text USING doors_json::text;

ALTER TABLE infra_node ALTER COLUMN doors_json SET DEFAULT '{}'::text;

-- Postgres changes no type of a column a trigger holds: they come down before it, and go back up once every column and function is in.
DROP TRIGGER IF EXISTS infra_node_status_on_change ON infra_node;
-- infra_node.endpoints_json was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE infra_node ALTER COLUMN endpoints_json TYPE text USING endpoints_json::text;

ALTER TABLE infra_node ALTER COLUMN endpoints_json SET DEFAULT '{}'::text;

-- infra_node.health_rules_json was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE infra_node ALTER COLUMN health_rules_json TYPE text USING health_rules_json::text;

-- infra_node.keep_disks_json was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE infra_node ALTER COLUMN keep_disks_json TYPE text USING keep_disks_json::text;

ALTER TABLE infra_node ALTER COLUMN keep_disks_json SET DEFAULT '[]'::text;

-- infra_node.notes_json was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE infra_node ALTER COLUMN notes_json TYPE text USING notes_json::text;

ALTER TABLE infra_node ALTER COLUMN notes_json SET DEFAULT '[]'::text;

-- Postgres changes no type of a column a trigger holds: they come down before it, and go back up once every column and function is in.
DROP TRIGGER IF EXISTS infra_node_status_on_change ON infra_node;
-- infra_node.public_paths_json was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE infra_node ALTER COLUMN public_paths_json TYPE text USING public_paths_json::text;

ALTER TABLE infra_node ALTER COLUMN public_paths_json SET DEFAULT '{}'::text;

DROP TRIGGER IF EXISTS infra_node_status_on_change ON infra_node;
CREATE TRIGGER infra_node_status_on_change AFTER UPDATE OF status, endpoints_json, public_paths_json, baked_json ON infra_node FOR EACH ROW WHEN (((new.status IS DISTINCT FROM old.status) OR (new.endpoints_json IS DISTINCT FROM old.endpoints_json) OR (new.public_paths_json IS DISTINCT FROM old.public_paths_json) OR (new.baked_json IS DISTINCT FROM old.baked_json))) EXECUTE FUNCTION infra_node_status_notify();
