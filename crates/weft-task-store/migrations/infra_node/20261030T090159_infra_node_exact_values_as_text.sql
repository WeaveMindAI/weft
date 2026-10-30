-- Postgres changes no type of a column a trigger holds: they come down around it.
DROP TRIGGER IF EXISTS infra_node_status_on_change ON infra_node;
-- infra_node.baked_json was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE infra_node ALTER COLUMN baked_json TYPE text USING baked_json::text;
DROP TRIGGER IF EXISTS infra_node_status_on_change ON infra_node;
CREATE TRIGGER infra_node_status_on_change AFTER UPDATE OF status, endpoints_json, public_paths_json, baked_json ON infra_node FOR EACH ROW WHEN (((new.status IS DISTINCT FROM old.status) OR (new.endpoints_json IS DISTINCT FROM old.endpoints_json) OR (new.public_paths_json IS DISTINCT FROM old.public_paths_json) OR (new.baked_json IS DISTINCT FROM old.baked_json))) EXECUTE FUNCTION infra_node_status_notify();

ALTER TABLE infra_node ALTER COLUMN baked_json SET DEFAULT '{}'::text;
