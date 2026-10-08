ALTER TABLE infra_node ADD COLUMN baked_json jsonb DEFAULT '{}'::jsonb NOT NULL;

ALTER TABLE infra_node ADD CONSTRAINT infra_node_baked_json_not_null NOT NULL baked_json;

DROP TRIGGER infra_node_status_on_change ON infra_node;
CREATE TRIGGER infra_node_status_on_change AFTER UPDATE OF status, endpoints_json, public_paths_json, baked_json ON infra_node FOR EACH ROW WHEN (((new.status IS DISTINCT FROM old.status) OR (new.endpoints_json IS DISTINCT FROM old.endpoints_json) OR (new.public_paths_json IS DISTINCT FROM old.public_paths_json) OR (new.baked_json IS DISTINCT FROM old.baked_json))) EXECUTE FUNCTION infra_node_status_notify();
