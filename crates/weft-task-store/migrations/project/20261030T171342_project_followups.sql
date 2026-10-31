-- project.infra_image_tags_json was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE project ALTER COLUMN infra_image_tags_json TYPE text USING infra_image_tags_json::text;

ALTER TABLE project ALTER COLUMN infra_image_tags_json SET DEFAULT '{}'::text;

-- Postgres changes no type of a column a trigger holds: they come down before it, and go back up once every column and function is in.
DROP TRIGGER IF EXISTS project_worker_settings_on_change ON project;
-- project.worker_settings_json was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE project ALTER COLUMN worker_settings_json TYPE text USING worker_settings_json::text;

ALTER TABLE project ALTER COLUMN worker_settings_json SET DEFAULT '{}'::text;

-- project_code.node_needs was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE project_code ALTER COLUMN node_needs TYPE text USING node_needs::text;

ALTER TABLE project_code ALTER COLUMN node_needs SET DEFAULT '{}'::text;

DROP TRIGGER IF EXISTS project_worker_settings_on_change ON project;
CREATE TRIGGER project_worker_settings_on_change AFTER UPDATE OF worker_settings_json ON project FOR EACH ROW WHEN ((new.worker_settings_json IS DISTINCT FROM old.worker_settings_json)) EXECUTE FUNCTION project_worker_settings_notify();
