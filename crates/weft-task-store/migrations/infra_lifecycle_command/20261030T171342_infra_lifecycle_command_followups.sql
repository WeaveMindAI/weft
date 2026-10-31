-- infra_health_rule.gave_up_seen was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE infra_health_rule ALTER COLUMN gave_up_seen TYPE text USING gave_up_seen::text;

-- infra_lifecycle_command.spec_json was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE infra_lifecycle_command ALTER COLUMN spec_json TYPE text USING spec_json::text;
