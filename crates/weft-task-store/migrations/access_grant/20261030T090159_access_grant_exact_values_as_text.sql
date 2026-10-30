-- access_connect_result.result_json was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE access_connect_result ALTER COLUMN result_json TYPE text USING result_json::text;

-- instance_value.value was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE instance_value ALTER COLUMN value TYPE text USING value::text;

-- signal_subscription.captures_json was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE signal_subscription ALTER COLUMN captures_json TYPE text USING captures_json::text;

ALTER TABLE signal_subscription ALTER COLUMN captures_json SET DEFAULT '{}'::text;
