ALTER TABLE access_connect RENAME COLUMN member_id TO instance_id;

ALTER TABLE access_grant RENAME COLUMN member_id TO instance_id;

ALTER TABLE access_picker RENAME COLUMN member_id TO instance_id;

ALTER TABLE signal_subscription RENAME COLUMN member_id TO instance_id;

ALTER TABLE member_value RENAME TO instance_value;

ALTER TABLE instance_value RENAME COLUMN member_id TO instance_id;

ALTER TABLE access_grant RENAME CONSTRAINT access_grant_member_has_project TO access_grant_instance_has_project;

ALTER TABLE instance_value RENAME CONSTRAINT member_value_field_not_null TO instance_value_field_not_null;

ALTER TABLE instance_value RENAME CONSTRAINT member_value_grant_id_fkey TO instance_value_grant_id_fkey;

ALTER TABLE instance_value RENAME CONSTRAINT member_value_member_id_not_null TO instance_value_instance_id_not_null;

ALTER TABLE instance_value RENAME CONSTRAINT member_value_pkey TO instance_value_pkey;

ALTER TABLE instance_value RENAME CONSTRAINT member_value_project_id_not_null TO instance_value_project_id_not_null;

ALTER TABLE instance_value RENAME CONSTRAINT member_value_set_at_not_null TO instance_value_set_at_not_null;

ALTER TABLE instance_value RENAME CONSTRAINT member_value_step_not_null TO instance_value_step_not_null;

ALTER TABLE instance_value RENAME CONSTRAINT member_value_tenant_id_not_null TO instance_value_tenant_id_not_null;

ALTER TABLE instance_value RENAME CONSTRAINT member_value_value_not_null TO instance_value_value_not_null;

ALTER INDEX access_grant_member RENAME TO access_grant_instance;

ALTER INDEX member_value_grant RENAME TO instance_value_grant;
