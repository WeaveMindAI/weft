ALTER TABLE role_loop_due ALTER COLUMN due_ms DROP NOT NULL;

ALTER TABLE role_loop_due DROP CONSTRAINT IF EXISTS role_loop_due_due_ms_not_null;
