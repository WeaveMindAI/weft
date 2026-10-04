ALTER TABLE project_frontend ALTER COLUMN token_id DROP NOT NULL;

ALTER TABLE project_frontend DROP CONSTRAINT IF EXISTS project_frontend_token_id_not_null;
