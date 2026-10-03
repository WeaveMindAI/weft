ALTER TABLE project_frontend ADD COLUMN next_token_id uuid;

ALTER TABLE project_frontend ADD COLUMN repo_id bigint;

ALTER TABLE project_frontend ADD CONSTRAINT project_frontend_check1 CHECK (((repo IS NULL) = (repo_id IS NULL)));
