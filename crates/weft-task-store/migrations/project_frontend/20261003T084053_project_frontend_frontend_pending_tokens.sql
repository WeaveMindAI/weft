ALTER TABLE project_frontend ADD COLUMN pending_token_ids uuid[] DEFAULT '{}'::uuid[] NOT NULL;

ALTER TABLE project_frontend ADD CONSTRAINT project_frontend_pending_token_ids_not_null NOT NULL pending_token_ids;

-- Throws away what is in project_frontend.next_token_id. Ship this in a later release than the one 
-- that stopped reading the column, so replicas still on the old release do not fall over.
ALTER TABLE project_frontend DROP COLUMN next_token_id;
