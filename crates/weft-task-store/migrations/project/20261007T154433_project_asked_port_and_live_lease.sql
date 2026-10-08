ALTER TABLE project ADD COLUMN api_port_asked boolean DEFAULT false NOT NULL;

ALTER TABLE project ADD CONSTRAINT project_api_port_asked_not_null NOT NULL api_port_asked;
