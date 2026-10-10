ALTER TABLE project ADD COLUMN build_asks bigint DEFAULT 0 NOT NULL;

ALTER TABLE project ADD COLUMN registered_ask bigint DEFAULT 0 NOT NULL;

ALTER TABLE project ADD CONSTRAINT project_build_asks_not_null NOT NULL build_asks;

ALTER TABLE project ADD CONSTRAINT project_registered_ask_not_null NOT NULL registered_ask;
