CREATE TABLE image_use (
    image_ref text NOT NULL,
    project_id uuid NOT NULL,
    running_since bigint NOT NULL
);

ALTER TABLE image_use ADD CONSTRAINT image_use_image_ref_not_null NOT NULL image_ref;

ALTER TABLE image_use ADD CONSTRAINT image_use_pkey PRIMARY KEY (project_id, image_ref);

ALTER TABLE image_use ADD CONSTRAINT image_use_project_id_not_null NOT NULL project_id;

ALTER TABLE image_use ADD CONSTRAINT image_use_running_since_not_null NOT NULL running_since;
