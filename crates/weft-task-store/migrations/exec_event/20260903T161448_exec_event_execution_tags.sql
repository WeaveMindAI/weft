CREATE TABLE execution_tag (
    color text NOT NULL,
    seq bigserial NOT NULL,
    tag text NOT NULL,
    tagged_at_unix bigint NOT NULL
);

ALTER TABLE execution_tag ADD CONSTRAINT execution_tag_color_not_null NOT NULL color;

ALTER TABLE execution_tag ADD CONSTRAINT execution_tag_color_tag_key UNIQUE (color, tag);

ALTER TABLE execution_tag ADD CONSTRAINT execution_tag_pkey PRIMARY KEY (seq);

ALTER TABLE execution_tag ADD CONSTRAINT execution_tag_seq_not_null NOT NULL seq;

ALTER TABLE execution_tag ADD CONSTRAINT execution_tag_tag_not_null NOT NULL tag;

ALTER TABLE execution_tag ADD CONSTRAINT execution_tag_tagged_at_unix_not_null NOT NULL tagged_at_unix;

CREATE INDEX idx_execution_tag_tag ON execution_tag USING btree (tag);
