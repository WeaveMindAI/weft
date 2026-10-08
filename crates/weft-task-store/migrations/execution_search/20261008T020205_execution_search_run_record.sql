CREATE TABLE run_search (
    execution_id uuid NOT NULL,
    nodes text[] NOT NULL,
    words tsvector NOT NULL
);

CREATE TABLE run_search_queue (
    execution_id uuid NOT NULL
);

ALTER TABLE run_search ADD CONSTRAINT run_search_execution_id_not_null NOT NULL execution_id;

ALTER TABLE run_search ADD CONSTRAINT run_search_nodes_not_null NOT NULL nodes;

ALTER TABLE run_search ADD CONSTRAINT run_search_pkey PRIMARY KEY (execution_id);

ALTER TABLE run_search ADD CONSTRAINT run_search_words_not_null NOT NULL words;

ALTER TABLE run_search_queue ADD CONSTRAINT run_search_queue_execution_id_not_null NOT NULL execution_id;

ALTER TABLE run_search_queue ADD CONSTRAINT run_search_queue_pkey PRIMARY KEY (execution_id);

CREATE INDEX run_search_nodes ON run_search USING gin (nodes);

CREATE INDEX run_search_words ON run_search USING gin (words);

-- Throws away every row in execution_search.
DROP TABLE execution_search;

DROP FUNCTION weft_run_search_text(p_execution_id text, p_max integer, p_value_max integer) CASCADE;
