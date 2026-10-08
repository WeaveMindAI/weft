CREATE OR REPLACE FUNCTION weft_journal_append(p_execution_ids text[], p_segments text[], p_dedup_keys text[], p_created_at bigint, p_replica text, p_owner text, p_seeds jsonb, p_ends jsonb)
 RETURNS SETOF text
 LANGUAGE plpgsql
AS $function$
            DECLARE
                v_execution_id TEXT;
                v_seed JSONB;
            BEGIN
                FOR v_execution_id IN
                    SELECT x FROM unnest(p_execution_ids) AS x
                    UNION SELECT s->>'execution_id' FROM jsonb_array_elements(p_seeds) AS s
                    ORDER BY 1
                LOOP
                    PERFORM weft_lock_execution(v_execution_id);
                END LOOP;
                FOR v_seed IN SELECT s FROM jsonb_array_elements(p_seeds) AS s LOOP
                    PERFORM weft_seed_execution(v_seed, p_owner);
                END LOOP;
                RETURN QUERY
                WITH taken AS (
                    SELECT e.execution_id, e.dedup_key, e.n,
                           CASE WHEN EXISTS (SELECT 1 FROM execution x
                                             WHERE x.execution_id = e.execution_id AND x.ended_at_unix IS NOT NULL)
                                THEN weft_without_endings(e.segment::jsonb)::text
                                ELSE e.segment END AS segment
                    FROM unnest(p_execution_ids, p_segments, p_dedup_keys) WITH ORDINALITY AS e(execution_id, segment, dedup_key, n)
                    WHERE p_owner IS NULL
                       OR EXISTS (SELECT 1 FROM execution x
                                  WHERE x.execution_id = e.execution_id AND x.owner_replica = p_owner)
                ), written AS (
                    INSERT INTO exec_event (execution_id, events_json, kinds, created_at, replica, dedup_key)
                        SELECT t.execution_id, t.segment,
                               ARRAY(SELECT x->>'kind' FROM jsonb_array_elements(t.segment::jsonb) WITH ORDINALITY AS j(x, i) ORDER BY i),
                               p_created_at, p_replica, t.dedup_key
                        FROM taken t
                        WHERE t.segment <> '[]'
                        ORDER BY t.n
                        ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING
                ), searched AS (
                    INSERT INTO execution_search (execution_id, words)
                        SELECT s->>'execution_id', to_tsvector('simple', s->>'text')
                        FROM jsonb_array_elements(p_ends) AS s
                        WHERE (s->>'execution_id') IN (SELECT t.execution_id FROM taken t)
                        ON CONFLICT (execution_id) DO NOTHING
                ), filed AS (
                    UPDATE execution ec SET wrote_files = (s->>'wrote_files')::boolean
                        FROM jsonb_array_elements(p_ends) AS s
                        WHERE ec.execution_id = s->>'execution_id'
                          AND ec.execution_id IN (SELECT t.execution_id FROM taken t)
                )
                SELECT t.execution_id FROM taken t
                UNION
                SELECT s->>'execution_id' FROM jsonb_array_elements(p_seeds) AS s
                WHERE p_owner IS NULL
                   OR EXISTS (SELECT 1 FROM execution x
                              WHERE x.execution_id = s->>'execution_id' AND x.owner_replica = p_owner);
            END;
            $function$
;
