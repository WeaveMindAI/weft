ALTER TABLE exec_event RENAME COLUMN payload_json TO events_json;

ALTER TABLE exec_event RENAME CONSTRAINT exec_event_payload_json_not_null TO exec_event_events_json_not_null;

ALTER TABLE execution ADD COLUMN wrote_files boolean;

CREATE OR REPLACE FUNCTION exec_event_ends_execution()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                UPDATE execution ec
                    SET ended_at_unix = COALESCE(ec.ended_at_unix, a.created_at),
                        outcome = COALESCE(ec.outcome, a.outcome)
                    FROM (
                        SELECT DISTINCT ON (execution_id) execution_id, created_at,
                            CASE WHEN 'execution_completed' = ANY(kinds) THEN 'completed'
                                 WHEN 'execution_failed' = ANY(kinds) THEN 'failed'
                                 ELSE 'cancelled' END AS outcome
                        FROM added
                        WHERE kinds && ARRAY['execution_completed', 'execution_failed', 'execution_cancelled']::text[]
                        ORDER BY execution_id, id
                    ) a
                    WHERE ec.execution_id = a.execution_id;
                RETURN NULL;
            END;
            $function$
;

CREATE OR REPLACE FUNCTION weft_execution_started(p jsonb, p_kicks jsonb)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
            DECLARE
                v_execution_id TEXT := p->'seed'->>'execution_id';
                v_kicks TEXT := (SELECT string_agg(',' || (k->>'payload'), '' ORDER BY n)
                                 FROM jsonb_array_elements(p_kicks) WITH ORDINALITY AS e(k, n));
            BEGIN
                PERFORM weft_lock_execution(v_execution_id);
                IF (p->>'journaled')::boolean THEN
                    PERFORM weft_journal_append(ARRAY[v_execution_id],
                        ARRAY['[' || (p->>'payload') || COALESCE(v_kicks, '') || ']'],
                        ARRAY[p->>'dedup_key'], (p->>'created_at')::bigint, NULL, NULL, jsonb_build_array(p->'seed'), '[]'::jsonb);
                ELSE
                    PERFORM weft_seed_execution(p->'seed', NULL);
                END IF;
            END;
            $function$
;

CREATE OR REPLACE FUNCTION weft_journal_append(p_execution_ids text[], p_segments text[], p_dedup_keys text[], p_created_at bigint, p_replica text, p_owner text, p_seeds jsonb, p_ends jsonb)
 RETURNS SETOF text
 LANGUAGE plpgsql
AS $function$
            DECLARE
                v_execution_id TEXT;
                v_seed JSONB;
            BEGIN
                FOR v_execution_id IN SELECT DISTINCT x FROM unnest(p_execution_ids) AS x ORDER BY x LOOP
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
                SELECT DISTINCT t.execution_id FROM taken t;
            END;
            $function$
;

CREATE OR REPLACE FUNCTION weft_seed_execution(p jsonb, p_owner text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
            DECLARE
                v_execution_id TEXT := p->>'execution_id';
                v_project UUID := (p->>'project_id')::uuid;
                seeded BIGINT;
            BEGIN
                IF p->>'source_version' IS NOT NULL THEN
                    PERFORM 1 FROM project_version
                        WHERE project_id = v_project AND id = p->>'source_version' FOR KEY SHARE;
                    IF NOT FOUND THEN
                        RAISE EXCEPTION 'source version % was removed during preparation; run the command again',
                            p->>'source_version';
                    END IF;
                END IF;
                INSERT INTO execution (execution_id, project_id, tenant_id, started_at_unix, phase, kind, instance_id, fired_by, owner_replica)
                    SELECT v_execution_id, v_project, pr.tenant_id, (p->>'at_unix')::bigint, p->>'phase',
                           p->>'kind', p->>'instance_id', p->>'fired_by', p_owner
                    FROM project pr WHERE pr.id = v_project
                    ON CONFLICT (execution_id) DO NOTHING;
                GET DIAGNOSTICS seeded = ROW_COUNT;
                IF seeded = 0 AND NOT EXISTS (SELECT 1 FROM execution WHERE execution_id = v_execution_id) THEN
                    RAISE EXCEPTION 'refuse to journal ExecutionStarted for execution %: project % has no row, so the execution seed (which the broker scope check and the terminal sweeps depend on) cannot be written; register the project first',
                        v_execution_id, v_project;
                END IF;
            END;
            $function$
;

CREATE OR REPLACE FUNCTION weft_start_execution(p jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
            DECLARE
                v_started JSONB := p->'started';
                v_task JSONB := p->'task';
                v_execution_id TEXT := v_started->'seed'->>'execution_id';
                v_project UUID := (v_started->'seed'->>'project_id')::uuid;
                v_journaled BOOLEAN := (v_started->>'journaled')::boolean;
                v_refused JSONB;
                v_inserted BOOLEAN;
            BEGIN
                PERFORM weft_lock_execution(v_execution_id);
                IF (v_journaled AND EXISTS (SELECT 1 FROM exec_event
                        WHERE execution_id = v_execution_id AND 'execution_started' = ANY(kinds)))
                   OR (NOT v_journaled AND EXISTS (SELECT 1 FROM execution WHERE execution_id = v_execution_id)) THEN
                    RETURN jsonb_build_object('outcome', 'already_started');
                END IF;
                IF jsonb_typeof(p->'admission') = 'object' THEN
                    v_refused := weft_admit(p->'admission');
                    IF v_refused IS NOT NULL THEN
                        RETURN jsonb_build_object('outcome', 'refused', 'refused', v_refused);
                    END IF;
                END IF;
                SELECT d.inserted INTO v_inserted FROM weft_enqueue_dedup(
                    (v_task->>'id')::uuid, v_task->>'kind', v_task->>'target', (v_task->>'project_id')::uuid,
                    v_task->>'dedup_key', v_task->>'execution_id', v_task->>'tenant_id', v_task->>'target_replica',
                    v_task->>'binary_hash', v_task->'payload', (v_task->>'created_at')::bigint) d;
                IF NOT v_inserted THEN
                    RETURN jsonb_build_object('outcome', 'already_started');
                END IF;
                PERFORM weft_execution_started(v_started, p->'kicks');
                -- A trigger setup is born only while the activation that asked
                -- for it still owns its rows (a cancel between the claim and
                -- here wins), and is recorded as in flight.
                IF jsonb_typeof(p->'trigger_setup') = 'object' THEN
                    IF (p->'trigger_setup'->>'for_activation')::boolean THEN
                        PERFORM 1 FROM trigger_activation
                            WHERE project_id = v_project
                              AND activating_execution_id = v_execution_id::uuid
                              AND status = 'activating'
                            FOR UPDATE;
                        IF NOT FOUND THEN
                            RAISE EXCEPTION 'activation % ended before trigger setup could start', v_execution_id;
                        END IF;
                    END IF;
                    INSERT INTO trigger_setup (project_id, execution_id) VALUES (v_project, v_execution_id);
                END IF;
                RETURN jsonb_build_object('outcome', 'started');
            END;
            $function$
;

CREATE OR REPLACE FUNCTION weft_without_endings(p_segment jsonb)
 RETURNS jsonb
 LANGUAGE sql
 IMMUTABLE
AS $function$
            SELECT COALESCE(jsonb_agg(x ORDER BY i), '[]'::jsonb)
            FROM jsonb_array_elements(p_segment) WITH ORDINALITY AS t(x, i)
            WHERE NOT (x->>'kind' = ANY(ARRAY['execution_completed', 'execution_failed', 'execution_cancelled']::text[]))
            $function$
;

CREATE TRIGGER exec_event_ends_execution_on_insert AFTER INSERT ON exec_event REFERENCING NEW TABLE AS added FOR EACH STATEMENT EXECUTE FUNCTION exec_event_ends_execution();

-- Throws away what is in exec_event.kind. Ship this in a later release than the one 
-- that stopped reading the column, so replicas still on the old release do not fall over.
ALTER TABLE exec_event DROP COLUMN kind;

DROP INDEX IF EXISTS idx_exec_event_kind;

DROP FUNCTION weft_execution_started(p jsonb) CASCADE;

DROP FUNCTION weft_journal_append(p_execution_id text, p_kinds text[], p_payloads text[], p_created_at bigint, p_replica text, p_owner text, p_dedup_key text) CASCADE;
