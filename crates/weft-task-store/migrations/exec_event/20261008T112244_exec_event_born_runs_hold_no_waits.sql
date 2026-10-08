CREATE OR REPLACE FUNCTION weft_record_batch(p_writer text, p_tenant text, p_lane text, p_now bigint, p_durable boolean, p_ids uuid[], p_epochs integer[], p_first integer[], p_last integer[], p_costs bigint[], p_skipped integer[], p_wrote boolean[], p_born jsonb, p_ended jsonb, p_row_run integer[], p_row_seq integer[], p_row_events bytea[], p_resolved_run integer[], p_resolved_token text[], p_selections jsonb)
 RETURNS TABLE(o_execution_id uuid, o_fate text, o_project_id uuid, o_tell_end boolean)
 LANGUAGE plpgsql
AS $function$
            DECLARE
                v_fates TEXT[];
                v_inserted BIGINT;
            BEGIN
                PERFORM set_config('synchronous_commit', CASE WHEN p_durable THEN 'on' ELSE 'off' END, true);
                INSERT INTO run_selection (digest, selection)
                    SELECT s->>'digest', s->'selection' FROM jsonb_array_elements(p_selections) AS s
                    ON CONFLICT (digest) DO NOTHING;
                PERFORM 1 FROM run r WHERE r.execution_id = ANY(p_ids) ORDER BY r.execution_id FOR UPDATE;
                SELECT array_agg(CASE
                        WHEN v.first_seq = 0 AND r.execution_id IS NULL THEN 'insert'
                        WHEN v.first_seq > 0 AND r.last_seq = v.first_seq - 1 AND r.owner = p_writer
                             AND r.epoch = v.epoch AND r.state = 'running' THEN 'continue'
                        WHEN EXISTS (SELECT 1 FROM run_log l
                                     WHERE l.execution_id = v.id AND l.seq = v.last_seq AND l.writer = p_writer) THEN 'already_applied'
                        WHEN v.first_seq = 0 THEN 'born_elsewhere'
                        ELSE 'refused' END ORDER BY v.n)
                    INTO v_fates
                    FROM unnest(p_ids, p_epochs, p_first, p_last) WITH ORDINALITY AS v(id, epoch, first_seq, last_seq, n)
                    LEFT JOIN run r ON r.execution_id = v.id;
                WITH born AS (
                    INSERT INTO run (execution_id, project_id, tenant_id, phase, kind, keeping, recorded, entry_node,
                                     instance_id, fired_by, source_version, definition_hash, binary_hash, selection,
                                     seed_of, state, owner, epoch, last_seq, started_at, ended_at, outcome, error,
                                     cancel_cause, skipped, wrote_files, cost_micro_usd, keep_for, keep_until)
                    SELECT v.id, (x.b->>'project_id')::uuid, p_tenant, x.b->>'phase', x.b->>'kind', x.b->>'keeping',
                           (x.b->>'recorded')::boolean, x.b->>'entry_node', x.b->>'instance_id', x.b->>'fired_by',
                           x.b->>'source_version', x.b->>'definition_hash', x.b->>'binary_hash', x.b->>'selection',
                           (x.b->>'seed_of')::uuid,
                           CASE WHEN x.e IS NULL THEN 'running' ELSE 'ended' END,
                           CASE WHEN x.e IS NULL THEN p_writer END,
                           v.epoch, v.last_seq, (x.b->>'started_at')::bigint, (x.e->>'at')::bigint, x.e->>'outcome',
                           x.e->>'error', x.e->'cancel_cause', v.skipped, v.wrote, v.costs, (x.b->>'keep_for')::bigint,
                           (x.e->>'at')::bigint + (x.b->>'keep_for')::bigint
                    FROM unnest(p_ids, p_epochs, p_last, p_costs, p_skipped, p_wrote, v_fates)
                            WITH ORDINALITY AS v(id, epoch, last_seq, costs, skipped, wrote, fate, n)
                        CROSS JOIN LATERAL (SELECT p_born->(v.n::integer - 1) AS b,
                                                   NULLIF(p_ended->(v.n::integer - 1), 'null'::jsonb) AS e) x
                    WHERE v.fate = 'insert'
                    ORDER BY v.id
                    ON CONFLICT (execution_id) DO NOTHING
                    RETURNING 1
                ) SELECT count(*) INTO v_inserted FROM born;
                -- A run another worker inserted between the read above and
                -- this insert (both bore one fire's run) is theirs.
                IF v_inserted < (SELECT count(*) FROM unnest(v_fates) AS f(fate) WHERE f.fate = 'insert') THEN
                    SELECT array_agg(CASE WHEN v.fate = 'insert' AND r.xmin <> xid(pg_current_xact_id())
                                          THEN 'born_elsewhere' ELSE v.fate END ORDER BY v.n)
                        INTO v_fates
                        FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        LEFT JOIN run r ON r.execution_id = v.id;
                END IF;
                UPDATE run r SET
                        last_seq = v.last_seq,
                        cost_micro_usd = r.cost_micro_usd + v.costs,
                        skipped = r.skipped + v.skipped,
                        wrote_files = r.wrote_files OR v.wrote,
                        state = CASE WHEN x.e IS NULL THEN r.state ELSE 'ended' END,
                        owner = CASE WHEN x.e IS NULL THEN r.owner END,
                        ended_at = (x.e->>'at')::bigint,
                        outcome = x.e->>'outcome',
                        error = x.e->>'error',
                        cancel_cause = x.e->'cancel_cause',
                        keep_until = (x.e->>'at')::bigint + r.keep_for,
                        holds_signals = x.e IS NOT NULL AND EXISTS (SELECT 1 FROM signal s WHERE s.execution_id = r.execution_id)
                    FROM unnest(p_ids, p_last, p_costs, p_skipped, p_wrote, v_fates)
                            WITH ORDINALITY AS v(id, last_seq, costs, skipped, wrote, fate, n)
                        CROSS JOIN LATERAL (SELECT NULLIF(p_ended->(v.n::integer - 1), 'null'::jsonb) AS e) x
                    WHERE v.fate = 'continue' AND r.execution_id = v.id;
                INSERT INTO run_log (execution_id, seq, events, writer, written_at)
                    SELECT p_ids[w.run], w.seq, w.events, p_writer, p_now
                    FROM unnest(p_row_run, p_row_seq, p_row_events) AS w(run, seq, events)
                    WHERE v_fates[w.run] IN ('insert', 'continue')
                    ORDER BY 1, 2
                    ON CONFLICT (execution_id, seq) DO NOTHING;
                DELETE FROM parked_fire pf
                    USING unnest(p_resolved_run, p_resolved_token) AS t(run, token)
                    WHERE v_fates[t.run] IN ('insert', 'continue') AND pf.token = t.token AND pf.is_resume;
                -- An answer handed to a run that ended without taking it
                -- has nobody left to take it.
                DELETE FROM parked_fire pf
                    USING unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                    WHERE v.fate IN ('insert', 'continue') AND p_ended->(v.n::integer - 1) <> 'null'::jsonb
                      AND pf.execution_id = v.id;
                INSERT INTO version_runs (project_id, source_version, lane, runs, last_run)
                    SELECT (x.b->>'project_id')::uuid, x.b->>'source_version', p_lane, count(*),
                           (array_agg(v.id ORDER BY v.id DESC))[1]
                    FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        CROSS JOIN LATERAL (SELECT p_born->(v.n::integer - 1) AS b) x
                    WHERE v.fate = 'insert' AND x.b->>'source_version' IS NOT NULL
                      AND x.b->>'kind' = 'execution' AND (x.b->>'recorded')::boolean
                    GROUP BY 1, 2
                    ORDER BY 1, 2
                    ON CONFLICT (project_id, source_version, lane) DO UPDATE
                        SET runs = version_runs.runs + EXCLUDED.runs,
                            last_run = GREATEST(version_runs.last_run, EXCLUDED.last_run);
                INSERT INTO run_search_queue (execution_id)
                    SELECT r.execution_id
                    FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        JOIN run r ON r.execution_id = v.id
                    WHERE v.fate IN ('insert', 'continue') AND p_ended->(v.n::integer - 1) <> 'null'::jsonb
                      AND r.recorded
                    ON CONFLICT (execution_id) DO NOTHING;
                INSERT INTO storage_sweep (execution_id, tenant_id, enqueued_at_unix)
                    SELECT r.execution_id, r.tenant_id, p_now
                    FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        JOIN run r ON r.execution_id = v.id
                    WHERE v.fate IN ('insert', 'continue') AND p_ended->(v.n::integer - 1) <> 'null'::jsonb
                      AND r.wrote_files
                    ON CONFLICT (execution_id) DO NOTHING;
                RETURN QUERY
                    SELECT v.id,
                           CASE WHEN v.fate IN ('insert', 'continue') THEN 'accepted' ELSE v.fate END,
                           r.project_id,
                           COALESCE(v.fate IN ('insert', 'continue') AND r.state = 'ended'
                                    AND (r.watch_end OR r.holds_signals), FALSE)
                    FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        LEFT JOIN run r ON r.execution_id = v.id
                    ORDER BY v.n;
            END;
            $function$
;
