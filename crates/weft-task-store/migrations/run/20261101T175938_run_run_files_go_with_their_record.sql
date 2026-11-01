-- weft_record_batch takes other arguments now: a call with the old ones fails loudly (no such function).
DROP FUNCTION weft_record_batch(text, text, text, bigint, bigint, boolean, uuid[], integer[], integer[], integer[], integer[], boolean[], jsonb, bigint[], text[], text[], text[], integer[], integer[], bytea[], integer[], text[], integer[], integer[], text[], integer[], text[], text[], text[], double precision[], text[], text[], bigint[], integer[], text[], integer[], uuid[], text[], jsonb, integer);
CREATE OR REPLACE FUNCTION weft_record_batch(p_writer text, p_tenant text, p_lane text, p_now bigint, p_delivered_until bigint, p_durable boolean, p_ids uuid[], p_epochs integer[], p_first integer[], p_last integer[], p_skipped integer[], p_born jsonb, p_ended_at bigint[], p_outcomes text[], p_errors text[], p_cancel_causes text[], p_row_run integer[], p_row_seq integer[], p_row_events bytea[], p_resolved_run integer[], p_resolved_token text[], p_cost_run integer[], p_cost_seq integer[], p_cost_id text[], p_cost_idx integer[], p_cost_node text[], p_cost_service text[], p_cost_model text[], p_cost_amount double precision[], p_cost_paid_by text[], p_cost_paid_by_instance text[], p_cost_at bigint[], p_cost_path_cost integer[], p_cost_path_site text[], p_secret_run integer[], p_secret_id uuid[], p_secret_sealed text[], p_programs jsonb, p_events_version integer)
 RETURNS TABLE(o_run_id uuid, o_fate text, o_project_id uuid, o_tell_end boolean, o_keep_until bigint, o_unwatched boolean)
 LANGUAGE plpgsql
AS $function$
            DECLARE
                v_fates TEXT[];
                v_inserted BIGINT;
                v_writer_alive BOOLEAN := TRUE;
            BEGIN
                PERFORM set_config('synchronous_commit', CASE WHEN p_durable THEN 'on' ELSE 'off' END, true);
                PERFORM 1 FROM run r WHERE r.run_id = ANY(p_ids) ORDER BY r.run_id FOR UPDATE;
                SELECT array_agg(CASE
                        WHEN v.first_seq = 0 AND r.run_id IS NULL THEN 'insert'
                        WHEN v.first_seq > 0 AND r.last_seq = v.first_seq - 1 AND r.owner = p_writer
                             AND r.epoch = v.epoch AND r.state = 'running' THEN 'continue'
                        WHEN EXISTS (SELECT 1 FROM run_log l
                                     WHERE l.run_id = v.id AND l.seq = v.last_seq AND l.writer = p_writer) THEN 'already_applied'
                        WHEN v.first_seq = 0 THEN 'born_elsewhere'
                        ELSE 'refused' END ORDER BY v.n)
                    INTO v_fates
                    FROM unnest(p_ids, p_epochs, p_first, p_last) WITH ORDINALITY AS v(id, epoch, first_seq, last_seq, n)
                    LEFT JOIN run r ON r.run_id = v.id;
                WITH born AS (
                    INSERT INTO run (run_id, project_id, tenant_id, phase, kind, keeping, recorded, entry_node,
                                     instance_id, fired_by, source_version, definition_hash, cut_from, binary_hash,
                                     seed_of, state, owner, epoch, last_seq, started_at, ended_at, outcome, error,
                                     cancel_cause, skipped, keep_for, keep_until,
                                     delivered_until, spec_version)
                    SELECT v.id, (x.b->>'project_id')::uuid, p_tenant, x.b->>'phase', x.b->>'kind', x.b->>'keeping',
                           (x.b->>'recorded')::boolean, x.b->>'entry_node', x.b->>'instance_id', x.b->>'fired_by',
                           x.b->>'source_version', x.b->>'definition_hash', x.b->>'cut_from', x.b->>'binary_hash',
                           (x.b->>'seed_of')::uuid,
                           CASE WHEN v.ended_at IS NULL THEN 'running' ELSE 'ended' END,
                           CASE WHEN v.ended_at IS NULL THEN p_writer END,
                           v.epoch, v.last_seq, (x.b->>'started_at')::bigint, v.ended_at, v.outcome,
                           v.error, v.cancel_cause, v.skipped, (x.b->>'keep_for')::bigint,
                           v.ended_at + (x.b->>'keep_for')::bigint,
                           CASE WHEN v.ended_at IS NULL THEN p_delivered_until END, (x.b->>'spec_version')::integer
                    FROM unnest(p_ids, p_epochs, p_last, p_skipped, v_fates,
                                p_ended_at, p_outcomes, p_errors, p_cancel_causes)
                            WITH ORDINALITY AS v(id, epoch, last_seq, skipped, fate,
                                                 ended_at, outcome, error, cancel_cause, n)
                        CROSS JOIN LATERAL (SELECT p_born->(v.n::integer - 1) AS b) x
                    WHERE v.fate = 'insert'
                      AND EXISTS (SELECT 1 FROM project p WHERE p.id = (x.b->>'project_id')::uuid AND NOT p.removing)
                    ORDER BY v.id
                    ON CONFLICT (run_id) DO NOTHING
                    RETURNING 1
                ) SELECT count(*) INTO v_inserted FROM born;
                -- A run another worker inserted between the read above and
                -- this insert (both bore one fire's run) is theirs; one
                -- whose project is gone or being removed went nowhere,
                -- and the worker is told which.
                IF v_inserted < (SELECT count(*) FROM unnest(v_fates) AS f(fate) WHERE f.fate = 'insert') THEN
                    SELECT array_agg(CASE WHEN v.fate = 'insert' AND r.run_id IS NULL AND p.removing THEN 'project_removing'
                                          WHEN v.fate = 'insert' AND r.run_id IS NULL THEN 'refused'
                                          WHEN v.fate = 'insert' AND r.xmin <> xid(pg_current_xact_id())
                                          THEN 'born_elsewhere' ELSE v.fate END ORDER BY v.n)
                        INTO v_fates
                        FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        LEFT JOIN run r ON r.run_id = v.id
                        LEFT JOIN project p ON p.id = (p_born->(v.n::integer - 1)->>'project_id')::uuid;
                END IF;
                -- A cut is kept only for a run born now that runs it,
                -- marked with the registered program it was cut from: a
                -- refused birth keeps nothing, and nothing but a cut a
                -- birth names goes in.
                INSERT INTO project_definition (project_id, definition_hash, project_json, project_json_version,
                                                recorded_at_unix, cut_from)
                    SELECT r.project_id, r.definition_hash, c->>'project_json',
                           (c->>'project_json_version')::integer, p_now, r.cut_from
                    FROM jsonb_array_elements(p_programs) AS c
                        JOIN unnest(p_ids, v_fates) AS v(id, fate) ON v.fate = 'insert'
                        JOIN run r ON r.run_id = v.id
                    WHERE r.project_id = (c->>'project_id')::uuid AND r.definition_hash = c->>'definition_hash'
                      AND r.cut_from IS NOT NULL
                    ON CONFLICT (project_id, definition_hash) DO NOTHING;
                UPDATE run r SET
                        last_seq = v.last_seq,
                        skipped = r.skipped + v.skipped,
                        state = CASE WHEN v.ended_at IS NULL THEN r.state ELSE 'ended' END,
                        owner = CASE WHEN v.ended_at IS NULL THEN r.owner END,
                        ended_at = v.ended_at,
                        outcome = v.outcome,
                        error = v.error,
                        cancel_cause = v.cancel_cause,
                        keep_until = v.ended_at + r.keep_for,
                        holds_signals = v.ended_at IS NOT NULL AND EXISTS (SELECT 1 FROM signal s WHERE s.run_id = r.run_id)
                    FROM unnest(p_ids, p_last, p_skipped, v_fates,
                                p_ended_at, p_outcomes, p_errors, p_cancel_causes)
                            WITH ORDINALITY AS v(id, last_seq, skipped, fate,
                                                 ended_at, outcome, error, cancel_cause, n)
                    WHERE v.fate = 'continue' AND r.run_id = v.id;
                -- Each cost is named by its run's ordinal, which names the
                -- run only when the run went in now: any other's costs go
                -- nowhere.
                PERFORM weft_record_costs(
                    ARRAY(SELECT CASE WHEN v_fates[c.run] IN ('insert', 'continue') THEN p_ids[c.run] END
                          FROM unnest(p_cost_run) WITH ORDINALITY AS c(run, n) ORDER BY c.n),
                    p_cost_seq, p_cost_id, p_cost_idx, p_cost_node, p_cost_service, p_cost_model, p_cost_amount,
                    p_cost_paid_by, p_cost_paid_by_instance, p_cost_at, p_cost_path_cost, p_cost_path_site);
                -- A run's secrets go in only when the run did, each once:
                -- a batch sent again finds them there.
                INSERT INTO run_secret (secret_id, project_id, run_id, sealed)
                    SELECT s.id, r.project_id, r.run_id, s.sealed
                    FROM unnest(p_secret_run, p_secret_id, p_secret_sealed) AS s(run, id, sealed)
                        JOIN run r ON r.run_id = p_ids[s.run]
                    WHERE v_fates[s.run] IN ('insert', 'continue')
                    ORDER BY s.id
                    ON CONFLICT (secret_id) DO NOTHING;
                INSERT INTO run_log (run_id, seq, events, events_version, writer, written_at)
                    SELECT p_ids[w.run], w.seq, w.events, p_events_version, p_writer, p_now
                    FROM unnest(p_row_run, p_row_seq, p_row_events) AS w(run, seq, events)
                    WHERE v_fates[w.run] IN ('insert', 'continue')
                    ORDER BY 1, 2
                    ON CONFLICT (run_id, seq) DO NOTHING;
                DELETE FROM parked_fire pf
                    USING unnest(p_resolved_run, p_resolved_token) AS t(run, token)
                    WHERE v_fates[t.run] IN ('insert', 'continue') AND pf.token = t.token AND pf.is_resume;
                -- An answer handed to a run that ended without taking it
                -- has nobody left to take it.
                DELETE FROM parked_fire pf
                    USING unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                    WHERE v.fate IN ('insert', 'continue') AND p_ended_at[v.n::integer] IS NOT NULL
                      AND pf.run_id = v.id;
                INSERT INTO version_runs (project_id, source_version, lane, runs, last_run)
                    SELECT (x.b->>'project_id')::uuid, x.b->>'source_version', p_lane, count(*),
                           (array_agg(v.id ORDER BY v.id DESC))[1]
                    FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        CROSS JOIN LATERAL (SELECT p_born->(v.n::integer - 1) AS b) x
                    WHERE v.fate = 'insert' AND x.b->>'source_version' IS NOT NULL
                      AND x.b->>'kind' = 'project' AND (x.b->>'recorded')::boolean
                    GROUP BY 1, 2
                    ORDER BY 1, 2
                    ON CONFLICT (project_id, source_version, lane) DO UPDATE
                        SET runs = version_runs.runs + EXCLUDED.runs,
                            last_run = GREATEST(version_runs.last_run, EXCLUDED.last_run);
                INSERT INTO run_search_queue (run_id)
                    SELECT r.run_id
                    FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        JOIN run r ON r.run_id = v.id
                    WHERE v.fate IN ('insert', 'continue') AND p_ended_at[v.n::integer] IS NOT NULL
                      AND r.recorded
                    ON CONFLICT (run_id) DO NOTHING;
                IF 'insert' = ANY(v_fates) THEN
                    v_writer_alive := EXISTS (SELECT 1 FROM worker_lease l
                                              WHERE l.replica = p_writer AND l.leased_until_unix >= EXTRACT(EPOCH FROM NOW())::BIGINT);
                END IF;
                RETURN QUERY
                    SELECT v.id,
                           CASE WHEN v.fate IN ('insert', 'continue') THEN 'accepted' ELSE v.fate END,
                           r.project_id,
                           COALESCE(v.fate IN ('insert', 'continue') AND r.state = 'ended'
                                    AND (r.watch_end OR r.holds_signals), FALSE),
                           CASE WHEN v.fate IN ('insert', 'continue') AND p_ended_at[v.n::integer] IS NOT NULL
                                THEN r.keep_until END,
                           COALESCE(v.fate = 'insert' AND r.state = 'running' AND NOT v_writer_alive, FALSE)
                    FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        LEFT JOIN run r ON r.run_id = v.id
                    ORDER BY v.n;
            END;
            $function$
;
