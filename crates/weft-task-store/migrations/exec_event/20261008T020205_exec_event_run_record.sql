-- execution_tag.execution_id was text. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE execution_tag ALTER COLUMN execution_id TYPE uuid USING execution_id::uuid;

CREATE TABLE run (
    attempts integer DEFAULT 1 NOT NULL,
    binary_hash text,
    cancel_cause jsonb,
    cancel_requested jsonb,
    cost_micro_usd bigint DEFAULT 0 NOT NULL,
    definition_hash text,
    delivered_until bigint,
    ended_at bigint,
    entry_node text,
    epoch integer DEFAULT 1 NOT NULL,
    error text,
    example text,
    execution_id uuid NOT NULL,
    fired_by text,
    holds_signals boolean DEFAULT false NOT NULL,
    instance_id text,
    keep_for bigint,
    keep_until bigint,
    keeping text NOT NULL,
    kind text NOT NULL,
    last_seq integer NOT NULL,
    outcome text,
    owner text,
    phase text NOT NULL,
    project_id uuid NOT NULL,
    recorded boolean NOT NULL,
    seed_of uuid,
    selection text,
    skipped integer DEFAULT 0 NOT NULL,
    source_version text,
    spec jsonb,
    stale text[] DEFAULT '{}'::text[] NOT NULL,
    started_at bigint NOT NULL,
    state text NOT NULL,
    tenant_id text NOT NULL,
    watch_end boolean DEFAULT false NOT NULL,
    wrote_files boolean DEFAULT false NOT NULL
);

CREATE TABLE run_log (
    events bytea NOT NULL,
    execution_id uuid NOT NULL,
    seq integer NOT NULL,
    writer text NOT NULL,
    written_at bigint NOT NULL
);

CREATE TABLE run_selection (
    digest text NOT NULL,
    selection jsonb NOT NULL
);

-- signal.execution_id was text. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE signal ALTER COLUMN execution_id TYPE uuid USING execution_id::uuid;

-- trigger_setup.execution_id was text. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE trigger_setup ALTER COLUMN execution_id TYPE uuid USING execution_id::uuid;

ALTER TABLE run ADD CONSTRAINT run_attempts_not_null NOT NULL attempts;

ALTER TABLE run ADD CONSTRAINT run_cost_micro_usd_not_null NOT NULL cost_micro_usd;

ALTER TABLE run ADD CONSTRAINT run_epoch_not_null NOT NULL epoch;

ALTER TABLE run ADD CONSTRAINT run_execution_id_not_null NOT NULL execution_id;

ALTER TABLE run ADD CONSTRAINT run_holds_signals_not_null NOT NULL holds_signals;

ALTER TABLE run ADD CONSTRAINT run_keeping_not_null NOT NULL keeping;

ALTER TABLE run ADD CONSTRAINT run_kind_not_null NOT NULL kind;

ALTER TABLE run ADD CONSTRAINT run_last_seq_not_null NOT NULL last_seq;

ALTER TABLE run ADD CONSTRAINT run_phase_not_null NOT NULL phase;

ALTER TABLE run ADD CONSTRAINT run_pkey PRIMARY KEY (execution_id);

ALTER TABLE run ADD CONSTRAINT run_project_id_not_null NOT NULL project_id;

ALTER TABLE run ADD CONSTRAINT run_recorded_not_null NOT NULL recorded;

ALTER TABLE run ADD CONSTRAINT run_skipped_not_null NOT NULL skipped;

ALTER TABLE run ADD CONSTRAINT run_stale_not_null NOT NULL stale;

ALTER TABLE run ADD CONSTRAINT run_started_at_not_null NOT NULL started_at;

ALTER TABLE run ADD CONSTRAINT run_state_not_null NOT NULL state;

ALTER TABLE run ADD CONSTRAINT run_tenant_id_not_null NOT NULL tenant_id;

ALTER TABLE run ADD CONSTRAINT run_watch_end_not_null NOT NULL watch_end;

ALTER TABLE run ADD CONSTRAINT run_wrote_files_not_null NOT NULL wrote_files;

ALTER TABLE run_log ADD CONSTRAINT run_log_events_not_null NOT NULL events;

ALTER TABLE run_log ADD CONSTRAINT run_log_execution_id_not_null NOT NULL execution_id;

ALTER TABLE run_log ADD CONSTRAINT run_log_pkey PRIMARY KEY (execution_id, seq);

ALTER TABLE run_log ADD CONSTRAINT run_log_seq_not_null NOT NULL seq;

ALTER TABLE run_log ADD CONSTRAINT run_log_writer_not_null NOT NULL writer;

ALTER TABLE run_log ADD CONSTRAINT run_log_written_at_not_null NOT NULL written_at;

ALTER TABLE run_selection ADD CONSTRAINT run_selection_digest_not_null NOT NULL digest;

ALTER TABLE run_selection ADD CONSTRAINT run_selection_pkey PRIMARY KEY (digest);

ALTER TABLE run_selection ADD CONSTRAINT run_selection_selection_not_null NOT NULL selection;

CREATE OR REPLACE FUNCTION triggers_notify_project()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    PERFORM pg_notify('weft_triggers', OLD.project_id::text);
                ELSE
                    PERFORM pg_notify('weft_triggers', NEW.project_id::text);
                END IF;
                RETURN NULL;
            END;
            $function$
;

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

CREATE INDEX run_end_unhandled ON run USING btree (project_id) WHERE ((state = 'ended'::text) AND (watch_end OR holds_signals));

CREATE INDEX run_expiry ON run USING btree (keep_until) WHERE (state = 'ended'::text);

CREATE INDEX run_in_flight ON run USING btree (owner) WHERE (state = 'running'::text);

CREATE INDEX run_listing ON run USING btree (project_id, started_at DESC);

CREATE INDEX run_live ON run USING btree (project_id) WHERE (state <> 'ended'::text);

CREATE INDEX run_queued ON run USING btree (started_at) WHERE (state = 'queued'::text);

CREATE TRIGGER signal_triggers_on_change AFTER UPDATE OF surface_kind, mount_path, mount_methods, project_id, node_id, spec_json, auth_kind, auth_config, port_snapshot, program_json, source_version, instance_id, activation_trigger, held_by ON signal FOR EACH ROW WHEN ((NOT new.is_resume)) EXECUTE FUNCTION triggers_notify_project();

CREATE TRIGGER signal_triggers_on_delete AFTER DELETE ON signal FOR EACH ROW WHEN ((NOT old.is_resume)) EXECUTE FUNCTION triggers_notify_project();

CREATE TRIGGER signal_triggers_on_insert AFTER INSERT ON signal FOR EACH ROW WHEN ((NOT new.is_resume)) EXECUTE FUNCTION triggers_notify_project();

-- Throws away every row in exec_event.
DROP TABLE exec_event;

-- Throws away every row in execution.
DROP TABLE execution;

DROP TRIGGER signal_parked_fire_notify_on_grow ON signal;

-- Throws away what is in signal.drain_claimed_at_unix. Ship this in a later release than the one 
-- that stopped reading the column, so replicas still on the old release do not fall over.
ALTER TABLE signal DROP COLUMN drain_claimed_at_unix;

-- Throws away what is in signal.drain_claimed_by. Ship this in a later release than the one 
-- that stopped reading the column, so replicas still on the old release do not fall over.
ALTER TABLE signal DROP COLUMN drain_claimed_by;

-- Throws away what is in signal.parked_fires. Ship this in a later release than the one 
-- that stopped reading the column, so replicas still on the old release do not fall over.
ALTER TABLE signal DROP COLUMN parked_fires;

DROP FUNCTION exec_event_ends_execution() CASCADE;

DROP FUNCTION exec_event_notify() CASCADE;

DROP FUNCTION signal_parked_fire_notify() CASCADE;

DROP FUNCTION weft_execution_started(p jsonb, p_kicks jsonb) CASCADE;

DROP FUNCTION weft_journal_append(p_execution_ids text[], p_segments text[], p_dedup_keys text[], p_created_at bigint, p_replica text, p_owner text, p_seeds jsonb, p_ends jsonb) CASCADE;

DROP FUNCTION weft_lock_execution(p_execution_id text) CASCADE;

DROP FUNCTION weft_seed_execution(p jsonb, p_owner text) CASCADE;

DROP FUNCTION weft_start_execution(p jsonb) CASCADE;

DROP FUNCTION weft_without_endings(p_segment jsonb) CASCADE;
