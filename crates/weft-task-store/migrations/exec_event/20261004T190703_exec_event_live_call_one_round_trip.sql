CREATE OR REPLACE FUNCTION routes_notify_tenant()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    PERFORM pg_notify('weft_routes', OLD.tenant_id);
                ELSE
                    PERFORM pg_notify('weft_routes', NEW.tenant_id);
                END IF;
                RETURN NULL;
            END;
            $function$
;

CREATE OR REPLACE FUNCTION weft_execution_started(p jsonb)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
            DECLARE
                v_execution_id TEXT := p->>'execution_id';
                v_project UUID := (p->>'project_id')::uuid;
                seeded BIGINT;
            BEGIN
                PERFORM weft_lock_execution(v_execution_id);
                IF p->>'source_version' IS NOT NULL THEN
                    PERFORM 1 FROM project_version
                        WHERE project_id = v_project AND id = p->>'source_version' FOR KEY SHARE;
                    IF NOT FOUND THEN
                        RAISE EXCEPTION 'source version % was removed during preparation; run the command again',
                            p->>'source_version';
                    END IF;
                END IF;
                IF (p->>'journaled')::boolean THEN
                    PERFORM weft_journal_append(v_execution_id, ARRAY[p->>'kind'], ARRAY[p->>'payload'],
                        (p->>'created_at')::bigint, NULL, NULL, p->>'dedup_key');
                END IF;
                INSERT INTO execution (execution_id, project_id, tenant_id, started_at_unix, phase, kind, instance_id, fired_by)
                    SELECT v_execution_id, v_project, pr.tenant_id, (p->>'at_unix')::bigint, p->>'phase',
                           p->>'run_kind', p->>'instance_id', p->>'fired_by'
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

CREATE OR REPLACE FUNCTION weft_journal_append(p_execution_id text, p_kinds text[], p_payloads text[], p_created_at bigint, p_replica text, p_owner text, p_dedup_key text)
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
            DECLARE
                written BIGINT;
            BEGIN
                PERFORM weft_lock_execution(p_execution_id);
                INSERT INTO exec_event (execution_id, kind, payload_json, created_at, replica, dedup_key)
                    SELECT p_execution_id, e.kind, e.payload, p_created_at, p_replica, p_dedup_key
                    FROM unnest(p_kinds, p_payloads) WITH ORDINALITY AS e(kind, payload, n)
                    WHERE p_owner IS NULL
                       OR EXISTS (SELECT 1 FROM execution x
                                  WHERE x.execution_id = p_execution_id AND x.owner_replica = p_owner)
                    ORDER BY e.n
                    ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING;
                GET DIAGNOSTICS written = ROW_COUNT;
                RETURN written;
            END;
            $function$
;

CREATE OR REPLACE FUNCTION weft_lock_execution(p_execution_id text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
            BEGIN
                PERFORM pg_advisory_xact_lock(hashtextextended('exec_event:' || p_execution_id, 0));
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
                v_execution_id TEXT := v_started->>'execution_id';
                v_project UUID := (v_started->>'project_id')::uuid;
                v_journaled BOOLEAN := (v_started->>'journaled')::boolean;
                v_refused JSONB;
                v_inserted BOOLEAN;
            BEGIN
                PERFORM weft_lock_execution(v_execution_id);
                IF (v_journaled AND EXISTS (SELECT 1 FROM exec_event
                        WHERE execution_id = v_execution_id AND kind = 'execution_started'))
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
                PERFORM weft_execution_started(v_started);
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
                IF v_journaled AND jsonb_array_length(p->'kicks') > 0 THEN
                    PERFORM weft_journal_append(v_execution_id,
                        ARRAY(SELECT k->>'kind' FROM jsonb_array_elements(p->'kicks') WITH ORDINALITY AS e(k, n) ORDER BY n),
                        ARRAY(SELECT k->>'payload' FROM jsonb_array_elements(p->'kicks') WITH ORDINALITY AS e(k, n) ORDER BY n),
                        (v_started->>'created_at')::bigint, NULL, NULL, NULL);
                END IF;
                RETURN jsonb_build_object('outcome', 'started');
            END;
            $function$
;

CREATE TRIGGER signal_routes_on_change AFTER UPDATE OF surface_kind, mount_path, mount_methods, project_id, node_id, spec_json, auth_kind, auth_config, port_snapshot, program_json, source_version, instance_id, activation_trigger ON signal FOR EACH ROW WHEN (((new.surface_kind = 'public_entry'::text) OR (old.surface_kind = 'public_entry'::text))) EXECUTE FUNCTION routes_notify_tenant();

CREATE TRIGGER signal_routes_on_delete AFTER DELETE ON signal FOR EACH ROW WHEN ((old.surface_kind = 'public_entry'::text)) EXECUTE FUNCTION routes_notify_tenant();

CREATE TRIGGER signal_routes_on_insert AFTER INSERT ON signal FOR EACH ROW WHEN ((new.surface_kind = 'public_entry'::text)) EXECUTE FUNCTION routes_notify_tenant();
