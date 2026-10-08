-- Written by hand: what this changes lives INSIDE stored JSON and in rows
-- a column is added for, which no generated migration sees. It runs before
-- the generated migration of the same change, which renames
-- `payload_json` to `events_json` and drops `kind`.
--
-- A journal row now holds one write's events, in order, as a JSON array,
-- with each event's kind in `kinds`. Every row written before held one
-- event, so it becomes a row of one. A birth names its program by its
-- worker binary (`binary_hash`, the identity being stored once per binary
-- with the project) and says how its run is kept (`settings`), where it
-- carried the whole identity (`program`) and the run's length
-- (`run_class`, gone with long runs). A recorded run that has not ended was started by a
-- runtime that put every step on record before the next, so it keeps
-- being kept that way (`keeping: durable`) and is picked up where it
-- stopped rather than ended as a fast run. A run's ending is stamped on
-- its `execution` row (`ended_at_unix`, `outcome`). pg_temp functions
-- vanish with the session, so they leave nothing in the schema.

CREATE OR REPLACE FUNCTION pg_temp.weft_birth_now(e jsonb, ended boolean) RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $f$
    SELECT CASE WHEN e->>'kind' = 'execution_started' THEN
        (e - 'program' - 'run_class'
           - CASE WHEN e->>'run_kind' = 'unrecorded' THEN 'run_kind' ELSE '' END)
        || CASE WHEN jsonb_typeof(e->'program') = 'object' AND e->'program' ? 'binary_hash'
                THEN jsonb_build_object('binary_hash', e->'program'->'binary_hash')
                ELSE '{}'::jsonb END
        || CASE WHEN s.settings = '{}'::jsonb THEN '{}'::jsonb
                ELSE jsonb_build_object('settings', s.settings) END
    ELSE e END
    FROM (SELECT jsonb_strip_nulls(jsonb_build_object(
        'recorded', CASE WHEN e->>'run_kind' = 'unrecorded' THEN false END,
        'keeping', CASE WHEN NOT ended AND e->>'run_kind' IS DISTINCT FROM 'unrecorded' THEN 'durable' END
    )) AS settings) s
$f$;

-- Births through JSON (their shape changes); every other row as text, so
-- a value a JSONB column cannot hold (an escaped NUL) still comes along.
UPDATE exec_event e SET payload_json = jsonb_build_array(pg_temp.weft_birth_now(
        e.payload_json::jsonb,
        EXISTS (SELECT 1 FROM exec_event t WHERE t.execution_id = e.execution_id
                AND t.kind IN ('execution_completed', 'execution_failed', 'execution_cancelled'))
    ))::text
    WHERE e.kind = 'execution_started';
UPDATE exec_event SET payload_json = '[' || payload_json || ']'
    WHERE kind <> 'execution_started';

ALTER TABLE exec_event ADD COLUMN kinds TEXT[];
UPDATE exec_event SET kinds = ARRAY[kind];
ALTER TABLE exec_event ALTER COLUMN kinds SET NOT NULL;

ALTER TABLE execution ADD COLUMN outcome TEXT;
UPDATE execution ec
    SET ended_at_unix = COALESCE(ec.ended_at_unix, t.created_at),
        outcome = t.outcome
    FROM (
        SELECT DISTINCT ON (execution_id) execution_id, created_at,
            CASE kind WHEN 'execution_completed' THEN 'completed'
                      WHEN 'execution_failed' THEN 'failed'
                      ELSE 'cancelled' END AS outcome
        FROM exec_event
        WHERE kind IN ('execution_completed', 'execution_failed', 'execution_cancelled')
        ORDER BY execution_id, id
    ) t
    WHERE ec.execution_id = t.execution_id;
