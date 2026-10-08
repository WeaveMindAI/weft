-- Written by hand: what this changes lives INSIDE stored JSON, which no
-- generated migration sees. An unrecorded run's execute task carries its
-- birth rows (`payload.unrecorded_birth`); a birth now names its program
-- by its worker binary (`binary_hash`) and says how its run is kept
-- (`settings`, an unrecorded run with `recorded: false`), where it carried
-- the whole identity (`program`), the run's length (`run_class`, gone
-- with long runs) and an `unrecorded` run kind. An execute or resume task
-- drops that length too, and says how its run is kept (`keeping`): one waiting or claimed now was made by a runtime
-- that put every step on record, so its run is durable and a lapsed claim
-- of it is picked up again (an unrecorded run's never is). pg_temp
-- functions vanish with the session, so they leave nothing in the schema.

CREATE OR REPLACE FUNCTION pg_temp.weft_birth_now(e jsonb) RETURNS jsonb
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
        'recorded', CASE WHEN e->>'run_kind' = 'unrecorded' THEN false END
    )) AS settings) s
$f$;

UPDATE task
    SET payload = jsonb_set(payload, '{unrecorded_birth}', (
        SELECT COALESCE(jsonb_agg(pg_temp.weft_birth_now(e) ORDER BY i), '[]'::jsonb)
        FROM jsonb_array_elements(payload->'unrecorded_birth') WITH ORDINALITY AS t(e, i)
    ))
    WHERE jsonb_typeof(payload->'unrecorded_birth') = 'array';

UPDATE task SET payload = payload - 'run_class' WHERE payload ? 'run_class';

UPDATE task
    SET payload = payload || jsonb_build_object('keeping',
        CASE WHEN jsonb_typeof(payload->'unrecorded_birth') = 'array' THEN 'fast' ELSE 'durable' END)
    WHERE kind IN ('execute', 'resume') AND status IN ('pending', 'claimed');
