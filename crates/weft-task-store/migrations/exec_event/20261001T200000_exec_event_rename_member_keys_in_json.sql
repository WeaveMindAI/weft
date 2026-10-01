-- Written by hand (the rename of member to instance moved keys INSIDE stored
-- JSON, which no generated migration sees). pg_temp functions vanish with
-- the session, so they leave nothing in the schema.
CREATE OR REPLACE FUNCTION pg_temp.weft_rekey(j jsonb, old text, new text) RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $f$
    SELECT CASE WHEN jsonb_typeof(j) = 'object' AND j ? old
        THEN (j - old) || jsonb_build_object(new, j -> old) ELSE j END
$f$;

CREATE OR REPLACE FUNCTION pg_temp.weft_rekey_each(a jsonb, old text, new text) RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $f$
    SELECT CASE WHEN jsonb_typeof(a) = 'array'
        THEN COALESCE((SELECT jsonb_agg(pg_temp.weft_rekey(e, old, new) ORDER BY i)
                       FROM jsonb_array_elements(a) WITH ORDINALITY AS t(e, i)), '[]'::jsonb)
        ELSE a END
$f$;

-- A connection as `GrantSummary` serializes it: the instance it belongs
-- to, and its owner (a credential owner, {"member": id} before).
CREATE OR REPLACE FUNCTION pg_temp.weft_grant(g jsonb) RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $f$
    SELECT CASE WHEN jsonb_typeof(r -> 'owner') = 'object'
        THEN jsonb_set(r, '{owner}', pg_temp.weft_rekey(r -> 'owner', 'member', 'instance')) ELSE r END
    FROM (SELECT pg_temp.weft_rekey(g, 'member', 'instance') AS r) AS t
$f$;

-- A program call's answer, by the call's journal name: the copy (or
-- copies) an infra call answers, the holdings of `weft.members.list` (now
-- `weft.instances.list`), the connections of `weft.connections.list`, and
-- the cost records with their credential owner.
CREATE OR REPLACE FUNCTION pg_temp.weft_program_answer(name text, v jsonb) RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $f$
    SELECT CASE
        WHEN name IN ('weft.infra.status', 'weft.infra.copies') THEN
            CASE WHEN jsonb_typeof(v) = 'array' THEN pg_temp.weft_rekey_each(v, 'member', 'instance')
                 ELSE pg_temp.weft_rekey(v, 'member', 'instance') END
        WHEN name = 'weft.members.list' AND jsonb_typeof(v) = 'array' THEN
            COALESCE((SELECT jsonb_agg(
                         CASE WHEN h ? 'copies'
                             THEN jsonb_set(pg_temp.weft_rekey(h, 'member', 'instance'), '{copies}',
                                            pg_temp.weft_rekey_each(h -> 'copies', 'member', 'instance'))
                             ELSE pg_temp.weft_rekey(h, 'member', 'instance') END ORDER BY i)
                      FROM jsonb_array_elements(v) WITH ORDINALITY AS t(h, i)), '[]'::jsonb)
        WHEN name = 'weft.connections.list' AND jsonb_typeof(v) = 'array' THEN
            COALESCE((SELECT jsonb_agg(pg_temp.weft_grant(g) ORDER BY i)
                      FROM jsonb_array_elements(v) WITH ORDINALITY AS t(g, i)), '[]'::jsonb)
        WHEN name = 'weft.costs.list' AND jsonb_typeof(v) = 'array' THEN
            COALESCE((SELECT jsonb_agg(
                         CASE WHEN c ? 'paid_by'
                             THEN jsonb_set(pg_temp.weft_rekey(c, 'member', 'instance'), '{paid_by}',
                                            pg_temp.weft_rekey(c -> 'paid_by', 'member', 'instance'))
                             ELSE pg_temp.weft_rekey(c, 'member', 'instance') END ORDER BY i)
                      FROM jsonb_array_elements(v) WITH ORDINALITY AS t(c, i)), '[]'::jsonb)
        ELSE v END
$f$;

-- One journal row (an `ExecEvent`): a birth's instance and its values, a
-- cost's credential owner, and a journaled program call's answer with the
-- one journal name that moved. Other rows come back as they were.
CREATE OR REPLACE FUNCTION pg_temp.weft_journal_row(r jsonb) RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $f$
    SELECT CASE r ->> 'kind'
        WHEN 'execution_started' THEN
            pg_temp.weft_rekey(pg_temp.weft_rekey(r, 'member', 'instance'), 'member_values', 'instance_values')
        WHEN 'cost_reported' THEN
            CASE WHEN jsonb_typeof(r -> 'origin') = 'object'
                THEN jsonb_set(r, '{origin}', pg_temp.weft_rekey(r -> 'origin', 'member', 'instance')) ELSE r END
        WHEN 'run_output' THEN
            jsonb_set(jsonb_set(r, '{value}', pg_temp.weft_program_answer(r ->> 'name', r -> 'value')),
                      '{name}',
                      to_jsonb(CASE WHEN r ->> 'name' = 'weft.members.list' THEN 'weft.instances.list' ELSE r ->> 'name' END))
        ELSE r END
$f$;

-- Every journal row the old build wrote with a renamed key: a birth, a
-- cost on an instance's credential, a program call's journaled answer.
UPDATE exec_event
SET payload_json = pg_temp.weft_journal_row(payload_json::jsonb)::text
WHERE kind IN ('execution_started', 'cost_reported', 'run_output')
  AND (payload_json LIKE '%"member"%' OR payload_json LIKE '%"member_values"%'
       OR payload_json::jsonb ->> 'name' = 'weft.members.list');

UPDATE trigger_bake
SET bake_json = pg_temp.weft_rekey(bake_json::jsonb, 'member', 'instance')::text
WHERE bake_json::jsonb ? 'member';

-- A parked fire waiting on a value its instance had not given.
UPDATE signal
SET parked_fires = pg_temp.weft_rekey_each(parked_fires, 'member_gap', 'instance_gap')
WHERE jsonb_path_exists(parked_fires, '$[*].member_gap');
