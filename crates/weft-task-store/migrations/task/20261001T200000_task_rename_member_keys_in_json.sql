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

-- A queued cost on an instance's own credential.
UPDATE task
SET payload = jsonb_set(payload, '{origin}', pg_temp.weft_rekey(payload -> 'origin', 'member', 'instance'))
WHERE kind = 'record_cost' AND jsonb_typeof(payload -> 'origin') = 'object' AND payload -> 'origin' ? 'member';

-- A live arrival names the worker replica that holds the caller.
UPDATE task SET payload = pg_temp.weft_rekey(payload, 'instance', 'replica')
WHERE kind = 'live_arrival' AND payload ? 'instance';
UPDATE task SET result = pg_temp.weft_rekey(result, 'instance', 'replica')
WHERE kind = 'live_arrival' AND result ->> 'outcome' = 'born' AND result ? 'instance';

-- A program call: the instance it names (on the call, its scope, its
-- filter), the one call renamed, and the answer it parked.
UPDATE task
SET payload = jsonb_set(payload, '{call}',
        (SELECT CASE WHEN c ? 'filter'
                    THEN jsonb_set(c, '{filter}',
                         CASE WHEN c -> 'filter' ->> 'paid_by' = 'member'
                             THEN jsonb_set(pg_temp.weft_rekey(c -> 'filter', 'member', 'instance'), '{paid_by}', '"instance"')
                             ELSE pg_temp.weft_rekey(c -> 'filter', 'member', 'instance') END)
                    ELSE c END
         FROM (SELECT CASE WHEN r ? 'scope' THEN jsonb_set(r, '{scope}', pg_temp.weft_rekey(r -> 'scope', 'member', 'instance')) ELSE r END AS c
               FROM (SELECT CASE WHEN q ->> 'call' = 'members_list' THEN jsonb_set(q, '{call}', '"instances_list"') ELSE q END AS r
                     FROM (SELECT pg_temp.weft_rekey(payload -> 'call', 'member', 'instance') AS q) AS a) AS b) AS d))
WHERE kind = 'program_call' AND jsonb_typeof(payload -> 'call') = 'object'
  AND (payload -> 'call' ? 'member'
       OR payload -> 'call' ->> 'call' = 'members_list'
       OR payload -> 'call' -> 'scope' ? 'member'
       OR payload -> 'call' -> 'filter' ? 'member'
       OR payload -> 'call' -> 'filter' ->> 'paid_by' = 'member');

UPDATE task
SET result = jsonb_set(result, '{value}', pg_temp.weft_program_answer(
        CASE payload -> 'call' ->> 'call'
            WHEN 'infra_status' THEN 'weft.infra.status'
            WHEN 'infra_copies' THEN 'weft.infra.copies'
            -- the holdings shape, under the name the answer function reads
            WHEN 'instances_list' THEN 'weft.members.list'
            WHEN 'connections_list' THEN 'weft.connections.list'
            WHEN 'costs_list' THEN 'weft.costs.list'
        END,
        result -> 'value'))
WHERE kind = 'program_call' AND result ? 'value'
  AND payload -> 'call' ->> 'call' IN ('infra_status', 'infra_copies', 'instances_list', 'connections_list', 'costs_list')
  AND result::text LIKE '%"member"%';

-- A run started unrecorded carries its birth rows (journal rows as JSON)
-- in its task, for the worker to seed its in-memory journal with.
UPDATE task
SET payload = jsonb_set(payload, '{unrecorded_birth}',
        (SELECT jsonb_agg(pg_temp.weft_journal_row(r) ORDER BY i)
         FROM jsonb_array_elements(payload -> 'unrecorded_birth') WITH ORDINALITY AS t(r, i)))
WHERE kind IN ('execute', 'resume') AND jsonb_typeof(payload -> 'unrecorded_birth') = 'array'
  AND jsonb_array_length(payload -> 'unrecorded_birth') > 0
  AND (payload -> 'unrecorded_birth')::text ~ '"member(_values)?"';
