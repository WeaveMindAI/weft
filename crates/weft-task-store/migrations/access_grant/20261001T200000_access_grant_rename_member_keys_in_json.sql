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

-- A parked connect outcome holds the whole connection it made.
UPDATE access_connect_result
SET result_json = jsonb_set(result_json, '{grant}', pg_temp.weft_grant(result_json -> 'grant'))
WHERE jsonb_typeof(result_json -> 'grant') = 'object'
  AND (result_json -> 'grant' ? 'member' OR result_json -> 'grant' -> 'owner' ? 'member');
