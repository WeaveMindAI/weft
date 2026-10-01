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

-- A started copy is named by its copy id (the old "instance" of a node).
UPDATE infra_event SET payload = pg_temp.weft_rekey(payload, 'instance_id', 'copy_id')
WHERE kind = 'started' AND payload ? 'instance_id';
