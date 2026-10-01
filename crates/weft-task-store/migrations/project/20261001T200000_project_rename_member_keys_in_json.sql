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

-- One node of a stored project definition: its per-instance mark, the
-- service and the rules each instance fills, and the live wire its
-- trigger answers on (once a bare `true`, now the wire by name: a Route
-- answers over http, a Socket over a websocket).
CREATE OR REPLACE FUNCTION pg_temp.weft_project_node(n jsonb) RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $f$
    SELECT CASE
        WHEN m -> 'features' -> 'liveConnection' = 'true'::jsonb AND m ->> 'nodeType' IN ('Route', 'Socket')
            THEN jsonb_set(m, '{features,liveConnection}',
                           to_jsonb(CASE m ->> 'nodeType' WHEN 'Route' THEN 'http' ELSE 'websocket' END))
        ELSE m END
    FROM (SELECT pg_temp.weft_rekey(pg_temp.weft_rekey(pg_temp.weft_rekey(n,
              'perMember', 'perInstance'), 'memberService', 'instanceService'), 'memberRules', 'instanceRules') AS m) AS t
$f$;

-- The whole definition: every node, then the marker a literal filled by
-- each instance carries (a key only weft writes, so a text swap is exact).
CREATE OR REPLACE FUNCTION pg_temp.weft_project(doc text) RETURNS text
LANGUAGE sql IMMUTABLE AS $f$
    SELECT replace(
        CASE WHEN jsonb_typeof(d -> 'nodes') = 'array'
            THEN jsonb_set(d, '{nodes}', COALESCE((SELECT jsonb_agg(pg_temp.weft_project_node(n) ORDER BY i)
                                                   FROM jsonb_array_elements(d -> 'nodes') WITH ORDINALITY AS t(n, i)), '[]'::jsonb))
            ELSE d END::text,
        '"__weft_member_filled__"', '"__weft_instance_filled__"')
    FROM (SELECT doc::jsonb AS d) AS s
$f$;

UPDATE project SET project_json = pg_temp.weft_project(project_json)
WHERE project_json ~ '"(perMember|memberService|memberRules|__weft_member_filled__)"|"liveConnection": ?true';

UPDATE project_definition SET project_json = pg_temp.weft_project(project_json)
WHERE project_json ~ '"(perMember|memberService|memberRules|__weft_member_filled__)"|"liveConnection": ?true';
