ALTER TABLE worker_lease ADD COLUMN binary_hash text;

ALTER TABLE worker_lease ADD COLUMN in_flight jsonb DEFAULT '{}'::jsonb NOT NULL;

ALTER TABLE worker_lease ADD CONSTRAINT worker_lease_in_flight_not_null NOT NULL in_flight;

CREATE OR REPLACE FUNCTION weft_door_tick(p_replica text, p_project uuid, p_tenant text, p_binary_hash text, p_leased_until bigint, p_in_flight jsonb, p_window bigint, p_keys text[], p_hits bigint[], p_tokens text[])
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
            BEGIN
                INSERT INTO worker_lease (replica, project_id, tenant_id, binary_hash, leased_until_unix, in_flight)
                    VALUES (p_replica, p_project, p_tenant, p_binary_hash, p_leased_until, p_in_flight)
                    ON CONFLICT (replica) DO UPDATE
                        SET leased_until_unix = EXCLUDED.leased_until_unix, in_flight = EXCLUDED.in_flight
                    WHERE worker_lease.project_id = EXCLUDED.project_id;
                INSERT INTO door_count (key, window_start, replica, project_id, hits)
                    SELECT k, p_window, p_replica, p_project, h FROM unnest(p_keys, p_hits) AS t(k, h)
                    ON CONFLICT (key, window_start, replica) DO UPDATE SET hits = EXCLUDED.hits
                    WHERE door_count.project_id = EXCLUDED.project_id;
                RETURN jsonb_build_object(
                    'others', COALESCE((
                        SELECT jsonb_agg(jsonb_build_object('key', o.key, 'hits', o.hits))
                        FROM (SELECT key, SUM(hits)::bigint AS hits FROM door_count
                              WHERE window_start = p_window AND replica <> p_replica
                                AND project_id = p_project
                                AND split_part(key, ':', 2) = ANY(p_tokens)
                              GROUP BY key) o), '[]'::jsonb),
                    'copies', (SELECT COUNT(*) FROM worker_lease l
                               WHERE l.project_id = p_project
                                 AND l.leased_until_unix >= EXTRACT(EPOCH FROM NOW())::BIGINT));
            END;
            $function$
;

DROP FUNCTION weft_door_tick(p_replica text, p_project uuid, p_tenant text, p_leased_until bigint, p_window bigint, p_keys text[], p_hits bigint[], p_tokens text[]) CASCADE;
