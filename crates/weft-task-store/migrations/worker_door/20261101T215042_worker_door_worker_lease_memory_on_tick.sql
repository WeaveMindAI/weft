-- weft_door_tick takes other arguments now: a call with the old ones fails loudly (no such function).
DROP FUNCTION weft_door_tick(text, uuid, text, text, bigint, jsonb, bigint, text[], bigint[], text[], boolean);
CREATE OR REPLACE FUNCTION weft_door_tick(p_replica text, p_project uuid, p_tenant text, p_binary_hash text, p_leased_until bigint, p_in_flight jsonb, p_window bigint, p_keys text[], p_hits bigint[], p_reasons text[], p_shortens boolean, p_memory_full_at bigint, p_memory_used bigint, p_memory_limit bigint)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
            BEGIN
                INSERT INTO worker_lease (replica, project_id, tenant_id, binary_hash, leased_until_unix, in_flight, alive_since,
                                          memory_full_at, memory_used, memory_limit)
                    VALUES (p_replica, p_project, p_tenant, p_binary_hash, p_leased_until, p_in_flight,
                            EXTRACT(EPOCH FROM NOW())::BIGINT, p_memory_full_at, p_memory_used, p_memory_limit)
                    ON CONFLICT (replica) DO UPDATE
                        SET leased_until_unix = EXCLUDED.leased_until_unix, in_flight = EXCLUDED.in_flight,
                            memory_full_at = EXCLUDED.memory_full_at, memory_used = EXCLUDED.memory_used,
                            memory_limit = EXCLUDED.memory_limit,
                            alive_since = CASE WHEN worker_lease.leased_until_unix >= EXTRACT(EPOCH FROM NOW())::BIGINT
                                               THEN worker_lease.alive_since ELSE EXCLUDED.alive_since END
                    WHERE worker_lease.project_id = EXCLUDED.project_id;
                INSERT INTO door_count (key, window_start, replica, project_id, hits, reason)
                    SELECT k, p_window, p_replica, p_project, h, r FROM unnest(p_keys, p_hits, p_reasons) AS t(k, h, r)
                    ON CONFLICT (key, window_start, replica) DO UPDATE SET hits = EXCLUDED.hits, reason = EXCLUDED.reason
                    WHERE door_count.project_id = EXCLUDED.project_id;
                -- This tick's lease ends before the one the worker vouched
                -- for last: the look at lapsed leases, booked for the
                -- longer one, is rung again.
                -- SYNC: 'weft_lease_alive' <-> LEASE_ALIVE_CHANNEL
                IF p_shortens THEN
                    PERFORM pg_notify('weft_lease_alive', p_replica);
                END IF;
                RETURN jsonb_build_object(
                    'others', COALESCE((
                        SELECT jsonb_agg(jsonb_build_object('key', o.key, 'hits', o.hits))
                        FROM (SELECT key, SUM(hits)::bigint AS hits FROM door_count
                              WHERE window_start = p_window AND replica <> p_replica
                                AND project_id = p_project
                                -- SYNC: '-' <-> crates/weft-engine/src/door/limits.rs (invalid_tokens_key)
                                AND split_part(key, ':', 2) <> '-' 
                              GROUP BY key) o), '[]'::jsonb),
                    'copies', (SELECT COUNT(*) FROM worker_lease l
                               WHERE l.project_id = p_project
                                 AND l.leased_until_unix >= EXTRACT(EPOCH FROM NOW())::BIGINT));
            END;
            $function$
;
