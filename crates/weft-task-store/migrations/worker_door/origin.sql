CREATE TABLE IF NOT EXISTS worker_lease (
            replica TEXT PRIMARY KEY,
            project_id UUID NOT NULL,
            tenant_id TEXT NOT NULL,
            leased_until_unix BIGINT NOT NULL
        );
CREATE INDEX IF NOT EXISTS idx_worker_lease_project ON worker_lease(project_id, leased_until_unix);
CREATE UNLOGGED TABLE IF NOT EXISTS door_count (
            key TEXT NOT NULL,
            window_start BIGINT NOT NULL,
            replica TEXT NOT NULL,
            project_id UUID NOT NULL,
            hits BIGINT NOT NULL,
            PRIMARY KEY (key, window_start, replica)
        );
CREATE INDEX IF NOT EXISTS idx_door_count_window ON door_count(window_start);
CREATE OR REPLACE FUNCTION weft_door_tick(
                p_replica TEXT, p_project UUID, p_tenant TEXT, p_leased_until BIGINT,
                p_window BIGINT, p_keys TEXT[], p_hits BIGINT[], p_tokens TEXT[]
            ) RETURNS JSONB AS $$
            BEGIN
                INSERT INTO worker_lease (replica, project_id, tenant_id, leased_until_unix)
                    VALUES (p_replica, p_project, p_tenant, p_leased_until)
                    ON CONFLICT (replica) DO UPDATE SET leased_until_unix = EXCLUDED.leased_until_unix
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
                    'copies', (SELECT COUNT(*) FROM worker_lease
                               WHERE project_id = p_project
                                 AND leased_until_unix >= EXTRACT(EPOCH FROM now())::bigint));
            END;
            $$ LANGUAGE plpgsql;
