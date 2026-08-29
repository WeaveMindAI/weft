CREATE TABLE IF NOT EXISTS listener_pod (
            pod_name          TEXT PRIMARY KEY,
            admin_url         TEXT NOT NULL,
            namespace         TEXT NOT NULL,
            owner_pod_id      TEXT NOT NULL,
            leased_until_unix BIGINT NOT NULL,
            grace_until_unix  BIGINT NOT NULL
        );
