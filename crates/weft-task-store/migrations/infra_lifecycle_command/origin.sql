CREATE TABLE IF NOT EXISTS infra_lifecycle_command (
            id                BIGSERIAL PRIMARY KEY,
            tenant_id         TEXT NOT NULL,
            project_id        TEXT NOT NULL,
            node_id           TEXT,
            verb              TEXT NOT NULL,
            -- Nullable because dispatcher-owned verbs
            -- (deactivate / reactivate) carry their running_policy
            -- inside spec_json (Deactivate) or have none at all
            -- (Reactivate). Stop / Terminate populate this; Apply
            -- ignores it. One source of truth per verb.
            running_policy    TEXT,
            spec_json         JSONB,
            issued_by_pod     TEXT NOT NULL,
            issued_at_unix    BIGINT NOT NULL,
            claimed_by_pod    TEXT,
            claimed_at_unix   BIGINT,
            completed_at_unix BIGINT,
            -- 'succeeded' | 'failed' | 'cancelled' | NULL.
            -- NULL = "no result yet" (pending or claimed).
            -- 'failed' = the claimer hit a real error executing the
            --   verb; the worker / caller treats this as a failure.
            -- 'cancelled' = the command was abandoned (e.g. the
            --   targeted node was removed before execution). NOT a
            --   failure; surfaces as "no longer applicable".
            outcome           TEXT,
            -- Human-readable message accompanying the outcome.
            -- NULL on 'succeeded' and on still-pending rows. The
            -- error message on 'failed'; the reason on 'cancelled'.
            -- Decoded into the right typed field based on `outcome`.
            outcome_message   TEXT,
            -- Stop only: force scale-to-zero EVERY unit, ignoring each
            -- unit's `on_stop` (so a NoOp unit comes down too). The
            -- explicit "I accept the downtime, take it all down so I
            -- can update it" override. Default false.
            force             BOOLEAN NOT NULL DEFAULT FALSE,
            -- The user requested cancellation of this command while it
            -- was CLAIMED (in flight). The executing supervisor polls
            -- this between kubectl steps and halts (leaving per-node
            -- partial state visible; kubectl is not transactional).
            -- Pending unclaimed rows are cancelled outright (outcome =
            -- 'cancelled') instead of flagged.
            cancel_requested  BOOLEAN NOT NULL DEFAULT FALSE,
            -- Cap on the running_policy=wait drain before the op
            -- proceeds anyway (loud warning). Per-command: the user
            -- picks it with the wait choice; the default mirrors
            -- weft_broker_client::protocol::DEFAULT_DRAIN_TIMEOUT_SECS.
            drain_timeout_secs BIGINT NOT NULL DEFAULT 600
        );
CREATE INDEX IF NOT EXISTS idx_lifecycle_cmd_pending
              ON infra_lifecycle_command(tenant_id)
              WHERE completed_at_unix IS NULL;
CREATE INDEX IF NOT EXISTS idx_lifecycle_cmd_supervisor_claim
              ON infra_lifecycle_command(id)
              WHERE completed_at_unix IS NULL
                AND verb IN ('apply', 'stop', 'terminate');
CREATE INDEX IF NOT EXISTS idx_lifecycle_cmd_dispatcher_claim
              ON infra_lifecycle_command(id)
              WHERE completed_at_unix IS NULL
                AND claimed_by_pod IS NULL
                AND verb IN ('deactivate', 'reactivate');
CREATE UNIQUE INDEX IF NOT EXISTS uq_lifecycle_cmd_pending_apply
              ON infra_lifecycle_command(project_id, node_id)
              WHERE completed_at_unix IS NULL AND verb = 'apply';
