CREATE TABLE IF NOT EXISTS infra_lifecycle_command (
            id                BIGSERIAL PRIMARY KEY,
            tenant_id         TEXT NOT NULL,
            project_id        UUID NOT NULL,
            node_id           TEXT,
            verb              TEXT NOT NULL,
            -- Nullable because dispatcher-owned verbs
            -- (deactivate / reactivate) carry their running_policy
            -- inside spec_json (Deactivate) or have none at all
            -- (Reactivate). Stop / Terminate populate this; Apply
            -- ignores it. One source of truth per verb.
            running_policy    TEXT,
            spec_json         JSONB,
            issued_by_instance     TEXT NOT NULL,
            issued_at_unix    BIGINT NOT NULL,
            claimed_by_instance    TEXT,
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
            -- this between calls to the platform and halts (leaving
            -- per-node partial state visible; no platform applies a
            -- whole project transactionally).
            -- Pending unclaimed rows are cancelled outright (outcome =
            -- 'cancelled') instead of flagged.
            cancel_requested  BOOLEAN NOT NULL DEFAULT FALSE,
            -- Cap on the running_policy=wait drain before the op
            -- proceeds anyway (loud warning). Per-command: the user
            -- picks it with the wait choice; the default mirrors
            -- weft_broker_client::protocol::DEFAULT_DRAIN_TIMEOUT_SECS
            -- (SYNC: the two numbers move together, by migration here).
            drain_timeout_secs BIGINT NOT NULL DEFAULT 60,
            -- Which copies of the infra the command acts on
            -- (`weft_core::member::Copies`): the shared ones (member_id
            -- NULL, every_copy FALSE), one member's (member_id set), or
            -- every copy there is (every_copy TRUE; the project going).
            -- An apply always names exactly one copy.
            member_id         TEXT,
            every_copy        BOOLEAN NOT NULL DEFAULT FALSE
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
                AND claimed_by_instance IS NULL
                AND verb IN ('deactivate', 'reactivate', 'upgrade');
CREATE UNIQUE INDEX IF NOT EXISTS uq_lifecycle_cmd_pending_apply
              ON infra_lifecycle_command(project_id, node_id, member_id) NULLS NOT DISTINCT
              WHERE completed_at_unix IS NULL AND verb = 'apply';
CREATE OR REPLACE FUNCTION infra_command_notify() RETURNS trigger AS $$
            BEGIN
                IF TG_OP = 'INSERT' THEN
                    PERFORM pg_notify('weft_infra_command', 'issued:' || NEW.project_id::text);
                ELSE
                    PERFORM pg_notify('weft_infra_command', 'done:' || NEW.id::text);
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS infra_command_notify_on_issue ON infra_lifecycle_command;
CREATE TRIGGER infra_command_notify_on_issue
            AFTER INSERT ON infra_lifecycle_command
            FOR EACH ROW
            EXECUTE FUNCTION infra_command_notify();
DROP TRIGGER IF EXISTS infra_command_notify_on_done ON infra_lifecycle_command;
CREATE TRIGGER infra_command_notify_on_done
            AFTER UPDATE OF completed_at_unix ON infra_lifecycle_command
            FOR EACH ROW
            WHEN (NEW.completed_at_unix IS NOT NULL AND OLD.completed_at_unix IS NULL)
            EXECUTE FUNCTION infra_command_notify();
