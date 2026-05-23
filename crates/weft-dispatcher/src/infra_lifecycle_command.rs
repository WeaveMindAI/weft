//! `infra_lifecycle_command` table.
//!
//! The single bidirectional channel between the dispatcher (router)
//! and a tenant's infra-supervisor (executor). Dispatcher writes
//! intent rows for the three verbs we expose:
//!
//! - **Apply**: provision a new infra node OR roll an existing one
//!   to a new spec. Body carries only the `InfraSpec` JSON. The
//!   supervisor reads the prior `infra_node` row itself, compiles
//!   the new spec with the real image-tag map + instance id, hashes,
//!   and decides skip / fresh / replace internally.
//! - **Stop**: scale to zero, preserve PVCs.
//! - **Terminate**: delete every resource by label, PVCs too.
//!
//! Supervisor polls per tenant via the broker's
//! `supervisor_claim_command`, runs kubectl, writes completion via
//! `supervisor_command_complete`. Dispatcher-claimable verbs
//! (Deactivate, Reactivate) go through `lifecycle_claimer` (this
//! crate). No HTTP between supervisor and dispatcher; both sides
//! talk to Postgres via the broker.

use anyhow::Result;
use serde_json::Value;
use sqlx::postgres::PgPool;

// `InfraLifecycleVerb` and `RunningPolicy` are the wire contract for
// the `verb` and `running_policy` columns. They live in
// `weft-broker-client::protocol` so the supervisor + dispatcher +
// broker share one source of truth. Re-export here so the rest of
// the dispatcher keeps the short module-relative path.
pub use weft_broker_client::protocol::{InfraLifecycleVerb, RunningPolicy};

pub async fn migrate(pool: &PgPool) -> Result<()> {
    let stmts = [
        r#"CREATE TABLE IF NOT EXISTS infra_lifecycle_command (
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
            force             BOOLEAN NOT NULL DEFAULT FALSE
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_lifecycle_cmd_pending
              ON infra_lifecycle_command(tenant_id)
              WHERE completed_at_unix IS NULL"#,
        // Tenant-scoped claim path for supervisor-owned verbs
        // (apply / stop / terminate). The supervisor `claim_command`
        // SELECT filters on this exact predicate set; a partial
        // index keyed on (tenant_id, id) skips the dispatcher-owned
        // rows the supervisor must not steal.
        r#"CREATE INDEX IF NOT EXISTS idx_lifecycle_cmd_supervisor_claim
              ON infra_lifecycle_command(tenant_id, id)
              WHERE completed_at_unix IS NULL
                AND claimed_by_pod IS NULL
                AND verb IN ('apply', 'stop', 'terminate')"#,
        // Mirror for the dispatcher claim loop (deactivate /
        // reactivate). No tenant filter: the dispatcher pool claims
        // across all tenants.
        r#"CREATE INDEX IF NOT EXISTS idx_lifecycle_cmd_dispatcher_claim
              ON infra_lifecycle_command(id)
              WHERE completed_at_unix IS NULL
                AND claimed_by_pod IS NULL
                AND verb IN ('deactivate', 'reactivate')"#,
        // Partial unique index: at most one pending apply for a
        // given (project_id, node_id). Stops a worker restart from
        // double-enqueueing the same apply; `infra_enqueue_apply`
        // catches the conflict and returns the existing row's id.
        r#"CREATE UNIQUE INDEX IF NOT EXISTS uq_lifecycle_cmd_pending_apply
              ON infra_lifecycle_command(project_id, node_id)
              WHERE completed_at_unix IS NULL AND verb = 'apply'"#,
    ];
    for sql in stmts {
        sqlx::query(sql).execute(pool).await?;
    }
    Ok(())
}

/// Enqueue a Stop or Terminate command. Returns its id; the
/// supervisor polling for the tenant claims it on its next tick.
pub async fn issue_lifecycle(
    pool: &PgPool,
    tenant_id: &str,
    project_id: &str,
    node_id: Option<&str>,
    verb: InfraLifecycleVerb,
    running_policy: RunningPolicy,
    force: bool,
    issued_by_pod: &str,
) -> Result<i64> {
    assert!(
        matches!(verb, InfraLifecycleVerb::Stop | InfraLifecycleVerb::Terminate),
        "issue_lifecycle is for Stop/Terminate; use issue_apply for Apply"
    );
    let row: (i64,) = sqlx::query_as(
        "INSERT INTO infra_lifecycle_command \
         (tenant_id, project_id, node_id, verb, running_policy, force, issued_by_pod, issued_at_unix) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, EXTRACT(EPOCH FROM NOW())::BIGINT) \
         RETURNING id",
    )
    .bind(tenant_id)
    .bind(project_id)
    .bind(node_id)
    .bind(verb.as_str())
    .bind(running_policy.as_str())
    .bind(force)
    .bind(issued_by_pod)
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}

/// Enqueue an Apply command. The supervisor reads the row,
/// deserializes `spec_json`, reads the prior `infra_node` row to
/// decide skip / fresh / replace, and applies via kubectl.
pub async fn issue_apply(
    pool: &PgPool,
    tenant_id: &str,
    project_id: &str,
    node_id: &str,
    spec: &Value,
    issued_by_pod: &str,
) -> Result<i64> {
    // Apply ignores `running_policy` (there's nothing to drain
    // before applying); leave it NULL. `SupervisorCommandRow.running_policy`
    // is `Option<RunningPolicy>` so this round-trips cleanly.
    let row: (i64,) = sqlx::query_as(
        "INSERT INTO infra_lifecycle_command \
         (tenant_id, project_id, node_id, verb, running_policy, \
          spec_json, issued_by_pod, issued_at_unix) \
         VALUES ($1, $2, $3, 'apply', NULL, $4, $5, \
                 EXTRACT(EPOCH FROM NOW())::BIGINT) \
         RETURNING id",
    )
    .bind(tenant_id)
    .bind(project_id)
    .bind(node_id)
    .bind(spec)
    .bind(issued_by_pod)
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}

// Note on missing `claim_one` / `complete`: claim/complete is owned
// by the broker (`supervisor_claim_command` + `supervisor_command_complete`)
// for supervisor verbs, and by the dispatcher's `lifecycle_claimer`
// for Deactivate/Reactivate. There is no shared helper because the
// two claim paths enforce different ownership invariants (broker
// requires SA-authenticated supervisor pod; dispatcher claims by
// pod_id with a lease).

/// Wait for a previously-issued command to reach a terminal state.
/// Returns a typed outcome that distinguishes "the claimer hit a
/// real error" from "the command was cancelled" (e.g. the targeted
/// node was removed). Callers branch differently on the two cases:
/// a `Failed` is a user-visible problem, a `Cancelled` is "no
/// longer applicable" and shouldn't show up as a failure.
///
/// `Timeout` fires when the supervisor never marks the row
/// complete within the deadline (typical when no supervisor pod is
/// alive in the tenant namespace; the caller decides whether to
/// proceed with cleanup anyway).
#[derive(Debug, Clone)]
pub enum WaitOutcome {
    Succeeded,
    Failed { error: String },
    Cancelled { reason: String },
    Timeout,
}

pub async fn wait_for_command(
    pool: &PgPool,
    command_id: i64,
    timeout: std::time::Duration,
) -> Result<WaitOutcome> {
    use weft_broker_client::protocol::LifecycleOutcome;
    let deadline = std::time::Instant::now() + timeout;
    loop {
        use sqlx::Row;
        let row = sqlx::query(
            "SELECT completed_at_unix, outcome, outcome_message \
             FROM infra_lifecycle_command WHERE id = $1",
        )
        .bind(command_id)
        .fetch_optional(pool)
        .await?;
        if let Some(r) = row {
            let done: Option<i64> = r.try_get("completed_at_unix")?;
            if done.is_some() {
                let outcome_str: Option<String> = r.try_get("outcome")?;
                let message: Option<String> = r.try_get("outcome_message")?;
                let outcome = outcome_str
                    .as_deref()
                    .and_then(LifecycleOutcome::parse)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "infra_lifecycle_command.id={command_id} has no/unknown outcome \
                             '{outcome_str:?}' but completed_at_unix is set"
                        )
                    })?;
                return Ok(match outcome {
                    LifecycleOutcome::Succeeded => WaitOutcome::Succeeded,
                    LifecycleOutcome::Failed => WaitOutcome::Failed {
                        error: message.unwrap_or_else(|| "unspecified error".into()),
                    },
                    LifecycleOutcome::Cancelled => WaitOutcome::Cancelled {
                        reason: message.unwrap_or_else(|| "cancelled".into()),
                    },
                });
            }
        }
        if std::time::Instant::now() >= deadline {
            return Ok(WaitOutcome::Timeout);
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

/// Non-blocking single read of a command's terminal state. `None`
/// means still pending (or the row is gone, treated as pending). The
/// HTTP command-status endpoint uses this so clients can poll a stop /
/// terminate to completion without the CLI guessing at rollup state
/// (a NoOp unit staying up means the rollup never reaches "stopped",
/// so the command outcome is the only honest "is it done" signal).
pub async fn read_command_outcome(
    pool: &PgPool,
    command_id: i64,
) -> Result<Option<WaitOutcome>> {
    use sqlx::Row;
    use weft_broker_client::protocol::LifecycleOutcome;
    let row = sqlx::query(
        "SELECT completed_at_unix, outcome, outcome_message \
         FROM infra_lifecycle_command WHERE id = $1",
    )
    .bind(command_id)
    .fetch_optional(pool)
    .await?;
    let Some(r) = row else {
        return Ok(None);
    };
    let done: Option<i64> = r.try_get("completed_at_unix")?;
    if done.is_none() {
        return Ok(None);
    }
    let outcome_str: Option<String> = r.try_get("outcome")?;
    let message: Option<String> = r.try_get("outcome_message")?;
    let outcome = outcome_str.as_deref().and_then(LifecycleOutcome::parse).ok_or_else(|| {
        anyhow::anyhow!(
            "infra_lifecycle_command.id={command_id} completed with no/unknown outcome '{outcome_str:?}'"
        )
    })?;
    Ok(Some(match outcome {
        LifecycleOutcome::Succeeded => WaitOutcome::Succeeded,
        LifecycleOutcome::Failed => WaitOutcome::Failed {
            error: message.unwrap_or_else(|| "unspecified error".into()),
        },
        LifecycleOutcome::Cancelled => WaitOutcome::Cancelled {
            reason: message.unwrap_or_else(|| "cancelled".into()),
        },
    }))
}

/// Wait for ALL the given commands to reach a terminal state, or
/// the deadline expires. One poll per cycle reads every row via
/// `WHERE id = ANY($1)`, collapsing N concurrent `wait_for_command`
/// calls into one. Returns `(id, outcome)` pairs in stable order.
///
/// Used by `reap_orphans` and `delete_project::terminate`, both of
/// which fan out N terminate commands and need to wait for the
/// set. The non-batched `wait_for_command` is still appropriate
/// for single-command waits (e.g. the worker's
/// `wait_apply` after `enqueue_apply`).
pub async fn wait_for_commands(
    pool: &PgPool,
    command_ids: &[i64],
    timeout: std::time::Duration,
) -> Result<Vec<(i64, WaitOutcome)>> {
    use weft_broker_client::protocol::LifecycleOutcome;
    if command_ids.is_empty() {
        return Ok(Vec::new());
    }
    let deadline = std::time::Instant::now() + timeout;
    let mut done: std::collections::HashMap<i64, WaitOutcome> =
        std::collections::HashMap::with_capacity(command_ids.len());
    loop {
        use sqlx::Row;
        let rows = sqlx::query(
            "SELECT id, completed_at_unix, outcome, outcome_message \
             FROM infra_lifecycle_command WHERE id = ANY($1)",
        )
        .bind(command_ids)
        .fetch_all(pool)
        .await?;
        for r in rows {
            let id: i64 = r.try_get("id")?;
            if done.contains_key(&id) {
                continue;
            }
            let completed: Option<i64> = r.try_get("completed_at_unix")?;
            if completed.is_none() {
                continue;
            }
            let outcome_str: Option<String> = r.try_get("outcome")?;
            let message: Option<String> = r.try_get("outcome_message")?;
            let outcome = outcome_str
                .as_deref()
                .and_then(LifecycleOutcome::parse)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "infra_lifecycle_command.id={id} has no/unknown outcome \
                         '{outcome_str:?}' but completed_at_unix is set"
                    )
                })?;
            done.insert(
                id,
                match outcome {
                    LifecycleOutcome::Succeeded => WaitOutcome::Succeeded,
                    LifecycleOutcome::Failed => WaitOutcome::Failed {
                        error: message.unwrap_or_else(|| "unspecified error".into()),
                    },
                    LifecycleOutcome::Cancelled => WaitOutcome::Cancelled {
                        reason: message.unwrap_or_else(|| "cancelled".into()),
                    },
                },
            );
        }
        if done.len() == command_ids.len() {
            break;
        }
        if std::time::Instant::now() >= deadline {
            // Fill the remaining with Timeout outcomes so the
            // caller sees one entry per requested id.
            for id in command_ids {
                done.entry(*id).or_insert(WaitOutcome::Timeout);
            }
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    // Return in input order. Contract: `command_ids` are distinct
    // (each is a fresh BIGSERIAL from a separate INSERT). The poll
    // loop only exits once every id has an entry: either
    // `done.len() == command_ids.len()` (all terminal) or the
    // deadline branch (inserts Timeout for any missing id). So
    // `.expect` is honest: a missing id here is a logic bug, not a
    // runtime case to paper over.
    Ok(command_ids
        .iter()
        .map(|id| (*id, done.get(id).cloned().expect("loop filled every id")))
        .collect())
}

/// Drop every row for a project. Called on `weft rm`.
pub async fn remove_project(pool: &PgPool, project_id: &str) -> Result<u64> {
    let res = sqlx::query("DELETE FROM infra_lifecycle_command WHERE project_id = $1")
        .bind(project_id)
        .execute(pool)
        .await?;
    Ok(res.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verb_round_trips() {
        for v in [
            InfraLifecycleVerb::Apply,
            InfraLifecycleVerb::Stop,
            InfraLifecycleVerb::Terminate,
        ] {
            assert_eq!(InfraLifecycleVerb::parse(v.as_str()), Some(v));
        }
    }

    #[test]
    fn verb_unknown_returns_none() {
        assert_eq!(InfraLifecycleVerb::parse("upgrade"), None);
        assert_eq!(InfraLifecycleVerb::parse(""), None);
    }

    #[test]
    fn running_policy_round_trips() {
        for p in [RunningPolicy::Cancel, RunningPolicy::Wait] {
            assert_eq!(RunningPolicy::parse(p.as_str()), Some(p));
        }
    }

    #[test]
    fn running_policy_default_is_wait() {
        assert_eq!(RunningPolicy::default(), RunningPolicy::Wait);
    }
}
