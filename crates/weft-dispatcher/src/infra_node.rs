//! Per-project infra registry, backed by Postgres `infra_node` rows.
//!
//! One row per `(project_id, node_id, member_id)` tracking the
//! desired-vs-applied state of one COPY of an infra node: the program's
//! shared copy (`member_id` NULL), or one member's copy of a node marked
//! `@per_member`. `node_id` is
//! the node's PLACE spelled the way a person writes it (`db`, or
//! `one.db` inside the file the site `one` includes). A file included
//! twice holds two instances, one row each, and the spelling is what
//! tells them apart; it is also what every reader prints and every
//! verb takes, so nothing translates on the way in or out. The
//! compiled id behind a place is never stored here. The supervisor pod
//! writes status transitions as it executes a claimed
//! `infra_lifecycle_command`, and writes runtime events (Flaky /
//! Recovered) and may flip status as part of that execution.
//!
//! Workers read `endpoints_json` through the broker's
//! `/v1/infra/endpoint_url` endpoint, which answers from
//! `weft_task_store::PostgresInfraReader` (NOT from this module).

use anyhow::Result;
use serde_json::Value;
use sqlx::postgres::PgPool;
use sqlx::Row;
use std::collections::BTreeMap;

// `InfraNodeStatus` and `FailureStage` are the wire contract for the
// `status` and `failure_stage` columns of `infra_node`. They live in
// `weft-broker-client::protocol` so the supervisor + dispatcher share
// one source of truth. Re-export here so this module stays the
// canonical home for infra_node glue.
pub use weft_broker_client::protocol::{FailureStage, InfraNodeStatus, UnitRuntime};

/// One row in `infra_node`.
#[derive(Debug, Clone)]
pub struct InfraNodeRow {
    pub project_id: uuid::Uuid,
    /// The instance's place, spelled (see the module doc).
    pub node_id: String,
    /// Whose copy: `None` for the shared one, else the member's.
    pub member: Option<weft_core::member::MemberId>,
    /// Stable per-apply id (Deployment name etc). Empty string when
    /// status is `Failed` and the apply never produced one.
    pub instance_id: String,
    /// Project namespace (`wft-project-<tenant>-<project>`).
    pub namespace: String,
    pub status: InfraNodeStatus,
    pub failure_stage: Option<FailureStage>,
    pub failure_message: Option<String>,
    /// Hash of the resolved manifest set this row was last applied
    /// against. Set by the apply task on success; used by the next
    /// apply attempt to decide skip-vs-roll.
    pub applied_spec_hash: Option<String>,
    pub applied_at_unix: Option<i64>,
    /// Endpoint name → cluster-internal URL. `BTreeMap`: callers
    /// resolve endpoints by name, and a deterministic order keeps any
    /// "first" semantics stable.
    pub endpoints: BTreeMap<String, String>,
    /// Endpoint name → the path a `TenantPublic` endpoint answers at on
    /// the front door (`/infra/<namespace>/<instance>/<declared path>`),
    /// for that kind of endpoint only. A path, not a URL: the front
    /// door's address is the install's, joined on read.
    pub public_paths: BTreeMap<String, String>,
    /// PVC names to KEEP on terminate. Carried from
    /// `InfraSpec.lifecycle.on_terminate.preserve_pvcs` at apply
    /// time so the supervisor can honor it at terminate time
    /// (terminate has no access to the spec; the worker that
    /// applied the spec is long gone).
    pub preserve_pvcs: Vec<String>,
    /// Per-unit runtime (status + resolved health windows +
    /// stop_behavior), keyed by unit name. The `status` column above
    /// is a rollup over these. Stamped at apply from the spec's units;
    /// the authoritative unit roster the supervisor operates on.
    pub units: BTreeMap<String, UnitRuntime>,
}

pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "infra_node",
    tables: &["infra_node"],
    ddl: &[
        r#"CREATE TABLE IF NOT EXISTS infra_node (
            project_id          UUID NOT NULL,
            node_id             TEXT NOT NULL,
            instance_id         TEXT NOT NULL DEFAULT '',
            namespace           TEXT NOT NULL,
            status              TEXT NOT NULL,
            failure_stage       TEXT,
            failure_message     TEXT,
            applied_spec_hash   TEXT,
            applied_at_unix     BIGINT,
            endpoints_json      JSONB NOT NULL DEFAULT '{}'::jsonb,
            -- Endpoint name to its front-door path, for TenantPublic
            -- endpoints only. Stamped at apply.
            public_paths_json   JSONB NOT NULL DEFAULT '{}'::jsonb,
            -- PVC names to preserve on terminate. JSON array;
            -- empty means "delete all matching PVCs."
            preserve_pvcs_json  JSONB NOT NULL DEFAULT '[]'::jsonb,
            -- Per-unit runtime (status + resolved health windows +
            -- stop_behavior) keyed by unit name. The `status` column
            -- is a rollup over these. Stamped at apply.
            units_json          JSONB NOT NULL DEFAULT '{}'::jsonb,
            -- Whose copy: NULL for the program's shared one, else the
            -- member whose copy of a `@per_member` node this is.
            member_id           TEXT
        )"#,
        r#"CREATE UNIQUE INDEX IF NOT EXISTS idx_infra_node_copy
             ON infra_node(project_id, node_id, member_id) NULLS NOT DISTINCT"#,
        r#"CREATE INDEX IF NOT EXISTS idx_infra_node_project   ON infra_node(project_id)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_infra_node_namespace ON infra_node(namespace)"#,
    ],
    seed: &[],
};

/// Update just the status column. Idempotent: identical writes
/// produce no observable effect. Used by the supervisor and the
/// stop/terminate API handlers for transient states like Stopping.
pub async fn set_status(
    pool: &PgPool,
    project_id: uuid::Uuid,
    node_id: &str,
    member: Option<&weft_core::member::MemberId>,
    status: InfraNodeStatus,
) -> Result<()> {
    sqlx::query(
        "UPDATE infra_node SET status = $1 \
         WHERE project_id = $2 AND node_id = $3 AND member_id IS NOT DISTINCT FROM $4",
    )
    .bind(status.as_str())
    .bind(project_id)
    .bind(node_id)
    .bind(member.map(|m| m.as_str()))
    .execute(pool)
    .await?;
    Ok(())
}

/// The columns every read decodes (`parse_row`).
const ROW_COLUMNS: &str = "project_id, node_id, member_id, instance_id, namespace, status, \
     failure_stage, failure_message, applied_spec_hash, \
     applied_at_unix, endpoints_json, public_paths_json, preserve_pvcs_json, units_json";

/// Read one copy's row. Returns None when the row doesn't exist (no
/// infra was ever applied for this copy).
pub async fn get(
    pool: &PgPool,
    project_id: uuid::Uuid,
    node_id: &str,
    member: Option<&weft_core::member::MemberId>,
) -> Result<Option<InfraNodeRow>> {
    let row = sqlx::query(&format!(
        "SELECT {ROW_COLUMNS} FROM infra_node \
         WHERE project_id = $1 AND node_id = $2 AND member_id IS NOT DISTINCT FROM $3"
    ))
    .bind(project_id)
    .bind(node_id)
    .bind(member.map(|m| m.as_str()))
    .fetch_optional(pool)
    .await?;
    match row {
        None => Ok(None),
        Some(r) => Ok(Some(parse_row(r)?)),
    }
}

/// List every row for a project. Drives the project status response.
pub async fn list_for_project(
    pool: &PgPool,
    project_id: uuid::Uuid,
) -> Result<Vec<InfraNodeRow>> {
    let rows = sqlx::query(&format!(
        "SELECT {ROW_COLUMNS} FROM infra_node WHERE project_id = $1 ORDER BY node_id, member_id NULLS FIRST"
    ))
    .bind(project_id)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(parse_row).collect()
}

/// Whether ANY infra_node row exists for the project (regardless of
/// status). The "live infra state exists" fact worker placement keys
/// on: a project whose infra was never provisioned (or fully
/// terminated) has no rows, so its worker (including the InfraSetup
/// provisioning execution) runs in the shared pool; the first apply
/// writes a row and subsequent workers land in the project namespace.
pub async fn any_for_project(pool: &PgPool, project_id: uuid::Uuid) -> Result<bool> {
    let (exists,): (bool,) =
        sqlx::query_as("SELECT EXISTS(SELECT 1 FROM infra_node WHERE project_id = $1)")
            .bind(project_id)
            .fetch_one(pool)
            .await?;
    Ok(exists)
}

/// Delete one copy's row. Idempotent. Called after a successful
/// terminate.
pub async fn remove(
    pool: &PgPool,
    project_id: uuid::Uuid,
    node_id: &str,
    member: Option<&weft_core::member::MemberId>,
) -> Result<bool> {
    let res = sqlx::query(
        "DELETE FROM infra_node WHERE project_id = $1 AND node_id = $2 AND member_id IS NOT DISTINCT FROM $3",
    )
    .bind(project_id)
    .bind(node_id)
    .bind(member.map(|m| m.as_str()))
    .execute(pool)
    .await?;
    Ok(res.rows_affected() > 0)
}

/// Drop every row for a project. Called on `weft rm` after the
/// project's terminate has completed.
pub async fn remove_project<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    project_id: uuid::Uuid,
) -> Result<u64> {
    let res = sqlx::query("DELETE FROM infra_node WHERE project_id = $1")
        .bind(project_id)
        .execute(executor)
        .await?;
    Ok(res.rows_affected())
}

/// What starts, stops and terminates are under way for a project's copies
/// that the rows do not show yet: an infra setup run still going (it will
/// issue its applies), and every lifecycle command the supervisor has not
/// finished. A row changes only when the supervisor gets to it, so a
/// reader that took the row alone would say `stopped` for a copy whose
/// start was just queued, and `running` for one whose stop was; every
/// reader goes through [`PendingOps::status_of`] instead, so they all
/// give one answer.
#[derive(Debug, Clone, Default)]
pub struct PendingOps {
    ops: Vec<PendingOp>,
}

/// One operation [`PendingOps`] holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingOp {
    pub kind: PendingKind,
    /// The place it acts on, spelled; `None` for every node of `copies`.
    pub node: Option<String>,
    pub copies: weft_core::member::Copies,
    /// The order the supervisor carries it out in: a command's id. A
    /// setup still running issues its applies after every command that
    /// exists now, so it orders after them all.
    pub order: i64,
    /// The setup run behind a start, when a setup run is what it is (a
    /// stop of the copy cancels it, so its applies never land after the
    /// stop).
    pub setup: Option<weft_core::Color>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingKind {
    Start,
    Stop,
    Terminate,
}

impl PendingOps {
    pub fn new(ops: Vec<PendingOp>) -> Self {
        Self { ops }
    }

    /// What the copy of `node` owned by `member` reads as, given its row's
    /// status (`None` when it has no row). The operation carried out last
    /// decides: a start makes a copy that is not up read `provisioning`,
    /// a stop makes a copy that is not already down read `stopping`, a
    /// terminate makes any copy read `terminating`. `None` means no copy
    /// and nothing starting one.
    pub fn status_of(
        &self,
        node: &str,
        member: Option<&weft_core::member::MemberId>,
        row: Option<InfraNodeStatus>,
    ) -> Option<InfraNodeStatus> {
        let last = self
            .ops
            .iter()
            .filter(|op| op.node.as_deref().is_none_or(|n| n == node) && op.copies.admits(member))
            .max_by_key(|op| op.order);
        match (last.map(|op| op.kind), row) {
            (None, row) => row,
            (Some(PendingKind::Start), Some(up @ (InfraNodeStatus::Running | InfraNodeStatus::Flaky))) => Some(up),
            (Some(PendingKind::Start), _) => Some(InfraNodeStatus::Provisioning),
            (Some(PendingKind::Stop), Some(down @ (InfraNodeStatus::Stopped | InfraNodeStatus::Terminating))) => Some(down),
            (Some(PendingKind::Stop), Some(_)) => Some(InfraNodeStatus::Stopping),
            (Some(PendingKind::Terminate), Some(_)) => Some(InfraNodeStatus::Terminating),
            (Some(PendingKind::Stop | PendingKind::Terminate), None) => None,
        }
    }

    /// Whether a start of this copy is under way.
    pub fn starting(&self, node: &str, member: Option<&weft_core::member::MemberId>) -> bool {
        self.status_of(node, member, None) == Some(InfraNodeStatus::Provisioning)
    }

    /// The setup runs bringing this copy up.
    pub fn setups_starting(&self, node: &str, member: Option<&weft_core::member::MemberId>) -> Vec<weft_core::Color> {
        let mut colors: Vec<weft_core::Color> = self
            .ops
            .iter()
            .filter(|op| op.node.as_deref() == Some(node) && op.copies.admits(member))
            .filter_map(|op| op.setup)
            .collect();
        colors.sort();
        colors.dedup();
        colors
    }

    /// Every copy with no row that a start is bringing up, as
    /// `(place, member)`: what a reader lists beside the rows. A start
    /// always names its copy (an apply names one, a setup run its
    /// member's places).
    fn starting_without_row(&self, rows: &[InfraNodeRow]) -> Vec<(String, Option<weft_core::member::MemberId>)> {
        let mut out: Vec<(String, Option<weft_core::member::MemberId>)> = Vec::new();
        for op in self.ops.iter().filter(|op| op.kind == PendingKind::Start) {
            let (Some(node), member) = (&op.node, match &op.copies {
                weft_core::member::Copies::Shared => None,
                weft_core::member::Copies::Member(m) => Some(m.clone()),
                weft_core::member::Copies::Every => continue,
            }) else {
                continue;
            };
            let has_row = rows.iter().any(|r| &r.node_id == node && r.member == member);
            let copy = (node.clone(), member);
            if !has_row && !out.contains(&copy) && self.starting(&copy.0, copy.1.as_ref()) {
                out.push(copy);
            }
        }
        out.sort();
        out
    }
}

/// Every copy of a project the way every reader shows it (see
/// [`PendingOps`]).
#[derive(Debug, Clone, Default)]
pub struct ObservedCopies {
    /// Each row, its `status` read through what is under way.
    pub rows: Vec<InfraNodeRow>,
    /// Copies with no row yet that a start is bringing up, as `(place,
    /// member)`: they read `provisioning`.
    pub starting: Vec<(String, Option<weft_core::member::MemberId>)>,
}

impl ObservedCopies {
    /// One copy's status, `None` when there is no copy and nothing
    /// starting one.
    pub fn status_of(&self, node: &str, member: Option<&weft_core::member::MemberId>) -> Option<InfraNodeStatus> {
        if let Some(row) = self.rows.iter().find(|r| r.node_id == node && r.member.as_ref() == member) {
            return Some(row.status);
        }
        self.starting
            .iter()
            .any(|(n, m)| n == node && m.as_ref() == member)
            .then_some(InfraNodeStatus::Provisioning)
    }
}

/// Read every copy of `project_id` with what is under way applied: the one
/// read every status reader goes through.
pub async fn observe(
    pool: &PgPool,
    project_id: uuid::Uuid,
    project: &weft_core::ProjectDefinition,
) -> Result<ObservedCopies> {
    Ok(observe_with(pool, project_id, project).await?.0)
}

/// [`observe`], with the operations under way it applied.
pub async fn observe_with(
    pool: &PgPool,
    project_id: uuid::Uuid,
    project: &weft_core::ProjectDefinition,
) -> Result<(ObservedCopies, PendingOps)> {
    let mut rows = list_for_project(pool, project_id).await?;
    let pending = pending_ops(pool, project_id, project).await?;
    for row in &mut rows {
        if let Some(status) = pending.status_of(&row.node_id, row.member.as_ref(), Some(row.status)) {
            row.status = status;
        }
    }
    let starting = pending.starting_without_row(&rows);
    Ok((ObservedCopies { rows, starting }, pending))
}

/// Read what is under way for `project_id`'s copies (see [`PendingOps`]).
/// `project` spells the places a setup run brings up.
pub async fn pending_ops(
    pool: &PgPool,
    project_id: uuid::Uuid,
    project: &weft_core::ProjectDefinition,
) -> Result<PendingOps> {
    let mut ops = Vec::new();
    /// id, node_id, verb, member_id, every_copy.
    type CommandRow = (i64, Option<String>, String, Option<String>, bool);
    let commands: Vec<CommandRow> = sqlx::query_as(&format!(
        "SELECT id, node_id, verb, member_id, every_copy FROM infra_lifecycle_command \
         WHERE project_id = $1 AND completed_at_unix IS NULL AND verb IN ({verbs})",
        verbs = weft_broker_client::lifecycle_command::SUPERVISOR_VERBS_SQL,
    ))
    .bind(project_id)
    .fetch_all(pool)
    .await?;
    for (id, node, verb, member, every) in commands {
        let kind = match crate::infra_lifecycle_command::InfraLifecycleVerb::parse(&verb) {
            Some(crate::infra_lifecycle_command::InfraLifecycleVerb::Apply) => PendingKind::Start,
            Some(crate::infra_lifecycle_command::InfraLifecycleVerb::Stop) => PendingKind::Stop,
            Some(crate::infra_lifecycle_command::InfraLifecycleVerb::Terminate) => PendingKind::Terminate,
            other => anyhow::bail!("infra_lifecycle_command {id} holds verb '{verb}' ({other:?}), which no supervisor carries out"),
        };
        let copies = weft_core::member::Copies::from_columns(member, every)
            .map_err(|e| anyhow::anyhow!("infra_lifecycle_command {id}: {e}"))?;
        ops.push(PendingOp { kind, node, copies, order: id, setup: None });
    }
    // A setup run still being worked on brings up the infra places its
    // selection holds, for its member. One recorded but abandoned (no
    // task left) starts nothing, and the next start ends it.
    let infra: std::collections::BTreeSet<String> = weft_core::project::infra_place_spellings(project);
    let setups: Vec<(String, Option<String>, String)> = sqlx::query_as(
        "SELECT ec.color, ec.member_id, e.payload_json FROM execution_color ec \
         JOIN exec_event e ON e.color = ec.color AND e.kind = 'execution_started' \
         WHERE ec.project_id = $1 AND ec.phase = 'infra_setup' \
           AND NOT EXISTS ( \
             SELECT 1 FROM exec_event t WHERE t.color = ec.color \
               AND t.kind IN ('execution_completed', 'execution_failed', 'execution_cancelled') \
           )",
    )
    .bind(project_id)
    .fetch_all(pool)
    .await?;
    for (color, member, payload) in setups {
        let color: weft_core::Color = color.parse().map_err(|e| anyhow::anyhow!("infra setup color '{color}': {e}"))?;
        if !crate::api::execution::execution_is_being_worked_on(pool, color).await? {
            continue;
        }
        let member = member
            .map(weft_core::member::MemberId::new)
            .transpose()
            .map_err(|e| anyhow::anyhow!("infra setup {color} member: {e}"))?;
        let weft_journal::ExecEvent::ExecutionStarted { subgraph, .. } =
            weft_journal::decode_event(color, &payload).map_err(anyhow::Error::msg)?
        else {
            anyhow::bail!("infra setup {color}: its execution_started row holds another event");
        };
        let Some(subgraph) = subgraph else {
            anyhow::bail!("infra setup {color} was born without its selection; it cannot say what it brings up");
        };
        for place in &subgraph.nodes {
            let spelled = weft_core::project::address_of(project, &place.id, &place.path);
            if infra.contains(&spelled) {
                ops.push(PendingOp {
                    kind: PendingKind::Start,
                    node: Some(spelled),
                    copies: weft_core::member::Copies::of(member.clone()),
                    order: i64::MAX,
                    setup: Some(color),
                });
            }
        }
    }
    Ok(PendingOps::new(ops))
}

/// Decode one `infra_node` row. Every NOT-NULL column is required;
/// every nullable column is `Option<T>` and surfaces as such. A
/// decode failure on ANY column is schema drift (or a wrong-typed
/// JSON column) and propagates as `Err` rather than being coerced
/// to None / empty (which would let downstream observe a half-valid
/// row).
fn parse_row(row: sqlx::postgres::PgRow) -> anyhow::Result<InfraNodeRow> {
    let project_id: uuid::Uuid = row.try_get("project_id")?;
    let node_id: String = row.try_get("node_id")?;
    let member: Option<String> = row.try_get("member_id")?;
    let member = member
        .map(weft_core::member::MemberId::new)
        .transpose()
        .map_err(|e| anyhow::anyhow!("infra_node.member_id for project={project_id} node={node_id}: {e}"))?;
    let instance_id: String = row.try_get("instance_id")?;
    let namespace: String = row.try_get("namespace")?;
    let status_str: String = row.try_get("status")?;
    let status = InfraNodeStatus::parse(&status_str).ok_or_else(|| {
        anyhow::anyhow!(
            "infra_node.status='{status_str}' for project={project_id} node={node_id} \
             is not a known InfraNodeStatus"
        )
    })?;
    let failure_stage_str: Option<String> = row.try_get("failure_stage")?;
    let failure_stage = match failure_stage_str.as_deref() {
        None => None,
        Some(s) => Some(FailureStage::parse(s).ok_or_else(|| {
            anyhow::anyhow!(
                "infra_node.failure_stage='{s}' for project={project_id} node={node_id} \
                 is not a known FailureStage"
            )
        })?),
    };
    let failure_message: Option<String> = row.try_get("failure_message")?;
    let applied_spec_hash: Option<String> = row.try_get("applied_spec_hash")?;
    let applied_at_unix: Option<i64> = row.try_get("applied_at_unix")?;
    let endpoints_json: Value = row.try_get("endpoints_json")?;
    let endpoints: BTreeMap<String, String> = serde_json::from_value(endpoints_json)
        .map_err(|e| {
            anyhow::anyhow!(
                "infra_node.endpoints_json for project={project_id} node={node_id} \
                 is not a string-to-string map: {e}"
            )
        })?;
    let public_paths_json: Value = row.try_get("public_paths_json")?;
    let public_paths: BTreeMap<String, String> = serde_json::from_value(public_paths_json)
        .map_err(|e| {
            anyhow::anyhow!(
                "infra_node.public_paths_json for project={project_id} node={node_id} \
                 is not a string-to-string map: {e}"
            )
        })?;
    let preserve_pvcs_json: Value = row.try_get("preserve_pvcs_json")?;
    let preserve_pvcs: Vec<String> = serde_json::from_value(preserve_pvcs_json)
        .map_err(|e| {
            anyhow::anyhow!(
                "infra_node.preserve_pvcs_json for project={project_id} node={node_id} \
                 is not a Vec<String>: {e}"
            )
        })?;
    let units_json: Value = row.try_get("units_json")?;
    let units = weft_broker_client::protocol::decode_units_json(units_json, project_id, &node_id)?;
    Ok(InfraNodeRow {
        project_id,
        node_id,
        member,
        instance_id,
        namespace,
        status,
        failure_stage,
        failure_message,
        applied_spec_hash,
        applied_at_unix,
        endpoints,
        public_paths,
        preserve_pvcs,
        units,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_round_trips() {
        for s in [
            InfraNodeStatus::Provisioning,
            InfraNodeStatus::Running,
            InfraNodeStatus::Stopped,
            InfraNodeStatus::Flaky,
            InfraNodeStatus::Failed,
            InfraNodeStatus::Stopping,
            InfraNodeStatus::Terminating,
        ] {
            assert_eq!(InfraNodeStatus::parse(s.as_str()), Some(s));
        }
    }

    #[test]
    fn status_parse_unknown_returns_none() {
        assert_eq!(InfraNodeStatus::parse("garbage"), None);
        assert_eq!(InfraNodeStatus::parse(""), None);
        // Casing matters; status strings are lowercase on the wire.
        assert_eq!(InfraNodeStatus::parse("Running"), None);
    }

    #[test]
    fn status_as_str_is_lowercase_snake() {
        // The supervisor + apply executor write these directly into
        // the row; the parse() round-trip above already proves them,
        // but pin exact wire bytes too so a casual rename doesn't
        // silently break the SSE protocol.
        assert_eq!(InfraNodeStatus::Provisioning.as_str(), "provisioning");
        assert_eq!(InfraNodeStatus::Running.as_str(), "running");
        assert_eq!(InfraNodeStatus::Stopped.as_str(), "stopped");
        assert_eq!(InfraNodeStatus::Flaky.as_str(), "flaky");
        assert_eq!(InfraNodeStatus::Failed.as_str(), "failed");
        assert_eq!(InfraNodeStatus::Stopping.as_str(), "stopping");
        assert_eq!(InfraNodeStatus::Terminating.as_str(), "terminating");
    }

    fn op(kind: PendingKind, node: Option<&str>, copies: weft_core::member::Copies, order: i64) -> PendingOp {
        PendingOp { kind, node: node.map(str::to_string), copies, order, setup: None }
    }

    fn ada() -> weft_core::member::MemberId {
        weft_core::member::MemberId::new("ada").unwrap()
    }

    /// With nothing under way the row answers, and no row is no copy.
    #[test]
    fn nothing_pending_reads_the_row() {
        let none = PendingOps::default();
        assert_eq!(none.status_of("db", None, Some(InfraNodeStatus::Stopped)), Some(InfraNodeStatus::Stopped));
        assert_eq!(none.status_of("db", None, None), None);
    }

    /// A start queued over a stopped row (a member resumed after a pause)
    /// reads as starting at once, not as the old `stopped`; one over a
    /// copy that runs leaves it running.
    #[test]
    fn a_queued_start_reads_as_provisioning() {
        use weft_core::member::Copies;
        let ops = PendingOps::new(vec![op(PendingKind::Start, Some("bridge"), Copies::Member(ada()), i64::MAX)]);
        assert_eq!(ops.status_of("bridge", Some(&ada()), Some(InfraNodeStatus::Stopped)), Some(InfraNodeStatus::Provisioning));
        assert_eq!(ops.status_of("bridge", Some(&ada()), None), Some(InfraNodeStatus::Provisioning));
        assert_eq!(ops.status_of("bridge", Some(&ada()), Some(InfraNodeStatus::Running)), Some(InfraNodeStatus::Running));
        // Another member's copy and the shared copy are not touched.
        let bob = weft_core::member::MemberId::new("bob").unwrap();
        assert_eq!(ops.status_of("bridge", Some(&bob), Some(InfraNodeStatus::Stopped)), Some(InfraNodeStatus::Stopped));
        assert_eq!(ops.status_of("bridge", None, None), None);
        assert!(ops.starting("bridge", Some(&ada())));
        assert!(!ops.starting("other", Some(&ada())));
    }

    /// A queued stop reads as stopping on a copy that is up, and leaves a
    /// copy that is down alone; a node-less command reaches every node of
    /// its copies.
    #[test]
    fn a_queued_stop_reads_as_stopping() {
        use weft_core::member::Copies;
        let ops = PendingOps::new(vec![op(PendingKind::Stop, None, Copies::Shared, 4)]);
        assert_eq!(ops.status_of("db", None, Some(InfraNodeStatus::Running)), Some(InfraNodeStatus::Stopping));
        assert_eq!(ops.status_of("db", None, Some(InfraNodeStatus::Stopped)), Some(InfraNodeStatus::Stopped));
        assert_eq!(ops.status_of("db", None, None), None);
        let term = PendingOps::new(vec![op(PendingKind::Terminate, Some("db"), Copies::Every, 5)]);
        assert_eq!(term.status_of("db", Some(&ada()), Some(InfraNodeStatus::Stopped)), Some(InfraNodeStatus::Terminating));
    }

    /// The operation carried out last decides: a stop queued before a
    /// start (a pause, then a resume) ends up starting; a stop queued
    /// after a start's apply ends up stopping.
    #[test]
    fn the_last_operation_decides() {
        use weft_core::member::Copies;
        let member = Copies::Member(ada());
        let resumed = PendingOps::new(vec![
            op(PendingKind::Stop, Some("bridge"), member.clone(), 7),
            op(PendingKind::Start, Some("bridge"), member.clone(), i64::MAX),
        ]);
        assert_eq!(resumed.status_of("bridge", Some(&ada()), Some(InfraNodeStatus::Running)), Some(InfraNodeStatus::Running));
        assert_eq!(resumed.status_of("bridge", Some(&ada()), Some(InfraNodeStatus::Stopping)), Some(InfraNodeStatus::Provisioning));
        let paused = PendingOps::new(vec![
            op(PendingKind::Start, Some("bridge"), member.clone(), 8),
            op(PendingKind::Stop, Some("bridge"), member, 9),
        ]);
        assert_eq!(paused.status_of("bridge", Some(&ada()), Some(InfraNodeStatus::Provisioning)), Some(InfraNodeStatus::Stopping));
    }

    /// A copy with no row that a start brings up is listed once.
    #[test]
    fn starting_copies_without_a_row_are_listed() {
        use weft_core::member::Copies;
        let ops = PendingOps::new(vec![op(PendingKind::Start, Some("bridge"), Copies::Member(ada()), i64::MAX)]);
        assert_eq!(ops.starting_without_row(&[]), vec![("bridge".to_string(), Some(ada()))]);
    }

    #[test]
    fn failure_stage_as_str() {
        assert_eq!(FailureStage::Provision.as_str(), "provision");
        assert_eq!(FailureStage::Apply.as_str(), "apply");
        assert_eq!(FailureStage::Run.as_str(), "run");
        assert_eq!(FailureStage::ApplyLifecycle.as_str(), "apply_lifecycle");
    }
}
