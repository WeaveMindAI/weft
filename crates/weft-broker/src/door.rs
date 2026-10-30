//! What a worker's door asks the broker (`weft_engine::door`): its
//! project's triggers, what a run starts with besides its caller, who
//! an instance token names, once a second the counts its limits are held
//! to and what it drives, and letting go of a run whose whole record is
//! written (parked on a wait, or handed back for another worker).
//!
//! A caller's call reaches the worker, and the worker runs every check on
//! it: the triggers and the
//! run facts are the rows the dispatcher read, and the caller's own check
//! is the broker's (`/v1/caller/verify`). Every answer here is about the
//! asking worker's own project, from its verified identity, never from
//! anything it sends.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::Router;
use serde_json::Value;
use weft_task_store::db::{self, Client, Pool, Row};

use weft_broker_client::protocol::{
    ArmedEntry, DoorEntry, DoorInstanceToken, DoorInstanceTokenRequest, DoorLetGo, DoorLetGoRequest, DoorParkRequest, DoorParked,
    DoorMount, DoorRunFacts, DoorRunFactsRequest, DoorTick, DoorTickRequest, DoorTrigger, DoorTriggers, DoorTriggersRequest,
    InfraCopyUp, LetGo, SIGNAL_ACTIVATION_JOIN,
};

use crate::auth::{AuthedCaller, CallerIdentity, Role};
use crate::handlers::unavailable_or_internal;
use crate::state::BrokerState;
use weft_core::wire_shape::Json;

type Resp<T> = Result<Json<T>, (StatusCode, String)>;

pub fn routes() -> Router<Arc<BrokerState>> {
    Router::new()
        .route("/v1/door/triggers", post(door_triggers))
        .route("/v1/door/park_fire", post(door_park_fire))
        .route("/v1/door/run_facts", post(door_run_facts))
        .route("/v1/door/instance_token", post(door_instance_token))
        .route("/v1/door/tick", post(door_tick))
        .route("/v1/door/let_go", post(door_let_go))
}

/// The asking worker's tenant and project, and its replica: every door
/// question is about its own project.
pub(crate) fn worker(caller: &CallerIdentity) -> Result<(&str, uuid::Uuid, &str), (StatusCode, String)> {
    let refused = || (StatusCode::FORBIDDEN, "only a project's worker asks at its door".to_string());
    if caller.role != Role::Worker {
        return Err(refused());
    }
    let tenant = caller.scope.pinned_tenant().ok_or_else(refused)?;
    let project = caller.scope.pinned_project().ok_or_else(refused)?;
    let replica = caller.replica.as_deref().ok_or_else(refused)?;
    Ok((tenant, project, replica))
}

/// What a trigger's row is read with: everything a door needs to arm it
/// and how its activation stands.
fn trigger_select(filter: &str) -> String {
    format!(
        "SELECT s.token, s.node_id, s.surface_kind, s.mount_path, s.mount_methods, s.spec_json, s.spec_json_version, \
                s.auth_kind, s.auth_config, s.port_snapshot, s.definition_hash, s.binary_hash, s.source_version, \
                s.instance_id, s.held_by, \
                CASE WHEN p.id IS NULL THEN 'inactive' ELSE COALESCE(a.status, 'active') END AS status, \
                CASE WHEN p.id IS NULL THEN FALSE ELSE COALESCE(a.accepting_fires, TRUE) END AS accepting_fires, \
                a.fires_deadline_unix \
         FROM signal s \
         LEFT JOIN project p ON p.id = s.project_id \
         {SIGNAL_ACTIVATION_JOIN} \
         WHERE s.project_id = $1 AND {filter}"
    )
}

/// `POST /v1/door/triggers`: see [`DoorTriggers`].
async fn door_triggers(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(_): Json<DoorTriggersRequest>,
) -> Resp<DoorTriggers> {
    let (tenant, project, _) = worker(&caller)?;
    let rows = state
        .pool
        .query(&trigger_select("NOT s.is_resume"), &[&project])
        .await
        .map_err(|e| unavailable_or_internal(anyhow::Error::from(e).context("read the project's triggers")))?;
    let mount = weft_core::route::SharedMount::new(tenant, project);
    let triggers = rows.iter().map(|row| door_trigger(row, mount)).collect::<anyhow::Result<Vec<_>>>().map_err(unavailable_or_internal)?;
    Ok(Json(DoorTriggers { triggers }))
}

/// `POST /v1/door/park_fire`: see [`DoorParkRequest`]. A project's worker
/// parks a fire of its own project's trigger; the listener parks the event
/// of any trigger whose worker it could not reach. An answer to a waiting
/// run is the install's to keep, never parked here, and a trigger that takes
/// no work keeps nothing.
async fn door_park_fire(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<DoorParkRequest>,
) -> Resp<DoorParked> {
    let asker = match caller.role {
        Role::Listener => None,
        _ => Some(worker(&caller)?.1),
    };
    let row: Option<Row> = state
        .pool
        .opt(
            &format!(
                "SELECT s.project_id, s.is_resume, \
                        CASE WHEN p.id IS NULL THEN 'inactive' ELSE COALESCE(a.status, 'active') END AS status, \
                        CASE WHEN p.id IS NULL THEN FALSE ELSE COALESCE(a.accepting_fires, TRUE) END AS accepting_fires, \
                        a.fires_deadline_unix \
                 FROM signal s LEFT JOIN project p ON p.id = s.project_id {SIGNAL_ACTIVATION_JOIN} \
                 WHERE s.token = $1"
            ),
            &[&req.token],
        )
        .await
        .map_err(|e| unavailable_or_internal(anyhow::Error::from(e).context("read a trigger")))?;
    let Some(row) = row else { return Ok(Json(DoorParked::Gone)) };
    let read = || -> anyhow::Result<(uuid::Uuid, bool, weft_core::arrival::Standing)> {
        let status: String = row.try_get("status")?;
        let status = weft_core::projects::ProjectStatus::parse(&status).ok_or_else(|| anyhow::anyhow!("unknown activation status '{status}'"))?;
        let standing = weft_core::arrival::Standing {
            status,
            accepting_fires: row.try_get("accepting_fires")?,
            fires_deadline_unix: row.try_get("fires_deadline_unix")?,
        };
        Ok((row.try_get("project_id")?, row.try_get("is_resume")?, standing))
    };
    let (owner, is_resume, standing) = read().map_err(unavailable_or_internal)?;
    if asker.is_some_and(|project| project != owner) {
        return Err((StatusCode::FORBIDDEN, "a fire for another project's trigger".into()));
    }
    if is_resume {
        return Err((StatusCode::BAD_REQUEST, "only an entry's fire is parked here".into()));
    }
    // A held connection's event is kept only while its holder still holds
    // the signal, checked in the append itself; once kept it is the
    // trigger's.
    if let Some(holder) = req.held_by.as_deref() {
        if caller.role == Role::Listener && !crate::held_signals::sent_by_its_holder(caller.replica.as_deref(), holder) {
            return Err((StatusCode::FORBIDDEN, "an event parked in another holder's name".into()));
        }
    }
    let now: i64 = state
        .pool
        .scalar("SELECT EXTRACT(EPOCH FROM NOW())::BIGINT", &[])
        .await
        .map_err(|e| unavailable_or_internal(anyhow::Error::from(e).context("read the database's clock")))?;
    if standing.arrival(now) == weft_core::arrival::Arrival::Refused {
        return Ok(Json(DoorParked::TakesNoWork));
    }
    use weft_task_store::parked_fires::{ParkAppend, ParkRefusal};
    let appended = weft_task_store::parked_fires::park(&state.pool, &req.token, &req.fire, req.held_by.as_deref())
        .await
        .map_err(|e| unavailable_or_internal(e.context("park a fire")))?;
    Ok(Json(match appended {
        ParkAppend::Parked | ParkAppend::Refused(ParkRefusal::AlreadyQueued) => DoorParked::Parked,
        ParkAppend::Refused(ParkRefusal::QueueFull) => DoorParked::QueueFull,
        ParkAppend::Refused(ParkRefusal::RowGone) => DoorParked::Gone,
        ParkAppend::Refused(ParkRefusal::NotHeld) => DoorParked::NotHeld,
        ParkAppend::Refused(ParkRefusal::ResumeAlreadyAnswered) => {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("signal {} turned into an answer to a waiting run", req.token)))
        }
    }))
}

/// One trigger as its signal row arms it: a public entry with a mount is
/// a route somebody calls, which must take a caller on the line.
fn door_trigger(row: &Row, mount: weft_core::route::SharedMount<'_>) -> anyhow::Result<DoorTrigger> {
    let node_id: String = row.try_get("node_id")?;
    let surface_kind: String = row.try_get("surface_kind")?;
    let route = match row.try_get::<_, Option<String>>("mount_path")? {
        Some(mount_path) if surface_kind == "public_entry" => Some(DoorMount {
            pattern: mount.pattern_of(&mount_path),
            methods: row.try_get("mount_methods")?,
        }),
        _ => None,
    };
    Ok(DoorTrigger {
        token: row.try_get("token")?,
        entry: armed_entry(row, &node_id, route.is_some())?,
        held_by: row.try_get("held_by")?,
        route,
        node_id,
    })
}

/// The entry a trigger leads to, or why work for it cannot be served. A
/// route (`caller`) must also take a caller on the line.
fn armed_entry(row: &Row, node_id: &str, caller: bool) -> anyhow::Result<DoorEntry> {
    let unservable = |status: StatusCode, why: String| DoorEntry::Unservable { status: status.as_u16(), why };
    let status: String = row.try_get("status")?;
    let status = weft_core::projects::ProjectStatus::parse(&status)
        .ok_or_else(|| anyhow::anyhow!("unknown activation status '{status}'"))?;
    let standing = weft_core::arrival::Standing {
        status,
        accepting_fires: row.try_get("accepting_fires")?,
        fires_deadline_unix: row.try_get("fires_deadline_unix")?,
    };
    let (Some(definition_hash), Some(binary_hash)) =
        (row.try_get::<_, Option<String>>("definition_hash")?, row.try_get::<_, Option<String>>("binary_hash")?)
    else {
        return Ok(unservable(
            StatusCode::PRECONDITION_REQUIRED,
            format!("trigger '{node_id}' has no armed code identity; activate it again"),
        ));
    };
    let Some(source_version) = row.try_get::<_, Option<String>>("source_version")? else {
        return Ok(unservable(
            StatusCode::PRECONDITION_REQUIRED,
            format!("trigger '{node_id}' has no original source version; activate it again"),
        ));
    };
    let spec_json: String = row.try_get("spec_json")?;
    let spec = match weft_core::primitive::SignalSpec::STORED.decode(spec_json.as_str(), row.try_get("spec_json_version")?) {
        Ok(spec) => spec,
        Err(e) => return Ok(unservable(StatusCode::INTERNAL_SERVER_ERROR, e.at(format_args!("trigger '{node_id}'")).to_string())),
    };
    // A kind that serves no caller on the line is not a live route.
    if caller {
        if weft_core::signal::caller_protocol(&spec.kind).is_none() {
            return Ok(unservable(StatusCode::BAD_REQUEST, format!("endpoint of '{node_id}' is not a live connection ({})", spec.kind)));
        }
        if let Err(e) = weft_core::signal::live_connection(&spec) {
            return Ok(unservable(StatusCode::INTERNAL_SERVER_ERROR, format!("trigger '{node_id}': live config: {e}")));
        }
    }
    let instance = row
        .try_get::<_, Option<String>>("instance_id")?
        .map(weft_core::instance::InstanceId::new)
        .transpose()
        .map_err(|e| anyhow::anyhow!("corrupt signal.instance_id: {e}"))?;
    Ok(DoorEntry::Armed(Box::new(ArmedEntry {
        spec,
        auth_kind: row.try_get("auth_kind")?,
        auth_config: row.try_get::<_, Option<db::JsonText<Value>>>("auth_config")?.map(|json| json.0),
        port_snapshot: row.try_get::<_, Option<db::JsonText<Value>>>("port_snapshot")?.map(|json| json.0),
        definition_hash,
        binary_hash,
        source_version,
        standing,
        instance,
    })))
}

/// `POST /v1/door/run_facts`: see [`DoorRunFacts`].
async fn door_run_facts(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<DoorRunFactsRequest>,
) -> Resp<DoorRunFacts> {
    let (tenant, project, _) = worker(&caller)?;
    let read = async {
        let infra: Vec<InfraCopyUp> =
            weft_task_store::infra_copies::statuses(&state.pool, project).await?.iter().map(weft_task_store::infra_copies::CopyStatus::up).collect();
        let instance_values = match &req.instance {
            Some(instance) => Some(weft_access_store::instance_values(&state.pool, tenant, project, instance).await?),
            None => None,
        };
        let picks = weft_access_store::install_picks(&state.pool, tenant, project).await?;
        anyhow::Ok(DoorRunFacts { infra, instance_values, picks })
    };
    Ok(Json(read.await.map_err(|e| unavailable_or_internal(e.context("read a run's facts")))?))
}

/// `POST /v1/door/instance_token`: see [`DoorInstanceToken`]. Every way a
/// token can fail answers the same `401`, so the door says nothing about
/// which tokens exist.
async fn door_instance_token(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<DoorInstanceTokenRequest>,
) -> Resp<DoorInstanceToken> {
    let (tenant, project, _) = worker(&caller)?;
    let hash = weft_core::signal_token::token_hash(req.token.trim());
    // tenant, projects it may open, instance, expiry, kind
    type TokenRow = (String, Vec<uuid::Uuid>, Option<String>, Option<i64>, String);
    let row: Option<TokenRow> = state
        .pool
        .opt("SELECT tenant_id, allowed_projects, instance_id, expires_at, kind FROM signal_token WHERE token_hash = $1", &[&hash])
        .await
        .map_err(|e| unavailable_or_internal(anyhow::Error::from(e).context("read an instance token")))?;
    let refused = || (StatusCode::UNAUTHORIZED, "this is not an instance token of this project".to_string());
    let Some((token_tenant, projects, instance, expires_at, kind)) = row else { return Err(refused()) };
    let now = chrono::Utc::now().timestamp();
    // An operator key is never taken at a door: it would be an admin key
    // living in a frontend.
    if token_tenant != tenant
        || kind != weft_core::signal_token::TokenKind::Caller.as_str()
        || expires_at.is_some_and(|at| now >= at)
        || projects.first() != Some(&project)
    {
        return Err(refused());
    }
    let instance = instance.ok_or_else(refused)?;
    let instance = weft_core::instance::InstanceId::new(instance).map_err(|e| unavailable_or_internal(anyhow::anyhow!("corrupt signal_token.instance_id: {e}")))?;
    Ok(Json(DoorInstanceToken { project_id: project, instance }))
}


/// `POST /v1/door/tick`: see [`DoorTick`]. One statement renews the
/// worker's lease (as long as it vouched for, when it did), stores its
/// counts and reads the other copies', so a tick is one round trip.
async fn door_tick(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<DoorTickRequest>,
) -> Resp<DoorTick> {
    let (tenant, project, replica) = worker(&caller)?;
    let now = chrono::Utc::now().timestamp();
    let (keys, hits): (Vec<String>, Vec<i64>) = req.counts.iter().map(|c| (c.key.clone(), c.hits)).unzip();
    let leased_until = weft_task_store::worker_door::lease_until(
        now,
        weft_core::time_scale::scaled_secs(weft_task_store::worker_door::WORKER_LEASE_SECS),
        req.vouch_until,
    );
    // `$10` is typed: the previous release's tick, kept while an upgrade
    // rolls out, takes the same first nine arguments.
    let answer: Value = state
        .pool
        .scalar(
            "SELECT weft_door_tick($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::boolean)",
            &[
                &replica,
                &project,
                &tenant,
                &req.binary_hash,
                &leased_until,
                &db::Json(&req.in_flight),
                &req.window_start,
                &keys,
                &hits,
                &req.shortens,
            ],
        )
        .await
        .map_err(|e| unavailable_or_internal(anyhow::Error::from(e).context("tick")))?;
    let tick: DoorTick =
        serde_json::from_value(answer).map_err(|e| unavailable_or_internal(anyhow::anyhow!("read a tick's answer: {e}")))?;
    Ok(Json(tick))
}

/// `POST /v1/door/let_go`: see [`DoorLetGoRequest`]. Only the worker
/// that drives the run lets go of it.
async fn door_let_go(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<DoorLetGoRequest>,
) -> Resp<DoorLetGo> {
    let (tenant, project, replica) = worker(&caller)?;
    let released = let_go(&state.pool, DoorWorker { tenant, project, replica }, req.run_id, req.why)
        .await
        .map_err(|e| unavailable_or_internal(e.context("let go of a run")))?;
    // A run parked whole that another worker drives already is that
    // worker's: nothing to let go of. One handed back must be the asking
    // worker's, or it would be queued by a worker that does not drive it.
    if !released && req.why == LetGo::HandedBack {
        return Err((StatusCode::CONFLICT, format!("run {} is not this worker's to hand back", req.run_id)));
    }
    Ok(Json(DoorLetGo {}))
}

/// A project's worker, as a door call names it.
pub struct DoorWorker<'a> {
    pub tenant: &'a str,
    pub project: uuid::Uuid,
    pub replica: &'a str,
}

/// `worker` lets go of `run_id` for `why` (`DoorLetGoRequest`), in
/// one transaction: the run is `parked` on its wait or `queued` for another
/// worker, with no owner, only while `worker` drives it. A parked run with
/// answers already waiting for it wakes the drain that hands them over,
/// and a queued one the delivery that hands it to a worker. Answers
/// whether the run was that worker's to let go of, or already let go of (a
/// retry whose first answer was lost).
pub async fn let_go(pool: &Pool, worker: DoorWorker<'_>, run_id: weft_core::RunId, why: LetGo) -> anyhow::Result<bool> {
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    let Some(locked) = weft_journal::record::lock_in(&tx, run_id).await? else { return Ok(false) };
    if locked.project_id != worker.project {
        return Ok(false);
    }
    // A run asked to stop while it was driven is queued whatever it was
    // let go for: its wake signals went with the cancel, so parked it
    // would wait for good with the ask unread. The worker that claims it
    // next reads the cancel with its claim and writes its ending. This is
    // the cancel that landed after the drive decided to park and before
    // it let go, so its flag fired on a drive that was already over.
    let to = match why {
        _ if locked.cancel_requested.is_some() => "queued",
        LetGo::Parked => "parked",
        LetGo::HandedBack => "queued",
    };
    if locked.owner.is_none() {
        // Let go of already: by this very call, whose answer was lost.
        return Ok(locked.state == to);
    }
    if locked.owner.as_deref() != Some(worker.replica) || locked.state != "running" {
        return Ok(false);
    }
    tx.execute("UPDATE run SET state = $2, owner = NULL, delivered_until = NULL WHERE run_id = $1", &[&run_id, &to]).await?;
    let locked = weft_journal::record::Locked { state: to.to_string(), owner: None, ..locked };
    // An answer handed to the run while its worker drove it, and not taken
    // before it let go, goes on its record now, and the run is queued to
    // carry on with it.
    let resolved = weft_journal::record::resolve_handed_in(&tx, run_id, &locked, worker.replica).await?;
    match to {
        _ if resolved => {}
        "queued" => weft_journal::record::notify_queued_in(&tx, worker.project).await?,
        // Parked for good, not queued again at once with an answer it was
        // handed: whoever follows the run hears that it now waits (`weft
        // wait --waiting-for-input`), which no record row would tell them.
        _ => notify_parked_in(&tx, worker.project, run_id).await?,
    }
    tx.commit().await?;
    weft_task_store::announce::committed(pool);
    Ok(true)
}

/// Announce on the caller's transaction that `run_id` of `project` parked
/// (`DispatcherEvent::RunParked`), so it is heard only if the park
/// commits. Every dispatcher pushes it to whoever follows the project.
async fn notify_parked_in(db: &impl Client, project: uuid::Uuid, run_id: weft_core::RunId) -> anyhow::Result<()> {
    let parked = weft_core::live_event::DispatcherEvent::RunParked { run_id, project_id: project };
    weft_task_store::runs::announce_event(db, &weft_core::live_event::IdentifiedEvent::transient(parked)).await
}
