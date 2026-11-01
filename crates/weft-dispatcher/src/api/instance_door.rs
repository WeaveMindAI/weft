//! The instance door: what an instance token reaches inside its one
//! instance of a program
// SYNC: the `/instance` door <-> packages/weft-connect/src/server/passthrough.ts PASSED_DOORS
//! (`Authorization: Bearer wft-...`), from a browser or the browser
//! extension, through the connect library (`packages/weft-connect`).
//!
//! An instance token names one instance of one project and nothing else, so
//! everything here is that instance's, in that project: the fields the
//! program writes `@instance_filled` and what was given for each (a
//! connection is one such value), the lists and pickers those fields fill
//! from, its connections (connecting one, forgetting one), and its
//! runs. The author's editor reaches the same store through `/access/*`
//! as the tenant; nothing here ever reaches the author's connections or
//! another instance's.
//!
//! An instance connects its OWN account: the shared door of a paste-less
//! service (the runtime's own key, spending the runtime's credit) is the
//! author's to use, never an instance's, and is refused here.

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use serde::Deserialize;

use weft_core::access::spec::Door;
use weft_core::access::wire::{
    BeginOAuth, CompletedConnect, ConnectDirect, DoorsRequest, DoorsStatus, GrantSummary,
    SharedDoorPick, StartedOAuth,
};
use weft_core::instance::InstanceId;
use weft_core::instance_door::{InstanceField, ValuesChanged};
use weft_core::storage::Tenanted;

use super::access_store_error;
use crate::state::DispatcherState;

type ApiError = (StatusCode, String);

/// Who is at the door: one instance of one project, from its token.
pub(crate) struct InstanceCaller {
    /// The project's owning tenant, which the instance's values and
    /// connections are keyed by (`instance_values::owning_tenant`).
    pub tenant: String,
    pub project: uuid::Uuid,
    pub instance: InstanceId,
}

impl InstanceCaller {
    /// The instance, as a connection's use is scoped to it.
    pub fn scope(&self) -> weft_core::instance::InstanceScope {
        weft_core::instance::InstanceScope { project_id: self.project, instance: self.instance.clone() }
    }
}

/// The instance a request's token acts inside, or why it cannot come in: no
/// token, an unknown or expired one (401), or a token that is not an
/// instance token (403, naming what it is).
pub(crate) async fn instance_caller(state: &DispatcherState, headers: &HeaderMap) -> Result<InstanceCaller, ApiError> {
    let token = crate::api::signal::token_from_bearer(state, headers).await?;
    let Some((project, instance)) = token.instance_scope() else {
        return Err((
            StatusCode::FORBIDDEN,
            "this door takes an instance token (`weft token mint --instance`, or a program's \
             ctx.tokens().mint_for_instance); this one names no instance"
                .into(),
        ));
    };
    let tenant = crate::instance_values::owning_tenant(state, project).await?;
    if token.tenant_id != tenant {
        return Err((
            StatusCode::FORBIDDEN,
            format!("this instance token was minted under tenant '{}', not the one project {project} belongs to", token.tenant_id),
        ));
    }
    Ok(InstanceCaller { tenant, project, instance: instance.clone() })
}

/// `GET /instance/fields`: every field the program asks the instance to fill,
/// in the program's order, with what they gave.
pub async fn fields(State(state): State<DispatcherState>, headers: HeaderMap) -> Result<Json<Vec<InstanceField>>, ApiError> {
    let caller = instance_caller(&state, &headers).await?;
    let project = caller_project(&state, &caller).await?;
    let values = weft_access_store::instance_values(&state.pg_pool, &caller.tenant, caller.project, &caller.instance)
        .await
        .map_err(access_store_error)?;
    let picks = crate::api::project::stored_picks(&state, caller.project).await?;
    let mut out = Vec::new();
    let rules = weft_core::rules::RuleContext::new(&project);
    for weft_core::project::FilledPlace { spelled: place, place: at, node } in weft_core::project::instance_filled_places(&project) {
        for input in &node.inputs {
            let Some(filled) = node.port_literals.get(&input.name).and_then(weft_core::instance::as_instance_filled) else {
                continue;
            };
            let is_connection = matches!(input.widget, Some(weft_core::node::Widget::Access { .. }));
            let connection = match input.widget {
                Some(weft_core::node::Widget::RemoteSelect { .. }) => {
                    weft_core::instance::lookup_connection(&project, &at, &input.name, &values, &picks)
                        .map_err(|why| (StatusCode::CONFLICT, why))?
                        .whose()
                }
                _ => weft_core::instance_door::FieldConnection::None,
            };
            out.push(InstanceField {
                field: input.name.clone(),
                node_type: node.node_type.clone(),
                label: node.label.clone(),
                fallback: filled.fallback.cloned(),
                needed: weft_core::instance::instance_field_needed(&rules, node, &input.name),
                spec: if is_connection { node.instance_service.clone() } else { None },
                connection,
                value: values.get(&place).and_then(|fields| fields.get(&input.name)).cloned(),
                input: input.clone(),
                node: place.clone(),
            });
        }
    }
    Ok(Json(out))
}

/// The program the caller's token is for, as it runs now.
async fn caller_project(state: &DispatcherState, caller: &InstanceCaller) -> Result<std::sync::Arc<weft_core::ProjectDefinition>, ApiError> {
    state
        .registered(caller.project)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e:#}")))?
        .map(|registered| registered.program)
        .ok_or((StatusCode::NOT_FOUND, "this token's project no longer exists".to_string()))
}

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    pub service: Option<String>,
}

/// `GET /instance/connections?service=`: the instance's connections.
pub async fn list_connections(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Query(q): Query<ListQuery>,
) -> Result<Json<Vec<GrantSummary>>, ApiError> {
    let caller = instance_caller(&state, &headers).await?;
    weft_access_store::list_grants(
        &state.pg_pool,
        &caller.tenant,
        q.service.as_deref(),
        weft_access_store::GrantOwnerScope::Instance { project_id: caller.project, instance: &caller.instance },
    )
    .await
    .map(Json)
    .map_err(access_store_error)
}

/// `DELETE /instance/connections/{id}`: forget one of the instance's
/// connections; their picks of it go with it.
pub async fn delete_connection(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Path(id): Path<uuid::Uuid>,
) -> Result<StatusCode, ApiError> {
    let caller = instance_caller(&state, &headers).await?;
    weft_access_store::delete_grant(
        &state.pg_pool,
        &caller.tenant,
        id,
        weft_access_store::GrantOwnerScope::Instance { project_id: caller.project, instance: &caller.instance },
    )
    .await
    .map_err(access_store_error)?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /instance/doors`: which doors a connect page offers for a
/// service, as `/access/doors` answers the editor.
pub async fn doors(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Json(req): Json<DoorsRequest>,
) -> Result<Json<DoorsStatus>, ApiError> {
    instance_caller(&state, &headers).await?;
    crate::api::access::doors_status(&state, &req).await.map(Json)
}

/// `POST /instance/connections/direct`: a paste connect for the instance,
/// stored as theirs in the token's project.
pub async fn connect_direct(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Json(mut req): Json<SharedDoorPick<ConnectDirect>>,
) -> Result<Json<CompletedConnect>, ApiError> {
    let caller = instance_caller(&state, &headers).await?;
    if req.inner.door == Door::Shared {
        return Err((
            StatusCode::FORBIDDEN,
            "an instance connects its own account; the shared key is the program author's".into(),
        ));
    }
    req.inner.project_id = Some(caller.project);
    req.inner.instance = Some(caller.instance);
    crate::broker_admin::forward_json(
        &state,
        weft_broker_client::protocol::ADMIN_CONNECT_DIRECT,
        &Tenanted { tenant: caller.tenant, inner: req },
    )
    .await
    .map(Json)
}

/// `POST /instance/connections/begin`: start a browser consent for the
/// instance, whose result lands as theirs in the token's project.
pub async fn connect_begin(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Json(mut req): Json<SharedDoorPick<BeginOAuth>>,
) -> Result<Json<StartedOAuth>, ApiError> {
    let caller = instance_caller(&state, &headers).await?;
    req.inner.redirect_uri = crate::api::access::redirect_uri(&state, &req.inner.spec).await?;
    req.inner.project_id = Some(caller.project);
    req.inner.instance = Some(caller.instance);
    crate::broker_admin::forward_json(
        &state,
        weft_broker_client::protocol::ADMIN_OAUTH_BEGIN,
        &Tenanted { tenant: caller.tenant, inner: req },
    )
    .await
    .map(Json)
}

#[derive(Debug, Deserialize)]
pub struct StatusQuery {
    pub state: String,
}

/// `GET /instance/connections/status?state=`: the connect page's poll after
/// opening a consent or a chooser. `null` while it is live, the outcome
/// once, then 410 (as for a state that is unknown or expired).
pub async fn connect_status(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Query(q): Query<StatusQuery>,
) -> Result<Json<Option<serde_json::Value>>, ApiError> {
    let caller = instance_caller(&state, &headers).await?;
    weft_access_store::take_connect_result(&state.pg_pool, &caller.tenant, &q.state)
        .await
        .map(Json)
        .map_err(access_store_error)
}

/// `PUT /instance/values`: give values for fields, and clear others, all at
/// once. Refused naming every value the program refuses; a live trigger
/// of the instance that reads one is set up again with the new values
/// before the answer (see `crate::instance_values`), and the answer names
/// it.
pub async fn set_values(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Json(req): Json<weft_core::instance_door::ValuesRequest>,
) -> Result<Json<ValuesChanged>, ApiError> {
    let caller = instance_caller(&state, &headers).await?;
    let clear: Vec<(String, String)> = req.clear.into_iter().map(|f| (f.node, f.field)).collect();
    crate::instance_values::change(&state, &caller.tenant, caller.project, &caller.instance, &req.set, &clear)
        .await
        .map(Json)
}

/// Which list, of which field, to look up.
// SYNC: LookupRequest <-> packages/weft-connect/src/core/wire.ts InstanceLookupRequest
#[derive(Debug, Deserialize)]
pub struct LookupRequest {
    pub node: String,
    pub field: String,
    /// Which of the field widget's `sources`, by position: a `list` or a
    /// `granted` one.
    pub source: usize,
    #[serde(default)]
    pub query: String,
    /// The picked values of the fields this one depends on.
    #[serde(default)]
    pub parents: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub cursor: Option<String>,
}

/// `POST /instance/lookup`: a page of the options a field's list offers,
/// read through the connection the instance's run would use there (see
/// `weft_core::instance::lookup_connection`). The instance names only the
/// field and which of its sources; the address called and the
/// connection signing it come from the program, so an instance token makes
/// no call the program does not declare.
pub async fn lookup(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Json(req): Json<LookupRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let caller = instance_caller(&state, &headers).await?;
    let (source, connection) = field_source(&state, &caller, &req.node, &req.field, req.source).await?;
    match source {
        weft_core::node::ResourceSource::List { lookup, .. } => {
            let access_id = if lookup.public { None } else { Some(signing(&connection, &req.node, &req.field)?.id) };
            let request = weft_access_store::LookupRequest {
                for_instance: Some(caller.scope()),
                access_id,
                service: connection.map(|wired| wired.service),
                lookup,
                query: req.query,
                parents: req.parents,
                cursor: req.cursor,
            };
            crate::broker_admin::forward_json(&state, weft_broker_client::protocol::ADMIN_LOOKUP, &Tenanted { tenant: caller.tenant, inner: request })
                .await
                .map(Json)
        }
        weft_core::node::ResourceSource::Granted { from, label, value } => {
            let weft_core::instance::WiredConnection { id: access_id, service, .. } =
                own_signing(&connection, &req.node, &req.field)?;
            let request =
                weft_access_store::GrantedQuery { for_instance: Some(caller.scope()), access_id, service, from, label, value };
            crate::broker_admin::forward_json(&state, weft_broker_client::protocol::ADMIN_GRANTED, &Tenanted { tenant: caller.tenant, inner: request })
                .await
                .map(Json)
        }
        _ => Err((
            StatusCode::BAD_REQUEST,
            format!("source {} of '{}.{}' is not a list to look up", req.source, req.node, req.field),
        )),
    }
}

/// Which field's picker to open.
// SYNC: PickerRequest <-> packages/weft-connect/src/core/wire.ts InstancePickerRequest
#[derive(Debug, Deserialize)]
pub struct PickerRequest {
    pub node: String,
    pub field: String,
    /// Which of the field widget's `sources`: a `picker` one.
    pub source: usize,
}

/// `POST /instance/picker`: open a field's provider chooser for the
/// instance, signed with the connection its run would use there. Answers
/// the page to open and the state to poll at
/// `/instance/connections/status`, exactly as the editor's picker does.
pub async fn picker(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Json(req): Json<PickerRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let caller = instance_caller(&state, &headers).await?;
    let (source, connection) = field_source(&state, &caller, &req.node, &req.field, req.source).await?;
    let weft_core::node::ResourceSource::Picker { script, code, grants, mime_types } = source else {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("source {} of '{}.{}' is not a picker", req.source, req.node, req.field),
        ));
    };
    let weft_core::instance::WiredConnection { id: access_id, service, .. } = own_signing(&connection, &req.node, &req.field)?;
    let base = crate::api::request_base(&headers)?;
    let picker_state = weft_access_store::begin_picker(
        &state.pg_pool,
        &caller.tenant,
        weft_access_store::BeginPicker { for_instance: Some(caller.scope()), access_id, service, script, code, mime_types, grants },
    )
    .await
    .map_err(access_store_error)?;
    let url = format!("{base}/access/picker/{picker_state}");
    Ok(Json(serde_json::json!({ "state": picker_state, "url": url })))
}

/// The source at position `index` of the widget of `field` at `place`, a
/// field the instance fills, and the connection the instance's run signs it
/// with there.
async fn field_source(
    state: &DispatcherState,
    caller: &InstanceCaller,
    place: &str,
    field: &str,
    index: usize,
) -> Result<(weft_core::node::ResourceSource, Option<weft_core::instance::WiredConnection>), ApiError> {
    let project = caller_project(state, caller).await?;
    let Some(filled) = weft_core::project::instance_filled_places(&project).into_iter().find(|filled| filled.spelled == place) else {
        return Err((StatusCode::NOT_FOUND, format!("'{place}' is no node with a field you fill")));
    };
    let node = filled.node;
    if !weft_core::instance::instance_filled_fields(node).any(|(filled, _)| filled == field) {
        return Err((StatusCode::NOT_FOUND, format!("'{place}.{field}' is no field you fill")));
    }
    let Some(weft_core::node::Widget::RemoteSelect { sources, .. }) =
        node.inputs.iter().find(|i| i.name == field).and_then(|i| i.widget.as_ref())
    else {
        return Err((StatusCode::BAD_REQUEST, format!("'{place}.{field}' has no list to look up")));
    };
    let source = sources
        .get(index)
        .cloned()
        .ok_or_else(|| (StatusCode::BAD_REQUEST, format!("'{place}.{field}' has no source {index}")))?;
    let values = weft_access_store::instance_values(&state.pg_pool, &caller.tenant, caller.project, &caller.instance)
        .await
        .map_err(access_store_error)?;
    let picks = crate::api::project::stored_picks(state, caller.project).await?;
    let connection = weft_core::instance::lookup_connection(&project, &filled.place, field, &values, &picks)
        .and_then(weft_core::instance::LookupConnection::signing)
        .map_err(|why| (StatusCode::CONFLICT, why))?;
    Ok((source, connection))
}

/// The connection a signed source needs, or why there is none.
fn signing(
    connection: &Option<weft_core::instance::WiredConnection>,
    node: &str,
    field: &str,
) -> Result<weft_core::instance::WiredConnection, ApiError> {
    connection.clone().ok_or_else(|| {
        (StatusCode::CONFLICT, format!("'{node}.{field}' reads its list through a connection, and none is wired to it"))
    })
}

/// The instance's OWN connection a `picker` or `granted` source needs:
/// both hand the connection to the instance's browser, so the author's
/// shared one never signs them (see `FieldConnection`).
fn own_signing(
    connection: &Option<weft_core::instance::WiredConnection>,
    node: &str,
    field: &str,
) -> Result<weft_core::instance::WiredConnection, ApiError> {
    let wired = signing(connection, node, field)?;
    if wired.whose != weft_core::instance_door::FieldConnection::Own {
        return Err((
            StatusCode::FORBIDDEN,
            format!("'{node}.{field}' reads through the program author's connection; this source only runs on your own"),
        ));
    }
    Ok(wired)
}

#[derive(Debug, Deserialize)]
pub struct RunsQuery {
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub status: Option<weft_core::program::RunStatus>,
}

/// `GET /instance/runs`: the instance's own runs in the token's project,
/// newest first, each failed one with the plain failure in place of why
/// it failed ([`plain_failures`]).
pub async fn runs(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Query(q): Query<RunsQuery>,
) -> Result<Json<weft_core::program::RunPage>, ApiError> {
    let caller = instance_caller(&state, &headers).await?;
    let query = crate::store::RunListQuery {
        limit: q.limit.unwrap_or(50).clamp(1, 200),
        offset: q.offset.unwrap_or(0),
        project_id: Some(caller.project),
        status: q.status,
        instance: Some(caller.instance),
        ..Default::default()
    };
    state
        .runs
        .list_runs(&caller.tenant, &query)
        .await
        .map(|page| Json(plain_failures(page)))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("runs: {e:#}")))
}

/// An instance token is a visitor of the program, never its owner: a
/// failed run reads as the plain failure (`weft_core::caller::PLAIN_FAILURE`),
/// its node paths and a provider's words left to the owner, who opens it
/// by the run id the summary already carries.
fn plain_failures(mut page: weft_core::program::RunPage) -> weft_core::program::RunPage {
    for run in &mut page.runs {
        if run.error.is_some() {
            run.error = Some(weft_core::caller::PLAIN_FAILURE.to_string());
        }
    }
    page
}

#[cfg(test)]
mod tests {
    use super::plain_failures;
    use weft_core::program::{RunPage, RunStatus, RunSummary};

    fn run(status: RunStatus, error: Option<&str>) -> RunSummary {
        RunSummary {
            run_id: uuid::Uuid::new_v4(),
            project_id: uuid::Uuid::nil(),
            entry_node: "route".into(),
            status,
            phase: weft_core::primitive::Phase::Fire,
            started_at: 1,
            completed_at: Some(2),
            tags: Vec::new(),
            cancel_cause: None,
            error: error.map(str::to_string),
            skipped_nodes: 0,
            instance: None,
        }
    }

    /// A failed run reads as the plain failure, keeping its id; a run
    /// that did not fail keeps no error.
    #[test]
    fn an_instance_reads_failures_plain() {
        let failed = run(RunStatus::Failed, Some("route.work.ingest: ffprobe failed probing /data/src.mp4"));
        let id = failed.run_id;
        let page = plain_failures(RunPage { runs: vec![failed, run(RunStatus::Completed, None)], total: 2 });
        assert_eq!(page.runs[0].error.as_deref(), Some("Something went wrong"));
        assert_eq!(page.runs[0].run_id, id);
        assert_eq!(page.runs[1].error, None);
    }
}
