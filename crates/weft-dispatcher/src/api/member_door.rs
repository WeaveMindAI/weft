//! The member door: what a member of a program reaches with their member
// SYNC: the `/member` door <-> packages/weft-connect/src/server/passthrough.ts PASSED_DOORS
//! token (`Authorization: Bearer wft-...`), from a browser or the browser
//! extension, through the connect library (`packages/weft-connect`).
//!
//! A member token names one member of one project and nothing else, so
//! everything here is that member's, in that project: the fields their
//! program writes `@member_filled` and what they gave for each (a
//! connection is one such value), the lists and pickers those fields fill
//! from, their connections (connecting one, forgetting one), and their
//! runs. The author's editor reaches the same store through `/access/*`
//! as the tenant; nothing here ever reaches the author's connections or
//! another member's.
//!
//! A member connects their OWN account: the shared door of a paste-less
//! service (the runtime's own key, spending the runtime's credit) is the
//! author's to use, never a member's, and is refused here.

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use serde::Deserialize;

use weft_core::access::spec::Door;
use weft_core::access::wire::{
    BeginOAuth, CompletedConnect, ConnectDirect, DoorsRequest, DoorsStatus, GrantSummary,
    SharedDoorPick, StartedOAuth,
};
use weft_core::member::MemberId;
use weft_core::member_door::{MemberField, ValuesChanged};
use weft_core::storage::Tenanted;

use crate::state::DispatcherState;

type ApiError = (StatusCode, String);

/// Who is at the door: one member of one project, from their token.
pub(crate) struct MemberCaller {
    /// The project's owning tenant, which the member's values and
    /// connections are keyed by (`member_values::owning_tenant`).
    pub tenant: String,
    pub project: uuid::Uuid,
    pub member: MemberId,
}

impl MemberCaller {
    /// The member, as a connection's use is scoped to them.
    pub fn scope(&self) -> weft_core::member::MemberScope {
        weft_core::member::MemberScope { project_id: self.project, member: self.member.clone() }
    }
}

/// The member a request's token acts as, or why it cannot come in: no
/// token, an unknown or expired one (401), or a token that is not a
/// member token (403, naming what it is).
pub(crate) async fn member_caller(state: &DispatcherState, headers: &HeaderMap) -> Result<MemberCaller, ApiError> {
    let token = crate::api::signal::token_from_bearer(state, headers).await?;
    let Some((project, member)) = token.member_scope() else {
        return Err((
            StatusCode::FORBIDDEN,
            "this door takes a member token (`weft token mint --member`, or a program's \
             ctx.tokens().mint_for_member); this one acts as nobody in particular"
                .into(),
        ));
    };
    let tenant = crate::member_values::owning_tenant(state, project).await?;
    if token.tenant_id != tenant {
        return Err((
            StatusCode::FORBIDDEN,
            format!("this member token was minted under tenant '{}', not the one project {project} belongs to", token.tenant_id),
        ));
    }
    Ok(MemberCaller { tenant, project, member: member.clone() })
}

/// `GET /member/fields`: every field the program asks the member to fill,
/// in the program's order, with what they gave.
pub async fn fields(State(state): State<DispatcherState>, headers: HeaderMap) -> Result<Json<Vec<MemberField>>, ApiError> {
    let caller = member_caller(&state, &headers).await?;
    let project = caller_project(&state, &caller).await?;
    let values = weft_access_store::member_values(&state.pg_pool, &caller.tenant, caller.project, &caller.member)
        .await
        .map_err(access_err)?;
    let picks = crate::api::project::stored_picks(&state, caller.project).await?;
    let mut out = Vec::new();
    for (step, node) in weft_core::project::member_filled_places(&project) {
        let (id, path) = weft_core::project::resolve_address(&project, &step);
        let at = weft_core::frames::Located::new(id, path);
        for input in &node.inputs {
            let Some(filled) = node.port_literals.get(&input.name).and_then(weft_core::member::as_member_filled) else {
                continue;
            };
            let is_connection = matches!(input.widget, Some(weft_core::node::Widget::Access { .. }));
            let connection = match input.widget {
                Some(weft_core::node::Widget::RemoteSelect { .. }) => {
                    weft_core::member::lookup_connection(&project, &at, &input.name, &values, &picks)
                        .map_err(|why| (StatusCode::CONFLICT, why))?
                        .whose()
                }
                _ => weft_core::member_door::FieldConnection::None,
            };
            out.push(MemberField {
                field: input.name.clone(),
                node_type: node.node_type.clone(),
                label: node.label.clone(),
                fallback: filled.fallback.cloned(),
                needed: weft_core::member::member_field_needed(&project, node, &input.name),
                spec: if is_connection { node.member_service.clone() } else { None },
                connection,
                value: values.get(&step).and_then(|fields| fields.get(&input.name)).cloned(),
                input: input.clone(),
                step: step.clone(),
            });
        }
    }
    Ok(Json(out))
}

/// The program the caller's token is for, as it runs now.
async fn caller_project(state: &DispatcherState, caller: &MemberCaller) -> Result<weft_core::ProjectDefinition, ApiError> {
    state
        .projects
        .project(caller.project)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "this token's project no longer exists".to_string()))
}

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    pub service: Option<String>,
}

/// `GET /member/connections?service=`: the member's connections.
pub async fn list_connections(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Query(q): Query<ListQuery>,
) -> Result<Json<Vec<GrantSummary>>, ApiError> {
    let caller = member_caller(&state, &headers).await?;
    weft_access_store::list_grants(
        &state.pg_pool,
        &caller.tenant,
        q.service.as_deref(),
        weft_access_store::GrantOwnerScope::Member { project_id: caller.project, member: &caller.member },
    )
    .await
    .map(Json)
    .map_err(access_err)
}

/// `DELETE /member/connections/{id}`: forget one of the member's
/// connections; their picks of it go with it.
pub async fn delete_connection(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Path(id): Path<uuid::Uuid>,
) -> Result<StatusCode, ApiError> {
    let caller = member_caller(&state, &headers).await?;
    weft_access_store::delete_grant(
        &state.pg_pool,
        &caller.tenant,
        id,
        weft_access_store::GrantOwnerScope::Member { project_id: caller.project, member: &caller.member },
    )
    .await
    .map_err(access_err)?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /member/doors`: which doors a connect page offers for a
/// service, as `/access/doors` answers the editor.
pub async fn doors(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Json(req): Json<DoorsRequest>,
) -> Result<Json<DoorsStatus>, ApiError> {
    member_caller(&state, &headers).await?;
    crate::api::access::doors_status(&state, &req).await.map(Json)
}

/// `POST /member/connections/direct`: a paste connect for the member,
/// stored as theirs in the token's project.
pub async fn connect_direct(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Json(mut req): Json<SharedDoorPick<ConnectDirect>>,
) -> Result<Json<CompletedConnect>, ApiError> {
    let caller = member_caller(&state, &headers).await?;
    if req.inner.door == Door::Shared {
        return Err((
            StatusCode::FORBIDDEN,
            "a member connects their own account; the shared key is the program author's".into(),
        ));
    }
    req.inner.project_id = Some(caller.project);
    req.inner.member = Some(caller.member);
    crate::broker_admin::forward_json(
        &state,
        "/v1/access/admin/connect/direct",
        &Tenanted { tenant: caller.tenant, inner: req },
    )
    .await
    .map(Json)
}

/// `POST /member/connections/begin`: start a browser consent for the
/// member, whose result lands as theirs in the token's project.
pub async fn connect_begin(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Json(mut req): Json<SharedDoorPick<BeginOAuth>>,
) -> Result<Json<StartedOAuth>, ApiError> {
    let caller = member_caller(&state, &headers).await?;
    req.inner.redirect_uri = crate::api::access::redirect_uri(&state, &req.inner.spec).await?;
    req.inner.project_id = Some(caller.project);
    req.inner.member = Some(caller.member);
    crate::broker_admin::forward_json(
        &state,
        "/v1/access/admin/oauth/begin",
        &Tenanted { tenant: caller.tenant, inner: req },
    )
    .await
    .map(Json)
}

#[derive(Debug, Deserialize)]
pub struct StatusQuery {
    pub state: String,
}

/// `GET /member/connections/status?state=`: the connect page's poll after
/// opening a consent or a chooser. `null` while it is live, the outcome
/// once, then 410 (as for a state that is unknown or expired).
pub async fn connect_status(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Query(q): Query<StatusQuery>,
) -> Result<Json<Option<serde_json::Value>>, ApiError> {
    let caller = member_caller(&state, &headers).await?;
    weft_access_store::take_connect_result(&state.pg_pool, &caller.tenant, &q.state)
        .await
        .map(Json)
        .map_err(access_err)
}

/// A change to the member's values.
// SYNC: ValuesRequest <-> packages/weft-connect/src/core/wire.ts ValuesRequest
#[derive(Debug, Deserialize)]
pub struct ValuesRequest {
    #[serde(default)]
    pub set: Vec<weft_core::run_spec::MemberValueInput>,
    #[serde(default)]
    pub clear: Vec<weft_core::run_spec::MemberFieldRef>,
}

/// `PUT /member/values`: give values for fields, and clear others, all at
/// once. Refused naming every value the program refuses; a live trigger
/// of the member that reads one is set up again with the new values
/// before the answer (see `crate::member_values`), and the answer names
/// it.
pub async fn set_values(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Json(req): Json<ValuesRequest>,
) -> Result<Json<ValuesChanged>, ApiError> {
    let caller = member_caller(&state, &headers).await?;
    let clear: Vec<(String, String)> = req.clear.into_iter().map(|f| (f.step, f.field)).collect();
    crate::member_values::change(&state, &caller.tenant, caller.project, &caller.member, &req.set, &clear)
        .await
        .map(Json)
}

/// Which list, of which field, to look up.
// SYNC: LookupRequest <-> packages/weft-connect/src/core/wire.ts MemberLookupRequest
#[derive(Debug, Deserialize)]
pub struct LookupRequest {
    pub step: String,
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

/// `POST /member/lookup`: a page of the options a field's list offers,
/// read through the connection the member's run would use there (see
/// `weft_core::member::lookup_connection`). The member names only the
/// field and which of its sources; the address called and the
/// connection signing it come from the program, so a member token makes
/// no call the program does not declare.
pub async fn lookup(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Json(req): Json<LookupRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let caller = member_caller(&state, &headers).await?;
    let (source, connection) = field_source(&state, &caller, &req.step, &req.field, req.source).await?;
    match source {
        weft_core::node::ResourceSource::List { lookup, .. } => {
            let access_id = if lookup.public { None } else { Some(signing(&connection, &req.step, &req.field)?.id) };
            let request = weft_access_store::LookupRequest {
                for_member: Some(caller.scope()),
                access_id,
                service: connection.map(|wired| wired.service),
                lookup,
                query: req.query,
                parents: req.parents,
                cursor: req.cursor,
            };
            crate::broker_admin::forward_json(&state, "/v1/access/admin/lookup", &Tenanted { tenant: caller.tenant, inner: request })
                .await
                .map(Json)
        }
        weft_core::node::ResourceSource::Granted { from, label, value } => {
            let weft_core::member::WiredConnection { id: access_id, service, .. } =
                own_signing(&connection, &req.step, &req.field)?;
            let request =
                weft_access_store::GrantedQuery { for_member: Some(caller.scope()), access_id, service, from, label, value };
            crate::broker_admin::forward_json(&state, "/v1/access/admin/granted", &Tenanted { tenant: caller.tenant, inner: request })
                .await
                .map(Json)
        }
        _ => Err((
            StatusCode::BAD_REQUEST,
            format!("source {} of '{}.{}' is not a list to look up", req.source, req.step, req.field),
        )),
    }
}

/// Which field's picker to open.
// SYNC: PickerRequest <-> packages/weft-connect/src/core/wire.ts MemberPickerRequest
#[derive(Debug, Deserialize)]
pub struct PickerRequest {
    pub step: String,
    pub field: String,
    /// Which of the field widget's `sources`: a `picker` one.
    pub source: usize,
}

/// `POST /member/picker`: open a field's provider chooser for the
/// member, signed with the connection their run would use there. Answers
/// the page to open and the state to poll at
/// `/member/connections/status`, exactly as the editor's picker does.
pub async fn picker(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Json(req): Json<PickerRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let caller = member_caller(&state, &headers).await?;
    let (source, connection) = field_source(&state, &caller, &req.step, &req.field, req.source).await?;
    let weft_core::node::ResourceSource::Picker { script, code, grants, mime_types } = source else {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("source {} of '{}.{}' is not a picker", req.source, req.step, req.field),
        ));
    };
    let weft_core::member::WiredConnection { id: access_id, service, .. } = own_signing(&connection, &req.step, &req.field)?;
    let base = crate::storage::LinkBase::for_request(&headers).map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    let picker_state = weft_access_store::begin_picker(
        &state.pg_pool,
        &caller.tenant,
        weft_access_store::BeginPicker { for_member: Some(caller.scope()), access_id, service, script, code, mime_types, grants },
    )
    .await
    .map_err(access_err)?;
    let url = format!("{}/access/picker/{picker_state}", base.as_str());
    Ok(Json(serde_json::json!({ "state": picker_state, "url": url })))
}

/// The source at position `index` of the widget of `field` at `step`, a
/// field the member fills, and the connection the member's run signs it
/// with there.
async fn field_source(
    state: &DispatcherState,
    caller: &MemberCaller,
    step: &str,
    field: &str,
    index: usize,
) -> Result<(weft_core::node::ResourceSource, Option<weft_core::member::WiredConnection>), ApiError> {
    let project = caller_project(state, caller).await?;
    let Some((_, node)) = weft_core::project::member_filled_places(&project).into_iter().find(|(place, _)| place == step) else {
        return Err((StatusCode::NOT_FOUND, format!("'{step}' is no step with a field you fill")));
    };
    if !weft_core::member::member_filled_fields(node).any(|(filled, _)| filled == field) {
        return Err((StatusCode::NOT_FOUND, format!("'{step}.{field}' is no field you fill")));
    }
    let Some(weft_core::node::Widget::RemoteSelect { sources, .. }) =
        node.inputs.iter().find(|i| i.name == field).and_then(|i| i.widget.as_ref())
    else {
        return Err((StatusCode::BAD_REQUEST, format!("'{step}.{field}' has no list to look up")));
    };
    let source = sources
        .get(index)
        .cloned()
        .ok_or_else(|| (StatusCode::BAD_REQUEST, format!("'{step}.{field}' has no source {index}")))?;
    let values = weft_access_store::member_values(&state.pg_pool, &caller.tenant, caller.project, &caller.member)
        .await
        .map_err(access_err)?;
    let picks = crate::api::project::stored_picks(state, caller.project).await?;
    let (id, path) = weft_core::project::resolve_address(&project, step);
    let connection = weft_core::member::lookup_connection(&project, &weft_core::frames::Located::new(id, path), field, &values, &picks)
        .and_then(weft_core::member::LookupConnection::signing)
        .map_err(|why| (StatusCode::CONFLICT, why))?;
    Ok((source, connection))
}

/// The connection a signed source needs, or why there is none.
fn signing(
    connection: &Option<weft_core::member::WiredConnection>,
    step: &str,
    field: &str,
) -> Result<weft_core::member::WiredConnection, ApiError> {
    connection.clone().ok_or_else(|| {
        (StatusCode::CONFLICT, format!("'{step}.{field}' reads its list through a connection, and none is wired to it"))
    })
}

/// The member's OWN connection a `picker` or `granted` source needs:
/// both hand the connection to the member's browser, so the author's
/// shared one never signs them (see `FieldConnection`).
fn own_signing(
    connection: &Option<weft_core::member::WiredConnection>,
    step: &str,
    field: &str,
) -> Result<weft_core::member::WiredConnection, ApiError> {
    let wired = signing(connection, step, field)?;
    if wired.whose != weft_core::member_door::FieldConnection::Own {
        return Err((
            StatusCode::FORBIDDEN,
            format!("'{step}.{field}' reads through the program author's connection; this source only runs on your own"),
        ));
    }
    Ok(wired)
}

#[derive(Debug, Deserialize)]
pub struct RunsQuery {
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub status: Option<String>,
}

/// `GET /member/runs`: the member's own runs in the token's project,
/// newest first.
pub async fn runs(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Query(q): Query<RunsQuery>,
) -> Result<Json<crate::journal::ExecutionPage>, ApiError> {
    let caller = member_caller(&state, &headers).await?;
    let query = crate::journal::ExecutionQuery {
        limit: q.limit.unwrap_or(50).clamp(1, 200),
        offset: q.offset.unwrap_or(0),
        project_id: Some(caller.project),
        status: q.status,
        member: Some(caller.member),
        ..Default::default()
    };
    state
        .journal
        .list_executions(&caller.tenant, &query)
        .await
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("runs: {e:#}")))
}

fn access_err(e: anyhow::Error) -> ApiError {
    let (status, message) = weft_access_store::client_error(e);
    (StatusCode::from_u16(status).expect("store status codes are valid"), message)
}
