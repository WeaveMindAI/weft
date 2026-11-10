//! The program door: the connections a program's own access nodes use on
//! this install (its picks, `weft_core::picks`), managed with a
//! connections key (`weft token mint --connections` in the project's folder,
//! `Authorization: Bearer wft-...`). What a website's admin page uses to
//! let its admin paste the program's keys, through the site's own server,
//! which holds the key (`packages/weft-connect`, `weftProgramPassThrough`).
// SYNC: the `/program` door <-> packages/weft-connect/src/server/passthrough.ts weftProgramPassThrough
//!
//! A connections key names one project and reaches nothing but this door:
//! it lists the program's picked connection fields and what each holds
//! (a handle, never a secret), stores a pasted key as one of the program
//! owner's connections and picks it, and clears a pick. A change goes
//! through `crate::install_picks` exactly as `weft connect` does,
//! re-arming the triggers that read it, and forgets the connection it
//! moves a pick off once nothing on the install uses it. A browser
//! consent is not offered here: an account signed in through a provider's
//! page is connected from the editor or with `weft connect`.
//!
//! The key never reaches a run: nothing here is a program call, and
//! whatever a run is handed is written to its journal in the clear.

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;

use weft_core::access::wire::{CompletedConnect, DoorsRequest, DoorsStatus, SharedDoorPick};
use weft_core::instance_door::{FilledField, ProgramConnectDirect, ProgramConnected, ValuesChanged};
use weft_core::picks::{PickInput, Picks};
use weft_core::project::{NodeDefinition, ProjectDefinition};

use super::instance_door::{field_entry, store_direct, token_program, ConnectionOwner};
use crate::state::DispatcherState;

type ApiError = (StatusCode, String);

/// Who is at the door: the program of one project, from its connections
/// key.
struct ProgramCaller {
    /// The project's owning tenant, whose connections the program's are.
    tenant: String,
    project: uuid::Uuid,
}

/// The project a request's connections key manages, or why it cannot come
/// in: no token, an unknown or expired one (401), or a token that is no
/// connections key (403, naming what the door takes).
async fn program_caller(state: &DispatcherState, headers: &HeaderMap) -> Result<ProgramCaller, ApiError> {
    let token = crate::api::signal::connections_key_from_bearer(state, headers).await?;
    let project = token.connections_project().map_err(|why| (StatusCode::FORBIDDEN, why))?;
    let tenant = crate::instance_values::owning_tenant(state, project).await?;
    if token.tenant_id != tenant {
        return Err((
            StatusCode::FORBIDDEN,
            format!("this connections key was minted under tenant '{}', not the one project {project} belongs to", token.tenant_id),
        ));
    }
    Ok(ProgramCaller { tenant, project })
}

/// `GET /program/connections`: every connection field of the program
/// picked on the install, in the program's order, each with what is
/// picked there.
pub async fn connections(State(state): State<DispatcherState>, headers: HeaderMap) -> Result<Json<Vec<FilledField>>, ApiError> {
    let caller = program_caller(&state, &headers).await?;
    let program = token_program(&state, caller.project).await?;
    let picks = crate::api::project::stored_picks(&state, caller.project).await?;
    Ok(Json(program_fields(&program, &picks)))
}

/// The connection fields of `program` picked on the install, as the door
/// lists them: `value` is the picked connection's handle (`{id,
/// identity}`, never a secret), `needed` whether a run is refused without
/// one (the field's widget says whether it may stay empty).
fn program_fields(program: &ProjectDefinition, picks: &Picks) -> Vec<FilledField> {
    let mut out = Vec::new();
    for (place, node) in weft_core::picks::picked_places(program) {
        for field in weft_core::picks::picked_fields(node) {
            let Some(input) = node.inputs.iter().find(|i| i.name == field) else { continue };
            out.push(FilledField {
                needed: !weft_core::picks::may_stay_unpicked(node, field),
                value: picks.get(&place).and_then(|fields| fields.get(field)).cloned(),
                ..field_entry(&place, node, input)
            });
        }
    }
    out
}

/// The node at `place` of `program` and the service its field `field`
/// connects to, when that field is a connection the program picks on the
/// install; else why this door cannot fill it.
fn picked_field<'a>(program: &'a ProjectDefinition, place: &str, field: &str) -> Result<(&'a NodeDefinition, String), ApiError> {
    let not_here = || (StatusCode::NOT_FOUND, format!("'{place}.{field}' is no connection the program picks on this install"));
    let (_, node) = weft_core::picks::picked_places(program).into_iter().find(|(spelled, _)| spelled == place).ok_or_else(not_here)?;
    if !weft_core::picks::picked_fields(node).any(|f| f == field) {
        return Err(not_here());
    }
    match weft_core::picks::service_of(node, field) {
        Some(service) => Ok((node, service.to_string())),
        None => Err((StatusCode::CONFLICT, format!("'{place}.{field}' names no service in the program this install built"))),
    }
}

/// Why a connect page at the program door offers no browser sign-in.
const NO_CONSENT_HERE: &str = "Signing in through the provider's own page is done from the weft editor's Connect button \
     or with `weft connect`; here, paste a key.";

/// `POST /program/doors`: which doors a connect page offers for a service,
/// as `/access/doors` answers the editor, with every browser sign-in
/// blocked: this door stores pasted keys alone.
pub async fn doors(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Json(req): Json<DoorsRequest>,
) -> Result<Json<DoorsStatus>, ApiError> {
    program_caller(&state, &headers).await?;
    let status = crate::api::access::doors_status(&state, &req).await?;
    Ok(Json(DoorsStatus { redirect_uri: None, consent_blocked: Some(NO_CONSENT_HERE.into()), ..status }))
}

/// `POST /program/connections/direct`: store a pasted key for one
/// connection field of the program as one of the owner's connections,
/// and pick it there, all or none: a pick that is refused forgets the
/// connection it just stored. The recipe is the program's own (the sent
/// one must name the field's service), so the key stores nothing the
/// program does not connect to.
pub async fn connect_direct(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Json(req): Json<ProgramConnectDirect>,
) -> Result<Json<ProgramConnected>, ApiError> {
    let caller = program_caller(&state, &headers).await?;
    let program = token_program(&state, caller.project).await?;
    let ProgramConnectDirect { node: place, field, mut connect } = req;
    let (node, service) = picked_field(&program, &place, &field)?;
    connect.spec = paste_recipe(node, &place, &field, &service, &connect)?;
    let CompletedConnect { grant } = store_direct(
        &state,
        caller.tenant,
        ConnectionOwner::Program(caller.project),
        SharedDoorPick { shared_app: None, inner: connect },
    )
    .await?;
    let pick = PickInput { node: place, field, connection: grant.id, service };
    let changed = crate::install_picks::change_forgetting(&state, caller.project, &[pick], &[], Some(grant.id)).await?;
    Ok(Json(ProgramConnected { grant, changed }))
}

/// The recipe a pasted connect for `field` of `node` (at `place`) runs on:
/// the program's own, carried on the node, once the connect is one this
/// door runs (the sent recipe names the field's service, and the service
/// takes a pasted key).
fn paste_recipe(
    node: &NodeDefinition,
    place: &str,
    field: &str,
    service: &str,
    connect: &weft_core::access::wire::ConnectDirect,
) -> Result<weft_core::AccessSpec, ApiError> {
    if connect.spec.service != service {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("'{place}.{field}' connects to '{service}', and this connect is for '{}'", connect.spec.service),
        ));
    }
    let spec = node.connection_service.clone().ok_or_else(|| {
        (
            StatusCode::CONFLICT,
            format!(
                "the program this install built carries no '{service}' recipe for '{place}.{field}'; build it again \
                 with this weft (`weft build`)"
            ),
        )
    })?;
    if spec.needs_browser_consent() && !connect.paste {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "'{service}' signs in through the provider's own page, which this door does not open; connect \
                 '{place}' from the editor's Connect button or with `weft connect {place}`"
            ),
        ));
    }
    Ok(spec)
}

/// `DELETE /program/connections/{node}/{field}`: clear the pick of one
/// connection field of the program, re-arming the triggers that read it,
/// and forget the connection it held once nothing on the install uses it
/// (the answer names it).
pub async fn clear(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Path((place, field)): Path<(String, String)>,
) -> Result<Json<ValuesChanged>, ApiError> {
    let caller = program_caller(&state, &headers).await?;
    let program = token_program(&state, caller.project).await?;
    picked_field(&program, &place, &field)?;
    crate::install_picks::change_forgetting(&state, caller.project, &[], &[(place, field)], None).await.map(Json)
}

#[cfg(test)]
mod tests {
    use super::{paste_recipe, picked_field, program_fields};
    use axum::http::StatusCode;
    use weft_core::project::{NodeDefinition, ProjectDefinition};

    fn spec(service: &str) -> serde_json::Value {
        serde_json::json!({ "service": service, "acquisition": { "kind": "static", "ways": [{ "fields": [{ "name": "key" }] }] } })
    }

    /// A program whose `vapi` node picks its `account` on the install
    /// (`optional` decides whether it may stay empty), beside a node
    /// whose connection an instance fills.
    fn program(optional: bool) -> ProjectDefinition {
        let picked = weft_core::picks::install_picked_literal();
        serde_json::from_value(serde_json::json!({
            "id": uuid::Uuid::nil(),
            "nodes": [
                {
                    "id": "vapi", "nodeType": "VapiAccess", "label": "Calls", "position": { "x": 0, "y": 0 }, "outputs": [],
                    "inputs": [{ "name": "account", "portType": "Access", "required": !optional,
                                 "widget": { "kind": "access", "service": "vapi", "optional": optional } }],
                    "portLiterals": { "account": picked },
                    "connectionService": spec("vapi")
                },
                {
                    "id": "slack", "nodeType": "SlackAccess", "position": { "x": 0, "y": 0 }, "outputs": [],
                    "inputs": [{ "name": "account", "portType": "Access", "required": true, "widget": { "kind": "access", "service": "slack" } }],
                    "portLiterals": { "account": { "__weft_instance_filled__": {} } }
                }
            ],
            "edges": []
        }))
        .unwrap()
    }

    fn node(program: &ProjectDefinition, id: &str) -> NodeDefinition {
        program.nodes.iter().find(|n| n.id == id).unwrap().clone()
    }

    /// The door lists the program's picked connection fields alone, each
    /// with its recipe and its pick, needed unless it may stay empty.
    #[test]
    fn the_door_lists_what_the_program_picks_on_the_install() {
        let none = program_fields(&program(false), &Default::default());
        assert_eq!(none.len(), 1, "an instance's connection is the instance door's: {none:?}");
        let field = &none[0];
        assert_eq!((field.node.as_str(), field.field.as_str(), field.node_type.as_str()), ("vapi", "account", "VapiAccess"));
        assert_eq!(field.label.as_deref(), Some("Calls"));
        assert!(field.needed && field.value.is_none());
        assert_eq!(field.spec.as_ref().map(|s| s.service.as_str()), Some("vapi"));
        assert_eq!(field.fallback, None);

        let mut picks = weft_core::picks::Picks::new();
        let handle = serde_json::json!({ "id": uuid::Uuid::from_u128(4), "identity": "acme" });
        picks.entry("vapi".into()).or_default().insert("account".into(), handle.clone());
        let picked = program_fields(&program(true), &picks);
        assert_eq!(picked[0].value.as_ref(), Some(&handle));
        assert!(!picked[0].needed, "a field that may stay empty is not needed");
    }

    /// Only a field the program picks on the install is this door's.
    #[test]
    fn the_door_fills_only_a_picked_connection() {
        let program = program(false);
        let (node, service) = picked_field(&program, "vapi", "account").unwrap();
        assert_eq!((node.id.as_str(), service.as_str()), ("vapi", "vapi"));
        assert_eq!(picked_field(&program, "slack", "account").unwrap_err().0, StatusCode::NOT_FOUND);
        assert_eq!(picked_field(&program, "vapi", "other").unwrap_err().0, StatusCode::NOT_FOUND);
        assert_eq!(picked_field(&program, "nope", "account").unwrap_err().0, StatusCode::NOT_FOUND);
    }

    /// A pasted connect runs on the program's recipe, for the field's
    /// service only, and never as a browser sign-in.
    #[test]
    fn a_paste_runs_on_the_programs_recipe() {
        let program = program(false);
        let vapi = node(&program, "vapi");
        let connect = |service: &str, paste: bool| -> weft_core::access::wire::ConnectDirect {
            serde_json::from_value(serde_json::json!({ "spec": spec(service), "values": { "key": "sk" }, "paste": paste, "projectId": null }))
                .unwrap()
        };
        let recipe = paste_recipe(&vapi, "vapi", "account", "vapi", &connect("vapi", false)).unwrap();
        assert_eq!(recipe.service, "vapi");
        let other = paste_recipe(&vapi, "vapi", "account", "vapi", &connect("openai", false)).unwrap_err();
        assert_eq!(other.0, StatusCode::BAD_REQUEST);
        assert!(other.1.contains("'openai'"), "{}", other.1);

        let bare = NodeDefinition { connection_service: None, ..vapi.clone() };
        let missing = paste_recipe(&bare, "vapi", "account", "vapi", &connect("vapi", false)).unwrap_err();
        assert_eq!(missing.0, StatusCode::CONFLICT);
        assert!(missing.1.contains("weft build"), "{}", missing.1);

        let consent: weft_core::AccessSpec = serde_json::from_value(serde_json::json!({
            "service": "vapi",
            "acquisition": { "kind": "oauth2", "grant": { "kind": "authorization_code", "authUrl": "https://a" }, "tokenUrl": "https://t" }
        }))
        .unwrap();
        let signs_in = NodeDefinition { connection_service: Some(consent), ..vapi };
        let refused = paste_recipe(&signs_in, "vapi", "account", "vapi", &connect("vapi", false)).unwrap_err();
        assert!(refused.1.contains("weft connect vapi"), "{}", refused.1);
    }
}
