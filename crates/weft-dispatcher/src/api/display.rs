//! A node's display, for an outside client.
//!
//! Two words for one thing, and each earns its place: DISPLAY is the
//! user-facing noun (the panel, `--display`, `/signal-token/displays`),
//! `/live` is the route both producers serve it on. So the doors here
//! are named for what a person asks for and the resolvers they call
//! (`infra::read_live`, `signal::read_signal_live`) for what they
//! fetch.
//!
//! Some nodes show something about themselves while they run. A
//! WhatsApp bridge shows a QR code to scan and then who is paired; a
//! database shows the credential it just minted; a trigger shows the
//! address it is listening on. Two node kinds carry one, and each
//! serves it from a different place:
//!
//!   - an INFRA node, from its own container's `/live`, which the
//!     node's author writes (`features.live_endpoint` names the
//!     endpoint serving it). It may carry buttons;
//!   - a TRIGGER node, from the listener Pod holding its signal. Read
//!     only: nothing about a registration is a reader's to change.
//!
//! The editor has had both since the beginning, through the
//! project-token routes under `/projects/{id}/...`. This module is the
//! SAME two feeds behind the signal token, so a website or an app
//! somebody builds on top of a weft program can show the QR code,
//! rather than telling their user to open the editor. One resolver per
//! feed (`infra::read_live`, `signal::read_signal_live`), two doors
//! onto it.
//!
//! ## What a token may see
//!
//! A display can be a credential. A bridge's QR code pairs the account
//! to whoever scans it, and a database's shows the password it just
//! minted, so a token reaches a display only when it was minted to:
//!
//!   - tenant: the project must belong to the token's tenant, or it is
//!     404 (indistinguishable from a project that does not exist, so a
//!     probe learns nothing about another account);
//!   - projects: `allowed_projects` empty means every project of the
//!     tenant, otherwise the project must be named in it;
//!   - displays: `<project id>/<node>` must be in `allowed_displays`,
//!     or the token must carry `all_displays`. Empty and not-all means
//!     this token reaches no display at all, which is what a token
//!     minted without a word about them gets. `weft token mint
//!     --displays` sets the wildcard within the token's projects;
//!     `--display <node>` names one display, spelled the way a person
//!     writes it and resolved against the project the CLI is run in,
//!     and the project scope above still bounds it.
//!
//! A grant covers the whole panel, reading it and pressing the buttons
//! its items carry, because a button is part of what the node serves.
//!
//! Tags do not come into it: a tag scopes which SIGNALS a token sees,
//! and a display is not a signal.

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use serde::Serialize;
use serde_json::Value;

use crate::state::DispatcherState;

/// One node that has something to show, as the listing gives it.
#[derive(Debug, Serialize)]
pub struct NodeDisplayEntry {
    pub project_id: String,
    pub project_name: String,
    /// The node, spelled the way a person writes it (`test.whatsapp`),
    /// which is also what the read door's path takes. The compiled id
    /// behind it stays inside the daemon.
    pub node: String,
    pub node_type: String,
    /// `infra` (served by the node's own container) or `trigger`
    /// (served by the listener holding its signal). A client that just
    /// renders what it is given does not need this; one that knows a
    /// particular node uses it to tell two same-named feeds apart.
    pub kind: &'static str,
    /// The node's label when it has one, so a client can title the
    /// panel without knowing the project.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

/// `GET /signal-token/displays` (signal token in `Authorization:
/// Bearer`). The displays this token reaches, and nothing else, each
/// spelled the way a person writes it. A client reads this once and
/// then polls the feeds it cares about.
///
/// One entry per PLACE, not per node: a node inside a file called from
/// two places has a name per call, and each name is its own display (a
/// trigger registers once per call, and each registration shows its
/// own address).
pub async fn list_displays(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
) -> Result<Json<Vec<NodeDisplayEntry>>, (StatusCode, String)> {
    // Scoped like every other door on this token: it lists what this
    // token reaches and nothing else. Finding a display to grant is
    // not this door's job, because the granting happens in the CLI,
    // inside the project, before any token exists.
    //
    // It does NOT filter on `fires_visible_to_consumers` the way the
    // signals door does, and that difference is the right one. That
    // flag hides what would START AN EXECUTION: a hibernating project
    // must not be fired into. A display starts nothing. Infra can be
    // up while the program sleeps, so its panel is worth reading the
    // whole time, and hiding it would say the node is gone when it is
    // running.
    let token = display_token(&state, &headers).await?;
    let summaries = state
        .projects
        .list(&token.tenant_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("list projects: {e}")))?;
    let mut out = Vec::new();
    for summary in summaries {
        if !token.covers_project(&summary.id) {
            continue;
        }
        let Some(project) = state
            .projects
            .project(summary.id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        else {
            // Listed a moment ago, gone now (a wipe raced this read).
            continue;
        };
        for (node, kind, address) in &displays_of(&project) {
            if !token_reaches_display(&token, &summary.id, address) {
                continue;
            }
            out.push(NodeDisplayEntry {
                project_id: summary.id.to_string(),
                project_name: summary.name.clone(),
                node: address.clone(),
                node_type: node.node_type.clone(),
                kind: kind.wire(),
                label: node.label.clone(),
            });
        }
    }
    Ok(Json(out))
}

/// `GET /signal-token/displays/{project_id}/{node}`, where `{node}` is
/// the name a person writes. What the node is showing right now: `{ "items": [...] }`, whichever of the two
/// produced it.
///
/// The feed is live, so it is read on every render, never stored: a QR
/// code expires in under a minute, and a paired bridge shows a phone
/// number instead.
pub async fn read_display(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Path((project_id, address)): Path<(String, String)>,
) -> Result<Json<weft_core::live::LiveFeed>, (StatusCode, String)> {
    let (id, kind) = resolve(&state, &headers, &project_id, &address).await?;
    // Both resolvers take the PLACE as the caller spelled it: a trigger's
    // registration and an infra node's instance are each one per place,
    // keyed by that spelling, so a file included twice is two displays
    // and a grant for one opens that one alone.
    Ok(Json(match kind {
        DisplayKind::Infra => crate::api::infra::read_live(&state, id, &address).await?,
        DisplayKind::Trigger => crate::api::signal::read_signal_live(&state, id, &address).await?,
    }))
}

/// `POST /signal-token/displays/{project_id}/{node}/action`. Press a
/// button one of the display's items carries (the bridge's "disconnect
/// phone", say). The node's own code decides what the press does and
/// what comes back; a refusal it writes arrives as a 400 with its text.
///
/// Only an infra node's display has buttons. A trigger's is read-only:
/// its panel says where a caller sends and how the door checks them,
/// and neither is the reader's to change from here.
pub async fn press_display(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Path((project_id, address)): Path<(String, String)>,
    // Taken raw, and parsed below: an extractor runs BEFORE the handler,
    // so `Json<_>` here would answer a caller holding no token at all
    // with axum's complaint about the body shape, telling them the route
    // exists and what it takes. Authorize first, read the body after.
    body: axum::body::Bytes,
) -> Result<Json<Value>, (StatusCode, String)> {
    let (id, kind) = resolve(&state, &headers, &project_id, &address).await?;
    let body: crate::api::infra::InfraActionBody = serde_json::from_slice(&body).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            format!("a press is {{ \"kind\": \"<actionKind>\", \"payload\": ... }}: {e}"),
        )
    })?;
    match kind {
        DisplayKind::Infra => Ok(Json(
            crate::api::infra::press_live(&state, id, &address, &body.kind, &body.payload).await?,
        )),
        // Named the way the CALLER named it: they asked for
        // `bridge.whatsapp`, so hearing about some other spelling of
        // that node would send them looking for a thing they never
        // wrote.
        DisplayKind::Trigger => Err((
            StatusCode::BAD_REQUEST,
            format!("'{address}' is a trigger, and a trigger's display is read-only"),
        )),
    }
}

/// Every display of a project, as the deciders on this surface read
/// it: the node, what serves its panel, and the address a person
/// writes to name it.
///
/// Built ONCE per request and passed down. Each decider used to build
/// its own, which made one listing cost a project walk per display per
/// grant, on a door meant to be polled.
type Displays<'a> =
    Vec<(&'a weft_core::project::NodeDefinition, DisplayKind, String)>;

/// The display `address` names, if a person could have written that
/// address for one: the node behind it and what serves its panel.
///
/// THE lookup on this surface. The door asks it of the name in its
/// path and mint asks it of the name being granted, so no spelling can
/// open a panel that could not also have been granted by name.
///
/// The compiled id is a name in none of them. Resolving the string
/// instead would take it, because the resolver hands back what it
/// cannot place, and the panel would answer to a spelling no grant
/// could be written for and no person would recognise.
fn display_at<'a>(
    displays: &Displays<'a>,
    address: &str,
) -> Option<(&'a weft_core::project::NodeDefinition, DisplayKind)> {
    displays.iter().find(|(_, _, addr)| addr == address).map(|(node, kind, _)| (*node, *kind))
}

/// THE question both display doors ask: may this token reach the
/// panel at this place, in this project?
///
/// A grant is the canonical pair `display_grant` writes, and mint
/// stores nothing else (`canonical_display_grants`), so the comparison
/// is one string against the pair built for this place. Both halves
/// have to match. The address half is compared as written, because a
/// place has exactly one name: a node inside a file called from two
/// places is two places, and a grant for `one.whatsapp` says nothing
/// about `two.whatsapp`. The project half is what anchors the address:
/// it names one place inside one project and says nothing outside it,
/// so two projects that both include `lib/setup.weft` each have their
/// own `setup.store` and a grant reaches exactly one of them.
///
/// Both halves, together, because either alone is a hole. The project
/// scope alone says nothing about which display to open, and one of
/// them can be a credential. The grants alone are the wildcard's blind
/// spot: `--displays` names no node, so nothing would bound it to the
/// projects the token was given.
///
/// Reaching a display means reading its panel AND pressing the buttons
/// its items carry: a button is part of what the node serves, and
/// splitting the press into a second dimension would be a knob nobody
/// asked for.
///
/// The tenant is checked separately, before the project is read at all.
fn token_reaches_display(
    token: &crate::journal::SignalToken,
    project_id: &uuid::Uuid,
    address: &str,
) -> bool {
    if !token.covers_project(project_id) {
        return false;
    }
    token.all_displays
        || token.allowed_displays.contains(&weft_core::live::display_grant(project_id, address))
}

/// What the door decides once it holds the project: whether this
/// address names a display, where its panel comes from, and whether
/// this token reaches it. The address itself is what the resolvers
/// then take (every runtime row is keyed by that spelling), so nothing
/// else about the node leaves here.
///
/// Lifted out of the request so it can be tested. `resolve` around it
/// is the token, the tenant and the fetches; this is the rule. `None`
/// is every miss, which the door answers with its one 404.
fn display_for_token(
    token: &crate::journal::SignalToken,
    project_id: &uuid::Uuid,
    project: &weft_core::project::ProjectDefinition,
    address: &str,
) -> Option<DisplayKind> {
    let (_, kind) = display_at(&displays_of(project), address)?;
    token_reaches_display(token, project_id, address).then_some(kind)
}

/// Every display of `project`: the node, what serves its panel, and
/// the address a person writes to name it.
///
/// One walk, because the listing and the mint check are the same
/// question asked twice and a second copy would drift.
///
/// One address names one place, which is what makes it a key. A node
/// inside a file called from two places is two places, each with its
/// own address and its own display.
fn displays_of(project: &weft_core::project::ProjectDefinition) -> Displays<'_> {
    // Indexed once: a place is looked up by id, and scanning the node
    // list per place made this cost the project twice over on a door
    // an outside client polls.
    let by_id: std::collections::HashMap<&str, &weft_core::project::NodeDefinition> =
        project.nodes.iter().map(|n| (n.id.as_str(), n)).collect();
    weft_core::project::selection::every_place(project)
        .into_iter()
        .filter_map(|place| {
            let node = *by_id.get(place.id.as_str())?;
            let kind = display_kind(node)?;
            let address = weft_core::project::address_of(project, &place.id, &place.path);
            Some((node, kind, address))
        })
        .collect()
}

/// The grants to STORE for these asked-for ones, or why one of them
/// cannot reach anything.
///
/// Checked at mint, where the person is still holding the flag they
/// got wrong. Every refusal here is one they would otherwise meet as a
/// 404 forever with nothing to say why: a grant that is not a
/// `<project id>/<address>` pair, a project that is not theirs or that
/// their own `--projects` scope excludes, a project removed mid-mint,
/// and an address that names no display, which lists that project's
/// displays instead.
///
/// What comes back is CANONICAL: `Uuid::parse_str` takes a braced or
/// urn spelling too, so a grant stored as it arrived could pass every
/// check here and then never equal the pair the door builds.
///
/// `--displays`, the wildcard, names no node and takes none of this;
/// the token's project scope is the whole of its bound.
pub(crate) async fn canonical_display_grants(
    state: &DispatcherState,
    tenant: &str,
    allowed_projects: &[uuid::Uuid],
    grants: &[String],
    all_displays: bool,
) -> Result<Vec<String>, (StatusCode, String)> {
    // The wildcard swallows named grants whole: the scope never reads
    // the list again, and `token ls` would print "(all)" over the two
    // the person typed. Two answers to one question is a mistake at the
    // flag, so it is refused there.
    if all_displays && !grants.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "--displays grants every display of this token's projects, so naming one \
             with --display as well says two different things. Pass one or the other."
                .into(),
        ));
    }
    let mut canonical = Vec::with_capacity(grants.len());
    for grant in grants {
        let (project_id, address) = split_grant(grant)?;
        // A grant outside the token's own project scope would be dead
        // the moment it was written: the door checks the scope too, and
        // a scope narrows. Say so here rather than mint a 404.
        if !allowed_projects.is_empty() && !allowed_projects.contains(&project_id) {
            return Err((
                StatusCode::BAD_REQUEST,
                format!(
                    "'{address}' is in a project this token is not scoped to. Add that \
                     project to --projects, or drop the grant."
                ),
            ));
        }
        if state
            .projects
            .tenant_for(project_id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("tenant: {e}")))?
            .as_deref()
            != Some(tenant)
        {
            return Err((StatusCode::NOT_FOUND, format!("no such project: {project_id}")));
        }
        let project = state
            .projects
            .project(project_id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
            .ok_or((
                StatusCode::CONFLICT,
                format!("project {project_id} was removed while this token was being minted"),
            ))?;
        // Through the lookup, not through the resolver: the compiled
        // id resolves to a node too, so checking that would accept
        // `Test.whatsapp` and `weft token ls` would print it straight
        // back at the person. A display is granted only by a name
        // somebody could have written for it.
        let available = displays_of(&project);
        if display_at(&available, address).is_none() {
            return Err((
                StatusCode::BAD_REQUEST,
                format!(
                    "'{address}' is not a display of that project. Its displays are: {}.",
                    if available.is_empty() {
                        "none".to_string()
                    } else {
                        available.iter().map(|(_, _, a)| a.clone()).collect::<Vec<_>>().join(", ")
                    }
                ),
            ));
        }
        canonical.push(weft_core::live::display_grant(&project_id, address));
    }
    Ok(canonical)
}

/// [`weft_core::live::split_display_grant`], with the refusal this
/// door owes a caller who sent something else.
fn split_grant(grant: &str) -> Result<(uuid::Uuid, &str), (StatusCode, String)> {
    weft_core::live::split_display_grant(grant).ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            format!(
                "'{grant}' is not a display grant; one reads <project id>/<node>, and \
                 `weft token mint --display <node>` writes it for you."
            ),
        )
    })
}

/// Where a node's display comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DisplayKind {
    Infra,
    Trigger,
}

impl DisplayKind {
    fn wire(self) -> &'static str {
        match self {
            DisplayKind::Infra => "infra",
            DisplayKind::Trigger => "trigger",
        }
    }
}

/// Does this node have a display, and whose is it?
///
/// An infra node has one when its author opted in by naming the
/// endpoint that serves `/live`; a node that speaks only TCP names
/// none and has nothing to show. A trigger always has one: the
/// listener's kind handler renders it.
///
/// Infra is checked first, so a node that somehow declared both stays
/// on one answer rather than flipping between calls.
fn display_kind(node: &weft_core::project::NodeDefinition) -> Option<DisplayKind> {
    if node.requires_infra && node.features.live_endpoint.is_some() {
        return Some(DisplayKind::Infra);
    }
    if node.features.is_trigger {
        return Some(DisplayKind::Trigger);
    }
    None
}

/// The token this request presents, refused unless it reaches any
/// display at all. A token minted without a word about displays is
/// told which flag would have given it one, and where to read the
/// grants, rather than meeting a bare 404 on every node it tries.
async fn display_token(
    state: &DispatcherState,
    headers: &HeaderMap,
) -> Result<crate::journal::SignalToken, (StatusCode, String)> {
    let token = crate::api::signal::token_from_bearer(state, headers).await?;
    if !token.reaches_displays() {
        return Err((
            StatusCode::FORBIDDEN,
            "this token reaches no node display. Mint one with `--displays` for every \
             display of its projects, or `--display <node>` for one, from that \
             project's folder."
                .into(),
        ));
    }
    Ok(token)
}

/// Authorize the token for this project and this node, and say which
/// node it is and where its display comes from.
///
/// `address` is the path segment: the node spelled the way a person
/// writes it (`test.whatsapp`), the same spelling the grant carries,
/// the listing hands back, and the runtime keys the node's rows by.
/// The compiled id behind it is looked at only to know whether the
/// address names a display at all; it goes nowhere.
async fn resolve(
    state: &DispatcherState,
    headers: &HeaderMap,
    project_id: &str,
    address: &str,
) -> Result<(uuid::Uuid, DisplayKind), (StatusCode, String)> {
    let token = display_token(state, headers).await?;
    let id = project_id
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad project id".to_string()))?;
    // A project the token may not see, a node it was not given, a node
    // that does not exist and a node with nothing to show all answer
    // the same: somebody walking ids learns only "nothing here".
    let not_found = || (StatusCode::NOT_FOUND, "no such node display".to_string());
    // The tenant first, so nothing below reads a project of somebody
    // else's; then the one question this surface is about.
    let owner = state
        .projects
        .tenant_for(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("tenant: {e}")))?;
    if owner.as_deref() != Some(token.tenant_id.as_str()) {
        return Err(not_found());
    }
    let project = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or_else(not_found)?;
    let kind = display_for_token(&token, &id, &project, address).ok_or_else(not_found)?;
    Ok((id, kind))
}

#[cfg(test)]
mod scope_tests {
    use super::{displays_of, split_grant};
    use weft_core::live::display_grant;
    use crate::journal::SignalToken;
    use weft_core::project::ProjectDefinition;

    fn project_a() -> uuid::Uuid {
        uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111111").expect("a fixed id")
    }

    fn project_b() -> uuid::Uuid {
        uuid::Uuid::parse_str("22222222-2222-2222-2222-222222222222").expect("a fixed id")
    }

    /// A project holding one trigger per id, so each has a display.
    fn project_of(ids: &[&str]) -> ProjectDefinition {
        let nodes: Vec<serde_json::Value> = ids
            .iter()
            .map(|id| {
                serde_json::json!({
                    "id": id, "nodeType": "T", "config": {},
                    "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [], "outputs": [],
                    "features": { "isTrigger": true },
                })
            })
            .collect();
        serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": nodes, "edges": [], "groups": []
        }))
        .expect("the fixture deserializes")
    }

    /// The shape an include compiles to: the file's nodes carry its
    /// prefix, and a call site brings them in under the name the user
    /// wrote (`test = @include("test.weft")`).
    fn project_with_include() -> ProjectDefinition {
        let node = |id: &str, node_type: &str, scope: &[&str], boundary: serde_json::Value| {
            serde_json::json!({
                "id": id, "nodeType": node_type, "config": {},
                "position": { "x": 0.0, "y": 0.0 }, "inputs": [], "outputs": [],
                "features": { "isTrigger": id == "Test.whatsapp" },
                "scope": scope, "groupBoundary": boundary,
            })
        };
        serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("test__in", "CallIn", &[], serde_json::json!({ "groupId": "test", "role": "In" })),
                node("test__out", "CallOut", &[], serde_json::json!({ "groupId": "test", "role": "Out" })),
                node("Test__in", "IncludeIn", &[], serde_json::json!({ "groupId": "Test", "role": "In" })),
                node("Test.whatsapp", "BaileyBridge", &["Test"], serde_json::Value::Null),
                node("Test__out", "IncludeOut", &[], serde_json::json!({ "groupId": "Test", "role": "Out" })),
            ],
            "edges": [],
            "groups": [
                { "id": "test", "kind": "call", "body": "Test", "nodeIds": [] },
                { "id": "Test", "kind": "body", "nodeIds": ["Test.whatsapp"] }
            ]
        }))
        .expect("the fixture deserializes")
    }

    fn token(displays: &[String], all: bool) -> SignalToken {
        scoped_token(displays, all, vec![])
    }

    /// The door's own rule, which is what these tests are about: the
    /// address names a display, and the token reaches it.
    fn reaches(
        token: &SignalToken,
        project_id: &uuid::Uuid,
        project: &ProjectDefinition,
        address: &str,
    ) -> bool {
        super::display_for_token(token, project_id, project, address).is_some()
    }

    /// `projects` empty is every project of the tenant, which is what
    /// these tests want unless they are about the scope itself.
    fn scoped_token(displays: &[String], all: bool, projects: Vec<uuid::Uuid>) -> SignalToken {
        SignalToken {
            id: uuid::Uuid::nil(),
            token_hash: "hash".into(),
            recognizer: "wft-test-…".into(),
            tenant_id: "t".into(),
            name: None,
            allowed_projects: projects,
            allowed_tags: vec![],
            allowed_displays: displays.to_vec(),
            all_displays: all,
            created_at: 0,
        }
    }

    #[test]
    fn a_token_minted_without_a_word_about_displays_reaches_none() {
        // The dimension that does not follow empty-means-any: a
        // display can be a credential, so silence means no.
        let t = token(&[], false);
        let p = project_of(&["whatsapp"]);
        assert!(!t.reaches_displays());
        assert!(!reaches(&t, &project_a(), &p, "whatsapp"));
    }

    #[test]
    fn a_grant_is_written_the_way_the_user_writes_the_node() {
        // `test.whatsapp` is the `whatsapp` of the file brought in as
        // `test`. The compiled id behind it (`Test.whatsapp`) is the
        // runtime's own spelling and appears nowhere a person looks.
        let p = project_with_include();
        let t = token(&[display_grant(&project_a(), "test.whatsapp")], false);
        assert!(reaches(&t, &project_a(), &p, "test.whatsapp"));
        let addresses: Vec<String> = displays_of(&p).into_iter().map(|(_, _, a)| a).collect();
        assert_eq!(addresses, vec!["test.whatsapp".to_string()]);
    }

    /// The same file brought in twice: `one = @include("bridge.weft")`
    /// and `two = @include("bridge.weft")`, each with its own call
    /// site over one shared body.
    fn project_with_two_sites() -> ProjectDefinition {
        let node = |id: &str, node_type: &str, scope: &[&str], boundary: serde_json::Value| {
            serde_json::json!({
                "id": id, "nodeType": node_type, "config": {},
                "position": { "x": 0.0, "y": 0.0 }, "inputs": [], "outputs": [],
                "features": { "isTrigger": id == "Bridge.whatsapp" },
                "scope": scope, "groupBoundary": boundary,
            })
        };
        serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("one__in", "CallIn", &[], serde_json::json!({ "groupId": "one", "role": "In" })),
                node("one__out", "CallOut", &[], serde_json::json!({ "groupId": "one", "role": "Out" })),
                node("two__in", "CallIn", &[], serde_json::json!({ "groupId": "two", "role": "In" })),
                node("two__out", "CallOut", &[], serde_json::json!({ "groupId": "two", "role": "Out" })),
                node("Bridge__in", "IncludeIn", &[], serde_json::json!({ "groupId": "Bridge", "role": "In" })),
                node("Bridge.whatsapp", "BaileyBridge", &["Bridge"], serde_json::Value::Null),
                node("Bridge__out", "IncludeOut", &[], serde_json::json!({ "groupId": "Bridge", "role": "Out" })),
            ],
            "edges": [],
            "groups": [
                { "id": "one", "kind": "call", "body": "Bridge", "nodeIds": [] },
                { "id": "two", "kind": "call", "body": "Bridge", "nodeIds": [] },
                { "id": "Bridge", "kind": "body", "nodeIds": ["Bridge.whatsapp"] }
            ]
        }))
        .expect("the fixture deserializes")
    }

    #[test]
    fn each_call_of_a_file_is_its_own_display_and_its_own_grant() {
        // The same node under two sites is two places. Each registers
        // its own signal under its own spelling, so each is listed, and
        // a grant for one says nothing about the other.
        let p = project_with_two_sites();
        let displays = displays_of(&p);
        let names: Vec<String> = displays.iter().map(|(_, _, a)| a.clone()).collect();
        assert_eq!(names, vec!["one.whatsapp".to_string(), "two.whatsapp".to_string()]);
        for asked in &names {
            let (node, kind) = super::display_at(&displays, asked).expect("each name is a display");
            assert_eq!((node.id.as_str(), kind), ("Bridge.whatsapp", super::DisplayKind::Trigger));
        }
        let t = token(&[display_grant(&project_a(), "one.whatsapp")], false);
        assert!(reaches(&t, &project_a(), &p, "one.whatsapp"));
        assert!(!reaches(&t, &project_a(), &p, "two.whatsapp"), "the same node, another place");
        let all = token(&[], true);
        assert!(reaches(&all, &project_a(), &p, "two.whatsapp"));
    }

    #[test]
    fn a_display_is_found_by_the_name_a_person_writes_and_by_no_other() {
        // The lookup the door, a grant and mint all go through. The
        // compiled id resolves to a node, so anything that RESOLVED
        // here would take it and the panel would answer to a name
        // nobody could be granted by.
        let p = project_with_include();
        let displays = displays_of(&p);
        let (node, kind) = super::display_at(&displays, "test.whatsapp").expect("the name they write finds it");
        assert_eq!(node.id, "Test.whatsapp");
        assert_eq!(kind, super::DisplayKind::Trigger);
        for not_a_name in ["Test.whatsapp", "", "whatsapp"] {
            assert!(
                super::display_at(&displays, not_a_name).is_none(),
                "{not_a_name} is not a name a person writes"
            );
        }
    }

    #[test]
    fn a_grant_written_with_the_compiled_id_reaches_nothing() {
        // Mint refuses to write one, and this is the door holding the
        // same line under it: a row that arrived another way opens no
        // panel.
        let p = project_with_include();
        let t = token(&[display_grant(&project_a(), "Test.whatsapp")], false);
        assert!(!reaches(&t, &project_a(), &p, "test.whatsapp"));
        let proper = token(&[display_grant(&project_a(), "test.whatsapp")], false);
        assert!(reaches(&proper, &project_a(), &p, "test.whatsapp"));
    }

    #[test]
    fn a_grant_names_one_display_in_one_project() {
        // An address is a key only inside a project: two projects that
        // include the same file each have their own `test.whatsapp`.
        let p = project_with_include();
        let t = token(&[display_grant(&project_a(), "test.whatsapp")], false);
        assert!(reaches(&t, &project_a(), &p, "test.whatsapp"));
        assert!(!reaches(&t, &project_b(), &p, "test.whatsapp"));
    }

    #[test]
    fn the_project_scope_still_bounds_a_grant() {
        // A scope narrows and never widens, so a grant in a project the
        // token was not given reaches nothing. Mint refuses to write
        // that pair; this is the door holding the same line under it.
        let p = project_of(&["whatsapp"]);
        let t = scoped_token(
            &[display_grant(&project_b(), "whatsapp")],
            false,
            vec![project_a()],
        );
        assert!(!reaches(&t, &project_b(), &p, "whatsapp"));
    }

    #[test]
    fn the_wildcard_is_bounded_by_the_project_scope() {
        // `--displays` names no node, so the project scope is the only
        // thing holding it, and no check at mint can help: there is no
        // grant to validate.
        let p = project_of(&["whatsapp"]);
        let t = scoped_token(&[], true, vec![project_a()]);
        assert!(reaches(&t, &project_a(), &p, "whatsapp"));
        assert!(!reaches(&t, &project_b(), &p, "whatsapp"));
    }

    #[test]
    fn a_grant_reaches_the_node_it_names_and_no_other() {
        let p = project_of(&["whatsapp", "database"]);
        let t = token(&[display_grant(&project_a(), "whatsapp")], false);
        assert!(reaches(&t, &project_a(), &p, "whatsapp"));
        assert!(!reaches(&t, &project_a(), &p, "database"));
    }

    #[test]
    fn a_uuid_spelling_that_is_not_the_canonical_one_still_reads_as_the_pair() {
        // `parse_str` takes the braced, urn and unhyphenated forms too.
        // A grant stored as it arrived would pass every check at mint
        // and then never equal what the door builds, which is why mint
        // stores what `display_grant` writes.
        let canonical = display_grant(&project_a(), "whatsapp");
        for spelling in [
            "{11111111-1111-1111-1111-111111111111}",
            "urn:uuid:11111111-1111-1111-1111-111111111111",
            "11111111111111111111111111111111",
            "11111111-1111-1111-1111-111111111111".to_uppercase().as_str(),
        ] {
            let typed = format!("{spelling}/whatsapp");
            let (project, address) = split_grant(&typed).expect("it parses");
            assert_eq!(weft_core::live::display_grant(&project, address), canonical, "{spelling}");
        }
    }

    #[test]
    fn a_grant_is_read_as_a_pair_and_says_how_it_is_malformed() {
        let one = display_grant(&project_a(), "test.whatsapp");
        let (project, address) = split_grant(&one).expect("a well-formed grant");
        assert_eq!(project, project_a());
        assert_eq!(address, "test.whatsapp");
        // Nothing that is not the pair, and the refusal names the
        // shape plus the flag that writes it, since a person never
        // builds one by hand.
        for bad in [
            "whatsapp",
            "11111111-1111-1111-1111-111111111111/",
            "11111111-1111-1111-1111-111111111111/   ",
            "not-a-uuid/whatsapp",
            "",
        ] {
            let (status, said) = split_grant(bad).expect_err("malformed");
            assert_eq!(status, axum::http::StatusCode::BAD_REQUEST, "{bad}");
            assert!(said.contains("<project id>/<node>"), "{bad}: {said}");
            assert!(said.contains("--display"), "{bad}: {said}");
        }
    }
}

#[cfg(test)]
mod display_kind_tests {
    use super::*;
    use weft_core::project::NodeDefinition;

    fn node(requires_infra: bool, live_endpoint: Option<&str>, is_trigger: bool) -> NodeDefinition {
        let mut n: NodeDefinition = serde_json::from_value(serde_json::json!({
            "id": "n", "nodeType": "T", "config": {},
            "position": { "x": 0.0, "y": 0.0 },
            "inputs": [], "outputs": [],
        }))
        .expect("a minimal node deserializes");
        n.requires_infra = requires_infra;
        n.features.live_endpoint = live_endpoint.map(str::to_string);
        n.features.is_trigger = is_trigger;
        n
    }

    #[test]
    fn an_infra_node_serving_live_has_a_display() {
        assert_eq!(display_kind(&node(true, Some("http"), false)), Some(DisplayKind::Infra));
    }

    #[test]
    fn a_tcp_only_infra_node_has_nothing_to_show() {
        // Postgres and friends name no live endpoint, so they stay out
        // of the listing rather than answering 502 from a refused
        // connection when somebody reads them.
        assert_eq!(display_kind(&node(true, None, false)), None);
    }

    #[test]
    fn a_trigger_has_a_display() {
        assert_eq!(display_kind(&node(false, None, true)), Some(DisplayKind::Trigger));
    }

    #[test]
    fn an_ordinary_node_has_none() {
        assert_eq!(display_kind(&node(false, None, false)), None);
    }
}