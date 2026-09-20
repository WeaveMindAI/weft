//! Read what a node is showing, through both doors onto it.
//!
//! A node's **display** is the `{ "items": [...] }` an infra node's
//! container serves on `/live`, and that a trigger's signal kind
//! serves through the listener. Two doors reach the same feed, and
//! they authorize differently, so only an e2e proves they agree. The
//! rig reads a TRIGGER's display through both; `tests/infra.rs` covers
//! the other half of the token door, an infra node with nothing to
//! show:
//!
//!   - the editor's, `GET /projects/{id}/signals/{node}/live`, on the
//!     tenant-authenticated surface;
//!   - an outside client's, `GET /signal-token/displays/...`, behind a
//!     signal token whose `--display` scope names the node.
//!
//! The display scope does NOT follow the empty-means-any rule the
//! other scope vectors use: a token that names no display reaches none,
//! because a display can be a credential (a bridge's QR code pairs the
//! account to whoever scans it). [`mint_display_token`] takes the
//! nodes explicitly for that reason.

use anyhow::{Context, Result};
use serde_json::{json, Value};

use crate::client::Dispatcher;

/// One node's display, as either door returns it. Thin typed accessor
/// over the item list.
// SYNC: Display <-> crates/weft-core/src/live.rs LiveFeed / LiveItem, packages/weft-graph/src/protocol.ts LiveDataItem
#[derive(Debug, Clone)]
pub struct Display(pub Vec<Value>);

impl Display {
    fn of(body: Value) -> Result<Self> {
        let items = body
            .get("items")
            .and_then(Value::as_array)
            .with_context(|| format!("a display answers {{ items: [...] }}, got {body}"))?;
        Ok(Self(items.clone()))
    }

    /// The `data` of the item labelled `label`, or None when the
    /// display carries no such line.
    pub fn data(&self, label: &str) -> Option<&Value> {
        self.item(label).and_then(|i| i.get("data"))
    }

    /// The whole item labelled `label`, for a test that needs its type
    /// or its action rather than its value.
    pub fn item(&self, label: &str) -> Option<&Value> {
        self.0
            .iter()
            .find(|i| i.get("label").and_then(Value::as_str) == Some(label))
    }

    /// Every label, in order, for an assertion message that shows what
    /// the display actually carried.
    pub fn labels(&self) -> Vec<&str> {
        self.0
            .iter()
            .filter_map(|i| i.get("label").and_then(Value::as_str))
            .collect()
    }

    /// The `data` of `label`, or a loud error naming what was there.
    pub fn expect_text(&self, label: &str) -> Result<String> {
        let value = self.data(label).with_context(|| {
            format!("no '{label}' line in the display; it carried {:?}", self.labels())
        })?;
        value
            .as_str()
            .map(str::to_string)
            .with_context(|| format!("'{label}' is not a string: {value}"))
    }
}

/// What a trigger node is showing, through the EDITOR's door (the
/// project-token surface the graph view polls). `node` is the trigger's
/// place spelled the way a person writes it (`door`, or `one.door` for
/// the `door` inside the file the site `one` includes), the same string
/// the token door takes: a registration is keyed by its place, and the
/// editor spells the place from the calls it walked into.
pub async fn as_editor(disp: &Dispatcher, project_id: &uuid::Uuid, node: &str) -> Result<Display> {
    let body: Value = disp
        .get_json(&format!("/projects/{project_id}/signals/{node}/live"))
        .await?;
    Display::of(body)
}

/// Mint a signal token that may read the named nodes' displays (and,
/// with `nodes` empty and `all` set, every display in the project).
/// Project-scoped like the rig's other tokens.
pub async fn mint_display_token(
    disp: &Dispatcher,
    project_id: &uuid::Uuid,
    name: &str,
    nodes: &[&str],
    all: bool,
) -> Result<String> {
    // A display grant is the `<project id>/<node>` pair the door
    // matches, the node spelled the way a person writes it. The CLI
    // fills the project in from the folder; the rig has the id here,
    // so it writes the pair itself.
    let grants: Vec<String> = nodes
        .iter()
        .map(|node| weft_core::live::display_grant(project_id, node))
        .collect();
    let body = json!({
        "name": name,
        "allowedProjects": [project_id.to_string()],
        "allowedTags": [],
        "allowedDisplays": grants,
        "allDisplays": all,
    });
    let resp: Value = disp.post_json("/signal-tokens", &body).await?;
    resp.get("token")
        .and_then(Value::as_str)
        .map(str::to_string)
        .context("mint token response missing `token`")
}

/// The node displays this token may watch, each spelled the way a
/// person writes it.
pub async fn list_for_token(disp: &Dispatcher, token: &str) -> Result<Vec<Value>> {
    disp.get_json_bearer("/signal-token/displays", token).await
}

/// What a node is showing, through an OUTSIDE CLIENT's door.
pub async fn as_token(
    disp: &Dispatcher,
    token: &str,
    project_id: &uuid::Uuid,
    node: &str,
) -> Result<Display> {
    let body: Value = disp
        .get_json_bearer(
            &format!("/signal-token/displays/{project_id}/{node}"),
            token,
        )
        .await?;
    Display::of(body)
}

/// The raw status of a token's read, for asserting a refusal rather
/// than a body.
pub async fn read_status(
    disp: &Dispatcher,
    token: &str,
    project_id: &uuid::Uuid,
    node: &str,
) -> Result<reqwest::StatusCode> {
    let (status, _) = disp
        .get_raw_bearer(
            &format!("/signal-token/displays/{project_id}/{node}"),
            token,
        )
        .await?;
    Ok(status)
}

/// The raw status of a token's listing, same reason.
pub async fn list_status(disp: &Dispatcher, token: &str) -> Result<(reqwest::StatusCode, String)> {
    disp.get_raw_bearer("/signal-token/displays", token).await
}

/// Press a button on one display, keeping the status and the body: the
/// refusals say what the caller may not do, and that text is the point.
pub async fn press_status(
    disp: &Dispatcher,
    token: &str,
    project_id: &uuid::Uuid,
    node: &str,
    action_kind: &str,
) -> Result<(reqwest::StatusCode, String)> {
    disp.post_raw_bearer(
        &format!("/signal-token/displays/{project_id}/{node}/action"),
        token,
        &json!({ "kind": action_kind, "payload": null }),
    )
    .await
}

/// Read one display, keeping the body: the refusals name the flag that
/// would have granted it, and that text is the point of the test.
pub async fn read_status_with_body(
    disp: &Dispatcher,
    token: &str,
    project_id: &uuid::Uuid,
    node: &str,
) -> Result<(reqwest::StatusCode, String)> {
    disp.get_raw_bearer(&format!("/signal-token/displays/{project_id}/{node}"), token)
        .await
}
