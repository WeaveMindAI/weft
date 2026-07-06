//! Discover and fire the triggers an outside party calls IN to.
//!
//! Covers two firing surfaces:
//!   - Public-entry webhooks: a registered signal with a `mount_path`, fired by
//!     `POST /{mount_path}` with the request body (and an `X-Api-Key` header if
//!     the signal declared api_key auth).
//!   - Token signals (forms, human-in-the-loop resumes): fired by
//!     `POST /signal/{signal_token}`.
//!
//! Discovery is via a signal token (in `Authorization: Bearer`):
//! `GET /signal-token/signals` returns the
//! consumer payload of every signal the token can see (each a
//! `{ token, nodeId, kind, mount_path?, auth?, formSchema?, ... }` object). The
//! rig mints a PROJECT-SCOPED token (via [`SignalScope`]), so the dispatcher's
//! own `allowedProjects` filter returns only that project's signals, then
//! matches the one it wants by node. The token's scope IS the isolation; the rig
//! never best-effort filters the (often project_id-less) payload after the fact.

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

use crate::client::Dispatcher;

/// A registered signal, as seen through the signal-token enumeration. Thin typed
/// accessor over the consumer payload JSON (read by name; SYNC by string with
/// the listener's render output).
#[derive(Debug, Clone)]
pub struct DiscoveredSignal(pub Value);

impl DiscoveredSignal {
    /// The signal token used to fire it via `/signal/{token}`.
    pub fn token(&self) -> Option<&str> {
        self.0.get("token").and_then(Value::as_str)
    }
    /// The trigger node id.
    pub fn node_id(&self) -> Option<&str> {
        self.0
            .get("nodeId")
            .or_else(|| self.0.get("node_id"))
            .and_then(Value::as_str)
    }
    /// The signal kind tag (`form`, `api_endpoint`, `live_socket`, ...).
    pub fn kind(&self) -> Option<&str> {
        self.0.get("kind").and_then(Value::as_str)
    }
    /// The public mount path, for public-entry / live kinds.
    pub fn mount_path(&self) -> Option<&str> {
        self.0.get("mount_path").and_then(Value::as_str)
    }
    /// The plaintext api key, if the signal declared api_key auth (surfaced
    /// once in the enumeration payload under `auth.secret`).
    pub fn api_key(&self) -> Option<&str> {
        self.0
            .get("auth")
            .and_then(|a| a.get("secret"))
            .and_then(Value::as_str)
    }
    /// The form schema, for `form` kinds.
    pub fn form_schema(&self) -> Option<&Value> {
        self.0.get("formSchema").or_else(|| self.0.get("form_schema"))
    }
    /// True for a RESUME signal (a one-shot reply to a paused execution), false
    /// for an ENTRY trigger (fireable repeatedly to start a run). The consumer
    /// splits its list on this: "Triggers" vs "Tasks".
    pub fn is_resume(&self) -> Option<bool> {
        self.0.get("isResume").and_then(Value::as_bool)
    }
}

/// Mint an api token scoped to a single project, so enumeration through it
/// returns ONLY that project's signals. The dispatcher filters the enumeration
/// server-side by the token's `allowedProjects` (a SQL pre-filter on the signal
/// row's project_id), which is the system's own isolation mechanism: it is what
/// keeps one project's consumer from seeing another's signals. The rig leans on
/// the same gate instead of best-effort filtering the (often project_id-less)
/// consumer payload after the fact. Localhost-gated mint, so this only works
/// against the local dev dispatcher (the rig's target).
pub async fn mint_project_token(
    disp: &Dispatcher,
    project_id: &uuid::Uuid,
    name: &str,
) -> Result<String> {
    let body = json!({
        "name": name,
        "style": "hard",
        "allowedProjects": [project_id.to_string()],
        "allowedTags": [],
    });
    let resp: Value = disp.post_json("/signal-tokens", &body).await?;
    resp.get("token")
        .and_then(Value::as_str)
        .map(str::to_string)
        .context("mint token response missing `token`")
}

/// Enumerate every signal a signal token can see. The token authenticates via
/// the `Authorization: Bearer` header (never a URL path).
pub async fn list_signals(disp: &Dispatcher, signal_token: &str) -> Result<Vec<DiscoveredSignal>> {
    let arr: Vec<Value> = disp
        .get_json_bearer("/signal-token/signals", signal_token)
        .await?;
    Ok(arr.into_iter().map(DiscoveredSignal).collect())
}

/// A project-scoped discovery handle: a token minted ONCE for one project,
/// reused across an entire poll-wait. Minting is a DB write, and a wait can poll
/// for up to a minute, so minting per poll iteration would litter the token
/// table with ~150 throwaway rows per wait. Minting once and reusing the handle
/// keeps it to one row per wait. The token's `allowedProjects` scope is what
/// isolates this project's signals from any other (see [`mint_project_token`]).
pub struct SignalScope {
    disp: Dispatcher,
    token: String,
    project_id: uuid::Uuid,
}

impl SignalScope {
    /// Mint the project-scoped token for `project_id`. Call once, then `find`
    /// against the returned scope as many times as a poll loop needs.
    pub async fn open(disp: &Dispatcher, project_id: &uuid::Uuid) -> Result<Self> {
        let token = mint_project_token(disp, project_id, "weft-e2e-discover").await?;
        Ok(Self {
            disp: disp.clone(),
            token,
            project_id: *project_id,
        })
    }

    /// Enumerate this scope's signals and pick the single one matching `pred`.
    /// Separates the two outcomes a poll loop treats differently: `Ok(None)`
    /// means "zero matches yet" (not registered, keep polling) and `Err` means
    /// "more than one matched" (ambiguous fixture, loud immediately). A real
    /// enumeration failure (HTTP / decode) propagates as `Err` from the `?`
    /// before the count check, so it too surfaces at once, never as a timeout.
    /// `what` names the thing being matched, for the ambiguity error.
    async fn find(
        &self,
        what: &str,
        pred: impl Fn(&DiscoveredSignal) -> bool,
    ) -> Result<Option<DiscoveredSignal>> {
        let all = list_signals(&self.disp, &self.token).await?;
        let mut matches = all.into_iter().filter(|s| pred(s));
        let first = matches.next();
        match (first, matches.next()) {
            (None, _) => Ok(None),
            (Some(sig), None) => Ok(Some(sig)),
            (Some(_), Some(_)) => bail!(
                "expected exactly one {what} for project {}, found more than one \
                 (ambiguous fixture)",
                self.project_id
            ),
        }
    }

    /// The single signal whose trigger node is `node_id` (`Ok(None)` = none yet).
    pub async fn signal_for_node(&self, node_id: &str) -> Result<Option<DiscoveredSignal>> {
        self.find(&format!("signal for node '{node_id}'"), |s| {
            s.node_id() == Some(node_id)
        })
        .await
    }
}

/// Fire a public-entry webhook by mount path with a JSON body, passing the api
/// key header if `api_key` is set. The mount path is the signal's `mount_path`
/// (with or without a leading slash; normalized here).
pub async fn fire_webhook(
    disp: &Dispatcher,
    mount_path: &str,
    body: &Value,
    api_key: Option<&str>,
) -> Result<()> {
    let path = format!("/{}", mount_path.trim_start_matches('/'));
    // Build the request directly so we can attach the optional header; the
    // shared client's post_empty doesn't carry custom headers.
    let url = format!("{}{}", disp.base(), path);
    let client = reqwest::Client::new();
    let mut req = client.post(&url).json(body);
    if let Some(key) = api_key {
        req = req.header("X-Api-Key", key);
    }
    let resp = req.send().await.with_context(|| format!("POST {url}"))?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if status.is_success() {
        Ok(())
    } else {
        bail!("POST {url} -> HTTP {status}: {text}")
    }
}

/// Fire a token signal (`POST /signal/{token}`) with a JSON body. Used for form
/// submissions and any task-callback fire.
pub async fn fire_token(disp: &Dispatcher, token: &str, body: &Value) -> Result<()> {
    disp.post_empty(&format!("/signal/{token}"), body).await
}
