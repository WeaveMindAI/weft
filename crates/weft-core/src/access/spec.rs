//! `AccessSpec`: the author-side recipe for PERSONAL accesses.
//!
//! A service is 100% declared data: pick an acquisition, list the auth
//! steps, list the permission catalogue, declare the doors, done. The
//! only code lives in weft's closed vocabularies (the acquisition
//! engines, the `Sign` kinds, the picker kinds), shared by every
//! service; there is never per-service Rust. If a service genuinely
//! cannot be declared, the answer is a new typed variant here, added
//! deliberately, never a per-service code hook.
//!
//! The spec lives in the access node's `metadata.json` under the
//! `service` key. The editor ships it to the store at connect time; the
//! store snapshots it on the grant row so run-time refresh needs no
//! catalog access.
//!
//! A credential of ANY shape fits: a bearer token, a key pair, or a
//! set of connection settings that no HTTP request transform can
//! express (a mail server's host + user + password, a database's
//! connection settings). `auth` declares how to sign a REQUEST, and a
//! service with nothing to sign simply declares none; what a
//! connection hands over is decided separately (see
//! [`AccessSpec::handoff_values`]).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The recipe for one service's personal accesses: how a grant is
/// acquired, how a request through it is authenticated, and how grants
/// coexist across projects.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessSpec {
    /// The service's identity in the store and on the wire
    /// (`[a-z0-9_]+`, same charset as provider names).
    pub service: String,
    /// Display name ("Slack"); defaults to `service`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// How grants of this service coexist across projects. A PROVIDER
    /// property, not a user choice: `coexisting` (Google-class, one
    /// token per consent, strict per-project grants) or `exclusive`
    /// (Slack-bot / GitHub-App class: structurally ONE grant per
    /// app+account, re-consent ROTATES it, so the grant is honestly
    /// shared and grows by explicit scope-union upgrades only).
    #[serde(default)]
    pub grants: GrantCoexistence,
    pub acquisition: Acquisition,
    /// How a request through this access is authenticated, applied by
    /// the client in the worker. Empty for a service whose credential
    /// is not a request transform at all (connection settings for a
    /// mail server, a database): those connections carry values and
    /// sign nothing.
    #[serde(default)]
    pub auth: Vec<AuthStep>,
    /// Declarative connect-time check: the store makes this call with
    /// the freshly acquired values and refuses the grant unless the
    /// status matches. Also the place identity captures usually hang.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub test: Option<TestCall>,
    /// Which stored value names a display identity is assembled from,
    /// as a template over captured/stored values ("{team} / {user}").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<Template>,
    /// The connect doors this service offers. `own` is the page where
    /// the user brings (or creates) their own credential; `shared` is
    /// the one-click door on a credential weft itself holds (a
    /// registered app for a consent service, the runtime's key for a
    /// pasted-key service). Hidden doors are not rendered. Defaults to
    /// `[own]`: every service can at least take the user's own
    /// credential, and offering `shared` is a deliberate declaration.
    #[serde(default = "default_doors", skip_serializing_if = "is_default_doors")]
    pub doors: Vec<Door>,
    /// The provider refuses plain-http OAuth callback URLs (Slack
    /// does; Google takes a loopback). Declaring it makes every
    /// consent for this service use an https address, failing loudly
    /// when this weft has none.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub callback_https: bool,
    /// What the "Your own" door renders beyond the paste fields (which
    /// are derived from the acquisition, see [`Self::own_fields`]): an
    /// optional create-it-for-me mint, and an optional generated guide.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub own_page: Option<OwnPage>,
    /// The service's permission catalogue, each entry with a human
    /// sentence. A description of the service, identical everywhere;
    /// which of these a REGISTERED app may ask for is that app's
    /// `covers` set in the apps file, never a flag here. Empty for
    /// services with no permission concept.
    ///
    /// Deliberately NOT exhaustive on providers with a huge scope pool
    /// (Google class): list only the permissions shipped nodes
    /// genuinely use, and grow the list with the node that needs the
    /// next one. `all_permissions_url` is what keeps that honest.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub permissions: Vec<Permission>,
    /// Where the PROVIDER's complete permission list lives, for
    /// services whose catalogue above is a curated subset. When set,
    /// the permission picker tells the user: a permission missing here
    /// can be found at this address and added to the access node's
    /// metadata. Absent = the catalogue IS the complete set, and no
    /// such hint is shown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub all_permissions_url: Option<String>,
    /// Where the ticked permissions take effect for this service, which
    /// decides where the editor renders the picker.
    #[serde(default, skip_serializing_if = "PermissionTiming::is_none")]
    pub permission_timing: PermissionTiming,
    /// What the connect-time check can actually learn about a fresh
    /// credential, and what running it costs. Decides whether the
    /// recorded permissions are VERIFIED (the provider stated them) or
    /// CLAIMED (the user says so), which in turn decides how hard a
    /// later shortfall fails.
    #[serde(default, skip_serializing_if = "Verification::is_default")]
    pub verification: Verification,
    /// Service-specific wording for how to recover a dead connection,
    /// appended to the reconnect error ("re-invite the bot, then
    /// reconnect"). Absent = the generic reconnect message alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconnect_action: Option<String>,
    /// How this service REPORTS events, when it does: named TOPICS,
    /// each a full recipe (fields, account, transports). A map
    /// because one provider genuinely reports along independent
    /// topologies at once. Empty = the service reports nothing, so a
    /// trigger on it cannot be served and says so at registration.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub events: BTreeMap<String, crate::access::events::EventsSpec>,
    /// Named groups of OPTIONAL fields, for a service whose optional
    /// fields come in sets that each unlock one capability (a
    /// mailbox's receiving server and its sending server).
    ///
    /// Declaring this imposes EXACTLY TWO rules on every connect,
    /// both enforced by [`capability_shortfall`], nothing implicit
    /// beyond them:
    ///
    /// 1. Each group is ALL-OR-NOTHING: fill every field of a group
    ///    or none of them. A partially-filled group is refused,
    ///    naming the missing fields and the group's label.
    /// 2. AT LEAST ONE group must be completely filled, so a stored
    ///    connection can always do at least one thing. Filling none
    ///    is refused, listing the labels.
    ///
    /// Fields no group names are untouched by both rules. Empty = the
    /// service declares no such rule and neither applies.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<Capability>,
}

/// One all-or-nothing group of optional fields; see
/// [`AccessSpec::capabilities`] for the two rules a group carries.
// SYNC: Capability <-> packages/weft-graph/src/protocol.ts Capability
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capability {
    /// What filling this group lets the connection do, in the words
    /// the refusals use ("receive mail"): the thing the user gains,
    /// never a field name.
    pub label: String,
    /// The field names that stand or fall together. Each must be a
    /// field the acquisition declares `optional` (validated at
    /// metadata load: a required field could never be the missing
    /// one, and an undeclared name could never be filled).
    pub fields: Vec<String>,
}

/// One connect door; see [`AccessSpec::doors`].
// SYNC: Door <-> packages/weft-graph/src/protocol.ts Door
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Door {
    /// One click on a credential weft holds: a registered app for a
    /// consent service, the runtime's own credential (spending its
    /// credit) for a key service.
    Shared,
    /// The user brings their own: one page with an optional mint
    /// button, an optional generated guide, and the paste fields.
    Own,
}

fn default_doors() -> Vec<Door> {
    vec![Door::Own]
}

fn is_default_doors(doors: &[Door]) -> bool {
    doors == [Door::Own]
}

/// The optional parts of the "Your own" page beyond its paste fields.
/// There is deliberately no mode switch: a page renders whichever of
/// these exist, plus the fields, as ONE form.
// SYNC: OwnPage <-> packages/weft-graph/src/protocol.ts OwnPage
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnPage {
    /// We can create the app for them: the provider's app-manifest
    /// endpoint and the payload to send. Renders "Create it for me".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mint: Option<MintApp>,
    /// How to create one by hand, generated from the ticked
    /// permissions. Renders foldable, UNFOLDED by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guide: Option<Guide>,
    /// "I already have a credential": paste it directly, no app.
    /// Renders as one more section of the same page; submitting it
    /// stores a connection whose snapshot is a `Static` acquisition
    /// over these fields (see [`AccessSpec::paste_variant`]). For a
    /// service whose primary acquisition is already `Static` this is
    /// redundant and refused at validation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paste: Option<Paste>,
}

/// The paste-a-credential section of the "Your own" page.
// SYNC: Paste <-> packages/weft-graph/src/protocol.ts OwnPageWire.paste
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Paste {
    /// What the user pastes (a bot token, a key pair).
    pub fields: Vec<CredentialField>,
}

/// A provider's create-an-app-by-API recipe (Slack/Zoom manifest class).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MintApp {
    /// POST target on the provider's API.
    pub url: String,
    /// The manifest payload. Any string value equal to
    /// `"{permissions}"` is replaced by the ticked permission ids
    /// (as a JSON array) before sending.
    pub payload: Value,
    /// Where the minted app's credentials sit in the response
    /// (`client_id` and `client_secret` captures at minimum).
    pub captures: Vec<Capture>,
}

/// The hand-creation guide on the "Your own" page. `steps` may embed
/// `{permissions}`, replaced by the ticked permissions' labels; see
/// [`AccessSpec::guide_steps`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Guide {
    /// A pre-filled creation link (a manifest / template URL). May
    /// embed `{permissions}` (ticked ids, comma-joined, url-encoded).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link: Option<String>,
    /// Ordered tutorial steps.
    pub steps: Vec<String>,
}

/// One entry of a service's permission catalogue.
// SYNC: Permission <-> packages/weft-graph/src/protocol.ts Permission
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Permission {
    /// The provider's own id for it (an OAuth scope string, a bot
    /// permission name). What consent URLs carry and `covers` names.
    pub id: String,
    /// Short human label ("Read files").
    pub label: String,
    /// One human sentence saying what ticking it allows.
    pub description: String,
    /// Starts ticked on the picker.
    #[serde(default)]
    pub default: bool,
}

/// Where a service's permissions take effect; drives where the editor
/// renders the picker.
// SYNC: PermissionTiming <-> packages/weft-graph/src/protocol.ts PermissionTiming
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionTiming {
    /// Baked into the minted/created app (Slack manifest class).
    AtMint,
    /// Configured on the app at the provider's site (GitHub App class).
    AtApp,
    /// Chosen on the consent screen (Google class).
    AtApprove,
    /// Both on the app and re-narrowed at consent.
    Both,
    /// The service has no permission concept (a plain key).
    #[default]
    None,
}

impl PermissionTiming {
    fn is_none(&self) -> bool {
        matches!(self, PermissionTiming::None)
    }
}

/// What connect-time verification can learn, and what it costs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Verification {
    #[serde(default)]
    pub rung: VerificationRung,
    #[serde(default)]
    pub cost: VerificationCost,
}

impl Verification {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

/// The verification ladder, top rung first. The top two rungs are
/// authoritative (the provider states what the credential holds), the
/// middle proves only liveness, `probe` infers from a real call's
/// refusal, and `silent` learns nothing (the permissions stay claimed).
// SYNC: VerificationRung <-> packages/weft-graph/src/protocol.ts VerificationRung
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationRung {
    /// The provider states what it granted (a granted-scope echo, an
    /// app-permissions endpoint).
    ReportsPermissions,
    /// The credential describes itself (a key-introspection endpoint).
    SelfIntrospect,
    /// Only "alive", never "what".
    ReportsValidity,
    /// A real call whose refusal is read back.
    Probe,
    /// Nothing knowable; the permissions are recorded as claimed.
    #[default]
    Silent,
}

impl VerificationRung {
    /// Does this rung make the recorded permission set AUTHORITATIVE
    /// (a later shortfall may hard-fail)? Only the top two do; a
    /// liveness check or a probe proves nothing about the full set.
    pub fn is_authoritative(self) -> bool {
        matches!(self, Self::ReportsPermissions | Self::SelfIntrospect)
    }
}

/// What running the verification costs. `paid` means the check is
/// NEVER run automatically: the credential is recorded as claimed and
/// the first real call surfaces the truth.
// SYNC: VerificationCost <-> packages/weft-graph/src/protocol.ts VerificationCost
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationCost {
    #[default]
    Free,
    /// May or may not bill depending on account state; treated as paid.
    Ambiguous,
    Paid,
}

impl VerificationCost {
    /// May the check run without anyone paying? `ambiguous` counts as
    /// no: a check that MIGHT bill is one we never auto-run.
    pub fn may_auto_run(self) -> bool {
        matches!(self, Self::Free)
    }
}

/// Env var naming the json file that holds the OAuth apps
/// (`{ "<service>": { "client_id": ..., "client_secret": ... } }`), used
/// when a project declares no app of its own. Named here because both
/// the runtime that reads the file and the tooling that installs it key
/// off the same variable.
// SYNC: APPS_FILE_ENV <-> deploy/k8s/broker.yaml (WEFT_ACCESS_APPS_FILE env +
//       the access-apps volume it points into)
pub const APPS_FILE_ENV: &str = "WEFT_ACCESS_APPS_FILE";

/// The credentials of a service's OAuth app: the client id, the secret
/// (absent for a public PKCE client), and any `registration_fields`
/// extras the service declares. Keyed by service name in
/// [`crate::node::NodeMetadata::access_apps`], where a project (or a
/// package root, inherited by every member) declares the app it uses.
///
/// Resolved at connect (by door: the registered apps file for the
/// shared door, the user's pasted app or the project's public app for
/// the own door) and snapshotted onto the grant so runtime refresh
/// reads it without a separate lookup.
// SYNC: AppRegistration <-> packages/weft-graph/src/protocol.ts AppRegistration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppRegistration {
    /// The app's display name, shown as the connection list's middle
    /// column so two connections to one account are distinguishable
    /// ("Google Drive", "Acme internal app"). Mandatory: an unlabelled
    /// app is useless in a dropdown. A label is a name and nothing
    /// more; it never describes who runs the app.
    pub label: String,
    pub client_id: String,
    /// Absent for a public (PKCE, no-secret) client.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<String>,
    /// Extra registration values a service declares beyond id + secret
    /// (e.g. a Zoom `account_id`), keyed by the `registration_fields`
    /// name. Flattened so the app reads as one flat object.
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, String>,
}

impl AppRegistration {
    /// The credential values as one map (`client_id`, `client_secret`
    /// when set, plus extras), the shape the token/auth machinery
    /// interpolates over.
    pub fn to_values(&self) -> BTreeMap<String, String> {
        let mut m = self.extra.clone();
        m.insert("client_id".to_string(), self.client_id.clone());
        if let Some(secret) = &self.client_secret {
            m.insert("client_secret".to_string(), secret.clone());
        }
        m
    }

    /// Refuse an app missing a declared `registration_field`. `client_id`
    /// is structurally required (the type demands it); this checks the
    /// service's declared extras are all present, so a half-filled app
    /// fails at connect with a clear message, not at the token call.
    pub fn validate(&self, fields: &[CredentialField]) -> Result<(), String> {
        if self.label.trim().is_empty() {
            return Err("the app needs a name (shown in the connection list)".to_string());
        }
        if self.client_id.trim().is_empty() {
            return Err("the app's client_id is empty".to_string());
        }
        for f in fields {
            if !f.optional && !self.extra.contains_key(&f.name) {
                return Err(format!("the app is missing the required field '{}'", f.name));
            }
        }
        Ok(())
    }
}

/// How grants of a service coexist across projects; see
/// [`AccessSpec::grants`].
// SYNC: GrantCoexistence <-> packages/weft-graph/src/protocol.ts GrantCoexistence
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GrantCoexistence {
    #[default]
    Coexisting,
    Exclusive,
}

/// One pasted credential field on the connect form. Values go
/// editor -> store directly and NEVER into node config (node config
/// rides the journal in plaintext).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialField {
    /// The stored value's name; what templates interpolate.
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// The connect accepts this field empty (an app-level token only
    /// some uses of the service need). An optional field left blank
    /// simply stores nothing; whatever needs it later says exactly
    /// which value is missing.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub optional: bool,
    /// Render as a password field and treat as secret. Default true:
    /// a pasted credential is a secret unless the author says
    /// otherwise (a region name, an account id).
    #[serde(default = "default_true")]
    pub secret: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placeholder: Option<String>,
}

fn default_true() -> bool {
    true
}

/// How a grant's stored values are acquired.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Acquisition {
    /// The user pastes the declared fields (a bot token, an API key
    /// pair). Scoping, if any, happened on the provider's own site, so
    /// a static spec never declares scopes.
    Static { fields: Vec<CredentialField> },
    /// OAuth2. The connect uses an app (client id + secret, plus any
    /// `registration_fields` extras): the door decides whose. The
    /// ticked permissions (from [`AccessSpec::permissions`]) drive the
    /// consent URL.
    #[serde(rename = "oauth2")]
    OAuth2 {
        grant: OAuthGrant,
        token_url: String,
        /// How ticked permission ids join in the consent URL. RFC
        /// default is a space; Slack-class providers use a comma.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        scope_delimiter: Option<String>,
        /// Extra CONSENT-URL params (Google's `access_type=offline` +
        /// `prompt=consent` for a refresh token). Plain strings; the
        /// consent URL has no stored values to interpolate.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        auth_params: BTreeMap<String, String>,
        /// Extra token-request params some providers demand (Zoom S2S
        /// `account_id`, audience params). Values are templates over
        /// registration values.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        extra_params: BTreeMap<String, Template>,
        /// Registration fields beyond the implicit client id + secret.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        registration_fields: Vec<CredentialField>,
        /// Values captured off the token response (Salesforce
        /// `instance_url`, Slack team/bot ids, the granted-scopes echo).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        captures: Vec<Capture>,
        /// How a stale grant is RENEWED, for providers whose renewal
        /// is not the standard refresh-token POST (Meta's long-lived
        /// token exchange: a GET interpolating the current token).
        /// A declared authenticated call, run at refresh time over
        /// the stored values plus the app's registration values; its
        /// captures write back like any acquisition's (capture the
        /// fresh credential under the name the auth steps
        /// interpolate), and a top-level `expires_in` in the answer
        /// sets the next expiry. Absent = the standard renewal for
        /// the grant kind (refresh_token / re-request).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        refresh: Option<ConnectCall>,
    },
    /// The runtime supplies the credential at call time: nothing is
    /// stored on the connection, nothing is validated at connect, and
    /// resolution asks the runtime's credential source (which reads its
    /// configured key: the shared-credentials file's `api_key` entry).
    /// The one-click door of a key service; the stored row only marks
    /// that the user picked it. Calls on it spend the runtime's credit,
    /// so the connect is what shows the spends-credits mark.
    Runtime {},
    /// Mint a short-lived JWT from a pasted private key, optionally
    /// exchange it for an installation/service token (GitHub App,
    /// Google service accounts). Minting happens lazily at resolution
    /// time, like a refresh.
    MintJwt {
        fields: Vec<CredentialField>,
        #[serde(default)]
        algorithm: JwtAlgorithm,
        /// JWT claims; values are templates over the pasted fields
        /// (`{"iss": "{app_id}"}`). `iat`/`exp` are set by the runtime.
        claims: BTreeMap<String, Template>,
        /// JWT lifetime. GitHub caps app JWTs at 600s.
        #[serde(default = "default_jwt_ttl")]
        jwt_ttl_secs: u64,
        /// Exchange the JWT for the token the auth steps interpolate.
        /// Absent = the JWT itself is stored as `token`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exchange: Option<TokenExchange>,
    },
}

fn default_jwt_ttl() -> u64 {
    600
}

/// The OAuth2 grant flow.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OAuthGrant {
    /// Browser consent -> code -> token. The self-host flow weft ships.
    AuthorizationCode {
        auth_url: String,
        /// PKCE on by default; a provider that rejects it opts out.
        #[serde(default = "default_true")]
        pkce: bool,
    },
    /// Server-to-server: token requested directly from client
    /// credentials, re-requested on expiry, no refresh token, no
    /// consent page (Zoom S2S, client-credential APIs).
    ClientCredentials,
}

/// One JSON-path capture off a token/test response, stored as a named
/// value templates can interpolate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capture {
    pub name: String,
    /// Dotted path into the response JSON (`team.name`,
    /// `response_metadata.next_cursor`, `authed_user.id`). Array
    /// indices are numeric segments (`items.0.id`).
    pub path: String,
    /// A missing path fails the acquisition loudly by default; an
    /// `optional` capture just stays unset.
    #[serde(default)]
    pub optional: bool,
}

/// Exchange a minted JWT for the working token (`MintJwt.exchange`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenExchange {
    /// POST target; a template over the pasted fields
    /// (`https://api.github.com/app/installations/{installation_id}/access_tokens`).
    pub url: Template,
    /// Path to the token in the response; stored as `token`.
    pub token_path: String,
    /// Path to the token's expiry (RFC3339 string or epoch seconds);
    /// absent = re-mint on every resolution past the JWT's own ttl.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_path: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub captures: Vec<Capture>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum JwtAlgorithm {
    #[default]
    RS256,
}

/// The connect-time verification call.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestCall {
    pub url: Template,
    #[serde(default)]
    pub method: TestMethod,
    /// The status that proves the credential works. Default 200.
    #[serde(default = "default_expect_status")]
    pub expect_status: u16,
    /// Identity/context values captured off the test response.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub captures: Vec<Capture>,
}

fn default_expect_status() -> u16 {
    200
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum TestMethod {
    #[default]
    Get,
    Post,
}

impl TestMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            TestMethod::Get => "GET",
            TestMethod::Post => "POST",
        }
    }
}

/// One request transform: how a call through the access is
/// authenticated. ~5 verbs cover the surveyed services; a mechanism
/// none expresses becomes a new typed variant, never author code.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuthStep {
    /// `Authorization: Bearer {token}`, `X-Shopify-Access-Token:
    /// {token}`, `Bot {token}` ... any header.
    Header { name: String, value: Template },
    /// `?key={token}` style query parameter.
    Query { name: String, value: Template },
    /// HTTP Basic in its four survey flavors (Stripe `key:`, Twilio
    /// `sid:token`, Mailgun `api:key`, Zendesk `email/token:token`).
    Basic { username: Template, password: Template },
    /// Prefix the URL path (Telegram `/bot{token}/`). Applied between
    /// the host and the path the node wrote.
    PathPrefix { value: Template },
    /// Replace the request URL's base (scheme + host + port, plus any
    /// path prefix the template carries) with a STORED value, keeping
    /// the path + query the node wrote. For services whose API base is
    /// per-account (an S3-compatible endpoint, Salesforce's
    /// `instance_url` captured at auth time): node code addresses a
    /// stand-in base (`https://service/...`) and this step aims it.
    BaseUrl { value: Template },
    /// Per-request cryptographic signing; mathematically inexpressible
    /// as static injection, so each scheme is a typed variant
    /// implemented in weft.
    Sign { with: SignKind },
}

/// The per-request signing schemes weft implements.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SignKind {
    /// AWS Signature V4: S3, R2, every S3-compatible store.
    #[serde(rename = "sigv4")]
    SigV4 {
        /// The AWS service name signed for ("s3").
        service: String,
        region: Template,
        access_key_id: Template,
        secret_access_key: Template,
    },
    /// OAuth 1.0a request signing (X/Twitter).
    #[serde(rename = "oauth1a")]
    OAuth1a {
        consumer_key: Template,
        consumer_secret: Template,
        token: Template,
        token_secret: Template,
    },
}

/// An authenticated HTTP call declared as data, whose answer feeds
/// the next step: the industry "ask the API where to connect, then
/// connect there" pattern (Slack's `apps.connections.open`, AWS
/// AppSync, Dialpad), and equally the "tell the API where to send
/// events" pattern (Google's `files.watch`, Telegram's `setWebhook`).
/// One shape for both, because both are "POST this body with these
/// credentials and read these values out of the answer".
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectCall {
    /// The address, as a template over the caller's values (a watch
    /// call addresses `.../files/{target}/watch`).
    pub url: Template,
    #[serde(default)]
    pub method: TestMethod,
    /// JSON body to send. Any string leaf is a template over the
    /// caller's values; absent = no body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<Value>,
    /// How this call authenticates, in the same vocabulary as a
    /// service's own `auth`: templates over the values the caller
    /// resolved (a socket mint uses `Bearer {app_token}`, which is a
    /// DIFFERENT stored value from the API token).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub auth: Vec<AuthStep>,
    /// Values read out of the answer and kept for the steps that
    /// follow (the socket URL, the subscription's expiry).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub captures: Vec<Capture>,
}

impl ConnectCall {
    /// Every value name this call interpolates (url + body + auth),
    /// which is exactly the handoff set it needs resolved. Also the
    /// validator: a malformed template is an `Err`.
    pub fn value_names(&self) -> Result<Vec<String>, String> {
        let mut names = self.url.placeholders()?;
        for name in worker_value_names_of(&self.auth)? {
            if !names.contains(&name) {
                names.push(name);
            }
        }
        if let Some(body) = &self.body {
            for name in template_names_in(body)? {
                if !names.contains(&name) {
                    names.push(name);
                }
            }
        }
        Ok(names)
    }

    /// Refuse a malformed call, naming the site (`what`) so the
    /// message says which recipe is wrong.
    pub fn validate(&self, what: &str) -> Result<(), String> {
        self.value_names().map_err(|e| format!("{what}: {e}"))?;
        validate_captures(&self.captures).map_err(|e| format!("{what}: {e}"))?;
        Ok(())
    }
}

/// Every placeholder name a JSON body's string leaves interpolate.
fn template_names_in(body: &Value) -> Result<Vec<String>, String> {
    let mut names = Vec::new();
    let mut stack = vec![body];
    while let Some(v) = stack.pop() {
        match v {
            Value::String(s) => {
                for name in Template::new(s.clone()).placeholders()? {
                    if !names.contains(&name) {
                        names.push(name);
                    }
                }
            }
            Value::Array(items) => stack.extend(items.iter()),
            Value::Object(map) => stack.extend(map.values()),
            _ => {}
        }
    }
    Ok(names)
}

/// Interpolate every string leaf of a JSON body against `values`.
/// The body counterpart of [`Template::resolve`], so a declared
/// request body reads values exactly the way a URL or a header does.
pub fn resolve_body(
    body: &Value,
    values: &BTreeMap<String, String>,
) -> Result<Value, String> {
    Ok(match body {
        Value::String(s) => Value::String(Template::new(s.clone()).resolve(values)?),
        Value::Array(items) => Value::Array(
            items.iter().map(|v| resolve_body(v, values)).collect::<Result<_, _>>()?,
        ),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| Ok((k.clone(), resolve_body(v, values)?)))
                .collect::<Result<serde_json::Map<_, _>, String>>()?,
        ),
        other => other.clone(),
    })
}

/// A declarative acknowledgement rule for a held socket: when an
/// inbound frame carries something at `when_field`, send `frame` back
/// with `{value}` replaced by what was found there.
///
/// This is the generic shape of every ack protocol surveyed (Slack's
/// envelope_id echo, a gateway's sequence ack): the language owns
/// "read a field, echo it in a frame", and WHICH field and WHICH
/// frame are the service's declared data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplyRule {
    /// Dotted path into the inbound frame. No value there = this rule
    /// does not apply to this frame.
    pub when_field: String,
    /// The frame to send, with `{value}` replaced by the found value.
    pub frame: String,
}

impl ReplyRule {
    /// The frame to send for an inbound payload, or `None` when this
    /// rule does not apply. Pure: the whole rule, testable without a
    /// socket.
    pub fn reply_to(&self, inbound: &Value) -> Option<String> {
        let found = lookup_path(inbound, &self.when_field)?;
        let value = match found {
            Value::String(s) => s.clone(),
            Value::Null => return None,
            other => other.to_string(),
        };
        Some(self.frame.replace("{value}", &value))
    }
}

/// The capture name a minted socket URL lands under by default.
pub const DEFAULT_URL_FROM: &str = "url";

fn default_url_from() -> String {
    DEFAULT_URL_FROM.to_string()
}

fn is_default_url_from(s: &str) -> bool {
    s == DEFAULT_URL_FROM
}

/// A socket whose address is MINTED by an authenticated call: the
/// call, which of its captures carries the address, and the
/// acknowledgements the gateway demands. The one shape behind every
/// "ask the API where to connect, then connect there" socket,
/// flattened into the recipes that dial out so their wire fields stay
/// flat.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MintedSocket {
    /// The authenticated call that answers the socket address. Runs
    /// again on every reconnect (these addresses are single-use).
    /// Optional because one carrier pairs it with a static URL; a
    /// carrier that cannot dial without it enforces presence in its
    /// own validation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect: Option<ConnectCall>,
    /// Which capture of `connect` carries the address.
    #[serde(default = "default_url_from", skip_serializing_if = "is_default_url_from")]
    pub url_from: String,
    /// Declarative acknowledgements: for every inbound frame, each
    /// matching rule sends one frame back.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replies: Vec<ReplyRule>,
}

impl Default for MintedSocket {
    fn default() -> Self {
        Self { connect: None, url_from: default_url_from(), replies: Vec::new() }
    }
}

impl MintedSocket {
    /// Refuse a malformed minted socket, naming the site (`what`) so
    /// the message says which recipe is wrong.
    pub fn validate(&self, what: &str) -> Result<(), String> {
        if let Some(call) = &self.connect {
            call.validate(&format!("{what}.connect"))?;
            if !call.captures.iter().any(|c| c.name == self.url_from) {
                return Err(format!(
                    "{what}.connect captures nothing named '{}', so the minted socket \
                     address would be unknown; capture it (or point `url_from` at the \
                     capture that carries it)",
                    self.url_from
                ));
            }
        }
        for rule in &self.replies {
            if rule.when_field.trim().is_empty() {
                return Err(format!("{what}.replies needs a non-empty when_field"));
            }
        }
        Ok(())
    }
}

/// A string interpolating STORED VALUES by name: `"Bearer {token}"`,
/// `"/bot{token}/"`. The only way a spec references a value; names
/// resolve against the grant's stored values at apply time, and an
/// unknown name is a loud error. `{` and `}` have no escape: a template
/// is an interpolation string, not general text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Template(pub String);

impl Template {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// The value names this template interpolates, in order of first
    /// appearance. Also the validator: a malformed placeholder is an
    /// `Err`. This is what decides which stored values a worker may
    /// receive: exactly the names the declared steps interpolate,
    /// nothing more.
    pub fn placeholders(&self) -> Result<Vec<String>, String> {
        let mut names = Vec::new();
        let mut rest = self.0.as_str();
        while let Some(start) = rest.find('{') {
            let after = &rest[start + 1..];
            let Some(end) = after.find('}') else {
                return Err(format!("unclosed '{{' in template {:?}", self.0));
            };
            let name = &after[..end];
            if name.is_empty()
                || !name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
            {
                return Err(format!(
                    "bad placeholder '{{{name}}}' in template {:?}: names are [a-z0-9_]+",
                    self.0
                ));
            }
            if !names.iter().any(|n| n == name) {
                names.push(name.to_string());
            }
            rest = &after[end + 1..];
        }
        if rest.contains('}') {
            return Err(format!("stray '}}' in template {:?}", self.0));
        }
        Ok(names)
    }

    /// Interpolate against `values`. Unknown names are loud errors
    /// (never an empty substitution: a blank secret spliced into an
    /// auth header is a silent authentication failure).
    pub fn resolve(&self, values: &BTreeMap<String, String>) -> Result<String, String> {
        let mut out = String::with_capacity(self.0.len());
        let mut rest = self.0.as_str();
        while let Some(start) = rest.find('{') {
            out.push_str(&rest[..start]);
            let after = &rest[start + 1..];
            let Some(end) = after.find('}') else {
                return Err(format!("unclosed '{{' in template {:?}", self.0));
            };
            let name = &after[..end];
            match values.get(name) {
                Some(v) => out.push_str(v),
                None => {
                    return Err(format!(
                        "template references '{{{name}}}' but the access stores no value \
                         named '{name}'"
                    ))
                }
            }
            rest = &after[end + 1..];
        }
        out.push_str(rest);
        Ok(out)
    }
}

/// Look a dotted [`Capture::path`] up in a JSON value. Numeric
/// segments index arrays. The one path-lookup used by every capture
/// site (store acquisition, test calls, lookup pagination).
pub fn lookup_path<'v>(value: &'v Value, path: &str) -> Option<&'v Value> {
    let mut cur = value;
    for seg in path.split('.') {
        cur = match cur {
            Value::Object(map) => map.get(seg)?,
            Value::Array(items) => items.get(seg.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cur)
}

/// Every template a list of auth steps declares, in order. Free so
/// callers holding only the steps (a resolved connection handoff) can
/// compute the same handoff set the spec would.
fn auth_templates_of(steps: &[AuthStep]) -> Vec<&Template> {
    let mut ts = Vec::new();
    for step in steps {
        match step {
            AuthStep::Header { value, .. } | AuthStep::Query { value, .. } => ts.push(value),
            AuthStep::Basic { username, password } => {
                ts.push(username);
                ts.push(password);
            }
            AuthStep::PathPrefix { value } => ts.push(value),
            AuthStep::BaseUrl { value } => ts.push(value),
            AuthStep::Sign { with } => match with {
                SignKind::SigV4 { region, access_key_id, secret_access_key, .. } => {
                    ts.push(region);
                    ts.push(access_key_id);
                    ts.push(secret_access_key);
                }
                SignKind::OAuth1a { consumer_key, consumer_secret, token, token_secret } => {
                    ts.push(consumer_key);
                    ts.push(consumer_secret);
                    ts.push(token);
                    ts.push(token_secret);
                }
            },
        }
    }
    ts
}

/// The ONE credential string behind a set of resolved auth steps, when
/// one exists: exactly one step whose template interpolates exactly one
/// stored value (a bearer key, a bot token). Anything else (two steps,
/// a Basic pair, request signing) has no single string that means
/// anything, and the error says why. DERIVED from the auth shape,
/// never declared; the one rule behind every raw-credential surface
/// (`OpenedConnection::credential`, the picker page's token).
pub fn single_credential<'v>(
    steps: &[AuthStep],
    values: &'v BTreeMap<String, String>,
) -> Result<&'v str, String> {
    let [step] = steps else {
        return Err("its sign-in applies more than one auth step".into());
    };
    let template = match step {
        AuthStep::Header { value, .. }
        | AuthStep::Query { value, .. }
        | AuthStep::PathPrefix { value }
        | AuthStep::BaseUrl { value } => value,
        AuthStep::Basic { .. } => {
            return Err("its sign-in is a username/password pair".into())
        }
        AuthStep::Sign { .. } => {
            return Err("its sign-in signs each request cryptographically".into())
        }
    };
    let names = template.placeholders()?;
    let [name] = names.as_slice() else {
        return Err("its auth step interpolates more than one stored value".into());
    };
    values
        .get(name)
        .map(String::as_str)
        .ok_or_else(|| format!("no value named '{name}' was resolved"))
}

/// Value names the STORE keeps to itself: material weft acquired on
/// the user's behalf to keep the connection alive, which nothing
/// downstream ever needs and which would be a standing credential in
/// a worker's hands.
///
/// A fixed, system-owned list on purpose. These names are written by
/// the store's own acquisition code, never by a node's metadata, so a
/// service recipe (which a project's author can edit) cannot add to
/// it, remove from it, or rename a value into or out of it. That is
/// what makes the wall hold even when the recipe is untrusted.
///
/// App credentials are not listed because they are not stored values
/// at all: they live on the grant's app registration and only ever
/// join a resolution deliberately (an own-door connection's own app).
pub const PRIVATE_VALUE_NAMES: [&str; 1] = ["refresh_token"];

/// The capability rule, pure: which declared group a connect broke.
/// `Ok(())` when every group is either fully filled or fully empty
/// AND at least one is filled (or the service declares no groups).
/// The message names the fix in the user's words.
pub fn capability_shortfall(
    capabilities: &[Capability],
    filled: impl Fn(&str) -> bool,
) -> Result<(), String> {
    if capabilities.is_empty() {
        return Ok(());
    }
    let mut any_complete = false;
    for cap in capabilities {
        let present: Vec<&String> = cap.fields.iter().filter(|f| filled(f)).collect();
        if present.is_empty() {
            continue;
        }
        if present.len() < cap.fields.len() {
            let missing: Vec<&str> = cap
                .fields
                .iter()
                .filter(|f| !filled(f))
                .map(String::as_str)
                .collect();
            return Err(format!(
                "to {}, this connection also needs {}",
                cap.label,
                missing.join(" and ")
            ));
        }
        any_complete = true;
    }
    if !any_complete {
        let choices: Vec<&str> = capabilities.iter().map(|c| c.label.as_str()).collect();
        return Err(format!(
            "this connection would be able to do nothing; fill the fields for at least one \
             of: {}",
            choices.join(", ")
        ));
    }
    Ok(())
}

/// May this stored value ever leave the store? Everything a
/// connection holds may, except the store's own keep-alive material.
///
/// This is the INVERSE of the rule weft first shipped, which handed
/// out only what the HTTP auth steps interpolated. That rule was a
/// wall defined by an accident of the HTTP vocabulary: a connection
/// whose credential is not a request transform (a mail server's
/// host/user/password, a database's connection settings) named
/// nothing, so it handed out nothing, silently. It also pushed
/// authors to write plain configuration into auth steps just to make
/// it travel (the S3 recipe's `endpoint` and `region` are exactly
/// that). Naming what is PRIVATE says the actual intent, holds for
/// credentials of any shape, and cannot be widened by metadata.
pub fn value_is_private(name: &str) -> bool {
    PRIVATE_VALUE_NAMES.contains(&name)
}

/// The stored value names a set of auth steps interpolates, deduped in
/// first-appearance order; see [`AccessSpec::worker_value_names`].
pub fn worker_value_names_of(steps: &[AuthStep]) -> Result<Vec<String>, String> {
    let mut names = Vec::new();
    for t in auth_templates_of(steps) {
        for n in t.placeholders()? {
            if !names.iter().any(|x| x == &n) {
                names.push(n);
            }
        }
    }
    Ok(names)
}

impl AccessSpec {
    /// Every template this spec declares, for validation and for
    /// computing the worker handoff set (the auth steps' placeholders).
    fn auth_templates(&self) -> Vec<&Template> {
        auth_templates_of(&self.auth)
    }

    /// The stored value names a WORKER may receive: everything the
    /// connection holds except the store's own keep-alive material
    /// (see [`value_is_private`]). A connection is the user's, and
    /// what it holds is theirs to use; weft withholds only what it
    /// acquired to keep the connection alive.
    ///
    /// The declared auth steps are still what SIGNS a request; they
    /// are simply no longer what decides visibility, because a
    /// credential that is not a request transform has no auth steps
    /// to be decided by.
    pub fn handoff_values(
        &self,
        stored: &BTreeMap<String, String>,
    ) -> BTreeMap<String, String> {
        stored
            .iter()
            .filter(|(name, _)| !value_is_private(name))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// The stored value names the declared auth steps interpolate:
    /// what a resolution must actually FIND to sign a request. Not a
    /// visibility rule (see [`Self::handoff_values`]); a completeness
    /// one, so a connection missing a value its own sign-in needs
    /// fails loudly at resolution instead of at the provider.
    pub fn worker_value_names(&self) -> Result<Vec<String>, String> {
        worker_value_names_of(&self.auth)
    }

    /// Serde-inexpressible rules; run wherever a spec enters the
    /// system (metadata load, store connect).
    pub fn validate(&self) -> Result<(), String> {
        if !crate::node::is_valid_provider_name(&self.service) {
            return Err(format!(
                "service name '{}' is invalid: use only lowercase letters, digits, and '_'",
                self.service
            ));
        }
        for t in self.auth_templates() {
            t.placeholders()?;
        }
        // Header and query names are static spec data, so a malformed
        // one is refused here, at load, rather than on every request.
        for step in &self.auth {
            match step {
                AuthStep::Header { name, .. } => {
                    // RFC 7230 token charset, what HeaderName accepts.
                    let legal = !name.is_empty()
                        && name.bytes().all(|b| {
                            b.is_ascii_alphanumeric()
                                || matches!(
                                    b,
                                    b'!' | b'#'
                                        | b'$'
                                        | b'%'
                                        | b'&'
                                        | b'\''
                                        | b'*'
                                        | b'+'
                                        | b'-'
                                        | b'.'
                                        | b'^'
                                        | b'_'
                                        | b'`'
                                        | b'|'
                                        | b'~'
                                )
                        });
                    if !legal {
                        return Err(format!(
                            "auth header name '{name}' is not a legal HTTP header name"
                        ));
                    }
                }
                AuthStep::Query { name, .. } => {
                    let legal = !name.is_empty()
                        && name
                            .bytes()
                            .all(|b| b.is_ascii_graphic() && !matches!(b, b'&' | b'=' | b'#'));
                    if !legal {
                        return Err(format!(
                            "auth query parameter name '{name}' is not a usable query name"
                        ));
                    }
                }
                _ => {}
            }
        }
        let field_lists: Vec<&Vec<CredentialField>> = match &self.acquisition {
            Acquisition::Static { fields } => {
                if fields.is_empty() {
                    return Err("a static acquisition needs at least one field".into());
                }
                vec![fields]
            }
            Acquisition::OAuth2 { extra_params, registration_fields, captures, refresh, .. } => {
                for t in extra_params.values() {
                    t.placeholders()?;
                }
                validate_captures(captures)?;
                if let Some(call) = refresh {
                    call.validate("acquisition.refresh")?;
                }
                vec![registration_fields]
            }
            Acquisition::Runtime {} => vec![],
            Acquisition::MintJwt { fields, claims, exchange, .. } => {
                if fields.is_empty() {
                    return Err("a mint_jwt acquisition needs at least one field".into());
                }
                for t in claims.values() {
                    t.placeholders()?;
                }
                if let Some(ex) = exchange {
                    ex.url.placeholders()?;
                    if ex.token_path.is_empty() {
                        return Err("token exchange needs a token_path".into());
                    }
                    validate_captures(&ex.captures)?;
                }
                vec![fields]
            }
        };
        // A capability naming a field the acquisition does not declare
        // could never be filled, so the connect would refuse forever.
        // Refuse at parse instead, where someone is watching.
        for cap in &self.capabilities {
            if cap.fields.is_empty() {
                return Err(format!(
                    "capability '{}' lists no fields, so nothing could ever satisfy it",
                    cap.label
                ));
            }
            for name in &cap.fields {
                let declared = field_lists.iter().flat_map(|fs| fs.iter()).find(|f| &f.name == name);
                match declared {
                    None => {
                        return Err(format!(
                            "capability '{}' names the field '{name}', which this service does \
                             not declare",
                            cap.label
                        ))
                    }
                    Some(f) if !f.optional => {
                        return Err(format!(
                            "capability '{}' names '{name}', but that field is required, so the \
                             group can never be the thing that is missing; mark it optional",
                            cap.label
                        ))
                    }
                    Some(_) => {}
                }
            }
        }
        for fields in field_lists {
            let mut seen = std::collections::HashSet::new();
            for f in fields {
                if !f.name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
                    || f.name.is_empty()
                {
                    return Err(format!(
                        "field name '{}' is invalid: use only lowercase letters, digits, and '_'",
                        f.name
                    ));
                }
                if !seen.insert(&f.name) {
                    return Err(format!("duplicate field name '{}'", f.name));
                }
            }
        }
        if let Some(test) = &self.test {
            test.url.placeholders()?;
            validate_captures(&test.captures)?;
        }
        if let Some(identity) = &self.identity {
            identity.placeholders()?;
        }
        if self.doors.is_empty() {
            return Err("a service must declare at least one door".into());
        }
        {
            let mut seen = std::collections::HashSet::new();
            for d in &self.doors {
                if !seen.insert(d) {
                    return Err("duplicate door".into());
                }
            }
        }
        // A request-signing service can never be shared: the shared
        // door's substitution needs the secret verbatim in the bytes,
        // and a signed request carries a hash computed FROM the secret
        // instead. Arithmetic, not policy; refused at parse so it can
        // never become a runtime surprise.
        if self.doors.contains(&Door::Shared)
            && self.auth.iter().any(|s| matches!(s, AuthStep::Sign { .. }))
        {
            return Err(format!(
                "service '{}' signs its requests, so it cannot offer the shared door; \
                 remove `shared` from `doors`",
                self.service
            ));
        }
        // A Runtime acquisition IS the shared door's stored form; a
        // spec DECLARED with it would have no "own" story at all.
        if matches!(self.acquisition, Acquisition::Runtime {}) {
            return Err(
                "do not declare a `runtime` acquisition: it is the stored form of a shared \
                 connect, written by the store, never authored"
                    .into(),
            );
        }
        {
            let mut seen = std::collections::HashSet::new();
            for p in &self.permissions {
                if p.id.is_empty() {
                    return Err("a permission needs a non-empty id".into());
                }
                if !seen.insert(&p.id) {
                    return Err(format!("duplicate permission '{}'", p.id));
                }
                if p.description.is_empty() {
                    return Err(format!(
                        "permission '{}' needs a one-sentence description",
                        p.id
                    ));
                }
            }
        }
        crate::access::events::validate_topics(&self.service, &self.events)?;
        if let Some(page) = &self.own_page {
            if let Some(mint) = &page.mint {
                validate_captures(&mint.captures)?;
            }
            if let Some(guide) = &page.guide {
                if guide.steps.is_empty() {
                    return Err("a guide needs at least one step".into());
                }
            }
            if let Some(paste) = &page.paste {
                if matches!(
                    self.acquisition,
                    Acquisition::Static { .. } | Acquisition::MintJwt { .. }
                ) {
                    return Err(
                        "own_page.paste is redundant: this acquisition already IS a paste \
                         form (its fields render on the page); declare the fields there"
                            .into(),
                    );
                }
                if paste.fields.is_empty() {
                    return Err("own_page.paste needs at least one field".into());
                }
                // The pasted values are ALL a static row stores, so
                // they must cover every value the auth steps
                // interpolate; a shortfall would connect fine and then
                // fail every resolution.
                let names = self.worker_value_names()?;
                for name in &names {
                    if !paste.fields.iter().any(|f| &f.name == name) {
                        return Err(format!(
                            "own_page.paste is missing a '{name}' field: the auth steps \
                             interpolate it, so a pasted connection must collect it"
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// The spec a PASTED connection stores: this spec with its
    /// acquisition replaced by `Static` over the `own_page.paste`
    /// fields. `None` when the service declares no paste section.
    /// The variant is what connect validates and snapshots, so a
    /// pasted row refreshes (i.e. asks for a fresh paste) and
    /// resolves exactly like a natively static service's.
    pub fn paste_variant(&self) -> Option<AccessSpec> {
        let paste = self.own_page.as_ref()?.paste.as_ref()?;
        let mut variant = self.clone();
        variant.acquisition = Acquisition::Static { fields: paste.fields.clone() };
        // Only capability groups the paste fields can actually fill
        // survive; one over registration fields would make the variant
        // refuse validation forever.
        variant.capabilities.retain(|cap| {
            cap.fields.iter().all(|f| paste.fields.iter().any(|pf| &pf.name == f))
        });
        // The page is connect-time UI; a stored snapshot has no use for
        // it (and validation would rightly refuse static + paste).
        variant.own_page = None;
        Some(variant)
    }

    /// Does this spec's acquisition go through an OAuth2 consent (the
    /// only acquisitions using an app registration)? Decides what the
    /// SHARED door means: a consent on a registered app (their own
    /// account through our app) vs the runtime's own credential
    /// (spending its credit).
    pub fn is_oauth(&self) -> bool {
        matches!(self.acquisition, Acquisition::OAuth2 { .. })
    }

    /// The catalogue entries that start ticked.
    // SYNC: AccessSpec::default_permissions <-> packages/weft-graph/src/webview/lib/components/project/own-fields.ts defaultPermissions
    pub fn default_permissions(&self) -> Vec<String> {
        self.permissions.iter().filter(|p| p.default).map(|p| p.id.clone()).collect()
    }

    /// Is `id` in the permission catalogue?
    pub fn declares_permission(&self, id: &str) -> bool {
        self.permissions.iter().any(|p| p.id == id)
    }

    /// The paste fields the "Your own" page renders, DERIVED from the
    /// acquisition rather than re-declared: a pasted-credential service
    /// pastes its own fields; a consent service pastes its app's
    /// credentials (client id, secret, declared extras). A name field
    /// for the connection list is always prepended by the editor,
    /// whatever this returns.
    // SYNC: AccessSpec::own_fields <-> packages/weft-graph/src/webview/lib/components/project/own-fields.ts ownFields
    pub fn own_fields(&self) -> Vec<CredentialField> {
        let field = |name: &str, label: &str, secret: bool| CredentialField {
            name: name.into(),
            label: Some(label.into()),
            optional: false,
            secret,
            placeholder: None,
        };
        match &self.acquisition {
            Acquisition::Static { fields } | Acquisition::MintJwt { fields, .. } => {
                fields.clone()
            }
            Acquisition::OAuth2 { registration_fields, .. } => {
                let mut out = vec![
                    field("client_id", "Client ID", false),
                    field("client_secret", "Client secret", true),
                ];
                out.extend(registration_fields.iter().cloned());
                out
            }
            Acquisition::Runtime {} => Vec::new(),
        }
    }

    /// The guide's steps with `{permissions}` replaced by the ticked
    /// permissions' labels (comma-joined), generated per pick rather
    /// than written as a static blob. Empty when no guide is declared.
    pub fn guide_steps(&self, ticked: &[String]) -> Vec<String> {
        let Some(guide) = self.own_page.as_ref().and_then(|p| p.guide.as_ref()) else {
            return Vec::new();
        };
        let labels: Vec<&str> = self
            .permissions
            .iter()
            .filter(|p| ticked.iter().any(|t| t == &p.id))
            .map(|p| p.label.as_str())
            .collect();
        let joined = labels.join(", ");
        guide.steps.iter().map(|s| s.replace("{permissions}", &joined)).collect()
    }
}

pub(crate) fn validate_captures(captures: &[Capture]) -> Result<(), String> {
    for c in captures {
        if c.name.is_empty() || c.path.is_empty() {
            return Err(format!("capture '{}' needs a non-empty name and path", c.name));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// An app parses its named creds plus flattened extras, and
    /// `to_values` produces the flat map the token machinery reads.
    #[test]
    fn app_registration_flattens_extras_and_round_trips() {
        let app: AppRegistration = serde_json::from_value(json!({
            "label": "My app",
            "client_id": "cid",
            "client_secret": "sec",
            "account_id": "acc-1"
        }))
        .unwrap();
        assert_eq!(app.label, "My app");
        assert_eq!(app.client_id, "cid");
        assert_eq!(app.client_secret.as_deref(), Some("sec"));
        assert_eq!(app.extra.get("account_id").map(String::as_str), Some("acc-1"));

        let v = app.to_values();
        assert_eq!(v["client_id"], "cid");
        assert_eq!(v["client_secret"], "sec");
        assert_eq!(v["account_id"], "acc-1");
        assert!(!v.contains_key("label"), "the label is a name, never a credential value");

        // Wire round trip (the grant snapshot is this JSON).
        let back: AppRegistration = serde_json::from_value(serde_json::to_value(&app).unwrap()).unwrap();
        assert_eq!(back, app);
    }

    /// A public (PKCE) client has no secret; `to_values` omits it.
    #[test]
    fn app_registration_without_secret_is_public() {
        let app: AppRegistration =
            serde_json::from_value(json!({ "label": "Pub", "client_id": "pub" })).unwrap();
        assert!(app.client_secret.is_none());
        assert!(!app.to_values().contains_key("client_secret"));
    }

    /// `validate` refuses an empty label, an empty client_id, and a
    /// missing declared extra.
    #[test]
    fn app_registration_validate_names_the_missing_field() {
        let need = vec![CredentialField {
            name: "account_id".into(),
            label: None,
            optional: false,
            secret: false,
            placeholder: None,
        }];
        let missing = AppRegistration {
            label: "App".into(),
            client_id: "cid".into(),
            client_secret: Some("s".into()),
            extra: BTreeMap::new(),
        };
        let err = missing.validate(&need).unwrap_err();
        assert!(err.contains("account_id"), "{err}");

        let empty = AppRegistration {
            label: "App".into(),
            client_id: " ".into(),
            client_secret: None,
            extra: BTreeMap::new(),
        };
        assert!(empty.validate(&[]).unwrap_err().contains("client_id"));

        let unnamed = AppRegistration {
            label: "  ".into(),
            client_id: "cid".into(),
            client_secret: None,
            extra: BTreeMap::new(),
        };
        assert!(unnamed.validate(&[]).unwrap_err().contains("name"));
    }

    fn slack_spec() -> AccessSpec {
        serde_json::from_value(json!({
            "service": "slack",
            "label": "Slack",
            "grants": "exclusive",
            "doors": ["shared", "own"],
            "acquisition": {
                "kind": "oauth2",
                "grant": { "kind": "authorization_code",
                           "auth_url": "https://slack.com/oauth/v2/authorize", "pkce": false },
                "token_url": "https://slack.com/api/oauth.v2.access",
                "captures": [
                    { "name": "team", "path": "team.name" },
                    { "name": "token", "path": "access_token" }
                ]
            },
            "permissions": [
                { "id": "chat:write", "label": "Send messages",
                  "description": "Post messages to channels the bot is in.", "default": true },
                { "id": "channels:read", "label": "List channels",
                  "description": "See the workspace's channel list.", "default": true },
                { "id": "files:write", "label": "Upload files",
                  "description": "Upload files into channels." }
            ],
            "permission_timing": "at_mint",
            "all_permissions_url": "https://docs.slack.dev/reference/scopes",
            "verification": { "rung": "reports_permissions", "cost": "free" },
            "own_page": {
                "guide": { "steps": [
                    "Create an app at api.slack.com/apps.",
                    "Add these bot scopes: {permissions}."
                ] }
            },
            "auth": [
                { "kind": "header", "name": "Authorization", "value": "Bearer {token}" }
            ],
            "test": { "url": "https://slack.com/api/auth.test", "method": "POST" },
            "identity": "{team}"
        }))
        .expect("slack spec parses")
    }

    /// `own_page.paste` on a consent service: valid when its fields
    /// cover the auth steps' values; the variant a connect stores is
    /// the same spec with a Static acquisition and no page.
    #[test]
    fn paste_section_yields_a_static_variant() {
        let mut spec = slack_spec();
        spec.own_page.as_mut().unwrap().paste = Some(Paste {
            fields: vec![CredentialField {
                name: "token".into(),
                label: Some("Bot token".into()),
                optional: false,
                secret: true,
                placeholder: Some("xoxb-...".into()),
            }],
        });
        spec.validate().expect("paste covering {token} is valid");
        let variant = spec.paste_variant().expect("declared, so present");
        assert!(
            matches!(&variant.acquisition, Acquisition::Static { fields } if fields.len() == 1),
            "the variant is a static acquisition over the paste fields"
        );
        assert!(variant.own_page.is_none(), "a snapshot carries no creation page");
        variant.validate().expect("the stored variant validates on its own");
        // The rest of the recipe (auth, test, identity) is untouched.
        assert_eq!(variant.auth, spec.auth);
    }

    /// A capability over a field the paste form does not collect (a
    /// registration field) cannot survive into the paste variant: the
    /// variant only stores the pasted fields, so keeping the group
    /// would make it refuse validation forever.
    #[test]
    fn paste_variant_drops_capabilities_its_fields_cannot_fill() {
        let mut spec = slack_spec();
        let Acquisition::OAuth2 { registration_fields, .. } = &mut spec.acquisition else {
            unreachable!()
        };
        registration_fields.push(CredentialField {
            name: "account_id".into(),
            label: None,
            optional: true,
            secret: false,
            placeholder: None,
        });
        spec.capabilities = vec![Capability {
            label: "act as the account".into(),
            fields: vec!["account_id".into()],
        }];
        spec.own_page.as_mut().unwrap().paste = Some(Paste {
            fields: vec![CredentialField {
                name: "token".into(),
                label: Some("Bot token".into()),
                optional: false,
                secret: true,
                placeholder: None,
            }],
        });
        spec.validate().expect("the full spec is valid");
        let variant = spec.paste_variant().expect("declared, so present");
        assert!(variant.capabilities.is_empty(), "the registration-field group is dropped");
        variant.validate().expect("the variant validates without the unfillable group");
    }

    /// A paste section missing a value the auth steps interpolate
    /// would connect fine and then fail every resolution; refused at
    /// validation instead.
    #[test]
    fn paste_missing_an_auth_value_is_refused() {
        let mut spec = slack_spec();
        spec.own_page.as_mut().unwrap().paste = Some(Paste {
            fields: vec![CredentialField {
                name: "api_key".into(),
                label: None,
                optional: false,
                secret: true,
                placeholder: None,
            }],
        });
        let err = spec.validate().unwrap_err();
        assert!(err.contains("'token'"), "names the missing value: {err}");
    }

    /// On a natively static service the acquisition already IS the
    /// paste form; a paste section would be the same concept twice.
    #[test]
    fn paste_on_a_static_service_is_refused() {
        let mut spec: AccessSpec = serde_json::from_value(json!({
            "service": "x",
            "acquisition": { "kind": "static",
                             "fields": [{ "name": "token" }] },
            "auth": [{ "kind": "header", "name": "Authorization", "value": "Bearer {token}" }]
        }))
        .unwrap();
        spec.own_page = Some(OwnPage {
            mint: None,
            guide: None,
            paste: Some(Paste {
                fields: vec![CredentialField {
                    name: "token".into(),
                    label: None,
                    optional: false,
                    secret: true,
                    placeholder: None,
                }],
            }),
        });
        assert!(spec.validate().unwrap_err().contains("redundant"));
    }

    /// A declared renewal call is validated like every other declared
    /// call: a malformed template is refused at load naming the site,
    /// and a well-formed one round-trips.
    #[test]
    fn a_declared_renewal_call_is_validated_at_load() {
        let mut spec = slack_spec();
        let Acquisition::OAuth2 { refresh, .. } = &mut spec.acquisition else { unreachable!() };
        *refresh = Some(
            serde_json::from_value(json!({
                "url": "https://slack.com/exchange?token={token",
                "captures": [{ "name": "token", "path": "access_token" }]
            }))
            .unwrap(),
        );
        let err = spec.validate().unwrap_err();
        assert!(err.contains("acquisition.refresh"), "{err}");

        let Acquisition::OAuth2 { refresh, .. } = &mut spec.acquisition else { unreachable!() };
        *refresh = Some(
            serde_json::from_value(json!({
                "url": "https://slack.com/exchange?token={token}",
                "captures": [{ "name": "token", "path": "access_token" }]
            }))
            .unwrap(),
        );
        spec.validate().expect("a well-formed renewal call validates");
        let back: AccessSpec =
            serde_json::from_value(serde_json::to_value(&spec).unwrap()).unwrap();
        assert_eq!(back, spec);
    }

    #[test]
    fn a_full_oauth_spec_round_trips_and_validates() {
        let spec = slack_spec();
        spec.validate().expect("valid");
        assert!(spec.is_oauth());
        assert_eq!(spec.grants, GrantCoexistence::Exclusive);
        assert_eq!(spec.doors, vec![Door::Shared, Door::Own]);
        assert_eq!(spec.default_permissions(), vec!["chat:write", "channels:read"]);
        // Wire-shape: the JSON form survives a round trip.
        let v = serde_json::to_value(&spec).unwrap();
        let back: AccessSpec = serde_json::from_value(v).unwrap();
        assert_eq!(back, spec);
    }

    /// The "Your own" page's paste fields are DERIVED from the
    /// acquisition, never re-declared: a consent service pastes its
    /// app's credentials, a pasted-credential service its own fields.
    #[test]
    fn own_fields_derive_from_the_acquisition() {
        let slack = slack_spec();
        let names: Vec<String> = slack.own_fields().into_iter().map(|f| f.name).collect();
        assert_eq!(names, vec!["client_id", "client_secret"]);

        let telegram: AccessSpec = serde_json::from_value(json!({
            "service": "telegram",
            "acquisition": { "kind": "static",
                             "fields": [{ "name": "token", "label": "Bot token" }] },
            "auth": [{ "kind": "path_prefix", "value": "/bot{token}" }]
        }))
        .unwrap();
        let names: Vec<String> = telegram.own_fields().into_iter().map(|f| f.name).collect();
        assert_eq!(names, vec!["token"]);
    }

    /// Guide steps are generated from the ticked permissions, never a
    /// static blob: `{permissions}` interpolates the ticked LABELS.
    #[test]
    fn guide_steps_interpolate_the_ticked_permissions() {
        let spec = slack_spec();
        let steps = spec.guide_steps(&["chat:write".into(), "files:write".into()]);
        assert_eq!(steps[0], "Create an app at api.slack.com/apps.");
        assert_eq!(steps[1], "Add these bot scopes: Send messages, Upload files.");
        assert!(spec.guide_steps(&[])[1].ends_with(": ."), "no ticks, empty list");
    }

    /// A signing service declaring the shared door is a PARSE error:
    /// the shared substitution needs the secret verbatim in the bytes,
    /// and a signed request carries a hash instead.
    #[test]
    fn a_signing_service_cannot_offer_the_shared_door() {
        let s3: AccessSpec = serde_json::from_value(json!({
            "service": "s3",
            "doors": ["shared", "own"],
            "acquisition": { "kind": "static", "fields": [
                { "name": "access_key_id", "secret": false },
                { "name": "secret_access_key" },
                { "name": "region", "secret": false }
            ]},
            "auth": [{ "kind": "sign", "with": {
                "kind": "sigv4", "service": "s3", "region": "{region}",
                "access_key_id": "{access_key_id}",
                "secret_access_key": "{secret_access_key}"
            }}]
        }))
        .unwrap();
        let err = s3.validate().unwrap_err();
        assert!(err.contains("shared"), "{err}");
    }

    /// A `runtime` acquisition is the STORED form of a shared connect;
    /// an authored spec declaring it is refused.
    #[test]
    fn a_declared_runtime_acquisition_is_refused() {
        let spec: AccessSpec = serde_json::from_value(json!({
            "service": "x",
            "acquisition": { "kind": "runtime" }
        }))
        .unwrap();
        assert!(spec.validate().unwrap_err().contains("runtime"));
    }

    #[test]
    fn static_and_mint_jwt_shapes_parse() {
        let telegram: AccessSpec = serde_json::from_value(json!({
            "service": "telegram",
            "acquisition": { "kind": "static",
                             "fields": [{ "name": "token", "label": "Bot token" }] },
            "auth": [{ "kind": "path_prefix", "value": "/bot{token}" }]
        }))
        .unwrap();
        telegram.validate().unwrap();
        assert!(!telegram.is_oauth());
        assert!(telegram.permissions.is_empty(), "a static spec declares no permissions");
        assert_eq!(telegram.doors, vec![Door::Own], "own is the default door");

        let github_app: AccessSpec = serde_json::from_value(json!({
            "service": "github_app",
            "acquisition": {
                "kind": "mint_jwt",
                "fields": [
                    { "name": "app_id", "secret": false },
                    { "name": "private_key" },
                    { "name": "installation_id", "secret": false }
                ],
                "claims": { "iss": "{app_id}" },
                "exchange": {
                    "url": "https://api.github.com/app/installations/{installation_id}/access_tokens",
                    "token_path": "token",
                    "expires_path": "expires_at"
                }
            },
            "auth": [{ "kind": "header", "name": "Authorization", "value": "Bearer {token}" }]
        }))
        .unwrap();
        github_app.validate().unwrap();
    }

    #[test]
    fn sigv4_and_oauth1a_are_typed_variants() {
        let s3: AccessSpec = serde_json::from_value(json!({
            "service": "s3",
            "acquisition": { "kind": "static", "fields": [
                { "name": "access_key_id", "secret": false },
                { "name": "secret_access_key" },
                { "name": "region", "secret": false }
            ]},
            "auth": [{ "kind": "sign", "with": {
                "kind": "sigv4", "service": "s3", "region": "{region}",
                "access_key_id": "{access_key_id}",
                "secret_access_key": "{secret_access_key}"
            }}]
        }))
        .unwrap();
        s3.validate().unwrap();
        assert_eq!(
            s3.worker_value_names().unwrap(),
            vec!["region", "access_key_id", "secret_access_key"]
        );
    }

    /// The capability rule, all four ways: a complete group passes, a
    /// half-filled one names what is missing, filling none names the
    /// choices, and a service declaring no groups is unaffected.
    #[test]
    fn capability_groups_are_all_or_nothing_and_at_least_one() {
        let caps = vec![
            Capability {
                label: "receive mail".into(),
                fields: vec!["imap_host".into(), "imap_port".into()],
            },
            Capability {
                label: "send mail".into(),
                fields: vec!["smtp_host".into(), "smtp_port".into()],
            },
        ];
        let filled = |names: &'static [&'static str]| move |n: &str| names.contains(&n);

        capability_shortfall(&caps, filled(&["imap_host", "imap_port"]))
            .expect("one complete group is enough");
        capability_shortfall(&caps, filled(&["smtp_host", "smtp_port"]))
            .expect("the other group alone is enough too");
        capability_shortfall(
            &caps,
            filled(&["imap_host", "imap_port", "smtp_host", "smtp_port"]),
        )
        .expect("both groups is the everyday case");

        // Half a group names the missing half and the capability.
        let err = capability_shortfall(&caps, filled(&["imap_host"])).unwrap_err();
        assert!(err.contains("receive mail") && err.contains("imap_port"), "{err}");

        // No group at all names every choice.
        let err = capability_shortfall(&caps, filled(&[])).unwrap_err();
        assert!(err.contains("receive mail") && err.contains("send mail"), "{err}");

        // A service with no groups declared never refuses.
        capability_shortfall(&[], filled(&[])).expect("no rule, no refusal");

        // A field NO group names is untouched by both rules: it
        // neither completes a group nor satisfies rule 2 on its own.
        let err = capability_shortfall(&caps, filled(&["send_as"])).unwrap_err();
        assert!(err.contains("nothing"), "an ungrouped field satisfies no group: {err}");
        capability_shortfall(&caps, filled(&["send_as", "imap_host", "imap_port"]))
            .expect("an ungrouped field never blocks a complete group either");
    }

    /// A capability naming a field the service does not declare (or a
    /// required one) is refused at parse: it could never be satisfied.
    #[test]
    fn capabilities_must_name_declared_optional_fields() {
        let mut spec: AccessSpec = serde_json::from_value(serde_json::json!({
            "service": "mail",
            "acquisition": { "kind": "static", "fields": [
                { "name": "user" },
                { "name": "imap_host", "optional": true }
            ]},
            "auth": [{ "kind": "header", "name": "A", "value": "{user}" }],
            "capabilities": [{ "label": "receive mail", "fields": ["imap_host"] }]
        }))
        .expect("parses");
        spec.validate().expect("a group over a declared optional field is fine");

        let mut unknown = spec.clone();
        unknown.capabilities[0].fields = vec!["nope".into()];
        assert!(unknown.validate().unwrap_err().contains("nope"));

        let mut required = spec.clone();
        required.capabilities[0].fields = vec!["user".into()];
        assert!(required.validate().unwrap_err().contains("optional"));

        spec.capabilities[0].fields.clear();
        assert!(spec.validate().unwrap_err().contains("no fields"));
    }

    #[test]
    fn validate_rejects_bad_shapes() {
        let mut spec = slack_spec();
        spec.service = "Slack!".into();
        assert!(spec.validate().unwrap_err().contains("invalid"));

        let mut dup = slack_spec();
        dup.permissions.push(Permission {
            id: "chat:write".into(),
            label: "Again".into(),
            description: "Duplicate.".into(),
            default: false,
        });
        assert!(dup.validate().unwrap_err().contains("duplicate permission"));

        let mut undesc = slack_spec();
        undesc.permissions[0].description = String::new();
        assert!(undesc.validate().unwrap_err().contains("description"));

        let mut doorless = slack_spec();
        doorless.doors.clear();
        assert!(doorless.validate().unwrap_err().contains("door"));

        let empty_static: AccessSpec = serde_json::from_value(json!({
            "service": "x",
            "acquisition": { "kind": "static", "fields": [] }
        }))
        .unwrap();
        assert!(empty_static.validate().is_err());

        // A malformed header or query name fails at spec load, not on
        // every request through the connection.
        let mut bad_header = slack_spec();
        bad_header.auth =
            vec![AuthStep::Header { name: "X Bad Name".into(), value: Template::new("{token}") }];
        let err = bad_header.validate().unwrap_err();
        assert!(err.contains("'X Bad Name'"), "{err}");

        let mut bad_query = slack_spec();
        bad_query.auth =
            vec![AuthStep::Query { name: "a=b".into(), value: Template::new("{token}") }];
        let err = bad_query.validate().unwrap_err();
        assert!(err.contains("'a=b'"), "{err}");

        let mut empty_query = slack_spec();
        empty_query.auth =
            vec![AuthStep::Query { name: String::new(), value: Template::new("{token}") }];
        assert!(empty_query.validate().is_err());
    }

    #[test]
    fn templates_interpolate_and_fail_loud() {
        let t = Template::new("Bearer {token}");
        assert_eq!(t.placeholders().unwrap(), vec!["token"]);
        let mut values = BTreeMap::new();
        values.insert("token".to_string(), "xoxb-1".to_string());
        assert_eq!(t.resolve(&values).unwrap(), "Bearer xoxb-1");

        let unknown = Template::new("Bearer {nope}").resolve(&values).unwrap_err();
        assert!(unknown.contains("'nope'"), "{unknown}");

        assert!(Template::new("Bearer {").placeholders().is_err());
        assert!(Template::new("Bearer }").placeholders().is_err());
        assert!(Template::new("Bearer {BAD}").placeholders().is_err());
        assert_eq!(Template::new("no holes").placeholders().unwrap(), Vec::<String>::new());
        // Repeats dedupe, order of first appearance.
        assert_eq!(
            Template::new("{a}{b}{a}").placeholders().unwrap(),
            vec!["a", "b"]
        );
    }

    #[test]
    fn worker_value_names_are_exactly_what_auth_interpolates() {
        let spec = slack_spec();
        // Still the COMPLETENESS set: what a resolution must find to
        // sign a request. No longer the visibility set.
        assert_eq!(spec.worker_value_names().unwrap(), vec!["token"]);
    }

    /// The handoff hands over everything the connection holds EXCEPT
    /// the store's own keep-alive material. Naming what is private is
    /// the wall; the auth steps decide signing, not visibility.
    #[test]
    fn the_handoff_withholds_only_the_stores_keep_alive_material() {
        let spec = slack_spec();
        let stored = BTreeMap::from([
            ("token".to_string(), "xoxb-1".to_string()),
            ("team".to_string(), "acme".to_string()),
            ("refresh_token".to_string(), "rt-secret".to_string()),
        ]);
        let handoff = spec.handoff_values(&stored);
        assert_eq!(handoff.get("token").map(String::as_str), Some("xoxb-1"));
        assert_eq!(
            handoff.get("team").map(String::as_str),
            Some("acme"),
            "a captured value the auth steps never mention still travels"
        );
        assert!(
            !handoff.contains_key("refresh_token"),
            "the store's keep-alive material never leaves"
        );
    }

    /// A credential that is NOT a request transform (a mail server's
    /// host + user + password) has no auth steps at all, and every one
    /// of its values still reaches whoever holds the connection. Under
    /// the old auth-derived rule this handed back nothing, silently.
    #[test]
    fn a_connection_with_no_request_signing_still_hands_over_its_values() {
        let mail: AccessSpec = serde_json::from_value(json!({
            "service": "mailbox",
            "acquisition": { "kind": "static", "fields": [
                { "name": "host", "secret": false },
                { "name": "port", "secret": false },
                { "name": "username", "secret": false },
                { "name": "password" }
            ]}
        }))
        .unwrap();
        mail.validate().expect("a service with no request signing is valid");
        assert!(mail.worker_value_names().unwrap().is_empty(), "nothing to sign");

        let stored = BTreeMap::from([
            ("host".to_string(), "imap.example.com".to_string()),
            ("port".to_string(), "993".to_string()),
            ("username".to_string(), "q@example.com".to_string()),
            ("password".to_string(), "hunter2".to_string()),
        ]);
        let handoff = mail.handoff_values(&stored);
        assert_eq!(handoff.len(), 4, "every stored value travels: {handoff:?}");
        assert_eq!(handoff.get("password").map(String::as_str), Some("hunter2"));
    }

    /// The private list is the STORE's, not the recipe's: a service
    /// recipe (which a project author can edit) cannot rename a value
    /// into or out of it, so an untrusted recipe cannot widen the
    /// wall.
    #[test]
    fn the_private_list_is_system_owned() {
        assert!(value_is_private("refresh_token"));
        assert!(!value_is_private("token"));
        assert!(!value_is_private("password"));
        // Case-exact by design: the store writes these names itself.
        assert!(!value_is_private("Refresh_Token"));
    }

    #[test]
    fn lookup_path_walks_objects_and_arrays() {
        let v = json!({"team": {"name": "acme"}, "channels": [{"id": "C1"}, {"id": "C2"}]});
        assert_eq!(lookup_path(&v, "team.name"), Some(&json!("acme")));
        assert_eq!(lookup_path(&v, "channels.1.id"), Some(&json!("C2")));
        assert_eq!(lookup_path(&v, "missing.path"), None);
        assert_eq!(lookup_path(&v, "team.name.deeper"), None);
    }
}
