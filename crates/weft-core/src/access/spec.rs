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

/// What a provider accepts as its OAuth callback address
/// ([`AccessSpec::callback`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallbackAddress {
    /// Any address, plain http on a loopback included.
    #[default]
    Any,
    /// An https address.
    Https,
    /// An https address whose host is a domain name, never an IP.
    Domain,
}

impl CallbackAddress {
    fn is_any(&self) -> bool {
        *self == Self::Any
    }
}

/// The steps a stored [`AccessSpec`] (a connection's snapshot of its
/// recipe, a program's published or instance service) takes from the
/// shape an older weft wrote; `crate::stored` says how they run.
pub const ACCESS_SPEC_UPGRADES: &[&[crate::stored::Op]] = &[ACCESS_SPEC_V2];

/// Version 2: a route gate's scheme and every push recipe's scheme moved
/// to the shape where the signature and the timestamp are each a place in
/// the request (`crate::access::events::verify_to_v2`).
pub const ACCESS_SPEC_V2: &[crate::stored::Op] = &[
    crate::stored::Op::Code { at: "verify", run: crate::access::events::verify_to_v2 },
    crate::stored::Op::Within { at: "events", ops: crate::access::events::EVENTS_V2 },
];

/// The recipe for one service's personal accesses: how a grant is
/// acquired, how a request through it is authenticated, and how grants
/// coexist across projects.
// SYNC: AccessSpec <-> packages/weft-connect/src/core/wire.ts AccessSpecWire
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
    /// The node can RUN without a connection picked (a custom endpoint
    /// that may be unauthenticated). By default every access node
    /// requires one: the language synthesizes the runtime "no
    /// connection picked" rule and the editor pins the unconnected
    /// node open, so node metadata never restates that boilerplate.
    /// Declaring this true turns both off for this node.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub connection_optional: bool,
    /// The connect doors this service offers. `own` is the page where
    /// the user brings (or creates) their own credential; `shared` is
    /// the one-click door on a credential weft itself holds (a
    /// registered app for a consent service, the runtime's key for a
    /// pasted-key service). Hidden doors are not rendered. Defaults to
    /// `[own]`: every service can at least take the user's own
    /// credential, and offering `shared` is a deliberate declaration.
    #[serde(default = "default_doors", skip_serializing_if = "is_default_doors")]
    pub doors: Vec<Door>,
    /// What address the provider accepts for its OAuth callback. Most
    /// take any; some refuse plain http (Slack), and some refuse a bare
    /// IP address (Google's console only takes a name ending in a public
    /// top-level domain). Every consent for this service uses an address
    /// that qualifies, failing loudly, with the fix, when this weft has
    /// none.
    #[serde(default, skip_serializing_if = "CallbackAddress::is_any")]
    pub callback: CallbackAddress,
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
    /// next one. `allPermissionsUrl` is what keeps that honest.
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
    /// What the connect-time check can actually learn about a fresh
    /// credential, and what running it costs. Decides whether the
    /// recorded permissions are VERIFIED (the provider stated them) or
    /// CLAIMED (the user says so), which in turn decides how hard a
    /// later shortfall fails.
    #[serde(default, skip_serializing_if = "Verification::is_default")]
    pub verification: Verification,
    /// How this service REPORTS events, when it does: named TOPICS,
    /// each a full recipe (fields, account, transports). A map
    /// because one provider genuinely reports along independent
    /// topologies at once. Empty = the service reports nothing, so a
    /// trigger on it cannot be served and says so at registration.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub events: BTreeMap<String, crate::access::events::EventsSpec>,
    /// How a CALLER presenting a connection of this service is checked
    /// when a live route is gated by it (`Route { auth: ... }`): the
    /// scheme (a shared-key set, an identity token, an HMAC) is data
    /// here, and the connection's stored values hold the material by
    /// the names the scheme reserves (`keys`, `signingSecret`,
    /// `publicKey`, `audience`; an `oidc` scheme names its addresses
    /// as templates over the fields). The broker runs it, secret-free
    /// from the gateway's side. Absent = connections of this service
    /// cannot gate a route.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verify: Option<crate::access::events::VerifyKind>,
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
// SYNC: Capability <-> packages/weft-connect/src/core/wire.ts Capability
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
// SYNC: Door <-> packages/weft-connect/src/core/wire.ts Door
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
// SYNC: OwnPage <-> packages/weft-connect/src/core/wire.ts OwnPage
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
// SYNC: Paste <-> packages/weft-connect/src/core/wire.ts OwnPageWire.paste
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Paste {
    /// What the user pastes (a bot token, a key pair).
    pub fields: Vec<CredentialField>,
}

/// A provider's create-an-app-by-API recipe (Slack/Zoom manifest class).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Guide {
    /// A pre-filled creation link (a manifest / template URL). May
    /// embed `{permissions}` (ticked ids, comma-joined, url-encoded).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link: Option<String>,
    /// Ordered tutorial steps.
    pub steps: Vec<String>,
}

/// One entry of a service's permission catalogue.
// SYNC: Permission <-> packages/weft-connect/src/core/wire.ts Permission
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
    /// Asked for on every consent, whatever is ticked, and never shown to
    /// tick: what the service's test call needs to learn who the account
    /// is (Google's `userinfo.email`) when no tickable permission is sure
    /// to cover it. Keep it to permissions the provider grants without
    /// review and an app allows without setup, since every app and every
    /// connection asks for it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub always: bool,
    /// This capability creates or reads things INSIDE the credential's
    /// own account (minted voices, configured agents, phone numbers),
    /// so a runtime-supplied credential can never serve it: the result
    /// would land in the runtime's account, shared across everyone.
    /// Resolution refuses an `Ours`-owned connection for a node
    /// requiring it, and the editor greys the shared option with the
    /// guide below.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub own_only: bool,
    /// The set-up tutorial for THIS capability, shown on the "Your
    /// own" page as its own foldable section (and linked from the
    /// refusal): what to create on the provider's site before a node
    /// needing this capability can run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guide: Option<Guide>,
}

/// What connect-time verification can learn, and what it costs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
// SYNC: VerificationRung <-> packages/weft-connect/src/core/wire.ts VerificationRung
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
// SYNC: VerificationCost <-> packages/weft-connect/src/core/wire.ts VerificationCost
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
// SYNC: APPS_FILE_ENV <-> crates/weft-cli/src/commands/daemon.rs (secrets),
//       deploy/terraform/gcp/machine.yaml.tftpl, deploy/terraform/gcp/serverless.tf
pub const APPS_FILE_ENV: &str = "WEFT_ACCESS_APPS_FILE";

/// The credentials of a service's OAuth app: the client id, the secret
/// (absent for a public PKCE client), and any `registrationFields`
/// extras the service declares. Keyed by service name in
/// [`crate::node::NodeMetadata::access_apps`], where a project (or a
/// package root, inherited by every member) declares the app it uses.
///
/// Resolved at connect (by door: the registered apps file for the
/// shared door, the user's pasted app or the project's public app for
/// the own door) and snapshotted onto the grant so runtime refresh
/// reads it without a separate lookup.
// SYNC: AppRegistration <-> packages/weft-connect/src/core/wire.ts AppRegistration
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
    /// (e.g. a Zoom `account_id`), keyed by the `registrationFields`
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

    /// The connect path's check, for an app somebody pastes or registers
    /// (it may hold a secret): a label and a `client_id`, every declared
    /// `registration_field` present, and no key the service does not
    /// declare, so a half-filled app fails at connect with a clear
    /// message, not at the token call. An app a node SHIPS is held to
    /// [`AppRegistration::check_shipped`] instead.
    pub fn validate(&self, fields: &[CredentialField]) -> Result<(), String> {
        self.check_named()?;
        if let Some(f) = fields.iter().find(|f| !f.optional && self.lacks(&f.name)) {
            return Err(format!("the app is missing the required field '{}'", f.name));
        }
        self.check_keys(fields, false)
    }

    /// Every rule for an app a node's `metadata.json` ships (`accessApps`).
    /// A metadata.json ships with the node to everyone who uses it, so the
    /// app is public: it holds no `client_secret` and no field the service
    /// marks secret, and a service that REQUIRES a secret field cannot
    /// ship one at all. Beyond that it is whole: a label, a `client_id`,
    /// every required field, and no key the service does not declare.
    /// `fields` are the service's `registrationFields`.
    pub fn check_shipped(&self, fields: &[CredentialField]) -> Result<(), String> {
        const ELSEWHERE: &str = "an app with a secret goes in the install's apps file \
            (`access-apps.json`), or is pasted on the editor's \"Your own\" page when connecting";
        if let Some(f) = fields.iter().find(|f| f.secret && !f.optional) {
            return Err(format!(
                "this service needs '{}', a secret, in every app, so it cannot ship a public app at \
                 all (a metadata.json ships with the node to everyone who uses it); remove this \
                 app: {ELSEWHERE}",
                f.name
            ));
        }
        self.check_named()?;
        if self.client_secret.is_some() {
            return Err(format!(
                "a shipped app must not carry a client_secret (a metadata.json ships with the node \
                 to everyone who uses it, so a secret here would go to all of them); declare only a \
                 public (PKCE) client here: {ELSEWHERE}"
            ));
        }
        if let Some(f) = fields.iter().find(|f| f.secret && self.extra.contains_key(&f.name)) {
            return Err(format!(
                "'{}' is a secret, and a metadata.json ships with the node to everyone who uses it; \
                 leave it out here: {ELSEWHERE}",
                f.name
            ));
        }
        self.check_keys(fields, true)?;
        if let Some(f) = fields.iter().find(|f| !f.optional && self.lacks(&f.name)) {
            return Err(format!("the app is missing the required field '{}'", f.name));
        }
        Ok(())
    }

    /// A label and a `client_id`, both non-empty.
    fn check_named(&self) -> Result<(), String> {
        if self.label.trim().is_empty() {
            return Err("the app needs a name (shown in the connection list)".to_string());
        }
        if self.client_id.trim().is_empty() {
            return Err("the app's client_id is empty".to_string());
        }
        Ok(())
    }

    /// Whether the extra `name` is absent or blank.
    fn lacks(&self, name: &str) -> bool {
        self.extra.get(name).is_none_or(|value| value.trim().is_empty())
    }

    /// Refuse a key the service does not declare. Every key beyond
    /// `label`, `client_id` and `client_secret` lands in `extra`, so a
    /// misspelled `client_secrte` would otherwise load as an extra and
    /// leave the app without its secret. `fields` are the service's
    /// declared registration fields. `shipped` is an app a node's
    /// metadata ships, which is public: the keys it may hold leave out
    /// the secret ones, so the message never offers one.
    fn check_keys(&self, fields: &[CredentialField], shipped: bool) -> Result<(), String> {
        match self.extra.keys().find(|key| !fields.iter().any(|f| f.name == **key)) {
            None => Ok(()),
            Some(key) => {
                let mut known = vec!["label", "client_id"];
                if !shipped {
                    known.push("client_secret");
                }
                known.extend(fields.iter().filter(|f| !(shipped && f.secret)).map(|f| f.name.as_str()));
                Err(format!("the app has a field '{key}' its service does not declare (known: {})", known.join(", ")))
            }
        }
    }
}

/// How grants of a service coexist across projects; see
/// [`AccessSpec::grants`].
// SYNC: GrantCoexistence <-> packages/weft-connect/src/core/wire.ts GrantCoexistence
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
// SYNC: CredentialField <-> packages/weft-connect/src/core/wire.ts CredentialFieldWire
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
// SYNC: Acquisition <-> packages/weft-connect/src/core/wire.ts AccessSpecWire.acquisition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", rename_all_fields = "camelCase", deny_unknown_fields)]
pub enum Acquisition {
    /// The user pastes the declared fields (a bot token, an API key
    /// pair). Scoping, if any, happened on the provider's own site, so
    /// a static spec never declares scopes.
    Static { fields: Vec<CredentialField> },
    /// OAuth2. The connect uses an app (client id + secret, plus any
    /// `registrationFields` extras): the door decides whose. The
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
        /// The parameter names the app's client id and secret travel
        /// under, on the consent URL and on every token call that
        /// carries them in its form. OAuth's `client_id` and
        /// `client_secret` by default; TikTok's `client_key`. Only the
        /// WIRE name changes: the stored values keep their names.
        #[serde(default, skip_serializing_if = "ClientParams::is_default")]
        client_params: ClientParams,
        /// Extra CONSENT-URL params (Google's `access_type=offline` +
        /// `prompt=consent` for a refresh token). Templates over the
        /// app's non-secret registration values (the consent URL is
        /// shown in the browser, so a secret never rides it).
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        auth_params: BTreeMap<String, Template>,
        /// Extra params some providers demand on EVERY token call
        /// (exchange, refresh, client credentials): Zoom S2S
        /// `account_id`, audience params. Values are templates over
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
        /// How the token endpoint authenticates the CLIENT, on every
        /// token call (exchange, refresh, client credentials). `Body`
        /// (the RFC's form-field option, the wider-supported default)
        /// sends the id and secret as form fields (named by
        /// `clientParams`); `Basic`
        /// sends them as HTTP Basic, for providers that ignore body
        /// credentials (Notion, Airtable).
        #[serde(default)]
        token_auth: TokenAuth,
        /// Where the token endpoint's answer keeps what weft stores
        /// (the token, the refresh token, the expiry, the granted
        /// scopes, a refusal), for the exchange, the standard refresh
        /// and client credentials. Flat on the acquisition.
        #[serde(flatten)]
        answer: TokenAnswer,
        /// One call run right after the code exchange, for providers
        /// whose exchange hands out a short-lived token that must be
        /// swapped at once (Instagram's long-lived token). Templated
        /// over the values the exchange just stored plus the app's
        /// registration values; its answer, read with its own reader,
        /// replaces the whole token set: the token, its expiry, and
        /// the refresh token (the one it carries, or none: a refresh
        /// token from the exchange belongs to the discarded token).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        after_exchange: Option<Box<TokenCall>>,
        /// How a stale grant is RENEWED, for providers whose renewal
        /// is not the standard refresh-token POST (Meta's long-lived
        /// token GET interpolating the current token). Run at refresh
        /// time over the stored values plus the app's registration
        /// values; its answer, read with its own reader, replaces the
        /// token and sets the next expiry, and its captures write
        /// back. Absent = the standard renewal for the grant kind
        /// (refresh_token / re-request).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        refresh: Option<Box<TokenCall>>,
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

/// How an OAuth2 token call authenticates the client; see the
/// `tokenAuth` field on [`Acquisition::OAuth2`]. (The editor's
/// acquisition wire passes unknown keys through, so this needs no
/// frontend mirror.)
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenAuth {
    #[default]
    Body,
    Basic,
}

/// The parameter names an app's client id and secret are sent under;
/// see the `clientParams` field on [`Acquisition::OAuth2`]. The keys
/// are weft's, the values the provider's spelling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClientParams {
    #[serde(default = "default_client_id_param")]
    pub id: String,
    #[serde(default = "default_client_secret_param")]
    pub secret: String,
}

fn default_client_id_param() -> String {
    "client_id".into()
}

fn default_client_secret_param() -> String {
    "client_secret".into()
}

impl Default for ClientParams {
    fn default() -> Self {
        Self { id: default_client_id_param(), secret: default_client_secret_param() }
    }
}

impl ClientParams {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }

    fn validate(&self) -> Result<(), String> {
        for (key, name) in [("id", &self.id), ("secret", &self.secret)] {
            if !is_param_name(name) {
                return Err(format!(
                    "clientParams.{key} '{name}' is not a usable parameter name"
                ));
            }
        }
        if self.id == self.secret {
            return Err(format!(
                "clientParams sends the client id and the secret under the same name '{}'",
                self.id
            ));
        }
        Ok(())
    }
}

/// A name usable as a query or form parameter: printable, and none of
/// the characters that would split it (`&`, `=`, `#`).
fn is_param_name(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().all(|b| b.is_ascii_graphic() && !matches!(b, b'&' | b'=' | b'#'))
}

/// Where a token answer keeps what weft stores from it. Every token
/// call reads its answer through one of these: the OAuth2 exchange,
/// refresh and client credentials (the acquisition's own), a declared
/// [`TokenCall`] (its own), and a minted JWT's exchange. Every field
/// is a dotted path ([`lookup_path`]); unset takes the OAuth answer's
/// standard name, so a provider that answers the RFC way declares
/// nothing. (The editor's acquisition wire passes unknown keys
/// through, so this, [`ClientParams`] and [`TokenCall`] need no
/// frontend mirror.)
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TokenAnswer {
    /// The token, stored as `token`. Default `access_token`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_path: Option<String>,
    /// The refresh token, stored as `refresh_token` when present.
    /// Default `refresh_token`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_token_path: Option<String>,
    /// The token's lifetime in seconds from now, a number or a string
    /// of digits. Default `expires_in`, unless `expiresPath` is set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_in_path: Option<String>,
    /// The token's absolute expiry instead: an RFC 3339 time or epoch
    /// seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_path: Option<String>,
    /// The token does not expire: no expiry is read, and nothing ever
    /// refreshes it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub never_expires: bool,
    /// The granted-scope echo: a string split on spaces and commas, or
    /// a list of strings. Default `scope`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope_path: Option<String>,
    /// A refusal answered with a success status: anything there but
    /// `null`, `false` or an empty string refuses the answer (an
    /// OAuth `error` code, or an error object). Default `error`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_path: Option<String>,
    /// A success flag: anything there but `true` or `null` refuses the
    /// answer (Slack's `ok`). Nothing there, or no path declared, passes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ok_path: Option<String>,
}

/// When a token read off an answer stops working.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenExpiry {
    At(TokenLife),
    /// The reader declares `neverExpires`.
    Never,
    /// The answer carries nothing where the reader looks.
    Unknown,
    /// The answer carries no expiry where the reader declares one, or
    /// one that is not a time; the reason names the path. The token and
    /// the refresh token are read all the same: the provider issued
    /// them, and a rotating provider has already spent the old ones.
    Unreadable(String),
}

/// A token's end and the lifetime it was issued with, both as the
/// provider stated them. The lifetime is the answer's `expires_in` when
/// it gave one; for an absolute expiry it is that time less the moment
/// the answer arrived, measured once there. Never re-measured later: a
/// lifetime measured at a write would shrink by every network call made
/// in between, and a renewal answer naming no usable expiry is given it
/// again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenLife {
    pub at: chrono::DateTime<chrono::Utc>,
    pub lifetime: chrono::Duration,
}

impl TokenLife {
    /// A token issued at `now` that lasts `lifetime`.
    pub fn lasting(now: chrono::DateTime<chrono::Utc>, lifetime: chrono::Duration) -> Self {
        TokenLife { at: now + lifetime, lifetime }
    }
}

/// What a [`TokenAnswer`] read off one answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenRead {
    pub token: String,
    pub refresh_token: Option<String>,
    pub expiry: TokenExpiry,
    pub scope: Option<Vec<String>>,
}

impl TokenAnswer {
    pub fn token_path(&self) -> &str {
        self.token_path.as_deref().unwrap_or("access_token")
    }

    pub fn refresh_token_path(&self) -> &str {
        self.refresh_token_path.as_deref().unwrap_or("refresh_token")
    }

    pub fn scope_path(&self) -> &str {
        self.scope_path.as_deref().unwrap_or("scope")
    }

    pub fn error_path(&self) -> &str {
        self.error_path.as_deref().unwrap_or(CONVENTIONAL_ERROR_PATH)
    }

    /// The relative-lifetime path in effect: the declared one, else
    /// `expires_in` unless the reader reads an absolute expiry or none.
    fn expires_in_path(&self) -> Option<&str> {
        match (&self.expires_in_path, &self.expires_path) {
            (Some(path), _) => Some(path),
            (None, None) if !self.never_expires => Some("expires_in"),
            _ => None,
        }
    }

    /// Where this reader looks for the expiry, in the words an error
    /// names it by.
    pub fn expiry_source(&self) -> String {
        match (&self.expires_path, self.expires_in_path()) {
            (Some(at), _) => format!("'{at}' (expiresPath)"),
            (None, Some(path)) => format!("'{path}' (expiresInPath)"),
            (None, None) => "nowhere (neverExpires)".into(),
        }
    }

    /// Refuse a contradictory reader, naming the site (`what`).
    pub fn validate(&self, what: &str) -> Result<(), String> {
        for (key, path) in [
            ("tokenPath", &self.token_path),
            ("refreshTokenPath", &self.refresh_token_path),
            ("expiresInPath", &self.expires_in_path),
            ("expiresPath", &self.expires_path),
            ("scopePath", &self.scope_path),
            ("errorPath", &self.error_path),
            ("okPath", &self.ok_path),
        ] {
            if path.as_deref().is_some_and(|p| p.trim().is_empty()) {
                return Err(format!("{what}.{key} is empty; name a path, or leave the key out"));
            }
        }
        if self.expires_in_path.is_some() && self.expires_path.is_some() {
            return Err(format!(
                "{what} declares both expiresInPath and expiresPath; a token has one expiry, \
                 keep the one the provider answers"
            ));
        }
        if self.never_expires && (self.expires_in_path.is_some() || self.expires_path.is_some()) {
            return Err(format!(
                "{what} declares neverExpires and also where the expiry is; keep one"
            ));
        }
        Ok(())
    }

    /// Read one answer. The token must be there; everything else is
    /// read when present, and a value present but unreadable is an
    /// error naming its path, except the expiry, whose trouble is
    /// [`TokenExpiry::Unreadable`] so the tokens are never dropped.
    /// `now` anchors a relative lifetime.
    pub fn read(&self, body: &Value, now: chrono::DateTime<chrono::Utc>) -> Result<TokenRead, String> {
        let token = match lookup_path(body, self.token_path()) {
            Some(Value::String(t)) if !t.is_empty() => t.clone(),
            _ => {
                return Err(format!(
                    "the answer carries no token at '{}' (declare `tokenPath` if the provider \
                     puts it elsewhere)",
                    self.token_path()
                ))
            }
        };
        let refresh_token = match lookup_path(body, self.refresh_token_path()) {
            None | Some(Value::Null) => None,
            Some(Value::String(t)) if t.is_empty() => None,
            Some(Value::String(t)) => Some(t.clone()),
            Some(other) => {
                return Err(format!(
                    "the refresh token at '{}' is not a string: {other}",
                    self.refresh_token_path()
                ))
            }
        };
        // A path the recipe declares is a promise, so its absence is
        // an error; the default `expires_in` may simply not be there.
        let declared_but_missing =
            |path: &str| format!("the answer carries no expiry at '{path}', which the recipe declares");
        // A token already dead when it arrives would be renewed on
        // every use, so a lifetime of nothing is unreadable, not a time.
        let lived = |path: &str, v: &Value, lifetime: chrono::Duration| {
            if lifetime > chrono::Duration::zero() {
                TokenExpiry::At(TokenLife::lasting(now, lifetime))
            } else {
                TokenExpiry::Unreadable(format!(
                    "the expiry at '{path}' ({v}) leaves the token no time to live from the moment \
                     the answer arrived"
                ))
            }
        };
        let expiry = if self.never_expires {
            TokenExpiry::Never
        } else if let Some(path) = &self.expires_path {
            match lookup_path(body, path) {
                None | Some(Value::Null) => TokenExpiry::Unreadable(declared_but_missing(path)),
                Some(v) => match absolute_time(v) {
                    Some(at) => lived(path, v, at - now),
                    None => TokenExpiry::Unreadable(format!(
                        "the expiry at '{path}' is neither an RFC 3339 time nor epoch seconds: {v}"
                    )),
                },
            }
        } else {
            let path = self.expires_in_path().unwrap_or("expires_in");
            match lookup_path(body, path) {
                None | Some(Value::Null) if self.expires_in_path.is_some() => {
                    TokenExpiry::Unreadable(declared_but_missing(path))
                }
                None | Some(Value::Null) => TokenExpiry::Unknown,
                Some(v) => match seconds_of(v) {
                    Some(secs) => lived(path, v, chrono::Duration::seconds(secs)),
                    None => TokenExpiry::Unreadable(format!(
                        "the lifetime at '{path}' is not a number of seconds: {v}"
                    )),
                },
            }
        };
        let scope = match lookup_path(body, self.scope_path()) {
            Some(Value::String(s)) => Some(
                s.split([' ', ','])
                    .filter(|p| !p.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>(),
            ),
            Some(Value::Array(items)) => {
                Some(items.iter().filter_map(Value::as_str).map(str::to_string).collect())
            }
            _ => None,
        }
        .filter(|v| !v.is_empty());
        Ok(TokenRead { token, refresh_token, expiry, scope })
    }
}

impl AnswerReader for TokenAnswer {
    fn refusal_paths(&self) -> RefusalPaths<'_> {
        RefusalPaths { error: Some(self.error_path()), ok: self.ok_path.as_deref() }
    }

    fn validate(&self, what: &str) -> Result<(), String> {
        TokenAnswer::validate(self, what)
    }
}

/// Where an answer says it was refused although its status said
/// success: the one refusal check every declared call, token call and
/// lookup goes through ([`RefusalPaths::refusal`]). A reader declares
/// the paths; this is what they are read with.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RefusalPaths<'a> {
    /// A refusal's code or message: anything there but `null`, `false`
    /// or an empty string refuses.
    pub error: Option<&'a str>,
    /// A success flag: anything there but `true` (or `null`) refuses.
    pub ok: Option<&'a str>,
}

/// OAuth's `error`, where most JSON APIs put a refusal's code: the
/// error path a [`TokenAnswer`] reads by default, and where a refused
/// answer's code is read to classify it when its reader declares no
/// error path (never, there, to decide that it refused).
pub const CONVENTIONAL_ERROR_PATH: &str = "error";

impl<'a> RefusalPaths<'a> {
    /// The refusal `body` carries, in the provider's words: the code at
    /// the error path (an error object's `message`, or the object
    /// itself), else, when only the success flag refused, the answer
    /// itself. `None` = not refused.
    pub fn refusal(&self, body: &Value) -> Option<String> {
        let said = self.error.and_then(|path| lookup_path(body, path)).filter(|v| {
            !matches!(v, Value::Null | Value::Bool(false)) && v.as_str() != Some("")
        });
        let flagged = self
            .ok
            .and_then(|path| lookup_path(body, path))
            .is_some_and(|v| !matches!(v, Value::Null | Value::Bool(true)));
        match said {
            Some(Value::String(code)) => Some(code.clone()),
            Some(other) => {
                let message = other.get("message").and_then(Value::as_str);
                Some(message.map(str::to_string).unwrap_or_else(|| {
                    crate::truncate_user_string(&other.to_string(), 300)
                }))
            }
            None => flagged.then(|| crate::truncate_user_string(&body.to_string(), 300)),
        }
    }

    /// The error codes a refused answer names, read at the declared
    /// error path ([`CONVENTIONAL_ERROR_PATH`] when none is): a code
    /// there, or an error object's `status` and the `reason` of each
    /// of its `errors` (Google's `{"error": {"status": code, "errors":
    /// [{"reason": code}]}}`). What classifies a refusal.
    pub fn codes<'b>(&self, body: &'b Value) -> Vec<&'b str> {
        let Some(error) = lookup_path(body, self.error.unwrap_or(CONVENTIONAL_ERROR_PATH)) else {
            return Vec::new();
        };
        if let Some(code) = error.as_str() {
            return vec![code];
        }
        let reasons = error.get("errors").and_then(Value::as_array).into_iter().flatten().filter_map(|e| e.get("reason"));
        error.get("status").into_iter().chain(reasons).filter_map(Value::as_str).collect()
    }
}

/// What a declared call's answer is read with: at least where it says
/// it refused. A plain call ([`RefusalReader`]) reads nothing more; a
/// token call ([`TokenAnswer`]) reads its token too.
pub trait AnswerReader {
    fn refusal_paths(&self) -> RefusalPaths<'_>;
    /// Refuse a contradictory reader, naming the site (`what`).
    fn validate(&self, what: &str) -> Result<(), String>;
}

/// The reader of a call that answers no token (a test call, a socket
/// mint, a subscribe): where its answer says it refused despite a
/// success status. Nothing is read by default, because a success
/// answer may legitimately carry an `error` field (TikTok's carries
/// `{"error": {"code": "ok"}}`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RefusalReader {
    /// A refusal's code or message: anything there but `null`, `false`
    /// or an empty string refuses the answer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_path: Option<String>,
    /// A success flag: anything there but `true` or `null` refuses the
    /// answer (Slack's `ok`). Nothing there passes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ok_path: Option<String>,
}

impl AnswerReader for RefusalReader {
    fn refusal_paths(&self) -> RefusalPaths<'_> {
        RefusalPaths { error: self.error_path.as_deref(), ok: self.ok_path.as_deref() }
    }

    fn validate(&self, what: &str) -> Result<(), String> {
        validate_refusal_paths(what, &self.error_path, &self.ok_path)
    }
}

/// Refuse an empty refusal path, naming the site (`what`).
pub fn validate_refusal_paths(what: &str, error_path: &Option<String>, ok_path: &Option<String>) -> Result<(), String> {
    for (key, path) in [("errorPath", error_path), ("okPath", ok_path)] {
        if path.as_deref().is_some_and(|p| p.trim().is_empty()) {
            return Err(format!("{what}.{key} is empty; name a path, or leave the key out"));
        }
    }
    Ok(())
}

/// A whole number of seconds, from a JSON number or a string of digits.
fn seconds_of(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// An absolute time, from an RFC 3339 string or epoch seconds (a
/// number, or a string of digits).
fn absolute_time(v: &Value) -> Option<chrono::DateTime<chrono::Utc>> {
    if let Some(Ok(at)) = v.as_str().map(chrono::DateTime::parse_from_rfc3339) {
        return Some(at.with_timezone(&chrono::Utc));
    }
    seconds_of(v).and_then(|secs| chrono::DateTime::from_timestamp(secs, 0))
}

/// A declared call that answers a token: a [`ConnectCall`] whose
/// answer is read with a [`TokenAnswer`], flat in one object. What
/// `afterExchange` and a declared `refresh` are.
pub type TokenCall = ConnectCall<TokenAnswer>;

impl TokenCall {
    /// Refuse a malformed call, or one interpolating a value nothing
    /// provides: every placeholder must be in `known`.
    fn validate_over(&self, what: &str, known: &[String]) -> Result<(), String> {
        self.validate(what)?;
        for name in self.value_names()? {
            if !known.contains(&name) {
                return Err(format!(
                    "{what} interpolates '{{{name}}}', which nothing stores at that point \
                     (known: {})",
                    known.join(", ")
                ));
            }
        }
        Ok(())
    }
}

/// The OAuth2 grant flow.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", rename_all_fields = "camelCase", deny_unknown_fields)]
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
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TokenExchange {
    /// POST target; a template over the pasted fields
    /// (`https://api.github.com/app/installations/{installation_id}/access_tokens`).
    pub url: Template,
    /// Where the answer keeps the token and its expiry, flat on the
    /// exchange (`tokenPath`, `expiresPath`, ...). An answer naming
    /// no expiry is re-minted once the JWT's own lifetime runs out. A
    /// minted token is re-minted, never refreshed, so a refresh token
    /// or a scope echo means nothing here.
    #[serde(flatten)]
    pub answer: TokenAnswer,
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
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
    /// Where the answer says the credential was refused despite the
    /// expected status (`errorPath`, `okPath`), flat on the call.
    #[serde(flatten)]
    pub answer: RefusalReader,
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
#[serde(tag = "kind", rename_all = "snake_case", rename_all_fields = "camelCase", deny_unknown_fields)]
pub enum AuthStep {
    /// `Authorization: Bearer {token}`, `X-Shopify-Access-Token:
    /// {token}`, `Bot {token}` ... any header. The name is a template
    /// too, so a recipe whose header name is itself something the user
    /// types (a generic API key connection) writes `"name": "{header}"`;
    /// it may only name a declared, non-secret field.
    Header { name: Template, value: Template },
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
#[serde(tag = "kind", rename_all = "snake_case", rename_all_fields = "camelCase", deny_unknown_fields)]
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
/// credentials and read these values out of the answer". `A` is what
/// the answer is read with: a [`RefusalReader`] for a plain call, a
/// [`TokenAnswer`] for a [`TokenCall`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectCall<A = RefusalReader> {
    /// The address, as a template over the caller's values (a watch
    /// call addresses `.../files/{target}/watch`).
    pub url: Template,
    #[serde(default)]
    pub method: TestMethod,
    /// JSON body to send. Any string leaf is a template over the
    /// caller's values; absent = no body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<Value>,
    /// A form body (`application/x-www-form-urlencoded`) instead, the
    /// way a token endpoint takes its parameters: the provider's
    /// parameter names, each value a template over the caller's
    /// values. Never beside `body`, never on a GET.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub form: BTreeMap<String, Template>,
    /// How this call authenticates, in the same vocabulary as a
    /// service's own `auth`: templates over the values the caller
    /// resolved (a socket mint uses `Bearer {appToken}`, which is a
    /// DIFFERENT stored value from the API token).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub auth: Vec<AuthStep>,
    /// Values read out of the answer and kept for the steps that
    /// follow (the socket URL, the subscription's expiry).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub captures: Vec<Capture>,
    /// What the answer is read with, flat on the call (`errorPath`,
    /// `okPath`, and a token call's `tokenPath` and the rest).
    #[serde(flatten)]
    pub answer: A,
}

impl<A: AnswerReader> ConnectCall<A> {
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
        for template in self.form.values() {
            for name in template.placeholders()? {
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
        self.answer.validate(what)?;
        if !self.form.is_empty() {
            if self.body.is_some() {
                return Err(format!("{what} declares both a JSON `body` and a `form`; send one"));
            }
            if self.method == TestMethod::Get {
                return Err(format!("{what} sends a `form` on a GET, which carries no body; use POST"));
            }
            if let Some(name) = self.form.keys().find(|k| !is_param_name(k)) {
                return Err(format!("{what}.form has '{name}', which is not a usable parameter name"));
            }
        }
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
/// inbound frame carries something at `whenField`, send `frame` back
/// with `{value}` replaced by what was found there.
///
/// This is the generic shape of every ack protocol surveyed (Slack's
/// envelope_id echo, a gateway's sequence ack): the language owns
/// "read a field, echo it in a frame", and WHICH field and WHICH
/// frame are the service's declared data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
#[serde(rename_all = "camelCase")]
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
                     address would be unknown; capture it (or point `urlFrom` at the \
                     capture that carries it)",
                    self.url_from
                ));
            }
        }
        for rule in &self.replies {
            if rule.when_field.trim().is_empty() {
                return Err(format!("{what}.replies needs a non-empty whenField"));
            }
        }
        Ok(())
    }
}

/// A string interpolating STORED VALUES by name: `"Bearer {token}"`,
/// `"/bot{token}/"`. The only way a spec references a value; names
/// resolve against the grant's stored values at apply time, and an
/// unknown name is a loud error. A literal brace is doubled: `{{` and
/// `}}` (a GraphQL query in a JSON body).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Template(pub String);


/// The values a consent stores that are secrets by nature, whatever the
/// recipe declares: the tokens (stored as `token` and `refresh_token`)
/// and the app's own secret.
const SECRET_TOKEN_NAMES: &[&str] = &["token", "refresh_token", "client_secret"];

/// Why a [`Template`] could not be filled in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemplateError {
    /// The template itself does not read: a `{` never closed, a `}`
    /// never opened, or a placeholder name outside [A-Za-z0-9_].
    Malformed(String),
    /// It names a value the caller did not give: the name.
    Missing(String),
}

impl std::fmt::Display for TemplateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TemplateError::Malformed(why) => f.write_str(why),
            TemplateError::Missing(name) => {
                write!(f, "template references '{{{name}}}' but the access stores no value named '{name}'")
            }
        }
    }
}

impl From<TemplateError> for String {
    fn from(e: TemplateError) -> Self {
        e.to_string()
    }
}

impl From<&str> for Template {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl Template {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// The template read into its pieces, in order: the one parser of
    /// the template grammar, behind both [`Template::placeholders`] and
    /// every interpolation, so what validates at load is exactly what
    /// fills in at use. A placeholder is `{name}`, the name made of
    /// [A-Za-z0-9_]; `{{` and `}}` are a literal `{` and `}` (a JSON
    /// body's GraphQL query, `{{ viewer {{ login }} }}`); a `{` never
    /// closed, a lone `}`, and an empty or badly spelled name are each
    /// [`TemplateError::Malformed`].
    fn pieces(&self) -> Result<Vec<TemplatePiece<'_>>, TemplateError> {
        let malformed = |why: String| TemplateError::Malformed(format!("{why} in template {:?}", self.0));
        let text = self.0.as_str();
        let bytes = text.as_bytes();
        let mut pieces = Vec::new();
        // Every brace is ASCII, so each index below is a char boundary.
        let mut literal_from = 0;
        let mut at = 0;
        fn literal<'a>(pieces: &mut Vec<TemplatePiece<'a>>, text: &'a str, from: usize, to: usize) {
            if to > from {
                pieces.push(TemplatePiece::Literal(&text[from..to]));
            }
        }
        while at < bytes.len() {
            match (bytes[at], bytes.get(at + 1)) {
                // A doubled brace is one literal brace: the text up to
                // and including the first, then on past the second.
                (b'{', Some(b'{')) | (b'}', Some(b'}')) => {
                    literal(&mut pieces, text, literal_from, at + 1);
                    at += 2;
                    literal_from = at;
                }
                (b'{', _) => {
                    literal(&mut pieces, text, literal_from, at);
                    let Some(len) = text[at + 1..].find('}') else {
                        return Err(malformed("unclosed '{'".into()));
                    };
                    let name = &text[at + 1..at + 1 + len];
                    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                        return Err(malformed(format!("bad placeholder '{{{name}}}' (names are [A-Za-z0-9_]+)")));
                    }
                    pieces.push(TemplatePiece::Value(name));
                    at += len + 2;
                    literal_from = at;
                }
                (b'}', _) => return Err(malformed("stray '}' (a literal one is written '}}')".into())),
                _ => at += 1,
            }
        }
        literal(&mut pieces, text, literal_from, bytes.len());
        Ok(pieces)
    }

    /// The value names this template interpolates, in order of first
    /// appearance. Also the validator: a malformed template is an `Err`
    /// ([`Template::pieces`]). This is what decides which stored values
    /// a worker may receive: exactly the names the declared steps
    /// interpolate, nothing more.
    pub fn placeholders(&self) -> Result<Vec<String>, String> {
        let mut names: Vec<String> = Vec::new();
        for piece in self.pieces()? {
            if let TemplatePiece::Value(name) = piece {
                if !names.iter().any(|n| n == name) {
                    names.push(name.to_string());
                }
            }
        }
        Ok(names)
    }

    /// Interpolate against `values`. Unknown names are loud errors
    /// (never an empty substitution: a blank secret spliced into an
    /// auth header is a silent authentication failure).
    pub fn resolve(&self, values: &BTreeMap<String, String>) -> Result<String, String> {
        Ok(self.interpolate(values, |_, value| value.to_string())?)
    }

    /// Interpolate a template that IS a URL. A value landing in the
    /// path or the query is percent-encoded (everything but RFC 3986's
    /// unreserved characters), so a secret holding `&` or `/` stays
    /// one value. A value landing before the path (the scheme, host
    /// and port: a captured API base like `{instance_url}/...`) is
    /// URL structure, and goes in as it is.
    pub fn resolve_url(&self, values: &BTreeMap<String, String>) -> Result<String, String> {
        Ok(self.resolve_url_typed(values)?)
    }

    /// [`Template::resolve_url`] with the reason typed, for a caller
    /// that names a missing value in its own words (a lookup's missing
    /// parent is one the person has to pick first).
    pub fn resolve_url_typed(&self, values: &BTreeMap<String, String>) -> Result<String, TemplateError> {
        self.interpolate(values, |before, value| {
            let path_started = before.contains('?')
                || before.split_once("://").is_some_and(|(_, rest)| rest.contains('/'));
            if path_started {
                percent_encode(value)
            } else {
                value.to_string()
            }
        })
    }

    /// The one interpolation loop: `place` turns a value into what is
    /// spliced in, given everything written before it.
    fn interpolate(
        &self,
        values: &BTreeMap<String, String>,
        place: impl Fn(&str, &str) -> String,
    ) -> Result<String, TemplateError> {
        let mut out = String::with_capacity(self.0.len());
        for piece in self.pieces()? {
            match piece {
                TemplatePiece::Literal(text) => out.push_str(text),
                TemplatePiece::Value(name) => {
                    let value = values.get(name).ok_or_else(|| TemplateError::Missing(name.to_string()))?;
                    let placed = place(&out, value);
                    out.push_str(&placed);
                }
            }
        }
        Ok(out)
    }
}

/// One piece of a parsed [`Template`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TemplatePiece<'t> {
    /// Text written as it is.
    Literal(&'t str),
    /// A `{name}` placeholder: the name.
    Value(&'t str),
}

/// Look a dotted [`Capture::path`] up in a JSON value. Numeric
/// segments index arrays. The one path-lookup used by every capture
/// site (store acquisition, test calls, lookup pagination).
pub fn lookup_path<'v>(value: &'v Value, path: &str) -> Option<&'v Value> {
    // The empty path names the root: what a lookup whose response IS
    // the item array declares (`"items": ""`).
    if path.is_empty() {
        return Some(value);
    }
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
            AuthStep::Header { name, value } => {
                ts.push(name);
                ts.push(value);
            }
            AuthStep::Query { value, .. } => ts.push(value),
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

/// RFC 7230 token charset, what `HeaderName` accepts: the one test for
/// an auth header name, run on a literal at load and on a templated
/// one once resolved.
pub fn is_header_name(name: &str) -> bool {
    !name.is_empty()
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
        })
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

/// What a recipe must look like to describe something a project RUNS
/// ITSELF, rather than a third party it calls.
///
/// Such a service has no provider behind it: nothing to consent to,
/// no events it pushes, no connect-time call to make. Saying that once
/// is what lets the compiler refuse it at build time (where the author
/// is looking) and the store refuse it at write time (where the input
/// is untrusted), without the two drifting apart.
///
/// Hands back the settings a publisher fills, because the caller that
/// needs them is the caller that just proved they exist. Returning
/// them here is what keeps a `let else { unreachable!() }` out of a
/// request handler serving untrusted callers.
pub fn publishable_fields(spec: &AccessSpec) -> Result<&[CredentialField], String> {
    let Acquisition::Static { fields } = &spec.acquisition else {
        return Err(format!(
            "a node publishes a connection to something it runs itself, which is described \
             by its stored settings alone; the '{}' service is acquired another way",
            spec.service
        ));
    };
    if !spec.events.is_empty() {
        return Err(format!(
            "the '{}' service reports provider events, which a service a project runs \
             itself cannot",
            spec.service
        ));
    }
    if spec.test.is_some() {
        return Err(format!(
            "the '{}' service declares a connect-time check, which has no meaning for a \
             service a project runs itself",
            spec.service
        ));
    }
    Ok(fields)
}

/// `values` reduced to what may actually be STORED for `spec`, or the
/// reason they cannot be: every required field present and non-blank,
/// no name the service does not declare, the service's all-or-nothing
/// groups satisfied, and blanks dropped so a later read sees "missing"
/// rather than an empty secret.
///
/// The gate for every set of values a PERSON or a NODE supplied: a
/// pasted connection, a minted one, and one a node published all
/// answer the same questions and are held to the same standard. A
/// browser consent is the one that does not come through here, and
/// cannot: its values are the provider's own answer to the token
/// exchange, not fields anybody filled in.
///
/// A node's publish reaches it through [`publishable_connection`],
/// which holds the recipe and the values together.
pub fn storable_values(
    spec: &AccessSpec,
    fields: &[CredentialField],
    mut values: BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, String> {
    for f in fields {
        let empty = values.get(&f.name).is_none_or(|v| v.trim().is_empty());
        if empty && !f.optional {
            return Err(format!("field '{}' is required", f.name));
        }
    }
    if let Some(unknown) = values.keys().find(|k| !fields.iter().any(|f| &&f.name == k)) {
        return Err(format!(
            "the '{}' service declares no value named '{unknown}'",
            spec.service
        ));
    }
    capability_shortfall(&spec.capabilities, |name| {
        values.get(name).is_some_and(|v| !v.trim().is_empty())
    })?;
    values.retain(|_, v| !v.trim().is_empty());
    Ok(values)
}

/// What a node's publish stores: the values as the store keeps them
/// (blanks dropped), and the connection's display identity resolved
/// from them, which the published connection carries back to the node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishableConnection {
    pub values: BTreeMap<String, String>,
    pub identity: Option<String>,
}

/// The whole check a node's publish passes, or the reason it cannot:
/// the recipe valid ([`AccessSpec::validate`]) and of the shape a
/// published connection has ([`publishable_fields`]), the values
/// storable ([`storable_values`]), and the identity resolvable from
/// what is stored (an identity naming an optional value left blank is
/// refused here, not shown as a broken label).
///
/// ONE definition, because two places hold a publish to it and they
/// must not be able to disagree: the access store, on every real
/// write, and the node-test fake rig, so a test of a node that
/// publishes refuses exactly what production refuses and answers the
/// identity production answers.
pub fn publishable_connection(
    spec: &AccessSpec,
    values: BTreeMap<String, String>,
) -> Result<PublishableConnection, String> {
    spec.validate()?;
    let fields = publishable_fields(spec)?;
    let values = storable_values(spec, fields, values)?;
    let identity = spec.identity_of(&values)?;
    Ok(PublishableConnection { values, identity })
}

/// The recipe a node publishing `service` carries, from whichever
/// node's metadata declares the service, held to the shape a published
/// connection can have ([`publishable_fields`]). `publisher` names the
/// publishing node in the refusal.
///
/// Discovery refuses a second node claiming one service, so a catalog
/// built from a node tree has exactly one answer. That is a guarantee
/// of the DISCOVERY, not of this function: hand it a registry built
/// another way and the first match wins, so give it the metadata a
/// discovered catalog produced.
///
/// `publisher` is what the refusal names the publishing node by: a
/// program's node id when a program is built, its node type when a
/// package's tests are.
///
/// ONE definition, because two places resolve it and they must not be
/// able to disagree, both against the WHOLE catalog: the compiler, to
/// stamp a publishing node's definition, and the node-test crate's
/// emitter, to bake the recipes a package's fake tests are held to.
/// Refused at build time because a recipe a published connection
/// cannot have is an authoring mistake, and it belongs on the author's
/// screen.
pub fn published_recipe<'a>(
    metadata: impl IntoIterator<Item = &'a crate::node::NodeMetadata>,
    publisher: &str,
    service: &str,
) -> Result<&'a AccessSpec, String> {
    let spec = metadata
        .into_iter()
        .find_map(|m| m.service.as_ref().filter(|spec| spec.service == service))
        .ok_or_else(|| {
            format!(
                "node '{publisher}' publishes a '{service}' connection, but no node in the \
                 catalog declares that service"
            )
        })?;
    publishable_fields(spec)?;
    Ok(spec)
}

impl AccessSpec {
    /// A connection's snapshot of its recipe, as its row stores it
    /// (`access_grant.spec_json`).
    pub const STORED: crate::stored::Kind<AccessSpec> = crate::stored::Kind::new("connection's recipe", ACCESS_SPEC_UPGRADES);

    /// Stamp this recipe's identity onto an access widget: the service
    /// name, and whether the owning node runs without a connection
    /// picked. THE one definition of the stamp, shared by the
    /// compiler's enrich pass and the metadata's compact wiring view,
    /// so the two can never drift apart. No-op on any other widget.
    pub fn stamp_onto(&self, widget: &mut crate::node::Widget) {
        if let crate::node::Widget::Access { service, optional } = widget {
            *service = Some(self.service.clone());
            *optional = self.connection_optional;
        }
    }

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

    /// The display identity of a connection holding `values`, from the
    /// recipe's `identity` template; `None` when it declares none. A
    /// name the values do not hold is refused, never shown blank.
    pub fn identity_of(&self, values: &BTreeMap<String, String>) -> Result<Option<String>, String> {
        self.identity.as_ref().map(|t| t.resolve(values)).transpose()
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
        // A literal header or query name is static spec data, so a
        // malformed one is refused here, at load, rather than on every
        // request. A templated header name is checked against the
        // declared fields below, and its resolved value when applied.
        for step in &self.auth {
            match step {
                AuthStep::Header { name, .. } => {
                    if name.placeholders()?.is_empty() && !is_header_name(&name.0) {
                        return Err(format!(
                            "auth header name '{}' is not a legal HTTP header name",
                            name.0
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
            Acquisition::OAuth2 {
                extra_params,
                auth_params,
                client_params,
                registration_fields,
                captures,
                answer,
                after_exchange,
                refresh,
                ..
            } => {
                client_params.validate()?;
                answer.validate("acquisition")?;
                // What each call can interpolate: the app's values
                // everywhere; the consent URL only the ones that are
                // not secret (it is shown in the browser); the calls
                // after the exchange also what it stored.
                let public: Vec<String> = std::iter::once("client_id".to_string())
                    .chain(registration_fields.iter().filter(|f| !f.secret).map(|f| f.name.clone()))
                    .collect();
                check_params("acquisition.authParams", auth_params, &public)?;
                check_params("acquisition.extraParams", extra_params, &oauth_registration(registration_fields))?;
                validate_captures(captures)?;
                if let Some(call) = after_exchange {
                    if !self.needs_browser_consent() {
                        return Err(
                            "acquisition.afterExchange runs after a code exchange, and a \
                             client_credentials grant has none; fold it into the token request"
                                .into(),
                        );
                    }
                    call.validate_over(
                        "acquisition.afterExchange",
                        &oauth_exchanged(registration_fields, captures),
                    )?;
                }
                if let Some(call) = refresh {
                    // The renewal runs over everything the connection
                    // stores: the exchange's, the call after it, and the
                    // connect-time check's captures.
                    let mut stored = oauth_exchanged(registration_fields, captures);
                    if let Some(after) = after_exchange {
                        stored.extend(after.captures.iter().map(|c| c.name.clone()));
                    }
                    if let Some(test) = &self.test {
                        stored.extend(test.captures.iter().map(|c| c.name.clone()));
                    }
                    call.validate_over("acquisition.refresh", &stored)?;
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
                    ex.answer.validate("acquisition.exchange")?;
                    if ex.answer.refresh_token_path.is_some() || ex.answer.scope_path.is_some() {
                        return Err(
                            "acquisition.exchange declares refreshTokenPath or scopePath, but a \
                             minted token is minted again when it runs out, never refreshed, \
                             and asks for no scopes; remove them"
                                .into(),
                        );
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
        let secret_fields: std::collections::HashSet<String> = field_lists
            .iter()
            .flat_map(|fs| fs.iter())
            .filter(|f| f.secret)
            .map(|f| f.name.clone())
            .collect();
        // A templated header name is filled from what the user typed at
        // connect, so it must name a field they are asked for, and a
        // non-secret one: a header name travels and prints in the clear.
        for step in &self.auth {
            let AuthStep::Header { name, .. } = step else { continue };
            for field in name.placeholders()? {
                let declared =
                    field_lists.iter().flat_map(|fs| fs.iter()).find(|f| f.name == field);
                match declared {
                    None => {
                        return Err(format!(
                            "auth header name '{}' names the field '{field}', which this \
                             service does not declare",
                            name.0
                        ))
                    }
                    Some(f) if f.secret => {
                        return Err(format!(
                            "auth header name '{}' names '{field}', a secret field: a header \
                             name is sent and shown in the clear, so build it from a \
                             non-secret field",
                            name.0
                        ))
                    }
                    Some(_) => {}
                }
            }
        }
        for fields in field_lists {
            let mut seen = std::collections::HashSet::new();
            for f in fields {
                if !f.name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                    || f.name.is_empty()
                {
                    return Err(format!(
                        "field name '{}' is invalid: use only letters, digits, and '_'",
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
            test.answer.validate("test")?;
        }
        if let Some(identity) = &self.identity {
            // The identity is shown wherever a connection is listed (the
            // editor, `weft connect --list`, an instance's settings page), so
            // it may never be assembled from a secret.
            let names = identity.placeholders()?;
            for name in &names {
                if SECRET_TOKEN_NAMES.contains(&name.as_str()) || secret_fields.contains(name) {
                    return Err(format!(
                        "identity names '{name}', a secret: it is shown wherever the connection \
                         is listed, so build it from a non-secret value (an account name, a host)"
                    ));
                }
            }
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
                if p.always && !self.needs_browser_consent() {
                    return Err(format!(
                        "permission '{}' is `always` (asked for on every consent), but this service \
                         connects without a browser consent, so nothing would ever ask for it",
                        p.id
                    ));
                }
                if p.always && (p.default || p.own_only) {
                    return Err(format!(
                        "permission '{}' is `always` (asked for on every consent, never ticked), so it \
                         cannot also be `default` or `ownOnly`",
                        p.id
                    ));
                }
            }
        }
        crate::access::events::validate_topics(&self.service, &self.events)?;
        if let Some(verify) = &self.verify {
            // The scheme reads the connection: every value it needs must
            // be a field the paste page declares, or no connection of
            // this service could ever verify anyone.
            let declared = self.own_fields();
            for name in verify.required_value_names()? {
                match declared.iter().find(|f| f.name == name) {
                    None => {
                        return Err(format!(
                            "service '{}' declares a `{}` verify that reads the stored value \
                             '{name}', but no credential field of that name is declared; add it \
                             to the acquisition fields",
                            self.service,
                            verify.tag()
                        ))
                    }
                    Some(field) if field.optional => {
                        return Err(format!(
                            "service '{}' declares a `{}` verify that reads the stored value \
                             '{name}' on every check, but its field is `optional`, so a connection \
                             left without it could verify nobody; drop `optional` from the field",
                            self.service,
                            verify.tag()
                        ))
                    }
                    Some(_) => {}
                }
            }
            verify.validate(&self.service, crate::access::events::VerifySide::Caller)?;
        }
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
                        "ownPage.paste is redundant: this acquisition already IS a paste \
                         form (its fields render on the page); declare the fields there"
                            .into(),
                    );
                }
                if paste.fields.is_empty() {
                    return Err("ownPage.paste needs at least one field".into());
                }
                // The pasted values are ALL a static row stores, so
                // they must cover every value the auth steps
                // interpolate; a shortfall would connect fine and then
                // fail every resolution.
                let names = self.worker_value_names()?;
                for name in &names {
                    if !paste.fields.iter().any(|f| &f.name == name) {
                        return Err(format!(
                            "ownPage.paste is missing a '{name}' field: the auth steps \
                             interpolate it, so a pasted connection must collect it"
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// The spec a PASTED connection stores: this spec with its
    /// acquisition replaced by `Static` over the `ownPage.paste`
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
        // A pasted credential is asked for nothing: what a consent asks
        // for every time does not apply to it.
        variant.permissions.retain(|p| !p.always);
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

    /// Does connecting run a BROWSER consent (begin + callback + poll)?
    /// Narrower than [`is_oauth`]: a `client_credentials` grant uses an
    /// app registration but exchanges it server-to-server in one
    /// request, so it connects through the direct path.
    // SYNC: needs_browser_consent <-> packages/weft-connect/src/core/recipe.ts needsConsent
    pub fn needs_browser_consent(&self) -> bool {
        matches!(
            self.acquisition,
            Acquisition::OAuth2 { grant: OAuthGrant::AuthorizationCode { .. }, .. }
        )
    }

    /// The service's display name: the declared label, else the service
    /// name itself.
    // SYNC: display_label <-> packages/weft-connect/src/core/recipe.ts displayLabel
    pub fn display_label(&self) -> &str {
        self.label.as_deref().unwrap_or(&self.service)
    }

    /// The catalogue entries that start ticked.
    // SYNC: AccessSpec::default_permissions <-> packages/weft-connect/src/core/recipe.ts defaultPermissions
    // Own-account-only entries are capability declarations, never
    // consent asks, so they are never ticked (and never ride a
    // consent URL).
    pub fn default_permissions(&self) -> Vec<String> {
        self.permissions
            .iter()
            .filter(|p| p.default && !p.own_only)
            .map(|p| p.id.clone())
            .collect()
    }

    /// The catalogue entries a person ticks: everything but what is asked
    /// for always, and the own-account-only capabilities (set up inside
    /// the provider's account, never asked for at connect).
    // SYNC: AccessSpec::tickable_permissions <-> packages/weft-connect/src/core/recipe.ts tickablePermissions
    pub fn tickable_permissions(&self) -> Vec<&Permission> {
        self.permissions.iter().filter(|p| !p.always && !p.own_only).collect()
    }

    /// What a consent asks for when `ticked` is ticked: the permissions
    /// asked for always ([`Permission::always`]), then the ticked ones,
    /// each once.
    // SYNC: AccessSpec::requested_permissions <-> packages/weft-connect/src/core/recipe.ts requestedPermissions
    pub fn requested_permissions(&self, ticked: &[String]) -> Vec<String> {
        let mut out: Vec<String> = self.permissions.iter().filter(|p| p.always).map(|p| p.id.clone()).collect();
        for id in ticked {
            if !out.contains(id) {
                out.push(id.clone());
            }
        }
        out
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
    // SYNC: AccessSpec::own_fields <-> packages/weft-connect/src/core/recipe.ts ownFields
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

    /// The guide's steps with `{permissions}` replaced by the requested
    /// permissions' labels (comma-joined), generated per pick rather
    /// than written as a static blob. Empty when no guide is declared.
    // SYNC: guide_steps <-> packages/weft-connect/src/core/recipe.ts guideSteps
    pub fn guide_steps(&self, ticked: &[String]) -> Vec<String> {
        let Some(guide) = self.own_page.as_ref().and_then(|p| p.guide.as_ref()) else {
            return Vec::new();
        };
        let requested = self.requested_permissions(ticked);
        let labels: Vec<&str> = self
            .permissions
            .iter()
            .filter(|p| requested.contains(&p.id))
            .map(|p| p.label.as_str())
            .collect();
        let joined = labels.join(", ");
        guide.steps.iter().map(|s| s.replace("{permissions}", &joined)).collect()
    }

    /// The guide's link with `{permissions}` replaced by the requested
    /// permission IDS, urlencoded and comma-joined: a link goes to a
    /// provider console, which wants machine ids where the steps'
    /// prose wants labels. The ONE substitution, so no surface renders
    /// the same link differently. None when no guide or no link is
    /// declared.
    // SYNC: guide_link <-> packages/weft-connect/src/core/recipe.ts guideLink
    pub fn guide_link(&self, ticked: &[String]) -> Option<String> {
        let link = self.own_page.as_ref()?.guide.as_ref()?.link.as_ref()?;
        Some(link.replace("{permissions}", &percent_encode(&self.requested_permissions(ticked).join(","))))
    }
}

/// Percent-encode everything outside the RFC 3986 unreserved set.
/// Hand-rolled because the pure type layer carries no url crate;
/// public so callers with the same need (a provider building a query
/// path) do not grow their own copy.
// SYNC: percent_encode <-> packages/weft-connect/src/server/passthrough.ts encodeSegment
pub fn percent_encode(s: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => {
                out.push('%');
                out.push(HEX[(b >> 4) as usize] as char);
                out.push(HEX[(b & 0x0f) as usize] as char);
            }
        }
    }
    out
}

/// What an OAuth2 acquisition's calls can interpolate from its app:
/// `client_id`, `client_secret`, and the app's own fields.
fn oauth_registration(registration_fields: &[CredentialField]) -> Vec<String> {
    ["client_id", "client_secret"]
        .into_iter()
        .map(String::from)
        .chain(registration_fields.iter().map(|f| f.name.clone()))
        .collect()
}

/// What an OAuth2 connection stores once its code exchange answered:
/// its app's values ([`oauth_registration`]), the token and refresh
/// token, and the exchange's `captures`.
fn oauth_exchanged(registration_fields: &[CredentialField], captures: &[Capture]) -> Vec<String> {
    let mut names = oauth_registration(registration_fields);
    names.extend(["token".to_string(), "refresh_token".to_string()]);
    names.extend(captures.iter().map(|c| c.name.clone()));
    names
}

/// Refuse a parameter map (`authParams`, `extraParams`) whose name is
/// unusable or whose value interpolates a name outside `known`.
fn check_params(
    what: &str,
    params: &BTreeMap<String, Template>,
    known: &[String],
) -> Result<(), String> {
    for (name, template) in params {
        if !is_param_name(name) {
            return Err(format!("{what} has '{name}', which is not a usable parameter name"));
        }
        for placeholder in template.placeholders()? {
            if !known.contains(&placeholder) {
                return Err(format!(
                    "{what}.{name} interpolates '{{{placeholder}}}', which it cannot use here \
                     (it can use: {})",
                    known.join(", ")
                ));
            }
        }
    }
    Ok(())
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

    /// A shipped app is public and whole: every rule `check_shipped` holds,
    /// one refusal each, and a public app with its required field passes.
    #[test]
    fn a_shipped_app_is_public_and_whole() {
        let field = |name: &str, secret: bool, optional: bool| CredentialField {
            name: name.into(),
            label: None,
            optional,
            secret,
            placeholder: None,
        };
        let app = |secret: Option<&str>, extra: &[(&str, &str)]| AppRegistration {
            label: "Ours".into(),
            client_id: "cid".into(),
            client_secret: secret.map(Into::into),
            extra: extra.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
        };
        let fields = vec![field("accountId", false, false), field("appToken", true, true)];

        assert_eq!(app(None, &[("accountId", "a")]).check_shipped(&fields), Ok(()));
        let e = app(Some("s"), &[("accountId", "a")]).check_shipped(&fields).unwrap_err();
        assert!(e.contains("must not carry a client_secret") && e.contains("access-apps.json"), "{e}");
        let e = app(None, &[("accountId", "a"), ("appToken", "t")]).check_shipped(&fields).unwrap_err();
        assert!(e.starts_with("'appToken' is a secret") && e.contains("\"Your own\""), "{e}");
        let e = app(None, &[("accountId", "a"), ("acountId", "a")]).check_shipped(&fields).unwrap_err();
        assert_eq!(e, "the app has a field 'acountId' its service does not declare (known: label, client_id, accountId)");
        let e = app(None, &[]).check_shipped(&fields).unwrap_err();
        assert_eq!(e, "the app is missing the required field 'accountId'");
        let mut unnamed = app(None, &[("accountId", "a")]);
        unnamed.label = " ".into();
        assert!(unnamed.check_shipped(&fields).unwrap_err().contains("needs a name"));

        // A service that needs a secret in every app cannot ship one.
        let needs_secret = vec![field("signingSecret", true, false)];
        let e = app(None, &[]).check_shipped(&needs_secret).unwrap_err();
        assert!(e.contains("cannot ship a public app at all") && e.contains("access-apps.json"), "{e}");

        // The connect path takes what a shipped app may not hold.
        assert_eq!(app(Some("s"), &[("accountId", "a"), ("appToken", "t")]).validate(&fields), Ok(()));
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
                           "authUrl": "https://slack.com/oauth/v2/authorize", "pkce": false },
                "tokenUrl": "https://slack.com/api/oauth.v2.access",
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
            "allPermissionsUrl": "https://docs.slack.dev/reference/scopes",
            "verification": { "rung": "reports_permissions", "cost": "free" },
            "ownPage": {
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

    /// `ownPage.paste` on a consent service: valid when its fields
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

    /// A connection's identity is listed everywhere, so a recipe may
    /// never build it from a secret field or a stored token.
    #[test]
    fn an_identity_built_from_a_secret_is_refused() {
        let spec = |identity: &str| -> AccessSpec {
            serde_json::from_value(json!({
                "service": "svc",
                "acquisition": { "kind": "static", "fields": [
                    { "name": "api_key", "label": "Key" },
                    { "name": "user", "label": "User", "secret": false }
                ] },
                "doors": ["own"],
                "identity": identity
            }))
            .unwrap()
        };
        assert!(spec("{user}").validate().is_ok());
        let err = spec("{api_key}").validate().unwrap_err();
        assert!(err.contains("'api_key', a secret"), "{err}");
        // The names a consent stores its tokens under.
        assert!(spec("{token}").validate().is_err());
        assert!(spec("{refresh_token}").validate().is_err());
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

    /// The guide LINK interpolates the ticked permission IDS,
    /// urlencoded (a provider console wants machine ids where the
    /// steps' prose wants labels).
    #[test]
    fn guide_link_interpolates_urlencoded_ids() {
        let mut spec = slack_spec();
        if let Some(guide) = spec.own_page.as_mut().and_then(|p| p.guide.as_mut()) {
            guide.link = Some("https://example.com/apps?scopes={permissions}".into());
        }
        let link = spec
            .guide_link(&["chat:write".into(), "files:write".into()])
            .expect("a link is declared");
        assert_eq!(link, "https://example.com/apps?scopes=chat%3Awrite%2Cfiles%3Awrite");
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
                { "name": "accessKeyId", "secret": false },
                { "name": "secretAccessKey" },
                { "name": "region", "secret": false }
            ]},
            "auth": [{ "kind": "sign", "with": {
                "kind": "sigv4", "service": "s3", "region": "{region}",
                "accessKeyId": "{accessKeyId}",
                "secretAccessKey": "{secretAccessKey}"
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
                    "tokenPath": "token",
                    "expiresPath": "expires_at"
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
                { "name": "accessKeyId", "secret": false },
                { "name": "secretAccessKey" },
                { "name": "region", "secret": false }
            ]},
            "auth": [{ "kind": "sign", "with": {
                "kind": "sigv4", "service": "s3", "region": "{region}",
                "accessKeyId": "{accessKeyId}",
                "secretAccessKey": "{secretAccessKey}"
            }}]
        }))
        .unwrap();
        s3.validate().unwrap();
        assert_eq!(
            s3.worker_value_names().unwrap(),
            vec!["region", "accessKeyId", "secretAccessKey"]
        );
    }

    /// The gate every set of values a person or a node supplied
    /// passes. A blank required value is MISSING, not stored: a
    /// downstream node would otherwise get an empty password and fail
    /// at the provider instead of here.
    #[test]
    fn the_value_check_holds_a_pasted_and_a_published_connection_to_one_rule() {
        // The fields come from the spec itself, so the rule and the
        // recipe cannot be a mismatched pair the production callers
        // could never produce.
        let spec: AccessSpec = serde_json::from_value(serde_json::json!({
            "service": "pg",
            "acquisition": { "kind": "static", "fields": [
                { "name": "host" },
                { "name": "port", "optional": true },
            ]},
        }))
        .expect("a static spec");
        let fields = publishable_fields(&spec).expect("a static spec publishes");
        let values = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
            pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
        };
        let check = |vals| storable_values(&spec, fields, vals);

        check(values(&[("host", "db"), ("port", "5432")])).expect("a complete set passes");

        let kept = check(values(&[("host", "db"), ("port", "   ")]))
            .expect("a blank optional passes");
        assert!(!kept.contains_key("port"), "a blank optional stores nothing");

        let e = check(values(&[("port", "5432")])).expect_err("required missing");
        assert!(e.contains("'host' is required"), "{e}");

        let e = check(values(&[("host", "  ")]))
            .expect_err("a blank required value is missing");
        assert!(e.contains("'host' is required"), "{e}");

        let e = check(values(&[("host", "db"), ("hostname", "db")]))
            .expect_err("undeclared name");
        assert!(e.contains("no value named 'hostname'"), "{e}");
    }

    /// A publish answers the identity the store would record, and an
    /// identity naming an optional value left blank is refused, since
    /// the stored values no longer hold it.
    #[test]
    fn a_publish_resolves_its_identity_from_what_is_stored() {
        let spec: AccessSpec = serde_json::from_value(serde_json::json!({
            "service": "pg",
            "acquisition": { "kind": "static", "fields": [
                { "name": "host", "secret": false },
                { "name": "database", "optional": true, "secret": false },
            ]},
            "identity": "{host}/{database}",
        }))
        .expect("a static spec");
        let values = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
            pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
        };

        let published = publishable_connection(&spec, values(&[("host", "db"), ("database", "app")]))
            .expect("a complete set publishes");
        assert_eq!(published.identity.as_deref(), Some("db/app"));
        assert_eq!(published.values, values(&[("host", "db"), ("database", "app")]));

        publishable_connection(&spec, values(&[("host", "db"), ("database", " ")]))
            .expect_err("the identity names a value the blank optional left out");
    }

    /// The capability rule, all four ways: a complete group passes, a
    /// half-filled one names what is missing, filling none names the
    /// choices, and a service declaring no groups is unaffected.
    #[test]
    fn capability_groups_are_all_or_nothing_and_at_least_one() {
        let caps = vec![
            Capability {
                label: "receive mail".into(),
                fields: vec!["imapHost".into(), "imapPort".into()],
            },
            Capability {
                label: "send mail".into(),
                fields: vec!["smtpHost".into(), "smtpPort".into()],
            },
        ];
        let filled = |names: &'static [&'static str]| move |n: &str| names.contains(&n);

        capability_shortfall(&caps, filled(&["imapHost", "imapPort"]))
            .expect("one complete group is enough");
        capability_shortfall(&caps, filled(&["smtpHost", "smtpPort"]))
            .expect("the other group alone is enough too");
        capability_shortfall(
            &caps,
            filled(&["imapHost", "imapPort", "smtpHost", "smtpPort"]),
        )
        .expect("both groups is the everyday case");

        // Half a group names the missing half and the capability.
        let err = capability_shortfall(&caps, filled(&["imapHost"])).unwrap_err();
        assert!(err.contains("receive mail") && err.contains("imapPort"), "{err}");

        // No group at all names every choice.
        let err = capability_shortfall(&caps, filled(&[])).unwrap_err();
        assert!(err.contains("able to do nothing") && err.contains("receive mail") && err.contains("send mail"), "{err}");

        // A service with no groups declared never refuses.
        capability_shortfall(&[], filled(&[])).expect("no rule, no refusal");

        // A field NO group names is untouched by both rules: it
        // neither completes a group nor satisfies rule 2 on its own.
        let err = capability_shortfall(&caps, filled(&["sendAs"])).unwrap_err();
        assert!(err.contains("nothing"), "an ungrouped field satisfies no group: {err}");
        capability_shortfall(&caps, filled(&["sendAs", "imapHost", "imapPort"]))
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
                { "name": "imapHost", "optional": true }
            ]},
            "auth": [{ "kind": "header", "name": "A", "value": "{user}" }],
            "capabilities": [{ "label": "receive mail", "fields": ["imapHost"] }]
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

    /// A consent asks for the permissions asked for always, then the
    /// ticked ones, each once; the guide names them all, since the app
    /// has to allow them all.
    #[test]
    fn a_consent_asks_for_the_always_permissions_and_the_ticked_ones() {
        let mut spec = slack_spec();
        spec.permissions[1].always = true;
        spec.permissions[1].default = false;
        let always = spec.permissions[1].id.clone();
        let first = spec.permissions[0].id.clone();
        assert_eq!(spec.requested_permissions(std::slice::from_ref(&first)), vec![always.clone(), first.clone()]);
        assert_eq!(spec.requested_permissions(&[always.clone(), first.clone()]), vec![always.clone(), first]);
        assert_eq!(spec.requested_permissions(&[]), vec![always]);
        assert!(spec.validate().is_ok());
    }

    /// Only a browser consent asks for anything, so `always` is refused on
    /// a service that connects any other way, and a consent service's
    /// paste variant (a credential pasted, asked for nothing) drops it.
    #[test]
    fn always_belongs_to_a_browser_consent() {
        let mut keyed: AccessSpec = serde_json::from_value(json!({
            "service": "keyed",
            "acquisition": { "kind": "static", "fields": [{ "name": "key", "secret": true }] },
            "permissions": [{ "id": "who", "label": "Who", "description": "Know the account.", "always": true }]
        }))
        .unwrap();
        assert!(keyed.validate().unwrap_err().contains("without a browser consent"));
        keyed.permissions[0].always = false;
        assert!(keyed.validate().is_ok());

        let mut consent = slack_spec();
        consent.permissions[1].always = true;
        consent.permissions[1].default = false;
        consent.own_page = Some(serde_json::from_value(json!({ "paste": { "fields": [{ "name": "token", "secret": true }] } })).unwrap());
        let pasted = consent.paste_variant().expect("a paste section");
        assert!(pasted.permissions.iter().all(|p| !p.always), "a paste asks for nothing");
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
            always: false,
            own_only: false,
            guide: None,
        });
        assert!(dup.validate().unwrap_err().contains("duplicate permission"));

        let mut ticked_always = slack_spec();
        ticked_always.permissions[0].always = true;
        assert!(ticked_always.permissions[0].default, "slack's first permission starts ticked");
        assert!(ticked_always.validate().unwrap_err().contains("`always`"));

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

        // A templated header name must name a declared, non-secret
        // field; a literal one is checked as a header name.
        let header_key = |fields: serde_json::Value, name: &str| -> Result<(), String> {
            let spec: AccessSpec = serde_json::from_value(json!({
                "service": "x",
                "label": "X",
                "doors": ["own"],
                "acquisition": { "kind": "static", "fields": fields },
                "auth": [{ "kind": "header", "name": name, "value": "{value}" }]
            }))
            .unwrap();
            spec.validate()
        };
        let fields = json!([
            { "name": "header", "label": "Header", "secret": false },
            { "name": "value", "label": "Value" }
        ]);
        header_key(fields.clone(), "{header}").unwrap();
        let err = header_key(fields.clone(), "{nope}").unwrap_err();
        assert!(err.contains("'nope'") && err.contains("does not declare"), "{err}");
        let err = header_key(fields.clone(), "{value}").unwrap_err();
        assert!(err.contains("secret"), "{err}");
        let err = header_key(fields, "X Bad").unwrap_err();
        assert!(err.contains("'X Bad'"), "{err}");

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
        assert!(Template::new("Bearer {bad-name}").placeholders().is_err());
        assert_eq!(Template::new("no holes").placeholders().unwrap(), Vec::<String>::new());
        // Repeats dedupe, order of first appearance.
        assert_eq!(
            Template::new("{a}{b}{a}").placeholders().unwrap(),
            vec!["a", "b"]
        );
    }

    /// One grammar for validation and interpolation: everything
    /// `placeholders` refuses, filling the template in refuses too, as
    /// `Malformed` (a stray `}` before a placeholder included, which
    /// neither caught before).
    #[test]
    fn interpolation_refuses_what_validation_refuses() {
        let values = BTreeMap::from([("token".to_string(), "t".to_string()), ("a".to_string(), "x".to_string())]);
        for bad in ["Bearer }", "a}b{token}", "Bearer {", "{bad-name}", "{}", "{a{token}"] {
            let template = Template::new(bad);
            assert!(template.placeholders().is_err(), "{bad} validates");
            for filled in [template.resolve_url_typed(&values), template.interpolate(&values, |_, v| v.to_string())] {
                assert!(matches!(filled, Err(TemplateError::Malformed(_))), "{bad} fills in: {filled:?}");
            }
        }
        assert!(Template::new("a}b{token}").placeholders().unwrap_err().contains("stray '}'"));
    }

    /// A doubled brace is a literal one, so a JSON body can carry a
    /// GraphQL query beside a placeholder; a single one stays a
    /// placeholder, and `{{name}}` is the literal text `{name}`.
    #[test]
    fn doubled_braces_are_literal_braces() {
        let values = BTreeMap::from([("login".to_string(), "ada".to_string())]);
        let query = Template::new("query {{ viewer {{ login }} }}");
        assert_eq!(query.placeholders().unwrap(), Vec::<String>::new());
        assert_eq!(query.resolve(&values).unwrap(), "query { viewer { login } }");
        assert_eq!(Template::new("{{ viewer }}").resolve(&values).unwrap(), "{ viewer }");
        let mixed = Template::new("{{\"user\": \"{login}\"}}");
        assert_eq!(mixed.placeholders().unwrap(), vec!["login"]);
        assert_eq!(mixed.resolve(&values).unwrap(), "{\"user\": \"ada\"}");
        assert_eq!(Template::new("{{login}}").resolve(&values).unwrap(), "{login}");
        let body = serde_json::json!({ "query": "{{ viewer {{ login }} }}", "as": "{login}" });
        assert_eq!(
            resolve_body(&body, &values).unwrap(),
            serde_json::json!({ "query": "{ viewer { login } }", "as": "ada" })
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

    fn verifier_spec(fields: &[&str], verify: serde_json::Value) -> serde_json::Result<AccessSpec> {
        serde_json::from_value(json!({
            "service": "api_key_auth",
            "acquisition": {
                "kind": "static",
                "fields": fields.iter().map(|f| json!({ "name": f })).collect::<Vec<_>>()
            },
            "verify": verify
        }))
    }

    /// A caller-verify recipe round-trips and is refused when it reads
    /// a value the paste page does not declare.
    #[test]
    fn a_verify_block_must_name_declared_fields() {
        let ok = verifier_spec(&["keys"], json!({ "kind": "api_keys" })).unwrap();
        ok.validate().expect("keys is declared");
        assert!(matches!(ok.verify, Some(crate::access::events::VerifyKind::ApiKeys { .. })));
        let v = serde_json::to_value(&ok).unwrap();
        assert_eq!(v["verify"], json!({ "kind": "api_keys" }), "defaults stay off the wire");

        let missing = verifier_spec(&["token"], json!({ "kind": "api_keys" })).unwrap();
        let err = missing.validate().expect_err("keys is not declared");
        assert!(err.contains("'keys'"), "{err}");

        let oidc = verifier_spec(
            &["jwksUrl", "issuer", "audience"],
            json!({ "kind": "oidc", "issuers": ["{issuer}"], "jwksUrl": "{jwksUrl}" }),
        )
        .unwrap();
        oidc.validate().expect("templated addresses over declared fields");
        let short = verifier_spec(
            &["issuer"],
            json!({ "kind": "oidc", "issuers": ["{issuer}"], "jwksUrl": "{jwksUrl}" }),
        )
        .unwrap();
        let err = short.validate().expect_err("jwksUrl is not declared");
        assert!(err.contains("'jwksUrl'"), "{err}");

        let echo = verifier_spec(&["token"], json!({ "kind": "token_echo" })).unwrap();
        let err = echo.validate().expect_err("token echo cannot gate callers");
        assert!(err.contains("token_echo"), "{err}");

        // A gate's signing scheme meets the same rules a push recipe
        // does: one signing {timestamp} with nowhere to read it from is
        // refused here, never verified at runtime with no replay check.
        let unsourced = verifier_spec(
            &["signingSecret"],
            json!({ "kind": "hmac", "signature": { "in": "header:X-Signature" }, "concat": "{timestamp}.{body}" }),
        )
        .unwrap();
        let err = unsourced.validate().expect_err("a signed timestamp needs a source");
        assert!(err.contains("no timestamp source"), "{err}");
    }

    /// The oidc scheme resolves its templates against a connection's
    /// values, and a push recipe may not carry placeholders at all.
    #[test]
    fn oidc_templates_resolve_against_the_connection() {
        use crate::access::events::VerifyKind;
        let kind: VerifyKind = serde_json::from_value(
            json!({ "kind": "oidc", "issuers": ["{issuer}"], "jwksUrl": "{jwksUrl}" }),
        )
        .unwrap();
        assert_eq!(
            kind.required_value_names().unwrap(),
            vec!["jwksUrl".to_string(), "issuer".to_string(), "audience".to_string()]
        );
        let values = BTreeMap::from([
            ("issuer".to_string(), "https://auth.example".to_string()),
            ("jwksUrl".to_string(), "https://auth.example/jwks".to_string()),
        ]);
        let resolved = kind.resolved(&values).unwrap();
        match resolved {
            VerifyKind::Oidc { issuers, jwks_url } => {
                assert_eq!(issuers[0].0, "https://auth.example");
                assert_eq!(jwks_url.0, "https://auth.example/jwks");
            }
            other => panic!("expected oidc, got {other:?}"),
        }
        let err = kind.resolved(&BTreeMap::new()).expect_err("a missing value is loud");
        assert!(err.contains("stores no value"), "{err}");

        let pushed = |verify: serde_json::Value| -> AccessSpec {
            serde_json::from_value(json!({
                "service": "svc",
                "acquisition": { "kind": "static", "fields": [{ "name": "token" }] },
                "events": {
                    "pushes": {
                        "fields": { "text": "event.text" },
                        "account": { "value": "token", "path": "account" },
                        "webhook": { "verify": verify }
                    }
                }
            }))
            .expect("parses")
        };
        let err = pushed(json!({ "kind": "oidc", "issuers": ["{issuer}"], "jwksUrl": "{jwksUrl}" }))
            .validate()
            .expect_err("a push recipe must be literal");
        assert!(err.contains("placeholder"), "{err}");
        let err = pushed(json!({ "kind": "api_keys" }))
            .validate()
            .expect_err("a push is never keyed");
        assert!(err.contains("api_keys"), "{err}");
    }

    fn read_at(answer: &TokenAnswer, body: serde_json::Value) -> Result<TokenRead, String> {
        let now = chrono::DateTime::from_timestamp(1_000_000, 0).unwrap();
        answer.read(&body, now)
    }

    /// The expiry of a token read at `read_at`'s moment (1_000_000)
    /// that dies at epoch `secs`.
    fn at(secs: i64) -> TokenExpiry {
        TokenExpiry::At(TokenLife {
            at: chrono::DateTime::from_timestamp(secs, 0).unwrap(),
            lifetime: chrono::Duration::seconds(secs - 1_000_000),
        })
    }

    /// The default reader is the RFC answer: `access_token`,
    /// `refresh_token`, `expires_in` (a number or a string of digits)
    /// and the `scope` echo split on spaces and commas.
    #[test]
    fn a_token_answer_reads_the_rfc_names_by_default() {
        let answer = TokenAnswer::default();
        let read = read_at(
            &answer,
            json!({ "access_token": "t", "refresh_token": "r", "expires_in": 3600, "scope": "a b,c" }),
        )
        .unwrap();
        assert_eq!(read.token, "t");
        assert_eq!(read.refresh_token.as_deref(), Some("r"));
        assert_eq!(read.expiry, at(1_003_600));
        assert_eq!(read.scope, Some(vec!["a".into(), "b".into(), "c".into()]));
        let read = read_at(&answer, json!({ "access_token": "t", "expires_in": "60" })).unwrap();
        assert_eq!(read.expiry, at(1_000_060), "a string of digits is seconds too");
        assert_eq!(read.refresh_token, None);
        let read = read_at(&answer, json!({ "access_token": "t" })).unwrap();
        assert_eq!(read.expiry, TokenExpiry::Unknown, "no expires_in is an unknown expiry, never an error");
        let err = read_at(&answer, json!({ "token": "t" })).unwrap_err();
        assert!(err.contains("'access_token'") && err.contains("tokenPath"), "{err}");
        let read = read_at(&answer, json!({ "access_token": "t", "refresh_token": "r2", "expires_in": "soon" })).unwrap();
        assert!(matches!(&read.expiry, TokenExpiry::Unreadable(why) if why.contains("expires_in")), "{:?}", read.expiry);
        assert_eq!(read.refresh_token.as_deref(), Some("r2"), "an unreadable expiry never drops the tokens");
    }

    fn unreadable(read: Result<TokenRead, String>) -> String {
        match read.unwrap().expiry {
            TokenExpiry::Unreadable(why) => why,
            other => panic!("expected an unreadable expiry, got {other:?}"),
        }
    }

    /// A nested answer (Instagram's `data[0]`) is read by declaring the
    /// paths; an absolute expiry reads RFC 3339 or epoch seconds; a
    /// declared expiry path that is missing is an unreadable expiry,
    /// since the recipe promised it.
    #[test]
    fn a_token_answer_reads_declared_paths() {
        let nested: TokenAnswer = serde_json::from_value(json!({
            "tokenPath": "data.0.access_token",
            "scopePath": "data.0.permissions",
            "expiresInPath": "data.0.ttl"
        }))
        .unwrap();
        let read = read_at(
            &nested,
            json!({ "data": [{ "access_token": "ig", "permissions": ["basic", "publish"], "ttl": 10 }] }),
        )
        .unwrap();
        assert_eq!(read.token, "ig");
        assert_eq!(read.scope, Some(vec!["basic".into(), "publish".into()]));
        assert_eq!(read.expiry, at(1_000_010));
        let err = unreadable(read_at(&nested, json!({ "data": [{ "access_token": "ig" }] })));
        assert!(err.contains("data.0.ttl"), "{err}");

        let absolute: TokenAnswer = serde_json::from_value(json!({ "expiresPath": "expires_at" })).unwrap();
        let read = read_at(&absolute, json!({ "access_token": "t", "expires_at": "1970-01-12T13:47:40Z" })).unwrap();
        assert_eq!(read.expiry, at(1_000_060), "the lifetime is measured from the moment the answer arrived");
        // An expiry at or before that moment would renew on every use.
        let err = unreadable(read_at(&absolute, json!({ "access_token": "t", "expires_at": "1970-01-12T13:46:40Z" })));
        assert!(err.contains("no time to live"), "{err}");
        let err = unreadable(read_at(&TokenAnswer::default(), json!({ "access_token": "t", "expires_in": 0 })));
        assert!(err.contains("expires_in"), "{err}");
        let read = read_at(&absolute, json!({ "access_token": "t", "expires_at": 2_000_000 })).unwrap();
        assert_eq!(read.expiry, at(2_000_000));
        // With an absolute path declared, `expires_in` is not read at all.
        let err = unreadable(read_at(&absolute, json!({ "access_token": "t", "expires_in": 5 })));
        assert!(err.contains("expires_at"), "{err}");

        let never: TokenAnswer = serde_json::from_value(json!({ "neverExpires": true })).unwrap();
        let read = read_at(&never, json!({ "access_token": "t", "expires_in": 5 })).unwrap();
        assert_eq!(read.expiry, TokenExpiry::Never);
    }

    /// A refusal answered with a success status: Slack's `ok: false`, an
    /// OAuth `error` code, or an error OBJECT (which used to pass), at the
    /// default `error` or a declared path.
    #[test]
    fn a_token_answer_sees_a_refusal_answered_200() {
        let answer = TokenAnswer::default();
        let refusal = |reader: &TokenAnswer, body: Value| reader.refusal_paths().refusal(&body);
        assert_eq!(refusal(&answer, json!({ "error": "invalid_grant" })).as_deref(), Some("invalid_grant"));
        assert_eq!(
            refusal(&answer, json!({ "error": { "message": "Invalid code", "code": 100 } })).as_deref(),
            Some("Invalid code")
        );
        assert!(refusal(&answer, json!({ "error": { "code": 100 } })).unwrap().contains("100"));
        for fine in [json!({ "access_token": "t" }), json!({ "error": null }), json!({ "error": "" }), json!({ "error": false })] {
            assert_eq!(refusal(&answer, fine.clone()), None, "{fine}");
        }
        // No success flag unless the reader declares one: `ok` means
        // nothing to a provider that never said it does.
        assert_eq!(refusal(&answer, json!({ "ok": false, "access_token": "t" })), None);
        let declared: TokenAnswer = serde_json::from_value(json!({ "errorPath": "error_type" })).unwrap();
        assert_eq!(
            refusal(&declared, json!({ "error_type": "OAuthException", "error_message": "used" })).as_deref(),
            Some("OAuthException")
        );
    }

    /// A declared success flag refuses anything there but `true`, in
    /// the error path's words when it has some, else the answer's own;
    /// nothing there passes.
    #[test]
    fn a_declared_success_flag_refuses_an_answer_that_is_not_ok() {
        let slack: TokenAnswer = serde_json::from_value(json!({ "okPath": "ok" })).unwrap();
        let refusal = |body: Value| slack.refusal_paths().refusal(&body);
        assert_eq!(refusal(json!({ "ok": false, "error": "invalid_code" })).as_deref(), Some("invalid_code"));
        assert!(refusal(json!({ "ok": false })).unwrap().contains("\"ok\":false"));
        assert!(refusal(json!({ "ok": "no" })).is_some(), "anything but true refuses");
        for fine in [json!({ "ok": true, "access_token": "t" }), json!({ "access_token": "t" }), json!({ "ok": null })] {
            assert_eq!(refusal(fine.clone()), None, "{fine}");
        }
        // A plain call reads no error path unless it declares one: a
        // success answer may carry an `error` object (TikTok's).
        let plain = RefusalReader::default();
        assert_eq!(plain.refusal_paths().refusal(&json!({ "error": { "code": "ok" } })), None);
        let flagged: RefusalReader = serde_json::from_value(json!({ "okPath": "ok", "errorPath": "error" })).unwrap();
        assert_eq!(flagged.refusal_paths().refusal(&json!({ "ok": false, "error": "invalid_auth" })).as_deref(), Some("invalid_auth"));
        assert!(serde_json::from_value::<RefusalReader>(json!({ "okPath": " " })).unwrap().validate("test").unwrap_err().contains("test.okPath"));
    }

    /// A refused answer's codes are read where the reader says the
    /// error sits, the conventional `error` when it says nothing.
    #[test]
    fn a_refusals_codes_are_read_at_the_declared_error_path() {
        let declared: TokenAnswer = serde_json::from_value(json!({ "errorPath": "error_type" })).unwrap();
        let body = json!({ "error_type": "temporarily_unavailable", "error": "ignored" });
        assert_eq!(declared.refusal_paths().codes(&body), vec!["temporarily_unavailable"]);
        let google = json!({ "error": { "status": "PERMISSION_DENIED", "errors": [{ "reason": "rateLimitExceeded" }] } });
        assert_eq!(RefusalReader::default().refusal_paths().codes(&google), vec!["PERMISSION_DENIED", "rateLimitExceeded"]);
    }

    #[test]
    fn a_contradictory_token_answer_is_refused() {
        for (bad, says) in [
            (json!({ "expiresInPath": "a", "expiresPath": "b" }), "both"),
            (json!({ "neverExpires": true, "expiresInPath": "a" }), "neverExpires"),
            (json!({ "tokenPath": " " }), "tokenPath"),
        ] {
            let answer: TokenAnswer = serde_json::from_value(bad.clone()).unwrap();
            let err = answer.validate("acquisition").unwrap_err();
            assert!(err.contains(says), "{bad}: {err}");
        }
        assert!(serde_json::from_value::<TokenAnswer>(json!({ "tokenpath": "x" })).is_err(), "a typo is refused");
    }

    /// A template that IS a URL encodes what lands in its path or query,
    /// and leaves a value that is the URL's base (scheme, host) as it is.
    #[test]
    fn a_url_template_encodes_its_path_and_query_values() {
        let values: BTreeMap<String, String> = [
            ("secret", "a&b=c/d e"),
            ("base", "https://eu.example.com"),
            ("id", "x/y"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let url = Template::new("https://h.example/t?client_secret={secret}&k=1").resolve_url(&values).unwrap();
        assert_eq!(url, "https://h.example/t?client_secret=a%26b%3Dc%2Fd%20e&k=1");
        let url = Template::new("{base}/files/{id}/watch").resolve_url(&values).unwrap();
        assert_eq!(url, "https://eu.example.com/files/x%2Fy/watch");
        // Outside a URL the value goes in raw (a header, a body).
        assert_eq!(Template::new("Bearer {secret}").resolve(&values).unwrap(), "Bearer a&b=c/d e");
    }

    /// The OAuth2 acquisition's new keys: they default (a stored
    /// snapshot without them parses), round-trip, and validate.
    #[test]
    fn the_token_keys_default_round_trip_and_validate() {
        let spec = slack_spec();
        let Acquisition::OAuth2 { client_params, answer, after_exchange, .. } = &spec.acquisition else { unreachable!() };
        assert_eq!(client_params.id, "client_id");
        assert_eq!(client_params.secret, "client_secret");
        assert_eq!(*answer, TokenAnswer::default());
        assert!(after_exchange.is_none());
        assert!(!serde_json::to_value(&spec).unwrap()["acquisition"].as_object().unwrap().contains_key("clientParams"));

        let full: AccessSpec = serde_json::from_value(json!({
            "service": "instagram",
            "acquisition": {
                "kind": "oauth2",
                "grant": { "kind": "authorization_code", "authUrl": "https://www.instagram.com/oauth/authorize", "pkce": false },
                "tokenUrl": "https://api.instagram.com/oauth/access_token",
                "clientParams": { "id": "client_key" },
                "tokenPath": "data.0.access_token",
                "scopePath": "data.0.permissions",
                "errorPath": "error_type",
                "authParams": { "app": "{client_id}" },
                "extraParams": { "aud": "{client_secret}" },
                "afterExchange": {
                    "url": "https://graph.instagram.com/access_token?grant_type=ig_exchange_token&client_secret={client_secret}&access_token={token}"
                },
                "refresh": {
                    "url": "https://graph.instagram.com/refresh_access_token",
                    "method": "POST",
                    "form": { "grant_type": "ig_refresh_token", "access_token": "{token}" },
                    "expiresInPath": "expires_in"
                }
            },
            "auth": [{ "kind": "header", "name": "Authorization", "value": "Bearer {token}" }]
        }))
        .unwrap();
        full.validate().expect("valid");
        let back: AccessSpec = serde_json::from_value(serde_json::to_value(&full).unwrap()).unwrap();
        assert_eq!(back, full);
        let Acquisition::OAuth2 { client_params, refresh, .. } = &full.acquisition else { unreachable!() };
        assert_eq!(client_params.id, "client_key");
        assert_eq!(client_params.secret, "client_secret", "an unset half keeps OAuth's name");
        let refresh = refresh.as_ref().unwrap();
        assert_eq!(refresh.form.get("grant_type").map(|t| t.0.as_str()), Some("ig_refresh_token"));
        assert_eq!(refresh.answer.expires_in_path.as_deref(), Some("expires_in"));

        let refused = |acquisition: serde_json::Value| -> String {
            let mut v = serde_json::to_value(&full).unwrap();
            for (k, val) in acquisition.as_object().unwrap() {
                v["acquisition"][k] = val.clone();
            }
            match serde_json::from_value::<AccessSpec>(v) {
                Ok(spec) => spec.validate().unwrap_err(),
                Err(e) => e.to_string(),
            }
        };
        assert!(refused(json!({ "authParams": { "x": "{client_secret}" } })).contains("authParams.x"));
        assert!(refused(json!({ "extraParams": { "x": "{nope}" } })).contains("extraParams.x"));
        assert!(refused(json!({ "afterExchange": { "url": "https://graph.instagram.com/x?t={nope}" } })).contains("afterExchange"));
        assert!(refused(json!({ "refresh": { "url": "https://g/x", "form": { "a": "b" } } })).contains("GET"));
        assert!(refused(json!({ "refresh": { "url": "https://g/x", "method": "POST", "form": { "a": "b" }, "body": {} } })).contains("both"));
        assert!(refused(json!({ "clientParams": { "id": "same", "secret": "same" } })).contains("same name"));
        assert!(refused(json!({ "clientParams": { "id": "a=b" } })).contains("clientParams.id"));
        assert!(refused(json!({ "refresh": { "url": "https://g/x", "tokenpath": "a" } })).contains("unknown field"));
        // A refresh may read what the connect-time check captured.
        let mut with_test = serde_json::to_value(&full).unwrap();
        with_test["test"] = json!({ "url": "https://graph.instagram.com/me", "captures": [{ "name": "userId", "path": "id" }] });
        with_test["acquisition"]["refresh"]["form"]["user"] = json!("{userId}");
        serde_json::from_value::<AccessSpec>(with_test).unwrap().validate().expect("test captures are stored values");
    }

    /// The minted JWT's exchange reads with the same reader, and the
    /// keys it always had (`tokenPath`, `expiresPath`) keep their
    /// spelling, so a stored snapshot still parses.
    #[test]
    fn the_jwt_exchange_reads_with_the_shared_reader() {
        let spec: AccessSpec = serde_json::from_value(json!({
            "service": "github_app",
            "acquisition": {
                "kind": "mint_jwt",
                "fields": [{ "name": "private_key" }, { "name": "installation_id", "secret": false }],
                "claims": { "iss": "1" },
                "exchange": {
                    "url": "https://api.github.com/app/installations/{installation_id}/access_tokens",
                    "tokenPath": "token",
                    "expiresPath": "expires_at"
                }
            }
        }))
        .unwrap();
        spec.validate().unwrap();
        let Acquisition::MintJwt { exchange: Some(exchange), .. } = &spec.acquisition else { unreachable!() };
        let read = read_at(&exchange.answer, json!({ "token": "ghs", "expires_at": "2030-01-01T00:00:00Z" })).unwrap();
        assert_eq!(read.token, "ghs");
        let v = serde_json::to_value(&spec).unwrap();
        assert_eq!(v["acquisition"]["exchange"]["tokenPath"], "token");
        let mut refreshable = v.clone();
        refreshable["acquisition"]["exchange"]["refreshTokenPath"] = json!("r");
        let err = serde_json::from_value::<AccessSpec>(refreshable).unwrap().validate().unwrap_err();
        assert!(err.contains("minted"), "{err}");
    }

    /// A stored value the consent writes is a secret whatever the recipe
    /// says, so an identity built from the token is refused.
    #[test]
    fn an_identity_built_from_the_stored_token_is_refused() {
        let mut spec = slack_spec();
        spec.identity = Some(Template::new("{token}"));
        assert!(spec.validate().unwrap_err().contains("'token'"));
    }
}
