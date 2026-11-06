//! The connect flow's wire types: what travels editor/CLI -> dispatcher
//! -> broker when a connection is listed, probed, created, or upgraded.
//! ONE definition per shape, here in the core so every client (the
//! access store that executes the flows, the dispatcher that forwards
//! them, the CLI's `weft connect`) deserializes the same struct instead
//! of re-declaring it.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::spec::{AccessSpec, AppRegistration, Door};
use super::CredentialOwner;

/// One registered app the editor offers as its own shared-door option:
/// its label and its FIXED permission set. The user picks an option;
/// they never tick permissions on the shared door.
// SYNC: SharedAppChoice <-> packages/weft-connect/src/core/wire.ts SharedAppChoice
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedAppChoice {
    pub label: String,
    pub covers: Vec<String>,
}

/// The doors probe's answer core: which shared-door options exist right
/// now. The ONE definition every service answering or forwarding the
/// probe uses (the editor hides what is not offered; it never greys).
// SYNC: DoorsAnswer <-> packages/weft-connect/src/core/wire.ts DoorsStatus (flattens it in)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DoorsAnswer {
    /// The registered apps, one shared-door option each (oauth
    /// services only). Empty = the one-click door is hidden.
    pub shared_apps: Vec<SharedAppChoice>,
    /// Whether a runtime credential backs the shared door of a
    /// non-oauth (key) service.
    pub shared_credential: bool,
}

/// What a node's published connection IS to everyone downstream: the
/// reference to open it with, plus the identity that names it in a
/// list. Both publish verbs and the read-back answer this ONE shape,
/// so a connection read back is the same value as the one published
/// (an identity that appeared on the first run and vanished on the
/// second was the bug this shape prevents).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishedConnection {
    /// The grant row's id. A string like every marker id on this wire.
    pub connection_id: String,
    pub identity: Option<String>,
}

/// The doors probe's request: the service recipe to probe. The client
/// (editor or CLI) posts it to the dispatcher, which forwards it to
/// the broker verbatim.
// SYNC: DoorsRequest <-> packages/weft-connect/src/core/transport.ts ConnectTransport.doors (its body)
#[derive(Debug, Serialize, Deserialize)]
pub struct DoorsRequest {
    pub spec: AccessSpec,
}

/// The full doors-probe answer as it reaches a client: the broker's
/// [`DoorsAnswer`] plus the consent-surface facts the dispatcher adds
/// (only it knows the public host).
// SYNC: DoorsStatus <-> packages/weft-connect/src/core/wire.ts DoorsStatus
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DoorsStatus {
    /// The broker's probe answer (shared-door options + the
    /// runtime-credential flag), flattened onto this wire unchanged.
    #[serde(flatten)]
    pub doors: DoorsAnswer,
    /// The callback URL an author registers on the provider's site,
    /// shown on the "Your own" page. `None` iff `consent_blocked`.
    pub redirect_uri: Option<String>,
    /// Why NO browser consent can run right now (the provider only
    /// accepts https callback URLs and this weft has none). Paste
    /// connects need no callback and stay available; the editor hides
    /// every consent button and shows this instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consent_blocked: Option<String>,
    /// The ids of the spec's ways to sign in this install offers, in the
    /// spec's order (`spec::LoginWay::offered_on`): a local-only way is
    /// left out on a cloud install. Empty for a service that takes no
    /// pasted credential, and for one none of whose ways works here.
    #[serde(default)]
    pub offered_ways: Vec<String>,
}

/// A connection row as the EDITOR sees it: names, ids, identity,
/// permissions. Never a stored value. Everything the connection list
/// renders (identity / app label / what it can do, plus the
/// spends-credits and verified marks) rides here, so the list needs no
/// second call.
// SYNC: GrantSummary <-> packages/weft-connect/src/core/wire.ts GrantSummary
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GrantSummary {
    pub id: uuid::Uuid,
    pub service: String,
    /// The owning project for a `coexisting`-class grant; `None` for an
    /// `exclusive`-class shared grant (every project referencing it
    /// follows its rotations).
    pub project_id: Option<uuid::Uuid>,
    /// The instance of `project_id` whose connection this is; `None` for
    /// the author's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<crate::instance::InstanceId>,
    pub identity: Option<String>,
    /// The connection list's middle column: the name the user typed,
    /// else the app's label, else the service's own. `None` only for a
    /// connection a node published without one.
    pub label: Option<String>,
    /// The way to sign in a pasted connection was made with
    /// (`spec::LoginWay::id`); `None` for one signed in any other way.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub way: Option<String>,
    /// The granted permission set (the "what it can do" column).
    pub scopes: Vec<String>,
    /// Whether `scopes` came from the provider (verified) or from the
    /// user's ticks (claimed). Decides how hard a shortfall fails.
    pub permissions_verified: bool,
    /// The NAMES of the values this connection stores (never the
    /// values themselves). What the editor's live `requiresValues`
    /// check compares against, for a service whose optional fields
    /// decide what a connection can do (a mailbox holding only the
    /// sending server can send and cannot receive).
    #[serde(default)]
    pub value_names: Vec<String>,
    /// Whose credential the row resolves to: `Platform` rows are the
    /// spends-credits connections, `Author` and `Instance` rows spend
    /// their owner's money.
    pub owner: CredentialOwner,
    /// Which door created it; drives the shared-door displacement
    /// warning (shown once per connection created through it).
    pub door: Door,
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Whether a credential stands behind the row right now. An
    /// `Author` or `Instance` row stores its own material, so always. A
    /// `Platform` row resolves to the runtime's own key at run time, which lives
    /// in the broker's shared-credentials file and can be gone (the
    /// file changed, the secret was emptied) while the row stays: the
    /// dispatcher asks the broker when it lists, and a picked row with
    /// nothing behind it is shown as not connected.
    #[serde(default)]
    pub has_credential: bool,
}

/// Which services the runtime holds its own credential for, asked by
/// the dispatcher when it lists connections (a `Platform` row is only a
/// connection while the key behind it exists).
// SYNC: SharedCredentialsQuery <-> crates/weft-broker/src/access_admin.rs shared_credentials
#[derive(Debug, Serialize, Deserialize)]
pub struct SharedCredentialsQuery {
    pub services: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SharedCredentialsAnswer {
    /// The subset of the asked services a runtime credential exists for.
    pub available: Vec<String>,
}

/// The services the apps file holds a sign-in app for (one with a
/// consent page): each such app lists the install's OAuth callback at its
/// provider, so it must be told when that callback moves.
// SYNC: SignInServices <-> crates/weft-broker/src/access_admin.rs sign_in_services
#[derive(Debug, Serialize, Deserialize)]
pub struct SignInServices {
    pub services: Vec<String>,
}

/// A ONE-REQUEST connect: the acquisitions needing no browser (`static`
/// paste, `mint_jwt`, server-to-server `oauth2`/`client_credentials`,
/// or the SHARED door of a key service). A browser-consent acquisition
/// is refused here; that is [`BeginOAuth`].
// SYNC: ConnectDirect <-> packages/weft-connect/src/core/transport.ts DirectConnect
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectDirect {
    pub spec: AccessSpec,
    /// Which door the user walked through. The SHARED door of a
    /// non-consent service stores a runtime-owned row; everything else
    /// is the user's own credential.
    #[serde(default = "own_door")]
    pub door: Door,
    /// The pasted field values, keyed by the spec's declared field
    /// names. Sent editor -> store directly; never node config.
    /// Empty for a shared-door connect.
    #[serde(default)]
    pub values: BTreeMap<String, String>,
    /// The name the user gave this connection (the list's middle
    /// column). `None` names it after its app, else after the service.
    #[serde(default)]
    pub label: Option<String>,
    /// The permissions the user CLAIMS the pasted credential holds
    /// (ticked on the picker, for key services with a permission
    /// concept). Recorded claimed unless verification upgrades them.
    /// On a SHARED-door connect the broker overwrites this with the
    /// chosen app's fixed `covers`; nothing the client sent survives.
    #[serde(default)]
    pub permissions: Vec<String>,
    /// The app to use, for the acquisitions that need one
    /// (`client_credentials`). Resolved at the broker before this is
    /// reached (the user's pasted app for the own door, the registered
    /// app for the shared door); `None` for a service using no app.
    #[serde(default)]
    pub registration: Option<AppRegistration>,
    /// The user pasted a ready credential through the service's
    /// `ownPage.paste` section instead of going through an app: the
    /// connect runs on [`AccessSpec::paste_variant`] (a `Static`
    /// acquisition over the paste fields).
    #[serde(default)]
    pub paste: bool,
    /// Which way to sign in `values` were filled for
    /// (`spec::LoginWay::id`), on the own door of a `static` service: what
    /// the connection records. May be left out when the service offers
    /// one way, which the paste section always is; a connect of any other
    /// kind names none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub way: Option<String>,
    /// The connecting project (grants are per-project by default; an
    /// exclusive-class OAuth grant ignores it, but direct connects are
    /// per-paste anyway).
    pub project_id: Option<uuid::Uuid>,
    /// The instance of `project_id` this connection is for. Stamped by the
    /// dispatcher from WHO is connecting (an instance token, or a trusted
    /// backend's `Weft-Instance` header), never taken from a client body:
    /// nobody connects in another instance's name.
    #[serde(default)]
    pub instance: Option<crate::instance::InstanceId>,
}

fn own_door() -> Door {
    Door::Own
}

/// The "Create it for me" app mint as it travels the wire (editor ->
/// dispatcher -> broker): the service's spec (whose `ownPage.mint`
/// recipe runs) and the ticked permissions baked into the manifest.
/// ONE definition so the hops cannot drift.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MintAppRequest {
    pub spec: AccessSpec,
    #[serde(default)]
    pub permissions: Vec<String>,
}

/// What the mint captured: the fresh app's values, for the editor to
/// prefill the "Your own" form (the user still names it and connects).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MintAppResponse {
    pub values: BTreeMap<String, String>,
}

/// The connect flow's answer: the summary the editor stores on the
/// node (`{id, identity}`) and shows.
// SYNC: CompletedConnect <-> packages/weft-connect/src/core/wire.ts CompletedConnect
#[derive(Debug, Serialize, Deserialize)]
pub struct CompletedConnect {
    pub grant: GrantSummary,
}

/// The shared-door wrapper on a connect request: which registered app
/// the user clicked, by label.
/// The label is consumed at the BROKER, which resolves it to the app
/// and its fixed `covers` before the store flow runs; the flows
/// themselves never read it. Defined here so the dispatcher's
/// forwarding and the broker's handling share one wire shape.
// SYNC: SharedDoorPick <-> packages/weft-connect/src/core/transport.ts DirectConnect.sharedApp, packages/weft-connect/src/core/transport.ts ConsentStart.sharedApp
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SharedDoorPick<T> {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shared_app: Option<String>,
    #[serde(flatten)]
    pub inner: T,
}

/// Start a browser consent (OAuth2 authorization_code): park the
/// pending state, answer the consent URL to open.
// SYNC: BeginOAuth <-> packages/weft-connect/src/core/transport.ts ConsentStart
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BeginOAuth {
    pub spec: AccessSpec,
    /// Which door the user walked through. Decides which app the
    /// broker resolves (the registered app for `shared`, the user's
    /// own / project app for `own`) and is recorded on the grant.
    #[serde(default = "own_door")]
    pub door: Door,
    /// The resolved app for the consent (client id, and the secret for
    /// the later code exchange). Resolved AT THE BROKER before this is
    /// reached; a client-sent app is only ever honored on the own door.
    #[serde(default)]
    pub registration: Option<AppRegistration>,
    /// The permissions the consent asks for (drive the consent URL and
    /// are recorded on the grant): the user's ticks on the own door,
    /// the chosen app's fixed `covers` on the shared door (overwritten
    /// by the broker; nothing the client sent survives there).
    pub permissions: Vec<String>,
    pub project_id: Option<uuid::Uuid>,
    /// The instance of `project_id` this consent is for; stamped by the
    /// dispatcher from who is connecting, exactly like
    /// [`ConnectDirect::instance`].
    #[serde(default)]
    pub instance: Option<crate::instance::InstanceId>,
    /// Upgrade/rotate this existing exclusive-class grant in place
    /// instead of minting a new row.
    pub upgrade_grant_id: Option<uuid::Uuid>,
    /// The callback URL the provider redirects to, derived from the
    /// dispatcher's public base and filled by it before the forward
    /// (the editor does not know the dispatcher's public host).
    #[serde(default)]
    pub redirect_uri: String,
}

// SYNC: StartedOAuth <-> packages/weft-connect/src/core/wire.ts StartedConsent
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartedOAuth {
    /// Open this in the user's browser.
    pub consent_url: String,
    pub state: String,
}

#[cfg(test)]
mod wire_tests {
    use super::*;
    use serde_json::json;

    fn spec() -> AccessSpec {
        serde_json::from_value(json!({
            "service": "openrouter",
            "acquisition": { "kind": "static", "ways": [{ "fields": [{ "name": "key" }] }] }
        }))
        .expect("minimal spec")
    }

    /// Layer-2: every struct on this wire round-trips through its JSON
    /// form. The dispatcher, broker, CLI, and editor all key off these
    /// shapes, so a silently-renamed field is the drift class this pins.
    #[test]
    fn every_wire_struct_round_trips() {
        let doors = DoorsStatus {
            doors: DoorsAnswer {
                shared_apps: vec![SharedAppChoice { label: "App".into(), covers: vec!["a".into()] }],
                shared_credential: true,
            },
            redirect_uri: Some("http://h/access/oauth/callback".into()),
            consent_blocked: None,
            offered_ways: vec!["apiKey".into()],
        };
        let v = serde_json::to_value(&doors).unwrap();
        // The broker's answer is FLATTENED onto the status: clients read
        // `shared_apps` at the top level, never under a `doors` key.
        assert!(v.get("sharedApps").is_some() && v.get("doors").is_none(), "{v}");
        assert_eq!(v["offeredWays"], serde_json::json!(["apiKey"]), "{v}");
        let back: DoorsStatus = serde_json::from_value(v).unwrap();
        assert_eq!(back.doors.shared_apps[0].label, "App");
        assert_eq!(back.offered_ways, vec!["apiKey"]);

        let grant = GrantSummary {
            id: uuid::Uuid::nil(),
            service: "openrouter".into(),
            project_id: None,
            instance: None,
            identity: Some("Q".into()),
            label: None,
            way: Some("apiKey".into()),
            scopes: vec!["s".into()],
            permissions_verified: true,
            value_names: vec!["key".into()],
            owner: CredentialOwner::Platform,
            door: Door::Shared,
            expires_at: None,
            has_credential: true,
        };
        let back: GrantSummary =
            serde_json::from_value(serde_json::to_value(&grant).unwrap()).unwrap();
        assert_eq!(back.id, grant.id);
        assert_eq!(back.owner, CredentialOwner::Platform);
        assert_eq!(back.door, Door::Shared);
        assert_eq!(back.way.as_deref(), Some("apiKey"));

        let done = CompletedConnect { grant };
        let back: CompletedConnect =
            serde_json::from_value(serde_json::to_value(&done).unwrap()).unwrap();
        assert_eq!(back.grant.service, "openrouter");

        let started = StartedOAuth { consent_url: "https://x".into(), state: "s".into() };
        let back: StartedOAuth =
            serde_json::from_value(serde_json::to_value(&started).unwrap()).unwrap();
        assert_eq!(back.state, "s");

        let mint = MintAppRequest { spec: spec(), permissions: vec!["p".into()] };
        let back: MintAppRequest =
            serde_json::from_value(serde_json::to_value(&mint).unwrap()).unwrap();
        assert_eq!(back.permissions, vec!["p"]);
        let resp = MintAppResponse { values: [("client_id".to_string(), "c".to_string())].into() };
        let back: MintAppResponse =
            serde_json::from_value(serde_json::to_value(&resp).unwrap()).unwrap();
        assert_eq!(back.values["client_id"], "c");

        let begin = BeginOAuth {
            spec: spec(),
            door: Door::Own,
            registration: None,
            permissions: vec![],
            project_id: Some(uuid::Uuid::nil()),
            instance: None,
            upgrade_grant_id: None,
            redirect_uri: String::new(),
        };
        let back: BeginOAuth =
            serde_json::from_value(serde_json::to_value(&begin).unwrap()).unwrap();
        assert_eq!(back.door, Door::Own);
    }

    /// The shared-door wrapper flattens to ONE object: `shared_app`
    /// beside the inner fields, absent when None; and an omitted `door`
    /// deserializes as the own door (the serde default).
    #[test]
    fn shared_door_pick_flattens_and_door_defaults_to_own() {
        let pick = SharedDoorPick {
            shared_app: None,
            inner: ConnectDirect {
                spec: spec(),
                door: Door::Shared,
                values: Default::default(),
                label: None,
                permissions: vec![],
                registration: None,
                paste: false,
                way: None,
                project_id: None,
                instance: None,
            },
        };
        let v = serde_json::to_value(&pick).unwrap();
        assert!(v.get("sharedApp").is_none(), "None sharedApp must be absent: {v}");
        assert_eq!(v.get("door").and_then(|d| d.as_str()), Some("shared"), "{v}");
        assert!(v.get("inner").is_none(), "the wrapper flattens; no `inner` key: {v}");

        let parsed: ConnectDirect = serde_json::from_value(json!({
            "spec": serde_json::to_value(spec()).unwrap(),
            "projectId": null
        }))
        .unwrap();
        assert_eq!(parsed.door, Door::Own, "an omitted door is the own door");
    }

    /// The exact nesting the dispatcher and broker deserialize: a full
    /// `ConnectDirect` inside the flattened `SharedDoorPick`, every
    /// field populated with a distinct value, plus the same for
    /// `BeginOAuth`. Nested flatten is where a serde attribute mistake
    /// hides, so this proves each field SURVIVES the whole trip.
    #[test]
    fn nested_flatten_round_trips_every_field() {
        let reg = AppRegistration {
            label: "My app".into(),
            client_id: "cid".into(),
            client_secret: Some("sec".into()),
            extra: [("team".to_string(), "t1".to_string())].into(),
        };
        let pick = SharedDoorPick {
            shared_app: Some("App".into()),
            inner: ConnectDirect {
                spec: spec(),
                door: Door::Own,
                values: [("key".to_string(), "v".to_string())].into(),
                label: Some("named".into()),
                permissions: vec!["read".into()],
                registration: Some(reg.clone()),
                paste: true,
                way: Some("apiKey".into()),
                project_id: Some(uuid::Uuid::nil()),
                instance: None,
            },
        };
        let v = serde_json::to_value(&pick).unwrap();
        let back: SharedDoorPick<ConnectDirect> = serde_json::from_value(v).unwrap();
        assert_eq!(back.shared_app.as_deref(), Some("App"));
        assert_eq!(back.inner.door, Door::Own);
        assert_eq!(back.inner.values["key"], "v");
        assert_eq!(back.inner.label.as_deref(), Some("named"));
        assert_eq!(back.inner.permissions, vec!["read"]);
        assert!(back.inner.paste);
        assert_eq!(back.inner.way.as_deref(), Some("apiKey"));
        assert_eq!(back.inner.project_id, Some(uuid::Uuid::nil()));
        let breg = back.inner.registration.expect("registration survives");
        assert_eq!(breg.client_id, "cid");
        assert_eq!(breg.client_secret.as_deref(), Some("sec"));
        assert_eq!(breg.extra["team"], "t1");

        let begin = SharedDoorPick {
            shared_app: None,
            inner: BeginOAuth {
                spec: spec(),
                door: Door::Shared,
                registration: Some(reg),
                permissions: vec!["write".into()],
                project_id: None,
                instance: None,
                upgrade_grant_id: Some(uuid::Uuid::nil()),
                redirect_uri: "http://h/cb".into(),
            },
        };
        let back: SharedDoorPick<BeginOAuth> =
            serde_json::from_value(serde_json::to_value(&begin).unwrap()).unwrap();
        assert_eq!(back.inner.door, Door::Shared);
        assert_eq!(back.inner.upgrade_grant_id, Some(uuid::Uuid::nil()));
        assert_eq!(back.inner.redirect_uri, "http://h/cb");

        let published =
            PublishedConnection { connection_id: "id-1".into(), identity: Some("who".into()) };
        let back: PublishedConnection =
            serde_json::from_value(serde_json::to_value(&published).unwrap()).unwrap();
        assert_eq!(back.connection_id, "id-1");
        assert_eq!(back.identity.as_deref(), Some("who"));

        let doors_req = DoorsRequest { spec: spec() };
        let back: DoorsRequest =
            serde_json::from_value(serde_json::to_value(&doors_req).unwrap()).unwrap();
        assert_eq!(back.spec.service, "openrouter");
    }
}
