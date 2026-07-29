//! The credentials weft's SHARED doors run on: every credential the
//! operator offers as a one-click connection, in ONE trusted json file.
//!
//! The file (path in `WEFT_ACCESS_APPS_FILE`) is keyed by service name,
//! each service holding a LIST of entries (always a list, even for one:
//! one shape, no sometimes-an-object branch). Every entry declares its
//! `kind`:
//!
//! - `"oauth_app"`: an app users sign into through the service's
//!   consent (client id + secret, the pinned sign-in addresses, a
//!   mandatory `covers` permission set shown as a FIXED option in the
//!   editor). Several apps per service let cheap permission tiers
//!   avoid a provider's review process.
//! - `"api_key"`: the runtime's own key for a pasted-key service (the
//!   "use the runtime's key" door). At most ONE per service; how it is
//!   handed out (directly, or swapped behind a relay) is the
//!   deployment's structure, never a knob in this file.
//!
//! ```json
//! { "google":     [ { "kind": "oauth_app", "label": "Google Drive",
//!                     "covers": ["..."],
//!                     "auth_url": "https://accounts.google.com/o/oauth2/v2/auth",
//!                     "token_url": "https://oauth2.googleapis.com/token",
//!                     "client_id": "...", "client_secret": "..." } ],
//!   "openrouter": [ { "kind": "api_key", "label": "Runtime key", "key": "sk-or-..." } ] }
//! ```
//!
//! The file's parse is cached against its raw bytes and reloaded the
//! moment the content changes, so editing it takes effect without a
//! restart and every request sees one consistent parse (a read rides
//! out an in-place rewrite rather than seeing it half-written). Absent
//! file = nothing configured, which is the honest state the access
//! node turns into "no one-click door; bring your own".
//!
//! `covers` entries are validated against the service's permission
//! catalogue wherever a spec is in hand (the door probe and every
//! shared connect), failing loudly naming the file and the string: a
//! typo must never silently remove the one-click door.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use serde::Deserialize;
use weft_core::{AccessSpec, AppRegistration};

pub use weft_core::access::spec::APPS_FILE_ENV;

/// One registered app: the credentials, the permissions it may ask
/// for, and the provider addresses it signs in against. `covers` is
/// mandatory, and empty means "may ask for nothing" (never
/// "everything": that default would let a typo'd permission name
/// silently grant a door that then dies at the consent screen).
///
/// `token_url` (and `auth_url` for consent services) pin where this
/// app's credentials are sent: a shared connect only ever talks to the
/// addresses written beside the app in this file, whatever a connect
/// request's recipe says. The operator registered the app at these
/// addresses, so writing them here is copying two lines they already
/// have.
#[derive(Debug, Clone, Deserialize)]
pub struct RegisteredApp {
    pub covers: Vec<String>,
    /// Present iff this app RECEIVES the service's event pushes: the
    /// per-deployment secret material the service's verification
    /// scheme keys on. Which scheme runs, and every path, comes from
    /// the service's events recipe; this block carries only what the
    /// operator was handed when registering the app.
    #[serde(default)]
    pub events: Option<AppEvents>,
    /// The token-exchange address this app's secret is POSTed to.
    pub token_url: String,
    /// The consent page (authorization-code services); absent for
    /// server-to-server (client-credentials) apps.
    #[serde(default)]
    pub auth_url: Option<String>,
    #[serde(flatten)]
    pub app: AppRegistration,
}

/// The receive-side material of one registered app; see
/// [`RegisteredApp::events`]. All fields optional because each
/// verification scheme needs a different subset; what a scheme
/// actually requires is refused loudly at verify time when missing.
#[derive(Debug, Clone, Deserialize)]
pub struct AppEvents {
    /// The shared secret an HMAC scheme keys on (Slack hands one out
    /// beside the client secret).
    #[serde(default)]
    pub signing_secret: Option<String>,
    /// The public key a signature scheme verifies against (Discord's
    /// hex key, SendGrid's base64 DER key). Not a secret, but handed
    /// out beside one, so it is configured in the same place.
    #[serde(default)]
    pub public_key: Option<String>,
    /// The audience a signed identity token must name. Defaults to
    /// the receiver's own address; set it when the push subscription
    /// was configured with an explicit audience.
    #[serde(default)]
    pub audience: Option<String>,
    /// The identity a signed identity token must be issued to (the
    /// account configured to push). Unset = any verified identity
    /// naming our audience.
    #[serde(default)]
    pub push_email: Option<String>,
}

/// The runtime's own key for a pasted-key service: what the shared
/// door hands the credential source. Just the credential and its
/// display name; whether workers receive it directly or a swapped
/// stand-in is the deployment's structure, deliberately NOT a field.
#[derive(Debug, Clone, Deserialize)]
pub struct ApiKeyEntry {
    pub label: String,
    pub key: String,
}

/// One entry of the shared-credentials file, by declared `kind`.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SharedCredential {
    OauthApp(RegisteredApp),
    ApiKey(ApiKeyEntry),
}

impl SharedCredential {
    fn label(&self) -> &str {
        match self {
            SharedCredential::OauthApp(app) => &app.app.label,
            SharedCredential::ApiKey(key) => &key.label,
        }
    }

    /// Trim the label in place at parse time, so what is stored (and
    /// compared, and displayed) is exactly what `app_by_label` will
    /// be asked for.
    fn trim_label(&mut self) {
        match self {
            SharedCredential::OauthApp(app) => app.app.label = app.app.label.trim().to_string(),
            SharedCredential::ApiKey(key) => key.label = key.label.trim().to_string(),
        }
    }
}

/// Resolves the registered apps a shared-door connect may use. Unlike
/// [`crate::credential::CredentialSource`], this takes no pool: an app is
/// static configuration read from a file, not runtime state that varies
/// per user.
#[async_trait::async_trait]
pub trait AppProvider: Send + Sync {
    /// Every registered OAuth app for `service`, in file order.
    async fn apps(&self, service: &str) -> anyhow::Result<Vec<RegisteredApp>>;
}

/// The default: apps read from a json file. A missing file means no apps
/// are configured (every shared door then stays hidden). A file that
/// exists but does not parse is a LOUD error on every lookup: a typo in
/// the app file must not read as "no app configured".
pub struct FileAppProvider {
    path: Option<PathBuf>,
    /// The parsed snapshot, keyed by the file's raw BYTES: the bytes
    /// are read on every lookup (cheap) and re-parsed only when they
    /// changed, so every request sees one consistent parse, an edit
    /// lands without a restart, and no filesystem timestamp
    /// granularity can hide a quick rewrite.
    #[allow(clippy::type_complexity)]
    cache: std::sync::Mutex<Option<(String, BTreeMap<String, Vec<SharedCredential>>)>>,
}

impl FileAppProvider {
    /// From `WEFT_ACCESS_APPS_FILE`; unset (or empty) means no file.
    pub fn from_env() -> Self {
        let path = std::env::var(APPS_FILE_ENV)
            .ok()
            .filter(|p| !p.trim().is_empty())
            .map(PathBuf::from);
        Self { path, cache: std::sync::Mutex::new(None) }
    }

    pub fn at(path: impl Into<PathBuf>) -> Self {
        Self { path: Some(path.into()), cache: std::sync::Mutex::new(None) }
    }

    /// The current parse of the file: the raw bytes are read on every
    /// lookup and the parse is cached against them, so a changed file
    /// (however quickly rewritten) always takes effect. A missing file
    /// is honestly "nothing configured"; a parse error is loud on
    /// EVERY lookup (never cached), exactly as if the file were read
    /// fresh each time.
    async fn snapshot(&self) -> anyhow::Result<BTreeMap<String, Vec<SharedCredential>>> {
        let Some(path) = &self.path else { return Ok(BTreeMap::new()) };
        let raw = match self.stable_read(path).await? {
            Some(raw) => raw,
            None => {
                self.cache.lock().expect("apps cache lock").take();
                return Ok(BTreeMap::new());
            }
        };
        if let Some((bytes, parsed)) = self.cache.lock().expect("apps cache lock").as_ref() {
            if *bytes == raw {
                return Ok(parsed.clone());
            }
        }
        let parsed = Self::parse_all(&raw, path)?;
        *self.cache.lock().expect("apps cache lock") = Some((raw, parsed.clone()));
        Ok(parsed)
    }

    /// Read the file's whole content, riding out an in-place rewrite:
    /// a plain save truncates then refills, so a single read can catch
    /// the file half-written or momentarily EMPTY, and an empty read
    /// silently taken as "nothing configured" would make the one-click
    /// doors vanish for that request with no error anywhere. Two
    /// identical consecutive reads = a stable view; an empty view
    /// while the cache remembers content is treated as mid-write and
    /// retried briefly. A file that genuinely stays empty (or
    /// malformed) is answered as such after the window. `None` = the
    /// file does not exist.
    async fn stable_read(&self, path: &Path) -> anyhow::Result<Option<String>> {
        let read = || match std::fs::read_to_string(path) {
            Ok(raw) => Ok(Some(raw)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(anyhow::Error::new(e))
                .with_context(|| format!("read access apps file {}", path.display())),
        };
        let remembered_content = self
            .cache
            .lock()
            .expect("apps cache lock")
            .as_ref()
            .is_some_and(|(bytes, _)| !bytes.trim().is_empty());
        let mut latest = read()?;
        for _ in 0..50 {
            let again = read()?;
            let stable = latest == again;
            let suspicious_empty = remembered_content
                && again.as_ref().is_some_and(|raw| raw.trim().is_empty());
            latest = again;
            if stable && !suspicious_empty {
                return Ok(latest);
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        Ok(latest)
    }

    /// Every entry the file's raw text declares, per service (`path`
    /// only names the file in errors).
    ///
    /// `_`-prefixed keys are ignored at both levels (the file's own
    /// notes, and per-entry setup hints), because json carries no
    /// comments and this file is meant to be read and edited by hand.
    /// Same `_`-means-not-data convention as node config keys.
    ///
    /// Shape rules checked here, loudly, naming the file: every service
    /// is a LIST, every entry declares its `kind` and a non-empty
    /// label (trimmed at parse, so the stored form is what the editor
    /// displays and `app_by_label` is asked for), labels are unique
    /// within a service ignoring case (the connection list would
    /// otherwise lie about which credential a connection used), and a
    /// service holds at most ONE `api_key` entry (the runtime-key
    /// door is a single option; two keys would be an ambiguous grant).
    fn parse_all(
        raw: &str,
        path: &Path,
    ) -> anyhow::Result<BTreeMap<String, Vec<SharedCredential>>> {
        // An empty file is an operator placeholder, not a parse error.
        if raw.trim().is_empty() {
            return Ok(BTreeMap::new());
        }
        let named: BTreeMap<String, serde_json::Value> = serde_json::from_str(&raw)
            .with_context(|| format!("parse access apps file {}", path.display()))?;
        let mut services = BTreeMap::new();
        for (service, value) in named {
            if service.starts_with('_') {
                continue;
            }
            let entries = value.as_array().cloned().ok_or_else(|| {
                anyhow::anyhow!(
                    "access apps '{service}' in {} must be a LIST of entries (always a \
                     list, even for one)",
                    path.display()
                )
            })?;
            let mut parsed: Vec<SharedCredential> = Vec::with_capacity(entries.len());
            for mut entry in entries {
                if let Some(obj) = entry.as_object_mut() {
                    obj.retain(|k, _| !k.starts_with('_'));
                }
                let mut cred: SharedCredential =
                    serde_json::from_value(entry).with_context(|| {
                        format!(
                            "entry under '{service}' in {} (every entry declares its \
                             kind: \"oauth_app\" or \"api_key\")",
                            path.display()
                        )
                    })?;
                cred.trim_label();
                if cred.label().is_empty() {
                    anyhow::bail!(
                        "an entry under '{service}' in {} has an empty label; every \
                         entry needs a name (it is the connection list's middle column)",
                        path.display()
                    );
                }
                if parsed.iter().any(|a| a.label().eq_ignore_ascii_case(cred.label())) {
                    anyhow::bail!(
                        "two entries under '{service}' in {} share the label '{}'; \
                         labels must be unique per service or the connection list lies",
                        path.display(),
                        cred.label()
                    );
                }
                if matches!(cred, SharedCredential::ApiKey(_))
                    && parsed.iter().any(|a| matches!(a, SharedCredential::ApiKey(_)))
                {
                    anyhow::bail!(
                        "'{service}' in {} declares two api_key entries; the runtime-key \
                         door is one option, keep one key per service",
                        path.display()
                    );
                }
                parsed.push(cred);
            }
            services.insert(service, parsed);
        }
        Ok(services)
    }

    /// The runtime's own key for `service`, if the file declares one.
    pub async fn api_key(&self, service: &str) -> anyhow::Result<Option<ApiKeyEntry>> {
        Ok(self
            .snapshot()
            .await?
            .remove(service)
            .unwrap_or_default()
            .into_iter()
            .find_map(|c| match c {
                SharedCredential::ApiKey(key) => Some(key),
                SharedCredential::OauthApp(_) => None,
            }))
    }
}

#[async_trait::async_trait]
impl AppProvider for FileAppProvider {
    async fn apps(&self, service: &str) -> anyhow::Result<Vec<RegisteredApp>> {
        Ok(self
            .snapshot()
            .await?
            .remove(service)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|c| match c {
                SharedCredential::OauthApp(app) => Some(app),
                SharedCredential::ApiKey(_) => None,
            })
            .collect())
    }
}

/// Refuse a `covers` entry the service's catalogue does not declare,
/// loudly naming the string: a typo would otherwise silently remove
/// the one-click door (the worst failure here: nothing appears and
/// nothing complains). Run wherever a spec is in hand (the door probe
/// and every shared connect).
pub fn check_covers(spec: &AccessSpec, apps: &[RegisteredApp]) -> anyhow::Result<()> {
    for app in apps {
        for covered in &app.covers {
            if !spec.declares_permission(covered) {
                anyhow::bail!(
                    "the access apps file's '{}' app for '{}' covers '{covered}', which is \
                     not in the service's permission catalogue; fix the apps file",
                    app.app.label,
                    spec.service
                );
            }
        }
    }
    Ok(())
}

/// Refuse an `events` block on a service whose recipe declares no
/// webhook transport on any topic: the operator configured receiving
/// for pushes that can never arrive, which is a misconfiguration to
/// say out loud, not to silently ignore. Runs inside
/// [`resolve_shared_app`] (so every shared connect enforces it) and
/// from the door probe (which has the apps in hand without picking
/// one).
pub fn check_events(
    spec: &weft_core::AccessSpec,
    apps: &[RegisteredApp],
) -> anyhow::Result<()> {
    let receives = spec.events.values().any(|t| t.webhook.is_some());
    for app in apps {
        if app.events.is_some() && !receives {
            anyhow::bail!(
                "the access apps file's '{}' app for '{}' declares an events block, but the \
                 service's recipe declares no webhook transport; remove the block or fix \
                 the recipe",
                app.app.label,
                spec.service
            );
        }
    }
    Ok(())
}

/// The registered app a SHARED-door connect uses, picked BY LABEL:
/// every registered app is its own option in the editor (each with its
/// fixed `covers` shown), so the user's click names exactly one app.
/// Labels are unique within a service (checked at file load).
pub fn app_by_label<'a>(apps: &'a [RegisteredApp], label: &str) -> Option<&'a RegisteredApp> {
    apps.iter().find(|a| a.app.label == label)
}

/// Refuse a shared connect whose recipe names sign-in addresses other
/// than the ones pinned beside the app in the file: a registered app's
/// credentials are only ever sent where the operator wrote, full stop.
/// (A user's own app has no pin; its addresses are theirs to point.)
pub fn check_endpoints(spec: &AccessSpec, app: &RegisteredApp) -> anyhow::Result<()> {
    let weft_core::access::spec::Acquisition::OAuth2 { grant, token_url, refresh, .. } =
        &spec.acquisition
    else {
        anyhow::bail!("a registered app only serves an oauth2 service");
    };
    if token_url != &app.token_url {
        anyhow::bail!(
            "the registered '{}' app for '{}' signs in against {} (pinned in the apps \
             file), not {token_url}; the one-click door only talks to its pinned addresses",
            app.app.label,
            spec.service,
            app.token_url
        );
    }
    // A declared renewal call runs with the app's credentials in
    // scope, so on a registered app it may only address the same
    // origin as the pinned token endpoint: the operator pinned where
    // this app's secrets travel, and the renewal is no exception.
    if let Some(call) = refresh {
        // The full origin (scheme + host + any explicit port), so a
        // pinned endpoint on a non-default port pins that port too.
        let pinned = url::Url::parse(&app.token_url)
            .ok()
            .map(|u| format!("{}/", u.origin().ascii_serialization()));
        let allowed = pinned
            .as_deref()
            .is_some_and(|origin| call.url.0.starts_with(origin));
        if !allowed {
            anyhow::bail!(
                "the '{}' service's renewal call addresses {}, but the registered '{}' \
                 app's credentials only travel to its pinned sign-in origin; the one-click \
                 door only talks to its pinned addresses",
                spec.service,
                call.url.0,
                app.app.label,
            );
        }
    }
    if let weft_core::access::spec::OAuthGrant::AuthorizationCode { auth_url, .. } = grant {
        match &app.auth_url {
            Some(pinned) if pinned == auth_url => {}
            Some(pinned) => anyhow::bail!(
                "the registered '{}' app for '{}' consents at {pinned} (pinned in the apps \
                 file), not {auth_url}; the one-click door only talks to its pinned addresses",
                app.app.label,
                spec.service,
            ),
            None => anyhow::bail!(
                "the registered '{}' app for '{}' pins no auth_url in the apps file, but \
                 the service signs in through a browser consent; add the app's auth_url \
                 beside its token_url",
                app.app.label,
                spec.service,
            ),
        }
    }
    Ok(())
}

/// The shared-door app for one connect, end to end: load the service's
/// registered apps, validate their `covers` against the catalogue and
/// their `events` blocks against the recipe, pick the one the user
/// chose by label, and hold the recipe to the app's pinned sign-in
/// addresses. Loud when the label names nothing (the file changed
/// since the editor listed the doors). The whole entry comes back,
/// because a shared connect's permissions ARE the app's `covers`:
/// fixed by the operator, never ticked by the user.
pub async fn resolve_shared_app(
    provider: &Arc<dyn AppProvider>,
    spec: &AccessSpec,
    label: &str,
) -> anyhow::Result<RegisteredApp> {
    let apps = provider.apps(&spec.service).await?;
    check_covers(spec, &apps)?;
    check_events(spec, &apps)?;
    let picked = app_by_label(&apps, label).ok_or_else(|| {
        anyhow::anyhow!(
            "no registered '{}' app is labelled '{label}'; reopen the connect panel \
             to see the current options",
            spec.service
        )
    })?;
    check_endpoints(spec, picked)?;
    Ok(picked.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &std::path::Path, name: &str, body: &str) -> PathBuf {
        let p = dir.join(name);
        std::fs::write(&p, body).unwrap();
        p
    }

    fn app(label: &str, covers: &[&str]) -> RegisteredApp {
        RegisteredApp {
            covers: covers.iter().map(|s| s.to_string()).collect(),
            events: None,
            token_url: "https://a/t".into(),
            auth_url: Some("https://a/x".into()),
            app: AppRegistration {
                label: label.into(),
                client_id: format!("cid-{label}"),
                client_secret: Some("sec".into()),
                extra: Default::default(),
            },
        }
    }

    fn spec_with_permissions(ids: &[&str]) -> AccessSpec {
        serde_json::from_value(serde_json::json!({
            "service": "google",
            "acquisition": { "kind": "oauth2",
                "grant": { "kind": "authorization_code", "auth_url": "https://a/x" },
                "token_url": "https://a/t" },
            "permissions": ids.iter().map(|id| serde_json::json!({
                "id": id, "label": id, "description": "d."
            })).collect::<Vec<_>>()
        }))
        .unwrap()
    }

    /// The user's click names exactly one app by label; an unknown
    /// label is nothing (the caller turns it into a loud error), never
    /// a different app substituted.
    #[test]
    fn app_by_label_picks_exactly_the_named_app() {
        let apps = vec![app("Broad", &["a", "b", "c"]), app("Narrow", &["a", "b"])];
        assert_eq!(app_by_label(&apps, "Narrow").unwrap().app.label, "Narrow");
        assert_eq!(app_by_label(&apps, "Broad").unwrap().covers.len(), 3);
        assert!(app_by_label(&apps, "Gone").is_none());
    }

    /// A `covers` entry outside the catalogue is a LOUD error naming
    /// the label and the string, never a silently-hidden door.
    #[test]
    fn check_covers_names_the_typo() {
        let spec = spec_with_permissions(&["a", "b"]);
        assert!(check_covers(&spec, &[app("Fine", &["a"])]).is_ok());
        let err = check_covers(&spec, &[app("Typo", &["a", "bb"])]).unwrap_err().to_string();
        assert!(err.contains("'bb'"), "{err}");
        assert!(err.contains("Typo"), "{err}");
    }

    /// A registered app's credentials only ever travel to the
    /// addresses pinned beside it in the file: a recipe naming any
    /// other token/consent address is refused loudly, and a consent
    /// service whose app pins no auth_url is a config error, never a
    /// consent at an unpinned page.
    #[test]
    fn check_endpoints_holds_the_recipe_to_the_pins() {
        let spec = spec_with_permissions(&["a"]);
        let good = app("Pinned", &["a"]);
        assert!(check_endpoints(&spec, &good).is_ok(), "matching pins pass");

        let mut wrong_token = good.clone();
        wrong_token.token_url = "https://pinned/other".into();
        let err = check_endpoints(&spec, &wrong_token).unwrap_err().to_string();
        assert!(err.contains("pinned"), "{err}");

        let mut wrong_auth = good.clone();
        wrong_auth.auth_url = Some("https://pinned/consent".into());
        assert!(check_endpoints(&spec, &wrong_auth).is_err());

        let mut unpinned = good;
        unpinned.auth_url = None;
        let err = check_endpoints(&spec, &unpinned).unwrap_err().to_string();
        assert!(err.contains("auth_url"), "{err}");
    }

    /// A declared renewal call runs with the app's credentials in
    /// scope, so on a registered app it may only address the pinned
    /// sign-in origin: same origin passes, anywhere else is refused.
    #[test]
    fn a_renewal_call_is_held_to_the_pinned_origin() {
        let renewal_spec = |url: &str| -> AccessSpec {
            let mut spec = spec_with_permissions(&["a"]);
            if let weft_core::access::spec::Acquisition::OAuth2 { refresh, .. } =
                &mut spec.acquisition
            {
                *refresh = Some(
                    serde_json::from_value(serde_json::json!({
                        "url": url,
                        "captures": [{ "name": "token", "path": "access_token" }]
                    }))
                    .unwrap(),
                );
            }
            spec
        };
        let app = app("Pinned", &["a"]);
        // The app pins https://a/t; a renewal on the same origin passes.
        check_endpoints(&renewal_spec("https://a/exchange?token={token}"), &app)
            .expect("same-origin renewal passes");
        let err = check_endpoints(&renewal_spec("https://evil.example/x?t={token}"), &app)
            .unwrap_err()
            .to_string();
        assert!(err.contains("renewal") && err.contains("pinned"), "{err}");
    }

    #[tokio::test]
    async fn the_file_supplies_app_lists_by_service_name() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(
            dir.path(),
            "apps.json",
            r#"{ "google": [
                   { "kind": "oauth_app", "label": "Google Drive", "covers": ["drive.file"],
                     "auth_url": "https://a/x", "token_url": "https://a/t",
                     "client_id": "cid", "client_secret": "sec" },
                   { "kind": "oauth_app", "label": "Google Calendar", "covers": ["calendar"],
                     "auth_url": "https://a/x", "token_url": "https://a/t",
                     "client_id": "cid2" }
                 ] }"#,
        );
        let provider = FileAppProvider::at(&path);
        let apps = provider.apps("google").await.unwrap();
        assert_eq!(apps.len(), 2);
        assert_eq!(apps[0].app.label, "Google Drive");
        assert_eq!(apps[0].covers, vec!["drive.file"]);
        assert!(apps[1].app.client_secret.is_none(), "a public client parses");
        assert!(provider.apps("github").await.unwrap().is_empty());
    }

    /// `api_key` entries live beside OAuth apps in the same file: the
    /// key reads back through `api_key()`, never through `apps()` (an
    /// OAuth-shaped consumer must not see it), at most one per service,
    /// and its label shares the per-service uniqueness rule.
    #[tokio::test]
    async fn api_key_entries_are_their_own_kind() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(
            dir.path(),
            "creds.json",
            r#"{ "openrouter": [{ "kind": "api_key", "label": "Runtime key", "key": "sk-x" }],
                 "slack": [{ "kind": "oauth_app", "label": "Slack", "covers": [],
                             "token_url": "https://a/t", "client_id": "c" }] }"#,
        );
        let provider = FileAppProvider::at(&path);
        let key = provider.api_key("openrouter").await.unwrap().expect("key declared");
        assert_eq!((key.label.as_str(), key.key.as_str()), ("Runtime key", "sk-x"));
        assert!(provider.apps("openrouter").await.unwrap().is_empty(), "not an oauth app");
        assert!(provider.api_key("slack").await.unwrap().is_none(), "an app is not a key");

        let two = FileAppProvider::at(write(
            dir.path(),
            "two.json",
            r#"{ "openrouter": [
                { "kind": "api_key", "label": "A", "key": "k1" },
                { "kind": "api_key", "label": "B", "key": "k2" } ] }"#,
        ));
        let err = format!("{:#}", two.api_key("openrouter").await.unwrap_err());
        assert!(err.contains("two api_key"), "{err}");

        let untagged = FileAppProvider::at(write(
            dir.path(),
            "untagged.json",
            r#"{ "openrouter": [{ "label": "A", "key": "k1" }] }"#,
        ));
        let err = format!("{:#}", untagged.api_key("openrouter").await.unwrap_err());
        assert!(err.contains("kind"), "an entry without a kind is refused: {err}");
    }

    #[tokio::test]
    async fn a_missing_or_empty_file_means_no_apps() {
        let dir = tempfile::tempdir().unwrap();
        let missing = FileAppProvider::at(dir.path().join("nope.json"));
        assert!(missing.apps("google").await.unwrap().is_empty());

        let empty = FileAppProvider::at(write(dir.path(), "empty.json", "  \n"));
        assert!(empty.apps("google").await.unwrap().is_empty());

        let none = FileAppProvider { path: None, cache: std::sync::Mutex::new(None) };
        assert!(none.apps("google").await.unwrap().is_empty());
    }

    /// Labels are trimmed at parse time (the stored form is what the
    /// editor displays and `app_by_label` is asked for), and
    /// uniqueness ignores case: " Slack " and "slack" are the same
    /// door twice, refused naming the file.
    #[tokio::test]
    async fn labels_are_trimmed_and_unique_ignoring_case() {
        let dir = tempfile::tempdir().unwrap();
        let provider = FileAppProvider::at(write(
            dir.path(),
            "trim.json",
            r#"{ "slack": [{ "kind": "oauth_app", "label": "  Slack Bot  ", "covers": [],
                             "token_url": "https://a/t", "client_id": "c" }] }"#,
        ));
        let apps = provider.apps("slack").await.unwrap();
        assert_eq!(apps[0].app.label, "Slack Bot", "stored trimmed");
        assert!(app_by_label(&apps, "Slack Bot").is_some());

        let dup = FileAppProvider::at(write(
            dir.path(),
            "casedup.json",
            r#"{ "slack": [
                { "kind": "oauth_app", "label": "Slack", "covers": [],
                  "token_url": "https://a/t", "client_id": "a" },
                { "kind": "oauth_app", "label": " sLaCk ", "covers": [],
                  "token_url": "https://a/t", "client_id": "b" } ] }"#,
        ));
        let err = format!("{:#}", dup.apps("slack").await.unwrap_err());
        assert!(err.contains("share the label"), "{err}");
    }

    /// The shipped example file parses, minus its `_` notes: the thing
    /// an operator copies must be valid input, not a broken template.
    #[tokio::test]
    async fn the_shipped_example_file_parses() {
        let example = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../access-apps.example.json");
        let raw = std::fs::read_to_string(&example).expect("example file reads");
        let services =
            FileAppProvider::parse_all(&raw, &example).expect("example file parses");
        assert!(services.contains_key("google"), "example declares google: {services:?}");
        for (service, entries) in &services {
            assert!(!service.starts_with('_'), "notes are not entries: {service}");
            for entry in entries {
                assert!(!entry.label().is_empty());
                if let SharedCredential::OauthApp(app) = entry {
                    assert!(
                        app.app.extra.keys().all(|k: &String| !k.starts_with('_')),
                        "per-app notes are dropped, not sent as credentials: {app:?}"
                    );
                }
            }
        }
    }

    /// Shape rules are LOUD: a non-list entry, a missing label /
    /// covers, and a duplicate label all fail naming the file.
    #[tokio::test]
    async fn malformed_shapes_fail_loud_naming_the_path() {
        let dir = tempfile::tempdir().unwrap();
        let cases = [
            ("notjson.json", "{ not json", "notjson.json"),
            (
                "object.json",
                r#"{ "google": { "kind": "oauth_app", "label": "X", "covers": [], "client_id": "c" } }"#,
                "must be a LIST",
            ),
            (
                "nolabel.json",
                r#"{ "google": [{ "kind": "oauth_app", "covers": [],
                                  "token_url": "https://a/t", "client_id": "c" }] }"#,
                "label",
            ),
            (
                "nocovers.json",
                r#"{ "google": [{ "kind": "oauth_app", "label": "X",
                                  "token_url": "https://a/t", "client_id": "c" }] }"#,
                "covers",
            ),
            (
                "notoken.json",
                r#"{ "google": [{ "kind": "oauth_app", "label": "X", "covers": [],
                                  "client_id": "c" }] }"#,
                "token_url",
            ),
            (
                "dup.json",
                r#"{ "google": [
                    { "kind": "oauth_app", "label": "X", "covers": [],
                      "token_url": "https://a/t", "client_id": "a" },
                    { "kind": "oauth_app", "label": "X", "covers": [],
                      "token_url": "https://a/t", "client_id": "b" } ] }"#,
                "share the label",
            ),
        ];
        for (name, body, needle) in cases {
            let provider = FileAppProvider::at(write(dir.path(), name, body));
            let err = provider.apps("google").await.unwrap_err();
            let msg = format!("{err:#}");
            assert!(msg.contains(needle), "{name}: {msg}");
        }
    }

    /// `_` keys are notes at both levels, never data; the bytes-keyed
    /// cache serves repeat lookups from one parse and reloads the
    /// moment the content changes, so edits land without a restart.
    #[tokio::test]
    async fn underscore_notes_and_live_reload() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(
            dir.path(),
            "apps.json",
            r#"{ "_readme": ["a note"],
                 "slack": [{ "_setup": "do this", "kind": "oauth_app", "label": "Slack",
                             "covers": [], "token_url": "https://a/t",
                             "client_id": "first" }] }"#,
        );
        let provider = FileAppProvider::at(&path);
        let apps = provider.apps("slack").await.unwrap();
        assert_eq!(apps[0].app.client_id, "first");
        assert!(apps[0].app.extra.is_empty(), "_setup never becomes a credential value");
        // A repeat lookup with an unchanged file serves the snapshot.
        assert_eq!(provider.apps("slack").await.unwrap()[0].app.client_id, "first");

        std::fs::write(
            &path,
            r#"{ "slack": [{ "kind": "oauth_app", "label": "Slack", "covers": [],
                             "token_url": "https://a/t", "client_id": "second" }] }"#,
        )
        .unwrap();
        // The cache keys on the file's BYTES, so the rewrite lands with
        // no timestamp games.
        assert_eq!(provider.apps("slack").await.unwrap()[0].app.client_id, "second");
    }

    /// A plain save truncates then refills; a lookup that lands in the
    /// gap must ride it out rather than silently answering "nothing
    /// configured" (the doors vanishing with no error). A file that
    /// genuinely STAYS empty is honestly empty after the window.
    #[tokio::test]
    async fn a_mid_rewrite_read_never_answers_empty() {
        let dir = tempfile::tempdir().unwrap();
        let content = r#"{ "slack": [{ "kind": "oauth_app", "label": "Slack", "covers": [],
                            "token_url": "https://a/t", "client_id": "c1" }] }"#;
        let path = write(dir.path(), "apps.json", content);
        let provider = FileAppProvider::at(&path);
        assert_eq!(provider.apps("slack").await.unwrap().len(), 1);

        // The mid-write gap: the file sits empty, the refill lands a
        // beat later from another thread, exactly as a save does.
        std::fs::write(&path, "").unwrap();
        let refill = {
            let path = path.clone();
            let content = content.to_string();
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(10));
                std::fs::write(&path, content).unwrap();
            })
        };
        assert_eq!(
            provider.apps("slack").await.unwrap().len(),
            1,
            "a lookup in the rewrite gap must see the refilled file, never an empty one"
        );
        refill.join().unwrap();

        // Genuinely emptied and left empty: honestly no doors.
        std::fs::write(&path, "").unwrap();
        assert_eq!(provider.apps("slack").await.unwrap().len(), 0);
    }
}

