//! The runtime-credential seam: how the broker answers a resolve on a
//! connection whose credential the runtime supplies (an `ours`-owned
//! row; the user picked the shared door instead of bringing their own).
//!
//! A source answers with what to authenticate with, and where calls on
//! it go (`None` = the service's own API). The default source reads the
//! shared-credentials file (the same `WEFT_ACCESS_APPS_FILE` the OAuth
//! apps live in; an `api_key` entry per service) and hands the key
//! itself: self-hosting means the configured key is the operator's own,
//! and the worker is the operator's own process, so there is nothing to
//! hide it from. A source that hands out time-bounded credentials
//! instead retires them in [`CredentialSource::close`] when the firing
//! releases the connection.

use anyhow::Result;

/// Who is asking for the runtime's credential, and for which service.
/// Everything a policy needs to decide: the tenant, the project, the
/// exact node, and the VERIFIED pod identity of the caller.
#[derive(Debug, Clone)]
pub struct KeyRequest {
    pub tenant: String,
    /// The opening execution, verified against the tenant by the handler.
    pub color: String,
    pub project_id: String,
    pub node_id: String,
    /// The opening firing's loop-frame coordinate: a source that books
    /// anything against the granted credential later (a measured cost)
    /// uses it to attribute the figure to the exact firing.
    pub frames: weft_core::LoopFrames,
    pub node_type: String,
    pub service: String,
    /// The declared auth steps the credential will ride (from the
    /// connection's spec). A source whose measuring side makes its own
    /// follow-up calls applies them to sign those calls in, since a
    /// meter never touches a credential itself.
    pub auth: Vec<weft_core::access::spec::AuthStep>,
    /// The calling pod, taken from the caller's verified token (not from the
    /// request body). A policy that resolves the running binary uses this;
    /// `None` means the token was not pod-bound.
    pub pod_name: Option<String>,
    /// How long the caller declared its provider work may take. A source
    /// that hands out time-bounded credentials bounds them by this (the
    /// crash backstop; the runtime normally releases first).
    pub window: std::time::Duration,
}

/// The source's answer.
#[derive(Debug)]
pub enum KeyResolution {
    /// Granted: authenticate with `credential`; send calls to
    /// `relay_url` when set (`None` = the service's own API).
    Access {
        credential: String,
        relay_url: Option<String>,
    },
    /// No credential is configured for this service. The caller turns
    /// this into "connect your own".
    NotConfigured,
    /// A credential is configured but this request is refused, with a
    /// user-facing reason (policy).
    Denied { reason: String },
}

/// Who is asking for a DESIGN-TIME credential: the editor signing a
/// `remote_select` lookup on an ours-owned connection. No execution
/// exists, so there is no firing to book against; a source with a
/// billing story decides from the tenant, the service, and the exact
/// URL the call will hit.
#[derive(Debug, Clone)]
pub struct DesignKeyRequest {
    pub tenant: String,
    pub service: String,
    /// The exact URL the lookup will call (the auth steps ride on top).
    pub url: String,
    /// The declared auth steps the credential will ride (from the
    /// connection's spec).
    pub auth: Vec<weft_core::access::spec::AuthStep>,
}

/// Resolves the runtime's provider keys. Policy lives in the impl.
/// `pool` is the broker's Postgres, passed in (same shape as
/// `EntitlementSource`) so a policy can consult runtime state without
/// holding a second pool.
#[async_trait::async_trait]
pub trait CredentialSource: Send + Sync {
    async fn resolve(&self, pool: &sqlx::PgPool, req: &KeyRequest) -> Result<KeyResolution>;

    /// Design-time twin of [`Self::resolve`]: sign an editor-side
    /// lookup on an ours-owned connection. Required, not defaulted:
    /// whether (and where) the runtime's key may travel outside an
    /// execution is a policy decision every source must make.
    async fn design_resolve(
        &self,
        pool: &sqlx::PgPool,
        req: &DesignKeyRequest,
    ) -> Result<KeyResolution>;

    /// Design-time probe: could this source answer for `service` at
    /// all? Drives whether the editor offers the shared door of a key
    /// service; the real gate stays [`Self::resolve`], asked per call.
    /// Default `false`: a source that does not say is one with nothing
    /// to offer.
    async fn available(&self, _service: &str) -> bool {
        false
    }

    /// The runtime gives a granted access back (the node that opened it
    /// finished): a source that hands out time-bounded credentials retires
    /// this one now instead of letting it live to its window. The default
    /// hands out the key itself, which is not retirable: nothing to do.
    async fn close(&self, _pool: &sqlx::PgPool, _credential: &str, _tenant: &str) -> Result<()> {
        Ok(())
    }
}

/// Default source: the shared-credentials file's `api_key` entries.
/// Self-hosting means the configured key is the operator's own, every
/// node of every tenant on this instance may use it (a single operator
/// has no policy to enforce), and calls go straight to the service.
/// Holds ONE file provider, so lookups share its mtime-keyed snapshot
/// (same freshness contract as the OAuth apps: an edit takes effect
/// without a restart).
pub struct FileCredentialSource {
    provider: crate::app_provider::FileAppProvider,
}

impl FileCredentialSource {
    /// Over the file named by `WEFT_ACCESS_APPS_FILE`.
    pub fn from_env() -> Self {
        Self { provider: crate::app_provider::FileAppProvider::from_env() }
    }
}

#[async_trait::async_trait]
impl CredentialSource for FileCredentialSource {
    async fn resolve(&self, _pool: &sqlx::PgPool, req: &KeyRequest) -> Result<KeyResolution> {
        match self.provider.api_key(&req.service).await? {
            Some(entry) if !entry.key.is_empty() => {
                Ok(KeyResolution::Access { credential: entry.key, relay_url: None })
            }
            _ => Ok(KeyResolution::NotConfigured),
        }
    }

    /// Same answer as [`Self::resolve`]: the configured key is the
    /// operator's own and the editor is the operator's own surface, so
    /// a design-time list call may sign with it directly.
    async fn design_resolve(
        &self,
        _pool: &sqlx::PgPool,
        req: &DesignKeyRequest,
    ) -> Result<KeyResolution> {
        match self.provider.api_key(&req.service).await? {
            Some(entry) if !entry.key.is_empty() => {
                Ok(KeyResolution::Access { credential: entry.key, relay_url: None })
            }
            _ => Ok(KeyResolution::NotConfigured),
        }
    }

    async fn available(&self, service: &str) -> bool {
        matches!(
            self.provider.api_key(service).await,
            Ok(Some(entry)) if !entry.key.is_empty()
        )
    }
}

/// Why a design-time signing did not produce a grant: a REFUSAL is
/// user-facing policy text the editor shows verbatim, an INTERNAL
/// failure is logged server-side and answered opaquely (its message
/// may quote configuration internals, e.g. a credentials-file parse
/// error, which must never travel to a caller).
pub enum DesignError {
    Refused(String),
    Internal(anyhow::Error),
}

/// A signed design-time lookup, ready to call: the final URL (rebuilt
/// onto the relay when the source answered with one) and the granted
/// credential, which the caller retires with [`CredentialSource::close`]
/// once the call finished, success or failure, so a source that mints
/// short-lived credentials never leaks one per lookup.
pub struct DesignGrant {
    pub url: String,
    pub credential: String,
}

/// Sign ONE editor-side lookup with the runtime's credential, end to
/// end: gate the URL on the meter allowlist (the SAME gate an
/// execution's direct lane runs, so an ours-owned credential is never
/// attached to a route the meter does not declare), ask the source for
/// the design credential, fill the resolved access's single auth
/// value, and answer the URL to call.
pub async fn design_sign(
    credentials: &dyn CredentialSource,
    pool: &sqlx::PgPool,
    tenant: &str,
    resolved: &mut weft_access_store::ResolvedAccess,
    url: String,
) -> std::result::Result<DesignGrant, DesignError> {
    let service = resolved.service.clone();
    // A lookup is always a GET; the gate runs BEFORE the source is
    // asked, so a refused URL never mints a credential at all.
    weft_providers::ours_route(&service, "GET", &url).map_err(DesignError::Refused)?;
    let name =
        single_value_name(&resolved.auth, &service).map_err(DesignError::Internal)?;
    let req = DesignKeyRequest {
        tenant: tenant.to_string(),
        service: service.clone(),
        url: url.clone(),
        auth: resolved.auth.clone(),
    };
    match credentials.design_resolve(pool, &req).await.map_err(DesignError::Internal)? {
        KeyResolution::Access { credential, relay_url } => {
            let url = match relay_url {
                Some(relay) => {
                    // ours_route above proved the meter exists.
                    let base = weft_providers::meter_for(&service)
                        .expect("ours_route ran")
                        .base_url();
                    weft_providers::relay_join(base, &relay, &url)
                        .map_err(DesignError::Refused)?
                }
                None => url,
            };
            resolved.values.insert(name, credential.clone());
            Ok(DesignGrant { url, credential })
        }
        KeyResolution::NotConfigured => Err(DesignError::Refused(format!(
            "no credential is configured for '{service}' here, so its resources cannot be \
             listed; connect your own credential, or type the id directly",
        ))),
        KeyResolution::Denied { reason } => Err(DesignError::Refused(reason)),
    }
}

/// The ONE stored value name an ours-owned connection authenticates
/// through: its auth steps' single placeholder. A shared door whose
/// steps interpolate zero or several names has no slot for the
/// runtime's one credential, which is a spec bug and fails loud.
pub fn single_value_name(
    auth: &[weft_core::access::spec::AuthStep],
    service: &str,
) -> Result<String> {
    let names = weft_core::access::spec::worker_value_names_of(auth)
        .map_err(|e| anyhow::anyhow!(e))?;
    let [name] = names.as_slice() else {
        anyhow::bail!(
            "an ours-owned '{service}' connection must authenticate through exactly one \
             stored value; its auth steps interpolate {}",
            names.len()
        );
    };
    Ok(name.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::access::spec::{AuthStep, Template};

    fn bearer(name: &str) -> Vec<AuthStep> {
        vec![AuthStep::Header {
            name: "Authorization".into(),
            value: Template::new(format!("Bearer {{{name}}}")),
        }]
    }

    #[test]
    fn single_value_name_reads_the_one_placeholder_or_refuses() {
        assert_eq!(single_value_name(&bearer("key"), "svc").unwrap(), "key");
        let none = single_value_name(&[], "svc").unwrap_err();
        assert!(none.to_string().contains("exactly one"), "{none}");
        let two = [bearer("a"), bearer("b")].concat();
        let err = single_value_name(&two, "svc").unwrap_err();
        assert!(err.to_string().contains("interpolate 2"), "{err}");
    }

    /// A dumb source: answers whatever it was built with, records the
    /// closes it receives.
    struct FakeSource {
        answer: fn() -> KeyResolution,
        closed: std::sync::Mutex<Vec<String>>,
    }
    #[async_trait::async_trait]
    impl CredentialSource for FakeSource {
        async fn resolve(&self, _p: &sqlx::PgPool, _r: &KeyRequest) -> Result<KeyResolution> {
            Ok((self.answer)())
        }
        async fn design_resolve(
            &self,
            _p: &sqlx::PgPool,
            _r: &DesignKeyRequest,
        ) -> Result<KeyResolution> {
            Ok((self.answer)())
        }
        async fn close(&self, _p: &sqlx::PgPool, credential: &str, _t: &str) -> Result<()> {
            self.closed.lock().unwrap().push(credential.to_string());
            Ok(())
        }
    }

    /// A pool that never dials: the fakes ignore it, and building one
    /// lazily keeps these layer-1 tests off the network entirely.
    fn lazy_pool() -> sqlx::PgPool {
        sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused@localhost/unused")
            .expect("lazy pool")
    }

    fn openrouter_resolved() -> weft_access_store::ResolvedAccess {
        weft_access_store::ResolvedAccess {
            values: std::collections::BTreeMap::new(),
            auth: bearer("key"),
            identity: None,
            service: "openrouter".into(),
            owner: weft_core::CredentialOwner::Ours,
            app_client_id: None,
        }
    }

    #[tokio::test]
    async fn design_sign_gates_fills_and_relays() {
        let pool = lazy_pool();
        let base = weft_providers::meter_for("openrouter").unwrap().base_url();
        let url = format!("{base}/models?q=gen");

        // Direct grant: value filled, URL unchanged, credential handed back.
        let direct = FakeSource {
            answer: || KeyResolution::Access { credential: "k-1".into(), relay_url: None },
            closed: Default::default(),
        };
        let mut resolved = openrouter_resolved();
        let grant = design_sign(&direct, &pool, "t1", &mut resolved, url.clone()).await;
        let grant = match grant {
            Ok(g) => g,
            Err(_) => panic!("direct grant refused"),
        };
        assert_eq!(grant.url, url);
        assert_eq!(grant.credential, "k-1");
        assert_eq!(resolved.values.get("key").map(String::as_str), Some("k-1"));

        // Relayed grant: URL rebuilt onto the relay.
        let relayed = FakeSource {
            answer: || KeyResolution::Access {
                credential: "standin-1".into(),
                relay_url: Some("https://relay.example/v1/provider/openrouter".into()),
            },
            closed: Default::default(),
        };
        let mut resolved = openrouter_resolved();
        let Ok(grant) = design_sign(&relayed, &pool, "t1", &mut resolved, url.clone()).await
        else {
            panic!("relayed grant refused")
        };
        assert_eq!(grant.url, "https://relay.example/v1/provider/openrouter/models?q=gen");

        // The meter gate runs BEFORE the source: a URL outside the
        // service's API never mints a credential.
        let mut resolved = openrouter_resolved();
        let refused = design_sign(
            &direct,
            &pool,
            "t1",
            &mut resolved,
            "https://attacker.example/collect".into(),
        )
        .await;
        match refused {
            Err(DesignError::Refused(m)) => assert!(m.contains("not under service"), "{m}"),
            _ => panic!("an outside URL must be refused"),
        }
        assert!(resolved.values.is_empty(), "a refusal fills nothing");

        // Policy answers surface verbatim; nothing is filled.
        let denied = FakeSource {
            answer: || KeyResolution::Denied { reason: "plan says no".into() },
            closed: Default::default(),
        };
        let mut resolved = openrouter_resolved();
        match design_sign(&denied, &pool, "t1", &mut resolved, url.clone()).await {
            Err(DesignError::Refused(m)) => assert_eq!(m, "plan says no"),
            _ => panic!("a denial must surface its reason"),
        }
        let missing = FakeSource {
            answer: || KeyResolution::NotConfigured,
            closed: Default::default(),
        };
        let mut resolved = openrouter_resolved();
        match design_sign(&missing, &pool, "t1", &mut resolved, url).await {
            Err(DesignError::Refused(m)) => assert!(m.contains("connect your own"), "{m}"),
            _ => panic!("not-configured must surface as a refusal"),
        }
    }
}
