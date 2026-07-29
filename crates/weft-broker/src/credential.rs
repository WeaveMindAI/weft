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

/// Resolves the runtime's provider keys. Policy lives in the impl.
/// `pool` is the broker's Postgres, passed in (same shape as
/// `EntitlementSource`) so a policy can consult runtime state without
/// holding a second pool.
#[async_trait::async_trait]
pub trait CredentialSource: Send + Sync {
    async fn resolve(&self, pool: &sqlx::PgPool, req: &KeyRequest) -> Result<KeyResolution>;

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

    async fn available(&self, service: &str) -> bool {
        matches!(
            self.provider.api_key(service).await,
            Ok(Some(entry)) if !entry.key.is_empty()
        )
    }
}
