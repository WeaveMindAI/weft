//! The deployment-key seam: how the broker answers a node that asked the
//! deployment to supply its configured provider key (the node's key input
//! was empty or the managed sentinel; a user-supplied key never reaches
//! here).
//!
//! A source answers with an ACCESS: what to authenticate with, and where
//! calls on it go (`None` = the provider's own API). The default source
//! reads the broker host's environment (`<PROVIDER>_API_KEY`) and hands the
//! key itself: self-hosting means the deployment's key is the operator's
//! own, and the worker is the operator's own process, so there is nothing
//! to hide it from. A source that hands out time-bounded credentials
//! instead retires them in [`CredentialSource::close`] when the runtime
//! gives the access back.

use anyhow::Result;

/// Who is asking for a deployment key, and for which provider. Everything a
/// policy needs to decide: the tenant, the project, the exact node, and the
/// VERIFIED pod identity of the caller.
#[derive(Debug, Clone)]
pub struct KeyRequest {
    pub tenant: String,
    /// The opening execution, verified against the tenant by the handler.
    pub color: String,
    pub project_id: String,
    pub node_id: String,
    /// The opening firing's loop-frame coordinate: a source that books
    /// anything against the granted access later (a measured cost) uses it
    /// to attribute the figure to the exact firing.
    pub frames: weft_core::LoopFrames,
    pub node_type: String,
    pub provider: String,
    /// The calling pod, taken from the caller's verified token (not from the
    /// request body). A policy that resolves the running binary uses this;
    /// `None` means the token was not pod-bound.
    pub pod_name: Option<String>,
    /// How long the caller declared its provider work may take. A source
    /// that hands out time-bounded credentials bounds them by this (the
    /// crash backstop; the runtime normally closes the access first).
    pub window: std::time::Duration,
}

/// The source's answer.
#[derive(Debug)]
pub enum KeyResolution {
    /// Access granted: authenticate with `credential`; send calls to
    /// `relay_url` when set (`None` = the provider's own API).
    Access {
        credential: String,
        relay_url: Option<String>,
    },
    /// The deployment has no key configured for this provider. The caller
    /// turns this into "set your own key for `provider`".
    NotConfigured,
    /// The deployment has a key but refuses THIS request, with a
    /// user-facing reason (policy).
    Denied { reason: String },
}

/// Resolves the deployment's provider keys. Policy lives in the impl.
/// `pool` is the broker's Postgres, passed in (same shape as
/// `EntitlementSource`) so a policy can consult runtime state without
/// holding a second pool.
#[async_trait::async_trait]
pub trait CredentialSource: Send + Sync {
    async fn resolve(&self, pool: &sqlx::PgPool, req: &KeyRequest) -> Result<KeyResolution>;

    /// The runtime gives a granted access back (the node that opened it
    /// finished): a source that hands out time-bounded credentials retires
    /// this one now instead of letting it live to its window. The default
    /// hands out the key itself, which is not retirable: nothing to do.
    async fn close(&self, _pool: &sqlx::PgPool, _credential: &str, _tenant: &str) -> Result<()> {
        Ok(())
    }
}

/// The env name a provider's key is read from: `<PROVIDER>_API_KEY`,
/// uppercased (e.g. `openrouter` -> `OPENROUTER_API_KEY`). A provider name
/// is validated at declaration to `[a-z0-9_]+`
/// (`weft_core::node::is_valid_provider_name`), so this uppercase mapping is
/// INJECTIVE: two distinct names can never collide onto one env var, and the
/// name is the key's identity. Pure so the mapping is testable and
/// documented in one place.
pub fn provider_env_var(provider: &str) -> String {
    let mut name: String = provider.to_ascii_uppercase();
    name.push_str("_API_KEY");
    name
}

/// Default source: the broker host's environment. Self-hosting means the
/// deployment's keys are the host's env; every node of every tenant on this
/// deployment may use them (single-operator deployments have no policy to
/// enforce), and calls go straight to the provider.
pub struct EnvCredentialSource;

#[async_trait::async_trait]
impl CredentialSource for EnvCredentialSource {
    async fn resolve(&self, _pool: &sqlx::PgPool, req: &KeyRequest) -> Result<KeyResolution> {
        match std::env::var(provider_env_var(&req.provider)) {
            Ok(key) if !key.is_empty() => {
                Ok(KeyResolution::Access { credential: key, relay_url: None })
            }
            _ => Ok(KeyResolution::NotConfigured),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_env_var_mapping() {
        assert_eq!(provider_env_var("openrouter"), "OPENROUTER_API_KEY");
        assert_eq!(provider_env_var("elevenlabs"), "ELEVENLABS_API_KEY");
        // Names are validated to [a-z0-9_]+, so the uppercase mapping is
        // injective: `some_provider` and a would-be `someprovider` never
        // collide, and the name IS the key's identity.
        assert_eq!(provider_env_var("some_provider"), "SOME_PROVIDER_API_KEY");
    }
}
