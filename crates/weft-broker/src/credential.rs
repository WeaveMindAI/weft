//! The deployment-key seam: how the broker resolves ITS configured provider
//! keys for nodes that asked the deployment to supply one (the node's key
//! input was empty or the managed sentinel; a user-supplied key never
//! reaches here).
//!
//! The default source reads the broker host's environment
//! (`<PROVIDER>_API_KEY`), which is exactly what a self-hosted deployment
//! means by "my configured key". A deployment with per-node policy (which
//! node may use which key) supplies its own source.

use anyhow::Result;

/// Who is asking for a deployment key, and for which provider. Everything a
/// policy needs to decide: the tenant, the project, the exact node, and the
/// VERIFIED pod identity of the caller.
#[derive(Debug, Clone)]
pub struct KeyRequest {
    pub tenant: String,
    pub project_id: String,
    pub node_id: String,
    pub node_type: String,
    pub provider: String,
    /// The calling pod, taken from the caller's verified token (not from the
    /// request body). A policy that resolves the running binary uses this;
    /// `None` means the token was not pod-bound.
    pub pod_name: Option<String>,
}

/// The source's answer.
#[derive(Debug)]
pub enum KeyResolution {
    /// The deployment's key for this provider, cleared for this node.
    Key(String),
    /// The deployment has no key configured for this provider. The caller
    /// turns this into "set your own key for `provider`".
    NotConfigured,
    /// The deployment has a key but refuses THIS node's use of it, with a
    /// user-facing reason (policy; e.g. the node is not cleared to handle
    /// the deployment's key).
    Denied { reason: String },
}

/// Resolves the deployment's provider keys. One method; policy lives in the
/// impl. `pool` is the broker's Postgres, passed in (same shape as
/// `EntitlementSource`) so a policy can consult runtime state (e.g. which
/// binary the calling pod runs) without holding a second pool.
#[async_trait::async_trait]
pub trait CredentialSource: Send + Sync {
    async fn resolve(&self, pool: &sqlx::PgPool, req: &KeyRequest) -> Result<KeyResolution>;
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
/// enforce).
pub struct EnvCredentialSource;

#[async_trait::async_trait]
impl CredentialSource for EnvCredentialSource {
    async fn resolve(&self, _pool: &sqlx::PgPool, req: &KeyRequest) -> Result<KeyResolution> {
        match std::env::var(provider_env_var(&req.provider)) {
            Ok(key) if !key.is_empty() => Ok(KeyResolution::Key(key)),
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
