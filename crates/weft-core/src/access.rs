//! Provider access: the node-facing surface for calling paid third-party
//! services.
//!
//! A node that talks to a paid provider does two things, and only two:
//!
//! 1. `ctx.provider_access(provider, user_key)` opens the access: what to
//!    authenticate with.
//! 2. `ctx.metered_client(&access)` hands back an ordinary HTTP client to
//!    make the calls with.
//!
//! Everything else (where the call is routed, what it cost, recording that
//! cost) is the runtime's job, done behind the client. A node never states
//! a cost and has no way to: every cost figure in the system is produced by
//! a provider meter (`weft-providers`), run by the runtime around the call.
//!
//! Two origins for an access:
//! - the user supplied their OWN key (a real key string on the node's key
//!   input): it is their provider account, used as-is.
//! - the DEPLOYMENT grants the access (the input is empty or the
//!   `__PLATFORM__` sentinel): the runtime this project runs on answers
//!   with a credential for its configured key, and may bound or refuse its
//!   use. The deployment may also answer with a RELAY address: calls on its
//!   credential are then sent there (the relay holds the actual provider
//!   relationship) instead of to the provider directly; the metered client
//!   does that routing, the node never sees it.

use serde::{Deserialize, Serialize};

/// The key-input sentinel meaning "the deployment running this project grants
/// access on its configured key for this provider". The editor writes it when
/// the user picks the managed option instead of pasting their own key. An
/// EMPTY/absent key input means the same thing (nothing user-supplied to
/// use).
// SYNC: PLATFORM_KEY_SENTINEL <-> weavemind/website editor key-input widget (the "Platform" option value)
pub const PLATFORM_KEY_SENTINEL: &str = "__PLATFORM__";

/// Where an access came from; decides whose money a call on it spends.
// SYNC: AccessOrigin <-> packages/weft-graph/src/protocol.ts CostOrigin
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AccessOrigin {
    /// The user's own key, their own provider account.
    UserProvided,
    /// A deployment-granted credential for the deployment's configured key.
    Deployment,
}

/// Access to a paid provider: what to authenticate with, and (for a
/// deployment-granted access) where the deployment routes calls on it.
///
/// The credential is used exactly like a key: spliced into the auth header
/// by whatever library makes the call. Whether it IS the key or a
/// deployment-managed credential for one is not the node's concern; the
/// node uses `credential()` and the metered client does the rest.
///
/// `Debug` redacts the credential so a key can never leak through an error
/// string or a log line.
#[derive(Clone)]
pub struct ProviderAccess {
    provider: String,
    credential: String,
    /// A deployment-granted access may carry a relay address: calls on its
    /// credential are sent THERE (the relay holds the provider
    /// relationship) instead of to the provider's own API. `None` = calls
    /// go straight to the provider. Consumed by the metered client's
    /// routing; nothing node-facing reads it.
    relay_url: Option<String>,
    origin: AccessOrigin,
    /// How long the provider work on this access may run: a deployment
    /// credential is guaranteed usable exactly that long (the runtime may
    /// retire it after). Declared once, when the access is opened.
    window: std::time::Duration,
}

impl ProviderAccess {
    /// Access on the user's own key, sent straight to the provider.
    pub fn own(
        provider: impl Into<String>,
        key: impl Into<String>,
        window: std::time::Duration,
    ) -> Self {
        Self {
            provider: provider.into(),
            credential: key.into(),
            relay_url: None,
            origin: AccessOrigin::UserProvided,
            window,
        }
    }

    /// Access granted by the deployment on its configured key.
    /// `relay_url` is where calls on `credential` must be sent when the
    /// deployment relays them; `None` = straight to the provider.
    pub fn deployment(
        provider: impl Into<String>,
        credential: impl Into<String>,
        relay_url: Option<String>,
        window: std::time::Duration,
    ) -> Self {
        Self {
            provider: provider.into(),
            credential: credential.into(),
            relay_url,
            origin: AccessOrigin::Deployment,
            window,
        }
    }

    /// How long the provider work on this access may run (the credential's
    /// guaranteed-usable window).
    pub fn window(&self) -> std::time::Duration {
        self.window
    }

    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// What to authenticate with. Never put this in an output port, a log
    /// line, or an error message.
    pub fn credential(&self) -> &str {
        &self.credential
    }

    /// The deployment's relay for calls on this access, when it routes
    /// them. Read by the metered client; nothing node-facing needs it.
    pub fn relay_url(&self) -> Option<&str> {
        self.relay_url.as_deref()
    }

    pub fn origin(&self) -> AccessOrigin {
        self.origin
    }
}

impl std::fmt::Debug for ProviderAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProviderAccess")
            .field("provider", &self.provider)
            .field("credential", &"<redacted>")
            .field("origin", &self.origin)
            .finish()
    }
}

/// Classify a node's key-input value: `Some(key)` = the user's own key,
/// `None` = the deployment should grant access on its own. Pure; the context
/// method wraps it. Empty, whitespace-only, and the sentinel all mean
/// "deployment" (a blank field is never a real key, so it must not be sent to
/// the provider as one).
///
/// The value is TRIMMED once and the trimmed value is what is classified AND
/// returned: a key pasted with stray whitespace is the key the user meant, so
/// it goes to the provider clean (an untrimmed one just gets rejected by the
/// provider), and a sentinel pasted with stray whitespace still routes to the
/// deployment instead of being sent upstream as if it were a key.
pub fn user_key_of(raw: Option<&str>) -> Option<&str> {
    match raw.map(str::trim) {
        Some(k) if !k.is_empty() && k != PLATFORM_KEY_SENTINEL => Some(k),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// How long the paid call may run: the access's window.
    const CALL_WINDOW: std::time::Duration = std::time::Duration::from_secs(600);

    #[test]
    fn user_key_classification() {
        assert_eq!(user_key_of(Some("sk-abc")), Some("sk-abc"));
        assert_eq!(user_key_of(Some("")), None);
        assert_eq!(user_key_of(Some("   ")), None, "a blank field is not a real key");
        assert_eq!(user_key_of(Some(PLATFORM_KEY_SENTINEL)), None);
        assert_eq!(user_key_of(None), None);
        // Trimmed once, and the TRIMMED value is what is classified and
        // returned: a pasted key with stray whitespace reaches the provider
        // clean, and a pasted sentinel with stray whitespace still routes to
        // the deployment instead of being sent upstream as a key.
        assert_eq!(user_key_of(Some(" sk-abc ")), Some("sk-abc"));
        assert_eq!(
            user_key_of(Some("  __PLATFORM__  ")),
            None,
            "a padded sentinel is still the sentinel, never a key to send"
        );
    }

    #[test]
    fn debug_never_prints_the_credential() {
        let access = ProviderAccess::own("openrouter", "sk-very-secret", CALL_WINDOW);
        let rendered = format!("{access:?}");
        assert!(!rendered.contains("sk-very-secret"), "{rendered}");
        assert!(rendered.contains("<redacted>"), "{rendered}");

        let managed = ProviderAccess::deployment(
            "openrouter",
            "managed-credential",
            Some("http://relay.internal/v1/provider/openrouter".into()),
            CALL_WINDOW,
        );
        let rendered = format!("{managed:?}");
        assert!(!rendered.contains("managed-credential"), "{rendered}");
    }

    /// An access carries what to authenticate with and, for a
    /// deployment-granted one, where its calls are routed. The node only
    /// ever reads the credential; the routing is the metered client's.
    #[test]
    fn an_access_carries_credential_and_routing() {
        let own = ProviderAccess::own("openrouter", "sk-users-own", CALL_WINDOW);
        assert_eq!(own.credential(), "sk-users-own");
        assert_eq!(own.relay_url(), None, "a user's key goes to the provider");
        assert_eq!(own.origin(), AccessOrigin::UserProvided);

        let relayed = ProviderAccess::deployment(
            "openrouter",
            "managed-credential",
            Some("http://relay.internal/v1/provider/openrouter".into()),
            CALL_WINDOW,
        );
        assert_eq!(relayed.credential(), "managed-credential");
        assert_eq!(
            relayed.relay_url(),
            Some("http://relay.internal/v1/provider/openrouter"),
        );
        assert_eq!(relayed.origin(), AccessOrigin::Deployment);

        let direct = ProviderAccess::deployment("openrouter", "the-key", None, CALL_WINDOW);
        assert_eq!(direct.relay_url(), None, "an unrelayed grant goes straight to the provider");
    }
}
