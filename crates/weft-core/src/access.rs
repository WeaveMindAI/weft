//! Provider access + cost provisioning: the node-facing surface for calling
//! paid third-party services.
//!
//! A node that talks to a paid provider never handles raw key plumbing.
//! It asks the context (`ctx.provider_access`) for access to the provider,
//! provisions an upper-bound cost before the call, makes the call, and
//! settles the actual cost after. The fixed ordering:
//!
//! 1. `ctx.provider_access(provider, user_key)` opens the access.
//! 2. Build the exact request about to be made.
//! 3. `ctx.provision_cost(&access, upper_bound)` declares the worst-case cost
//!    of THAT request; the deployment may refuse (usage limit reached).
//! 4. Make the call on the access.
//! 5. `ctx.settle_cost(hold, actual)` settles to what it really cost, on
//!    EVERY path (success, failure, partial): a failed call may still
//!    have cost money. Settling IS the recording: the amount lands on
//!    the execution's durable cost trail, and a metered hold (when the
//!    provision took one) is released down to the actual.
//! 6. `ctx.close_access(access)` gives the access back.
//!
//! Two origins for an access:
//! - the user supplied their OWN key (a real key string on the node's key
//!   input): it is their provider account; provision is a no-op and the
//!   settle only records the cost for the user's own tracking.
//! - the DEPLOYMENT grants the access (the input is empty or the
//!   `__PLATFORM__` sentinel): the runtime this project runs on grants access
//!   on its configured key for the provider, and may meter/limit its usage.

use serde::{Deserialize, Serialize};

/// The key-input sentinel meaning "the deployment running this project grants
/// access on its configured key for this provider". The editor writes it when
/// the user picks the managed option instead of pasting their own key. An
/// EMPTY/absent key input means the same thing (nothing user-supplied to
/// use).
// SYNC: PLATFORM_KEY_SENTINEL <-> weavemind/website editor key-input widget (the "Platform" option value)
pub const PLATFORM_KEY_SENTINEL: &str = "__PLATFORM__";

/// A deployment access carries a stand-in, and it starts with this. The real
/// key never enters a worker: the broker hands out a stand-in, and swaps it
/// for the key when the request comes back through the broker's provider
/// proxy (see the broker's `provider_proxy` module, which scans for this
/// prefix).
pub const STANDIN_PREFIX: &str = "weftstandin-";

/// The broker's proxy for a provider: a deployment access sends its requests
/// here instead of to the provider's own API, and the broker swaps the
/// stand-in for the real key on the way out. `broker_url` is the broker as
/// the caller reaches it.
///
/// One definition, used by both sides: the worker builds the address, the
/// broker serves it. The request that goes there is the provider call as the
/// caller built it, with the stand-in where the key goes and no other
/// credential.
pub fn provider_proxy_url(broker_url: &str, provider: &str) -> String {
    format!("{}/v1/provider/{provider}", broker_url.trim_end_matches('/'))
}

/// Where an access came from; decides whether provision/settle enforce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AccessOrigin {
    /// The user's own key, their own provider account. Nothing to enforce.
    UserProvided,
    /// The deployment's configured key. Usage of it may be metered and
    /// refused by the deployment.
    Deployment,
}

/// Access to a paid provider: what to authenticate with, and the address to
/// send it to.
///
/// The two go together, which is what keeps a deployment's keys safe. Access
/// on the user's OWN key carries the real key and the provider's own address.
/// A DEPLOYMENT's key never enters this process: the credential is a stand-in
/// for it, and the address is the broker's proxy for that provider, where
/// the stand-in is swapped for the real key on the way out. So a caller just
/// uses both, and nothing else has to know which kind it holds.
///
/// `Debug` redacts the credential so a key can never leak through an error
/// string or a log line.
#[derive(Clone)]
pub struct ProviderAccess {
    provider: String,
    credential: String,
    base_url: Option<String>,
    origin: AccessOrigin,
    /// How long the paid call this access is for may run: a deployment
    /// access's stand-in is alive exactly that long (and is retired earlier,
    /// at close). Declared once, by the node, when it opens the access.
    window: std::time::Duration,
}

impl ProviderAccess {
    /// Access on the user's own key, sent straight to the provider (no
    /// address of its own: the provider's default stands).
    pub fn own(
        provider: impl Into<String>,
        key: impl Into<String>,
        window: std::time::Duration,
    ) -> Self {
        Self {
            provider: provider.into(),
            credential: key.into(),
            base_url: None,
            origin: AccessOrigin::UserProvided,
            window,
        }
    }

    /// Access on the deployment's key: `standin` stands in for the key, and
    /// `base_url` is the broker proxy that swaps it for the real key.
    pub fn deployment(
        provider: impl Into<String>,
        standin: impl Into<String>,
        base_url: impl Into<String>,
        window: std::time::Duration,
    ) -> Self {
        Self {
            provider: provider.into(),
            credential: standin.into(),
            base_url: Some(base_url.into()),
            origin: AccessOrigin::Deployment,
            window,
        }
    }

    /// How long the paid call this access is for may run. The cost provision
    /// takes its deadline from here, so the access's life and the provision's
    /// window are the same declaration, made once.
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

    /// Where requests on this access must go, given the provider's own API
    /// address: the provider itself when the access is on the user's own key,
    /// the broker proxy (which holds the real key) when it is on the
    /// deployment's. Callers pass the address they would otherwise have used,
    /// and get the one to actually use.
    pub fn base_url(&self, provider_api: &str) -> String {
        self.base_url.clone().unwrap_or_else(|| provider_api.to_string())
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

/// What a node declares before a paid call: the cost and how well it
/// knows it.
///
/// `exact: false` (an [`estimate`](Self::estimate)) is the metered-output
/// family (an LLM call): `amount_usd` is a deliberately-high ceiling and
/// the settle brings it down to the actual. `exact: true` (an
/// [`exact`](Self::exact) provision) is the fixed-price family (one
/// search = one credit): the amount IS the price, so a provision that
/// was never settled (the runtime died between the call and the settle)
/// can be resolved at the provisioned amount itself.
///
/// How long the paid action may run comes from the [`ProviderAccess`] the
/// call is made on ([`ProviderAccess::window`]): the node declares it once,
/// when it opens the access, and it bounds both the access's life and how
/// long an unsettled provision waits before it is treated as abandoned.
#[derive(Debug, Clone)]
pub struct CostProvision {
    pub model: Option<String>,
    pub amount_usd: f64,
    pub exact: bool,
}

impl CostProvision {
    /// A ceiling estimate (metered-output pricing): settle brings it down.
    pub fn estimate(amount_usd: f64) -> Self {
        Self { model: None, amount_usd, exact: false }
    }

    /// An exact price (fixed-per-call pricing): the amount IS the cost.
    pub fn exact(amount_usd: f64) -> Self {
        Self { exact: true, ..Self::estimate(amount_usd) }
    }

    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }
}

/// A live cost provision, returned by `ctx.provision_cost` and consumed by
/// `ctx.settle_cost`. Consuming (not `Clone`) so one provision gets
/// exactly one settle. Remembers who it was provisioned for (service, model),
/// so settling needs only the actual amount.
///
#[must_use = "a provisioned cost must be settled (ctx.settle_cost), on failure paths too"]
#[derive(Debug)]
pub struct CostHold {
    /// The deployment's hold id when the access is on the deployment's key
    /// and the deployment meters usage; `None` for user keys / unmetered
    /// deployments.
    pub(crate) hold_id: Option<String>,
    /// The billed service (the provisioning access's provider).
    pub(crate) service: String,
    /// The model the provisioned call targets, when there is one.
    pub(crate) model: Option<String>,
}

impl CostHold {
    /// Only `ExecutionContext::provision_cost` mints a hold, so one provision
    /// yields exactly one hold (and, with `settle_cost` consuming it, one
    /// settle): node code cannot fabricate one. Hence `pub(crate)`.
    pub(crate) fn new(hold_id: Option<String>, service: String, model: Option<String>) -> Self {
        Self { hold_id, service, model }
    }

    pub fn hold_id(&self) -> Option<&str> {
        self.hold_id.as_deref()
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
    }

    /// An access answers where its requests go, given the provider's own API:
    /// access on the user's own key goes to the provider itself, access on
    /// the deployment's key goes to the broker proxy that holds the real key.
    /// One unconditional question, so no caller branches on the origin.
    #[test]
    fn an_access_answers_where_its_requests_go() {
        let provider_api = "https://openrouter.ai/api/v1";

        let own = ProviderAccess::own("openrouter", "sk-users-own", CALL_WINDOW);
        assert_eq!(own.base_url(provider_api), provider_api, "a user's key goes to the provider");
        assert_eq!(own.credential(), "sk-users-own");

        let proxy = provider_proxy_url("http://broker:9090", "openrouter");
        let deployment =
            ProviderAccess::deployment("openrouter", "weftstandin-abc", &proxy, CALL_WINDOW);
        assert_eq!(
            deployment.base_url(provider_api),
            "http://broker:9090/v1/provider/openrouter",
            "deployment access goes to the broker proxy, never the provider"
        );
        assert!(
            deployment.credential().starts_with(STANDIN_PREFIX),
            "deployment access carries a stand-in, never the key"
        );
    }
}
