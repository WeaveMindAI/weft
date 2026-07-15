//! Provider meters: per-provider code that computes the REAL cost of a paid
//! API call from the bytes of the request and the response.
//!
//! A meter is the single trusted artifact of the paid-call system. Node code
//! never states a cost; the runtime runs the provider's meter around the
//! call (classify the route, prepare the request so its cost becomes
//! reportable, tap the response, resolve the dollars) and every cost figure
//! in the system is a meter's output. All the rigor lives here so node
//! authors can be careless: a node cannot produce an incorrect bill no
//! matter what it does, because it is never asked.
//!
//! One meter impl per provider, in this crate's provider modules
//! (`openrouter`, ...). Adding a provider to this folder is what makes it a
//! supported, meterable provider; the meter's `base_url` is the single
//! authority for where the provider lives, and its route table is the single
//! authority for what can be measured on it.
//!
//! Route matching is a security boundary (an unknown route must never be
//! mistaken for a known one), so it is EXACT string matching on the raw
//! relative path:
//! traversal (`../`), encoded traversal (`%2e%2e`), userinfo (`@host`), and
//! backslash tricks simply fail to match and classify as [`RouteClass::Unknown`].

pub mod providers;
pub mod sse;

use serde_json::Value;

// Re-exported so `register_meter!` (used from provider modules) resolves
// `$crate::inventory` without each module depending on the crate directly.
#[doc(hidden)]
pub use inventory;

/// How a meter classifies one route of its provider's API.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RouteClass {
    /// This route spends money on the provider account. It gets prepared,
    /// observed, and resolved to a dollar figure. Carries how the route
    /// PRICES, which is also the fallback policy when a cost genuinely
    /// cannot be resolved (see [`Pricing`]).
    Billable(Pricing),
    /// A known route that costs nothing (a cost lookup, a model list, a
    /// health check). Costs are NEVER booked for it, which is what makes a
    /// node re-querying its own cost harmless (it gets its answer and is
    /// billed nothing) and the meter's own follow-up query safe to make.
    Free,
    /// Not a route this meter knows, so not one it can measure. Whether an
    /// unknown route is refused or passed through unmeasured is the
    /// caller's policy, not the meter's.
    Unknown,
}

/// How a billable route prices, and therefore what an UNRESOLVABLE cost
/// means:
///
/// - a FIXED-price call that provably went out cost its price; the price
///   stands without measurement, exact, not a guess.
/// - a METERED call whose figure cannot be resolved has no honest number
///   at all: it is recorded as unknown, never guessed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Pricing {
    /// The cost depends on the response (LLM tokens): only measurement
    /// answers it.
    Metered,
    /// One call = one known price, in USD.
    Fixed { usd: f64 },
}

/// What a per-call observation saw, handed to [`ProviderMeter::resolve`].
/// `data` is meter-owned (produced by the meter's own observer, consumed by
/// the same meter's resolve); callers treat it as opaque.
#[derive(Debug)]
pub struct ObservedCall {
    /// The response ended before the provider finished (the caller hung up
    /// or the connection died). The generation may still have cost money.
    pub interrupted: bool,
    pub data: Value,
}

/// Everything a meter needs to make its own follow-up query when the
/// provider only reports cost out-of-band (e.g. OpenRouter's
/// `/generation?id=...`): an HTTP client, the provider base to ask, and the
/// credential the original call was made with. The follow-up is the METER's
/// call, on a route the meter itself classifies as free.
pub struct FollowUp<'a> {
    pub http: &'a reqwest::Client,
    pub base_url: &'a str,
    pub credential: &'a str,
}

/// A meter's verdict on what one call cost.
///
/// `amount_usd: None` means the cost is genuinely unknown (e.g. the stream
/// was cut before the usage arrived and the provider has no ledger to ask).
/// An unknown is recorded AS unknown; it is never booked as $0.
#[derive(Debug, Clone)]
pub struct MeasuredCost {
    pub amount_usd: Option<f64>,
    /// The model the call was served with, when the provider reports one.
    pub model: Option<String>,
    /// Meter-specific detail for the cost trail (token counts, generation
    /// id, how the number was resolved).
    pub metadata: Value,
}

/// Per-call response tap. The meter mints one per Billable call; the caller
/// feeds it every response byte AS THE BYTES FLOW THROUGH (a tap, not a
/// buffer: the real consumer sees each chunk in real time) and ends it when
/// the stream ends or is cut.
pub trait CallObservation: Send {
    /// The response status line arrived. Called once, before any chunk.
    fn on_status(&mut self, status: u16);
    /// One chunk of response body bytes, in order.
    fn on_chunk(&mut self, bytes: &[u8]);
    /// The response ended. `interrupted` = it was cut before the provider
    /// finished (client hang-up, dead connection).
    fn end(self: Box<Self>, interrupted: bool) -> ObservedCall;
}

/// One provider's meter: the reviewed, trusted answer to "what did this
/// call really cost". The same impl runs wherever the measuring happens;
/// where it runs decides only whether its number is authoritative.
#[async_trait::async_trait]
pub trait ProviderMeter: Send + Sync {
    /// The provider's name: the key identity (`<NAME>_API_KEY`) and the
    /// string nodes pass to `ctx.provider_access`.
    fn provider(&self) -> &'static str;

    /// The provider's real API base URL (scheme + host + path prefix, no
    /// trailing '/'). The single authority for where this provider lives;
    /// no caller ever accepts a host from a request instead.
    fn base_url(&self) -> &'static str;

    /// Classify a route. `method` is the uppercase HTTP method; `path` is
    /// the request path RELATIVE to [`Self::base_url`], without a leading
    /// '/' and without the query string. Matching is exact (see the module
    /// docs); anything else is [`RouteClass::Unknown`].
    fn classify(&self, method: &str, path: &str) -> RouteClass;

    /// Rewrite a Billable call's outgoing body so its cost becomes
    /// reportable at all (e.g. the provider's usage-accounting opt-in).
    /// `Ok(None)` = send as-is. An unparseable body on a route that needs
    /// rewriting is a loud error: an unpreparable call would produce an
    /// unmeasurable spend.
    fn prepare(&self, path: &str, body: &[u8]) -> anyhow::Result<Option<Vec<u8>>>;

    /// A worst-case price for the Billable call described by `body`,
    /// computable BEFORE the call goes out and only from the request bytes
    /// (never from anything a caller could hand over separately). Errors
    /// when the call cannot be priced (unknown model, no output bound);
    /// never guesses.
    async fn ceiling_usd(
        &self,
        path: &str,
        body: &[u8],
        http: &reqwest::Client,
    ) -> anyhow::Result<f64>;

    /// A fresh observer for one Billable call's response.
    fn observe(&self) -> Box<dyn CallObservation>;

    /// Turn an observation into dollars. Pure when the provider reported
    /// the cost inline; when it only answers out-of-band, THE METER makes
    /// that follow-up query itself via `follow_up` (its own free route).
    /// Never errors: a cost that cannot be resolved is an honest
    /// `amount_usd: None`, recorded as unknown.
    async fn resolve(&self, observed: ObservedCall, follow_up: FollowUp<'_>) -> MeasuredCost;
}

/// One provider meter's self-registration. Each provider file submits one
/// (via [`register_meter!`]) so the registry needs no central list: adding a
/// provider is a file plus a `mod` line, nothing else. The registry IS the
/// supported-provider list, so a missing registration means a provider is
/// simply not measurable, which surfaces as a loud refusal wherever a
/// measured call is required, never as a silent wrong answer.
pub struct MeterEntry {
    pub meter: &'static dyn ProviderMeter,
}

inventory::collect!(MeterEntry);

/// Submit a provider's meter into the registry. Call it once, at the bottom
/// of the provider's module, with a `'static` meter value:
/// `weft_providers::register_meter!(OPENROUTER);`.
#[macro_export]
macro_rules! register_meter {
    ($meter:expr) => {
        $crate::inventory::submit! {
            $crate::MeterEntry { meter: &$meter }
        }
    };
}

/// Every meter this crate ships, collected from the per-provider
/// registrations at link time. No central array to keep in sync.
pub fn meters() -> impl Iterator<Item = &'static dyn ProviderMeter> {
    inventory::iter::<MeterEntry>.into_iter().map(|e| e.meter)
}

/// The meter for `provider`, if this crate ships one.
pub fn meter_for(provider: &str) -> Option<&'static dyn ProviderMeter> {
    meters().find(|m| m.provider() == provider)
}

/// The relative route path of `url` under `base_url`: `Some("chat/completions")`
/// for `https://host/api/v1/chat/completions` under `https://host/api/v1`,
/// `None` when the URL does not live under the base (different host, different
/// prefix). Comparison is raw-string, on the boundary of a path segment, so
/// `https://host/api/v1evil/...` is NOT under `https://host/api/v1`.
pub fn route_under<'a>(base_url: &str, url: &'a str) -> Option<&'a str> {
    let base = base_url.trim_end_matches('/');
    let rest = url.strip_prefix(base)?;
    // The base must end exactly at a path-segment boundary: the next char is
    // '/' (a deeper path), '?' (query on the base itself), or nothing.
    match rest.as_bytes().first() {
        None => Some(""),
        Some(b'/') => {
            let rest = &rest[1..];
            Some(rest.split(['?', '#']).next().unwrap_or(""))
        }
        Some(b'?') | Some(b'#') => Some(""),
        Some(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_registry_answers_by_provider_name_only() {
        assert!(meter_for("openrouter").is_some());
        assert!(meter_for("openrouterX").is_none());
        assert!(meter_for("OPENROUTER").is_none(), "names are exact, not case-folded");
        assert!(meter_for("").is_none());
    }

    /// The self-registration is sound: at least one meter is collected, and
    /// every one has a unique, non-empty provider name and a base URL its
    /// own routes live under. A duplicate name (two `register_meter!` for one
    /// provider) or an empty name would make `meter_for` ambiguous or dead;
    /// catch it here rather than at a call site in production.
    #[test]
    fn the_registry_is_well_formed() {
        let names: Vec<&str> = meters().map(|m| m.provider()).collect();
        assert!(!names.is_empty(), "no provider meters registered; did register_meter! run?");
        for name in &names {
            assert!(!name.is_empty(), "a meter registered an empty provider name");
            assert_eq!(
                names.iter().filter(|n| *n == name).count(),
                1,
                "provider '{name}' is registered more than once"
            );
        }
        // Each meter's base_url is a valid prefix its own classification can
        // sit under (route_under returns the empty route for the bare base).
        for m in meters() {
            assert_eq!(
                route_under(m.base_url(), m.base_url()),
                Some(""),
                "meter '{}' has a base_url route_under cannot parse: {}",
                m.provider(),
                m.base_url()
            );
        }
    }

    #[test]
    fn route_under_requires_a_segment_boundary() {
        let base = "https://openrouter.ai/api/v1";
        assert_eq!(
            route_under(base, "https://openrouter.ai/api/v1/chat/completions"),
            Some("chat/completions")
        );
        assert_eq!(
            route_under(base, "https://openrouter.ai/api/v1/generation?id=gen-1"),
            Some("generation")
        );
        assert_eq!(route_under(base, "https://openrouter.ai/api/v1"), Some(""));
        assert_eq!(route_under(base, "https://openrouter.ai/api/v1/"), Some(""));
        // Not under the base: different host, different prefix, or a prefix
        // that merely STARTS with the base string.
        assert_eq!(route_under(base, "https://evil.example/api/v1/chat"), None);
        assert_eq!(route_under(base, "https://openrouter.ai/api/v2/chat"), None);
        assert_eq!(route_under(base, "https://openrouter.ai/api/v1evil/chat"), None);
    }
}
