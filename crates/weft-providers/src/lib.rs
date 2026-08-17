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
    /// A long-lived two-way channel (a WebSocket) whose cost accrues from
    /// the frames that travel it, with no total knowable up front. Measured
    /// by a [`SessionObservation`] fed every frame both directions; admitted
    /// on a prepaid balance in slices ([`ProviderMeter::session_slice_usd`])
    /// topped up as cost accrues, never by one pre-call ceiling.
    BillableSession,
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
    /// The response status line. What lets a resolve on a fixed-price
    /// route tell a refused call (bill nothing) from an accepted one.
    pub status: u16,
    pub data: Value,
}

/// The meter's own signed side-query lane: an ALREADY-SIGNED-IN HTTP
/// client (the caller applies the same auth the original call rode) and
/// the provider base to ask. A meter never touches a credential, which
/// is exactly what makes the same meter work on a pasted key and on a
/// sign-in. Two consumers: a resolve follow-up when the provider only
/// reports cost out-of-band (e.g. OpenRouter's `/generation?id=...`),
/// and a ceiling's rate-catalog lookup when the provider publishes its
/// prices behind its own authenticated API (e.g. fal's pricing catalog).
/// Either way the query is the METER's call, on a provider route that
/// bills nothing; it may address any of the provider's own origins, but
/// never anything a caller could influence.
pub struct FollowUp<'a> {
    pub http: &'a reqwest_middleware::ClientWithMiddleware,
    pub base_url: &'a str,
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

/// Per-session frame tap, the [`CallObservation`] twin for a
/// [`RouteClass::BillableSession`] route. The meter mints one per session;
/// the caller feeds it every frame's payload in BOTH directions as it
/// passes (a tap, never a buffer) and ends it when the socket closes.
/// `accrued_usd` may be read at any moment between frames; a prepaid
/// admission uses it to decide when to reserve the next slice.
pub trait SessionObservation: Send {
    /// One frame from the caller to the provider (e.g. an audio chunk).
    fn on_frame_to_provider(&mut self, payload: &[u8]);
    /// One frame from the provider to the caller (e.g. a transcript).
    fn on_frame_to_caller(&mut self, payload: &[u8]);
    /// Dollars accrued so far. Monotone; exact for what has passed.
    fn accrued_usd(&self) -> f64;
    /// The session ended (cleanly or cut). The figure is final: a session
    /// is measured from its own frames, so unlike a one-shot call there is
    /// nothing out-of-band left to ask.
    fn end(self: Box<Self>, interrupted: bool) -> MeasuredCost;
}

/// One provider's meter: the reviewed, trusted answer to "what did this
/// call really cost". The same impl runs wherever the measuring happens;
/// where it runs decides only whether its number is authoritative.
#[async_trait::async_trait]
pub trait ProviderMeter: Send + Sync {
    /// The SERVICE this meter measures: the same name the service's
    /// access node declares. The registry is keyed by it, and the mere
    /// existence of a registered meter for a service is the whole
    /// declaration that its calls are measured; no flag anywhere else.
    fn service(&self) -> &'static str;

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

    /// The worst-case price this provider could charge for the Billable call
    /// described by `body`: computed BEFORE the call goes out, from the
    /// request bytes ALONE (the model, the output bound, the provider's own
    /// rate card) and nothing a caller could hand over separately. Errors
    /// when the request cannot be priced (unknown model, no output bound);
    /// never guesses. It is the pre-call twin of [`Self::resolve`] (which
    /// prices the call after the fact from the response): the same provider
    /// cost math asked forward instead of backward.
    ///
    /// OPTIONAL to implement; the default refuses. A provider is fully usable
    /// on the user's OWN key without it (a caller paying from their own
    /// provider account bounds nothing up front). Implementing it correctly
    /// is the bar to have the provider promoted to run on the platform keys
    /// in app.weavemind.ai: a prepaid balance can only admit a call whose
    /// worst case it can bound first, so a provider that will not price ahead
    /// of time cannot spend the platform's money. Until it is promoted, keep
    /// the default; the refusal is what keeps an unbounded call off a
    /// platform key. (A future weft feature may also consume it to PREDICT a
    /// run's cost up front: a second consumer, not a second reason to
    /// implement it.)
    async fn ceiling_usd(
        &self,
        _path: &str,
        _body: &[u8],
        _follow_up: FollowUp<'_>,
    ) -> anyhow::Result<f64> {
        anyhow::bail!(
            "service '{}' does not price calls ahead of time, so it cannot run on the \
             app.weavemind.ai platform keys; implement `ceiling_usd` to have it promoted, or \
             connect your own '{}' credential on the node to use it now",
            self.service(),
            self.service(),
        )
    }

    /// A fresh observer for one Billable call's response, on `path`
    /// (relative, as [`Self::classify`] receives it). `query` is the
    /// request URL's raw query string (no leading '?') and
    /// `request_body` the bytes actually sent (after [`Self::prepare`]),
    /// because some routes' cost is a function of the REQUEST (a TTS
    /// call prices its text's characters; an output format in the
    /// query decides bytes-per-second); routes priced purely off the
    /// response ignore both.
    fn observe(&self, path: &str, query: &str, request_body: &[u8]) -> Box<dyn CallObservation>;

    /// A fresh observer for one session on a `BillableSession` route.
    /// `query` is the raw query string of the session URL (no leading
    /// '?'), because a session's rate can depend on its negotiated
    /// parameters (an audio format decides bytes-per-second). REQUIRED
    /// for any meter that classifies a route `BillableSession`; the
    /// default refuses so a session route without session math is a loud
    /// meter bug, never an unmeasured spend.
    fn observe_session(
        &self,
        path: &str,
        _query: &str,
    ) -> anyhow::Result<Box<dyn SessionObservation>> {
        anyhow::bail!(
            "meter for '{}' classifies '{path}' as a session route but implements no \
             session observation; this is a meter bug",
            self.service(),
        )
    }

    /// The pre-carve amount a prepaid admission reserves per slice for a
    /// session on this route (e.g. one minute of audio worth). Reserved
    /// once at admission and again every time the accrued cost approaches
    /// the reserved total, until the balance refuses (the session is then
    /// cut and settled at the accrued figure). Like [`Self::ceiling_usd`],
    /// implementing it is the bar for the route to run on the platform
    /// keys in app.weavemind.ai; the default refuses, and a session on the
    /// caller's OWN credential needs only [`Self::observe_session`].
    fn session_slice_usd(&self, path: &str) -> anyhow::Result<f64> {
        anyhow::bail!(
            "service '{}' does not price sessions ahead of time on '{path}', so a session \
             cannot be paid for with the platform key; connect your own '{}' credential \
             on the node to use it now",
            self.service(),
            self.service(),
        )
    }

    /// The largest single frame a session admission should accept on this
    /// route, sized so one frame can never accrue more than one admission
    /// slice: one slice's worth of payload at the route's dearest per-byte
    /// rate, in its wire form (base64 expansion and envelope included).
    /// Implemented alongside [`Self::session_slice_usd`]; the default
    /// refuses so an admitted session without a frame bound is a loud
    /// meter bug, never an unbounded accrual.
    fn session_max_frame_bytes(&self, path: &str) -> anyhow::Result<usize> {
        anyhow::bail!(
            "meter for '{}' declares no per-frame size bound on '{path}', so a session \
             admission cannot cap what one frame may accrue; this is a meter bug",
            self.service(),
        )
    }

    /// Price one observed call on `path` (relative, as
    /// [`Self::classify`] received it). The route arrives as a FACT so
    /// a meter never infers it from the response's shape; a multi-route
    /// meter opens with a match on it and delegates per route. Pure when
    /// the provider reported the cost inline; when it only answers
    /// out-of-band, THE METER makes that follow-up query itself via
    /// `follow_up` (its own free route). Never errors: a cost that cannot
    /// be resolved is an honest `amount_usd: None`, recorded as unknown.
    async fn resolve(
        &self,
        path: &str,
        observed: ObservedCall,
        follow_up: FollowUp<'_>,
    ) -> MeasuredCost;
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

/// The meter for `service`, if one is registered. `Some` IS the
/// declaration that the service's calls are measured.
pub fn meter_for(service: &str) -> Option<&'static dyn ProviderMeter> {
    meters().find(|m| m.service() == service)
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

/// The gate a runtime-supplied ("ours") credential must pass before it
/// is attached to a request, answering the route relative to the
/// service's API base. Three refusals, each loud: the service must
/// have a meter (the meter IS the allowlist; without one no route can
/// be classified), the URL must be under the meter's base, and the
/// route must be one the meter declares (billable in any pricing, or
/// explicitly free). ONE function so every lane that signs with the
/// runtime's credential enforces the identical allowlist and the
/// lanes cannot drift.
pub fn ours_route(service: &str, method: &str, url: &str) -> Result<String, String> {
    let Some(meter) = meter_for(service) else {
        return Err(format!(
            "service '{service}' registers no meter, so a runtime-supplied credential \
             cannot travel on its calls; connect your own credential"
        ));
    };
    ours_route_on(meter, service, method, url)
}

/// [`ours_route`] against a CALLER-HELD meter: the seam test rigs
/// inject through (same pattern as the metering middleware's rig), and
/// what the middleware itself uses since it already resolved its meter
/// once at assembly.
pub fn ours_route_on(
    meter: &dyn ProviderMeter,
    service: &str,
    method: &str,
    url: &str,
) -> Result<String, String> {
    let Some(route) = route_under(meter.base_url(), url) else {
        return Err(format!(
            "this call ({url}) is not under service '{service}''s API ({}); a \
             runtime-supplied credential only travels on the service's own API",
            meter.base_url(),
        ));
    };
    if matches!(meter.classify(method, route), RouteClass::Unknown) {
        return Err(format!(
            "'{route}' is not a route the {service} meter declares, so the runtime's \
             credential will not be sent on it; connect your own credential to call it"
        ));
    }
    Ok(route.to_string())
}

/// Rebuild a service-API URL onto a relay: the route relative to
/// `base_url` joined onto the relay's own base, the original query
/// carried over. The ONE relay-join every lane uses (HTTP, socket,
/// design-time), so a relay with a trailing slash or its own path is
/// handled once, identically. The relay's scheme is kept; a socket
/// caller swaps it for the ws twin on the parsed result. A relay
/// carrying a query or fragment cannot host a joined route and is
/// refused rather than silently mangled.
pub fn relay_join(base_url: &str, relay: &str, url: &str) -> Result<String, String> {
    let Some(route) = route_under(base_url, url) else {
        return Err(format!(
            "this call ({url}) is not under the service's API ({base_url}), so it \
             cannot be relayed"
        ));
    };
    let relay: url::Url =
        relay.parse().map_err(|e| format!("relay URL {relay:?} does not parse: {e}"))?;
    if relay.query().is_some() || relay.fragment().is_some() {
        return Err(format!(
            "relay URL {relay} carries a query or fragment, so a route cannot be \
             joined onto it"
        ));
    }
    let query = url.split_once('?').map(|(_, q)| format!("?{q}")).unwrap_or_default();
    Ok(format!(
        "{}://{}{}/{route}{query}",
        relay.scheme(),
        relay.authority(),
        relay.path().trim_end_matches('/'),
    ))
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
        let names: Vec<&str> = meters().map(|m| m.service()).collect();
        assert!(!names.is_empty(), "no provider meters registered; did register_meter! run?");
        for name in &names {
            assert!(!name.is_empty(), "a meter registered an empty service name");
            assert_eq!(
                names.iter().filter(|n| *n == name).count(),
                1,
                "service '{name}' is registered more than once"
            );
        }
        // Each meter's base_url is a valid prefix its own classification can
        // sit under (route_under returns the empty route for the bare base).
        for m in meters() {
            assert_eq!(
                route_under(m.base_url(), m.base_url()),
                Some(""),
                "meter '{}' has a base_url route_under cannot parse: {}",
                m.service(),
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

    #[test]
    fn ours_route_refuses_each_gap_and_answers_the_route() {
        let base = meter_for("openrouter").unwrap().base_url();
        assert_eq!(
            ours_route("openrouter", "GET", &format!("{base}/models?q=x")).unwrap(),
            "models"
        );
        let no_meter = ours_route("no_such_service", "GET", "https://x.example/y").unwrap_err();
        assert!(no_meter.contains("registers no meter"), "{no_meter}");
        let outside =
            ours_route("openrouter", "GET", "https://attacker.example/collect").unwrap_err();
        assert!(outside.contains("not under service"), "{outside}");
        let unknown = ours_route("openrouter", "GET", &format!("{base}/keys")).unwrap_err();
        assert!(unknown.contains("not a route"), "{unknown}");
    }

    #[test]
    fn relay_join_handles_relay_paths_queries_and_slashes() {
        let base = "https://openrouter.ai/api/v1";
        let url = format!("{base}/models?q=gen");
        assert_eq!(
            relay_join(base, "https://relay.example/v1/provider/openrouter/", &url).unwrap(),
            "https://relay.example/v1/provider/openrouter/models?q=gen"
        );
        assert_eq!(
            relay_join(base, "https://relay.example", &format!("{base}/chat")).unwrap(),
            "https://relay.example/chat"
        );
        let queried =
            relay_join(base, "https://relay.example/p?tenant=x", &url).unwrap_err();
        assert!(queried.contains("query or fragment"), "{queried}");
        let outside = relay_join(base, "https://relay.example", "https://evil.example/x")
            .unwrap_err();
        assert!(outside.contains("cannot be relayed"), "{outside}");
    }
}
