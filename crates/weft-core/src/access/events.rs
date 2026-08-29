//! `EventsSpec`: how a service REPORTS one TOPIC of events, declared
//! as data on the same service recipe that declares how to call it.
//!
//! A service declares a MAP of topics (`AccessSpec.events`), because
//! one provider genuinely reports along independent topologies at
//! once (the same Google connection watches Drive files through
//! expiring channels AND receives mailbox pushes through a queue
//! relay). A subscription names its service + topic; everything else
//! about the topic is this recipe.
//!
//! Everything here is a fact about the service, identical for
//! everybody, which is why it lives beside `auth` and `test` on
//! [`crate::AccessSpec`] and rides the grant row's snapshot: the
//! machinery serving a subscription reads the recipe off the
//! connection, with no catalog lookup and no per-service code.
//!
//! Two transports, and a service may declare either, both, or
//! neither:
//!
//!   - **dial-out** ([`SocketRecipe`]): weft makes an authenticated
//!     call that answers a socket address, connects out, and holds
//!     the line. Needs no address of our own, so it works from a
//!     laptop; only serves an app whose installs all belong to one
//!     party (the socket is app-wide, not per-install).
//!   - **dial-in** ([`WebhookRecipe`]): the provider posts each event
//!     to an address we published. The only transport a shared app
//!     can use, and the only one most services offer at all.
//!
//! A subscription never names either. It names a connection and a
//! filter; the transport follows from what the service declares and
//! what the environment can serve.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::spec::{ConnectCall, MintedSocket};

/// A path prefix marking a field that lives in the HTTP HEADERS of an
/// incoming push rather than in its body. Several providers put the
/// whole event there (a Drive notification has NO body at all;
/// Shopify names the shop in a header), so a fields table that could
/// only read a body would be unable to express them.
pub const HEADER_PREFIX: &str = "header:";

/// How a service reports events. Attached to the service recipe, so
/// it travels with every connection made to that service.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventsSpec {
    /// The named facts of one event, each mapped to where it lives in
    /// the provider's payload (a dotted body path, or
    /// `header:X-Some-Header`). Subscriptions filter on these NAMES
    /// and trigger nodes fan them out, so neither ever writes a
    /// provider path and a service that renames a field breaks in one
    /// place.
    pub fields: BTreeMap<String, String>,
    /// Which provider account an event belongs to: the stored value
    /// that identifies the account on a CONNECTION, and where the
    /// same identifier sits on an incoming EVENT. The pair is what
    /// routes a shared app's events to the right connection.
    pub account: EventAccount,
    /// Present when the service offers the dial-out transport.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub socket: Option<SocketRecipe>,
    /// Present when the service pushes events to an address we
    /// publish.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhook: Option<WebhookRecipe>,
}

/// Which account an event concerns; see [`EventsSpec::account`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventAccount {
    /// The name of the stored value that identifies the account on a
    /// connection (Slack captures `team` at connect; a mailbox
    /// connection captures its address). Written onto the connection
    /// row's indexed account column so an inbound event finds it
    /// without digging through stored values.
    pub value: String,
    /// Where the same identifier sits on an incoming event (a body
    /// path, or a `header:` path).
    pub path: String,
}

/// The dial-out recipe: how to obtain a socket and what to say on it.
/// No `deny_unknown_fields`: serde cannot combine it with `flatten`,
/// so the shape rules live in [`EventsSpec::validate`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SocketRecipe {
    /// The mint call, the capture carrying the address, and the
    /// acknowledgements the gateway demands. The `connect` call is
    /// required here (an events socket has no static address);
    /// validation refuses a recipe without one.
    #[serde(flatten)]
    pub minted: MintedSocket,
    /// Where the event sits inside an inbound frame (Slack wraps
    /// events in an envelope under `payload.event`). Empty = the
    /// frame IS the event.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub event_path: String,
    /// True when this ONE socket carries frames for several installs'
    /// accounts (the app-scoped funnel). Each frame then names the
    /// account it concerns through the topic's account path (see
    /// [`EventsSpec::socket_frame_account`]), and an account-scoped
    /// subscription only hears frames proven to be its own account's.
    /// False = the socket is single-account and everything on it is
    /// the owner's.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub multi_account: bool,
    /// Keys the recipe declares that nothing above consumes. Serde's
    /// `flatten` on `minted` rules out `deny_unknown_fields`, so the
    /// unknown-key refusal lives in [`EventsSpec::validate`] instead:
    /// a typoed key must fail at load, never silently fall back to a
    /// default at runtime.
    #[serde(flatten, skip_serializing)]
    pub unknown: std::collections::BTreeMap<String, serde_json::Value>,
}

/// The dial-in recipe: how an incoming push is proven genuine, whose
/// connection it concerns, and (when the provider needs telling where
/// to send) how to ask for and keep the subscription.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebhookRecipe {
    /// The one-off exchange a provider runs to prove the address is
    /// ours before it starts sending. Absent = it just starts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handshake: Option<Handshake>,
    /// Where the event sits inside a push's (decoded) body. Empty =
    /// the body IS the event. Named fields read from here; the
    /// account path reads from the OUTER body (providers put the
    /// routing id on the envelope, not inside the event).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub event_path: String,
    /// Where a push carries the subscription id weft minted; required
    /// by (and only meaningful for) `route_by: subscription`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id_path: Option<String>,
    /// Where a push carries the token weft minted; required by (and
    /// only meaningful for) the token-echo verification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_path: Option<String>,
    /// How a push is proven to come from the provider. The SECRET
    /// material is never here: it belongs to the app that receives
    /// (operator configuration), while WHICH scheme to run is a fact
    /// about the service.
    pub verify: VerifyKind,
    /// How an incoming event names what it belongs to.
    #[serde(default)]
    pub route_by: RouteBy,
    /// Unwrap a payload that arrives encoded inside an envelope
    /// (a queue relay carries the real event base64'd in a field).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decode: Option<Decode>,
    /// Present when the provider must be TOLD to send (and, when the
    /// subscription expires, re-told before it lapses).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscribe: Option<SubscriptionCalls>,
}

/// The address-proving exchange, in the three shapes providers use.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Handshake {
    /// The first POST carries a challenge in its body; echo it back
    /// (Slack's `challenge`).
    EchoBodyField { path: String },
    /// The provider GETs the address with a token in the query
    /// string; answer it as plain text (Microsoft Graph's
    /// `validationToken`).
    EchoQueryParam { param: String },
    /// The body carries a URL to visit to confirm (AWS SNS's
    /// `SubscribeURL`).
    CallbackUrl { path: String },
}

/// The proof schemes weft implements, each a PROTOCOL parameterized
/// by data, never a provider name: which headers, what concatenation,
/// which issuer are all declared in the recipe. The closed part is
/// only the arithmetic (an HMAC, a constant-time compare, an OIDC
/// signature check), extended deliberately like the request-signing
/// kinds. The SECRET material is never here: it belongs to the app
/// that receives (operator configuration) or to the subscription weft
/// minted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum VerifyKind {
    /// An HMAC over a declared concatenation of request parts,
    /// compared against a signature header. The shape most signing
    /// providers use (Slack, GitHub, Shopify, Stripe, Twilio); which
    /// headers, what concatenation, which hash, and how the digest is
    /// written are all declared here.
    Hmac {
        /// The header carrying the signature to compare against.
        signature_header: String,
        /// The header carrying the unix timestamp the signature
        /// covers, for providers that sign one (what makes a replayed
        /// push refusable). Absent = no timestamp is checked (GitHub,
        /// Shopify sign the body alone; Stripe packs its timestamp
        /// into the signature header instead, see `packed`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timestamp_header: Option<String>,
        /// The signature header packs `k=v` pairs instead of a bare
        /// digest (Stripe's `t=1699…,v1=5257a…`): read the timestamp
        /// and the signature out of these keys. When several pairs
        /// carry the signature key (a rolled secret's overlap), any
        /// one matching verifies. Mutually exclusive with
        /// `timestamp_header` and with `prefix`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        packed: Option<PackedSignature>,
        /// The signed string, as a template over the request parts:
        /// `{body}` (always required), `{timestamp}` (iff a timestamp
        /// source is declared), `{url}` (the public address the
        /// provider was told to post to), `{method}`, and
        /// `{sorted_form_params}` (the form-encoded body's pairs,
        /// sorted by name, concatenated name+value: Twilio's rule).
        concat: SignedConcat,
        /// A prefix the presented signature carries before the digest
        /// ("v0=", "sha256="). Empty = the bare digest.
        #[serde(default, skip_serializing_if = "String::is_empty")]
        prefix: String,
        /// The hash inside the HMAC: sha256 (nearly everyone) or
        /// sha1 (Twilio, by its spec).
        #[serde(default, skip_serializing_if = "HmacAlgorithm::is_default")]
        algorithm: HmacAlgorithm,
        /// How the digest is written out: hex (Slack, GitHub) or
        /// base64 (Shopify, Twilio).
        #[serde(default, skip_serializing_if = "DigestEncoding::is_default")]
        encoding: DigestEncoding,
    },
    /// A public-key signature over a declared concatenation, verified
    /// against the receiving app's configured public key (never a
    /// shared secret). Discord signs with Ed25519; SendGrid with
    /// ECDSA P-256.
    Signature {
        scheme: SignatureScheme,
        /// The header carrying the signature.
        signature_header: String,
        /// The header carrying the signed timestamp, when the scheme
        /// signs one (both surveyed ones do).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timestamp_header: Option<String>,
        /// The signed string, same placeholder vocabulary as `hmac`.
        concat: SignedConcat,
        /// How the signature is written: hex (Discord) or base64
        /// (SendGrid, DER-encoded inside).
        #[serde(default, skip_serializing_if = "DigestEncoding::is_default")]
        encoding: DigestEncoding,
    },
    /// No signature at all: the proof is a token WE minted when we
    /// asked for the subscription and the provider echoes back on
    /// every push (per-subscription and unguessable, so matching it
    /// is the whole proof; Google's watch channels).
    TokenEcho,
    /// A signed OIDC identity token in the push's Authorization
    /// header, verified against the named issuer's published keys and
    /// our expected audience (the queue-relay shape).
    Oidc {
        /// The `iss` values the token may carry.
        issuers: Vec<String>,
        /// Where the issuer publishes its signing keys (JWKS).
        jwks_url: String,
    },
}

/// How a digest or signature is written into its header, shared by
/// the HMAC and the public-key schemes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DigestEncoding {
    #[default]
    Hex,
    Base64,
}

impl DigestEncoding {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

/// One resolvable part of a signed concat; see [`SIGNED_PARTS`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignedPart {
    Body,
    Timestamp,
    Url,
    Method,
    SortedFormParams,
}

impl SignedPart {
    fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "body" => Self::Body,
            "timestamp" => Self::Timestamp,
            "url" => Self::Url,
            "method" => Self::Method,
            "sorted_form_params" => Self::SortedFormParams,
            _ => return None,
        })
    }
}

/// One segment of a parsed signed concat: literal text, or a part
/// spliced from the push.
#[derive(Debug, Clone, PartialEq)]
pub enum SignedSegment {
    Literal(String),
    Part(SignedPart),
}

/// A signed-concat template, validated by construction: parsing (from
/// the wire string, once, where the recipe is read) refuses an
/// unclosed brace or a placeholder outside [`SIGNED_PARTS`], so the
/// downstream resolver (`access::verify::signed_bytes`) walks typed
/// segments with no malformed-template case left. On the wire it is
/// the plain template string, exactly as before.
#[derive(Debug, Clone, PartialEq)]
pub struct SignedConcat {
    raw: String,
    segments: Vec<SignedSegment>,
}

impl SignedConcat {
    pub fn as_str(&self) -> &str {
        &self.raw
    }

    pub fn segments(&self) -> &[SignedSegment] {
        &self.segments
    }

    /// Does the concat splice `part`? What the cross-field rules read.
    pub fn covers(&self, part: SignedPart) -> bool {
        self.segments.iter().any(|s| matches!(s, SignedSegment::Part(p) if *p == part))
    }
}

impl std::str::FromStr for SignedConcat {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, String> {
        let mut segments = Vec::new();
        let mut rest = raw;
        while let Some(start) = rest.find('{') {
            if start > 0 {
                segments.push(SignedSegment::Literal(rest[..start].to_string()));
            }
            let after = &rest[start + 1..];
            let Some(end) = after.find('}') else {
                return Err(format!("unclosed '{{' in concat {raw:?}"));
            };
            let name = &after[..end];
            let Some(part) = SignedPart::from_name(name) else {
                return Err(format!(
                    "concat {raw:?} interpolates '{{{name}}}'; it may name only {}",
                    SIGNED_PARTS.map(|p| format!("{{{p}}}")).join(", ")
                ));
            };
            segments.push(SignedSegment::Part(part));
            rest = &after[end + 1..];
        }
        if rest.contains('}') {
            return Err(format!("stray '}}' in concat {raw:?}"));
        }
        if !rest.is_empty() {
            segments.push(SignedSegment::Literal(rest.to_string()));
        }
        Ok(Self { raw: raw.to_string(), segments })
    }
}

impl Serialize for SignedConcat {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.raw)
    }
}

impl<'de> Deserialize<'de> for SignedConcat {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        String::deserialize(d)?.parse().map_err(serde::de::Error::custom)
    }
}

/// The hash an HMAC scheme runs; see [`VerifyKind::Hmac`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HmacAlgorithm {
    #[default]
    Sha256,
    Sha1,
}

impl HmacAlgorithm {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

/// The `k=v` keys of a packed signature header; see
/// [`VerifyKind::Hmac::packed`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackedSignature {
    /// The key carrying the unix timestamp ("t").
    pub timestamp: String,
    /// The key carrying the digest ("v1").
    pub signature: String,
}

/// The public-key signature schemes weft implements; see
/// [`VerifyKind::Signature`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignatureScheme {
    /// Ed25519 over the concat bytes; the configured public key is
    /// the provider's hex form (Discord's).
    Ed25519,
    /// ECDSA P-256 (SHA-256) with a DER-encoded signature; the
    /// configured public key is base64 DER/SPKI (SendGrid's).
    EcdsaP256,
}

/// How an incoming event is matched to the subscription it feeds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteBy {
    /// The event names the provider account (a workspace id, a
    /// mailbox address); it feeds every subscription of every
    /// connection to that account. The common shape.
    #[default]
    Account,
    /// The event names an id WE minted when we subscribed; it feeds
    /// exactly the subscription that minted it. The shape services
    /// use when a subscription is per-resource rather than
    /// per-account (a watch on one file).
    Subscription,
}

/// Unwrap an event that arrives encoded inside an envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Decode {
    /// Where the encoded event sits in the envelope: a body path for
    /// `base64_json`, a FORM FIELD NAME for `form_json` (that body is
    /// urlencoded form data, not JSON, so there is no path to walk).
    pub path: String,
    pub encoding: Encoding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Encoding {
    /// Base64 (standard or url-safe) of JSON text.
    Base64Json,
    /// The raw body is `application/x-www-form-urlencoded`; the field
    /// named by `path` holds JSON text (Slack's interactivity
    /// `payload=` shape).
    FormJson,
}

/// Asking the provider to send, and keeping that arrangement alive.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionCalls {
    /// The call that asks the provider to start sending. Its values
    /// include the ones weft mints for this subscription (see
    /// [`MINTED_ID`], [`MINTED_TOKEN`], [`RECEIVER_URL`]) plus the
    /// connection's own stored values and the node's target.
    pub subscribe: ConnectCall,
    /// The call that stops it. Absent = the subscription simply
    /// lapses (and the node's deactivation cannot hurry it).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unsubscribe: Option<ConnectCall>,
    /// Set when the subscription EXPIRES: re-subscribe this many
    /// seconds before the recorded expiry. The subscribe call must
    /// then capture [`EXPIRES_AT`]. Absent = it never expires.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub renew_margin_secs: Option<u64>,
}

/// The value name a subscribe call reads to learn the id weft minted
/// for this subscription (what `route_by: subscription` matches on).
pub const MINTED_ID: &str = "subscription_id";
/// The value name carrying the secret weft minted for this
/// subscription (what [`VerifyKind::TokenEcho`] compares against).
pub const MINTED_TOKEN: &str = "subscription_token";
/// The value name carrying the address the provider should post to.
pub const RECEIVER_URL: &str = "receiver_url";
/// The capture name a subscribe call must use for the moment its
/// subscription dies, when it declares a renewal margin. RFC3339 or
/// epoch seconds (milliseconds are accepted too: Google answers
/// epoch millis).
pub const EXPIRES_AT: &str = "expires_at";

/// The value names weft supplies to a subscribe/unsubscribe call, on
/// top of the connection's own stored values.
pub const MINTED_VALUE_NAMES: [&str; 3] = [MINTED_ID, MINTED_TOKEN, RECEIVER_URL];

/// What the side SERVING a connection's event subscriptions needs,
/// resolved fresh off the stored connection: the connection's auth
/// (steps + exactly the values they interpolate), the service's event
/// topics, and the extra stored values the recipes' own calls name (a
/// socket mint reads an app-level token no ordinary API call ever
/// uses). ONE definition, beside the recipe vocabulary it carries, so
/// the resolver and every hop relaying the answer share the shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedEventSource {
    pub values: BTreeMap<String, String>,
    pub auth: Vec<super::spec::AuthStep>,
    /// The service's event topics, off the connection's spec snapshot.
    pub events: BTreeMap<String, EventsSpec>,
    /// The values the recipes' own calls name, on top of `values`.
    /// Same rule as the worker handoff: exactly what the declared
    /// templates interpolate, nothing more.
    pub recipe_values: BTreeMap<String, String>,
    /// The provider account the connection belongs to (captured at
    /// connect); what an account-scoped subscription on a shared
    /// multi-install socket filters its frames by.
    #[serde(default)]
    pub provider_account: Option<String>,
}

impl EventsSpec {
    /// Rebuild one event as a flat object of the service's NAMED
    /// fields, reading each from wherever the service says it lives
    /// (body path or header). This is what predicates run against and
    /// what a trigger node's payload is, so a subscription and a node
    /// body both speak names and never provider paths.
    ///
    /// Absent fields are simply not present in the result, and a JSON
    /// null value reads as absent too: a predicate reads both as
    /// "missing", which is the honest answer.
    pub fn named_event(&self, body: &Value, headers: &BTreeMap<String, String>) -> Value {
        let mut out = serde_json::Map::with_capacity(self.fields.len());
        for (name, path) in &self.fields {
            if let Some(v) = read_path(body, headers, path) {
                if !v.is_null() {
                    out.insert(name.clone(), v);
                }
            }
        }
        Value::Object(out)
    }

    /// The account identifier an incoming event names, per
    /// [`EventAccount::path`].
    pub fn account_of(&self, body: &Value, headers: &BTreeMap<String, String>) -> Option<String> {
        read_path(body, headers, &self.account.path).map(|v| match v {
            Value::String(s) => s,
            other => other.to_string(),
        })
    }

    /// The account a socket FRAME concerns, on a socket marked
    /// `multi_account`: the topic's account path, read from the
    /// object the event sits in (`event_path` minus its last segment;
    /// the frame itself when the path is flat or empty). A socket
    /// frame nests the push body inside its own envelope and
    /// `event_path` already names that nesting, so the one account
    /// concept ([`EventsSpec::account`]) serves every surface an
    /// event arrives on.
    pub fn socket_frame_account(&self, frame: &Value) -> Option<String> {
        let socket = self.socket.as_ref()?;
        let container = match socket.event_path.rsplit_once('.') {
            Some((parent, _)) => super::spec::lookup_path(frame, parent)?,
            None => frame,
        };
        self.account_of(container, &BTreeMap::new())
    }

    /// Serde-inexpressible rules. Run wherever a spec enters the
    /// system, alongside the rest of the recipe's validation.
    pub fn validate(&self, service: &str) -> Result<(), String> {
        // `service` arrives as "<service>.<topic>" from the map-level
        // validation, so every message below names the exact topic.
        if self.fields.is_empty() {
            return Err(format!(
                "service '{service}' declares an events section with no fields; a \
                 subscription would have nothing to filter on"
            ));
        }
        for (name, path) in &self.fields {
            if path.trim().is_empty() || path == HEADER_PREFIX {
                return Err(format!(
                    "service '{service}' maps the event field '{name}' to an empty path"
                ));
            }
        }
        if self.account.value.trim().is_empty() || self.account.path.trim().is_empty() {
            return Err(format!(
                "service '{service}' needs both the stored value naming an account and the \
                 path where an event carries it"
            ));
        }
        if self.socket.is_none() && self.webhook.is_none() {
            return Err(format!(
                "service '{service}' declares an events section but no transport; declare \
                 the socket recipe, the webhook recipe, or both"
            ));
        }
        if let Some(socket) = &self.socket {
            if socket.minted.connect.is_none() {
                return Err(format!(
                    "service '{service}' declares a socket recipe with no connect call to \
                     mint the address; declare `socket.connect`"
                ));
            }
            socket.minted.validate(&format!("service '{service}' events.socket"))?;
            if let Some(key) = socket.unknown.keys().next() {
                return Err(format!(
                    "service '{service}' events.socket declares an unknown key '{key}'; \
                     fix the spelling (the recipe's fields are connect, url_from, \
                     replies, event_path, multi_account)"
                ));
            }
            if socket.multi_account && self.account.path.starts_with(HEADER_PREFIX) {
                return Err(format!(
                    "service '{service}' marks its socket multi_account, but the account \
                     path '{}' reads a header and a socket frame has no headers; point \
                     the account path at a body field the frame carries",
                    self.account.path
                ));
            }
        }
        if let Some(webhook) = &self.webhook {
            webhook.validate(service, &self.account)?;
        }
        Ok(())
    }
}

impl WebhookRecipe {
    fn validate(&self, service: &str, account: &EventAccount) -> Result<(), String> {
        if let Some(decode) = &self.decode {
            if decode.path.trim().is_empty() {
                return Err(format!("service '{service}' declares a decode with no path"));
            }
        }
        // Routing by a minted id only means something when we minted
        // one, which only happens when we subscribe.
        if self.route_by == RouteBy::Subscription && self.subscribe.is_none() {
            return Err(format!(
                "service '{service}' routes its events by the id weft mints, but declares \
                 no subscribe call to mint one in"
            ));
        }
        if self.route_by == RouteBy::Subscription && self.id_path.is_none() {
            return Err(format!(
                "service '{service}' routes its events by the minted subscription id, so it \
                 must say where a push carries it (`id_path`)"
            ));
        }
        if matches!(self.verify, VerifyKind::TokenEcho) && self.token_path.is_none() {
            return Err(format!(
                "service '{service}' verifies pushes against the minted token, so it must \
                 say where a push carries it (`token_path`)"
            ));
        }
        // The echoed token is the one we minted at subscribe; with no
        // subscribe call there is nothing to compare against.
        if matches!(self.verify, VerifyKind::TokenEcho) && self.subscribe.is_none() {
            return Err(format!(
                "service '{service}' verifies pushes with the token weft mints at subscribe \
                 time, but declares no subscribe call"
            ));
        }
        // The query-echo handshake is a GET with no body, so a verify
        // that signs the request could never accept it; it pairs with
        // token-echo-style schemes.
        if matches!(self.handshake, Some(Handshake::EchoQueryParam { .. }))
            && matches!(self.verify, VerifyKind::Hmac { .. } | VerifyKind::Signature { .. })
        {
            return Err(format!(
                "service '{service}' proves its address with a query-echo GET, which \
                 carries no body or signature for a signing verify to check; pair \
                 echo_query_param with a token-echo-style verify"
            ));
        }
        // The scheme parameters are data; hold them to their shape
        // here, where the recipe's author is watching.
        match &self.verify {
            VerifyKind::Hmac {
                signature_header,
                timestamp_header,
                packed,
                concat,
                prefix,
                ..
            } => {
                if signature_header.trim().is_empty()
                    || timestamp_header.as_deref().is_some_and(|h| h.trim().is_empty())
                {
                    return Err(format!(
                        "service '{service}' declares an hmac verify with an empty header name"
                    ));
                }
                if packed.is_some() && timestamp_header.is_some() {
                    return Err(format!(
                        "service '{service}' declares an hmac verify with both `packed` and \
                         `timestamp_header`; a packed header carries its own timestamp key"
                    ));
                }
                if packed.is_some() && !prefix.is_empty() {
                    return Err(format!(
                        "service '{service}' declares an hmac verify with both `packed` and \
                         a `prefix`; a packed header's key already delimits the digest"
                    ));
                }
                if let Some(p) = packed {
                    if p.timestamp.trim().is_empty() || p.signature.trim().is_empty() {
                        return Err(format!(
                            "service '{service}' declares a packed hmac header with an \
                             empty key name"
                        ));
                    }
                }
                let has_timestamp = timestamp_header.is_some() || packed.is_some();
                validate_signed_concat(service, "hmac", concat, has_timestamp)?;
            }
            VerifyKind::Signature { signature_header, timestamp_header, concat, .. } => {
                if signature_header.trim().is_empty()
                    || timestamp_header.as_deref().is_some_and(|h| h.trim().is_empty())
                {
                    return Err(format!(
                        "service '{service}' declares a signature verify with an empty \
                         header name"
                    ));
                }
                validate_signed_concat(service, "signature", concat, timestamp_header.is_some())?;
            }
            VerifyKind::Oidc { issuers, jwks_url } => {
                if issuers.is_empty() {
                    return Err(format!(
                        "service '{service}' declares an oidc verify naming no issuers"
                    ));
                }
                if !jwks_url.starts_with("https://") {
                    return Err(format!(
                        "service '{service}' declares an oidc verify whose jwks_url is not \
                         https; signing keys must come over TLS"
                    ));
                }
            }
            VerifyKind::TokenEcho => {}
        }
        if let Some(calls) = &self.subscribe {
            calls.subscribe.validate("events.webhook.subscribe")?;
            if let Some(unsub) = &calls.unsubscribe {
                unsub.validate("events.webhook.unsubscribe")?;
            }
            // A capture named after a value weft mints would shadow the
            // minted value in the stop call; refuse the collision.
            for capture in &calls.subscribe.captures {
                if MINTED_VALUE_NAMES.contains(&capture.name.as_str()) {
                    return Err(format!(
                        "service '{service}' captures '{}' from its subscribe answer, but \
                         that name is one weft mints itself; capture it under another name",
                        capture.name
                    ));
                }
            }
            // At stop time the supplied names are the connection's
            // values, the recipe's, the minted id and token, and the
            // subscribe call's captures; a stop template naming any
            // other minted value could never resolve.
            if let Some(unsub) = &calls.unsubscribe {
                for name in unsub.value_names()? {
                    if MINTED_VALUE_NAMES.contains(&name.as_str())
                        && name != MINTED_ID
                        && name != MINTED_TOKEN
                    {
                        return Err(format!(
                            "service '{service}' interpolates '{{{name}}}' in its \
                             unsubscribe call, but that value exists only while \
                             subscribing; a stop call may use the connection's values, \
                             the recipe's, {{{MINTED_ID}}}, {{{MINTED_TOKEN}}}, and the \
                             subscribe call's captures"
                        ));
                    }
                }
            }
            let names = calls.subscribe.value_names()?;
            // A queue-relayed topic (`decode` present) is the one
            // legitimate case where the subscribe names no address:
            // delivery goes topic -> queue -> receiver, and the queue
            // side was pointed at the receiver once, out of band.
            if self.decode.is_none() && !names.iter().any(|n| n == RECEIVER_URL) {
                return Err(format!(
                    "service '{service}' subscribes without telling the provider where to \
                     send; interpolate {{{RECEIVER_URL}}} in the subscribe call"
                ));
            }
            if self.route_by == RouteBy::Subscription && !names.iter().any(|n| n == MINTED_ID) {
                return Err(format!(
                    "service '{service}' routes by the minted subscription id, so its \
                     subscribe call must send it: interpolate {{{MINTED_ID}}}"
                ));
            }
            if matches!(self.verify, VerifyKind::TokenEcho)
                && !names.iter().any(|n| n == MINTED_TOKEN)
            {
                return Err(format!(
                    "service '{service}' verifies pushes against the token weft mints, so its \
                     subscribe call must send it: interpolate {{{MINTED_TOKEN}}}"
                ));
            }
            match (calls.renew_margin_secs, expiry_capture(calls)) {
                (Some(0), _) => {
                    return Err(format!(
                        "service '{service}' declares a zero renewal margin; a subscription \
                         renewed at the instant it dies has already lapsed"
                    ))
                }
                (Some(_), false) => {
                    return Err(format!(
                        "service '{service}' renews its subscriptions, so the subscribe call \
                         must capture '{EXPIRES_AT}'"
                    ))
                }
                (None, true) => {
                    return Err(format!(
                        "service '{service}' captures '{EXPIRES_AT}' but declares no \
                         renew_margin_secs, so the subscription would silently lapse; declare \
                         the margin"
                    ))
                }
                _ => {}
            }
        }
        // Routing by account needs the account's own path, which the
        // events section always declares; state the dependency here
        // so a service that gets it wrong hears about it once.
        if self.route_by == RouteBy::Account && account.path.trim().is_empty() {
            return Err(format!(
                "service '{service}' routes its events by account, so it must say where an \
                 event carries the account identifier"
            ));
        }
        Ok(())
    }
}

/// The placeholder names a signed concat may interpolate; see
/// [`VerifyKind::Hmac::concat`]. One vocabulary for every signing
/// scheme, resolved by `access::verify::signed_bytes`.
pub const SIGNED_PARTS: [&str; 5] =
    ["body", "timestamp", "url", "method", "sorted_form_params"];

/// The cross-field rules of a signed concat (its own shape is checked
/// at parse; see [`SignedConcat`]): something of the request actually
/// covered, and the timestamp either fully in (a declared source,
/// signed, replay-checked) or fully out. A declared timestamp the
/// signature does not cover proves nothing (anyone can send any
/// timestamp), and a concat naming an undeclared source can never
/// resolve.
fn validate_signed_concat(
    service: &str,
    what: &str,
    concat: &SignedConcat,
    has_timestamp_source: bool,
) -> Result<(), String> {
    if !concat.covers(SignedPart::Body) && !concat.covers(SignedPart::SortedFormParams) {
        return Err(format!(
            "service '{service}' declares a {what} verify whose concat never covers the \
             request ({{body}} or {{sorted_form_params}}); a signature over nothing \
             proves nothing"
        ));
    }
    let signed = concat.covers(SignedPart::Timestamp);
    match (has_timestamp_source, signed) {
        (true, false) => Err(format!(
            "service '{service}' declares a {what} timestamp its concat never signs; \
             sign it ({{timestamp}}) or drop the timestamp declaration"
        )),
        (false, true) => Err(format!(
            "service '{service}' signs {{timestamp}} in its {what} concat but declares \
             no timestamp source to read it from"
        )),
        _ => Ok(()),
    }
}

fn expiry_capture(calls: &SubscriptionCalls) -> bool {
    calls.subscribe.captures.iter().any(|c| c.name == EXPIRES_AT)
}

/// Validate a service's whole topic map: each topic's own rules,
/// plus the cross-topic rule that every topic naming an account
/// names the SAME stored value. The provider account of a service is
/// one concept (one indexed column on the connection row), so two
/// topics disagreeing about which stored value it is would make the
/// column's content depend on iteration order.
pub fn validate_topics(
    service: &str,
    topics: &BTreeMap<String, EventsSpec>,
) -> Result<(), String> {
    let mut account_value: Option<(&str, &str)> = None;
    for (topic, spec) in topics {
        if topic.trim().is_empty()
            || !topic.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        {
            return Err(format!(
                "service '{service}' declares an event topic named '{topic}'; topic names \
                 are [a-z0-9_]+ (they ride URLs)"
            ));
        }
        spec.validate(&format!("{service}.{topic}"))?;
        match account_value {
            None => account_value = Some((topic, &spec.account.value)),
            Some((first, value)) if value != spec.account.value => {
                return Err(format!(
                    "service '{service}' topics '{first}' and '{topic}' disagree about \
                     which stored value names the account ('{value}' vs '{}'); a service \
                     has one account identity",
                    spec.account.value
                ))
            }
            Some(_) => {}
        }
    }
    Ok(())
}

/// Read one declared path out of an event: a `header:`-prefixed path
/// names an HTTP header (case-insensitively, as headers are), and
/// anything else is a dotted path into the body.
pub fn read_path(
    body: &Value,
    headers: &BTreeMap<String, String>,
    path: &str,
) -> Option<Value> {
    match path.strip_prefix(HEADER_PREFIX) {
        Some(name) => headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name.trim()))
            .map(|(_, v)| Value::String(v.clone())),
        None => super::spec::lookup_path(body, path).cloned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn slack_events() -> EventsSpec {
        serde_json::from_value(json!({
            "fields": {
                "type": "type",
                "channel": "channel",
                "text": "text",
                "user": "user",
                "bot": "bot_id",
                "thread": "thread_ts"
            },
            "account": { "value": "team", "path": "team_id" },
            "socket": {
                "connect": {
                    "url": "https://slack.com/api/apps.connections.open",
                    "method": "POST",
                    "auth": [{ "kind": "header", "name": "Authorization",
                               "value": "Bearer {app_token}" }],
                    "captures": [{ "name": "url", "path": "url" }]
                },
                "replies": [{ "when_field": "envelope_id",
                              "frame": "{\"envelope_id\":\"{value}\"}" }],
                "event_path": "payload.event"
            },
            "webhook": {
                "handshake": { "kind": "echo_body_field", "path": "challenge" },
                "verify": { "kind": "hmac",
                            "signature_header": "X-Slack-Signature",
                            "timestamp_header": "X-Slack-Request-Timestamp",
                            "concat": "v0:{timestamp}:{body}",
                            "prefix": "v0=" }
            }
        }))
        .expect("the slack events recipe parses")
    }

    fn drive_events() -> EventsSpec {
        serde_json::from_value(json!({
            "fields": {
                "state": "header:X-Goog-Resource-State",
                "changed": "header:X-Goog-Changed",
                "resource": "header:X-Goog-Resource-Id"
            },
            "account": { "value": "account", "path": "header:X-Goog-Channel-Token" },
            "webhook": {
                "verify": { "kind": "token_echo" },
                "route_by": "subscription",
                "id_path": "header:X-Goog-Channel-ID",
                "token_path": "header:X-Goog-Channel-Token",
                "subscribe": {
                    "subscribe": {
                        "url": "https://www.googleapis.com/drive/v3/files/{target}/watch",
                        "method": "POST",
                        "body": {
                            "id": "{subscription_id}",
                            "type": "web_hook",
                            "address": "{receiver_url}",
                            "token": "{subscription_token}"
                        },
                        "auth": [{ "kind": "header", "name": "Authorization",
                                   "value": "Bearer {token}" }],
                        "captures": [{ "name": "expires_at", "path": "expiration" },
                                     { "name": "resource_id", "path": "resourceId" }]
                    },
                    "unsubscribe": {
                        "url": "https://www.googleapis.com/drive/v3/channels/stop",
                        "method": "POST",
                        "body": { "id": "{subscription_id}", "resourceId": "{resource_id}" },
                        "auth": [{ "kind": "header", "name": "Authorization",
                                   "value": "Bearer {token}" }]
                    },
                    "renew_margin_secs": 3600
                }
            }
        }))
        .expect("the drive watch recipe parses")
    }

    #[test]
    fn a_socket_recipe_validates_and_round_trips() {
        let spec = slack_events();
        spec.validate("slack").expect("valid");
        let back: EventsSpec =
            serde_json::from_value(serde_json::to_value(&spec).unwrap()).unwrap();
        assert_eq!(back, spec);
    }

    /// `flatten` on the minted half rules out serde's own unknown-field
    /// refusal, so validate carries it: a typoed socket key fails at
    /// load, never a silent default at runtime.
    #[test]
    fn a_typoed_socket_key_is_refused_at_validate() {
        let mut raw = serde_json::to_value(slack_events()).unwrap();
        raw["socket"]["evnt_path"] = serde_json::json!("payload.event");
        let spec: EventsSpec = serde_json::from_value(raw).expect("still parses");
        let err = spec.validate("slack").expect_err("the typo must refuse");
        assert!(err.contains("evnt_path"), "{err}");
    }

    #[test]
    fn an_expiring_webhook_recipe_validates_and_round_trips() {
        let spec = drive_events();
        spec.validate("google").expect("valid");
        let back: EventsSpec =
            serde_json::from_value(serde_json::to_value(&spec).unwrap()).unwrap();
        assert_eq!(back, spec);
    }

    /// Named fields are what everything downstream speaks: an event
    /// becomes a flat object keyed by the service's names, read from
    /// the body or the headers as declared.
    #[test]
    fn an_event_becomes_the_services_named_fields() {
        let spec = slack_events();
        let body = json!({
            "type": "message", "channel": "C42", "text": "hi", "user": "U1",
            "team_id": "T9", "ignored": "not declared", "thread_ts": null
        });
        let named = spec.named_event(&body, &BTreeMap::new());
        assert_eq!(named["type"], "message");
        assert_eq!(named["channel"], "C42");
        assert_eq!(named["text"], "hi");
        assert!(named.get("ignored").is_none(), "only declared fields ride");
        assert!(named.get("bot").is_none(), "an absent field stays absent");
        assert!(named.get("thread").is_none(), "a null value reads as absent");
        assert_eq!(spec.account_of(&body, &BTreeMap::new()).as_deref(), Some("T9"));
    }

    /// A push whose data is entirely in headers (no body at all) is
    /// read the same way, through `header:` paths.
    #[test]
    fn header_paths_read_a_bodyless_push() {
        let spec = drive_events();
        let headers = BTreeMap::from([
            ("x-goog-resource-state".to_string(), "update".to_string()),
            ("X-Goog-Channel-Token".to_string(), "tok-1".to_string()),
        ]);
        let named = spec.named_event(&Value::Null, &headers);
        assert_eq!(named["state"], "update", "headers match case-insensitively");
        assert!(named.get("changed").is_none());
        assert_eq!(spec.account_of(&Value::Null, &headers).as_deref(), Some("tok-1"));
    }

    /// Every dependency between the webhook parts is enforced at
    /// parse time, because each one is a trigger that would otherwise
    /// register fine and then never fire.
    #[test]
    fn the_webhook_recipes_dependencies_are_refused_loudly() {
        // Routing by a minted id with nothing that mints one.
        let mut spec = drive_events();
        spec.webhook.as_mut().unwrap().subscribe = None;
        let err = spec.validate("google").unwrap_err();
        assert!(err.contains("mint"), "{err}");

        // A renewal margin with no expiry capture.
        let mut spec = drive_events();
        let calls = spec.webhook.as_mut().unwrap().subscribe.as_mut().unwrap();
        calls.subscribe.captures.retain(|c| c.name != EXPIRES_AT);
        let err = spec.validate("google").unwrap_err();
        assert!(err.contains(EXPIRES_AT), "{err}");

        // An expiry capture with no margin: a silent lapse.
        let mut spec = drive_events();
        spec.webhook.as_mut().unwrap().subscribe.as_mut().unwrap().renew_margin_secs = None;
        let err = spec.validate("google").unwrap_err();
        assert!(err.contains("lapse"), "{err}");

        // A subscribe that never says where to send.
        let mut spec = drive_events();
        let calls = spec.webhook.as_mut().unwrap().subscribe.as_mut().unwrap();
        calls.subscribe.body = Some(json!({ "id": "{subscription_id}" }));
        let err = spec.validate("google").unwrap_err();
        assert!(err.contains(RECEIVER_URL), "{err}");

        // A channel-token check whose subscribe never sends the token.
        let mut spec = drive_events();
        let calls = spec.webhook.as_mut().unwrap().subscribe.as_mut().unwrap();
        calls.subscribe.body = Some(json!({
            "id": "{subscription_id}", "address": "{receiver_url}"
        }));
        let err = spec.validate("google").unwrap_err();
        assert!(err.contains(MINTED_TOKEN), "{err}");
    }

    /// A subscribe capture named after a minted value would shadow the
    /// minted token/id in the stop call; refused naming the capture.
    #[test]
    fn a_capture_shadowing_a_minted_name_is_refused() {
        let mut spec = drive_events();
        let calls = spec.webhook.as_mut().unwrap().subscribe.as_mut().unwrap();
        calls.subscribe.captures.push(crate::access::spec::Capture {
            name: MINTED_TOKEN.into(),
            path: "token".into(),
            optional: false,
        });
        let err = spec.validate("google").unwrap_err();
        assert!(err.contains(MINTED_TOKEN) && err.contains("mints"), "{err}");
    }

    /// A stop call naming a value only supplied while subscribing
    /// (the receiver address) could never resolve; refused by name.
    #[test]
    fn an_unsubscribe_naming_a_subscribe_only_value_is_refused() {
        let mut spec = drive_events();
        let calls = spec.webhook.as_mut().unwrap().subscribe.as_mut().unwrap();
        let unsub = calls.unsubscribe.as_mut().unwrap();
        unsub.body = Some(json!({ "id": "{subscription_id}", "address": "{receiver_url}" }));
        let err = spec.validate("google").unwrap_err();
        assert!(err.contains(RECEIVER_URL) && err.contains("unsubscribe"), "{err}");

        // The minted id and token stay legal in a stop call.
        let mut spec = drive_events();
        let calls = spec.webhook.as_mut().unwrap().subscribe.as_mut().unwrap();
        let unsub = calls.unsubscribe.as_mut().unwrap();
        unsub.body = Some(json!({ "id": "{subscription_id}", "token": "{subscription_token}" }));
        spec.validate("google").expect("minted id + token resolve at stop time");
    }

    /// The query-echo handshake is a bodyless GET, so a signing verify
    /// could never accept it; the pairing is refused at parse.
    #[test]
    fn a_query_echo_handshake_cannot_pair_with_a_signing_verify() {
        let mut spec = slack_events();
        spec.webhook.as_mut().unwrap().handshake =
            Some(Handshake::EchoQueryParam { param: "validationToken".into() });
        let err = spec.validate("slack").unwrap_err();
        assert!(err.contains("query-echo"), "{err}");
    }

    /// Topic names ride URLs, so anything outside [a-z0-9_] is refused
    /// at validate time naming the charset.
    #[test]
    fn topic_names_outside_the_url_charset_are_refused() {
        let topics = BTreeMap::from([("Bad-Topic".to_string(), slack_events())]);
        let err = validate_topics("slack", &topics).unwrap_err();
        assert!(err.contains("[a-z0-9_]"), "{err}");
    }

    /// A concat is validated where the recipe is read: an unknown
    /// placeholder or an unclosed brace never becomes a value at all.
    #[test]
    fn a_malformed_concat_fails_at_parse() {
        let err = "{nope}{body}".parse::<SignedConcat>().unwrap_err();
        assert!(err.contains("nope"), "{err}");
        assert!("v0:{timestamp".parse::<SignedConcat>().is_err());
        assert!("v0:{timestamp}:{body}".parse::<SignedConcat>().is_ok());
        // And through serde, where a recipe actually enters.
        let err = serde_json::from_value::<SignedConcat>(json!("{nope}")).unwrap_err();
        assert!(err.to_string().contains("nope"), "{err}");
    }

    /// The hmac timestamp is all-in or all-out: a declared header the
    /// concat never signs is spoofable (refused), and a concat naming
    /// an undeclared header can never resolve (refused). Both sides
    /// pinned, plus the valid timestampless (body-only) shape.
    #[test]
    fn the_hmac_timestamp_is_signed_or_absent() {
        let webhook = |verify: serde_json::Value| -> Result<(), String> {
            let spec: EventsSpec = serde_json::from_value(json!({
                "fields": { "kind": "kind" },
                "account": { "value": "user", "path": "account" },
                "webhook": { "verify": verify }
            }))
            .unwrap();
            spec.validate("svc")
        };

        let err = webhook(json!({
            "kind": "hmac", "signature_header": "X-Sig",
            "timestamp_header": "X-Ts", "concat": "{body}"
        }))
        .unwrap_err();
        assert!(err.contains("never signs"), "{err}");

        let err = webhook(json!({
            "kind": "hmac", "signature_header": "X-Sig",
            "concat": "v0:{timestamp}:{body}"
        }))
        .unwrap_err();
        assert!(err.contains("no timestamp source"), "{err}");

        webhook(json!({
            "kind": "hmac", "signature_header": "X-Hub-Signature-256",
            "concat": "{body}", "prefix": "sha256=", "encoding": "base64"
        }))
        .expect("the body-only shape is valid");
    }

    /// An events section with no transport, no fields, or no account
    /// is a recipe that cannot serve anything.
    #[test]
    fn an_unservable_events_section_is_refused() {
        let mut spec = slack_events();
        spec.socket = None;
        spec.webhook = None;
        assert!(spec.validate("slack").unwrap_err().contains("transport"));

        let mut spec = slack_events();
        spec.fields.clear();
        assert!(spec.validate("slack").unwrap_err().contains("fields"));

        let mut spec = slack_events();
        spec.account.value = String::new();
        assert!(spec.validate("slack").unwrap_err().contains("account"));

        let mut spec = slack_events();
        spec.socket.as_mut().unwrap().minted.url_from = "nowhere".into();
        assert!(spec.validate("slack").unwrap_err().contains("nowhere"));

        let mut spec = slack_events();
        spec.socket.as_mut().unwrap().minted.connect = None;
        assert!(spec.validate("slack").unwrap_err().contains("connect"));
    }

    /// A multi-account socket reads each frame's account through the
    /// topic's account path; a path only a push's HEADERS could
    /// answer can never resolve on a frame, so it is refused where
    /// the recipe is written.
    #[test]
    fn a_multi_account_socket_needs_a_body_account_path() {
        let mut spec = slack_events();
        spec.socket.as_mut().unwrap().multi_account = true;
        spec.validate("slack").expect("a body account path serves the socket");

        spec.account.path = "header:X-Team".into();
        let err = spec.validate("slack").unwrap_err();
        assert!(err.contains("header"), "{err}");
    }

    /// The frame's account is the topic's account path read from the
    /// object the event sits in: the envelope level for a nested
    /// `event_path`, the frame itself for a flat one, and None when
    /// the frame carries nothing there.
    #[test]
    fn a_socket_frame_names_its_account_at_the_events_level() {
        let spec = slack_events();
        // event_path is "payload.event": the account reads from
        // `payload`, beside the event, exactly where the push body's
        // fields sit.
        let frame = json!({
            "envelope_id": "e-1",
            "payload": { "team_id": "T9", "event": { "type": "message" } }
        });
        assert_eq!(spec.socket_frame_account(&frame).as_deref(), Some("T9"));
        assert_eq!(
            spec.socket_frame_account(&json!({ "payload": { "event": {} } })),
            None,
            "a frame naming no account answers None, never a guess"
        );

        // A flat event_path reads the account off the frame itself.
        let mut flat = slack_events();
        flat.socket.as_mut().unwrap().event_path = "event".into();
        let frame = json!({ "team_id": "T3", "event": { "type": "message" } });
        assert_eq!(flat.socket_frame_account(&frame).as_deref(), Some("T3"));
    }
}
