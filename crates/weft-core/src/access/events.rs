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

/// The header a `header:<Name>` place names, trimmed (empty for a bare
/// `header:`); `None` for a place that is not a header. The one reader of
/// the prefix, for event paths and for the places a verify scheme reads.
pub fn header_name(place: &str) -> Option<&str> {
    place.strip_prefix(HEADER_PREFIX).map(str::trim)
}

/// A value the request carries more than once with different values, so
/// which one counts cannot be told. Names what was read, for the message.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the request carries the {0} more than once with different values, so which one counts cannot be told")]
pub struct Ambiguous(pub String);

/// The single distinct value among `values`: `None` when there is none,
/// the value when every copy agrees, [`Ambiguous`] (naming `what`) when
/// two differ. The one rule for a value sent more than once.
pub fn single_value<'v, V: PartialEq + ?Sized>(
    mut values: impl Iterator<Item = &'v V>,
    what: impl FnOnce() -> String,
) -> Result<Option<&'v V>, Ambiguous> {
    let Some(first) = values.next() else { return Ok(None) };
    if values.any(|v| v != first) {
        return Err(Ambiguous(what()));
    }
    Ok(Some(first))
}

/// The one value of the header `name` (matched case-insensitively, as
/// headers are), by [`single_value`]'s rule: sent twice with the same
/// value it is still one, sent with two it is ambiguous.
pub fn header_value<'h>(headers: &'h [(String, String)], name: &str) -> Result<Option<&'h str>, Ambiguous> {
    single_value(
        headers.iter().filter(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str()),
        || format!("{name} header"),
    )
}

/// Where a JSON Web Key Set may be fetched from: an `https://` address
/// with a host, since the keys decide which identity tokens verify, and
/// keys fetched over plain http could be swapped by anyone on the path.
/// Checked when a push recipe loads and again right before each fetch (a
/// gated route's address is a stored value, unknown until then).
pub fn check_jwks_url(url: &str) -> Result<(), String> {
    let refused = |why: &str| format!("the jwksUrl '{url}' {why}, so set it to the issuer's public https:// address");
    let parsed = url::Url::parse(url).map_err(|e| refused(&format!("is not an https:// address ({e})")))?;
    if parsed.scheme() != "https" {
        return Err(refused("is not an https:// address; signing keys must come over TLS"));
    }
    if parsed.host().is_none() {
        return Err(refused("names no host"));
    }
    Ok(())
}

/// How a service reports events. Attached to the service recipe, so
/// it travels with every connection made to that service.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
    /// by `routeBy: subscription` and by the token-echo verification
    /// (the token to compare against is that subscription's).
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
#[serde(tag = "kind", rename_all = "snake_case", rename_all_fields = "camelCase", deny_unknown_fields)]
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
/// by data, never a provider name: where the signature sits and how it
/// is laid out, what is signed, which issuer, all declared in the
/// recipe. The closed part is only the arithmetic (an HMAC, a
/// constant-time compare, a public-key or OIDC signature check),
/// extended deliberately like the request-signing kinds. The SECRET
/// material is never here: it belongs to the app that receives
/// (operator configuration), to the connection a gated route names,
/// or to the subscription weft minted.
///
/// One scheme serves two sides ([`VerifySide`]): a provider's pushes
/// (`WebhookRecipe::verify`) and the callers of a gated route
/// (`AccessSpec::verify`). [`VerifyKind::validate`] holds both to the
/// same rules.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", rename_all_fields = "camelCase", deny_unknown_fields)]
pub enum VerifyKind {
    /// An HMAC over a declared concatenation of request parts,
    /// compared against the signature(s) the request carries. The
    /// shape most signing providers use (Slack, GitHub, Shopify,
    /// Stripe, Twilio, Paddle, Standard Webhooks, Mailgun, Linear).
    Hmac {
        /// Where the signature sits and how its value is laid out.
        signature: SignatureSource,
        /// Where the timestamp the signature covers sits, for a scheme
        /// that signs one (what makes a replayed request refusable).
        /// Absent = no timestamp is checked (GitHub, Shopify sign the
        /// body alone).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timestamp: Option<TimestampSource>,
        /// The signed bytes, as a template over the request; see
        /// [`SignedPart`] for the placeholders.
        concat: SignedConcat,
        /// How the stored secret becomes the HMAC key. Absent = its
        /// bytes as stored.
        #[serde(default, skip_serializing_if = "SecretKey::is_default")]
        secret: SecretKey,
        /// The hash inside the HMAC.
        #[serde(default, skip_serializing_if = "HmacAlgorithm::is_default")]
        algorithm: HmacAlgorithm,
        /// How the digest is written out.
        #[serde(default, skip_serializing_if = "DigestEncoding::is_default")]
        encoding: DigestEncoding,
    },
    /// A public-key signature over a declared concatenation, verified
    /// against the configured public key (never a shared secret).
    /// Discord signs with Ed25519; SendGrid with ECDSA P-256. Same
    /// signature layout and placeholders as `hmac`.
    Signature {
        scheme: SignatureScheme,
        signature: SignatureSource,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timestamp: Option<TimestampSource>,
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
    /// our expected audience (the queue-relay shape). The two
    /// addresses are templates over the connection's stored values
    /// (`"{jwksUrl}"`) when the scheme verifies CALLERS of a route
    /// through an auth connection, and plain literals when it verifies
    /// a provider's pushes (there is no connection to read at push
    /// time, so a push recipe must spell them out).
    Oidc {
        /// The `iss` values the token may carry.
        issuers: Vec<super::spec::Template>,
        /// Where the issuer publishes its signing keys (JWKS).
        jwks_url: super::spec::Template,
    },
    /// A shared key presented on every request, compared in constant
    /// time against the set the connection stores (one per line in its
    /// `keys` value). The identity is the matched key's position, so a
    /// program can tell its callers apart without ever seeing a key.
    /// Only meaningful for callers of a route (a provider never pushes
    /// with one of our keys), so a push recipe may not declare it.
    ApiKeys {
        /// The header the key rides in.
        #[serde(default = "default_api_key_header", skip_serializing_if = "is_default_api_key_header")]
        header: String,
        /// Also accept the key as `Authorization: Bearer <key>`.
        #[serde(default = "default_true", skip_serializing_if = "Clone::clone")]
        bearer: bool,
    },
}

/// The steps a stored map of events recipes (topic name to [`EventsSpec`],
/// as `AccessSpec::events` and the recorded recipes hold it) takes from
/// the shape an older weft wrote; `crate::stored` says how they run.
pub const EVENTS_UPGRADES: &[&[crate::stored::Op]] = &[EVENTS_V2];

/// A service's events recipes as weft records them, kept under their hash
/// (`service_events_recipe.events_json`).
pub const STORED_EVENTS: crate::stored::Kind<std::collections::BTreeMap<String, EventsSpec>> =
    crate::stored::Kind::new("recorded events recipe", EVENTS_UPGRADES);

/// Version 2: each push recipe's scheme moved to the shape where the
/// signature and the timestamp are each a place in the request
/// ([`verify_to_v2`]).
pub const EVENTS_V2: &[crate::stored::Op] =
    &[crate::stored::Op::Code { at: "*.webhook.verify", run: verify_to_v2 }];

/// A stored [`VerifyKind`] written before a signature and a timestamp
/// were each a place in the request ([`SignatureSource`],
/// [`TimestampSource`]), carried to that shape. The old keys of `hmac`
/// and `signature`, and what each becomes:
///
/// - `signatureHeader: H`: `signature: {in: "header:H"}`.
/// - `timestampHeader: T`: `timestamp: {in: "header:T"}`.
/// - `prefix: "v0="` (the digest written after a key and a separator):
///   `signature.pair: "="` and `signature.key: "v0"`.
/// - `packed: {timestamp: t, signature: s}` (Stripe's `t=…,v1=…`):
///   `signature.list: ","`, `signature.pair: "="`, `signature.key: s`,
///   and `timestamp: {in: "signature:t"}`.
///
/// The other schemes did not change.
pub fn verify_to_v2(verify: &mut Value) -> Result<(), String> {
    let Value::Object(scheme) = verify else {
        return Err(format!("a verify scheme is an object, not {verify}"));
    };
    if !matches!(scheme.get("kind").and_then(Value::as_str), Some("hmac" | "signature")) {
        return Ok(());
    }
    let text = |value: Value, key: &str| match value {
        Value::String(s) => Ok(s),
        other => Err(format!("`{key}` is a string, not {other}")),
    };
    let header = scheme
        .remove("signatureHeader")
        .ok_or_else(|| "the scheme names no `signatureHeader`".to_string())
        .and_then(|h| text(h, "signatureHeader"))?;
    let mut signature = serde_json::Map::new();
    signature.insert("in".into(), Value::String(format!("header:{header}")));
    if let Some(ts) = scheme.remove("timestampHeader") {
        let ts = text(ts, "timestampHeader")?;
        scheme.insert("timestamp".into(), serde_json::json!({ "in": format!("header:{ts}") }));
    }
    if let Some(packed) = scheme.remove("packed") {
        let key = |name: &str| {
            packed
                .get(name)
                .and_then(Value::as_str)
                .map(str::to_string)
                .ok_or_else(|| format!("`packed.{name}` is missing"))
        };
        if scheme.contains_key("timestamp") {
            return Err("the scheme declares both `packed` and `timestampHeader`".into());
        }
        scheme.insert("timestamp".into(), serde_json::json!({ "in": format!("signature:{}", key("timestamp")?) }));
        signature.insert("list".into(), ",".into());
        signature.insert("pair".into(), "=".into());
        signature.insert("key".into(), key("signature")?.into());
    }
    if let Some(prefix) = scheme.remove("prefix") {
        let prefix = text(prefix, "prefix")?;
        if signature.contains_key("pair") {
            return Err("the scheme declares both `packed` and `prefix`".into());
        }
        // The prefix is a key and the one separator after it ("v0=",
        // "sha256="): the entry is cut there, and the key must match.
        let pair = prefix.chars().last().filter(|c| !c.is_ascii_alphanumeric());
        match pair {
            Some(pair) if prefix.len() > pair.len_utf8() => {
                let key = &prefix[..prefix.len() - pair.len_utf8()];
                signature.insert("pair".into(), pair.to_string().into());
                signature.insert("key".into(), key.into());
            }
            _ => {
                return Err(format!(
                    "the prefix '{prefix}' is not a key followed by one separator (like \"v0=\"), \
                     so it has no place in the signature's layout"
                ))
            }
        }
    }
    scheme.insert("signature".into(), Value::Object(signature));
    Ok(())
}

fn default_api_key_header() -> String {
    "X-Api-Key".to_string()
}

fn is_default_api_key_header(h: &String) -> bool {
    h == "X-Api-Key"
}

fn default_true() -> bool {
    true
}

impl VerifyKind {
    /// The request headers this scheme reads a credential from: what a run
    /// it admits holds as secrets (`crate::caller::split_credentials`).
    pub fn credential_headers(&self) -> Vec<String> {
        match self {
            VerifyKind::Hmac { signature, .. } | VerifyKind::Signature { signature, .. } => {
                match &signature.at {
                    RequestValue::Header(name) => vec![name.clone()],
                    RequestValue::BodyField(_) | RequestValue::SignatureKey(_) => Vec::new(),
                }
            }
            VerifyKind::ApiKeys { header, .. } => vec![header.clone(), "authorization".to_string()],
            VerifyKind::Oidc { .. } => vec!["authorization".to_string()],
            VerifyKind::TokenEcho => Vec::new(),
        }
    }

    /// The scheme's wire tag (`hmac`, `oidc`, ...), for messages.
    pub fn tag(&self) -> &'static str {
        match self {
            VerifyKind::Hmac { .. } => "hmac",
            VerifyKind::Signature { .. } => "signature",
            VerifyKind::TokenEcho => "token_echo",
            VerifyKind::Oidc { .. } => "oidc",
            VerifyKind::ApiKeys { .. } => "api_keys",
        }
    }

    /// The stored value names the scheme READS off a connection when
    /// it verifies callers: the material it keys on (`signingSecret`,
    /// `publicKey`, `keys`, and for `oidc` the `audience` that tells this
    /// gate's tokens apart from every other app's of the same issuer)
    /// plus every placeholder its templates name. A recipe's paste page
    /// must declare each of them, not optional, checked at metadata load.
    pub fn required_value_names(&self) -> Result<Vec<String>, String> {
        Ok(match self {
            VerifyKind::Hmac { .. } => vec![SIGNING_SECRET.to_string()],
            VerifyKind::Signature { .. } => vec![PUBLIC_KEY.to_string()],
            VerifyKind::ApiKeys { .. } => vec![API_KEYS.to_string()],
            VerifyKind::TokenEcho => Vec::new(),
            VerifyKind::Oidc { .. } => {
                let mut names = self.placeholder_names()?;
                if !names.iter().any(|n| n == AUDIENCE) {
                    names.push(AUDIENCE.to_string());
                }
                names
            }
        })
    }

    /// The stored values the scheme's templates interpolate (only `oidc`
    /// has templates).
    fn placeholder_names(&self) -> Result<Vec<String>, String> {
        let VerifyKind::Oidc { issuers, jwks_url } = self else { return Ok(Vec::new()) };
        let mut names = jwks_url.placeholders()?;
        for issuer in issuers {
            for name in issuer.placeholders()? {
                if !names.contains(&name) {
                    names.push(name);
                }
            }
        }
        Ok(names)
    }

    /// The scheme with its templates resolved against a connection's
    /// stored values: what the verifier runs. A literal recipe resolves
    /// to itself; a placeholder the connection does not store is a loud
    /// error naming it.
    pub fn resolved(&self, values: &BTreeMap<String, String>) -> Result<VerifyKind, String> {
        Ok(match self {
            VerifyKind::Oidc { issuers, jwks_url } => VerifyKind::Oidc {
                issuers: issuers
                    .iter()
                    .map(|t| t.resolve(values).map(super::spec::Template::new))
                    .collect::<Result<Vec<_>, _>>()?,
                jwks_url: super::spec::Template::new(jwks_url.resolve(values)?),
            },
            other => other.clone(),
        })
    }

    /// Does the scheme interpolate any stored value? A push recipe has
    /// no connection to read, so its scheme must be fully literal.
    pub fn has_placeholders(&self) -> Result<bool, String> {
        Ok(!self.placeholder_names()?.is_empty())
    }

    /// The scheme's rules, ONE set for both sides it serves, plus what
    /// only one side can use. Run wherever a recipe enters the system:
    /// a push recipe through [`EventsSpec::validate`], a route gate
    /// through `AccessSpec::validate`.
    pub fn validate(&self, service: &str, side: VerifySide) -> Result<(), String> {
        let tag = self.tag();
        match self {
            VerifyKind::Hmac { signature, timestamp, concat, .. }
            | VerifyKind::Signature { signature, timestamp, concat, .. } => {
                validate_signing(service, tag, signature, timestamp.as_ref(), concat)
            }
            VerifyKind::Oidc { issuers, jwks_url } => {
                if issuers.is_empty() {
                    return Err(format!("service '{service}' declares an oidc verify naming no issuers"));
                }
                if side == VerifySide::Push {
                    // A push has no connection to read, so the addresses
                    // must be spelled out here.
                    if self.has_placeholders()? {
                        return Err(format!(
                            "service '{service}' declares an oidc push verify with a \
                             placeholder; a push recipe names its issuers and jwksUrl \
                             literally (placeholders are for the `verify` block of an \
                             access recipe, which reads the connection)"
                        ));
                    }
                    check_jwks_url(&jwks_url.0).map_err(|e| format!("service '{service}' declares an oidc verify: {e}"))?;
                }
                Ok(())
            }
            VerifyKind::ApiKeys { header, .. } => match side {
                VerifySide::Push => Err(format!(
                    "service '{service}' declares an api_keys push verify; a provider \
                     never presents one of our keys, so a push is verified by hmac, \
                     signature, token_echo or oidc (api_keys is for the `verify` block \
                     of an access recipe, which gates callers of a route)"
                )),
                VerifySide::Caller if header.trim().is_empty() => {
                    Err(format!("service '{service}' declares an api_keys verify with an empty header name"))
                }
                VerifySide::Caller => Ok(()),
            },
            VerifyKind::TokenEcho => match side {
                VerifySide::Push => Ok(()),
                VerifySide::Caller => Err(format!(
                    "service '{service}' declares a token_echo verify for callers, but that scheme \
                     compares a token weft minted when subscribing, which a caller of a \
                     route never has; use api_keys, hmac, signature or oidc"
                )),
            },
        }
    }
}

/// Which side a [`VerifyKind`] serves: a provider's pushes to the
/// events receiver, or the callers of a route it gates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifySide {
    Push,
    Caller,
}

/// The rules of a signing scheme (`hmac` or `signature`): the layout
/// is coherent, something of the request is actually covered, and the
/// timestamp is either fully in (declared, signed, replay-checked) or
/// fully out. A declared timestamp the signature does not cover proves
/// nothing (anyone can send any timestamp), and a concat naming an
/// undeclared one can never resolve.
fn validate_signing(
    service: &str,
    tag: &str,
    signature: &SignatureSource,
    timestamp: Option<&TimestampSource>,
    concat: &SignedConcat,
) -> Result<(), String> {
    let refuse = |what: String| Err(format!("service '{service}' declares a `{tag}` verify {what}"));
    if let RequestValue::SignatureKey(_) = signature.at {
        return refuse(
            "whose signature sits in its own layout; write `in` as header:<Name> or body:<path>".into(),
        );
    }
    for (name, sep) in [("list", &signature.list), ("pair", &signature.pair)] {
        if sep.as_deref().is_some_and(str::is_empty) {
            return refuse(format!("with an empty `{name}` separator"));
        }
    }
    if signature.list.is_some() && signature.list == signature.pair {
        return refuse("whose `list` and `pair` separators are the same, so no entry could be cut".into());
    }
    match (&signature.pair, &signature.key) {
        (Some(_), None) => {
            return refuse("whose signature entries are keyed (`pair`) but names no `key` holding the signature".into())
        }
        (None, Some(_)) => {
            return refuse("naming a signature `key` but no `pair` separator to cut an entry into key and value".into())
        }
        (Some(_), Some(key)) if key.trim().is_empty() => return refuse("with an empty signature `key`".into()),
        _ => {}
    }
    // A signature cannot sign itself: the provider computes it before
    // it exists.
    let carries_signature = concat.covers(&SignedPart::Value(signature.at.clone()))
        || (matches!(signature.at, RequestValue::BodyField(_))
            && (concat.covers(&SignedPart::Body) || concat.covers(&SignedPart::SortedFormParams)));
    if carries_signature {
        return refuse(format!(
            "whose concat signs the very part the signature sits in ({}); sign the other parts",
            signature.at
        ));
    }

    let whole_body = concat.covers(&SignedPart::Body) || concat.covers(&SignedPart::SortedFormParams);
    if !whole_body && !concat.covers_a_body_field() {
        return refuse(
            "whose concat never covers the request ({body}, {sorted_form_params} or a \
             {body:<path>} field); a signature over nothing proves nothing"
                .into(),
        );
    }
    if let Some(ts) = timestamp {
        if ts.tolerance_secs == 0 {
            return refuse("with a zero timestamp tolerance, which refuses every request".into());
        }
        if let RequestValue::SignatureKey(key) = &ts.at {
            if signature.pair.is_none() {
                return refuse(format!(
                    "reading its timestamp from the signature's key '{key}', but the \
                     signature's entries are not keyed; declare `pair`"
                ));
            }
            if signature.key.as_deref() == Some(key.as_str()) {
                return refuse(format!("whose timestamp key and signature key are both '{key}'"));
            }
        }
    }
    // The timestamp is signed when the concat splices it, or splices
    // the place it sits in (a header by name, a body field by path or
    // with the whole body).
    let signed = concat.covers(&SignedPart::Timestamp);
    let covered = timestamp.is_some_and(|ts| match &ts.at {
        RequestValue::BodyField(_) if whole_body => true,
        RequestValue::SignatureKey(_) => false,
        at => concat.covers(&SignedPart::Value(at.clone())),
    });
    match (timestamp.is_some(), signed || covered) {
        (true, false) => {
            return refuse(
                "with a timestamp its concat never signs; sign it ({timestamp}) or drop the \
                 timestamp declaration"
                    .into(),
            )
        }
        (false, true) => {
            return refuse("whose concat signs {timestamp} but declares no timestamp source to read it from".into())
        }
        _ => {}
    }
    // A scheme that signs only some fields of the body (Mailgun's
    // timestamp and token) leaves the rest of the body unbound. What
    // stops a captured signature from carrying other contents is the
    // timestamp, so such a scheme must sign one.
    if !whole_body && timestamp.is_none() {
        return refuse(
            "whose concat signs only fields of the body, so nothing binds the rest of it and \
             a captured signature would verify other contents forever; declare the timestamp \
             the provider signs"
                .into(),
        );
    }
    Ok(())
}

/// The stored value name an HMAC scheme keys on.
pub const SIGNING_SECRET: &str = "signingSecret";
/// The stored value name a public-key scheme verifies against.
pub const PUBLIC_KEY: &str = "publicKey";
/// The stored value name holding a caller's accepted keys, one per line.
pub const API_KEYS: &str = "keys";
/// The stored value name an identity-token scheme checks the token's
/// audience against. A gated route needs it: without one, a token the
/// same issuer minted for any other app would be admitted.
pub const AUDIENCE: &str = "audience";

/// How a digest or signature is written out, shared by the HMAC and
/// the public-key schemes. A presented value is decoded and compared
/// as bytes, so hex reads in either case and base64 with or without
/// its `=` padding.
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

/// Where one value of a request is read, spelled the way a signed
/// concat's placeholders spell it (without the braces):
/// `header:<Name>` (matched case-insensitively, as headers are),
/// `body:<dotted.path>` (a field of the JSON body), and, for a
/// timestamp only, `signature:<key>` (a key of the signature value's
/// own layout, like Stripe's `t`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestValue {
    Header(String),
    BodyField(String),
    SignatureKey(String),
}

impl std::str::FromStr for RequestValue {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, String> {
        let named = |prefix: &str| raw.strip_prefix(prefix).map(str::trim).filter(|rest| !rest.is_empty());
        if let Some(name) = header_name(raw).filter(|name| !name.is_empty()) {
            Ok(Self::Header(name.to_string()))
        } else if let Some(path) = named("body:") {
            Ok(Self::BodyField(path.to_string()))
        } else if let Some(key) = named("signature:") {
            Ok(Self::SignatureKey(key.to_string()))
        } else {
            Err(format!(
                "'{raw}' names no part of the request; write header:<Name>, body:<dotted.path> \
                 or (for a timestamp) signature:<key>"
            ))
        }
    }
}

impl std::fmt::Display for RequestValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Header(name) => write!(f, "header:{name}"),
            Self::BodyField(path) => write!(f, "body:{path}"),
            Self::SignatureKey(key) => write!(f, "signature:{key}"),
        }
    }
}

impl Serialize for RequestValue {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for RequestValue {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        String::deserialize(d)?.parse().map_err(serde::de::Error::custom)
    }
}

/// Where a request's signature sits and how its value is laid out.
/// One description covers every layout surveyed: a bare digest
/// (Shopify, Twilio), one keyed entry (`v0=<hex>` for Slack,
/// `sha256=<hex>` for GitHub), and a list of keyed entries (Stripe's
/// `t=…,v1=…`, Paddle's `ts=…;h1=…`, Standard Webhooks'
/// `v1,<b64> v1,<b64>`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SignatureSource {
    /// Where the value sits: `header:<Name>`, or `body:<path>` for a
    /// provider that puts it in the body (Mailgun).
    #[serde(rename = "in")]
    pub at: RequestValue,
    /// The separator between entries when the value lists several
    /// (`,` for Stripe, `;` for Paddle, a space for Standard
    /// Webhooks). Absent = the whole value is one entry. A header sent
    /// more than once gives the entries of every copy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub list: Option<String>,
    /// The separator between a key and its value inside an entry (`=`
    /// in `v1=<sig>`, `,` in `v1,<sig>`); an entry is cut at its first
    /// one. Absent = every entry is a bare signature.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pair: Option<String>,
    /// With `pair`: the key whose entries hold a signature (`v0`,
    /// `sha256`, `v1`, `h1`). Entries under other keys are skipped,
    /// and when several carry it (a secret being rolled), any one
    /// that matches verifies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

/// The default of [`TimestampSource::tolerance_secs`]: five minutes,
/// the window Slack, Stripe and Standard Webhooks' own libraries use.
pub const TIMESTAMP_TOLERANCE_SECS: u32 = 60 * 5;

/// Where the timestamp a signature covers sits, how it is written,
/// and how far from now it may be before the request is refused as a
/// replay.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TimestampSource {
    /// `header:<Name>`, `body:<path>` (Mailgun, Linear), or
    /// `signature:<key>` for a key of the signature's own layout
    /// (Stripe's `t`, Paddle's `ts`).
    #[serde(rename = "in")]
    pub at: RequestValue,
    #[serde(default, skip_serializing_if = "TimestampFormat::is_default")]
    pub format: TimestampFormat,
    /// Seconds either side of now a timestamp may be.
    #[serde(default = "default_tolerance", skip_serializing_if = "is_default_tolerance")]
    pub tolerance_secs: u32,
}

fn default_tolerance() -> u32 {
    TIMESTAMP_TOLERANCE_SECS
}

fn is_default_tolerance(secs: &u32) -> bool {
    *secs == TIMESTAMP_TOLERANCE_SECS
}

/// How a signed timestamp is written. The `{timestamp}` placeholder
/// always splices the text exactly as the request carried it; the
/// format only says how to read it for the replay check.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampFormat {
    /// Unix seconds (nearly everyone).
    #[default]
    Seconds,
    /// Unix milliseconds (Linear).
    Milliseconds,
    /// An RFC 3339 date and time (`2024-05-01T12:00:00Z`).
    Rfc3339,
}

impl TimestampFormat {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

/// How the stored signing secret becomes the HMAC key.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SecretKey {
    /// A prefix the stored secret carries that is not part of the key
    /// (Standard Webhooks' `whsec_`). A stored secret without it is
    /// refused as unusable, naming the prefix.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub strip_prefix: String,
    /// How what remains is written.
    #[serde(default, skip_serializing_if = "KeyEncoding::is_default")]
    pub decode: KeyEncoding,
}

impl SecretKey {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

/// See [`SecretKey::decode`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyEncoding {
    /// The secret's bytes are the key (nearly everyone).
    #[default]
    Raw,
    /// The key is base64 (Standard Webhooks).
    Base64,
    /// The key is hex.
    Hex,
}

impl KeyEncoding {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

/// One resolvable part of a signed concat, spelled inside braces:
/// `{body}` (the raw bytes, never reparsed), `{timestamp}` (the
/// declared timestamp, as the request wrote it), `{url}` (the full
/// public address the request was sent to, query included),
/// `{method}`, `{sorted_form_params}` (the form-encoded body's pairs,
/// sorted by name, appended name+value: Twilio's rule),
/// `{header:<Name>}` (one header's value) and `{body:<path>}` (one
/// JSON body field: a string as is, a number or boolean as written).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignedPart {
    Body,
    Timestamp,
    Url,
    Method,
    SortedFormParams,
    /// A header or a body field (never a `signature:` key: a layout
    /// key reaches a concat only as `{timestamp}`).
    Value(RequestValue),
}

impl SignedPart {
    fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "body" => Self::Body,
            "timestamp" => Self::Timestamp,
            "url" => Self::Url,
            "method" => Self::Method,
            "sorted_form_params" => Self::SortedFormParams,
            other => match other.parse().ok()? {
                RequestValue::SignatureKey(_) => return None,
                value => Self::Value(value),
            },
        })
    }
}

/// One segment of a parsed signed concat: literal text, or a part
/// spliced from the request.
#[derive(Debug, Clone, PartialEq)]
pub enum SignedSegment {
    Literal(String),
    Part(SignedPart),
}

/// A signed-concat template, validated by construction: parsing (from
/// the wire string, once, where the recipe is read) refuses an
/// unclosed brace or a placeholder that is not a [`SignedPart`], so
/// the resolver (`access::verify::signed_bytes`) walks typed segments
/// with no malformed-template case left. On the wire it is the plain
/// template string.
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

    fn parts(&self) -> impl Iterator<Item = &SignedPart> {
        self.segments.iter().filter_map(|s| match s {
            SignedSegment::Part(p) => Some(p),
            SignedSegment::Literal(_) => None,
        })
    }

    /// Does the concat splice `part`? What the cross-field rules read.
    /// Header names compare case-insensitively.
    pub fn covers(&self, part: &SignedPart) -> bool {
        self.parts().any(|p| match (p, part) {
            (SignedPart::Value(RequestValue::Header(a)), SignedPart::Value(RequestValue::Header(b))) => {
                a.eq_ignore_ascii_case(b)
            }
            (p, part) => p == part,
        })
    }

    /// Does the concat splice any field of the JSON body?
    fn covers_a_body_field(&self) -> bool {
        self.parts().any(|p| matches!(p, SignedPart::Value(RequestValue::BodyField(_))))
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
    /// Twilio, by its spec.
    Sha1,
    Sha512,
}

impl HmacAlgorithm {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
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
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
/// for this subscription (what `routeBy: subscription` matches on).
pub const MINTED_ID: &str = "subscriptionId";
/// The value name carrying the secret weft minted for this
/// subscription (what [`VerifyKind::TokenEcho`] compares against).
pub const MINTED_TOKEN: &str = "subscriptionToken";
/// The value name carrying the address the provider should post to.
pub const RECEIVER_URL: &str = "receiverUrl";
/// The capture name a subscribe call must use for the moment its
/// subscription dies, when it declares a renewal margin. RFC3339 or
/// epoch seconds (milliseconds are accepted too: Google answers
/// epoch millis).
pub const EXPIRES_AT: &str = "expiresAt";

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
    /// When the credential in `auth`/`values` stops being good (unix
    /// seconds, a refresh margin before its token expires), for a side
    /// that keeps it between uses: it resolves again before then. `None`
    /// for a credential that does not expire.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_unix: Option<i64>,
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
    /// "missing", which is the honest answer. A header field sent twice
    /// with two values is refused ([`read_path`]).
    pub fn named_event(&self, body: &Value, headers: &[(String, String)]) -> Result<Value, Ambiguous> {
        let mut out = serde_json::Map::with_capacity(self.fields.len());
        for (name, path) in &self.fields {
            if let Some(v) = read_path(body, headers, path)? {
                if !v.is_null() {
                    out.insert(name.clone(), v);
                }
            }
        }
        Ok(Value::Object(out))
    }

    /// The account identifier an incoming event names, per
    /// [`EventAccount::path`].
    pub fn account_of(&self, body: &Value, headers: &[(String, String)]) -> Result<Option<String>, Ambiguous> {
        Ok(read_path(body, headers, &self.account.path)?.map(|v| match v {
            Value::String(s) => s,
            other => other.to_string(),
        }))
    }

    /// The account a socket FRAME concerns, on a socket marked
    /// `multiAccount`: the topic's account path, read from the
    /// object the event sits in (`eventPath` minus its last segment;
    /// the frame itself when the path is flat or empty). A socket
    /// frame nests the push body inside its own envelope and
    /// `eventPath` already names that nesting, so the one account
    /// concept ([`EventsSpec::account`]) serves every surface an
    /// event arrives on.
    pub fn socket_frame_account(&self, frame: &Value) -> Option<String> {
        let socket = self.socket.as_ref()?;
        let container = match socket.event_path.rsplit_once('.') {
            Some((parent, _)) => super::spec::lookup_path(frame, parent)?,
            None => frame,
        };
        self.account_of(container, &[]).expect("a frame has no headers, so nothing it carries is read twice")
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
            if path.trim().is_empty() || header_name(path) == Some("") {
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
                     fix the spelling (the recipe's fields are connect, urlFrom, \
                     replies, eventPath, multiAccount)"
                ));
            }
            if socket.multi_account && header_name(&self.account.path).is_some() {
                return Err(format!(
                    "service '{service}' marks its socket multiAccount, but the account \
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
                 must say where a push carries it (`idPath`)"
            ));
        }
        // The token to compare against is the named subscription's, so a
        // push must name it, whatever the events are routed by.
        if matches!(self.verify, VerifyKind::TokenEcho) && self.id_path.is_none() {
            return Err(format!(
                "service '{service}' verifies pushes against the token weft minted for a \
                 subscription, so it must say where a push names that subscription (`idPath`)"
            ));
        }
        if matches!(self.verify, VerifyKind::TokenEcho) && self.token_path.is_none() {
            return Err(format!(
                "service '{service}' verifies pushes against the minted token, so it must \
                 say where a push carries it (`tokenPath`)"
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
        self.verify.validate(service, VerifySide::Push)?;
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

/// The placeholders a signed concat may interpolate, as the parse
/// error lists them; see [`SignedPart`].
pub const SIGNED_PARTS: [&str; 7] =
    ["body", "timestamp", "url", "method", "sorted_form_params", "header:<Name>", "body:<path>"];

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
/// names an HTTP header ([`header_value`]: case-insensitively, and sent
/// twice with two values it is [`Ambiguous`]), and anything else is a
/// dotted path into the body.
pub fn read_path(body: &Value, headers: &[(String, String)], path: &str) -> Result<Option<Value>, Ambiguous> {
    match header_name(path) {
        Some(name) => Ok(header_value(headers, name)?.map(|v| Value::String(v.to_string()))),
        None => Ok(super::spec::lookup_path(body, path).cloned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Each old spelling lands on the scheme that verifies the same
    /// pushes, and reads as one.
    #[test]
    fn a_version_one_scheme_becomes_the_same_scheme_at_version_two() {
        let cases = [
            (
                json!({ "kind": "hmac", "signatureHeader": "X-Slack-Signature",
                        "timestampHeader": "X-Slack-Request-Timestamp",
                        "concat": "v0:{timestamp}:{body}", "prefix": "v0=" }),
                json!({ "kind": "hmac",
                        "signature": { "in": "header:X-Slack-Signature", "pair": "=", "key": "v0" },
                        "timestamp": { "in": "header:X-Slack-Request-Timestamp" },
                        "concat": "v0:{timestamp}:{body}" }),
            ),
            (
                json!({ "kind": "hmac", "signatureHeader": "Stripe-Signature",
                        "packed": { "timestamp": "t", "signature": "v1" },
                        "concat": "{timestamp}.{body}" }),
                json!({ "kind": "hmac",
                        "signature": { "in": "header:Stripe-Signature", "list": ",", "pair": "=", "key": "v1" },
                        "timestamp": { "in": "signature:t" },
                        "concat": "{timestamp}.{body}" }),
            ),
            (
                json!({ "kind": "signature", "scheme": "ed25519", "signatureHeader": "X-Signature-Ed25519",
                        "timestampHeader": "X-Signature-Timestamp", "concat": "{timestamp}{body}" }),
                json!({ "kind": "signature", "scheme": "ed25519",
                        "signature": { "in": "header:X-Signature-Ed25519" },
                        "timestamp": { "in": "header:X-Signature-Timestamp" },
                        "concat": "{timestamp}{body}" }),
            ),
            (json!({ "kind": "token_echo" }), json!({ "kind": "token_echo" })),
        ];
        for (old, new) in cases {
            let mut upgraded = old.clone();
            verify_to_v2(&mut upgraded).unwrap();
            assert_eq!(upgraded, new, "{old}");
            let scheme: VerifyKind = serde_json::from_value(upgraded).unwrap();
            assert_eq!(serde_json::to_value(scheme).unwrap(), new);
        }
        let mut bare = json!({ "kind": "hmac", "signatureHeader": "X", "concat": "{body}", "prefix": "v0" });
        assert!(verify_to_v2(&mut bare).unwrap_err().contains("'v0' is not a key followed by one separator"));
    }

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
                               "value": "Bearer {appToken}" }],
                    "captures": [{ "name": "url", "path": "url" }]
                },
                "replies": [{ "whenField": "envelope_id",
                              "frame": "{\"envelope_id\":\"{value}\"}" }],
                "eventPath": "payload.event"
            },
            "webhook": {
                "handshake": { "kind": "echo_body_field", "path": "challenge" },
                "verify": { "kind": "hmac",
                            "signature": { "in": "header:X-Slack-Signature",
                                           "pair": "=", "key": "v0" },
                            "timestamp": { "in": "header:X-Slack-Request-Timestamp" },
                            "concat": "v0:{timestamp}:{body}" }
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
                "routeBy": "subscription",
                "idPath": "header:X-Goog-Channel-ID",
                "tokenPath": "header:X-Goog-Channel-Token",
                "subscribe": {
                    "subscribe": {
                        "url": "https://www.googleapis.com/drive/v3/files/{target}/watch",
                        "method": "POST",
                        "body": {
                            "id": "{subscriptionId}",
                            "type": "web_hook",
                            "address": "{receiverUrl}",
                            "token": "{subscriptionToken}"
                        },
                        "auth": [{ "kind": "header", "name": "Authorization",
                                   "value": "Bearer {token}" }],
                        "captures": [{ "name": "expiresAt", "path": "expiration" },
                                     { "name": "resourceId", "path": "resourceId" }]
                    },
                    "unsubscribe": {
                        "url": "https://www.googleapis.com/drive/v3/channels/stop",
                        "method": "POST",
                        "body": { "id": "{subscriptionId}", "resourceId": "{resourceId}" },
                        "auth": [{ "kind": "header", "name": "Authorization",
                                   "value": "Bearer {token}" }]
                    },
                    "renewMarginSecs": 3600
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
        let named = spec.named_event(&body, &[]).unwrap();
        assert_eq!(named["type"], "message");
        assert_eq!(named["channel"], "C42");
        assert_eq!(named["text"], "hi");
        assert!(named.get("ignored").is_none(), "only declared fields ride");
        assert!(named.get("bot").is_none(), "an absent field stays absent");
        assert!(named.get("thread").is_none(), "a null value reads as absent");
        assert_eq!(spec.account_of(&body, &[]).unwrap().as_deref(), Some("T9"));
    }

    /// A push whose data is entirely in headers (no body at all) is
    /// read the same way, through `header:` paths.
    #[test]
    fn header_paths_read_a_bodyless_push() {
        let spec = drive_events();
        let headers = vec![
            ("x-goog-resource-state".to_string(), "update".to_string()),
            ("X-Goog-Channel-Token".to_string(), "tok-1".to_string()),
        ];
        let named = spec.named_event(&Value::Null, &headers).unwrap();
        assert_eq!(named["state"], "update", "headers match case-insensitively");
        assert!(named.get("changed").is_none());
        assert_eq!(spec.account_of(&Value::Null, &headers).unwrap().as_deref(), Some("tok-1"));
    }

    /// A routing header sent twice is read by the same rule the verify
    /// schemes read headers by: the same value twice is one value, two
    /// different values are refused rather than the first one winning.
    #[test]
    fn a_repeated_routing_header_with_two_values_is_ambiguous() {
        let spec = drive_events();
        let pairs = |values: &[&str]| -> Vec<(String, String)> {
            values.iter().map(|v| ("X-Goog-Channel-Token".to_string(), v.to_string())).collect()
        };
        assert_eq!(spec.account_of(&Value::Null, &pairs(&["tok-1", "tok-1"])).unwrap().as_deref(), Some("tok-1"));
        let err = spec.account_of(&Value::Null, &pairs(&["tok-1", "tok-2"])).unwrap_err();
        assert_eq!(err, Ambiguous("X-Goog-Channel-Token header".into()));
        assert!(read_path(&Value::Null, &pairs(&["a", "b"]), "header:x-goog-channel-token").is_err());
        assert!(spec.named_event(&Value::Null, &[("x-goog-resource-state".into(), "a".into()), ("X-Goog-Resource-State".into(), "b".into())]).is_err());
    }

    /// A route gated by identity tokens reads an audience on every check,
    /// so its recipe must declare one, and not as optional.
    #[test]
    fn an_identity_token_gate_declares_a_required_audience() {
        let gate = |fields: Value| -> crate::AccessSpec {
            serde_json::from_value(json!({
                "service": "jwt",
                "acquisition": { "kind": "static", "fields": fields },
                "verify": { "kind": "oidc", "issuers": ["{issuer}"], "jwksUrl": "{jwksUrl}" }
            }))
            .unwrap()
        };
        gate(json!([{ "name": "issuer" }, { "name": "jwksUrl" }, { "name": "audience" }])).validate().expect("declared");
        let err = gate(json!([{ "name": "issuer" }, { "name": "jwksUrl" }])).validate().unwrap_err();
        assert!(err.contains("'audience'"), "{err}");
        let err = gate(json!([{ "name": "issuer" }, { "name": "jwksUrl" }, { "name": "audience", "optional": true }]))
            .validate()
            .unwrap_err();
        assert!(err.contains("'audience'") && err.contains("optional"), "{err}");
    }

    /// Signing keys come over TLS only, whatever case the scheme is in.
    #[test]
    fn a_jwks_address_must_be_https() {
        for good in ["https://issuer.example/jwks.json", "HTTPS://issuer.example/jwks.json", "https://localhost:8443/jwks.json"] {
            check_jwks_url(good).unwrap_or_else(|e| panic!("{good}: {e}"));
        }
        for bad in ["http://issuer.example/jwks.json", "ftp://x", "issuer.example", "", "https://"] {
            let err = check_jwks_url(bad).unwrap_err();
            assert!(err.contains("not an https://") && err.contains(&format!("'{bad}'")), "{bad}: {err}");
        }
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
        calls.subscribe.body = Some(json!({ "id": "{subscriptionId}" }));
        let err = spec.validate("google").unwrap_err();
        assert!(err.contains(RECEIVER_URL), "{err}");

        // A channel-token check whose subscribe never sends the token.
        let mut spec = drive_events();
        let calls = spec.webhook.as_mut().unwrap().subscribe.as_mut().unwrap();
        calls.subscribe.body = Some(json!({
            "id": "{subscriptionId}", "address": "{receiverUrl}"
        }));
        let err = spec.validate("google").unwrap_err();
        assert!(err.contains(MINTED_TOKEN), "{err}");

        // A token echo with no subscription id to find the token by,
        // even routed by account.
        let mut spec = drive_events();
        let webhook = spec.webhook.as_mut().unwrap();
        webhook.route_by = RouteBy::Account;
        webhook.id_path = None;
        let err = spec.validate("google").unwrap_err();
        assert!(err.contains("idPath"), "{err}");
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
        unsub.body = Some(json!({ "id": "{subscriptionId}", "address": "{receiverUrl}" }));
        let err = spec.validate("google").unwrap_err();
        assert!(err.contains(RECEIVER_URL) && err.contains("unsubscribe"), "{err}");

        // The minted id and token stay legal in a stop call.
        let mut spec = drive_events();
        let calls = spec.webhook.as_mut().unwrap().subscribe.as_mut().unwrap();
        let unsub = calls.unsubscribe.as_mut().unwrap();
        unsub.body = Some(json!({ "id": "{subscriptionId}", "token": "{subscriptionToken}" }));
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

    fn webhook_verify(verify: serde_json::Value) -> Result<(), String> {
        let spec: EventsSpec = serde_json::from_value(json!({
            "fields": { "kind": "kind" },
            "account": { "value": "user", "path": "account" },
            "webhook": { "verify": verify }
        }))
        .map_err(|e| e.to_string())?;
        spec.validate("svc")
    }

    /// The timestamp is all-in or all-out: a declared one the concat
    /// never signs is spoofable (refused), and a concat naming an
    /// undeclared one can never resolve (refused). A body-field
    /// timestamp is signed by a concat that signs the whole body
    /// (Linear's shape), and the timestampless body-only shape stays
    /// valid.
    #[test]
    fn the_hmac_timestamp_is_signed_or_absent() {
        let err = webhook_verify(json!({
            "kind": "hmac", "signature": { "in": "header:X-Sig" },
            "timestamp": { "in": "header:X-Ts" }, "concat": "{body}"
        }))
        .unwrap_err();
        assert!(err.contains("never signs"), "{err}");

        let err = webhook_verify(json!({
            "kind": "hmac", "signature": { "in": "header:X-Sig" },
            "concat": "v0:{timestamp}:{body}"
        }))
        .unwrap_err();
        assert!(err.contains("no timestamp source"), "{err}");

        webhook_verify(json!({
            "kind": "hmac", "signature": { "in": "header:X-Hub-Signature-256", "pair": "=", "key": "sha256" },
            "concat": "{body}", "encoding": "base64"
        }))
        .expect("the body-only shape is valid");

        webhook_verify(json!({
            "kind": "hmac", "signature": { "in": "header:Linear-Signature" },
            "timestamp": { "in": "body:webhookTimestamp", "format": "milliseconds", "toleranceSecs": 60 },
            "concat": "{body}"
        }))
        .expect("a body timestamp is signed with the whole body");

        webhook_verify(json!({
            "kind": "hmac", "signature": { "in": "header:X-Sig" },
            "timestamp": { "in": "header:X-Ts" }, "concat": "{header:x-ts}.{body}"
        }))
        .expect("signing the timestamp's own header signs it");
    }

    /// A scheme that signs only fields of the body (Mailgun's
    /// timestamp and token) is valid only with the timestamp that
    /// dates the signature; without one a captured signature would
    /// verify other contents forever.
    #[test]
    fn a_scheme_signing_only_body_fields_must_sign_a_timestamp() {
        webhook_verify(json!({
            "kind": "hmac", "signature": { "in": "body:signature.signature" },
            "timestamp": { "in": "body:signature.timestamp" },
            "concat": "{timestamp}{body:signature.token}"
        }))
        .expect("Mailgun's shape");

        let err = webhook_verify(json!({
            "kind": "hmac", "signature": { "in": "body:signature.signature" },
            "concat": "{body:signature.token}"
        }))
        .unwrap_err();
        assert!(err.contains("only fields of the body"), "{err}");

        let err = webhook_verify(json!({
            "kind": "hmac", "signature": { "in": "header:X-Sig" },
            "concat": "{method}{url}"
        }))
        .unwrap_err();
        assert!(err.contains("never covers the request"), "{err}");
    }

    /// The layout and the sources are held to a coherent shape where
    /// the recipe is written, each refusal naming the fix.
    #[test]
    fn an_incoherent_signature_layout_is_refused() {
        let refused = |verify: serde_json::Value, needle: &str| {
            let err = webhook_verify(verify).unwrap_err();
            assert!(err.contains(needle), "expected '{needle}' in: {err}");
        };
        refused(json!({ "kind": "hmac", "signature": { "in": "header:S", "pair": "=" }, "concat": "{body}" }), "no `key`");
        refused(json!({ "kind": "hmac", "signature": { "in": "header:S", "key": "v1" }, "concat": "{body}" }), "no `pair`");
        refused(
            json!({ "kind": "hmac", "signature": { "in": "header:S", "list": ",", "pair": "," , "key": "v1" }, "concat": "{body}" }),
            "the same",
        );
        refused(
            json!({ "kind": "hmac", "signature": { "in": "header:S" }, "timestamp": { "in": "signature:t" },
                    "concat": "{timestamp}.{body}" }),
            "not keyed",
        );
        refused(
            json!({ "kind": "hmac", "signature": { "in": "header:S", "pair": "=", "key": "t" },
                    "timestamp": { "in": "signature:t" }, "concat": "{timestamp}.{body}" }),
            "both 't'",
        );
        refused(json!({ "kind": "hmac", "signature": { "in": "header:S" }, "concat": "{header:s}{body}" }), "very part");
        refused(json!({ "kind": "hmac", "signature": { "in": "body:sig" }, "concat": "{body}" }), "very part");
        refused(
            json!({ "kind": "hmac", "signature": { "in": "body:s.sig" }, "timestamp": { "in": "body:s.ts" },
                    "concat": "{timestamp}{body:s.sig}" }),
            "very part",
        );
        refused(
            json!({ "kind": "hmac", "signature": { "in": "header:S" }, "timestamp": { "in": "header:T", "toleranceSecs": 0 },
                    "concat": "{timestamp}{body}" }),
            "zero",
        );
        // A source that names no part of the request never parses.
        refused(json!({ "kind": "hmac", "signature": { "in": "X-Sig" }, "concat": "{body}" }), "names no part");
        refused(json!({ "kind": "hmac", "signature": { "in": "signature:v1" }, "concat": "{body}" }), "its own layout");
    }

    /// The placeholders round-trip as the template string, and the
    /// parse knows each one.
    #[test]
    fn a_concat_names_headers_and_body_fields() {
        let concat: SignedConcat = "{header:webhook-id}.{timestamp}.{body}".parse().unwrap();
        assert!(concat.covers(&SignedPart::Value(RequestValue::Header("Webhook-Id".into()))));
        assert!(concat.covers(&SignedPart::Body));
        let concat: SignedConcat = "{timestamp}{body:signature.token}".parse().unwrap();
        assert!(concat.covers_a_body_field());
        assert!("{signature:t}{body}".parse::<SignedConcat>().is_err(), "a layout key is only ever {{timestamp}}");
        assert_eq!(serde_json::to_value(&concat).unwrap(), json!("{timestamp}{body:signature.token}"));
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
    /// `eventPath`, the frame itself for a flat one, and None when
    /// the frame carries nothing there.
    #[test]
    fn a_socket_frame_names_its_account_at_the_events_level() {
        let spec = slack_events();
        // eventPath is "payload.event": the account reads from
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

        // A flat eventPath reads the account off the frame itself.
        let mut flat = slack_events();
        flat.socket.as_mut().unwrap().event_path = "event".into();
        let frame = json!({ "team_id": "T3", "event": { "type": "message" } });
        assert_eq!(flat.socket_frame_account(&frame).as_deref(), Some("T3"));
    }
}
