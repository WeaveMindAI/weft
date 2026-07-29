//! Proving an inbound push genuinely came from the provider.
//!
//! Each scheme in [`VerifyKind`] is a PROTOCOL: the arithmetic (an
//! HMAC, a constant-time compare, an OIDC signature check) is
//! implemented here once, and everything provider-shaped about it
//! (which headers, what concatenation, which issuer) is data the
//! recipe declares. The SECRET material never lives in the recipe: it
//! is handed in per call by whoever holds it (the app the operator
//! registered, or the token weft minted when it subscribed).
//!
//! Everything here is PURE. The signed-identity scheme needs the
//! issuer's published keys, which is I/O: that fetch belongs to the
//! side holding the network, and this module owns the two halves it
//! can decide alone ([`needs_issuer_keys`] and
//! [`check_identity_claims`]).

use std::collections::BTreeMap;

use base64::Engine as _;
// Both signature crates re-export the same `signature` crate's
// Verifier trait, so this one import serves ed25519 and ECDSA.
use ed25519_dalek::Verifier as _;
use hmac::{Hmac, Mac};
use p256::pkcs8::DecodePublicKey as _;
use sha1::Sha1;
use sha2::Sha256;

use super::events::{
    DigestEncoding, HmacAlgorithm, SignatureScheme, SignedConcat, SignedPart, SignedSegment,
    VerifyKind,
};
use super::{hex_of, hex_to_bytes};

/// How far a signed timestamp may be from now before the request is
/// treated as a replay. Five minutes is the window the surveyed
/// schemes' own documentation specifies.
pub const TIMESTAMP_TOLERANCE_SECS: i64 = 60 * 5;

/// What one scheme needs beyond the request itself. Assembled by the
/// side that holds the material (the broker, from the receiving app's
/// configuration and from the subscription row), never by the caller
/// of the endpoint.
#[derive(Debug, Clone, Default)]
pub struct VerifySecrets {
    /// The shared secret an HMAC scheme keys on.
    pub signing_secret: Option<String>,
    /// The public key a signature scheme verifies against (not a
    /// secret, but configured in the same place: on the app that
    /// receives). Hex for ed25519, base64 DER for ecdsa_p256.
    pub public_key: Option<String>,
    /// The token weft minted when it subscribed, which a
    /// no-signature provider echoes back on every push.
    pub minted_token: Option<String>,
    /// Where that echoed token arrives, as a path in the events
    /// recipe's vocabulary (a `header:` path for every provider
    /// surveyed). Read off the push by the caller, which is the side
    /// holding the recipe.
    pub presented_token: Option<String>,
    /// The audience a signed identity token must name.
    pub audience: Option<String>,
    /// The issuer identity a signed identity token must carry
    /// (the account configured to push).
    pub expected_email: Option<String>,
}

/// Why a push was refused. Never echoed to the caller in detail (a
/// verifier that says WHICH part failed is an oracle); logged where
/// the operator reads and answered as a flat refusal.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum VerifyError {
    #[error("the push carries no {0} header")]
    MissingHeader(String),
    #[error("the push's signature does not match")]
    BadSignature,
    #[error("the push's timestamp is {0}s away from now, outside the replay window")]
    StaleTimestamp(i64),
    #[error("no {0} is configured for this service, so its pushes cannot be verified")]
    NotConfigured(&'static str),
    #[error("the push's identity token is not usable: {0}")]
    BadIdentityToken(String),
}

/// One raw push, as the schemes see it: the EXACT bytes received
/// (every signing scheme signs the bytes, so a parsed-and-reserialized
/// body would never match), the headers, and, for the schemes that
/// sign the request address (Twilio's), where the provider was told
/// to post and how it called.
#[derive(Debug, Clone, Copy)]
pub struct PushParts<'a> {
    pub body: &'a [u8],
    pub headers: &'a BTreeMap<String, String>,
    /// The public address the provider posts to; `None` when the
    /// receiver cannot know it (an address-signing scheme then
    /// refuses as unconfigured, never verifies blind).
    pub url: Option<&'a str>,
    pub method: &'a str,
}

/// Run the scheme a service declares against one raw push.
///
/// Pure. The signed-identity scheme needs the issuer's published
/// keys, so it is not decided here; see [`needs_issuer_keys`].
pub fn verify_push(
    kind: &VerifyKind,
    push: &PushParts<'_>,
    secrets: &VerifySecrets,
    now_unix: i64,
) -> Result<(), VerifyError> {
    match kind {
        VerifyKind::Hmac {
            signature_header,
            timestamp_header,
            packed,
            concat,
            prefix,
            algorithm,
            encoding,
        } => {
            let secret = secrets
                .signing_secret
                .as_deref()
                .ok_or(VerifyError::NotConfigured("signing secret"))?;
            let raw = header(push.headers, signature_header)
                .ok_or_else(|| VerifyError::MissingHeader(signature_header.clone()))?;
            // Where the timestamp and the digest(s) come from: their
            // own headers, or `k=v` pairs packed into the signature
            // header (Stripe's shape). Several pairs may carry the
            // digest key during a secret roll; any one matching
            // verifies.
            let (sent, candidates): (i64, Vec<String>) = match packed {
                Some(keys) => {
                    let pairs = packed_pairs(raw);
                    let ts = pairs
                        .iter()
                        .find(|(k, _)| k == &keys.timestamp)
                        .map(|(_, v)| v.as_str())
                        .ok_or_else(|| VerifyError::MissingHeader(keys.timestamp.clone()))?;
                    let sent = checked_timestamp(ts, &keys.timestamp, now_unix)?;
                    let sigs: Vec<String> = pairs
                        .into_iter()
                        .filter(|(k, _)| k == &keys.signature)
                        .map(|(_, v)| v)
                        .collect();
                    if sigs.is_empty() {
                        return Err(VerifyError::MissingHeader(keys.signature.clone()));
                    }
                    (sent, sigs)
                }
                None => {
                    // A provider that signs a timestamp gets it
                    // replay-checked; one that signs the body alone
                    // has no timestamp to check, and the recipe
                    // validation guarantees its concat never asks
                    // for one.
                    let sent = match timestamp_header {
                        Some(ts_header) => {
                            let ts = header(push.headers, ts_header)
                                .ok_or_else(|| VerifyError::MissingHeader(ts_header.clone()))?;
                            checked_timestamp(ts, ts_header, now_unix)?
                        }
                        None => 0,
                    };
                    (sent, vec![raw.trim().to_string()])
                }
            };
            let expected =
                hmac_signature(secret, concat, prefix, *algorithm, *encoding, sent, push)?;
            if candidates
                .iter()
                .any(|c| constant_time_eq(c.as_bytes(), expected.as_bytes()))
            {
                Ok(())
            } else {
                Err(VerifyError::BadSignature)
            }
        }
        VerifyKind::Signature { scheme, signature_header, timestamp_header, concat, encoding } => {
            let key = secrets
                .public_key
                .as_deref()
                .ok_or(VerifyError::NotConfigured("public key"))?;
            let presented = header(push.headers, signature_header)
                .ok_or_else(|| VerifyError::MissingHeader(signature_header.clone()))?;
            let sent = match timestamp_header {
                Some(ts_header) => {
                    let ts = header(push.headers, ts_header)
                        .ok_or_else(|| VerifyError::MissingHeader(ts_header.clone()))?;
                    checked_timestamp(ts, ts_header, now_unix)?
                }
                None => 0,
            };
            let signature = match encoding {
                DigestEncoding::Hex => hex_to_bytes(presented.trim())
                    .ok_or(VerifyError::BadSignature)?,
                DigestEncoding::Base64 => base64::engine::general_purpose::STANDARD
                    .decode(presented.trim())
                    .map_err(|_| VerifyError::BadSignature)?,
            };
            let message = signed_bytes(concat, sent, push)?;
            match scheme {
                SignatureScheme::Ed25519 => {
                    let key_bytes: [u8; 32] = hex_to_bytes(key.trim())
                        .and_then(|b| b.try_into().ok())
                        .ok_or(VerifyError::NotConfigured(
                            "usable public key (ed25519 keys are 32 hex-encoded bytes)",
                        ))?;
                    let verifier = ed25519_dalek::VerifyingKey::from_bytes(&key_bytes)
                        .map_err(|_| {
                            VerifyError::NotConfigured("usable public key (not an ed25519 point)")
                        })?;
                    let sig_bytes: [u8; 64] = signature
                        .try_into()
                        .map_err(|_| VerifyError::BadSignature)?;
                    verifier
                        .verify(&message, &ed25519_dalek::Signature::from_bytes(&sig_bytes))
                        .map_err(|_| VerifyError::BadSignature)
                }
                SignatureScheme::EcdsaP256 => {
                    let der = base64::engine::general_purpose::STANDARD
                        .decode(key.trim())
                        .map_err(|_| {
                            VerifyError::NotConfigured(
                                "usable public key (ecdsa_p256 keys are base64 DER)",
                            )
                        })?;
                    let verifier = p256::ecdsa::VerifyingKey::from_public_key_der(&der)
                        .map_err(|_| {
                            VerifyError::NotConfigured("usable public key (not a P-256 key)")
                        })?;
                    let sig = p256::ecdsa::Signature::from_der(&signature)
                        .map_err(|_| VerifyError::BadSignature)?;
                    verifier.verify(&message, &sig).map_err(|_| VerifyError::BadSignature)
                }
            }
        }
        VerifyKind::TokenEcho => {
            // The provider signs nothing; it echoes back the secret
            // weft minted when it asked for the subscription. That
            // token is per-subscription and unguessable, so matching
            // it is the whole proof.
            let minted = secrets
                .minted_token
                .as_deref()
                .ok_or(VerifyError::NotConfigured("subscription token"))?;
            let presented = secrets
                .presented_token
                .as_deref()
                .ok_or_else(|| VerifyError::MissingHeader("subscription token".into()))?;
            if constant_time_eq(presented.as_bytes(), minted.as_bytes()) {
                Ok(())
            } else {
                Err(VerifyError::BadSignature)
            }
        }
        VerifyKind::Oidc { .. } => Err(VerifyError::NotConfigured(
            "signed identity tokens are verified against the issuer's published keys; \
             use the async identity-token path",
        )),
    }
}

/// The exact bytes a signing scheme covers: the declared `concat`
/// template resolved against the push, splicing the body as BYTES
/// (never parsed) and the other parts as text. The one resolver
/// behind both the HMAC and the public-key schemes; pure and
/// separately testable against a scheme's documented example.
pub fn signed_bytes(
    concat: &SignedConcat,
    timestamp: i64,
    push: &PushParts<'_>,
) -> Result<Vec<u8>, VerifyError> {
    let ts = timestamp.to_string();
    let mut out = Vec::with_capacity(concat.as_str().len() + push.body.len());
    for segment in concat.segments() {
        match segment {
            SignedSegment::Literal(text) => out.extend_from_slice(text.as_bytes()),
            SignedSegment::Part(SignedPart::Timestamp) => out.extend_from_slice(ts.as_bytes()),
            SignedSegment::Part(SignedPart::Body) => out.extend_from_slice(push.body),
            SignedSegment::Part(SignedPart::Method) => {
                out.extend_from_slice(push.method.as_bytes())
            }
            SignedSegment::Part(SignedPart::Url) => {
                let url = push
                    .url
                    .ok_or(VerifyError::NotConfigured("public receiver address"))?;
                out.extend_from_slice(url.as_bytes());
            }
            SignedSegment::Part(SignedPart::SortedFormParams) => {
                out.extend_from_slice(sorted_form_params(push.body).as_bytes())
            }
        }
    }
    Ok(out)
}

/// The form-encoded body's pairs, sorted by name (then value),
/// concatenated as name+value with nothing between: the rule
/// Twilio-class schemes sign POST parameters by.
fn sorted_form_params(body: &[u8]) -> String {
    let mut pairs: Vec<(String, String)> = url::form_urlencoded::parse(body)
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    pairs.sort();
    let mut out = String::new();
    for (k, v) in pairs {
        out.push_str(&k);
        out.push_str(&v);
    }
    out
}

/// The signature an HMAC-scheme push should carry: the declared hash
/// over [`signed_bytes`], written in the declared encoding, prefixed
/// with the declared prefix.
pub fn hmac_signature(
    secret: &str,
    concat: &SignedConcat,
    prefix: &str,
    algorithm: HmacAlgorithm,
    encoding: DigestEncoding,
    timestamp: i64,
    push: &PushParts<'_>,
) -> Result<String, VerifyError> {
    let message = signed_bytes(concat, timestamp, push)?;
    let digest: Vec<u8> = match algorithm {
        HmacAlgorithm::Sha256 => {
            let mut mac = <Hmac<Sha256>>::new_from_slice(secret.as_bytes())
                .expect("hmac accepts a key of any length");
            mac.update(&message);
            mac.finalize().into_bytes().to_vec()
        }
        HmacAlgorithm::Sha1 => {
            let mut mac = <Hmac<Sha1>>::new_from_slice(secret.as_bytes())
                .expect("hmac accepts a key of any length");
            mac.update(&message);
            mac.finalize().into_bytes().to_vec()
        }
    };
    let written = match encoding {
        DigestEncoding::Hex => hex_of(&digest),
        DigestEncoding::Base64 => base64::engine::general_purpose::STANDARD.encode(digest),
    };
    Ok(format!("{prefix}{written}"))
}

/// A packed signature header's `k=v` pairs (`t=1699…,v1=5257a…`),
/// comma-separated, whitespace-tolerant. Pairs without a `=` are
/// dropped (nothing they could mean).
fn packed_pairs(raw: &str) -> Vec<(String, String)> {
    raw.split(',')
        .filter_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            Some((k.trim().to_string(), v.trim().to_string()))
        })
        .collect()
}

/// Parse and replay-check a signed timestamp: within the tolerance
/// window of now, or refused.
fn checked_timestamp(raw: &str, name: &str, now_unix: i64) -> Result<i64, VerifyError> {
    let sent: i64 = raw
        .trim()
        .parse()
        .map_err(|_| VerifyError::MissingHeader(name.to_string()))?;
    let drift = (now_unix - sent).abs();
    if drift > TIMESTAMP_TOLERANCE_SECS {
        return Err(VerifyError::StaleTimestamp(drift));
    }
    Ok(sent)
}

/// Whether a scheme is verified against keys fetched from the issuer
/// rather than against material we already hold. Those take a
/// different (async) path; everything else is [`verify_push`].
pub fn needs_issuer_keys(kind: &VerifyKind) -> bool {
    matches!(kind, VerifyKind::Oidc { .. })
}

/// The claims a signed identity token must satisfy, checked after its
/// signature. Pure, so the policy is testable without any key at all.
pub fn check_identity_claims(
    claims: &IdentityClaims,
    secrets: &VerifySecrets,
) -> Result<(), VerifyError> {
    let expected_audience = secrets
        .audience
        .as_deref()
        .ok_or(VerifyError::NotConfigured("push audience"))?;
    if claims.audience != expected_audience {
        return Err(VerifyError::BadIdentityToken(format!(
            "it names the audience '{}', not this receiver",
            claims.audience
        )));
    }
    // An unverified address proves nothing about who is pushing.
    if !claims.email_verified {
        return Err(VerifyError::BadIdentityToken(
            "its issuing identity is not verified".into(),
        ));
    }
    if let Some(expected) = secrets.expected_email.as_deref() {
        if claims.email.as_deref() != Some(expected) {
            return Err(VerifyError::BadIdentityToken(
                "it was issued to a different identity than the one configured to push".into(),
            ));
        }
    }
    Ok(())
}

/// The claims read off a signed identity token.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IdentityClaims {
    #[serde(rename = "aud")]
    pub audience: String,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub email_verified: bool,
    #[serde(rename = "exp")]
    pub expires_at: i64,
}

/// Constant-time byte comparison: a signature check that returns
/// early on the first differing byte leaks the correct prefix through
/// timing.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Read a header case-insensitively, as HTTP headers are.
fn header<'h>(headers: &'h BTreeMap<String, String>, name: &str) -> Option<&'h str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

/// The Authorization header's bearer value, which is where a signed
/// identity token rides.
pub fn bearer_token(headers: &BTreeMap<String, String>) -> Option<&str> {
    let raw = header(headers, "Authorization")?;
    raw.strip_prefix("Bearer ").or_else(|| raw.strip_prefix("bearer ")).map(str::trim)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secrets(signing: &str) -> VerifySecrets {
        VerifySecrets {
            signing_secret: Some(signing.into()),
            ..Default::default()
        }
    }

    fn parts<'a>(body: &'a [u8], headers: &'a BTreeMap<String, String>) -> PushParts<'a> {
        PushParts { body, headers, url: None, method: "POST" }
    }

    /// Slack's scheme, declared as data: the parameterization every
    /// hmac test below runs against.
    fn slack_kind() -> VerifyKind {
        VerifyKind::Hmac {
            signature_header: "X-Slack-Signature".into(),
            timestamp_header: Some("X-Slack-Request-Timestamp".into()),
            packed: None,
            concat: "v0:{timestamp}:{body}".parse().unwrap(),
            prefix: "v0=".into(),
            algorithm: HmacAlgorithm::Sha256,
            encoding: DigestEncoding::Hex,
        }
    }

    fn hmac_of(secret: &str, concat: &str, prefix: &str, encoding: DigestEncoding, timestamp: i64, body: &[u8]) -> String {
        let empty = BTreeMap::new();
        hmac_signature(
            secret,
            &concat.parse().unwrap(),
            prefix,
            HmacAlgorithm::Sha256,
            encoding,
            timestamp,
            &parts(body, &empty),
        )
        .expect("no unresolvable part in these tests")
    }

    fn slack_signature(secret: &str, timestamp: i64, body: &[u8]) -> String {
        hmac_of(secret, "v0:{timestamp}:{body}", "v0=", DigestEncoding::Hex, timestamp, body)
    }

    /// Slack's documented worked example: this exact secret, timestamp
    /// and body produce this exact signature THROUGH THE GENERIC
    /// TEMPLATE. If the interpolation ever drifts (a delimiter, the
    /// prefix, a reserialized body), this is what catches it.
    #[test]
    fn the_slack_parameterization_matches_its_documented_example() {
        let secret = "8f742231b10e8888abcd99yyyzzz85a5";
        let timestamp = 1531420618;
        let body = b"token=xyzz0WbapA4vBCDEFasx0q6G&team_id=T1DC2JH3J&team_domain=testteamnow&channel_id=G8PSS9T3V&channel_name=foobar&user_id=U2CERLKJA&user_name=roadrunner&command=%2Fwebhook-collect&text=&response_url=https%3A%2F%2Fhooks.slack.com%2Fcommands%2FT1DC2JH3J%2F397700885554%2F96rGlfmibIGlgcZRskXaIFfN&trigger_id=398738663015.47445629121.803a0bc887a14d10d2c447fce8b6703c";
        assert_eq!(
            slack_signature(secret, timestamp, body),
            "v0=a2114d57b48eac39b9ad189dd8316235a7b4a8d21a10bd27519666489c69b503"
        );
    }

    #[test]
    fn a_genuine_push_verifies_and_a_tampered_one_does_not() {
        let secret = "topsecret";
        let now = 1_700_000_000;
        let body = br#"{"event":{"type":"message"}}"#;
        let headers = BTreeMap::from([
            ("X-Slack-Request-Timestamp".to_string(), now.to_string()),
            ("X-Slack-Signature".to_string(), slack_signature(secret, now, body)),
        ]);
        verify_push(&slack_kind(), &parts(body, &headers),&secrets(secret), now)
            .expect("a genuine push verifies");

        // One byte of the body changed: the signature no longer covers it.
        let tampered = br#"{"event":{"type":"messagE"}}"#;
        assert_eq!(
            verify_push(&slack_kind(), &parts(tampered, &headers), &secrets(secret), now),
            Err(VerifyError::BadSignature)
        );

        // The right body under the wrong secret.
        assert_eq!(
            verify_push(&slack_kind(), &parts(body, &headers),&secrets("other"), now),
            Err(VerifyError::BadSignature)
        );
    }

    /// A replayed push (a genuine signature from long ago) is refused
    /// on the timestamp, which is the whole reason the timestamp is
    /// inside the signed string.
    #[test]
    fn a_replayed_push_is_refused_on_its_age() {
        let secret = "topsecret";
        let signed_at = 1_700_000_000;
        let body = b"{}";
        let headers = BTreeMap::from([
            ("X-Slack-Request-Timestamp".to_string(), signed_at.to_string()),
            ("X-Slack-Signature".to_string(), slack_signature(secret, signed_at, body)),
        ]);
        let much_later = signed_at + TIMESTAMP_TOLERANCE_SECS + 1;
        assert!(matches!(
            verify_push(&slack_kind(), &parts(body, &headers),&secrets(secret), much_later),
            Err(VerifyError::StaleTimestamp(_))
        ));
        // Inside the window it still verifies.
        verify_push(
            &slack_kind(),
            &parts(body, &headers),
            &secrets(secret),
            signed_at + TIMESTAMP_TOLERANCE_SECS,
        )
        .expect("inside the window");
    }

    /// Headers are read case-insensitively, and a push missing either
    /// header is refused by name rather than silently failing the
    /// comparison.
    #[test]
    fn missing_or_oddly_cased_headers_are_handled() {
        let secret = "s";
        let now = 1_700_000_000;
        let body = b"{}";
        let lowercased = BTreeMap::from([
            ("x-slack-request-timestamp".to_string(), now.to_string()),
            ("x-slack-signature".to_string(), slack_signature(secret, now, body)),
        ]);
        verify_push(&slack_kind(), &parts(body, &lowercased), &secrets(secret), now)
            .expect("headers are case-insensitive");

        let empty = BTreeMap::new();
        assert_eq!(
            verify_push(&slack_kind(), &parts(body, &empty), &secrets(secret), now),
            Err(VerifyError::MissingHeader("X-Slack-Signature".into()))
        );
    }

    /// With no secret configured the answer is "cannot verify", never
    /// "verified": an unconfigured receiver must not accept pushes.
    #[test]
    fn an_unconfigured_receiver_refuses_rather_than_accepts() {
        let now = 1_700_000_000;
        let empty = BTreeMap::new();
        let err = verify_push(
            &slack_kind(),
            &parts(b"{}", &empty),
            &VerifySecrets::default(),
            now,
        )
        .unwrap_err();
        assert!(matches!(err, VerifyError::NotConfigured(_)), "{err}");
    }

    /// A timestampless hmac scheme (GitHub / Shopify shape: the body
    /// alone is signed): verifies with no timestamp header in sight,
    /// still refuses a tampered body, and writes base64 when asked.
    #[test]
    fn a_body_only_hmac_scheme_needs_no_timestamp() {
        let secret = "gh-secret";
        let body = br#"{"action":"opened"}"#;
        let github = VerifyKind::Hmac {
            signature_header: "X-Hub-Signature-256".into(),
            timestamp_header: None,
            packed: None,
            concat: "{body}".parse().unwrap(),
            prefix: "sha256=".into(),
            algorithm: HmacAlgorithm::Sha256,
            encoding: DigestEncoding::Hex,
        };
        let sig = hmac_of(secret, "{body}", "sha256=", DigestEncoding::Hex, 0, body);
        let headers = BTreeMap::from([("X-Hub-Signature-256".to_string(), sig)]);
        verify_push(&github, &parts(body, &headers), &secrets(secret), 1_700_000_000)
            .expect("a body-only signature verifies with no timestamp");
        assert_eq!(
            verify_push(&github, &parts(b"{}", &headers), &secrets(secret), 1_700_000_000),
            Err(VerifyError::BadSignature)
        );

        // The base64 flavor (Shopify shape) round-trips too.
        let shopify = VerifyKind::Hmac {
            signature_header: "X-Shopify-Hmac-Sha256".into(),
            timestamp_header: None,
            packed: None,
            concat: "{body}".parse().unwrap(),
            prefix: String::new(),
            algorithm: HmacAlgorithm::Sha256,
            encoding: DigestEncoding::Base64,
        };
        let sig = hmac_of(secret, "{body}", "", DigestEncoding::Base64, 0, body);
        assert!(!sig.chars().all(|c| c.is_ascii_hexdigit()), "base64, not hex: {sig}");
        let headers = BTreeMap::from([("X-Shopify-Hmac-Sha256".to_string(), sig)]);
        verify_push(&shopify, &parts(body, &headers), &secrets(secret), 0)
            .expect("base64 verifies");
    }

    /// The packed-header shape (Stripe's `t=…,v1=…`): the timestamp
    /// and digest come out of the signature header's own pairs, a
    /// second digest pair (a rolled secret's overlap) still verifies,
    /// and the packed timestamp is replay-checked like a header one.
    #[test]
    fn a_packed_signature_header_carries_its_own_timestamp() {
        let secret = "whsec_test";
        let body = br#"{"id":"evt_1"}"#;
        let now = 1_700_000_000;
        let stripe = VerifyKind::Hmac {
            signature_header: "Stripe-Signature".into(),
            timestamp_header: None,
            packed: Some(super::super::events::PackedSignature {
                timestamp: "t".into(),
                signature: "v1".into(),
            }),
            concat: "{timestamp}.{body}".parse().unwrap(),
            prefix: String::new(),
            algorithm: HmacAlgorithm::Sha256,
            encoding: DigestEncoding::Hex,
        };
        let sig = hmac_of(secret, "{timestamp}.{body}", "", DigestEncoding::Hex, now, body);
        let headers = BTreeMap::from([(
            "Stripe-Signature".to_string(),
            format!("t={now},v1=deadbeef,v1={sig}"),
        )]);
        verify_push(&stripe, &parts(body, &headers), &secrets(secret), now)
            .expect("any matching digest pair verifies");

        let stale = BTreeMap::from([(
            "Stripe-Signature".to_string(),
            format!("t={},v1={sig}", now - TIMESTAMP_TOLERANCE_SECS - 10),
        )]);
        assert!(matches!(
            verify_push(&stripe, &parts(body, &stale), &secrets(secret), now),
            Err(VerifyError::StaleTimestamp(_))
        ));

        let wrong = BTreeMap::from([(
            "Stripe-Signature".to_string(),
            format!("t={now},v1=deadbeef"),
        )]);
        assert_eq!(
            verify_push(&stripe, &parts(body, &wrong), &secrets(secret), now),
            Err(VerifyError::BadSignature)
        );
    }

    /// The Twilio concatenation rule, pinned on the signed BYTES: the
    /// posted form's params sorted by name, appended name+value to
    /// the address the provider was told to post to.
    #[test]
    fn the_twilio_concatenation_sorts_the_form_params_onto_the_url() {
        let body = b"To=%2B15551234&Body=hello%20there&From=%2B15559876";
        let headers = BTreeMap::new();
        let push = PushParts {
            body,
            headers: &headers,
            url: Some("https://weft.example/events/twilio/sms"),
            method: "POST",
        };
        let signed = signed_bytes(&"{url}{sorted_form_params}".parse().unwrap(), 0,&push).unwrap();
        assert_eq!(
            String::from_utf8(signed).unwrap(),
            "https://weft.example/events/twilio/smsBodyhello thereFrom+15559876To+15551234"
        );

        // An address-signing scheme with no known address refuses as
        // unconfigured, never verifies blind.
        let blind = PushParts { body, headers: &headers, url: None, method: "POST" };
        assert!(matches!(
            signed_bytes(&"{url}{sorted_form_params}".parse().unwrap(), 0,&blind),
            Err(VerifyError::NotConfigured(_))
        ));

        // The sha1 + base64 pipeline runs end to end on it.
        let twilio = VerifyKind::Hmac {
            signature_header: "X-Twilio-Signature".into(),
            timestamp_header: None,
            packed: None,
            concat: "{url}{sorted_form_params}".parse().unwrap(),
            prefix: String::new(),
            algorithm: HmacAlgorithm::Sha1,
            encoding: DigestEncoding::Base64,
        };
        let sig = hmac_signature(
            "auth-token",
            &"{url}{sorted_form_params}".parse().unwrap(),
            "",
            HmacAlgorithm::Sha1,
            DigestEncoding::Base64,
            0,
            &push,
        )
        .unwrap();
        let signed_headers = BTreeMap::from([("X-Twilio-Signature".to_string(), sig)]);
        let signed_push = PushParts {
            body,
            headers: &signed_headers,
            url: Some("https://weft.example/events/twilio/sms"),
            method: "POST",
        };
        verify_push(&twilio, &signed_push, &secrets("auth-token"), 0)
            .expect("the sha1/base64 flavor verifies");
    }

    /// The public-key schemes: a genuine ed25519 signature (Discord's
    /// shape) and a genuine ECDSA P-256 one (SendGrid's shape) verify
    /// against the configured public key; a tampered body does not,
    /// and no configured key refuses rather than accepts.
    #[test]
    fn the_public_key_schemes_verify_and_refuse() {
        // One Signer import serves both crates (the shared trait).
        use ed25519_dalek::Signer as _;

        let body = br#"{"type":1}"#;
        let now = 1_700_000_000;

        // Ed25519 (Discord): hex key, hex signature over {timestamp}{body}.
        let signing = ed25519_dalek::SigningKey::from_bytes(&[7u8; 32]);
        let message = [now.to_string().as_bytes(), body.as_slice()].concat();
        let sig = signing.sign(&message);
        let kind = VerifyKind::Signature {
            scheme: SignatureScheme::Ed25519,
            signature_header: "X-Signature-Ed25519".into(),
            timestamp_header: Some("X-Signature-Timestamp".into()),
            concat: "{timestamp}{body}".parse().unwrap(),
            encoding: DigestEncoding::Hex,
        };
        let headers = BTreeMap::from([
            ("X-Signature-Ed25519".to_string(), hex_of(&sig.to_bytes())),
            ("X-Signature-Timestamp".to_string(), now.to_string()),
        ]);
        let key = VerifySecrets {
            public_key: Some(hex_of(signing.verifying_key().as_bytes())),
            ..Default::default()
        };
        verify_push(&kind, &parts(body, &headers), &key, now).expect("a genuine ed25519 push");
        assert_eq!(
            verify_push(&kind, &parts(b"{}", &headers), &key, now),
            Err(VerifyError::BadSignature)
        );
        assert!(matches!(
            verify_push(&kind, &parts(body, &headers), &VerifySecrets::default(), now),
            Err(VerifyError::NotConfigured(_))
        ));

        // ECDSA P-256 (SendGrid): base64 DER key, base64 DER signature.
        let signing = p256::ecdsa::SigningKey::from_bytes((&[9u8; 32]).into()).unwrap();
        let der_sig: p256::ecdsa::DerSignature = signing.sign(&message);
        let kind = VerifyKind::Signature {
            scheme: SignatureScheme::EcdsaP256,
            signature_header: "X-Twilio-Email-Event-Webhook-Signature".into(),
            timestamp_header: Some("X-Twilio-Email-Event-Webhook-Timestamp".into()),
            concat: "{timestamp}{body}".parse().unwrap(),
            encoding: DigestEncoding::Base64,
        };
        let headers = BTreeMap::from([
            (
                "X-Twilio-Email-Event-Webhook-Signature".to_string(),
                base64::engine::general_purpose::STANDARD.encode(der_sig.as_bytes()),
            ),
            ("X-Twilio-Email-Event-Webhook-Timestamp".to_string(), now.to_string()),
        ]);
        let spki = p256::pkcs8::EncodePublicKey::to_public_key_der(signing.verifying_key())
            .unwrap();
        let key = VerifySecrets {
            public_key: Some(base64::engine::general_purpose::STANDARD.encode(spki.as_bytes())),
            ..Default::default()
        };
        verify_push(&kind, &parts(body, &headers), &key, now).expect("a genuine ecdsa push");
        assert_eq!(
            verify_push(&kind, &parts(b"{}", &headers), &key, now),
            Err(VerifyError::BadSignature)
        );
    }

    /// The no-signature scheme compares the echoed token against the
    /// one weft minted, in constant time.
    #[test]
    fn the_minted_token_scheme_compares_what_was_echoed() {
        let secrets = VerifySecrets {
            presented_token: Some("echoed-token".into()),
            minted_token: Some("echoed-token".into()),
            ..Default::default()
        };
        verify_push(&VerifyKind::TokenEcho, &parts(b"", &BTreeMap::new()), &secrets, 0)
            .expect("the echoed token matches what we minted");

        let wrong = VerifySecrets {
            presented_token: Some("guessed".into()),
            minted_token: Some("echoed-token".into()),
            ..Default::default()
        };
        assert_eq!(
            verify_push(&VerifyKind::TokenEcho, &parts(b"", &BTreeMap::new()), &wrong, 0),
            Err(VerifyError::BadSignature)
        );

        // A subscription we never minted a token for cannot verify.
        let unminted = VerifySecrets {
            presented_token: Some("anything".into()),
            ..Default::default()
        };
        assert!(matches!(
            verify_push(&VerifyKind::TokenEcho, &parts(b"", &BTreeMap::new()), &unminted, 0),
            Err(VerifyError::NotConfigured(_))
        ));
    }

    /// The identity-token claim policy, independent of any signature:
    /// the audience must be ours, the identity must be verified, and
    /// (when configured) it must be the identity we expect.
    #[test]
    fn identity_claims_are_checked_against_the_configured_receiver() {
        let claims = |aud: &str, verified: bool, email: &str| IdentityClaims {
            audience: aud.into(),
            email: Some(email.into()),
            email_verified: verified,
            expires_at: 0,
        };
        let secrets = VerifySecrets {
            audience: Some("https://weft.example/events/google".into()),
            expected_email: Some("pusher@project.iam.gserviceaccount.com".into()),
            ..Default::default()
        };
        check_identity_claims(
            &claims("https://weft.example/events/google", true, "pusher@project.iam.gserviceaccount.com"),
            &secrets,
        )
        .expect("our audience, verified, the configured identity");

        let err = check_identity_claims(
            &claims("https://somebody-else/events", true, "pusher@project.iam.gserviceaccount.com"),
            &secrets,
        )
        .unwrap_err();
        assert!(format!("{err}").contains("audience"), "{err}");

        let err = check_identity_claims(
            &claims("https://weft.example/events/google", false, "pusher@project.iam.gserviceaccount.com"),
            &secrets,
        )
        .unwrap_err();
        assert!(format!("{err}").contains("not verified"), "{err}");

        let err = check_identity_claims(
            &claims("https://weft.example/events/google", true, "someone@else.com"),
            &secrets,
        )
        .unwrap_err();
        assert!(format!("{err}").contains("different identity"), "{err}");
    }

    #[test]
    fn the_bearer_token_is_read_off_the_authorization_header() {
        let headers = BTreeMap::from([("authorization".to_string(), "Bearer abc.def.ghi".to_string())]);
        assert_eq!(bearer_token(&headers), Some("abc.def.ghi"));
        assert_eq!(bearer_token(&BTreeMap::new()), None);
        let basic = BTreeMap::from([("Authorization".to_string(), "Basic zzz".to_string())]);
        assert_eq!(bearer_token(&basic), None);
    }
}
