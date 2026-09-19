//! Signed routing token for a live caller connection. The dispatcher
//! mints one at the control handshake (after auth + ensuring the worker
//! is up); the gateway forwards it to the worker; the worker verifies it
//! before asking for the execution and attaching the connection to it. A
//! worker rejects any connection whose token is missing, expired, forged,
//! or addressed to a different pod, so a worker that stays cluster-private
//! only ever serves connections the dispatcher signed.
//!
//! The token is also the handshake's memory: nothing is born at the
//! handshake (a caller who never follows the redirect leaves nothing
//! behind), so what the birth needs and the arriving request cannot
//! supply rides in the claims: the route matched, the gate's verdict,
//! the path captures. The request itself (method, headers, query) is
//! read again from the caller when they arrive.
//!
//! The crypto + wire format live ONCE in [`crate::signed_token`] (HMAC-SHA256
//! over a base64url JSON payload, `v1.<payload>.<sig>`); this module is just
//! the routing-token CLAIMS plus thin typed wrappers. The storage download
//! capability is the same machinery with different claims. Kept here in core
//! (not the dispatcher) because both the dispatcher (mint) and the
//! engine/worker (verify) need it and both depend on core.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::signed_token::{self, SignedClaims};
use crate::Color;

/// Caller-safe noun for this token's error strings (no secret leak).
const NOUN: &str = "routing token";

/// What a routing token grants: the right to have execution `color` of
/// `project_id` born on the worker pod `pod_name` and to attach ONE live
/// connection to it, until `exp` (unix seconds).
///
/// `pod_name` is the pin: a held connection lives on exactly one pod for
/// its life, and the worker rejects a token addressed to a different pod.
/// `color` binds the connection to the one execution so `ctx.caller()`
/// resolves to the right run. `signal`, `path`, `params` and `caller`
/// are what the handshake established and the birth reads back when the
/// caller arrives (see the module doc).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallerTokenClaims {
    pub color: Color,
    pub project_id: String,
    pub pod_name: String,
    /// The signal token of the route the handshake matched: the row the
    /// birth reads the trigger, its spec and its armed program from.
    pub signal: String,
    /// The path as the route sees it (the tenant segment off, no
    /// leading slash), and the route pattern's captures for it.
    pub path: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, String>,
    /// The identity the gate established at the handshake (`None` on an
    /// open route).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caller: Option<Value>,
    /// A fingerprint of the request the gate approved, present only
    /// when something was checked (an open route approves nothing, so
    /// there is nothing to hold the caller to).
    ///
    /// Without it the door says "this is Alice" and nothing about what
    /// Alice asked for, so a caller could pass the gate with one
    /// request and then send a different one through the door they were
    /// given. For a password-shaped check that costs nothing, since the
    /// answer really was only about who they are. For a SIGNATURE it
    /// costs everything: a signature's whole claim is about one exact
    /// request, and handing out a door that any request fits throws
    /// that claim away.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approved: Option<RequestFingerprint>,
    pub exp: i64,
}

/// What the gate saw, small enough to sign and exact enough to hold a
/// caller to. The body is a hash rather than the body, so the door stays
/// a token rather than a copy of the request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestFingerprint {
    pub method: String,
    pub path: String,
    /// The query string as the gate received it, hashed.
    pub query_sha256: String,
    /// The body as the gate received it, hashed. Empty body hashes like
    /// any other, so "sent nothing" and "sent something" are different
    /// fingerprints rather than both being absent.
    pub body_sha256: String,
}

impl RequestFingerprint {
    /// Take the fingerprint of a request. One function, called by the
    /// gate when it approves and by the worker when the caller arrives,
    /// so the two can never disagree about what they are comparing.
    pub fn of(method: &str, path: &str, query: &str, body: &[u8]) -> Self {
        Self {
            method: method.to_ascii_uppercase(),
            path: path.trim_start_matches('/').to_string(),
            query_sha256: sha256_hex(query.as_bytes()),
            body_sha256: sha256_hex(body),
        }
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    format!("{:x}", h.finalize())
}

impl SignedClaims for CallerTokenClaims {
    fn exp(&self) -> i64 {
        self.exp
    }
}

/// Mint a signed routing token. `secret` is the cluster's dispatcher
/// signing key (same provisioning path as the broker / storage HMAC
/// secrets).
pub fn mint(secret: &[u8], claims: &CallerTokenClaims) -> String {
    signed_token::mint(secret, claims)
}

/// Validate a routing token and return its claims. Rejects on format,
/// signature, and expiry; reasons are caller-safe (no secret leak). The
/// worker additionally checks `claims.pod_name == own_pod` and
/// `claims.color` resolves to a live execution; those are policy checks
/// on top of this cryptographic validation, not part of it.
pub fn validate(secret: &[u8], token: &str, now_unix: i64) -> Result<CallerTokenClaims, String> {
    signed_token::validate(secret, token, now_unix, NOUN)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    const SECRET: &[u8] = b"test-secret-32-bytes-aaaaaaaaaaa";

    fn color() -> Color {
        Uuid::from_u128(0x1234)
    }

    fn claims(pod_name: &str, exp: i64) -> CallerTokenClaims {
        CallerTokenClaims {
            color: color(),
            project_id: "proj-1".into(),
            pod_name: pod_name.into(),
            signal: "sig-9".into(),
            path: "chat/room7".into(),
            params: [("room".to_string(), "room7".to_string())].into_iter().collect(),
            caller: Some(serde_json::json!({ "sub": "ada" })),
            approved: Some(RequestFingerprint::of("post", "/chat/room7", "a=1", b"{}")),
            exp,
        }
    }

    /// The fingerprint is what holds a caller to the request the gate
    /// approved, so it has to answer the same for the same request and
    /// differently for anything else about it.
    #[test]
    fn a_fingerprint_pins_the_request_that_was_approved() {
        let approved = RequestFingerprint::of("POST", "orders", "n=1", b"{\"amount\":5}");
        // The same request, spelled the way the two sides happen to
        // spell it: the method's case and the path's leading slash are
        // not a difference.
        assert_eq!(approved, RequestFingerprint::of("post", "/orders", "n=1", b"{\"amount\":5}"));
        // Everything that IS a difference.
        assert_ne!(approved, RequestFingerprint::of("DELETE", "orders", "n=1", b"{\"amount\":5}"));
        assert_ne!(approved, RequestFingerprint::of("POST", "refunds", "n=1", b"{\"amount\":5}"));
        assert_ne!(approved, RequestFingerprint::of("POST", "orders", "n=2", b"{\"amount\":5}"));
        assert_ne!(
            approved,
            RequestFingerprint::of("POST", "orders", "n=1", b"{\"amount\":999999}")
        );
        // An empty body is a fingerprint of its own, so dropping the
        // body is a mismatch rather than an absence nobody notices.
        assert_ne!(approved, RequestFingerprint::of("POST", "orders", "n=1", b""));
    }

    #[test]
    fn mint_validate_round_trip() {
        let tok = mint(SECRET, &claims("pod-7", 1_000));
        let back = validate(SECRET, &tok, 999).unwrap();
        assert_eq!(back, claims("pod-7", 1_000));
        // An open route on a bare path carries neither captures nor a caller.
        let bare = CallerTokenClaims { params: BTreeMap::new(), caller: None, ..claims("pod-7", 1_000) };
        assert_eq!(validate(SECRET, &mint(SECRET, &bare), 0).unwrap(), bare);
    }

    #[test]
    fn rejects_expired() {
        let tok = mint(SECRET, &claims("pod", 1_000));
        assert_eq!(
            validate(SECRET, &tok, 1_000).unwrap_err(),
            "routing token expired"
        );
        assert!(validate(SECRET, &tok, 2_000).is_err());
    }

    #[test]
    fn rejects_wrong_secret_and_tampering() {
        let tok = mint(SECRET, &claims("pod-7", 1_000));
        assert!(validate(b"other-secret-bbbbbbbbbbbbbbbbbbbb", &tok, 0).is_err());
        // Tamper: re-mint with a different pod under a DIFFERENT secret, then
        // splice that forged payload onto the real token's signature. The sig
        // was computed over the original payload, so it cannot validate the
        // re-pointed one.
        let forged_full = mint(b"attacker-secret-cccccccccccccccc", &claims("attacker-pod", 1_000));
        let forged_payload = forged_full.split('.').nth(1).unwrap();
        let real_sig = tok.split('.').nth(2).unwrap();
        let forged = format!("v1.{forged_payload}.{real_sig}");
        assert!(
            validate(SECRET, &forged, 0).is_err(),
            "a token re-pointed to another pod must fail signature check"
        );
    }

    #[test]
    fn rejects_malformed() {
        for bad in ["", "v1", "v1.abc", "v2.a.b", "v1.!!.??"] {
            assert!(validate(SECRET, bad, 0).is_err(), "should reject {bad:?}");
        }
    }
}
