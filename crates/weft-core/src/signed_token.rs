//! Generic short-lived signed bearer token: HMAC-SHA256 over a base64url
//! JSON claims payload, wire format `v1.<payload-b64url>.<sig-b64url>`. ONE
//! implementation of the crypto + wire format, parameterized over the claims
//! type, so every signed-token flavor in the codebase (live-caller routing
//! tokens, storage download capabilities, any future single-resource bearer)
//! shares the exact same minting/validation path. A crypto fix or format bump
//! happens here once, never drifts between copies.
//!
//! A flavor supplies: a claims struct (`Serialize + DeserializeOwned`) that
//! implements [`SignedClaims`] (just exposes its `exp`), plus a short `noun`
//! used in the caller-safe error strings (e.g. "routing token",
//! "capability"). The crypto is identical regardless.
//!
//! Pure functions; the caller passes `now`. Constant-time signature check
//! (`verify_slice`); error reasons never leak the secret or the payload.

use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{de::DeserializeOwned, Serialize};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::URL_SAFE_NO_PAD;

/// A claims type a signed token carries. The only thing the generic machinery
/// needs from the claims (beyond serde) is the expiry, checked at validation.
pub trait SignedClaims: Serialize + DeserializeOwned {
    /// Unix-seconds expiry. The token is rejected once `now_unix >= exp`.
    fn exp(&self) -> i64;
}

/// Mint a signed token for `claims`. `secret` is the cluster signing key.
pub fn mint<C: SignedClaims>(secret: &[u8], claims: &C) -> String {
    let payload = B64.encode(serde_json::to_vec(claims).expect("claims serialize"));
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(payload.as_bytes());
    let sig = B64.encode(mac.finalize().into_bytes());
    format!("v1.{payload}.{sig}")
}

/// Validate a signed token and return its claims. Rejects on format,
/// signature, and expiry. `noun` names the artifact in the (caller-safe,
/// secret-free) error strings, e.g. `"routing token"` -> "malformed routing
/// token". Any extra policy (pod pinning, resource binding) is the caller's
/// to apply on top of the returned claims.
pub fn validate<C: SignedClaims>(
    secret: &[u8],
    token: &str,
    now_unix: i64,
    noun: &str,
) -> Result<C, String> {
    let mut parts = token.splitn(3, '.');
    let (Some("v1"), Some(payload), Some(sig)) = (parts.next(), parts.next(), parts.next()) else {
        return Err(format!("malformed {noun}"));
    };
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(payload.as_bytes());
    let sig_bytes = B64
        .decode(sig)
        .map_err(|_| format!("malformed {noun} signature"))?;
    // verify_slice is constant-time.
    mac.verify_slice(&sig_bytes)
        .map_err(|_| format!("invalid {noun} signature"))?;
    let claims: C = serde_json::from_slice(
        &B64.decode(payload)
            .map_err(|_| format!("malformed {noun} payload"))?,
    )
    .map_err(|_| format!("malformed {noun} claims"))?;
    if now_unix >= claims.exp() {
        return Err(format!("{noun} expired"));
    }
    Ok(claims)
}
