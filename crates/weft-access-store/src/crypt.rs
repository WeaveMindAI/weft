//! At-rest sealing of stored credential material (AES-256-GCM).
//!
//! Every secret the store persists (a grant's values, an app snapshot,
//! a pending consent's app + PKCE verifier, a subscription's echo
//! token) is sealed before it lands in Postgres and opened right after
//! it is read, so a database dump alone carries no usable credential.
//! Non-secret row data (service names, scopes, value NAMES, a public
//! client id) stays plain so queries keep working on it.
//!
//! The key comes from `CREDENTIAL_ENCRYPTION_KEY` (base64, exactly 32
//! bytes; generate one with `openssl rand -base64 32`). A malformed key
//! is a panic at first use: silently sealing under the wrong key would
//! strand every credential. When the variable is unset, a built-in
//! development key is used and a warning is logged once: an instance
//! whose operator has not set a key still gets a working store, and
//! the warning names the fix.
//!
//! Sealed format: base64(nonce || ciphertext), 12-byte nonce, random
//! per seal. There is no key rotation: changing the key strands every
//! sealed row, so rotate by reconnecting.

use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;

const NONCE_SIZE: usize = 12;

/// The development key sealing falls back to when
/// `CREDENTIAL_ENCRYPTION_KEY` is unset. Public by definition; the
/// warning at first use names the real fix.
const DEV_KEY: &[u8; 32] = b"weft-dev-credential-sealing-key!";

fn cipher() -> &'static Aes256Gcm {
    static CIPHER: std::sync::OnceLock<Aes256Gcm> = std::sync::OnceLock::new();
    CIPHER.get_or_init(|| {
        let key: [u8; 32] = match std::env::var("CREDENTIAL_ENCRYPTION_KEY") {
            Ok(b64) => BASE64
                .decode(b64.trim())
                .ok()
                .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
                .unwrap_or_else(|| {
                    panic!(
                        "CREDENTIAL_ENCRYPTION_KEY must be base64 of exactly 32 bytes; \
                         generate one with: openssl rand -base64 32"
                    )
                }),
            Err(_) => {
                tracing::warn!(
                    "CREDENTIAL_ENCRYPTION_KEY is not set; stored credentials are sealed \
                     with the built-in development key. Set it (openssl rand -base64 32) \
                     before storing credentials you care about."
                );
                *DEV_KEY
            }
        };
        Aes256Gcm::new_from_slice(&key).expect("a 32-byte key always fits AES-256")
    })
}

fn seal_bytes(plain: &[u8]) -> String {
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let ct = cipher().encrypt(&nonce, plain).expect("AES-GCM encryption cannot fail");
    let mut combined = Vec::with_capacity(NONCE_SIZE + ct.len());
    combined.extend_from_slice(&nonce);
    combined.extend_from_slice(&ct);
    BASE64.encode(combined)
}

fn open_bytes(sealed: &str) -> anyhow::Result<Vec<u8>> {
    let combined = BASE64
        .decode(sealed)
        .map_err(|e| anyhow::anyhow!("sealed credential data is not base64: {e}"))?;
    if combined.len() < NONCE_SIZE {
        anyhow::bail!("sealed credential data is too short to carry a nonce");
    }
    let (nonce, ct) = combined.split_at(NONCE_SIZE);
    cipher().decrypt(Nonce::from_slice(nonce), ct).map_err(|_| {
        anyhow::anyhow!(
            "stored credential data does not open under the current \
             CREDENTIAL_ENCRYPTION_KEY; if the key changed, the sealed rows it \
             wrote are gone: reconnect the affected connections"
        )
    })
}

/// Seal a JSON value (a values map, an app snapshot) for storage.
pub fn seal_json(v: &serde_json::Value) -> anyhow::Result<String> {
    Ok(seal_bytes(&serde_json::to_vec(v)?))
}

/// Open a sealed JSON value. Loud on a key mismatch, naming the fix.
pub fn open_json(sealed: &str) -> anyhow::Result<serde_json::Value> {
    Ok(serde_json::from_slice(&open_bytes(sealed)?)?)
}

/// Seal a single secret string (a PKCE verifier, an echo token).
pub fn seal_str(s: &str) -> String {
    seal_bytes(s.as_bytes())
}

/// Open a sealed secret string.
pub fn open_str(sealed: &str) -> anyhow::Result<String> {
    Ok(String::from_utf8(open_bytes(sealed)?)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sealed_data_round_trips_and_is_not_plaintext() {
        let v = serde_json::json!({"token": "xoxb-secret", "refresh_token": "r-secret"});
        let sealed = seal_json(&v).unwrap();
        assert!(!sealed.contains("xoxb-secret"));
        assert!(!sealed.contains("token"));
        assert_eq!(open_json(&sealed).unwrap(), v);

        let sealed = seal_str("verifier-123");
        assert!(!sealed.contains("verifier"));
        assert_eq!(open_str(&sealed).unwrap(), "verifier-123");
    }

    #[test]
    fn every_seal_uses_a_fresh_nonce() {
        // Same plaintext, different ciphertext: a dump cannot even tell
        // two connections share a credential.
        assert_ne!(seal_str("same"), seal_str("same"));
    }

    #[test]
    fn tampered_data_refuses_to_open() {
        let sealed = seal_str("secret");
        // Flip a character in the middle of the base64 payload (the
        // last char only part-fills a byte, so flipping it can be a
        // no-op after decoding).
        let mid = sealed.len() / 2;
        let flipped = if sealed.as_bytes()[mid] == b'A' { "B" } else { "A" };
        let tampered = format!("{}{}{}", &sealed[..mid], flipped, &sealed[mid + 1..]);
        let err = open_str(&tampered).unwrap_err().to_string();
        assert!(err.contains("CREDENTIAL_ENCRYPTION_KEY"), "{err}");
    }
}
