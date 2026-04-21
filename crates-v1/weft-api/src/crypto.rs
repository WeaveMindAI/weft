//! Credential encryption module for secure storage of sensitive data.
//!
//! Uses AES-256-GCM for authenticated encryption. The encryption key is loaded
//! from the CREDENTIAL_ENCRYPTION_KEY environment variable (base64-encoded 32-byte key).
//!
//! For local development, a default key is used if not set.
//! For production, the key should be stored in GCP Secret Manager.

use aes_gcm::{
    aead::{Aead, KeyInit, OsRng},
    Aes256Gcm, Nonce,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use rand::RngCore;

const NONCE_SIZE: usize = 12;

/// Get the encryption key from environment.
/// 
/// Environment variable: CREDENTIAL_ENCRYPTION_KEY (base64-encoded 32-byte key)
/// 
/// In cloud mode: PANICS if key is not set (fail loudly - security critical)
/// In local mode: Falls back to development key with warning
fn get_encryption_key() -> [u8; 32] {
    let is_cloud_mode = std::env::var("DEPLOYMENT_MODE")
        .map(|v| v.to_lowercase() == "cloud")
        .unwrap_or(false);
    
    match std::env::var("CREDENTIAL_ENCRYPTION_KEY") {
        Ok(key_b64) => {
            let key_bytes = BASE64.decode(&key_b64).expect("Invalid base64 encryption key");
            if key_bytes.len() != 32 {
                panic!("CREDENTIAL_ENCRYPTION_KEY must be exactly 32 bytes (256 bits)");
            }
            let mut key = [0u8; 32];
            key.copy_from_slice(&key_bytes);
            key
        }
        Err(_) => {
            if is_cloud_mode {
                panic!(
                    "CREDENTIAL_ENCRYPTION_KEY is NOT SET but DEPLOYMENT_MODE=cloud. \
                    This is a CRITICAL SECURITY ERROR. \
                    Generate a key with: openssl rand -base64 32"
                );
            }
            
            // Development fallback - deterministic key for local testing only
            tracing::warn!("CREDENTIAL_ENCRYPTION_KEY not set - using development key (local mode only)");
            let mut key = [0u8; 32];
            for (i, byte) in b"weavemind-dev-encryption-key-32!".iter().enumerate() {
                key[i] = *byte;
            }
            key
        }
    }
}

/// Encrypt sensitive data (e.g., credentials JSON).
/// Returns base64-encoded ciphertext with prepended nonce.
pub fn encrypt_credentials(plaintext: &serde_json::Value) -> Result<String, String> {
    let key = get_encryption_key();
    let cipher = Aes256Gcm::new_from_slice(&key)
        .map_err(|e| format!("Failed to create cipher: {}", e))?;
    
    // Generate random nonce
    let mut nonce_bytes = [0u8; NONCE_SIZE];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);
    
    // Serialize and encrypt
    let plaintext_bytes = serde_json::to_vec(plaintext)
        .map_err(|e| format!("Failed to serialize credentials: {}", e))?;
    
    let ciphertext = cipher.encrypt(nonce, plaintext_bytes.as_ref())
        .map_err(|e| format!("Encryption failed: {}", e))?;
    
    // Prepend nonce to ciphertext and base64 encode
    let mut combined = Vec::with_capacity(NONCE_SIZE + ciphertext.len());
    combined.extend_from_slice(&nonce_bytes);
    combined.extend_from_slice(&ciphertext);
    
    Ok(BASE64.encode(&combined))
}

/// Decrypt credentials from base64-encoded ciphertext.
/// Returns the decrypted JSON value.
pub fn decrypt_credentials(encrypted_b64: &str) -> Result<serde_json::Value, String> {
    let key = get_encryption_key();
    let cipher = Aes256Gcm::new_from_slice(&key)
        .map_err(|e| format!("Failed to create cipher: {}", e))?;
    
    // Decode base64
    let combined = BASE64.decode(encrypted_b64)
        .map_err(|e| format!("Invalid base64: {}", e))?;
    
    if combined.len() < NONCE_SIZE {
        return Err("Ciphertext too short".to_string());
    }
    
    // Split nonce and ciphertext
    let (nonce_bytes, ciphertext) = combined.split_at(NONCE_SIZE);
    let nonce = Nonce::from_slice(nonce_bytes);
    
    // Decrypt
    let plaintext = cipher.decrypt(nonce, ciphertext)
        .map_err(|e| format!("Decryption failed: {}", e))?;
    
    // Parse JSON
    serde_json::from_slice(&plaintext)
        .map_err(|e| format!("Failed to parse decrypted credentials: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let credentials = serde_json::json!({
            "apiKey": "sk-test-12345",
            "secret": "my-secret-value"
        });
        
        let encrypted = encrypt_credentials(&credentials).unwrap();
        let decrypted = decrypt_credentials(&encrypted).unwrap();
        
        assert_eq!(credentials, decrypted);
    }
    
    #[test]
    fn test_different_encryptions_produce_different_ciphertext() {
        let credentials = serde_json::json!({"key": "value"});
        
        let encrypted1 = encrypt_credentials(&credentials).unwrap();
        let encrypted2 = encrypt_credentials(&credentials).unwrap();
        
        // Due to random nonce, same plaintext should produce different ciphertext
        assert_ne!(encrypted1, encrypted2);
        
        // But both should decrypt to the same value
        assert_eq!(
            decrypt_credentials(&encrypted1).unwrap(),
            decrypt_credentials(&encrypted2).unwrap()
        );
    }
    
}
