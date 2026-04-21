//! Utilities for masking sensitive data in logs.
//!
//! This module provides functions to redact sensitive information before logging,
//! preventing accidental exposure of credentials, tokens, and other secrets.

use serde_json::Value;

/// List of keys that should be redacted in JSON objects
const SENSITIVE_KEYS: &[&str] = &[
    "password",
    "secret",
    "token",
    "api_key",
    "apiKey",
    "api-key",
    "credentials",
    "credential",
    "auth",
    "authorization",
    "bearer",
    "access_token",
    "accessToken",
    "refresh_token",
    "refreshToken",
    "private_key",
    "privateKey",
    "client_secret",
    "clientSecret",
    "botToken",
    "bot_token",
    "smtpPassword",
    "imapPassword",
];

/// Check if a key name suggests it contains sensitive data
fn is_sensitive_key(key: &str) -> bool {
    let key_lower = key.to_lowercase();
    SENSITIVE_KEYS.iter().any(|&sensitive| key_lower.contains(sensitive))
}

/// Redact sensitive fields from a JSON value for safe logging.
/// 
/// This recursively walks the JSON structure and replaces values
/// of keys that match sensitive patterns with "[REDACTED]".
pub fn redact_sensitive_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut redacted = serde_json::Map::new();
            for (key, val) in map {
                if is_sensitive_key(key) {
                    redacted.insert(key.clone(), Value::String("[REDACTED]".to_string()));
                } else {
                    redacted.insert(key.clone(), redact_sensitive_json(val));
                }
            }
            Value::Object(redacted)
        }
        Value::Array(arr) => {
            Value::Array(arr.iter().map(redact_sensitive_json).collect())
        }
        // Non-object/array values pass through unchanged
        other => other.clone(),
    }
}

/// Format a JSON value for logging with sensitive data redacted.
pub fn safe_json_log(value: &Value) -> String {
    let redacted = redact_sensitive_json(value);
    serde_json::to_string(&redacted).unwrap_or_else(|_| "[invalid json]".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_redact_sensitive_json() {
        let input = json!({
            "name": "test",
            "api_key": "secret123",
            "nested": {
                "password": "hunter2",
                "safe": "visible"
            }
        });
        
        let redacted = redact_sensitive_json(&input);
        
        assert_eq!(redacted["name"], "test");
        assert_eq!(redacted["api_key"], "[REDACTED]");
        assert_eq!(redacted["nested"]["password"], "[REDACTED]");
        assert_eq!(redacted["nested"]["safe"], "visible");
    }
}
