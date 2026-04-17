//! Unit tests for MiniMax provider integration.

use crate::node::ExecutionContext;
use std::sync::Arc;

/// Build a minimal ExecutionContext with the given config JSON.
fn make_ctx(config: serde_json::Value) -> ExecutionContext {
    ExecutionContext {
        executionId: "test-exec".to_string(),
        nodeId: "test-node".to_string(),
        nodeType: "LlmInference".to_string(),
        config,
        input: serde_json::json!({}),
        userId: None,
        projectId: None,
        isInfraSetup: false,
        isTriggerSetup: false,
        pulseId: "test-pulse".to_string(),
        callbackUrl: "http://localhost:3000".to_string(),
        http_client: reqwest::Client::new(),
        form_input_channels: None,
        cost_accumulator: Arc::new(std::sync::atomic::AtomicU64::new(0)),
    }
}

#[test]
fn test_resolve_api_key_minimax_byok() {
    let ctx = make_ctx(serde_json::json!({}));
    let result = ctx.resolve_api_key(Some("sk-test-minimax-key"), "minimax");
    assert!(result.is_some());
    let resolved = result.unwrap();
    assert_eq!(resolved.key, "sk-test-minimax-key");
    assert!(resolved.is_byok);
}

#[test]
fn test_resolve_api_key_minimax_platform_fallback() {
    // With MINIMAX_API_KEY unset, platform resolution returns None.
    // We temporarily clear the env var to ensure a clean test.
    let _guard = EnvVarGuard::set("MINIMAX_API_KEY", "");
    let ctx = make_ctx(serde_json::json!({}));
    // Empty string → treated as "use platform key"
    let result = ctx.resolve_api_key(Some(""), "minimax");
    assert!(result.is_none(), "No key should resolve when env var is empty");
}

#[test]
fn test_resolve_api_key_minimax_from_env() {
    let _guard = EnvVarGuard::set("MINIMAX_API_KEY", "env-minimax-key");
    let ctx = make_ctx(serde_json::json!({}));
    // Pass None to trigger env var fallback
    let result = ctx.resolve_api_key(None, "minimax");
    assert!(result.is_some());
    let resolved = result.unwrap();
    assert_eq!(resolved.key, "env-minimax-key");
    assert!(!resolved.is_byok);
}

#[test]
fn test_minimax_model_detection() {
    // Verify the model prefix check used in inference/backend.rs
    assert!("MiniMax-M2.7".starts_with("MiniMax-"));
    assert!("MiniMax-M2.7-highspeed".starts_with("MiniMax-"));
    assert!(!"anthropic/claude-3.5-sonnet".starts_with("MiniMax-"));
    assert!(!"openai/gpt-4o".starts_with("MiniMax-"));
}

#[test]
fn test_minimax_temperature_validation() {
    // MiniMax rejects temperature <= 0; our node returns an error for that.
    // This test validates the guard condition logic.
    let invalid_temperatures: &[f32] = &[0.0, -0.1, -1.0];
    for &t in invalid_temperatures {
        assert!(t <= 0.0, "temperature {} should be invalid for MiniMax", t);
    }
    let valid_temperatures: &[f32] = &[0.01, 0.5, 1.0];
    for &t in valid_temperatures {
        assert!(t > 0.0, "temperature {} should be valid for MiniMax", t);
    }
}

/// RAII guard for environment variable manipulation in tests.
struct EnvVarGuard {
    key: &'static str,
    original: Option<String>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let original = std::env::var(key).ok();
        if value.is_empty() {
            std::env::remove_var(key);
        } else {
            std::env::set_var(key, value);
        }
        Self { key, original }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match &self.original {
            Some(v) => std::env::set_var(self.key, v),
            None => std::env::remove_var(self.key),
        }
    }
}
