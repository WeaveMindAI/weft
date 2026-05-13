//! Webhook handler. The dispatcher mounts the public route; the
//! listener only registers the in-RAM entry and returns the routing
//! shape. Optional api-key gate is enforced by the dispatcher reading
//! `auth_config.value_hash` and comparing to the request header.

use std::sync::Arc;

use anyhow::Result;
use dashmap::DashMap;
use serde_json::Value;
use tokio::task::JoinHandle;
use weft_core::primitive::{SignalAuth, SignalRouting, SignalSpec, SignalSurface};
use weft_core::signal::{Signal, Webhook, WebhookAuth};

use crate::config::ListenerConfig;
use crate::fire_sink::FireSignalSink;
use crate::protocol::{ProcessOutcome, ProcessTarget};
use crate::registry::RegisteredSignal;

use super::{
    default_api_key_header, mint_api_key, sha256_hex, KindHandler,
};

pub struct WebhookHandler;

impl KindHandler for WebhookHandler {
    fn tag(&self) -> &'static str {
        Webhook::TAG
    }

    fn compute_routing(
        &self,
        token: &str,
        spec: &SignalSpec,
        secret_cache: &Arc<DashMap<String, String>>,
    ) -> Result<SignalRouting> {
        let parsed = parse(spec)?;
        let surface = SignalSurface::PublicEntry { path: parsed.path.clone() };
        Ok(match parsed.auth {
            WebhookAuth::None => SignalRouting {
                surface,
                auth: SignalAuth::None,
                auth_config: Value::Null,
            },
            WebhookAuth::OptionalApiKey => {
                let plaintext = mint_api_key();
                let hash = sha256_hex(&plaintext);
                secret_cache.insert(token.to_string(), plaintext);
                SignalRouting {
                    surface,
                    auth: SignalAuth::ApiKey,
                    auth_config: serde_json::json!({
                        "header_name": default_api_key_header(),
                        "value_hash": hash,
                    }),
                }
            }
        })
    }

    fn spawn_task(
        &self,
        _token: &str,
        _spec: &SignalSpec,
        _kind_state: &Value,
        _sink: FireSignalSink,
        _config: Arc<ListenerConfig>,
    ) -> Result<Option<JoinHandle<()>>> {
        Ok(None)
    }

    fn process_entry(
        &self,
        _sig: &RegisteredSignal,
        payload: Value,
    ) -> ProcessOutcome {
        ProcessOutcome {
            value: payload,
            target: ProcessTarget::Entry,
        }
    }

    fn render(&self, _token: &str, _sig: &RegisteredSignal) -> Result<Option<Value>> {
        Ok(None)
    }

    fn handle_action(
        &self,
        token: &str,
        action: &str,
        _payload: Value,
        sig: &RegisteredSignal,
        secret_cache: &Arc<DashMap<String, String>>,
    ) -> Result<(Value, Option<SignalRouting>)> {
        match action {
            "regenerate_api_key" => {
                let parsed = parse(&sig.spec)?;
                let plaintext = mint_api_key();
                let hash = sha256_hex(&plaintext);
                secret_cache.insert(token.to_string(), plaintext.clone());
                let updated = SignalRouting {
                    surface: SignalSurface::PublicEntry { path: parsed.path },
                    auth: SignalAuth::ApiKey,
                    auth_config: serde_json::json!({
                        "header_name": default_api_key_header(),
                        "value_hash": hash,
                    }),
                };
                Ok((serde_json::json!({ "secret": plaintext }), Some(updated)))
            }
            other => anyhow::bail!("webhook has no action '{other}'"),
        }
    }
}

/// Parse the spec's opaque config into the typed kind. Fails loudly
/// on malformed config so register surfaces a 400 instead of silently
/// mounting a webhook at "/" with no auth.
fn parse(spec: &SignalSpec) -> Result<Webhook> {
    serde_json::from_value(spec.config.clone())
        .map_err(|e| anyhow::anyhow!("malformed webhook spec: {e}"))
}

inventory::submit!(&WebhookHandler as &dyn KindHandler);

#[cfg(test)]
mod tests {
    use super::*;

    use weft_core::primitive::SignalSurface;

    fn webhook_spec(path: &str, auth: WebhookAuth) -> SignalSpec {
        weft_core::signal::to_spec(Webhook { path: path.into(), auth })
    }

    #[test]
    fn no_auth_yields_public_entry_with_path() {
        let spec = webhook_spec("stripe", WebhookAuth::None);
        let cache = Arc::new(DashMap::new());
        let r = WebhookHandler.compute_routing("tok", &spec, &cache).expect("routing ok");
        assert!(matches!(
            r.surface,
            SignalSurface::PublicEntry { ref path } if path == "stripe"
        ));
        assert!(matches!(r.auth, weft_core::primitive::SignalAuth::None));
        assert!(cache.is_empty(), "no key minted");
    }

    #[test]
    fn optional_api_key_mints_key() {
        let spec = webhook_spec("x", WebhookAuth::OptionalApiKey);
        let cache = Arc::new(DashMap::new());
        let r = WebhookHandler.compute_routing("tok-1", &spec, &cache).expect("routing ok");
        assert!(matches!(r.auth, weft_core::primitive::SignalAuth::ApiKey));
        let plaintext = cache
            .get("tok-1")
            .map(|v| v.clone())
            .expect("plaintext stored in cache");
        let hash = r
            .auth_config
            .get("value_hash")
            .and_then(|v| v.as_str())
            .expect("value_hash present");
        assert_eq!(hash, &sha256_hex(&plaintext));
    }

    #[test]
    fn regenerate_api_key_rotates_plaintext_and_hash() {
        let spec = webhook_spec("x", WebhookAuth::OptionalApiKey);
        let cache = Arc::new(DashMap::new());
        let initial = WebhookHandler.compute_routing("tok-2", &spec, &cache).expect("routing ok");
        let initial_plaintext = cache.get("tok-2").map(|v| v.clone()).unwrap();
        let sig = RegisteredSignal {
            spec: spec.clone(),
            node_id: "n".into(),
            is_resume: false,
            color: None,
            task: None,
            routing: initial.clone(),
        };
        let (result, updated) = WebhookHandler
            .handle_action("tok-2", "regenerate_api_key", Value::Null, &sig, &cache)
            .expect("regenerate ok");
        let new_plaintext = result
            .get("secret")
            .and_then(|v| v.as_str())
            .expect("secret in response")
            .to_string();
        assert_ne!(new_plaintext, initial_plaintext);
        let updated = updated.expect("routing updated");
        let new_hash = updated
            .auth_config
            .get("value_hash")
            .and_then(|v| v.as_str())
            .unwrap();
        assert_eq!(new_hash, &sha256_hex(&new_plaintext));
        assert_eq!(
            cache.get("tok-2").map(|v| v.clone()),
            Some(new_plaintext)
        );
    }

    #[test]
    fn unknown_action_errors() {
        let spec = webhook_spec("x", WebhookAuth::None);
        let sig = RegisteredSignal {
            spec,
            node_id: "n".into(),
            is_resume: false,
            color: None,
            task: None,
            routing: SignalRouting {
                surface: SignalSurface::PublicEntry { path: "x".into() },
                auth: weft_core::primitive::SignalAuth::None,
                auth_config: Value::Null,
            },
        };
        let cache = Arc::new(DashMap::new());
        let err = WebhookHandler
            .handle_action("tok", "does_not_exist", Value::Null, &sig, &cache)
            .expect_err("unknown action");
        assert!(err.to_string().contains("does_not_exist"));
    }
}
