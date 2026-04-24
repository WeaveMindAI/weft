//! Webhook and Form share the same HTTP-driven path: a POST to
//! `/signal/{token}` is the fire. Form additionally exposes its
//! schema on GET so the extension can render without talking to
//! the dispatcher.

use crate::config::ListenerConfig;

pub fn user_url(token: &str, path: &str, config: &ListenerConfig) -> String {
    let base = config.public_base_url.trim_end_matches('/');
    if path.is_empty() {
        format!("{}/signal/{}", base, token)
    } else {
        format!("{}/signal/{}/{}", base, token, path.trim_start_matches('/'))
    }
}
