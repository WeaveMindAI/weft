//! HTTP POST webhook. The listener mounts a public route at the
//! dispatcher root; external callers POST to fire the signal.
//! Optional api-key gate is enforced by the dispatcher's auth gate
//! using a hash stored on the signal row.

use serde::{Deserialize, Serialize};

use super::Signal;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Webhook {
    /// Route suffix under the dispatcher root. Empty means root.
    /// Must not start with `/` (the framework rejects at validation).
    pub path: String,
    /// Auth policy. `None` = anyone with the URL can fire.
    /// `OptionalApiKey` = listener mints a plaintext at register and
    /// stores its sha256 on the signal row; `/display` returns the
    /// plaintext until the listener pod restarts.
    #[serde(default)]
    pub auth: WebhookAuth,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WebhookAuth {
    #[default]
    None,
    OptionalApiKey,
}

impl Signal for Webhook {
    const TAG: &'static str = "webhook";

    fn validate(&self) -> Result<(), String> {
        if self.path.starts_with('/') {
            return Err(format!(
                "webhook path must not start with '/': got '{}'",
                self.path
            ));
        }
        Ok(())
    }
}

crate::register_signal_kind!(Webhook);
