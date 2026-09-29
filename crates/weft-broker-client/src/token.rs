//! What a call to the broker authenticates with: the caller's platform
//! identity for the broker's address, and the process instance it comes
//! from.
//!
//! The token comes from the platform (`IdentityTokens`): a token the
//! install signed and handed to the process at start on a local install, a
//! token the cloud mints for the process's own service account on a cloud.
//! It is asked for on every call, so a platform that rotates tokens needs
//! no refresh logic here. The instance id names this running copy of the
//! caller (`weft_platform_traits::identity::INSTANCE_HEADER`).

use std::sync::Arc;

use weft_platform_traits::identity::{IdentityTokens, INSTANCE_HEADER, ROLE_HEADER};
use weft_platform_traits::CoreRole;

#[derive(Clone)]
pub struct TokenSource {
    tokens: Arc<dyn IdentityTokens>,
    instance: String,
    /// The role a weft role calls as; `None` for a worker.
    role: Option<CoreRole>,
}

impl TokenSource {
    /// A worker's source: its platform identity names its project.
    pub fn worker(tokens: Arc<dyn IdentityTokens>, instance: impl Into<String>) -> Self {
        Self { tokens, instance: instance.into(), role: None }
    }

    /// One of weft's own roles calling the broker.
    pub fn role(tokens: Arc<dyn IdentityTokens>, instance: impl Into<String>, role: CoreRole) -> Self {
        Self { tokens, instance: instance.into(), role: Some(role) }
    }

    /// The bearer token for a call to `audience` (the broker's base URL).
    pub async fn read(&self, audience: &str) -> anyhow::Result<String> {
        self.tokens.token_for(audience).await
    }

    /// This process instance's id, sent on every call.
    pub fn instance(&self) -> &str {
        &self.instance
    }

    /// The headers every call carries besides the bearer: the instance
    /// id, and for a weft role, which role it calls as.
    pub fn headers(&self) -> Vec<(&'static str, String)> {
        let mut headers = vec![(INSTANCE_HEADER, self.instance.clone())];
        if let Some(role) = self.role {
            headers.push((ROLE_HEADER, role.as_str().to_string()));
        }
        headers
    }
}
