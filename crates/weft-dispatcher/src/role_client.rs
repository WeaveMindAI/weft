//! The dispatcher's authenticated client of another weft role (the
//! broker, the listener). One place owns the role's address, the
//! dispatcher's platform identity for it, and the URL joining; each role's
//! surface builds its own request/response handling on top (the storage
//! sweep wants typed retry classes, the access forwards want verbatim
//! status passthrough, the listener wants its typed wire structs).

use anyhow::{Context, Result};
use std::sync::Arc;
use weft_platform_traits::identity::{IdentityTokens, ROLE_HEADER};
use weft_platform_traits::CoreRole;

/// One role as the dispatcher reaches it: its internal address, the
/// dispatcher's identity for it, and the process's shared HTTP client
/// (`DispatcherState::http`, which follows no redirect: a role answers in
/// place).
#[derive(Clone)]
pub struct RoleClient {
    role: CoreRole,
    base_url: String,
    tokens: Arc<dyn IdentityTokens>,
    http: reqwest::Client,
}

impl RoleClient {
    /// `base_url` is the role's internal address
    /// (`InstallConfig::role_addresses()`).
    pub fn new(role: CoreRole, base_url: String, tokens: Arc<dyn IdentityTokens>, http: reqwest::Client) -> Self {
        Self { role, base_url: base_url.trim_end_matches('/').to_string(), tokens, http }
    }

    /// A request to `path` on the role, carrying the dispatcher's
    /// identity: every role's internal surface answers weft's own roles
    /// only.
    pub async fn request(&self, method: reqwest::Method, path: &str) -> Result<reqwest::RequestBuilder> {
        let bearer = self
            .tokens
            .token_for(&self.base_url)
            .await
            .with_context(|| format!("get an identity token for the {}", self.role.as_str()))?;
        Ok(self
            .http
            .request(method, format!("{}{path}", self.base_url))
            .bearer_auth(bearer)
            .header(ROLE_HEADER, CoreRole::Dispatcher.as_str()))
    }
}
