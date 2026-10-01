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
        let url = self.url_for(path)?;
        let bearer = self
            .tokens
            .token_for(&self.base_url)
            .await
            .with_context(|| format!("get an identity token for the {}", self.role.as_str()))?;
        Ok(self
            .http
            .request(method, url)
            .bearer_auth(bearer)
            .header(ROLE_HEADER, CoreRole::Dispatcher.as_str()))
    }

    /// The role's URL for `path`. Paths carry values from requests (a
    /// file key, a token), so a path that does not start at the role's
    /// root, climbs out with `..`, or would land on another origin is
    /// refused instead of sent with the dispatcher's identity.
    fn url_for(&self, path: &str) -> Result<reqwest::Url> {
        anyhow::ensure!(path.starts_with('/'), "a role path starts with '/': {path:?}");
        anyhow::ensure!(
            !path.split(['/', '?', '#']).any(|seg| seg == ".." || seg == "."),
            "a role path may not climb out of its route: {path:?}"
        );
        let base = reqwest::Url::parse(&self.base_url).with_context(|| format!("the {} address {}", self.role.as_str(), self.base_url))?;
        let url = reqwest::Url::parse(&format!("{}{path}", self.base_url)).with_context(|| format!("a role path {path:?}"))?;
        anyhow::ensure!(url.origin() == base.origin(), "a role path may not change the address it is sent to: {path:?}");
        Ok(url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client() -> RoleClient {
        let tokens: Arc<dyn IdentityTokens> = Arc::new(weft_platform_traits::FixedToken("t".into()));
        RoleClient::new(CoreRole::Broker, "http://127.0.0.1:9000/".into(), tokens, reqwest::Client::new())
    }

    /// A path built from a request's values reaches only the role's own
    /// routes: it cannot name another host or climb out of its route.
    #[test]
    fn a_role_path_stays_on_the_role() {
        let role = client();
        assert_eq!(role.url_for("/v1/storage/admin/meta/t/asset/x").unwrap().as_str(), "http://127.0.0.1:9000/v1/storage/admin/meta/t/asset/x");
        for bad in ["@evil.example/x", "v1/x", "/v1/storage/../admin", "/v1/./x"] {
            assert!(role.url_for(bad).is_err(), "{bad} is refused");
        }
    }
}
