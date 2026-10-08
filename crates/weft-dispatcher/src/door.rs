//! The install's public door: every request to the public API is routed
//! by the name it came for.
//!
//! The install answers at its own address (a local port, a Cloud Run
//! service's `run.app` name) and at each domain it stores (`crate::domains`,
//! reached through the platform's `DomainHosting`, which holds the
//! certificates). A project's frontend domain is passed on to where the
//! frontend runs; a project's API domain serves that project's routes at
//! its root; the install's own domains and any other name reach the
//! public API itself.
//!
//! A project's API answers one path of the install's besides its routes:
//! the file links its callers are handed (`/public/files/{token}`), which
//! are built on the address the caller used (`crate::storage::LinkBase`).
//!
//! The domains are held in memory and read again when one comes or goes
//! (`crate::held`), so routing a request costs no trip to the database.

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::header::InvalidHeaderValue;
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Router;
use tower::ServiceExt as _;
use weft_core::install::{Domain, DomainServes};

use crate::state::DispatcherState;

/// The scheme a request that came by a stored domain was reached over:
/// the platform's door in front of the domains holds their certificates
/// (`weft_platform_traits::DomainHosting`).
const DOMAIN_SCHEME: &str = "https";

/// Where a request is going, by the name it came for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Destination {
    /// The install itself: the whole public API. `by_domain` when it came
    /// by one of the install's own domains rather than its own address.
    Install { by_domain: bool },
    /// A project's frontend at `domain`, running at `upstream`.
    Frontend { domain: String, upstream: String },
    /// A project's API.
    Api { project: uuid::Uuid },
}

impl Destination {
    /// Whether the request came by a stored domain, so through the door
    /// the platform puts in front of them.
    fn by_domain(&self) -> bool {
        !matches!(self, Destination::Install { by_domain: false })
    }
}

/// Where a request that came for `host` goes. A name that is no stored
/// domain (the install's own address, or no `Host` at all) is the install.
pub fn destination(host: Option<&str>, domains: &[Domain]) -> Destination {
    let Some(host) = host else { return Destination::Install { by_domain: false } };
    let bare = without_port(host).to_ascii_lowercase();
    match domains.iter().find(|d| d.name == bare) {
        None => Destination::Install { by_domain: false },
        Some(Domain { serves: DomainServes::Install, .. }) => Destination::Install { by_domain: true },
        Some(Domain { name, serves: DomainServes::Frontend { upstream, .. } }) => {
            Destination::Frontend { domain: name.clone(), upstream: upstream.clone() }
        }
        Some(Domain { serves: DomainServes::Api { project }, .. }) => Destination::Api { project: *project },
    }
}

/// Tell a frontend where its visitor really was: at `domain`, over
/// [`DOMAIN_SCHEME`], mounted at its root. What the caller wrote in these
/// headers is replaced: the frontend builds its links (a sign-in's
/// redirect among them) from them (`crate::proxy`, then
/// `weft_core::net::request_base_url`), and nothing stands between the
/// platform's door and this one that could have set them.
pub fn as_reached_at(headers: &mut HeaderMap, domain: &str) -> Result<(), InvalidHeaderValue> {
    headers.insert("x-forwarded-host", HeaderValue::from_str(domain)?);
    headers.insert(weft_core::net::WEFT_FORWARDED_PROTO, HeaderValue::from_static(DOMAIN_SCHEME));
    headers.remove("x-forwarded-prefix");
    Ok(())
}

/// `host` without a `:port` (an IPv6 literal keeps its brackets).
fn without_port(host: &str) -> &str {
    if host.starts_with('[') {
        return host.split_inclusive(']').next().unwrap_or(host);
    }
    match host.rsplit_once(':') {
        Some((name, port)) if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) => name,
        _ => host,
    }
}

/// The install's file links (`/public/files/{token}`), which a project's
/// API answers too: they are built on the address a caller used, a
/// project's own among them.
const FILE_LINKS: &str = "/public/files/";

/// Where the path of an API domain's request goes on the install: one of
/// the project's routes, passed on through the relay under its tenant.
pub fn api_path(tenant: &str, path_and_query: &str) -> String {
    format!("/connect/{tenant}/{}", path_and_query.trim_start_matches('/'))
}

#[derive(Clone)]
struct Door {
    inner: Router,
    state: DispatcherState,
}

impl Door {
    /// The stored domains, from the install's held copy. Refused when
    /// they cannot be read: routing as if there were no domains would send
    /// a frontend's visitors to the install.
    async fn domains(&self) -> Result<Arc<Vec<Domain>>, Response> {
        self.state.held.domains.get_or_load((), || crate::domains::list(&self.state.pg_pool)).await.map_err(|e| {
            tracing::warn!(target: "weft_dispatcher::door", error = %format!("{e:#}"), "could not read the install's domains");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("the install could not read its domains to route this request ({e:#}); try again shortly"),
            )
                .into_response()
        })
    }
}

/// `inner` (the public API) behind the door that routes by name.
pub fn router(state: DispatcherState, inner: Router) -> Router {
    Router::new().fallback(route).with_state(Door { inner, state })
}

async fn route(State(door): State<Door>, mut request: Request) -> Response {
    // Only this door says which project an API domain serves.
    request.headers_mut().remove(crate::api::signal::API_PROJECT_HEADER);
    let host = request
        .headers()
        .get(axum::http::header::HOST)
        .and_then(|h| h.to_str().ok())
        .map(str::to_string)
        .or_else(|| request.uri().authority().map(|a| a.to_string()));
    let domains = match door.domains().await {
        Ok(domains) => domains,
        Err(refused) => return refused,
    };
    let to = destination(host.as_deref(), &domains);
    if to.by_domain() {
        // It came through the door in front of the install's domains,
        // which is one more proxy in front.
        request.extensions_mut().insert(crate::api::TrustedHops(door.state.edge.trusted_proxy_hops.domains));
    }
    match to {
        Destination::Install { .. } => door.inner.oneshot(request).await.unwrap_or_else(|never| match never {}),
        Destination::Frontend { domain, upstream } => {
            if let Err(e) = as_reached_at(request.headers_mut(), &domain) {
                return (StatusCode::INTERNAL_SERVER_ERROR, format!("the domain '{domain}' is not a header value: {e}")).into_response();
            }
            let path_and_query = request.uri().path_and_query().map(|p| p.as_str().to_string()).unwrap_or_else(|| "/".into());
            let upstream = crate::proxy::Upstream { what: "the frontend", base_url: upstream, auth: None, hold: None };
            crate::proxy::forward(&door.state.http, upstream, path_and_query, request, crate::proxy::Forwarding::Proxied).await
        }
        Destination::Api { .. } if request.uri().path().starts_with(FILE_LINKS) => {
            door.inner.oneshot(request).await.unwrap_or_else(|never| match never {})
        }
        Destination::Api { project } => {
            let tenant = match door.state.projects.tenant_for(project).await {
                Ok(Some(tenant)) => tenant,
                Ok(None) => return (StatusCode::NOT_FOUND, "this domain's project no longer exists").into_response(),
                Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("the domain's project: {e:#}")).into_response(),
            };
            let path_and_query = request.uri().path_and_query().map(|p| p.as_str().to_string()).unwrap_or_else(|| "/".into());
            let Ok(uri) = api_path(&tenant, &path_and_query).parse::<Uri>() else {
                return (StatusCode::BAD_REQUEST, "the request's path is not a path").into_response();
            };
            *request.uri_mut() = uri;
            request.headers_mut().insert(
                crate::api::signal::API_PROJECT_HEADER,
                HeaderValue::from_str(&project.to_string()).expect("a uuid is a header value"),
            );
            door.inner.oneshot(request).await.unwrap_or_else(|never| match never {})
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn domains() -> Vec<Domain> {
        let project = uuid::Uuid::from_u128(7);
        vec![
            Domain { name: "weft.example.com".into(), serves: DomainServes::Install },
            Domain { name: "app.example.com".into(), serves: DomainServes::Frontend { project, upstream: "https://front-x.run.app".into() } },
            Domain { name: "api.example.com".into(), serves: DomainServes::Api { project } },
        ]
    }

    #[test]
    fn a_request_goes_where_its_name_says() {
        let d = domains();
        let own = Destination::Install { by_domain: false };
        assert_eq!(destination(Some("weft-role-dispatcher-1.us-central1.run.app"), &d), own);
        assert_eq!(destination(Some("127.0.0.1:14111"), &d), own);
        assert_eq!(destination(None, &d), own);
        assert_eq!(destination(Some("[2600::1]:443"), &d), own);
        assert_eq!(destination(Some("Weft.Example.com:443"), &d), Destination::Install { by_domain: true });
        assert_eq!(
            destination(Some("app.example.com"), &d),
            Destination::Frontend { domain: "app.example.com".into(), upstream: "https://front-x.run.app".into() }
        );
        assert_eq!(destination(Some("api.example.com"), &d), Destination::Api { project: uuid::Uuid::from_u128(7) });
    }

    /// Only a request that came by a stored domain went through the door
    /// in front of them, the one more proxy its caller's address is read
    /// past.
    #[test]
    fn a_request_by_a_domain_came_through_the_platforms_door() {
        let d = domains();
        assert!(!destination(Some("127.0.0.1:14111"), &d).by_domain());
        assert!(!destination(None, &d).by_domain());
        for host in ["weft.example.com", "app.example.com", "api.example.com"] {
            assert!(destination(Some(host), &d).by_domain(), "{host}");
        }
    }

    /// A caller cannot pick the host, scheme or mount the frontend builds
    /// its links from: they are the domain it came by, over https, at the
    /// root.
    #[test]
    fn a_frontend_is_told_the_domain_whatever_the_caller_wrote() {
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_static("app.example.com"));
        headers.insert("x-forwarded-host", HeaderValue::from_static("evil.example"));
        headers.insert("x-forwarded-proto", HeaderValue::from_static("http"));
        headers.insert(weft_core::net::WEFT_FORWARDED_PROTO, HeaderValue::from_static("http"));
        headers.insert("x-forwarded-prefix", HeaderValue::from_static("/phish"));
        as_reached_at(&mut headers, "app.example.com").unwrap();
        assert_eq!(weft_core::net::request_base_url(&headers).as_deref(), Some("https://app.example.com"));
        assert!(headers.get("x-forwarded-prefix").is_none());
    }

    #[test]
    fn an_api_domain_serves_the_projects_routes_at_its_root() {
        assert_eq!(api_path("local", "/users/42?x=1"), "/connect/local/users/42?x=1");
        assert_eq!(api_path("local", "/"), "/connect/local/");
        assert_eq!(api_path("local", "/chat?a=1&wct=t"), "/connect/local/chat?a=1&wct=t");
    }
}
