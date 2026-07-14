//! The provider proxy: the broker stands in for a paid provider's API, so
//! the deployment's key never leaves this process.
//!
//! `open_provider_access` hands a worker a STAND-IN (`weftstandin-<32 hex>`,
//! stored in Postgres, alive for the window the caller declared) instead of
//! the key, plus this proxy's address for the provider. The worker uses them
//! as it would any key and address, so its request lands here
//! (`/v1/provider/<provider>/<the provider's own path>`, method, headers,
//! body and query intact). This module validates the stand-ins the request
//! carries, substitutes the deployment's key for them, forwards to the
//! provider's address the calling node's own registered source declared
//! (never an address from the request), and streams the response back.
//!
//! A stand-in is bound to the tenant, provider, project and node its access
//! was opened for; `close_standin` retires it when the caller is done.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::body::Body;
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::Response;

use crate::credential::{KeyRequest, KeyResolution};
use crate::state::BrokerState;

/// The stand-in prefix is the wire contract with the worker side; it lives on
/// weft-core, which both sides depend on.
pub use weft_core::access::STANDIN_PREFIX;

/// Marks a proxy-layer error response (vs an upstream error passed through),
/// so a caller can tell "the broker refused" from "the provider answered
/// 4xx".
const PROXY_ERROR_HEADER: &str = "x-weft-provider-proxy-error";

pub async fn migrate(pool: &sqlx::PgPool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS provider_standin (
            standin TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            provider TEXT NOT NULL,
            project_id TEXT NOT NULL,
            node_id TEXT NOT NULL,
            node_type TEXT NOT NULL,
            pod_name TEXT,
            created_at BIGINT NOT NULL,
            expires_at BIGINT NOT NULL
        )
        "#,
    )
    .execute(pool)
    .await
    .context("create provider_standin")?;
    Ok(())
}

/// Mint the stand-in an opening access carries, live until `expires_at`: the
/// caller declares how long its paid call may take, so the stand-in is
/// spendable for that call and not a minute longer. A crash that never closes
/// the access still lets it expire on its own.
///
/// The KEY is not stored: the proxy re-resolves it through the credential
/// source at spend time, so policy is re-checked and the DB never holds key
/// material.
pub async fn mint_standin(
    pool: &sqlx::PgPool,
    tenant: &str,
    req: &KeyRequest,
    expires_at: i64,
) -> Result<String> {
    let standin = format!("{STANDIN_PREFIX}{}", uuid::Uuid::new_v4().simple());
    sqlx::query(
        "INSERT INTO provider_standin
             (standin, tenant_id, provider, project_id, node_id, node_type, pod_name,
              created_at, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(&standin)
    .bind(tenant)
    .bind(&req.provider)
    .bind(&req.project_id)
    .bind(&req.node_id)
    .bind(&req.node_type)
    .bind(&req.pod_name)
    .bind(chrono::Utc::now().timestamp())
    .bind(expires_at)
    .execute(pool)
    .await
    .context("insert provider_standin")?;
    Ok(standin)
}

/// Retire a stand-in the moment the node is done with the provider: it stops
/// working immediately, so a leaked one is dead well before its window lapses
/// (the window is only the crash backstop). Scoped to the owning tenant, so a
/// worker cannot retire someone else's; idempotent.
pub async fn close_standin(pool: &sqlx::PgPool, standin: &str, tenant: &str) -> Result<()> {
    sqlx::query("DELETE FROM provider_standin WHERE standin = $1 AND tenant_id = $2")
        .bind(standin)
        .bind(tenant)
        .execute(pool)
        .await
        .context("close provider_standin")?;
    Ok(())
}

/// Delete expired stand-ins; returns how many. Called from the broker's
/// periodic sweep.
pub async fn sweep_expired_standins(pool: &sqlx::PgPool) -> Result<u64> {
    let now = chrono::Utc::now().timestamp();
    let done = sqlx::query("DELETE FROM provider_standin WHERE expires_at < $1")
        .bind(now)
        .execute(pool)
        .await
        .context("sweep provider_standin")?;
    Ok(done.rows_affected())
}

/// The provider's real API base for a caller, from the caller's own DECLARED
/// SOURCE: the verified calling pod names the binary the dispatcher stamped
/// on it (`worker_pod.binary_hash`), and the registration-recorded
/// `provider_declaration` rows (written from the build's own walk of the node
/// sources, never from a workload) say what URL that node's metadata
/// declared for `provider`. `None` = the node's source declares no such
/// provider, so nothing on its behalf goes anywhere: a loud refusal at the
/// caller, never a default host.
///
/// The URL is resolved from the declaration, never from the request: the
/// worker sends a provider name and nothing else about the provider. The
/// declaration lives in the node's own `metadata.json`, which is part of the
/// node's source hash, so the base_url is bound to the node's identity and
/// does not need a separate host list. Do not add one.
pub async fn declared_base_url(
    pool: &sqlx::PgPool,
    pod_name: Option<&str>,
    node_type: &str,
    provider: &str,
) -> Result<Option<String>> {
    let Some(pod_name) = pod_name else {
        // Not a pod-bound caller: there is no binary to anchor a declaration
        // on, hence no declared URL.
        return Ok(None);
    };
    sqlx::query_scalar(
        "SELECT d.base_url
         FROM provider_declaration d
         JOIN worker_pod w ON w.binary_hash = d.binary_hash
         WHERE w.pod_name = $1 AND d.node_type = $2 AND d.provider = $3",
    )
    .bind(pod_name)
    .bind(node_type)
    .bind(provider)
    .fetch_optional(pool)
    .await
    .context("resolve provider declaration")
}

/// Scan bytes for every distinct stand-in (the prefix + 32 lowercase hex).
fn find_standins(bytes: &[u8], into: &mut std::collections::HashSet<String>) {
    let prefix = STANDIN_PREFIX.as_bytes();
    let mut i = 0;
    while i + prefix.len() + 32 <= bytes.len() {
        if &bytes[i..i + prefix.len()] == prefix {
            let candidate = &bytes[i + prefix.len()..i + prefix.len() + 32];
            if candidate.iter().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()) {
                into.insert(String::from_utf8_lossy(&bytes[i..i + prefix.len() + 32]).into_owned());
                i += prefix.len() + 32;
                continue;
            }
        }
        i += 1;
    }
}

/// Replace every occurrence of each stand-in with the key it stands in for.
fn substitute_standins(bytes: &[u8], subs: &HashMap<String, String>) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    'outer: while i < bytes.len() {
        for (standin, key) in subs {
            let s = standin.as_bytes();
            if bytes[i..].starts_with(s) {
                out.extend_from_slice(key.as_bytes());
                i += s.len();
                continue 'outer;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    out
}

/// The upstream URL: the provider's real base plus the path (and query) the
/// caller addressed on the proxy.
fn upstream_url(base: &str, path: &str, query: Option<&str>) -> String {
    let mut url = format!("{}/{}", base.trim_end_matches('/'), path.trim_start_matches('/'));
    if let Some(query) = query.filter(|q| !q.is_empty()) {
        url.push('?');
        url.push_str(query);
    }
    url
}

fn proxy_error(status: StatusCode, message: impl Into<String>) -> Response {
    Response::builder()
        .status(status)
        .header(PROXY_ERROR_HEADER, "1")
        .body(Body::from(message.into()))
        .expect("static error response")
}

/// The proxy handler: `/v1/provider/{provider}/{*path}`, any method. The
/// request arrives exactly as the caller built it for the provider, with
/// stand-ins where the key goes; see the module docs.
pub async fn serve(
    State(state): State<Arc<BrokerState>>,
    Path((provider, path)): Path<(String, String)>,
    RawQuery(query): RawQuery,
    method: Method,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    // No broker credential here, and none is needed: the stand-in the
    // request carries IS one (only an authenticated worker can obtain one,
    // it is bound to its tenant, it expires, and it is useless anywhere but
    // this proxy). So the request looks exactly like the provider call it is.

    // Collect the stand-ins the request carries (headers + body).
    let mut standins = std::collections::HashSet::new();
    find_standins(&body, &mut standins);
    for value in headers.values() {
        find_standins(value.as_bytes(), &mut standins);
    }
    if standins.is_empty() {
        return proxy_error(
            StatusCode::BAD_REQUEST,
            "no provider stand-in in the request; a request on your own key goes to the provider \
             directly, not through the proxy",
        );
    }

    // Validate each stand-in, resolve the URL its node's declared source
    // specifies for the provider, and resolve the key it stands in for. The
    // key is never stored; the credential source re-answers (re-checking
    // policy) now. The URL comes from the declaration rail, never from the
    // request (see `declared_base_url`).
    let now = chrono::Utc::now().timestamp();
    let mut substitutions: HashMap<String, String> = HashMap::new();
    let mut base: Option<String> = None;
    for standin in &standins {
        // Live only until its deadline, and gone as soon as the access that
        // carried it was closed (the node closes it on every path): a leaked
        // stand-in is dead the moment the work it paid for is over.
        let row = sqlx::query_as::<_, (String, String, String, String, String, Option<String>)>(
            "SELECT tenant_id, provider, project_id, node_id, node_type, pod_name
             FROM provider_standin WHERE standin = $1 AND expires_at >= $2",
        )
        .bind(standin)
        .bind(now)
        .fetch_optional(&state.pool)
        .await;
        let row = match row {
            Ok(row) => row,
            Err(e) => {
                return proxy_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("stand-in lookup: {e}"),
                )
            }
        };
        let Some((tenant_id, standin_provider, project_id, node_id, node_type, pod_name)) = row
        else {
            return proxy_error(
                StatusCode::FORBIDDEN,
                "this stand-in is not spendable: unknown, expired, or the access that carried it \
                 was already closed",
            );
        };
        if standin_provider != provider {
            return proxy_error(
                StatusCode::FORBIDDEN,
                format!(
                    "this is access to the deployment's '{standin_provider}' key; it cannot be \
                     sent to '{provider}'"
                ),
            );
        }
        let declared =
            match declared_base_url(&state.pool, pod_name.as_deref(), &node_type, &provider).await
            {
                Ok(declared) => declared,
                Err(e) => {
                    return proxy_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("resolve provider declaration: {e}"),
                    )
                }
            };
        let Some(declared) = declared else {
            return proxy_error(
                StatusCode::PRECONDITION_FAILED,
                format!(
                    "no API address for '{provider}': the calling node's source ({node_type}) \
                     declares no such provider in its metadata"
                ),
            );
        };
        match &base {
            None => base = Some(declared),
            // Every stand-in in one request must agree on where the request
            // goes; one binary can't disagree with itself (the build refuses
            // conflicting declarations), so this only trips on stand-ins
            // minted for different binaries mixed into one request.
            Some(existing) if *existing != declared => {
                return proxy_error(
                    StatusCode::PRECONDITION_FAILED,
                    format!(
                        "the request mixes stand-ins whose sources declare different API \
                         addresses for '{provider}' ({existing} vs {declared}); one request, \
                         one destination"
                    ),
                );
            }
            Some(_) => {}
        }
        let key_req = KeyRequest {
            tenant: tenant_id,
            project_id,
            node_id,
            node_type,
            provider: standin_provider,
            pod_name,
        };
        let key = match state.credentials.resolve(&state.pool, &key_req).await {
            Ok(KeyResolution::Key(key)) => key,
            Ok(KeyResolution::NotConfigured) => {
                return proxy_error(
                    StatusCode::PRECONDITION_FAILED,
                    format!("no key configured for '{}' anymore", key_req.provider),
                )
            }
            Ok(KeyResolution::Denied { reason }) => {
                return proxy_error(StatusCode::FORBIDDEN, reason)
            }
            Err(e) => {
                return proxy_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("resolve key: {e}"),
                )
            }
        };
        substitutions.insert(standin.clone(), key);
    }

    // Rebuild the request for the provider: same method, path, and body,
    // stand-ins swapped, the broker's own auth and hop-by-hop headers dropped.
    let mut out_headers = reqwest::header::HeaderMap::new();
    for (name, value) in &headers {
        let n = name.as_str();
        if n == "host" || n == "content-length" || n == "connection" || n == "transfer-encoding" {
            continue;
        }
        let substituted = substitute_standins(value.as_bytes(), &substitutions);
        match HeaderValue::from_bytes(&substituted) {
            Ok(v) => {
                out_headers.insert(name.clone(), v);
            }
            Err(e) => return proxy_error(StatusCode::BAD_REQUEST, format!("bad header {n}: {e}")),
        }
    }
    let out_body = substitute_standins(&body, &substitutions);
    // Non-empty stand-ins (checked above) means the loop resolved a base.
    let Some(base) = base else {
        return proxy_error(StatusCode::INTERNAL_SERVER_ERROR, "no base URL resolved");
    };
    let url = upstream_url(&base, &path, query.as_deref());

    let upstream = match upstream_client()
        .request(method, &url)
        .headers(out_headers)
        .body(out_body)
        .send()
        .await
    {
        Ok(response) => response,
        Err(e) => return proxy_error(StatusCode::BAD_GATEWAY, format!("upstream send: {e}")),
    };

    // Pass the provider's response through, streaming the body.
    let mut builder = Response::builder().status(upstream.status());
    for (name, value) in upstream.headers() {
        let n = name.as_str();
        if n == "connection" || n == "transfer-encoding" || n == "content-length" {
            continue;
        }
        builder = builder.header(name, value);
    }
    match builder.body(Body::from_stream(upstream.bytes_stream())) {
        Ok(response) => response,
        Err(e) => {
            proxy_error(StatusCode::INTERNAL_SERVER_ERROR, format!("assemble response: {e}"))
        }
    }
}

/// The upstream client: redirects DISABLED (a redirect could re-aim a
/// substituted key at another host), no total timeout (streams run long; the
/// connect timeout bounds a dead host).
fn upstream_client() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("provider proxy upstream client")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn standins_in(bytes: &[u8]) -> Vec<String> {
        let mut set = std::collections::HashSet::new();
        find_standins(bytes, &mut set);
        let mut v: Vec<String> = set.into_iter().collect();
        v.sort();
        v
    }

    #[test]
    fn scanner_finds_standins_anywhere_and_ignores_lookalikes() {
        let s1 = format!("{STANDIN_PREFIX}{}", "a".repeat(32));
        let s2 = format!("{STANDIN_PREFIX}0123456789abcdef0123456789abcdef");
        let body = format!("{{\"auth\":\"Bearer {s1}\",\"x\":\"{s2}\"}} {s1}");
        let mut expected = vec![s1.clone(), s2];
        expected.sort();
        assert_eq!(standins_in(body.as_bytes()), expected);
        // Too short, wrong charset, uppercase hex: not stand-ins.
        assert!(standins_in(format!("{STANDIN_PREFIX}abc").as_bytes()).is_empty());
        assert!(standins_in(format!("{STANDIN_PREFIX}{}", "Z".repeat(32)).as_bytes()).is_empty());
        assert!(standins_in(format!("{STANDIN_PREFIX}{}", "A".repeat(32)).as_bytes()).is_empty());
    }

    #[test]
    fn substitution_replaces_every_occurrence_of_every_standin() {
        let s1 = format!("{STANDIN_PREFIX}{}", "a".repeat(32));
        let s2 = format!("{STANDIN_PREFIX}{}", "b".repeat(32));
        let subs: HashMap<String, String> =
            [(s1.clone(), "sk-real-1".into()), (s2.clone(), "sk-real-2".into())].into();
        let body = format!("Bearer {s1} + {s2} + {s1}");
        let out = substitute_standins(body.as_bytes(), &subs);
        assert_eq!(String::from_utf8(out).unwrap(), "Bearer sk-real-1 + sk-real-2 + sk-real-1");
    }

    /// The node addresses the proxy; the proxy rebuilds the same call against
    /// the provider's REAL base, resolved from the node's own registered
    /// declaration. A node never names a host in a REQUEST, so it cannot aim
    /// a key at one.
    #[test]
    fn the_upstream_url_is_the_providers_own_base_plus_the_callers_path() {
        assert_eq!(
            upstream_url("https://openrouter.ai/api/v1", "chat/completions", None),
            "https://openrouter.ai/api/v1/chat/completions"
        );
        assert_eq!(
            upstream_url("https://openrouter.ai/api/v1/", "/generation", Some("id=gen-1")),
            "https://openrouter.ai/api/v1/generation?id=gen-1"
        );
    }

}
