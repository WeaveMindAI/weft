//! The authenticated client behind a personal access: apply a
//! service's declared [`AuthStep`]s to every request.
//!
//! ONE implementation serves every place a personal access makes a
//! call: the worker (`ctx.client(&access)`), the store's connect-time
//! test call, and the editor's resource lookups (run store-side). Node
//! authors and node code never see the stored values; auth is a
//! request transform, and the hard cases (SigV4, OAuth 1.0a) are typed
//! signing variants implemented HERE, never author code.
//!
//! Split for testability: [`resolve_steps`] (templates -> concrete
//! strings, pure) and [`apply_steps`] (transform one request, no I/O)
//! carry the logic; the middleware is a thin shim over them. The
//! signers take their time/nonce as inputs, so the official test
//! vectors pin them exactly.

use std::collections::BTreeMap;

use base64::Engine as _;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use super::hex_of;
use super::spec::{AuthStep, SignKind, Template};

/// The shared hygiene base for every connection HTTP client:
/// connection establishment bounded, NO total timeout (streams run
/// long, and user-facing waits take no deadline). Redirects follow the
/// library's STANDARD handling, nothing custom: the hop is followed
/// with the request's headers, and the well-known auth headers are
/// dropped when the host changes, exactly as every ordinary HTTP tool
/// does. Whether a redirecting service is acceptable is the caller's
/// call, like anywhere else.
pub fn base_client() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::limited(10))
            .connect_timeout(std::time::Duration::from_secs(10))
            // Some APIs (GitHub) refuse UA-less requests outright.
            .user_agent("weft")
            .build()
            .expect("client construction only parses defaults")
    })
}

/// Execute one declared authenticated call ([`ConnectCall`]): resolve
/// the URL, the body and the auth against `values`, send, refuse a
/// non-success answer loudly with the provider's own words, and hand
/// back the JSON body (Null when the provider answers empty, which
/// several stop calls do). The one executor behind every recipe call
/// (socket mints, subscribe/unsubscribe), wherever it runs.
pub async fn run_connect_call(
    call: &crate::access::spec::ConnectCall,
    values: &BTreeMap<String, String>,
) -> Result<serde_json::Value, String> {
    use crate::access::spec::TestMethod;
    let url = call.url.resolve(values)?;
    // Errors name the call by its TEMPLATE, never the resolved URL: a
    // recipe may interpolate a secret into the query, and these strings
    // travel to logs and the user.
    let shown = &call.url.0;
    let steps = resolve_steps(&call.auth, values)?;
    let client = authed_client(steps);
    let mut req = match call.method {
        TestMethod::Get => client.get(&url),
        TestMethod::Post => client.post(&url),
    };
    if let Some(body) = &call.body {
        req = req.json(&crate::access::spec::resolve_body(body, values)?);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| format!("the call to {shown} failed: {}", send_error(e)))?;
    let status = resp.status();
    let body_text = resp.text().await.unwrap_or_default();
    let body: serde_json::Value = serde_json::from_str(&body_text).unwrap_or(serde_json::Value::Null);
    if !status.is_success() {
        return Err(format!(
            "{shown} answered {status}: {}",
            body.get("error")
                .map(|e| e.to_string())
                .unwrap_or_else(|| body_text.chars().take(300).collect())
        ));
    }
    // The Slack-style 200-with-ok:false refusal, same guard as every
    // other declared call.
    if body.get("ok").and_then(serde_json::Value::as_bool) == Some(false) {
        return Err(format!(
            "{shown} refused: {}",
            body.get("error").and_then(serde_json::Value::as_str).unwrap_or("unknown error")
        ));
    }
    Ok(body)
}

/// A send failure's text with the URL stripped: reqwest prints the full
/// URL (query and path included) in its errors, and by the time a send
/// fails the auth steps have landed on the request, so that URL can
/// carry a credential (a query token, a path-prefix token).
pub fn send_error(e: reqwest_middleware::Error) -> String {
    match e {
        reqwest_middleware::Error::Reqwest(e) => e.without_url().to_string(),
        e => e.to_string(),
    }
}

/// GET `url` expecting a JSON answer, failing loudly with the
/// provider's own words. The one-call shape of what every API node
/// repeats (send, refuse a non-success status quoting the body, parse):
/// `what` names the attempt so the error reads
/// "<what>: ..." / "the service answered <status> trying to <what>".
#[cfg(feature = "runtime")]
pub async fn get_json(
    client: &reqwest_middleware::ClientWithMiddleware,
    url: &str,
    what: &str,
) -> crate::WeftResult<serde_json::Value> {
    json_call(client.get(url), what).await
}

/// POST `body` as JSON to `url` expecting a JSON answer; the POST
/// twin of [`get_json`].
#[cfg(feature = "runtime")]
pub async fn post_json(
    client: &reqwest_middleware::ClientWithMiddleware,
    url: &str,
    body: &serde_json::Value,
    what: &str,
) -> crate::WeftResult<serde_json::Value> {
    json_call(client.post(url).json(body), what).await
}

/// Send a prepared request expecting a JSON answer: refuse a
/// non-success status quoting the provider's own words, then parse.
/// The one status-and-bail shape behind [`get_json`] / [`post_json`];
/// public for callers whose request needs its own preparation (a
/// custom content type, a raw multipart body) but the same contract.
///
/// The error text quotes the provider's own message when the failure
/// body carries one of the common JSON error envelopes:
/// `{"error": {"message": ...}}` / `{"error": "..."}` (Google, and most
/// JSON APIs), top-level `{"message": ...}` (GitHub), or top-level
/// `{"detail": ...}` (FastAPI services). Otherwise it quotes the raw
/// body (truncated), so an HTML 502 from a proxy is still legible.
#[cfg(feature = "runtime")]
pub async fn json_call(
    req: reqwest_middleware::RequestBuilder,
    what: &str,
) -> crate::WeftResult<serde_json::Value> {
    use crate::error::{node_error, NodeErrExt};
    let resp = req
        .send()
        .await
        .map_err(|e| node_error(format!("{what}: {}", send_error(e))))?;
    let status = resp.status();
    let body = resp.text().await.node_err(what)?;
    if !status.is_success() {
        let nested = serde_json::from_str::<serde_json::Value>(&body).ok().and_then(|v| {
            v["error"]["message"]
                .as_str()
                .or_else(|| v["error"].as_str())
                .or_else(|| v["message"].as_str())
                .or_else(|| v["detail"].as_str())
                .map(str::to_string)
        });
        let detail = nested.unwrap_or_else(|| body.chars().take(500).collect::<String>());
        return Err(node_error(format!(
            "the service answered {status} trying to {what}: {detail}"
        )));
    }
    serde_json::from_str(&body).node_err(what)
}

/// A REQUIRED string field of a JSON answer: absent or non-string fails
/// the node loudly naming the call and the field, so a blank id never
/// travels downstream to surface later as a confusing failure on an
/// empty value. `what` names what answered (the method, the call).
#[cfg(feature = "runtime")]
pub fn required_str<'a>(
    answer: &'a serde_json::Value,
    what: &str,
    name: &str,
) -> crate::WeftResult<&'a str> {
    use crate::error::NodeErrExt;
    answer
        .get(name)
        .and_then(serde_json::Value::as_str)
        .node_err(format!("the {what} answer carries no {name}"))
}

/// A hand-framed multipart body: `multipart/form-data` for the
/// name-addressed field shape (an upload endpoint's named fields plus
/// a file part), `multipart/related` for the typed-part shape (a JSON
/// metadata part plus the bytes, Drive-style). Collect the parts, then
/// [`Multipart::build`] frames the body.
///
/// The boundary is minted at random PER BUILD and verified absent from
/// every part's headers and payload (multipart framing truncates at
/// the first match, and part content is arbitrary workflow data);
/// the astronomically rare collision re-mints. The scan looks for the
/// BARE `--<boundary>` (no leading CRLF): the part header's own
/// CRLF-CRLF terminator donates a newline right before the payload's
/// first byte, so a payload STARTING with the delimiter would frame a
/// boundary a CRLF-prefixed scan would miss.
pub struct Multipart {
    subtype: &'static str,
    parts: Vec<MultipartPart>,
}

/// One framed part: its header lines (everything between the boundary
/// line and the blank line) and its payload bytes.
struct MultipartPart {
    headers: String,
    payload: Vec<u8>,
}

/// A value placed inside a quoted-string parameter (a filename in a
/// Content-Disposition): quotes and backslashes escape, and the CR/LF
/// bytes that would break the header line become '_'.
fn quoted_param(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace(['\r', '\n'], "_")
}

/// Whether `--<boundary>` occurs in any part's headers or payload.
fn boundary_collides(parts: &[MultipartPart], boundary: &str) -> bool {
    let marker = format!("--{boundary}");
    parts.iter().any(|p| {
        p.headers.contains(&marker)
            || p.payload.windows(marker.len()).any(|w| w == marker.as_bytes())
    })
}

impl Multipart {
    /// A `multipart/form-data` body ([`Self::text`] / [`Self::file`] parts).
    pub fn form_data() -> Self {
        Self { subtype: "form-data", parts: Vec::new() }
    }

    /// A `multipart/related` body ([`Self::part`] parts).
    pub fn related() -> Self {
        Self { subtype: "related", parts: Vec::new() }
    }

    /// A named text field (form-data).
    pub fn text(mut self, name: &str, value: &str) -> Self {
        assert_eq!(self.subtype, "form-data", "named fields belong to a form-data body");
        self.parts.push(MultipartPart {
            headers: format!(
                "Content-Disposition: form-data; name=\"{}\"",
                quoted_param(name)
            ),
            payload: value.as_bytes().to_vec(),
        });
        self
    }

    /// A named file field with filename and content type (form-data).
    pub fn file(
        mut self,
        name: &str,
        filename: &str,
        content_type: &str,
        bytes: impl Into<Vec<u8>>,
    ) -> Self {
        assert_eq!(self.subtype, "form-data", "named fields belong to a form-data body");
        self.parts.push(MultipartPart {
            headers: format!(
                "Content-Disposition: form-data; name=\"{}\"; filename=\"{}\"\r\n\
                 Content-Type: {content_type}",
                quoted_param(name),
                quoted_param(filename),
            ),
            payload: bytes.into(),
        });
        self
    }

    /// A typed part, addressed by position (related).
    pub fn part(mut self, content_type: &str, bytes: impl Into<Vec<u8>>) -> Self {
        assert_eq!(self.subtype, "related", "typed parts belong to a related body");
        self.parts.push(MultipartPart {
            headers: format!("Content-Type: {content_type}"),
            payload: bytes.into(),
        });
        self
    }

    /// Frame the body: mints the verified boundary and returns the
    /// Content-Type header value plus the body bytes.
    pub fn build(self) -> (String, Vec<u8>) {
        let boundary = loop {
            let candidate = format!("weft-{}", uuid::Uuid::new_v4().simple());
            if !boundary_collides(&self.parts, &candidate) {
                break candidate;
            }
        };
        let mut body: Vec<u8> = Vec::new();
        for part in &self.parts {
            body.extend_from_slice(format!("--{boundary}\r\n{}\r\n\r\n", part.headers).as_bytes());
            body.extend_from_slice(&part.payload);
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
        (format!("multipart/{}; boundary={boundary}", self.subtype), body)
    }
}

/// One auth step with its templates RESOLVED against the grant's
/// stored values: what actually gets applied to a request.
#[derive(Clone, PartialEq)]
pub enum AppliedStep {
    Header { name: String, value: String },
    Query { name: String, value: String },
    Basic { username: String, password: String },
    PathPrefix { value: String },
    BaseUrl { value: String },
    SigV4 { service: String, region: String, access_key_id: String, secret_access_key: String },
    OAuth1a { consumer_key: String, consumer_secret: String, token: String, token_secret: String },
}

impl std::fmt::Debug for AppliedStep {
    /// Redacted: an applied step's values are credentials.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match self {
            AppliedStep::Header { .. } => "header",
            AppliedStep::Query { .. } => "query",
            AppliedStep::Basic { .. } => "basic",
            AppliedStep::PathPrefix { .. } => "path_prefix",
            AppliedStep::BaseUrl { .. } => "base_url",
            AppliedStep::SigV4 { .. } => "sigv4",
            AppliedStep::OAuth1a { .. } => "oauth1a",
        };
        write!(f, "AppliedStep::{kind}(<redacted>)")
    }
}

/// Resolve a spec's auth steps against the grant's stored values.
/// Pure; loud on an unknown value name (never an empty substitution).
pub fn resolve_steps(
    steps: &[AuthStep],
    values: &BTreeMap<String, String>,
) -> Result<Vec<AppliedStep>, String> {
    let r = |t: &Template| t.resolve(values);
    steps
        .iter()
        .map(|step| {
            Ok(match step {
                AuthStep::Header { name, value } => {
                    AppliedStep::Header { name: name.clone(), value: r(value)? }
                }
                AuthStep::Query { name, value } => {
                    AppliedStep::Query { name: name.clone(), value: r(value)? }
                }
                AuthStep::Basic { username, password } => {
                    AppliedStep::Basic { username: r(username)?, password: r(password)? }
                }
                AuthStep::PathPrefix { value } => AppliedStep::PathPrefix { value: r(value)? },
                AuthStep::BaseUrl { value } => AppliedStep::BaseUrl { value: r(value)? },
                AuthStep::Sign { with } => match with {
                    SignKind::SigV4 { service, region, access_key_id, secret_access_key } => {
                        AppliedStep::SigV4 {
                            service: service.clone(),
                            region: r(region)?,
                            access_key_id: r(access_key_id)?,
                            secret_access_key: r(secret_access_key)?,
                        }
                    }
                    SignKind::OAuth1a { consumer_key, consumer_secret, token, token_secret } => {
                        AppliedStep::OAuth1a {
                            consumer_key: r(consumer_key)?,
                            consumer_secret: r(consumer_secret)?,
                            token: r(token)?,
                            token_secret: r(token_secret)?,
                        }
                    }
                },
            })
        })
        .collect()
}

/// Apply one non-signing step to a URL + header set, the one step
/// application behind every transport (an HTTP request, a socket
/// handshake). Credential-bearing header values are marked sensitive so
/// they never print. Signing steps have no meaning here; the caller
/// routes them (HTTP signs after the plain steps, a handshake refuses
/// them) and handing one in is a caller bug.
pub fn apply_plain_step(
    url: &mut url::Url,
    headers: &mut reqwest::header::HeaderMap,
    step: &AppliedStep,
) -> Result<(), String> {
    match step {
        AppliedStep::Header { name, value } => {
            let name: reqwest::header::HeaderName =
                name.parse().map_err(|_| format!("bad auth header name '{name}'"))?;
            let mut value: reqwest::header::HeaderValue = value
                .parse()
                .map_err(|_| format!("auth header '{name:?}' value is not a legal header"))?;
            value.set_sensitive(true);
            headers.insert(name, value);
        }
        AppliedStep::Query { name, value } => {
            url.query_pairs_mut().append_pair(name, value);
        }
        AppliedStep::Basic { username, password } => {
            let raw = format!("{username}:{password}");
            let encoded = base64::engine::general_purpose::STANDARD.encode(raw);
            let mut value: reqwest::header::HeaderValue =
                format!("Basic {encoded}").parse().map_err(|_| {
                    "basic auth credentials contain bytes illegal in a header".to_string()
                })?;
            value.set_sensitive(true);
            headers.insert(reqwest::header::AUTHORIZATION, value);
        }
        AppliedStep::PathPrefix { value } => {
            let path = format!("{}{}", value, url.path());
            url.set_path(&path);
        }
        AppliedStep::BaseUrl { value } => {
            let base: url::Url = value
                .parse()
                .map_err(|e| format!("the access's base URL does not parse: {e}"))?;
            // A stored base decides where this connection's credential
            // travels, so plain http may carry it only inside a private
            // network (loopback, RFC-1918, link-local, unique-local: a
            // local or in-cluster S3-compatible store, a LAN service);
            // any host reachable from the public internet must be https.
            let private = match base.host() {
                Some(url::Host::Domain(d)) => d.eq_ignore_ascii_case("localhost"),
                Some(url::Host::Ipv4(ip)) => {
                    ip.is_loopback() || ip.is_private() || ip.is_link_local()
                }
                Some(url::Host::Ipv6(ip)) => {
                    ip.is_loopback() || ip.is_unique_local() || ip.is_unicast_link_local()
                }
                None => false,
            };
            if base.scheme() != "https" && !private {
                return Err(format!(
                    "the access's base URL must be https (its scheme is '{}'); a stored \
                     base decides where this connection's credential travels, and only a \
                     private-network host may receive it over plain http",
                    base.scheme()
                ));
            }
            let node_path = url.path().to_string();
            url.set_scheme(base.scheme())
                .map_err(|_| "the access's base URL has an unusable scheme".to_string())?;
            url.set_host(base.host_str())
                .map_err(|e| format!("the access's base URL has an unusable host: {e}"))?;
            url.set_port(base.port()).map_err(|_| "cannot set port".to_string())?;
            // A base path (https://host/api/v2) prefixes the path
            // the node wrote; a bare host keeps it verbatim.
            let base_path = base.path().trim_end_matches('/');
            if !base_path.is_empty() {
                url.set_path(&format!("{base_path}{node_path}"));
            }
        }
        AppliedStep::SigV4 { .. } | AppliedStep::OAuth1a { .. } => {
            unreachable!("signing steps are routed by the caller, never applied here")
        }
    }
    Ok(())
}

/// Apply the resolved steps to one request, in declaration order.
/// Signing steps run LAST regardless of order (a signature must cover
/// the final URL and headers), which is enforced here rather than
/// trusted to spec authors. `now`/`nonce` feed the signers; production
/// passes the clock and a fresh nonce, tests pass the vector's.
pub fn apply_steps(
    req: &mut reqwest::Request,
    steps: &[AppliedStep],
    now: chrono::DateTime<chrono::Utc>,
    nonce: &str,
) -> Result<(), String> {
    let (signing, plain): (Vec<&AppliedStep>, Vec<&AppliedStep>) = steps
        .iter()
        .partition(|s| matches!(s, AppliedStep::SigV4 { .. } | AppliedStep::OAuth1a { .. }));
    // The request cannot lend its URL and headers at once, so the plain
    // steps stage their headers aside and land in one extend (same
    // insert semantics: a staged name replaces the request's).
    let mut staged = reqwest::header::HeaderMap::new();
    for step in plain {
        apply_plain_step(req.url_mut(), &mut staged, step)?;
    }
    req.headers_mut().extend(staged);
    for step in signing {
        match step {
            AppliedStep::SigV4 { service, region, access_key_id, secret_access_key } => {
                sigv4::sign_request(req, service, region, access_key_id, secret_access_key, now)?;
            }
            AppliedStep::OAuth1a { consumer_key, consumer_secret, token, token_secret } => {
                oauth1a::sign_request(
                    req,
                    consumer_key,
                    consumer_secret,
                    token,
                    token_secret,
                    now.timestamp(),
                    nonce,
                )?;
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}

/// An HTTP client that applies `steps` (already resolved against the
/// connection's values) to every request. What the store uses for test
/// calls and lookups; the worker composes [`AuthMiddleware`] with its
/// metering middleware instead.
pub fn authed_client(steps: Vec<AppliedStep>) -> reqwest_middleware::ClientWithMiddleware {
    reqwest_middleware::ClientBuilder::new(base_client().clone())
        .with(AuthMiddleware::new(steps))
        .build()
}

/// A middleware-shaped client with NO auth at all, for a node whose
/// optional connection input is absent (the address it calls serves
/// link-shared resources anonymously). Same hygiene base as every
/// connection client, same type, so node code has one client shape.
pub fn plain_client() -> reqwest_middleware::ClientWithMiddleware {
    reqwest_middleware::ClientBuilder::new(base_client().clone()).build()
}

/// The auth application as a composable middleware, so the worker can
/// stack it with metering (auth and measurement on ONE client).
pub struct AuthMiddleware {
    steps: Vec<AppliedStep>,
}

impl AuthMiddleware {
    pub fn new(steps: Vec<AppliedStep>) -> Self {
        Self { steps }
    }
}

#[async_trait::async_trait]
impl reqwest_middleware::Middleware for AuthMiddleware {
    async fn handle(
        &self,
        mut req: reqwest::Request,
        extensions: &mut http::Extensions,
        next: reqwest_middleware::Next<'_>,
    ) -> reqwest_middleware::Result<reqwest::Response> {
        let nonce = uuid::Uuid::new_v4().simple().to_string();
        apply_steps(&mut req, &self.steps, chrono::Utc::now(), &nonce)
            .map_err(|e| reqwest_middleware::Error::Middleware(anyhow::anyhow!(e)))?;
        next.run(req, extensions).await
    }
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn sha256_hex(data: &[u8]) -> String {
    hex_of(&Sha256::digest(data))
}

/// AWS Signature Version 4 (S3, R2, every S3-compatible store).
///
/// The pure core ([`sigv4::signature`]) takes every input explicitly
/// and is pinned to the official AWS documentation vector; the request
/// wrapper adds the `x-amz-date` / `x-amz-content-sha256` headers and
/// the `Authorization` header.
pub mod sigv4 {
    use super::*;

    /// The AWS "uri-encode" charset: unreserved chars stay, everything
    /// else percent-encodes (uppercase hex), '/' included. The only
    /// caller is the canonical query, where a slash inside a value is
    /// data, never a segment separator.
    fn uri_encode(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        for b in s.bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                    out.push(b as char)
                }
                _ => out.push_str(&format!("%{b:02X}")),
            }
        }
        out
    }

    /// The canonical query string: pairs sorted by encoded name then
    /// encoded value.
    fn canonical_query(url: &url::Url) -> String {
        let mut pairs: Vec<(String, String)> = url
            .query_pairs()
            .map(|(k, v)| (uri_encode(&k), uri_encode(&v)))
            .collect();
        pairs.sort();
        pairs.into_iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&")
    }

    /// The pure SigV4 signature over explicit inputs. `headers` are the
    /// SIGNED headers as (lowercase name, trimmed value), pre-sorted by
    /// name; `payload_hash` is hex(sha256(body)) or `UNSIGNED-PAYLOAD`.
    /// Returns (signature hex, credential scope, signed-headers list).
    #[allow(clippy::too_many_arguments)]
    pub fn signature(
        method: &str,
        url: &url::Url,
        headers: &[(String, String)],
        payload_hash: &str,
        service: &str,
        region: &str,
        secret_access_key: &str,
        at: chrono::DateTime<chrono::Utc>,
    ) -> (String, String, String) {
        let date = at.format("%Y%m%d").to_string();
        let timestamp = at.format("%Y%m%dT%H%M%SZ").to_string();
        let canonical_headers: String =
            headers.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
        let signed_headers =
            headers.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>().join(";");
        // S3's single-encode convention: the canonical URI is the path
        // EXACTLY as it goes on the wire (`url.path()` is already
        // percent-encoded once; reqwest sends those bytes verbatim).
        // Re-encoding here would double-encode every escaped byte
        // (`%20` -> `%2520`) and sign a different path than the one
        // sent, so any key with a space, `+`, `&`, or non-ASCII would
        // answer 403 SignatureDoesNotMatch.
        let canonical_request = format!(
            "{method}\n{}\n{}\n{canonical_headers}\n{signed_headers}\n{payload_hash}",
            url.path(),
            canonical_query(url),
        );
        let scope = format!("{date}/{region}/{service}/aws4_request");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
            sha256_hex(canonical_request.as_bytes())
        );
        let k_date = hmac_sha256(format!("AWS4{secret_access_key}").as_bytes(), date.as_bytes());
        let k_region = hmac_sha256(&k_date, region.as_bytes());
        let k_service = hmac_sha256(&k_region, service.as_bytes());
        let k_signing = hmac_sha256(&k_service, b"aws4_request");
        let sig = hex_of(&hmac_sha256(&k_signing, string_to_sign.as_bytes()));
        (sig, scope, signed_headers)
    }

    /// Sign a request in place: stamps `x-amz-date`,
    /// `x-amz-content-sha256`, and `Authorization`. A buffered body is
    /// hashed; a streaming body signs as `UNSIGNED-PAYLOAD` (the S3
    /// streaming convention).
    pub fn sign_request(
        req: &mut reqwest::Request,
        service: &str,
        region: &str,
        access_key_id: &str,
        secret_access_key: &str,
        at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), String> {
        let timestamp = at.format("%Y%m%dT%H%M%SZ").to_string();
        let payload_hash = match req.body() {
            None => sha256_hex(b""),
            Some(body) => match body.as_bytes() {
                Some(bytes) => sha256_hex(bytes),
                None => "UNSIGNED-PAYLOAD".to_string(),
            },
        };
        let host = req
            .url()
            .host_str()
            .map(|h| match req.url().port() {
                Some(p) => format!("{h}:{p}"),
                None => h.to_string(),
            })
            .ok_or("sigv4: request URL has no host")?;
        // The three headers every S3-class request signs. Sorted by name.
        let headers = vec![
            ("host".to_string(), host),
            ("x-amz-content-sha256".to_string(), payload_hash.clone()),
            ("x-amz-date".to_string(), timestamp.clone()),
        ];
        let (sig, scope, signed_headers) = signature(
            req.method().as_str(),
            req.url(),
            &headers,
            &payload_hash,
            service,
            region,
            secret_access_key,
            at,
        );
        let auth = format!(
            "AWS4-HMAC-SHA256 Credential={access_key_id}/{scope}, \
             SignedHeaders={signed_headers}, Signature={sig}"
        );
        let h = req.headers_mut();
        h.insert("x-amz-date", timestamp.parse().expect("timestamp is ascii"));
        h.insert(
            "x-amz-content-sha256",
            payload_hash.parse().expect("hash is ascii"),
        );
        let mut auth_value: reqwest::header::HeaderValue =
            auth.parse().expect("signature is ascii");
        auth_value.set_sensitive(true);
        h.insert(reqwest::header::AUTHORIZATION, auth_value);
        Ok(())
    }
}

/// OAuth 1.0a request signing, HMAC-SHA1 (X/Twitter). Pure core pinned
/// to the RFC/Twitter documentation vector; the request wrapper
/// collects query (and form-body) parameters and sets `Authorization`.
pub mod oauth1a {
    use super::*;
    use sha1::Sha1;

    /// RFC 3986 percent-encode (the OAuth 1.0a rule).
    fn pct(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        for b in s.bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                    out.push(b as char)
                }
                _ => out.push_str(&format!("%{b:02X}")),
            }
        }
        out
    }

    /// The pure signature: base string over method, base URL (no
    /// query), and ALL parameters (query + form body + oauth params),
    /// signed HMAC-SHA1, base64. Returns (signature, oauth params used)
    /// so the wrapper can assemble the header without recomputing.
    #[allow(clippy::too_many_arguments)]
    pub fn signature(
        method: &str,
        base_url: &str,
        request_params: &[(String, String)],
        consumer_key: &str,
        consumer_secret: &str,
        token: &str,
        token_secret: &str,
        timestamp: i64,
        nonce: &str,
    ) -> (String, Vec<(String, String)>) {
        let oauth_params: Vec<(String, String)> = vec![
            ("oauth_consumer_key".into(), consumer_key.into()),
            ("oauth_nonce".into(), nonce.into()),
            ("oauth_signature_method".into(), "HMAC-SHA1".into()),
            ("oauth_timestamp".into(), timestamp.to_string()),
            ("oauth_token".into(), token.into()),
            ("oauth_version".into(), "1.0".into()),
        ];
        let mut all: Vec<(String, String)> = request_params
            .iter()
            .chain(oauth_params.iter())
            .map(|(k, v)| (pct(k), pct(v)))
            .collect();
        all.sort();
        let param_string =
            all.into_iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");
        let base_string =
            format!("{}&{}&{}", method.to_uppercase(), pct(base_url), pct(&param_string));
        let signing_key = format!("{}&{}", pct(consumer_secret), pct(token_secret));
        let mut mac = Hmac::<Sha1>::new_from_slice(signing_key.as_bytes())
            .expect("hmac accepts any key length");
        mac.update(base_string.as_bytes());
        let sig = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
        (sig, oauth_params)
    }

    /// Sign a request in place: sets the `Authorization: OAuth ...`
    /// header. Query parameters are always part of the signature; a
    /// form-encoded body's parameters are too (the OAuth 1.0a rule);
    /// other body kinds (JSON) are excluded by the spec.
    pub fn sign_request(
        req: &mut reqwest::Request,
        consumer_key: &str,
        consumer_secret: &str,
        token: &str,
        token_secret: &str,
        timestamp: i64,
        nonce: &str,
    ) -> Result<(), String> {
        let mut params: Vec<(String, String)> = req
            .url()
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        let is_form = req
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("application/x-www-form-urlencoded"));
        if is_form {
            if let Some(bytes) = req.body().and_then(|b| b.as_bytes()) {
                for (k, v) in url::form_urlencoded::parse(bytes) {
                    params.push((k.into_owned(), v.into_owned()));
                }
            }
        }
        let mut base_url = req.url().clone();
        base_url.set_query(None);
        base_url.set_fragment(None);
        let (sig, oauth_params) = signature(
            req.method().as_str(),
            base_url.as_str(),
            &params,
            consumer_key,
            consumer_secret,
            token,
            token_secret,
            timestamp,
            nonce,
        );
        let mut header_params: Vec<(String, String)> = oauth_params;
        header_params.push(("oauth_signature".into(), sig));
        header_params.sort();
        let header = format!(
            "OAuth {}",
            header_params
                .into_iter()
                .map(|(k, v)| format!("{k}=\"{}\"", pct(&v)))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let mut value: reqwest::header::HeaderValue =
            header.parse().map_err(|_| "oauth1a header assembly produced illegal bytes")?;
        value.set_sensitive(true);
        req.headers_mut().insert(reqwest::header::AUTHORIZATION, value);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::spec::{AuthStep, SignKind, Template};
    use chrono::TimeZone;

    fn values(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    fn get(url: &str) -> reqwest::Request {
        reqwest::Request::new(reqwest::Method::GET, url.parse().unwrap())
    }

    fn t0() -> chrono::DateTime<chrono::Utc> {
        chrono::Utc.with_ymd_and_hms(2015, 8, 30, 12, 36, 0).unwrap()
    }

    #[tokio::test]
    async fn a_failed_declared_call_never_echoes_interpolated_values() {
        // Port 9 on localhost refuses; the send fails, and the error must
        // name the call by its TEMPLATE, never the resolved URL (the query
        // interpolates a secret here).
        let call: crate::access::spec::ConnectCall = serde_json::from_value(serde_json::json!({
            "url": "http://127.0.0.1:9/exchange?client_secret={client_secret}",
        }))
        .unwrap();
        let err = run_connect_call(&call, &values(&[("client_secret", "s3cr3t-value")]))
            .await
            .unwrap_err();
        assert!(err.contains("{client_secret}"), "names the template: {err}");
        assert!(!err.contains("s3cr3t-value"), "echoed the secret: {err}");
    }

    #[test]
    fn header_query_basic_and_path_prefix_apply() {
        let steps = resolve_steps(
            &[
                AuthStep::Header {
                    name: "Authorization".into(),
                    value: Template::new("Bearer {token}"),
                },
                AuthStep::Query { name: "key".into(), value: Template::new("{token}") },
                AuthStep::PathPrefix { value: Template::new("/bot{token}") },
            ],
            &values(&[("token", "xoxb-1")]),
        )
        .unwrap();
        let mut req = get("https://api.telegram.org/sendMessage?chat_id=5");
        apply_steps(&mut req, &steps, t0(), "n").unwrap();
        assert_eq!(req.headers()["authorization"], "Bearer xoxb-1");
        assert_eq!(req.url().path(), "/botxoxb-1/sendMessage");
        assert!(req.url().query().unwrap().contains("key=xoxb-1"));
        assert!(req.headers()["authorization"].is_sensitive());

        let steps = resolve_steps(
            &[AuthStep::Basic {
                username: Template::new("{sid}"),
                password: Template::new("{secret}"),
            }],
            &values(&[("sid", "AC1"), ("secret", "s3")]),
        )
        .unwrap();
        let mut req = get("https://api.twilio.com/x");
        apply_steps(&mut req, &steps, t0(), "n").unwrap();
        // base64("AC1:s3")
        assert_eq!(req.headers()["authorization"], "Basic QUMxOnMz");
    }

    /// base_url re-aims the node's stand-in base at the stored one,
    /// keeping the node's path + query; a base carrying its own path
    /// prefixes it. Signing declared alongside covers the final URL.
    #[test]
    fn base_url_reaims_the_request() {
        let steps = resolve_steps(
            &[AuthStep::BaseUrl { value: Template::new("{endpoint}") }],
            &values(&[("endpoint", "https://minio.example:9000")]),
        )
        .unwrap();
        let mut req = get("https://service/bucket/key.txt?list-type=2");
        apply_steps(&mut req, &steps, t0(), "n").unwrap();
        assert_eq!(req.url().as_str(), "https://minio.example:9000/bucket/key.txt?list-type=2");

        let steps = resolve_steps(
            &[AuthStep::BaseUrl { value: Template::new("{instance}") }],
            &values(&[("instance", "https://acme.my.salesforce.com/services/data")]),
        )
        .unwrap();
        let mut req = get("https://service/v60.0/query");
        apply_steps(&mut req, &steps, t0(), "n").unwrap();
        assert_eq!(
            req.url().as_str(),
            "https://acme.my.salesforce.com/services/data/v60.0/query"
        );
    }

    /// A stored base carries the credential's destination, so a plain
    /// http base is refused for any publicly reachable host; a
    /// private-network host (loopback, RFC-1918: a local MinIO, an
    /// in-cluster SeaweedFS) is where cleartext never crosses the
    /// public internet.
    #[test]
    fn a_base_url_must_be_https_unless_private() {
        let apply_base = |base: &str| {
            let steps = resolve_steps(
                &[AuthStep::BaseUrl { value: Template::new("{endpoint}") }],
                &values(&[("endpoint", base)]),
            )
            .unwrap();
            let mut req = get("https://service/bucket/key.txt");
            apply_steps(&mut req, &steps, t0(), "n").map(|_| req.url().to_string())
        };

        let err = apply_base("http://minio.example:9000").unwrap_err();
        assert!(err.contains("must be https") && err.contains("'http'"), "{err}");
        let err = apply_base("http://8.8.8.8:9000").unwrap_err();
        assert!(err.contains("must be https"), "{err}");

        assert_eq!(
            apply_base("http://127.0.0.1:9000").unwrap(),
            "http://127.0.0.1:9000/bucket/key.txt"
        );
        assert_eq!(
            apply_base("http://172.18.0.1:9096").unwrap(),
            "http://172.18.0.1:9096/bucket/key.txt"
        );
        assert_eq!(
            apply_base("http://192.168.1.20:9000").unwrap(),
            "http://192.168.1.20:9000/bucket/key.txt"
        );
        assert_eq!(
            apply_base("https://minio.example:9000").unwrap(),
            "https://minio.example:9000/bucket/key.txt"
        );
    }

    /// The built body frames each payload verbatim, and the minted
    /// boundary occurs ONLY as framing: parts + 1 bare `--<boundary>`
    /// markers, none contributed by content.
    #[test]
    fn multipart_boundary_is_absent_from_every_payload() {
        let payload = b"binary \xff\x00 --weft-looks-like-a-marker".to_vec();
        let (content_type, body) = Multipart::form_data()
            .text("purpose", "ocr")
            .file("file", "doc.pdf", "application/pdf", payload.clone())
            .build();
        let boundary = content_type
            .strip_prefix("multipart/form-data; boundary=")
            .expect("the content type carries the boundary");
        let marker = format!("--{boundary}");
        let hits = body
            .windows(marker.len())
            .filter(|w| *w == marker.as_bytes())
            .count();
        assert_eq!(hits, 3, "two part openers + the terminator, nothing from content");
        assert!(
            body.windows(payload.len()).any(|w| w == payload.as_slice()),
            "the file bytes ride verbatim"
        );
        // The collision scan itself, pinned deterministically: a
        // boundary occurring in a payload (or a header) is refused.
        let parts = [MultipartPart {
            headers: "Content-Type: text/plain".into(),
            payload: b"data --clash data".to_vec(),
        }];
        assert!(boundary_collides(&parts, "clash"));
        assert!(!boundary_collides(&parts, "no-clash"));
    }

    /// A filename with quotes, backslashes, and CR/LF lands escaped in
    /// the Content-Disposition instead of breaking the header framing.
    #[test]
    fn multipart_filenames_escape_quotes_and_backslashes() {
        let (_, body) = Multipart::form_data()
            .file("file", "a\"b\\c\r\nd.pdf", "application/pdf", b"x".to_vec())
            .build();
        let text = String::from_utf8_lossy(&body);
        assert!(text.contains(r#"filename="a\"b\\c__d.pdf""#), "{text}");
    }

    /// form-data parts are name-addressed; related parts are typed and
    /// positional (no Content-Disposition at all).
    #[test]
    fn multipart_related_and_form_data_frame_their_shapes() {
        let (content_type, body) = Multipart::related()
            .part("application/json; charset=UTF-8", b"{\"name\":\"x\"}".to_vec())
            .part("application/pdf", b"bytes".to_vec())
            .build();
        assert!(content_type.starts_with("multipart/related; boundary="));
        let text = String::from_utf8_lossy(&body);
        assert!(!text.contains("Content-Disposition"), "{text}");
        assert!(text.contains("Content-Type: application/json; charset=UTF-8\r\n\r\n{\"name\":\"x\"}\r\n"), "{text}");
        assert!(text.ends_with("--\r\n"), "{text}");

        let (content_type, body) =
            Multipart::form_data().text("chat_id", "42").build();
        assert!(content_type.starts_with("multipart/form-data; boundary="));
        let text = String::from_utf8_lossy(&body);
        assert!(text.contains("Content-Disposition: form-data; name=\"chat_id\"\r\n\r\n42\r\n"), "{text}");
    }

    /// `required_str` hands back a present string field and fails loud
    /// (naming the call and the field) on absence or a non-string.
    #[cfg(feature = "runtime")]
    #[test]
    fn required_str_reads_or_names_the_missing_field() {
        let answer = serde_json::json!({ "id": "f1", "n": 7 });
        assert_eq!(required_str(&answer, "copy", "id").unwrap(), "f1");
        let err = required_str(&answer, "copy", "webViewLink").unwrap_err().to_string();
        assert!(err.contains("copy") && err.contains("webViewLink"), "{err}");
        assert!(required_str(&answer, "copy", "n").is_err());
    }

    #[test]
    fn resolve_steps_fails_loud_on_unknown_value() {
        let e = resolve_steps(
            &[AuthStep::Header { name: "A".into(), value: Template::new("{nope}") }],
            &values(&[("token", "t")]),
        )
        .unwrap_err();
        assert!(e.contains("'nope'"), "{e}");
    }

    /// The official AWS documentation vector (GET iam ListUsers,
    /// 2015-08-30, us-east-1): pins the canonicalization, the key
    /// derivation, and the final signature exactly.
    #[test]
    fn sigv4_matches_the_aws_documentation_vector() {
        let url: url::Url =
            "https://iam.amazonaws.com/?Action=ListUsers&Version=2010-05-08".parse().unwrap();
        let headers = vec![
            (
                "content-type".to_string(),
                "application/x-www-form-urlencoded; charset=utf-8".to_string(),
            ),
            ("host".to_string(), "iam.amazonaws.com".to_string()),
            ("x-amz-date".to_string(), "20150830T123600Z".to_string()),
        ];
        let (sig, scope, signed) = sigv4::signature(
            "GET",
            &url,
            &headers,
            &sha256_hex(b""),
            "iam",
            "us-east-1",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            t0(),
        );
        assert_eq!(scope, "20150830/us-east-1/iam/aws4_request");
        assert_eq!(signed, "content-type;host;x-amz-date");
        assert_eq!(sig, "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7");
    }

    /// A key with a space and a `+` signs over the path EXACTLY as it
    /// goes on the wire (`/bkt/a%20b%2Bc.txt`, encoded once). The hex
    /// is a pinned regression vector: re-encoding the already-encoded
    /// path in the canonical request (`%20` -> `%2520`) flips it, and
    /// that double-encode is precisely the bug that made every
    /// non-trivial S3 key answer 403 SignatureDoesNotMatch.
    #[test]
    fn sigv4_signs_the_wire_path_once_for_encoded_keys() {
        let url: url::Url = "https://bkt.s3.amazonaws.com/bkt/a%20b%2Bc.txt".parse().unwrap();
        assert_eq!(url.path(), "/bkt/a%20b%2Bc.txt", "the wire path keeps the single encode");
        let headers = vec![
            ("host".to_string(), "bkt.s3.amazonaws.com".to_string()),
            ("x-amz-date".to_string(), "20150830T123600Z".to_string()),
        ];
        let (sig, ..) = sigv4::signature(
            "GET",
            &url,
            &headers,
            &sha256_hex(b""),
            "s3",
            "us-east-1",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            t0(),
        );
        assert_eq!(sig, "f77ded97ed653b5aa527347295b0c735877e7e040996f070cbe677f8816efd22");
    }

    #[test]
    fn sigv4_request_wrapper_stamps_the_three_headers() {
        let steps = resolve_steps(
            &[AuthStep::Sign {
                with: SignKind::SigV4 {
                    service: "s3".into(),
                    region: Template::new("{region}"),
                    access_key_id: Template::new("{access_key_id}"),
                    secret_access_key: Template::new("{secret_access_key}"),
                },
            }],
            &values(&[
                ("region", "us-east-1"),
                ("access_key_id", "AKIDEXAMPLE"),
                ("secret_access_key", "shh"),
            ]),
        )
        .unwrap();
        let mut req = get("https://bucket.s3.amazonaws.com/key.txt");
        apply_steps(&mut req, &steps, t0(), "n").unwrap();
        assert_eq!(req.headers()["x-amz-date"], "20150830T123600Z");
        assert_eq!(req.headers()["x-amz-content-sha256"], sha256_hex(b"").as_str());
        let auth = req.headers()["authorization"].to_str().unwrap();
        assert!(auth.starts_with("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/s3/aws4_request"), "{auth}");
        assert!(auth.contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date"), "{auth}");
        assert!(req.headers()["authorization"].is_sensitive());
    }

    /// The X/Twitter API documentation vector ("creating a signature"):
    /// pins parameter collection (query + form body + oauth params),
    /// the base string, and the HMAC-SHA1 signature.
    #[test]
    fn oauth1a_matches_the_twitter_documentation_vector() {
        let params = vec![
            ("include_entities".to_string(), "true".to_string()),
            (
                "status".to_string(),
                "Hello Ladies + Gentlemen, a signed OAuth request!".to_string(),
            ),
        ];
        let (sig, _) = oauth1a::signature(
            "POST",
            "https://api.twitter.com/1.1/statuses/update.json",
            &params,
            "xvz1evFS4wEEPTGEFPHBog",
            "kAcSOqF21Fu85e7zjz7ZN2U4ZRhfV3WpwPAoE3Z7kBw",
            "370773112-GmHxMAgYyLbNEtIKZeRNFsMKPR9EyMZeS9weJAEb",
            "LswwdoUaIvS8ltyTt5jkRh4J50vUPVVHtR2YPi5kE",
            1318622958,
            "kYjzVBB8Y0ZFabxSWbWovY3uYSQ2pTgmZeNu2VS4cg",
        );
        assert_eq!(sig, "hCtSmYh+iHYCEqBWrE7C7hYmtUk=");
    }

    #[test]
    fn oauth1a_request_wrapper_signs_query_and_form_body() {
        let steps = resolve_steps(
            &[AuthStep::Sign {
                with: SignKind::OAuth1a {
                    consumer_key: Template::new("{ck}"),
                    consumer_secret: Template::new("{cs}"),
                    token: Template::new("{t}"),
                    token_secret: Template::new("{ts}"),
                },
            }],
            &values(&[("ck", "k"), ("cs", "s"), ("t", "tk"), ("ts", "tss")]),
        )
        .unwrap();
        let mut req = get("https://api.twitter.com/2/tweets?max_results=5");
        apply_steps(&mut req, &steps, t0(), "fixed-nonce").unwrap();
        let auth = req.headers()["authorization"].to_str().unwrap();
        assert!(auth.starts_with("OAuth "), "{auth}");
        assert!(auth.contains("oauth_consumer_key=\"k\""), "{auth}");
        assert!(auth.contains("oauth_nonce=\"fixed-nonce\""), "{auth}");
        assert!(auth.contains("oauth_signature=\""), "{auth}");
    }

    /// Signing runs LAST even when declared first, so the signature
    /// covers a path-prefix rewrite.
    #[test]
    fn signing_covers_prior_transforms() {
        let vals = values(&[
            ("region", "r"),
            ("access_key_id", "id"),
            ("secret_access_key", "sec"),
            ("token", "tok"),
        ]);
        let steps = resolve_steps(
            &[
                AuthStep::Sign {
                    with: SignKind::SigV4 {
                        service: "s3".into(),
                        region: Template::new("{region}"),
                        access_key_id: Template::new("{access_key_id}"),
                        secret_access_key: Template::new("{secret_access_key}"),
                    },
                },
                AuthStep::PathPrefix { value: Template::new("/p{token}") },
            ],
            &vals,
        )
        .unwrap();
        let mut req = get("https://h.example/obj");
        apply_steps(&mut req, &steps, t0(), "n").unwrap();
        assert_eq!(req.url().path(), "/ptok/obj");
        // Recompute what the signature over the REWRITTEN path would be;
        // the request must carry exactly that (i.e. the prefix was
        // applied before signing).
        let headers = vec![
            ("host".to_string(), "h.example".to_string()),
            ("x-amz-content-sha256".to_string(), sha256_hex(b"")),
            ("x-amz-date".to_string(), "20150830T123600Z".to_string()),
        ];
        let (expected, ..) = sigv4::signature(
            "GET",
            req.url(),
            &headers,
            &sha256_hex(b""),
            "s3",
            "r",
            "sec",
            t0(),
        );
        let auth = req.headers()["authorization"].to_str().unwrap();
        assert!(auth.ends_with(&format!("Signature={expected}")), "{auth}");
    }
}

