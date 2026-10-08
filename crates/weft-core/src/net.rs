//! The network edges, in and out.
//!
//! OUTBOUND: raw pipes. Dial a `host:port` address over TCP, optionally
//! under TLS with the system's trust roots. The ONE implementation for
//! everything in weft that holds a non-HTTP connection: the listener's
//! raw-pipe engine dials through here, and node code speaking a wire
//! protocol (IMAP, SMTP beyond what its library wraps, a message broker)
//! hands the connected stream to its protocol library from here, instead
//! of assembling its own TLS stack.
//!
//! INBOUND: [`request_base_url`], the address a request arrived at. One
//! install answers at several addresses at once, so a link the caller
//! will fetch is built from that caller's own request rather than from
//! anything configured.
//!
//! OUTBOUND HTTP: [`EmptyBody`], for a call that sends nothing.

use std::sync::Arc;

use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls;

/// Whether a URL points at a loopback-only address (localhost or a
/// loopback IP). An unparseable URL counts as loopback: the callers
/// use this to decide "is this address reachable from outside", and
/// failing toward the teaching error beats acting on an address
/// nobody could reach.
pub fn is_loopback_url(url: &str) -> bool {
    url::Url::parse(url)
        .ok()
        .and_then(|u| {
            u.host_str().map(|h| {
                // An IPv6 host keeps its brackets in host_str.
                let bare = h.trim_start_matches('[').trim_end_matches(']');
                h.eq_ignore_ascii_case("localhost")
                    || bare
                        .parse::<std::net::IpAddr>()
                        .map(|ip| ip.is_loopback())
                        .unwrap_or(false)
            })
        })
        .unwrap_or(true)
}

/// Install ring as the process-level rustls crypto provider, once.
/// Called at every binary's startup (and the worker's process entry).
/// ring is the only provider weft builds (the S3 stack is handed a ring
/// client too), but rustls refuses to guess the moment a dependency (a
/// node's library, a future crate) brings a second one, and any library
/// that builds TLS from the process default (a WebSocket client, a mail
/// library) would then panic at its first dial. Installing the default up
/// front means every present AND future dependency builds TLS from a
/// provider that is actually installed; weft's own dialers additionally
/// pin ring explicitly in [`tls_config`]. Idempotent (a second install is
/// a no-op).
pub fn install_crypto_provider() {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
}

/// Dial `host:port` and wrap the pipe in TLS, verifying the peer
/// against the system's trust roots (SNI from the host part).
pub async fn tls_connect(address: &str) -> Result<TlsStream<TcpStream>, String> {
    let tcp = TcpStream::connect(address)
        .await
        .map_err(|e| format!("connecting to {address}: {e}"))?;
    let host = address.rsplit_once(':').map(|(h, _)| h).unwrap_or(address).to_string();
    let server_name = rustls::pki_types::ServerName::try_from(host.clone())
        .map_err(|e| format!("'{host}' is not a valid TLS server name: {e}"))?;
    let connector = tokio_rustls::TlsConnector::from(tls_config()?);
    connector
        .connect(server_name, tcp)
        .await
        .map_err(|e| format!("TLS handshake with {address} failed: {e}"))
}

/// One TLS client config for the whole process, built once: system
/// roots, ring provider (chosen explicitly; the process may carry a
/// second provider through other dependencies), safe defaults.
pub fn tls_config() -> Result<Arc<rustls::ClientConfig>, String> {
    static CONFIG: std::sync::OnceLock<Result<Arc<rustls::ClientConfig>, String>> =
        std::sync::OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let mut roots = rustls::RootCertStore::empty();
            let native = rustls_native_certs::load_native_certs();
            for cert in native.certs {
                // Individual unparsable certs in a system store are
                // common; the load fails only when NOTHING loads.
                let _ = roots.add(cert);
            }
            if roots.is_empty() {
                return Err(format!(
                    "no usable system trust roots were found (errors: {:?}); TLS pipes \
                     cannot verify any peer",
                    native.errors
                ));
            }
            rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .map_err(|e| format!("TLS protocol setup failed: {e}"))
            .map(|b| Arc::new(b.with_root_certificates(roots).with_no_client_auth()))
        })
        .clone()
}

/// The scheme a caller used, as weft's own doors pass it on. It wins over
/// `X-Forwarded-Proto`, which Cloud Run's front end replaces with the
/// https it was itself reached on, whatever the caller used before it.
// SYNC: WEFT_FORWARDED_PROTO <-> packages/weft-connect/src/server/passthrough.ts WEFT_FORWARDED_PROTO
pub const WEFT_FORWARDED_PROTO: &str = "x-weft-forwarded-proto";

/// What weft's relay tells a project's worker about a call it passed on
/// from an address the install shares (`/connect/<tenant>/<project>/...`, an API
/// domain, a local project's port): where the caller stood, which the
/// worker cannot see past the relay. A worker reads these only on a hop
/// that carries weft's own credential, and removes them (with that
/// credential) before anything else reads the request, so a run sees the
/// call as its caller sent it.
pub mod relay_hop {
    /// The caller's address, as the relay read it.
    pub const CALLER_ADDRESS: &str = "x-weft-caller-address";
    /// The `Host` the caller sent the relay, which the hop to the worker
    /// replaces with the worker's own.
    pub const CALLER_HOST: &str = "x-weft-caller-host";
    /// The path the project's routes sit under at that address
    /// (`/connect/<tenant>/<project>`, or empty at a project's own address): what a
    /// browser's socket URL is built on.
    pub const ROUTE_PREFIX: &str = "x-weft-route-prefix";
    /// Every one of them.
    pub const ALL: [&str; 3] = [CALLER_ADDRESS, CALLER_HOST, ROUTE_PREFIX];
}

/// The absolute base URL a client used to reach this service, from the
/// request's own `Host` (and `X-Forwarded-Proto` when a proxy fronted
/// it), or `None` when the request carries no usable host.
///
/// Why a request-derived base rather than a configured one: a link a
/// client is about to fetch has exactly one correct host, the one that
/// client can already reach, and only the request knows it. The same
/// install is reached at several addresses at once (a mapped port on
/// the operator's loopback, a tunnel's public name, an ingress host),
/// so any single configured value is wrong for every caller arriving
/// at one of the others. Deriving it per request is right for all of
/// them at once and stays right when an address is added or changes,
/// with nothing to reconfigure.
///
/// This is for links the CALLER will fetch. A link handed to someone
/// who is NOT the caller (a provider's webhook target, an address
/// pasted into another machine) must stay on a configured, stable
/// address instead: the requester's host says nothing about what a
/// third party can reach.
///
/// A proxy in front (a website passing a browser's request through,
/// mounted under a path of its own) says where the caller really was:
/// `X-Forwarded-Host` wins over `Host`, `X-Forwarded-Proto` names the
/// scheme, and `X-Forwarded-Prefix` is the path the proxy is mounted
/// under (`/weft`), which the base then ends with. Each takes its first
/// hop when a chain of proxies lists several. A prefix that is not a
/// plain path gives no base at all, rather than a link to somewhere
/// the caller never was.
///
/// Trust: these headers are attacker-controlled on a direct request, so
/// the links this builds are only ever capability URLs whose token is
/// the credential. A forged host redirects the forger to their own
/// server with a token they already had, which grants them nothing new.
// SYNC: the forwarded headers <-> packages/weft-connect/src/server/passthrough.ts FORWARDED_HOST, FORWARDED_PROTO, WEFT_FORWARDED_PROTO, FORWARDED_PREFIX
pub fn request_base_url(headers: &http::HeaderMap) -> Option<String> {
    let host = match headers.get("x-forwarded-host") {
        Some(forwarded) => first_hop(forwarded)?,
        None => headers.get(http::header::HOST)?.to_str().ok()?.trim(),
    };
    if host.is_empty() {
        return None;
    }
    let prefix = match headers.get("x-forwarded-prefix") {
        Some(forwarded) => forwarded_prefix(first_hop(forwarded)?)?,
        None => String::new(),
    };
    // A proxy that terminated TLS says so; otherwise the scheme is
    // whatever this listener speaks, which is plain http.
    let scheme = headers
        .get(WEFT_FORWARDED_PROTO)
        .or_else(|| headers.get("x-forwarded-proto"))
        .and_then(first_hop)
        .map(|v| v.to_ascii_lowercase())
        .filter(|v| v == "http" || v == "https")
        .unwrap_or_else(|| "http".to_string());
    let base = format!("{scheme}://{host}");
    // Two separate jobs here, and the second one is what makes this
    // safe rather than nearly safe.
    //
    // First, REFUSE anything that is not a bare authority. `Url::parse`
    // accepts plenty that is not: `Host: a.example/x` parses fine and
    // would hand callers a base with a path already on it,
    // `Host: evil@good.example` parses with userinfo, and `Host: a.example/`
    // parses with path "/", which is what a bare authority parses to as
    // well, so the parse cannot see that one at all and the host's own
    // text has to be checked for the slash.
    //
    // Second, return the PARSE's own spelling of the authority, never
    // the text we formatted. Those are two different strings whenever
    // the parse normalizes anything (case, a default port), and handing
    // back the raw text meant every such difference was one more thing
    // the checks above had to have thought of. The origin is a bare
    // authority by construction, so a link path concatenated onto it
    // lands where it says it does.
    let parsed = url::Url::parse(&base).ok()?;
    if host.contains('/') {
        return None;
    }
    let bare_authority = parsed.path() == "/"
        && parsed.query().is_none()
        && parsed.fragment().is_none()
        && parsed.username().is_empty()
        && parsed.password().is_none();
    if !bare_authority {
        return None;
    }
    Some(format!("{}{prefix}", parsed.origin().ascii_serialization()))
}

/// The caller's address: the `X-Forwarded-For` entries followed by the
/// peer the listener saw, read `trusted_hops` from the right. Every
/// trusted proxy appends the address it received from, so the entries a
/// caller sent itself sit further left and are never read. A request
/// that passed fewer proxies than that (straight to the listener's own
/// port) has a shorter list, and its leftmost entry is the caller.
pub fn caller_address(forwarded_for: Option<&str>, peer: std::net::IpAddr, trusted_hops: usize) -> std::net::IpAddr {
    let mut chain: Vec<std::net::IpAddr> = forwarded_for
        .unwrap_or("")
        .split(',')
        .filter_map(|entry| entry.trim().parse::<std::net::IpAddr>().ok())
        .collect();
    chain.push(peer);
    let index = chain.len().saturating_sub(1).saturating_sub(trusted_hops);
    chain[index]
}

/// A call that sends no body, saying so with `Content-Length: 0`.
///
/// Sent with no body at all, a POST goes out over HTTP/1.1 with no length,
/// and Google's front end, in front of every Cloud Run service, refuses
/// it with `411 Length Required` before the service ever sees it. A local
/// install has nothing in between, so only a cloud install meets it.
/// Offering HTTP/2 instead would get past the front end too, but HTTP/2
/// puts every call to one service on one connection, which Cloud Run caps
/// at 100 calls at once: a call held for a run's whole life (a run handed
/// to a worker) would make the 101st wait. So weft's calls stay HTTP/1.1,
/// one connection each, and a call with nothing to send says its length.
pub trait EmptyBody {
    fn empty_body(self) -> Self;
}

impl EmptyBody for reqwest::RequestBuilder {
    fn empty_body(self) -> Self {
        self.header(reqwest::header::CONTENT_LENGTH, 0)
    }
}

/// The first value of a header a chain of proxies may list comma-separated.
fn first_hop(value: &http::HeaderValue) -> Option<&str> {
    let text = value.to_str().ok()?;
    Some(text.split(',').next().unwrap_or(text).trim())
}

/// A forwarded mount path as a base's tail: `/weft` (a trailing slash
/// dropped), empty for the root. `None` for anything that is not a plain
/// path of ordinary segments, so no query, host or `..` rides into a link.
fn forwarded_prefix(prefix: &str) -> Option<String> {
    let trimmed = prefix.trim_end_matches('/');
    if trimmed.is_empty() {
        return Some(String::new());
    }
    let segments = trimmed.strip_prefix('/')?;
    let plain = |segment: &str| {
        !segment.is_empty()
            && segment != "."
            && segment != ".."
            && segment.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_' | '~'))
    };
    segments.split('/').all(plain).then(|| trimmed.to_string())
}

#[cfg(test)]
mod request_base_url_tests {
    /// The base is the PARSE's authority, not the header text, so a
    /// spelling the parse normalizes cannot reach a caller's link.
    #[test]
    fn the_base_is_the_parsed_authority() {
        assert_eq!(
            request_base_url(&headers(&[("host", "API.Example:80")])).as_deref(),
            Some("http://api.example"),
        );
        assert_eq!(
            request_base_url(&headers(&[("host", "api.example:8080")])).as_deref(),
            Some("http://api.example:8080"),
        );
    }

    use super::request_base_url;

    fn headers(pairs: &[(&str, &str)]) -> http::HeaderMap {
        let mut h = http::HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                http::HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    /// A request a door relayed yields the door's forwarded host and
    /// scheme, not the worker's own address.
    #[test]
    fn a_relayed_request_yields_the_door_it_came_through() {
        assert_eq!(
            request_base_url(&headers(&[
                ("host", "10.10.0.2:14113"),
                ("x-forwarded-host", "127.0.0.1:14111"),
                ("x-forwarded-proto", "http"),
            ]))
            .as_deref(),
            Some("http://127.0.0.1:14111")
        );
        assert_eq!(
            request_base_url(&headers(&[
                ("host", "10.10.0.2:14113"),
                ("x-forwarded-host", "weft.example.com"),
                ("x-forwarded-proto", "https"),
            ]))
            .as_deref(),
            Some("https://weft.example.com")
        );
    }

    /// The caller's own host is the base, and a fronting proxy's
    /// scheme wins over the listener's plain http.
    #[test]
    fn the_base_is_the_host_the_caller_used() {
        assert_eq!(
            request_base_url(&headers(&[("host", "127.0.0.1:14112")])).as_deref(),
            Some("http://127.0.0.1:14112")
        );
        assert_eq!(
            request_base_url(&headers(&[
                ("host", "weft-dev-copper-lantern.weavemind.ai"),
                ("x-forwarded-proto", "https"),
            ]))
            .as_deref(),
            Some("https://weft-dev-copper-lantern.weavemind.ai")
        );
    }

    /// A proxy chain lists the original scheme first.
    #[test]
    fn a_chained_forwarded_proto_takes_the_first_hop() {
        assert_eq!(
            request_base_url(&headers(&[("host", "a.example.com"), ("x-forwarded-proto", "https, http")])).as_deref(),
            Some("https://a.example.com")
        );
    }

    /// No host, an empty one, or a scheme that is not http(s): no base.
    #[test]
    fn a_host_that_is_not_a_bare_authority_has_no_base() {
        // Each of these parses as a URL and would hand the caller a
        // base that is not the address they asked at.
        // The trailing slash is the subtle one: it parses to the same
        // path a bare authority does, so the parse check alone passed it
        // and the base came back with a slash already on it.
        for bad in ["a.example.com/x", "a.example.com/", "evil@good.example.com", "a.example.com?q=1", "a.example.com#f"] {
            assert_eq!(request_base_url(&headers(&[("host", bad)])), None, "{bad}");
        }
    }

    /// Every shape a real deployment answers at must still produce a
    /// base: rejecting one means no link at all, and the caller then
    /// falls back to a configured address that is wrong for whoever
    /// asked, which is the exact bug this rule exists to prevent.
    #[test]
    fn every_ordinary_host_shape_still_makes_a_base() {
        for host in [
            "a.example.com",
            "a.example.com:8080",
            "127.0.0.1:3000",
            "localhost:8080",
            "[::1]:8080",
            "[2001:db8::1]",
            "a.example.com.",
        ] {
            assert_eq!(
                request_base_url(&headers(&[("host", host)])),
                Some(format!("http://{host}")),
                "{host} must still make a base"
            );
        }
    }

    #[test]
    fn an_uppercase_forwarded_proto_is_still_https() {
        // Proxies are free to spell the token however they like, and a
        // TLS-terminating one spelling it `HTTPS` used to mint `http://`
        // links that the browser then refused as mixed content.
        assert_eq!(
            request_base_url(&headers(&[("host", "a.example.com"), ("x-forwarded-proto", "HTTPS")])),
            Some("https://a.example.com".to_string())
        );
    }

    /// Behind a proxy that passes the request through under a path of
    /// its own, the base is where the caller really was.
    #[test]
    fn a_forwarding_proxy_names_the_host_scheme_and_mount() {
        assert_eq!(
            request_base_url(&headers(&[
                ("host", "10.0.0.7:14111"),
                ("x-forwarded-host", "app.example.com, 10.0.0.2"),
                ("x-forwarded-proto", "https"),
                ("x-forwarded-prefix", "/weft/"),
            ]))
            .as_deref(),
            Some("https://app.example.com/weft")
        );
        assert_eq!(
            request_base_url(&headers(&[("host", "a.example.com"), ("x-forwarded-prefix", "/")])).as_deref(),
            Some("http://a.example.com")
        );
        for bad in ["weft", "/a/../b", "/a//b", "/a?b", "//evil.example", "/a b"] {
            assert_eq!(
                request_base_url(&headers(&[("host", "a.example.com"), ("x-forwarded-prefix", bad)])),
                None,
                "{bad}"
            );
        }
        assert_eq!(
            request_base_url(&headers(&[("host", "a.example.com"), ("x-forwarded-host", "evil@x.example")])),
            None
        );
    }

    #[test]
    fn a_request_without_a_usable_host_has_no_base() {
        assert_eq!(request_base_url(&headers(&[])), None);
        assert_eq!(request_base_url(&headers(&[("host", "  ")])), None);
        assert_eq!(
            request_base_url(&headers(&[("host", "a.example.com"), ("x-forwarded-proto", "gopher")])).as_deref(),
            Some("http://a.example.com"),
            "an unknown scheme falls back to the listener's own"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Loopback detection over the shapes deployments actually
    /// publish, and the fail-toward-unreachable rule: an unparseable
    /// URL counts as loopback so callers meet a teaching error rather
    /// than acting on an address nobody could reach.
    #[test]
    fn loopback_urls_are_told_from_public_ones() {
        for local in [
            "http://localhost:8080",
            "http://LOCALHOST/x",
            "http://127.0.0.1:14111",
            "https://[::1]/events",
            "not a url",
            "",
        ] {
            assert!(is_loopback_url(local), "{local}");
        }
        for public in [
            "https://weft.example.com",
            "http://10.0.0.5:8080",
            "https://tunnel.trycloudflare.com/x",
        ] {
            assert!(!is_loopback_url(public), "{public}");
        }
    }
}

#[cfg(test)]
mod caller_address_tests {
    use super::caller_address;

    fn ip(s: &str) -> std::net::IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn the_caller_is_read_trusted_hops_from_the_right() {
        // Through the tunnel and the front door on kind: the caller, the
        // tunnel's process, then the peer the listener saw (the proxy).
        assert_eq!(caller_address(Some("203.0.113.9, 10.244.0.7"), ip("10.244.0.9"), 2), ip("203.0.113.9"));
        // A forged entry sent by the caller sits further left and is
        // never read.
        assert_eq!(caller_address(Some("1.1.1.1, 203.0.113.9, 10.244.0.7"), ip("10.244.0.9"), 2), ip("203.0.113.9"));
        // Straight to the listener's own port: the peer is the caller.
        assert_eq!(caller_address(None, ip("127.0.0.1"), 2), ip("127.0.0.1"));
        // Garbage entries are skipped, never read as an address.
        assert_eq!(caller_address(Some("not-an-ip, 203.0.113.9"), ip("10.0.0.1"), 1), ip("203.0.113.9"));
        assert_eq!(caller_address(Some("203.0.113.9"), ip("10.0.0.1"), 0), ip("10.0.0.1"));
    }
}

#[cfg(test)]
mod empty_body_tests {
    use super::EmptyBody;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A server on a free port that answers the way Google's front end
    /// does: `411` to a POST that names no length, `204` to anything else.
    async fn length_required_server() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut head = Vec::new();
                    let mut byte = [0u8; 1];
                    while !head.ends_with(b"\r\n\r\n") {
                        if socket.read(&mut byte).await.unwrap_or(0) == 0 {
                            return;
                        }
                        head.push(byte[0]);
                    }
                    let head = String::from_utf8_lossy(&head).to_ascii_lowercase();
                    let status = match head.starts_with("post ") && !head.contains("\r\ncontent-length:") {
                        true => "411 Length Required",
                        false => "204 No Content",
                    };
                    let _ = socket.write_all(format!("HTTP/1.1 {status}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n").as_bytes()).await;
                });
            }
        });
        address
    }

    #[tokio::test]
    async fn a_post_with_nothing_to_send_names_its_length() {
        let url = format!("{}/_weft/tick", length_required_server().await);
        let http = reqwest::Client::new();
        assert_eq!(http.post(&url).send().await.unwrap().status(), 411, "the refusal this guards against");
        assert_eq!(http.post(&url).empty_body().send().await.unwrap().status(), 204);
    }
}
