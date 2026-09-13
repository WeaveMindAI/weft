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
/// Called at every binary's startup (and the worker's pod entry): the
/// dependency graph carries TWO providers (ring everywhere, aws-lc-rs
/// via the S3 stack), and rustls refuses to guess between them, so any
/// library that builds TLS from the process default (a WebSocket
/// client, a mail library) would panic at its first dial. Installing
/// the default up front means every present AND future dependency
/// builds TLS from a provider that is actually installed; weft's own
/// dialers additionally pin ring explicitly in
/// [`tls_config`]. Idempotent (a second install is a no-op).
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

/// The absolute base URL a client used to reach this service, from the
/// request's own `Host` (and `X-Forwarded-Proto` when a proxy fronted
/// it), or `None` when the request carries no usable host.
///
/// Why a request-derived base rather than a configured one: a link a
/// client is about to fetch has exactly one correct host, the one that
/// client can already reach, and only the request knows it. The same
/// install is reached at several addresses at once (a port-forward on
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
/// Trust: `Host` is attacker-controlled on a direct request, so the
/// links this builds are only ever capability URLs whose token is the
/// credential. A forged host redirects the forger to their own server
/// with a token they already had, which grants them nothing new.
pub fn request_base_url(headers: &http::HeaderMap) -> Option<String> {
    let host = headers.get(http::header::HOST)?.to_str().ok()?.trim();
    if host.is_empty() {
        return None;
    }
    // A proxy that terminated TLS says so; otherwise the scheme is
    // whatever this listener speaks, which is plain http.
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.split(',').next().unwrap_or(v).trim().to_string())
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
    Some(parsed.origin().ascii_serialization())
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

    /// The caller's own host is the base, and a fronting proxy's
    /// scheme wins over the listener's plain http.
    #[test]
    fn the_base_is_the_host_the_caller_used() {
        assert_eq!(
            request_base_url(&headers(&[("host", "127.0.0.1:9998")])).as_deref(),
            Some("http://127.0.0.1:9998")
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
            "http://127.0.0.1:9999",
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
