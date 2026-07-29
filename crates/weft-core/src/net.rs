//! Raw outbound pipes: dial a `host:port` address over TCP,
//! optionally under TLS with the system's trust roots. The ONE
//! implementation for everything in weft that holds a non-HTTP
//! connection: the listener's raw-pipe engine dials through here,
//! and node code speaking a wire protocol (IMAP, SMTP beyond what
//! its library wraps, a message broker) hands the connected stream
//! to its protocol library from here, instead of assembling its own
//! TLS stack.

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
