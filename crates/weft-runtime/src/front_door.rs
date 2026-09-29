//! The machine's front door: HTTPS on its static address.
//!
//! A cloud install's machine is reached straight from the internet, with
//! no load balancer in front, so it terminates TLS itself. It gets its
//! certificates from an ACME authority (Let's Encrypt) over `http-01`: one
//! for the bare address (a short-lived certificate, the only kind issued
//! for an IP), and one per domain the install stores (`crate::domains` in
//! the dispatcher), once that domain's DNS points here. It renews each
//! before a third of its life is left, and keeps them on disk so a
//! restart serves at once.
//!
//! Every request is routed by the name it came for: the address or an
//! install domain reaches everything the public port serves; a project's
//! frontend domain is passed on to where the frontend runs; a project's
//! API domain serves that project's routes at its root. Plain HTTP
//! answers the authority's challenges and sends everything else to HTTPS.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result};
use axum::extract::{ConnectInfo, Request, State};
use axum::http::{HeaderValue, StatusCode, Uri};
use axum::response::{IntoResponse, Redirect, Response};
use axum::Router;
use instant_acme::{Account, AccountCredentials, ChallengeType, Identifier, NewAccount, NewOrder, OrderStatus, RetryPolicy};
use parking_lot::RwLock;
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use tower::ServiceExt as _;
use weft_core::install::{Domain, DomainServes};
use weft_platform_traits::config::FrontDoor;

/// How often the stored domains and the certificates are looked at.
const LOOK_EVERY: Duration = Duration::from_secs(30);

/// How long a name whose certificate could not be had waits before it is
/// asked for again. The authority limits failed validations per name, so
/// asking every look would lock the name out.
const RETRY_AFTER_FAILURE: Duration = Duration::from_secs(15 * 60);

/// The ACME profile the authority issues IP certificates under.
const IP_PROFILE: &str = "shortlived";

/// Where a request is going, by the name it came for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Destination {
    /// The install itself: everything its public port serves.
    Install,
    /// A project's frontend, running at `upstream`.
    Frontend { upstream: String },
    /// A project's API.
    Api { project: uuid::Uuid },
    /// A name this install does not answer at.
    Unknown { host: String },
}

/// Where a request that came for `host` goes. No host (an HTTP/1.0
/// caller) is taken as the address itself.
pub fn destination(host: Option<&str>, address: IpAddr, domains: &[Domain]) -> Destination {
    let Some(host) = host else { return Destination::Install };
    let bare = without_port(host).to_ascii_lowercase();
    if bare.trim_start_matches('[').trim_end_matches(']').parse::<IpAddr>().ok() == Some(address) {
        return Destination::Install;
    }
    match domains.iter().find(|d| d.name == bare) {
        Some(Domain { serves: DomainServes::Install, .. }) => Destination::Install,
        Some(Domain { serves: DomainServes::Frontend { upstream, .. }, .. }) => Destination::Frontend { upstream: upstream.clone() },
        Some(Domain { serves: DomainServes::Api { project }, .. }) => Destination::Api { project: *project },
        None => Destination::Unknown { host: bare },
    }
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

/// Where the path of an API domain's request goes on the install: the
/// live door stays as it is (the handshake sent the caller there), and
/// everything else is one of the project's routes, reached through the
/// handshake under its tenant.
pub fn api_path(tenant: &str, path_and_query: &str) -> String {
    if path_and_query == weft_dispatcher::live_relay::LIVE_PREFIX
        || path_and_query.starts_with(&format!("{}/", weft_dispatcher::live_relay::LIVE_PREFIX))
    {
        return path_and_query.to_string();
    }
    format!("/connect/{tenant}/{}", path_and_query.trim_start_matches('/'))
}

/// What a certificate's name is asked for as.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Wanted {
    Address(IpAddr),
    Domain(String),
}

impl Wanted {
    fn name(&self) -> String {
        match self {
            Self::Address(ip) => ip.to_string(),
            Self::Domain(name) => name.clone(),
        }
    }
}

/// Whether a certificate valid from `not_before` to `not_after` (unix
/// seconds) is due for renewal at `now`: once less than a third of its
/// life is left.
pub fn due_for_renewal(not_before: i64, not_after: i64, now: i64) -> bool {
    let life = (not_after - not_before).max(0);
    now >= not_after - life / 3
}

/// The certificates the door holds, by name. A client that names no
/// server (it connected to the bare address) gets the address's.
struct Certs {
    address: String,
    by_name: RwLock<HashMap<String, Held>>,
}

#[derive(Clone)]
struct Held {
    key: Arc<CertifiedKey>,
    not_before: i64,
    not_after: i64,
}

impl std::fmt::Debug for Certs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Certs").field("names", &self.by_name.read().keys().collect::<Vec<_>>()).finish()
    }
}

impl ResolvesServerCert for Certs {
    fn resolve(&self, hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        let name = hello.server_name().map(str::to_ascii_lowercase).unwrap_or_else(|| self.address.clone());
        self.by_name.read().get(&name).map(|h| h.key.clone())
    }
}

/// A certificate chain and its key, as PEM, made into what the door
/// serves, with its validity.
fn held_from_pem(chain_pem: &str, key_pem: &str) -> Result<Held> {
    use rustls::pki_types::pem::PemObject as _;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    let chain: Vec<CertificateDer<'static>> =
        CertificateDer::pem_slice_iter(chain_pem.as_bytes()).collect::<Result<_, _>>().context("read the certificate chain")?;
    let leaf = chain.first().context("the certificate chain is empty")?;
    let (_, parsed) = x509_parser::parse_x509_certificate(leaf.as_ref()).context("read the certificate")?;
    let (not_before, not_after) = (parsed.validity().not_before.timestamp(), parsed.validity().not_after.timestamp());
    let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes()).context("read the certificate's key")?;
    let signing = rustls::crypto::ring::sign::any_supported_type(&key).context("the certificate's key is of no supported type")?;
    Ok(Held {
        key: Arc::new(CertifiedKey::new(chain, signing)),
        not_before,
        not_after,
    })
}

/// Where the door keeps what it must not ask for again.
struct Disk {
    dir: PathBuf,
}

impl Disk {
    fn account(&self) -> PathBuf {
        self.dir.join("acme-account.json")
    }
    fn chain(&self, name: &str) -> PathBuf {
        self.dir.join("certs").join(format!("{name}.crt"))
    }
    fn key(&self, name: &str) -> PathBuf {
        self.dir.join("certs").join(format!("{name}.key"))
    }

    fn load(&self, name: &str) -> Option<Held> {
        let chain = std::fs::read_to_string(self.chain(name)).ok()?;
        let key = std::fs::read_to_string(self.key(name)).ok()?;
        match held_from_pem(&chain, &key) {
            Ok(held) => Some(held),
            Err(e) => {
                tracing::warn!(target: "weft_runtime::front_door", %name, error = %format!("{e:#}"), "the stored certificate is unreadable; asking for a new one");
                None
            }
        }
    }

    fn store(&self, name: &str, chain: &str, key: &str) -> Result<()> {
        std::fs::create_dir_all(self.dir.join("certs"))?;
        std::fs::write(self.chain(name), chain)?;
        write_private(&self.key(name), key)
    }
}

fn write_private(path: &Path, body: &str) -> Result<()> {
    use std::io::Write as _;
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt as _;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(path).with_context(|| format!("write {}", path.display()))?;
    file.write_all(body.as_bytes())?;
    Ok(())
}

/// The challenges waiting for the authority to fetch: token to key
/// authorization.
type Challenges = Arc<RwLock<HashMap<String, String>>>;

/// The door's ACME account: the stored one, or a new one stored for next
/// time.
async fn account(door: &FrontDoor, disk: &Disk) -> Result<Account> {
    if let Ok(raw) = std::fs::read_to_string(disk.account()) {
        let credentials: AccountCredentials = serde_json::from_str(&raw).context("read the stored ACME account")?;
        return Account::builder()?.from_credentials(credentials).await.context("restore the ACME account");
    }
    let (account, credentials) = Account::builder()?
        .create(
            &NewAccount { contact: &[], terms_of_service_agreed: true, only_return_existing: false },
            door.acme_directory.clone(),
            None,
        )
        .await
        .with_context(|| format!("create an account at {}", door.acme_directory))?;
    std::fs::create_dir_all(&disk.dir)?;
    write_private(&disk.account(), &serde_json::to_string(&credentials)?)?;
    Ok(account)
}

/// Ask the authority for a certificate for `wanted`, answering its
/// `http-01` challenge on the plain port. Answers the chain and its key,
/// as PEM.
async fn issue(account: &Account, wanted: &Wanted, challenges: &Challenges) -> Result<(String, String)> {
    let identifier = match wanted {
        Wanted::Address(ip) => Identifier::Ip(*ip),
        Wanted::Domain(name) => Identifier::Dns(name.clone()),
    };
    let identifiers = [identifier];
    let mut request = NewOrder::new(&identifiers);
    if matches!(wanted, Wanted::Address(_)) {
        request = request.profile(IP_PROFILE);
    }
    let mut order = account.new_order(&request).await.context("place the order")?;
    let mut tokens = Vec::new();
    let answered = async {
        let mut authorizations = order.authorizations();
        while let Some(authorization) = authorizations.next().await {
            let mut authorization = authorization.context("read an authorization")?;
            match authorization.status {
                instant_acme::AuthorizationStatus::Pending => {}
                instant_acme::AuthorizationStatus::Valid => continue,
                other => anyhow::bail!("the authorization is {other:?}"),
            }
            let mut challenge = authorization
                .challenge(ChallengeType::Http01)
                .context("the authority offers no http-01 challenge")?;
            challenges.write().insert(challenge.token.clone(), challenge.key_authorization().as_str().to_string());
            tokens.push(challenge.token.clone());
            challenge.set_ready().await.context("tell the authority the challenge is ready")?;
        }
        Ok(())
    }
    .await;
    let ready = match answered {
        Ok(()) => order.poll_ready(&RetryPolicy::default()).await.context("wait for the authority's validation"),
        Err(e) => Err(e),
    };
    for token in &tokens {
        challenges.write().remove(token);
    }
    let status = ready?;
    anyhow::ensure!(status == OrderStatus::Ready, "the authority did not validate {} (the order is {status:?})", wanted.name());

    let mut params = rcgen::CertificateParams::default();
    params.distinguished_name = rcgen::DistinguishedName::new();
    params.subject_alt_names = vec![match wanted {
        Wanted::Address(ip) => rcgen::SanType::IpAddress(*ip),
        Wanted::Domain(name) => rcgen::SanType::DnsName(name.clone().try_into().context("the domain is not a DNS name")?),
    }];
    let key = rcgen::KeyPair::generate().context("make the certificate's key")?;
    let csr = params.serialize_request(&key).context("make the signing request")?;
    order.finalize_csr(csr.der()).await.context("send the signing request")?;
    let chain = order.poll_certificate(&RetryPolicy::default()).await.context("fetch the certificate")?;
    Ok((chain, key.serialize_pem()))
}

/// Whether `name` resolves to `address` yet (the DNS record the person
/// was told to set is in place).
async fn points_here(name: &str, address: IpAddr) -> bool {
    match tokio::net::lookup_host((name, 443)).await {
        Ok(addrs) => addrs.map(|a| a.ip()).any(|ip| ip == address),
        Err(_) => false,
    }
}

/// What the router in front of everything reads.
#[derive(Clone)]
struct Front {
    inner: Router,
    address: IpAddr,
    domains: Arc<RwLock<Vec<Domain>>>,
    projects: weft_dispatcher::ProjectStore,
    http: reqwest::Client,
}

/// What a caller says about the hops before this door. The door is the
/// first hop (the internet reaches it directly), so anything there was
/// written by the caller, and would otherwise pick the host and scheme
/// the install builds its links from.
const CALLER_WRITTEN: [&str; 5] = ["x-forwarded-for", "x-forwarded-host", "x-forwarded-proto", "x-forwarded-prefix", "forwarded"];

async fn route(State(front): State<Front>, mut request: Request) -> Response {
    request.headers_mut().remove(weft_dispatcher::api::signal::API_PROJECT_HEADER);
    for name in CALLER_WRITTEN {
        request.headers_mut().remove(name);
    }
    let host = request
        .headers()
        .get(axum::http::header::HOST)
        .and_then(|h| h.to_str().ok())
        .map(str::to_string)
        .or_else(|| request.uri().authority().map(|a| a.to_string()));
    // This door terminated TLS: whatever serves the request, here or past
    // a proxy, builds its links for HTTPS at the name the caller used. An
    // HTTP/2 request names it in its authority rather than a `Host`.
    request.headers_mut().insert("x-forwarded-proto", HeaderValue::from_static("https"));
    if let Some(value) = host.as_deref().and_then(|h| HeaderValue::from_str(h).ok()) {
        request.headers_mut().insert("x-forwarded-host", value);
    }
    let to = destination(host.as_deref(), front.address, &front.domains.read());
    match to {
        Destination::Install => front.inner.oneshot(request).await.unwrap_or_else(|never| match never {}),
        Destination::Frontend { upstream } => {
            let path_and_query = request.uri().path_and_query().map(|p| p.as_str().to_string()).unwrap_or_else(|| "/".into());
            let upstream = weft_dispatcher::proxy::Upstream { what: "the frontend", base_url: upstream, auth: None, hold: None };
            weft_dispatcher::proxy::forward(&front.http, upstream, path_and_query, request).await
        }
        Destination::Api { project } => {
            let tenant = match front.projects.tenant_for(project).await {
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
                weft_dispatcher::api::signal::API_PROJECT_HEADER,
                HeaderValue::from_str(&project.to_string()).expect("a uuid is a header value"),
            );
            front.inner.oneshot(request).await.unwrap_or_else(|never| match never {})
        }
        Destination::Unknown { host } => {
            (StatusCode::MISDIRECTED_REQUEST, format!("this install does not answer at '{host}'")).into_response()
        }
    }
}

/// The plain port: the authority's challenges, and a redirect to HTTPS.
fn plain_router(challenges: Challenges) -> Router {
    Router::new()
        .route(
            "/.well-known/acme-challenge/{token}",
            axum::routing::get(|State(c): State<Challenges>, axum::extract::Path(token): axum::extract::Path<String>| async move {
                match c.read().get(&token) {
                    Some(answer) => answer.clone().into_response(),
                    None => StatusCode::NOT_FOUND.into_response(),
                }
            }),
        )
        .fallback(|request: Request| async move {
            let host = request.headers().get(axum::http::header::HOST).and_then(|h| h.to_str().ok()).unwrap_or_default();
            let path = request.uri().path_and_query().map(|p| p.as_str()).unwrap_or("/");
            if host.is_empty() {
                return (StatusCode::BAD_REQUEST, "no Host header").into_response();
            }
            Redirect::permanent(&format!("https://{}{path}", without_port(host))).into_response()
        })
        .with_state(challenges)
}

/// Start the front door: the plain and TLS ports, and the loop keeping the
/// domains and certificates current. Runs until the process ends.
pub async fn start(door: FrontDoor, pool: sqlx::PgPool, inner: Router) -> Result<tokio::task::JoinHandle<Result<()>>> {
    let disk = Disk { dir: door.state_dir.clone() };
    let certs = Arc::new(Certs { address: door.address.to_string(), by_name: RwLock::default() });
    let challenges: Challenges = Arc::default();
    let domains: Arc<RwLock<Vec<Domain>>> = Arc::default();

    let front = Front {
        inner,
        address: door.address,
        domains: domains.clone(),
        projects: Arc::new(weft_dispatcher::PostgresProjectStore::new(pool.clone())),
        http: reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build()?,
    };
    let app = Router::new().fallback(route).with_state(front);

    let plain = tokio::net::TcpListener::bind(door.http).await.with_context(|| format!("bind the plain port {}", door.http))?;
    let tls = tokio::net::TcpListener::bind(door.https).await.with_context(|| format!("bind the https port {}", door.https))?;
    let mut server = rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .context("the TLS protocol versions")?
        .with_no_client_auth()
        .with_cert_resolver(certs.clone());
    server.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
    tracing::info!(target: "weft_runtime::front_door", https = %door.https, http = %door.http, address = %door.address, "front door serving");

    let plain_server = tokio::spawn(crate::server::serve(plain, plain_router(challenges.clone())));
    let tls_server = tokio::spawn(serve_tls(tls, acceptor, app));
    let keeper = tokio::spawn(keep_current(door, disk, pool, certs, challenges, domains));
    Ok(tokio::spawn(async move {
        tokio::select! {
            r = plain_server => r?,
            r = tls_server => r?,
            r = keeper => r?,
        }
    }))
}

/// Accept TLS connections and serve `app` on each, with the caller's
/// address on every request (the public door counts callers by it).
async fn serve_tls(listener: tokio::net::TcpListener, acceptor: tokio_rustls::TlsAcceptor, app: Router) -> Result<()> {
    loop {
        let (stream, peer) = listener.accept().await.context("accept a connection")?;
        let acceptor = acceptor.clone();
        let app = app.clone();
        tokio::spawn(async move {
            let stream = match acceptor.accept(stream).await {
                Ok(stream) => stream,
                Err(e) => {
                    tracing::debug!(target: "weft_runtime::front_door", %peer, error = %e, "a TLS handshake failed");
                    return;
                }
            };
            let service = hyper::service::service_fn(move |mut request: hyper::Request<hyper::body::Incoming>| {
                request.extensions_mut().insert(ConnectInfo::<SocketAddr>(peer));
                app.clone().oneshot(request.map(axum::body::Body::new))
            });
            let served = hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
                .serve_connection_with_upgrades(hyper_util::rt::TokioIo::new(stream), service)
                .await;
            if let Err(e) = served {
                tracing::debug!(target: "weft_runtime::front_door", %peer, error = %e, "a connection ended with an error");
            }
        });
    }
}

/// Keep the domains the router reads and the certificates the door
/// serves current, for the life of the process.
async fn keep_current(
    door: FrontDoor,
    disk: Disk,
    pool: sqlx::PgPool,
    certs: Arc<Certs>,
    challenges: Challenges,
    domains: Arc<RwLock<Vec<Domain>>>,
) -> Result<()> {
    let mut account: Option<Account> = None;
    let mut failed_at: HashMap<String, std::time::Instant> = HashMap::new();
    loop {
        match weft_dispatcher::domains::list(&pool).await {
            Ok(stored) => *domains.write() = stored,
            Err(e) => tracing::warn!(target: "weft_runtime::front_door", error = %format!("{e:#}"), "could not read the install's domains; keeping the last ones"),
        }
        let mut wanted = vec![Wanted::Address(door.address)];
        wanted.extend(domains.read().iter().map(|d| Wanted::Domain(d.name.clone())));
        let now = chrono_now();
        certs.by_name.write().retain(|name, _| wanted.iter().any(|w| &w.name() == name));
        for w in &wanted {
            let name = w.name();
            let held = certs.by_name.read().get(&name).cloned().or_else(|| {
                let loaded = disk.load(&name)?;
                certs.by_name.write().insert(name.clone(), loaded.clone());
                Some(loaded)
            });
            if held.is_some_and(|h| !due_for_renewal(h.not_before, h.not_after, now)) {
                continue;
            }
            if failed_at.get(&name).is_some_and(|at| at.elapsed() < RETRY_AFTER_FAILURE) {
                continue;
            }
            if let Wanted::Domain(domain) = w {
                if !points_here(domain, door.address).await {
                    tracing::info!(target: "weft_runtime::front_door", %domain, address = %door.address, "waiting for the domain's DNS record to point here");
                    continue;
                }
            }
            let result = async {
                if account.is_none() {
                    account = Some(self::account(&door, &disk).await?);
                }
                let (chain, key) = issue(account.as_ref().expect("set above"), w, &challenges).await?;
                let fresh = held_from_pem(&chain, &key)?;
                disk.store(&name, &chain, &key)?;
                certs.by_name.write().insert(name.clone(), fresh);
                anyhow::Ok(())
            }
            .await;
            match result {
                Ok(()) => {
                    failed_at.remove(&name);
                    tracing::info!(target: "weft_runtime::front_door", %name, "certificate issued");
                }
                Err(e) => {
                    failed_at.insert(name.clone(), std::time::Instant::now());
                    tracing::warn!(target: "weft_runtime::front_door", %name, error = %format!("{e:#}"), "could not get a certificate; asking again later");
                }
            }
        }
        tokio::time::sleep(LOOK_EVERY).await;
    }
}

fn chrono_now() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("system clock past UNIX_EPOCH").as_secs() as i64
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
        let address: IpAddr = "34.1.2.3".parse().unwrap();
        let d = domains();
        assert_eq!(destination(Some("34.1.2.3"), address, &d), Destination::Install);
        assert_eq!(destination(Some("34.1.2.3:443"), address, &d), Destination::Install);
        assert_eq!(destination(None, address, &d), Destination::Install);
        assert_eq!(destination(Some("Weft.Example.com:443"), address, &d), Destination::Install);
        assert_eq!(
            destination(Some("app.example.com"), address, &d),
            Destination::Frontend { upstream: "https://front-x.run.app".into() }
        );
        assert_eq!(destination(Some("api.example.com"), address, &d), Destination::Api { project: uuid::Uuid::from_u128(7) });
        assert_eq!(destination(Some("other.example.com"), address, &d), Destination::Unknown { host: "other.example.com".into() });
        assert_eq!(destination(Some("35.9.9.9"), address, &d), Destination::Unknown { host: "35.9.9.9".into() });
        let v6: IpAddr = "2600::1".parse().unwrap();
        assert_eq!(destination(Some("[2600::1]:443"), v6, &d), Destination::Install);
    }

    #[test]
    fn an_api_domain_serves_the_projects_routes_at_its_root_and_the_live_door_as_is() {
        assert_eq!(api_path("local", "/users/42?x=1"), "/connect/local/users/42?x=1");
        assert_eq!(api_path("local", "/"), "/connect/local/");
        assert_eq!(api_path("local", "/live/p/chat?wct=t"), "/live/p/chat?wct=t");
        assert_eq!(api_path("local", "/lively"), "/connect/local/lively");
    }

    #[test]
    fn a_certificate_is_renewed_once_a_third_of_its_life_is_left() {
        // A 90 day certificate renews 30 days before it ends; a 6 day one,
        // 2 days before.
        let day = 86_400;
        assert!(!due_for_renewal(0, 90 * day, 59 * day));
        assert!(due_for_renewal(0, 90 * day, 60 * day));
        assert!(!due_for_renewal(0, 6 * day, 3 * day));
        assert!(due_for_renewal(0, 6 * day, 4 * day));
        assert!(due_for_renewal(0, 6 * day, 7 * day), "an expired one");
    }

    #[test]
    fn a_self_signed_pair_reads_back_with_its_validity() {
        let mut params = rcgen::CertificateParams::new(vec!["weft.example.com".to_string()]).unwrap();
        params.not_before = rcgen::date_time_ymd(2026, 1, 1);
        params.not_after = rcgen::date_time_ymd(2026, 4, 1);
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        let held = held_from_pem(&cert.pem(), &key.serialize_pem()).unwrap();
        assert_eq!(held.not_after - held.not_before, 90 * 86_400);
        assert!(held_from_pem("not a certificate", &key.serialize_pem()).is_err());
    }
}
