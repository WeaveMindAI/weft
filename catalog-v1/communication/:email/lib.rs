//! Shared IMAP connection helpers for email nodes.
//! Supports three security modes: tls (default), starttls, and none.

use std::collections::HashSet;
use futures::StreamExt;

type TlsImapSession = async_imap::Session<tokio_native_tls::TlsStream<tokio::net::TcpStream>>;
type PlainImapSession = async_imap::Session<tokio::net::TcpStream>;

pub enum ImapSession {
    Tls(TlsImapSession),
    Plain(PlainImapSession),
}

impl ImapSession {
    pub async fn select(&mut self, mailbox: &str) -> Result<async_imap::types::Mailbox, async_imap::error::Error> {
        match self {
            ImapSession::Tls(s) => s.select(mailbox).await,
            ImapSession::Plain(s) => s.select(mailbox).await,
        }
    }

    pub async fn uid_search(&mut self, query: &str) -> Result<HashSet<u32>, async_imap::error::Error> {
        match self {
            ImapSession::Tls(s) => s.uid_search(query).await,
            ImapSession::Plain(s) => s.uid_search(query).await,
        }
    }

    pub async fn uid_fetch_bodies(&mut self, uid_set: &str) -> Result<Vec<Vec<u8>>, async_imap::error::Error> {
        match self {
            ImapSession::Tls(s) => collect_bodies(s, uid_set).await,
            ImapSession::Plain(s) => collect_bodies(s, uid_set).await,
        }
    }

    pub async fn list(&mut self, reference: Option<&str>, pattern: Option<&str>) -> Result<Vec<String>, async_imap::error::Error> {
        match self {
            ImapSession::Tls(s) => collect_list(s, reference, pattern).await,
            ImapSession::Plain(s) => collect_list(s, reference, pattern).await,
        }
    }

    pub async fn gmail_fetch_thrid(&mut self, uid: u32) -> Option<u64> {
        match self {
            ImapSession::Tls(s) => fetch_gmail_thrid(s, uid).await,
            ImapSession::Plain(s) => fetch_gmail_thrid(s, uid).await,
        }
    }

    pub async fn logout(&mut self) -> Result<(), async_imap::error::Error> {
        match self {
            ImapSession::Tls(s) => s.logout().await,
            ImapSession::Plain(s) => s.logout().await,
        }
    }
}

async fn collect_bodies<T>(
    session: &mut async_imap::Session<T>,
    uid_set: &str,
) -> Result<Vec<Vec<u8>>, async_imap::error::Error>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + std::fmt::Debug,
{
    let mut bodies = Vec::new();
    let mut fetches = session.uid_fetch(uid_set, "(RFC822)").await?;
    while let Some(Ok(fetch)) = fetches.next().await {
        if let Some(body) = fetch.body() {
            bodies.push(body.to_vec());
        }
    }
    Ok(bodies)
}

async fn collect_list<T>(
    session: &mut async_imap::Session<T>,
    reference: Option<&str>,
    pattern: Option<&str>,
) -> Result<Vec<String>, async_imap::error::Error>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + std::fmt::Debug,
{
    let mut names = Vec::new();
    let mut list_stream = session.list(reference, pattern).await?;
    while let Some(Ok(name)) = list_stream.next().await {
        names.push(name.name().to_string());
    }
    Ok(names)
}

async fn fetch_gmail_thrid<T>(
    session: &mut async_imap::Session<T>,
    uid: u32,
) -> Option<u64>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + std::fmt::Debug,
{
    let cmd = format!("UID FETCH {} (X-GM-THRID)", uid);
    if session.run_command(&cmd).await.is_err() {
        return None;
    }
    let mut thrid = None;
    use async_imap::imap_proto::types::{AttributeValue, Response};
    while let Ok(Some(resp)) = session.read_response().await {
        if let Response::Fetch(_, attrs) = resp.parsed() {
            for attr in attrs {
                if let AttributeValue::GmailThrId(id) = attr {
                    thrid = Some(*id);
                    break;
                }
            }
        }
        if let Response::Done { .. } = resp.parsed() {
            break;
        }
    }
    thrid
}

fn build_tls_connector(accept_invalid: bool) -> Result<tokio_native_tls::TlsConnector, String> {
    let mut builder = native_tls::TlsConnector::builder();
    if accept_invalid {
        builder.danger_accept_invalid_certs(true);
    }
    let native_connector = builder.build()
        .map_err(|e| format!("TLS connector creation failed: {}", e))?;
    Ok(tokio_native_tls::TlsConnector::from(native_connector))
}

pub async fn connect_imap(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
    security: &str,
    tls_accept_invalid: bool,
) -> Result<ImapSession, String> {
    let tcp = tokio::net::TcpStream::connect((host, port))
        .await
        .map_err(|e| format!("TCP connect to {}:{} failed: {}", host, port, e))?;

    match security {
        "none" => {
            let mut client = async_imap::Client::new(tcp);
            let _greeting = client.read_response().await
                .map_err(|e| format!("IMAP greeting failed: {}", e))?;
            let session = client.login(username, password)
                .await
                .map_err(|(e, _)| format!("IMAP login failed: {}", e))?;
            Ok(ImapSession::Plain(session))
        }
        "starttls" => {
            let mut client = async_imap::Client::new(tcp);
            let _greeting = client.read_response().await
                .map_err(|e| format!("IMAP greeting failed: {}", e))?;
            client.run_command_and_check_ok("STARTTLS", None).await
                .map_err(|e| format!("IMAP STARTTLS command failed: {}", e))?;
            let raw_stream = client.into_inner();
            let tls_connector = build_tls_connector(tls_accept_invalid)?;
            let tls_stream = tls_connector.connect(host, raw_stream)
                .await
                .map_err(|e| format!("TLS handshake after STARTTLS failed: {}", e))?;
            let client = async_imap::Client::new(tls_stream);
            let session = client.login(username, password)
                .await
                .map_err(|(e, _)| format!("IMAP login failed: {}", e))?;
            Ok(ImapSession::Tls(session))
        }
        _ => {
            // Direct TLS (default, typically port 993)
            let tls_connector = build_tls_connector(tls_accept_invalid)?;
            let tls_stream = tls_connector.connect(host, tcp)
                .await
                .map_err(|e| format!("TLS handshake failed: {}", e))?;
            let mut client = async_imap::Client::new(tls_stream);
            let _greeting = client.read_response().await
                .map_err(|e| format!("IMAP greeting failed: {}", e))?;
            let session = client.login(username, password)
                .await
                .map_err(|(e, _)| format!("IMAP login failed: {}", e))?;
            Ok(ImapSession::Tls(session))
        }
    }
}
