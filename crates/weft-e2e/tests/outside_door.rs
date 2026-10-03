//! The outside port (the one the public tunnel carries) answers only the
//! doors outside callers use, however a path is spelled: the management
//! API, which trusts every caller of the loopback public port, is never
//! reachable through it.
//!
//! The requests are raw HTTP/1.1, because an HTTP client would tidy
//! `/signal/../projects` into `/projects` before sending it, and the whole
//! point is to see what the runtime does with the untidy one.
#![cfg(feature = "e2e")]

use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use weft_e2e::ensure;

/// The status line's code of one raw request.
async fn status(port: u16, method: &str, path: &str) -> Result<u16> {
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port)).await?;
    let request = format!("{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 2\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{{}}");
    stream.write_all(request.as_bytes()).await?;
    let mut answer = Vec::new();
    stream.read_to_end(&mut answer).await?;
    let answer = String::from_utf8_lossy(&answer);
    answer
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .with_context(|| format!("{method} {path}: no status line in {answer:?}"))
}

#[tokio::test]
async fn the_outside_port_answers_only_the_outside_doors() -> Result<()> {
    let disp = ensure::up().await?;
    let config_path = ensure::install_dir(&weft_core::infra::Install::default_install()).join("config.json");
    let config: weft_platform_traits::InstallConfig = serde_json::from_str(&std::fs::read_to_string(&config_path)?)?;
    let weft_platform_traits::PlatformConfig::Local(local) = config.platform else { anyhow::bail!("a local install's config") };
    let port = local.listen.outside.context("every local install serves its outside port")?.port();

    // What the management API answers on the loopback public port, and
    // must never answer through the outside one.
    for (method, path) in [
        ("GET", "/projects"),
        ("GET", "/install"),
        ("GET", "/access/grants"),
        ("POST", "/projects"),
        ("GET", "/signal/../projects"),
        ("GET", "/signal/%2e%2e/projects"),
        ("GET", "//projects"),
        ("GET", "/signal-token/../projects"),
        ("GET", "/storage/files"),
        ("GET", "/install/domains"),
    ] {
        let code = status(port, method, path).await?;
        anyhow::ensure!(!(200..300).contains(&code), "{method} {path} answered {code} through the outside port");
    }
    let listed = disp.get_json::<serde_json::Value>("/projects").await;
    anyhow::ensure!(listed.is_ok(), "the same API answers on the public port: {listed:?}");

    // The doors themselves are there: an unknown signal is refused by the
    // signal door, not missing.
    let code = status(port, "GET", "/signal-token/health").await?;
    anyhow::ensure!(code != 404, "the signal-token door answers through the outside port, got {code}");
    Ok(())
}
