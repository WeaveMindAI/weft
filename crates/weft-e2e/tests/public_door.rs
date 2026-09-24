//! The front door's `public` listener (the one a public tunnel forwards to)
//! lets through its allowlist and nothing else, however the path is spelled.
//!
//! The requests go straight to the listener's port on the gateway's own proxy
//! pod, as raw HTTP/1.1, because an HTTP client would tidy `/signal/../projects`
//! into `/projects` before sending it, and the whole point is to see what Envoy
//! does with the untidy one.
//!
//! "Reached the dispatcher" is read off `access-control-allow-origin`: the
//! dispatcher's CORS layer puts it on every answer it writes, and Envoy never
//! adds it to one it wrote itself (a 404 for no matching route, a 400 for a
//! refused path). Envoy Gateway strips `x-envoy-upstream-service-time`, so that
//! header tells nothing here.
// SYNC: the CORS header <-> crates/weft-dispatcher/src/api/mod.rs (core_routes' CorsLayer)
#![cfg(feature = "e2e")]

use std::time::Duration;

use anyhow::{Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use weft_e2e::ensure;

// SYNC: gateway `weft-live-gateway` / namespace `envoy-gateway-system` /
//       listener `public` (8080) <-> deploy/k8s/gateway.yaml
const GATEWAY_NAMESPACE: &str = "envoy-gateway-system";
const PUBLIC_PORT: u16 = 8080;

#[tokio::test]
async fn the_public_door_lets_through_only_its_allowlist() -> Result<()> {
    let _disp = ensure::up().await?;
    let door = PublicDoor::open().await?;

    let reached = door.send("POST", "/events/e2e-public-door/probe").await?;
    assert!(
        reached.from_upstream,
        "POST /events/<service>/<topic> must reach the dispatcher, got {} from Envoy itself",
        reached.status
    );

    for (method, path) in [
        ("GET", "/events/project/x"),
        ("GET", "/projects"),
        ("GET", "/signal/../projects"),
        ("GET", "/signal/%2e%2e/projects"),
        ("GET", "/signal%2F..%2Fprojects"),
        ("GET", "//projects"),
    ] {
        let answer = door.send(method, path).await?;
        assert!(
            !answer.from_upstream,
            "{method} {path} must never reach the dispatcher, but it answered {}",
            answer.status
        );
        assert!(
            matches!(answer.status, 400 | 404),
            "{method} {path} should be refused by Envoy with 400 or 404, got {}",
            answer.status
        );
    }
    Ok(())
}

/// A port-forward onto the gateway proxy pod's `public` listener, owned so it
/// comes down with the test.
struct PublicDoor {
    port: u16,
    _forward: tokio::process::Child,
}

struct Answer {
    status: u16,
    from_upstream: bool,
}

impl PublicDoor {
    async fn open() -> Result<Self> {
        let pod = tokio::process::Command::new("kubectl")
            .args([
                "-n",
                GATEWAY_NAMESPACE,
                "get",
                "pod",
                "-l",
                "app.kubernetes.io/name=envoy,\
                 gateway.envoyproxy.io/owning-gateway-name=weft-live-gateway,\
                 gateway.envoyproxy.io/owning-gateway-namespace=envoy-gateway-system",
                "--field-selector=status.phase=Running",
                "-o",
                "jsonpath={.items[0].metadata.name}",
            ])
            .output()
            .await
            .context("spawn kubectl")?;
        let pod = String::from_utf8_lossy(&pod.stdout).trim().to_string();
        anyhow::ensure!(!pod.is_empty(), "no running proxy pod for weft-live-gateway");

        let mut forward = tokio::process::Command::new("kubectl")
            .args(["-n", GATEWAY_NAMESPACE, "port-forward", &format!("pod/{pod}"), &format!(":{PUBLIC_PORT}")])
            .kill_on_drop(true)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .context("spawn kubectl port-forward onto the gateway proxy")?;
        let stdout = forward.stdout.take().expect("stdout piped");
        let port = tokio::time::timeout(Duration::from_secs(30), async move {
            let mut lines = tokio::io::BufReader::new(stdout).lines();
            while let Some(line) = lines.next_line().await? {
                let port = line
                    .strip_prefix("Forwarding from 127.0.0.1:")
                    .and_then(|rest| rest.split_whitespace().next())
                    .and_then(|p| p.parse::<u16>().ok());
                if let Some(port) = port {
                    tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
                    return Ok(port);
                }
            }
            anyhow::bail!("the port-forward exited before it listened")
        })
        .await
        .context("the port-forward never said which port it bound")??;
        Ok(Self { port, _forward: forward })
    }

    /// One request with `path` sent byte for byte as written.
    async fn send(&self, method: &str, path: &str) -> Result<Answer> {
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", self.port))
            .await
            .context("connect to the forwarded public listener")?;
        let body = "{}";
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: public-door.e2e\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(request.as_bytes()).await?;
        let mut raw = Vec::new();
        tokio::time::timeout(Duration::from_secs(30), stream.read_to_end(&mut raw))
            .await
            .with_context(|| format!("{method} {path}: no answer within 30s"))??;
        let text = String::from_utf8_lossy(&raw);
        let head = text.split("\r\n\r\n").next().unwrap_or_default();
        let status = head
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse::<u16>().ok())
            .with_context(|| format!("{method} {path}: unreadable answer {head:?}"))?;
        let from_upstream = head
            .lines()
            .skip(1)
            .any(|line| line.to_ascii_lowercase().starts_with("access-control-allow-origin:"));
        Ok(Answer { status, from_upstream })
    }
}
