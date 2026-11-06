//! One network check of an infra unit, against `127.0.0.1`: what a
//! container unit's agent runs from inside the unit's network
//! (`weft-runtime unit-agent serve`), and what the host runs itself
//! against a process unit's port, which is on the machine.

use std::time::Duration;

use weft_core::infra::ProbeKind;
use weft_platform_traits::unit_agent::ProbeAnswer;

/// Run one network check against `127.0.0.1`.
pub async fn run_probe(probe: &ProbeKind, timeout: Duration) -> ProbeAnswer {
    let failed = |why: String| ProbeAnswer { ok: false, why: Some(why) };
    match probe {
        ProbeKind::Tcp { port } => match tokio::time::timeout(timeout, tokio::net::TcpStream::connect(("127.0.0.1", *port))).await {
            Ok(Ok(_)) => ProbeAnswer { ok: true, why: None },
            Ok(Err(e)) => failed(format!("port {port}: {e}")),
            Err(_) => failed(format!("port {port}: no answer within {}s", timeout.as_secs())),
        },
        ProbeKind::Http { path, port } => {
            let client = weft_core::net::http_client()
                .timeout(timeout)
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("reqwest client");
            match client.get(format!("http://127.0.0.1:{port}{path}")).send().await {
                Ok(r) if r.status().is_success() || r.status().is_redirection() => ProbeAnswer { ok: true, why: None },
                Ok(r) => {
                    let status = r.status().as_u16();
                    // The body rides the same client timeout; one that does
                    // not arrive is a check that said nothing.
                    let body = body_start(r).await;
                    match what_it_said(&body) {
                        Some(said) => failed(format!("GET {path} on {port} answered {status}: {said}")),
                        None => failed(format!("GET {path} on {port} answered {status}")),
                    }
                }
                Err(e) => failed(format!("GET {path} on {port}: {e}")),
            }
        }
        ProbeKind::Exec { .. } => failed("an exec check runs inside its own container, never through the agent".into()),
    }
}

/// The longest stretch of a failing check's body that its reason carries.
const SAID_MAX_CHARS: usize = 300;

/// How much of a failing check's body is read: enough for any JSON
/// `error` a container means to tell, never a whole page a misbehaving one
/// streams back.
const BODY_MAX_BYTES: usize = 4096;

/// The first [`BODY_MAX_BYTES`] of a response's body, read lossily as
/// text; what arrived before an error when the body breaks off.
async fn body_start(mut response: reqwest::Response) -> String {
    let mut body = Vec::new();
    while body.len() < BODY_MAX_BYTES {
        match response.chunk().await {
            Ok(Some(chunk)) => body.extend_from_slice(&chunk[..chunk.len().min(BODY_MAX_BYTES - body.len())]),
            Ok(None) | Err(_) => break,
        }
    }
    String::from_utf8_lossy(&body).into_owned()
}

/// What a failing HTTP check's body says, so a container that knows why
/// it is not ready can tell it: the string `error` field of a JSON body,
/// else the body's text, trimmed and cut to [`SAID_MAX_CHARS`]. `None`
/// when it says nothing.
fn what_it_said(body: &str) -> Option<String> {
    let error = serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|value| value.get("error").and_then(serde_json::Value::as_str).map(str::to_owned));
    let said = error.as_deref().unwrap_or(body).trim();
    if said.is_empty() {
        return None;
    }
    Some(said.chars().take(SAID_MAX_CHARS).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_tcp_check_passes_on_a_listening_port_and_fails_on_a_closed_one() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        assert!(run_probe(&ProbeKind::Tcp { port }, Duration::from_secs(1)).await.ok);
        drop(listener);
        let answer = run_probe(&ProbeKind::Tcp { port }, Duration::from_secs(1)).await;
        assert!(!answer.ok);
        assert!(answer.why.unwrap().contains(&port.to_string()));
    }

    #[tokio::test]
    async fn an_http_check_reads_the_status() {
        let app = axum::Router::new()
            .route("/ok", axum::routing::get(|| async { "fine" }))
            .route("/down", axum::routing::get(|| async { axum::http::StatusCode::SERVICE_UNAVAILABLE }))
            .route(
                "/broken",
                axum::routing::get(|| async { (axum::http::StatusCode::SERVICE_UNAVAILABLE, r#"{"error": "no extension"}"#) }),
            )
            .route("/long", axum::routing::get(|| async { (axum::http::StatusCode::SERVICE_UNAVAILABLE, "x".repeat(1 << 20)) }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        assert!(run_probe(&ProbeKind::Http { path: "/ok".into(), port }, Duration::from_secs(2)).await.ok);
        let down = run_probe(&ProbeKind::Http { path: "/down".into(), port }, Duration::from_secs(2)).await;
        assert_eq!(down.why.as_deref(), Some(format!("GET /down on {port} answered 503").as_str()));
        let broken = run_probe(&ProbeKind::Http { path: "/broken".into(), port }, Duration::from_secs(2)).await;
        assert_eq!(broken.why.as_deref(), Some(format!("GET /broken on {port} answered 503: no extension").as_str()));
        let long = run_probe(&ProbeKind::Http { path: "/long".into(), port }, Duration::from_secs(2)).await;
        assert_eq!(long.why, Some(format!("GET /long on {port} answered 503: {}", "x".repeat(SAID_MAX_CHARS))));
    }

    /// A failing check's body is told by its JSON `error`, else by its
    /// trimmed text, cut on a character boundary, and not at all when
    /// it is empty.
    #[test]
    fn a_failing_check_tells_what_its_body_said() {
        assert_eq!(
            what_it_said(r#"{"error": "the database could not create the extension 'foo'", "ready": false}"#).as_deref(),
            Some("the database could not create the extension 'foo'")
        );
        assert_eq!(what_it_said("  still warming up\n").as_deref(), Some("still warming up"));
        assert_eq!(what_it_said(r#"{"ready": false}"#).as_deref(), Some(r#"{"ready": false}"#));
        assert_eq!(what_it_said(""), None);
        assert_eq!(what_it_said(" \n "), None);
        assert_eq!(what_it_said(r#"{"error": "  "}"#), None);
        let long = "é".repeat(400);
        let cut = what_it_said(&long).unwrap();
        assert_eq!(cut.chars().count(), SAID_MAX_CHARS);
        assert!(long.starts_with(&cut));
    }
}
