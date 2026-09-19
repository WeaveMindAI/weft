//! A route gated by a real identity issuer through `JwtAuth`: the gateway
//! verifies a bearer token against the issuer's published keys before a
//! run starts, and the token's claims reach the trigger's `caller` port.
//! Needs an OAuth issuer you control (see the README's variable table):
//! the issuer URL, an audience, and a machine-to-machine client id and
//! secret. The test mints its own token from those on every run (the
//! client-credentials grant), so nothing in the environment expires.
#![cfg(feature = "e2e")]

use anyhow::Context;
use reqwest::Method;
use serde_json::{json, Value};
use weft_e2e::access::{catalog_spec, connect_direct, set_account};
use weft_e2e::ensure::{self, env_group_or_skip};
use weft_e2e::{live, project::Project};

#[tokio::test]
async fn a_genuine_token_is_admitted_and_its_claims_reach_the_run() -> anyhow::Result<()> {
    let Some(vars) = env_group_or_skip(
        "jwt issuer",
        &[
            "WEFT_E2E_JWT_ISSUER",
            "WEFT_E2E_JWT_AUDIENCE",
            "WEFT_E2E_JWT_CLIENT_ID",
            "WEFT_E2E_JWT_CLIENT_SECRET",
        ],
    ) else {
        return Ok(());
    };
    let [issuer, audience, client_id, client_secret] =
        <[String; 4]>::try_from(vars).expect("four variables");
    // The issuer's standard addresses: its keys, and its token endpoint.
    let issuer_base = issuer.trim_end_matches('/');
    let jwks_url = format!("{issuer_base}/.well-known/jwks.json");
    let minted: Value = reqwest::Client::new()
        .post(format!("{issuer_base}/oauth/token"))
        .json(&json!({
            "client_id": client_id,
            "client_secret": client_secret,
            "audience": audience,
            "grant_type": "client_credentials",
        }))
        .send()
        .await
        .context("mint a token from the issuer")?
        .error_for_status()
        .context("the issuer refused to mint a token; check the client id, secret and audience")?
        .json()
        .await?;
    let token = minted["access_token"]
        .as_str()
        .context("the issuer's answer carries no access_token")?
        .to_string();

    let disp = ensure::up().await?;
    let mut project = Project::prepare("api_jwt", disp.clone()).await?;
    let base = project.unique_live_path()?;

    let values = json!({ "issuer": issuer, "jwks_url": jwks_url, "audience": audience });
    let conn = connect_direct(&disp, catalog_spec("api", "jwt_auth")?, "own", values).await?;
    set_account(&project, "issuer", "account", conn.handle())?;
    project.activate().await?;

    let me = format!("{base}/me");
    let (status, _, _) = live::http_request(&disp, Method::GET, &me, &[], None).await?;
    assert_eq!(status, 401, "no token is refused");
    let forged = "eyJhbGciOiJSUzI1NiIsImtpZCI6Im5vcGUifQ.eyJzdWIiOiJ4In0.bm9wZQ";
    let (status, _, _) = live::http_request(
        &disp,
        Method::GET,
        &me,
        &[("authorization", &format!("Bearer {forged}"))],
        None,
    )
    .await?;
    assert_eq!(status, 401, "a token the issuer never signed is refused");
    let (status, _, body) = live::http_request(
        &disp,
        Method::GET,
        &me,
        &[("authorization", &format!("Bearer {token}"))],
        None,
    )
    .await?;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let v: Value = serde_json::from_slice(&body)?;
    assert_eq!(v["iss"], issuer, "the token's claims are the caller: {v}");
    assert!(v["sub"].is_string(), "a subject claim rides along: {v}");

    project.finish().await?;
    conn.finish().await
}
