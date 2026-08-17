//! The accesses feature, driven against REAL providers: one e2e per
//! proof service in the catalog, each exercising its auth shape end to
//! end (connect through the dispatcher's access store, run a real
//! project, make a real authenticated call) PLUS the runtime backstops
//! at marker resolution (permissions live on the stored connection, so
//! there is deliberately no compile-time check to drive).
//!
//! Credentials arrive via env vars, one documented set per service; a
//! MISSING var SKIPS that service's e2e with a loud message naming it
//! (absent creds never fake a pass and never fail the suite):
//!
//!   exa         WEFT_NODE_TEST_EXA_KEY (shared with the node-test tier)
//!   telegram    WEFT_E2E_TELEGRAM_TOKEN, WEFT_E2E_TELEGRAM_CHAT_ID
//!   slack       WEFT_E2E_SLACK_BOT_TOKEN (xoxb-, a hand-made bot),
//!               WEFT_E2E_SLACK_CHANNEL_ID (the bot must be in it)
//!   gdrive      WEFT_E2E_GOOGLE_CLIENT_ID, WEFT_E2E_GOOGLE_CLIENT_SECRET,
//!               WEFT_E2E_GOOGLE_REFRESH_TOKEN (a pre-obtained one)
//!   s3          WEFT_E2E_S3_ENDPOINT, WEFT_E2E_S3_REGION,
//!               WEFT_E2E_S3_ACCESS_KEY_ID, WEFT_E2E_S3_SECRET_ACCESS_KEY,
//!               WEFT_E2E_S3_BUCKET
//!   db seeding  WEFT_E2E_DATABASE_URL (the store's Postgres, e.g. a
//!               port-forward of the dev cluster's weft-postgres): the
//!               drift-backstop and refresh e2es seed grant rows
//!               directly (no HTTP door writes arbitrary grants).
//!
//! The browser OAuth consent itself is not driven headlessly here: the
//! store's layer-3 tests cover the begin/complete flow against a fake
//! provider, and the OAuth e2es exercise the TOKEN side (a seeded
//! refresh token driving a real refresh).
#![cfg(feature = "e2e")]

use anyhow::{Context, Result};
use serde_json::{json, Value};

use weft_e2e::access::{catalog_spec, connect_direct, connect_paste, set_account, SeededGrant};
use weft_e2e::ensure::{self, env_group_or_skip, env_or_skip};
use weft_e2e::{project::Project, run};

// ---------- runtime stale-connection backstop (no credentials needed) ----------

/// A marker referencing a connection the store does not hold (a
/// project imported from elsewhere, a deleted connection) fails LOUD
/// at resolution, naming the fix. Needs a cluster, no creds.
#[tokio::test]
async fn a_stale_connection_fails_loud_at_resolution() -> Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("access_stale", disp).await?;
    set_account(
        &project,
        "ws",
        "account",
        &json!({ "id": uuid::Uuid::new_v4().to_string(), "identity": "someone-elses" }),
    )?;
    project.set_node_config("send", "channel", "\"C000\"")?;
    let settled = run::run_and_settle(&mut project).await?;
    settled.failed_with("pick one on the access node")?;
    project.finish().await
}

// ---------- live proofs, one per auth shape ----------

/// Static key in a header (Exa): connect validates through the
/// declared test call, the run performs a real search.
#[tokio::test]
async fn exa_key_runs_a_real_search() -> Result<()> {
    let Some(env) = env_group_or_skip("exa key", &["WEFT_NODE_TEST_EXA_KEY"]) else {
        return Ok(());
    };
    let [key] = <[String; 1]>::try_from(env).expect("one var requested");
    let disp = ensure::up().await?;
    let conn =
        connect_direct(&disp, catalog_spec("web", "exa_access")?, "own", json!({ "key": key }))
            .await?;

    let mut project = Project::prepare("access_exa", disp).await?;
    set_account(&project, "exa", "account", conn.handle())?;
    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    let count = settled
        .input_of("out")
        .and_then(|i| i.get("data").and_then(Value::as_f64))
        .unwrap_or_default();
    anyhow::ensure!(count >= 1.0, "the search answered no results");
    project.finish().await?;
    conn.finish().await
}

/// Static key in the URL PATH (Telegram): proves the PathPrefix step.
#[tokio::test]
async fn telegram_bot_sends_a_real_message() -> Result<()> {
    let Some(env) =
        env_group_or_skip("telegram", &["WEFT_E2E_TELEGRAM_TOKEN", "WEFT_E2E_TELEGRAM_CHAT_ID"])
    else {
        return Ok(());
    };
    let [token, chat] = <[String; 2]>::try_from(env).expect("two vars requested");
    let disp = ensure::up().await?;
    let conn =
        connect_direct(&disp, catalog_spec("telegram", "access")?, "own", json!({ "token": token }))
            .await?;

    let mut project = Project::prepare("access_telegram", disp).await?;
    set_account(&project, "bot", "account", conn.handle())?;
    project.set_node_config("send", "chatId", &format!("{chat:?}"))?;
    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    anyhow::ensure!(
        settled.input_of("out").and_then(|i| i.get("data").and_then(Value::as_i64)).is_some(),
        "no message id came back"
    );
    project.finish().await?;
    conn.finish().await
}

/// Hand-made Slack bot token through the `own_page.paste` section of
/// the ONE Slack service (the pasted token's permissions are CLAIMED,
/// never verified, so the consumer's chat:write requirement is let
/// through and this run proves the call itself).
#[tokio::test]
async fn slack_bot_token_posts_a_real_message() -> Result<()> {
    let Some(env) =
        env_group_or_skip("slack bot", &["WEFT_E2E_SLACK_BOT_TOKEN", "WEFT_E2E_SLACK_CHANNEL_ID"])
    else {
        return Ok(());
    };
    let [token, channel] = <[String; 2]>::try_from(env).expect("two vars requested");
    let disp = ensure::up().await?;
    let conn =
        connect_paste(&disp, catalog_spec("slack", "access")?, json!({ "token": token })).await?;

    let mut project = Project::prepare("access_slack_bot", disp).await?;
    set_account(&project, "ws", "account", conn.handle())?;
    project.set_node_config("send", "channel", &format!("{channel:?}"))?;
    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    anyhow::ensure!(
        settled.input_of("out").and_then(|i| i.get("data").and_then(Value::as_str).map(str::to_string)).is_some(),
        "no message ts came back"
    );
    project.finish().await?;
    conn.finish().await
}

/// The RUNTIME drift backstop: a connection whose VERIFIED granted set
/// misses a permission the consuming node requires refuses at marker
/// resolution, before any provider call. Needs the store's Postgres,
/// no Slack credentials. (A CLAIMED shortfall passes by design: nobody
/// actually knows what an unverified credential holds.)
#[tokio::test]
async fn scope_drift_is_refused_at_resolution() -> Result<()> {
    let Some(db_url) = env_or_skip("WEFT_E2E_DATABASE_URL") else { return Ok(()) };
    let disp = ensure::up().await?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url)
        .await
        .context("connect WEFT_E2E_DATABASE_URL")?;

    // Seed a slack connection VERIFIED to hold only channels:read; the
    // fixture's SlackSendMessage requires chat:write.
    let grant_id = uuid::Uuid::new_v4();
    sqlx::query(
        "INSERT INTO access_grant
           (id, tenant_id, service, registration_sealed, project_id, spec_json, values_sealed,
            granted_scopes, permissions_verified, identity, expires_at)
         VALUES ($1, 'local', 'slack', NULL, NULL, $2, $3, $4, TRUE, 'drift-test', NULL)",
    )
    .bind(grant_id)
    .bind(catalog_spec("slack", "access")?)
    .bind(weft_access_store::seal_json(&json!({ "token": "xoxb-never-used" }))?)
    .bind(json!(["channels:read"]))
    .execute(&pool)
    .await?;
    let seeded = SeededGrant::new(pool.clone(), grant_id);

    let mut project = Project::prepare("access_slack_oauth", disp).await?;
    set_account(
        &project,
        "ws",
        "account",
        &json!({ "id": grant_id.to_string(), "identity": "drift-test" }),
    )?;
    project.set_node_config("send", "channel", "\"C000\"")?;
    let settled = run::run_and_settle(&mut project).await?;
    settled.failed_with("chat:write")?;
    settled.failed_with("reconnect")?;

    seeded.finish().await?;
    project.finish().await
}

/// OAuth coexisting class (Google Drive): a seeded, ALREADY-EXPIRED
/// grant forces a real lazy refresh against Google's token endpoint on
/// the first resolution, then lists real files.
#[tokio::test]
async fn google_drive_refreshes_lazily_and_lists_real_files() -> Result<()> {
    let Some(env) = env_group_or_skip(
        "google drive",
        &[
            "WEFT_E2E_GOOGLE_CLIENT_ID",
            "WEFT_E2E_GOOGLE_CLIENT_SECRET",
            "WEFT_E2E_GOOGLE_REFRESH_TOKEN",
            "WEFT_E2E_DATABASE_URL",
        ],
    ) else {
        return Ok(());
    };
    let [client_id, client_secret, refresh_token, db_url] =
        <[String; 4]>::try_from(env).expect("four vars requested");
    let disp = ensure::up().await?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url)
        .await
        .context("connect WEFT_E2E_DATABASE_URL")?;

    // The app the refresh authenticates with, snapshotted on the grant
    // exactly as a real connect would (label + client id + secret).
    let registration =
        json!({ "label": "gdrive-e2e app", "client_id": client_id, "client_secret": client_secret });

    // A grant whose token EXPIRED in the past: resolution must refresh.
    let grant_id = uuid::Uuid::new_v4();
    sqlx::query(
        "INSERT INTO access_grant
           (id, tenant_id, service, registration_sealed, client_id, project_id, spec_json,
            values_sealed, granted_scopes, identity, expires_at)
         VALUES ($1, 'local', 'google', $2, $3, NULL, $4, $5, $6, 'gdrive-e2e',
                 now() - interval '1 hour')",
    )
    .bind(grant_id)
    .bind(weft_access_store::seal_json(&registration)?)
    .bind(&client_id)
    .bind(catalog_spec("google", "access")?)
    .bind(weft_access_store::seal_json(
        &json!({ "token": "expired-token", "refresh_token": refresh_token }),
    )?)
    .bind(json!(["https://www.googleapis.com/auth/drive.file"]))
    .execute(&pool)
    .await?;
    let seeded = SeededGrant::new(pool.clone(), grant_id);

    let mut project = Project::prepare("access_gdrive", disp).await?;
    set_account(
        &project,
        "acct",
        "account",
        &json!({ "id": grant_id.to_string(), "identity": "gdrive-e2e" }),
    )?;
    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    anyhow::ensure!(
        settled.input_of("out").and_then(|i| i.get("data").cloned()).map_or(false, |v| v.is_array()),
        "no file list came back"
    );

    // The refresh really happened: the stored token rotated away from
    // the seeded sentinel, and the expiry moved into the future.
    let (values_sealed, expires_at): (String, Option<chrono::DateTime<chrono::Utc>>) =
        sqlx::query_as("SELECT values_sealed, expires_at FROM access_grant WHERE id = $1")
            .bind(grant_id)
            .fetch_one(&pool)
            .await?;
    let values = weft_access_store::open_json(&values_sealed)?;
    anyhow::ensure!(
        values.get("token").and_then(Value::as_str) != Some("expired-token"),
        "the stored token did not rotate"
    );
    anyhow::ensure!(
        expires_at.is_some_and(|t| t > chrono::Utc::now()),
        "the expiry did not move forward"
    );

    seeded.finish().await?;
    project.finish().await
}

/// Per-request signing (SigV4 against a real S3-compatible endpoint):
/// proves the base_url + sign steps; costs cents at most.
#[tokio::test]
async fn s3_sigv4_uploads_a_real_object() -> Result<()> {
    let Some(env) = env_group_or_skip(
        "s3",
        &[
            "WEFT_E2E_S3_ENDPOINT",
            "WEFT_E2E_S3_REGION",
            "WEFT_E2E_S3_ACCESS_KEY_ID",
            "WEFT_E2E_S3_SECRET_ACCESS_KEY",
            "WEFT_E2E_S3_BUCKET",
        ],
    ) else {
        return Ok(());
    };
    let [endpoint, region, key_id, secret, bucket] =
        <[String; 5]>::try_from(env).expect("five vars requested");
    let disp = ensure::up().await?;
    let conn = connect_direct(
        &disp,
        catalog_spec("s3", "access")?,
        "own",
        json!({
            "endpoint": endpoint, "region": region,
            "access_key_id": key_id, "secret_access_key": secret,
        }),
    )
    .await?;

    let mut project = Project::prepare("access_s3", disp).await?;
    set_account(&project, "store", "account", conn.handle())?;
    project.set_node_config("put", "bucket", &format!("{bucket:?}"))?;
    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    anyhow::ensure!(
        settled.input_of("out").and_then(|i| i.get("data").and_then(Value::as_str).map(str::to_string)).is_some(),
        "no etag came back"
    );
    project.finish().await?;
    conn.finish().await
}

// The MintJwt auth shape (a signed app JWT exchanged for a short-lived
// installation token) currently has no catalog vehicle: its only user
// was the GitHub package, removed to be rebuilt properly (see
// ROADMAP.md, "GitHub package, done properly"). Restore its live proof
// here the day a MintJwt service returns.
