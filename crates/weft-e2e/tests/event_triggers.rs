//! Provider event triggers, end to end on the real cluster: the BOT
//! (push) pattern, proven with a SIGNED synthetic Slack event so no
//! real Slack is involved.
//!
//! Env (skipped loudly when unset; the values must match the
//! cluster's own `access-apps.json`, which is what verifies the
//! push):
//!
//!   WEFT_E2E_SLACK_SIGNING_SECRET  the slack app's events signing
//!                                  secret, as configured in the apps
//!                                  file's `events` block
//!   WEFT_E2E_SLACK_CLIENT_ID       that same app's client_id (the
//!                                  seeded connection is pinned to it)
//!   WEFT_E2E_DATABASE_URL          the store's Postgres, for seeding
//!                                  the connection + the recipe
//!
//! The APP (dial-out socket) pattern lives below in this same file:
//! its live proof (a real Slack app + app-level token), the gate that
//! keeps a bot-pattern connection off the app-wide firehose, and the
//! shared door's fixed-permissions contract. The socket machinery
//! itself is also proven in the listener's contract tests against an
//! in-process gateway.
#![cfg(feature = "e2e")]

use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::{json, Value};

use weft_e2e::access::{catalog_spec, SeededGrant};
use weft_e2e::ensure::{self, env_group_or_skip, env_or_skip};
use weft_e2e::platform::Platform;
use weft_e2e::{project::Project, run, SettledRun};

/// Sign a synthetic Slack push exactly as Slack signs (the v0 scheme
/// over `v0:{timestamp}:{body}`).
fn slack_signature(signing_secret: &str, timestamp: i64, body: &str) -> String {
    let headers = std::collections::BTreeMap::new();
    let concat: weft_core::access::events::SignedConcat =
        "v0:{timestamp}:{body}".parse().expect("a well-formed slack concat");
    weft_core::access::verify::hmac_signature(
        signing_secret,
        &concat,
        "v0=",
        weft_core::access::events::HmacAlgorithm::Sha256,
        weft_core::access::events::DigestEncoding::Hex,
        timestamp,
        &weft_core::access::verify::PushParts {
            body: body.as_bytes(),
            headers: &headers,
            url: None,
            method: "POST",
        },
    )
    .expect("no unresolvable part in the slack concat")
}

/// A signed synthetic Slack event lands at the public events
/// receiver: it is verified against the operator's app, routed to the
/// seeded connection's workspace, filtered by the node's channel, and
/// fires an execution carrying the message's named fields. A
/// TAMPERED copy of the same push is refused with 401 and fires
/// nothing extra.
#[tokio::test]
async fn a_signed_synthetic_slack_event_fires_the_trigger() -> Result<()> {
    let Some(env) = env_group_or_skip(
        "slack events",
        &[
            "WEFT_E2E_SLACK_SIGNING_SECRET",
            "WEFT_E2E_SLACK_CLIENT_ID",
            "WEFT_E2E_DATABASE_URL",
        ],
    ) else {
        return Ok(());
    };
    let [signing_secret, client_id, db_url] =
        <[String; 3]>::try_from(env).expect("three vars requested");
    let disp = ensure::up().await?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url)
        .await
        .context("connect WEFT_E2E_DATABASE_URL")?;

    // Seed the connection an inbound event routes to: workspace
    // T0E2E, made through the operator's app (the client_id pin), and
    // the service recipe the receiver reads (normally recorded when a
    // project's access node renders; the seed writes it directly).
    let spec = catalog_spec("slack", "access")?;
    // The recipe hash that scopes routing: computed exactly as the
    // store computes it (over the typed events map), written on both
    // the grant and the recipe row so the verified push routes here.
    let events: std::collections::BTreeMap<String, weft_core::access::events::EventsSpec> =
        serde_json::from_value(spec.get("events").cloned().unwrap_or(Value::Null))
            .context("parse the slack recipe's events block")?;
    let recipe_hash = weft_access_store::events_recipe_hash(&events)
        .context("the slack recipe declares events")?;
    let grant_id = uuid::Uuid::new_v4();
    sqlx::query(
        "INSERT INTO access_grant
           (id, tenant_id, service, registration_sealed, client_id, project_id, spec_json,
            events_recipe_hash, values_sealed, granted_scopes, permissions_verified,
            identity, provider_account)
         VALUES ($1, 'local', 'slack', $2, $3, NULL, $4, $5, $6, '[]', FALSE, 'events-e2e',
                 'T0E2E')",
    )
    .bind(grant_id)
    .bind(weft_access_store::seal_json(&json!({ "label": "E2E app", "client_id": client_id }))?)
    .bind(&client_id)
    .bind(&spec)
    .bind(&recipe_hash)
    .bind(weft_access_store::seal_json(
        &json!({ "token": "xoxb-never-used", "team_id": "T0E2E" }),
    )?)
    .execute(&pool)
    .await?;
    let seeded = SeededGrant::new(pool.clone(), grant_id);
    sqlx::query(
        "INSERT INTO service_events_recipe (service, recipe_hash, events_json)
         VALUES ('slack', $1, $2)
         ON CONFLICT (service, recipe_hash) DO UPDATE SET updated_at = now()",
    )
    .bind(&recipe_hash)
    .bind(serde_json::to_value(&events)?)
    .execute(&pool)
    .await?;

    // The trigger project: fires on messages in channel C0E2E.
    let mut project = Project::prepare("slack_receive", disp.clone()).await?;
    let pid = project.id();
    project.set_node_config(
        "ws",
        "account",
        &json!({ "id": grant_id.to_string(), "identity": "events-e2e" }).to_string(),
    )?;
    project.activate().await?;
    let before = run::execution_colors(&disp, &pid).await?;

    // The synthetic push, signed exactly as Slack signs (v0 scheme
    // over the raw bytes).
    let body = json!({
        "team_id": "T0E2E",
        "event": {
            "type": "message",
            "channel": "C0E2E",
            "user": "U1",
            "text": "hello from the events e2e",
            "ts": "1700000000.000100"
        }
    })
    .to_string();
    let timestamp = chrono::Utc::now().timestamp();
    let signature = slack_signature(&signing_secret, timestamp, &body);
    let base = std::env::var("WEFT_DISPATCHER_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:9999".to_string());
    let http = reqwest::Client::new();
    let events_url = format!("{}/events/slack/messages", base.trim_end_matches('/'));
    let resp = http
        .post(&events_url)
        .header("X-Slack-Request-Timestamp", timestamp.to_string())
        .header("X-Slack-Signature", &signature)
        .header("content-type", "application/json")
        .body(body.clone())
        .send()
        .await
        .context("POST the signed event")?;
    anyhow::ensure!(
        resp.status().is_success(),
        "the signed push was refused: {} {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    // The execution fires with the message's named fields.
    let color =
        run::wait_for_triggered_execution(&disp, &pid, &before, Duration::from_secs(60)).await?;
    let settled = SettledRun::observe(&disp, color).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!("hello from the events e2e"))?;

    // A tampered copy (one byte changed, same signature) is refused
    // and fires nothing.
    let tampered = body.replace("hello", "HELLO");
    let resp = http
        .post(&events_url)
        .header("X-Slack-Request-Timestamp", timestamp.to_string())
        .header("X-Slack-Signature", &signature)
        .header("content-type", "application/json")
        .body(tampered)
        .send()
        .await?;
    anyhow::ensure!(
        resp.status() == reqwest::StatusCode::UNAUTHORIZED,
        "a tampered push must be refused, got {}",
        resp.status()
    );

    // Prove "fired nothing" by a POSITIVE bound rather than a timer: a
    // second VALIDLY signed push goes in AFTER the tampered one; the
    // pipeline is ordered, so its execution arriving proves everything
    // sent before it (the tampered push included) has drained.
    let body2 = json!({
        "team_id": "T0E2E",
        "event": {
            "type": "message",
            "channel": "C0E2E",
            "user": "U1",
            "text": "drain proof from the events e2e",
            "ts": "1700000000.000200"
        }
    })
    .to_string();
    let timestamp2 = chrono::Utc::now().timestamp();
    let signature2 = slack_signature(&signing_secret, timestamp2, &body2);
    let resp = http
        .post(&events_url)
        .header("X-Slack-Request-Timestamp", timestamp2.to_string())
        .header("X-Slack-Signature", &signature2)
        .header("content-type", "application/json")
        .body(body2)
        .send()
        .await
        .context("POST the second signed event")?;
    anyhow::ensure!(
        resp.status().is_success(),
        "the second signed push was refused: {} {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );
    let mut known = before.clone();
    known.insert(color);
    let color2 =
        run::wait_for_triggered_execution(&disp, &pid, &known, Duration::from_secs(60)).await?;
    SettledRun::observe(&disp, color2).await?.completed()?;

    // Exactly the two valid pushes fired: the baseline plus two.
    let after = run::execution_colors(&disp, &pid).await?;
    anyhow::ensure!(
        after.len() == before.len() + 2,
        "the tampered push must fire nothing: saw {} executions, expected {} (the two \
         validly signed pushes only)",
        after.len(),
        before.len() + 2
    );

    seeded.finish().await?;
    project.finish().await
}

// ---------- The APP (dial-out socket) pattern ----------
//
// Env for the live socket proof (a hand-made Slack app of your own):
//
//   WEFT_E2E_SLACK_BOT_TOKEN   the app's bot token (xoxb-), invited to
//                              the channel below
//   WEFT_E2E_SLACK_APP_TOKEN   the SAME app's app-level token (xapp-,
//                              Basic Information > App-Level Tokens,
//                              scope connections:write), with Socket
//                              Mode enabled and the bot subscribed to
//                              message.channels
//   WEFT_E2E_SLACK_CHANNEL_ID  a channel the bot is in
//
// The negative gate needs only WEFT_E2E_SLACK_BOT_TOKEN.

/// The APP pattern live: a connection through one's own app with the
/// app-level token pasted arms the firehose over Socket Mode (weft
/// dials out and holds the line; no public address involved), and a
/// real message posted in the workspace fires the trigger.
#[tokio::test]
async fn slack_app_socket_receives_a_real_workspace_message() -> Result<()> {
    let Some(env) = env_group_or_skip(
        "slack app socket",
        &[
            "WEFT_E2E_SLACK_BOT_TOKEN",
            "WEFT_E2E_SLACK_APP_TOKEN",
            "WEFT_E2E_SLACK_CHANNEL_ID",
        ],
    ) else {
        return Ok(());
    };
    let [bot_token, app_token, channel] =
        <[String; 3]>::try_from(env).expect("three vars requested");
    let disp = weft_e2e::ensure::up().await?;
    let conn = weft_e2e::access::connect_paste(
        &disp,
        catalog_spec("slack", "access")?,
        json!({ "token": bot_token, "app_token": app_token }),
    )
    .await?;

    let mut project = Project::prepare("slack_app_messages", disp.clone()).await?;
    let pid = project.id();
    weft_e2e::access::set_account(&project, "ws", "account", conn.handle())?;
    // The proof posts AS THE BOT below, and bot messages are ignored
    // by default (a workflow must not trigger itself unasked).
    project.set_node_config("recv", "includeBots", "true")?;
    project.activate().await?;
    let before = run::execution_colors(&disp, &pid).await?;

    // Wait until the listener has actually dialed the socket (its own
    // log line), then post a real message AS THE BOT (its own posts
    // ride the firehose too). Generous window: when this is the
    // suite's first trigger after a bring-up rollout, the pooled
    // listener pod is spawned from nothing first.
    Platform::connect()
        .await?
        .wait_for_listener_log(
            "the listener to dial the app socket ('socket connected' in its log)",
            "socket connected",
            Duration::from_secs(240),
        )
        .await?;
    let marker = format!("weft-e2e-app-{}", uuid::Uuid::new_v4());
    let resp: Value = reqwest::Client::new()
        .post("https://slack.com/api/chat.postMessage")
        .bearer_auth(&bot_token)
        .json(&json!({ "channel": channel, "text": marker }))
        .send()
        .await?
        .json()
        .await?;
    anyhow::ensure!(
        resp.get("ok").and_then(Value::as_bool) == Some(true),
        "chat.postMessage refused: {resp}"
    );

    let color =
        run::wait_for_triggered_execution(&disp, &pid, &before, Duration::from_secs(90)).await?;
    let settled = SettledRun::observe(&disp, color).await?;
    settled.completed()?;
    let text = settled
        .input_of("out")
        .and_then(|i| i.get("data").and_then(Value::as_str).map(str::to_string))
        .unwrap_or_default();
    anyhow::ensure!(text.contains(&marker), "the fired text was: {text:?}");

    project.finish().await?;
    conn.finish().await
}

/// The gate between the two patterns: a BOT-pattern connection (a
/// pasted bot token, no app-level token) cannot open the app-wide
/// firehose. The trigger declares it needs the connection's
/// `app_token`, and the register-time resolve refuses to arm it, so
/// ACTIVATION fails loudly naming the missing value instead of
/// minting a dead trigger. The values live on the store's grant
/// row, so nothing a client sends can fake the token's presence.
#[tokio::test]
async fn a_bot_pattern_connection_cannot_open_the_app_firehose() -> Result<()> {
    let Some(bot_token) = env_or_skip("WEFT_E2E_SLACK_BOT_TOKEN") else {
        return Ok(());
    };
    let disp = weft_e2e::ensure::up().await?;
    let conn = weft_e2e::access::connect_paste(
        &disp,
        catalog_spec("slack", "access")?,
        json!({ "token": bot_token }),
    )
    .await?;

    let mut project = Project::prepare("slack_app_messages", disp.clone()).await?;
    weft_e2e::access::set_account(&project, "ws", "account", conn.handle())?;
    let output = project.activate_refused().await?;
    anyhow::ensure!(
        output.contains("needs the connection's 'app_token'"),
        "the refusal must name the missing app_token; activate said:\n{output}"
    );

    project.finish().await?;
    conn.finish().await
}

// ---------- The shared door's fixed permissions ----------

/// The doors probe lists each registered app as its own option with
/// its FIXED `covers`, and a shared consent pins exactly the chosen
/// app: its client_id and its covers ride the consent URL, and an
/// unknown label is refused. On a weft with no https address the
/// probe instead reports every consent blocked (Slack only accepts
/// https callbacks), which is the other half of the same contract.
#[tokio::test]
async fn the_shared_door_offers_each_registered_app_with_fixed_permissions() -> Result<()> {
    let Some(client_id) = env_or_skip("WEFT_E2E_SLACK_CLIENT_ID") else {
        return Ok(());
    };
    let disp = weft_e2e::ensure::up().await?;
    let spec = catalog_spec("slack", "access")?;
    let doors: Value = disp.post_json("/access/doors", &json!({ "spec": spec })).await?;

    if let Some(blocked) = doors.get("consent_blocked").and_then(Value::as_str) {
        anyhow::ensure!(
            blocked.contains("https"),
            "the consent block must teach the https fix: {blocked}"
        );
        eprintln!("SKIPPED consent-url half: no https address on this weft ({blocked})");
        return Ok(());
    }

    let apps = doors
        .get("shared_apps")
        .and_then(Value::as_array)
        .cloned()
        .context("doors answered no shared_apps")?;
    anyhow::ensure!(!apps.is_empty(), "the apps file offers no slack app to this probe");
    for app in &apps {
        anyhow::ensure!(
            app.get("covers").and_then(Value::as_array).is_some_and(|c| !c.is_empty()),
            "a registered app must show its fixed permissions: {app}"
        );
    }
    let first = &apps[0];
    let label = first.get("label").and_then(Value::as_str).context("app without label")?;
    let covers: Vec<String> = first
        .get("covers")
        .and_then(Value::as_array)
        .unwrap()
        .iter()
        .filter_map(|c| c.as_str().map(str::to_string))
        .collect();

    let started: Value = disp
        .post_json(
            "/access/connect/begin",
            &json!({ "spec": spec, "door": "shared", "shared_app": label,
                     "permissions": [], "project_id": null,
                     "upgrade_grant_id": null, "registration": null }),
        )
        .await?;
    let consent = started
        .get("consent_url")
        .and_then(Value::as_str)
        .context("begin answered no consent_url")?;
    anyhow::ensure!(consent.contains(&client_id), "the consent must pin the app: {consent}");
    // Read the scope parameter off the parsed URL (query_pairs decodes
    // the percent-encoding) and assert set membership on the split
    // scope list, so a permission that merely appears as a substring
    // of another never fakes a pass.
    let parsed = url::Url::parse(consent).context("parse the consent url")?;
    let scope = parsed
        .query_pairs()
        .find(|(k, _)| k == "scope")
        .map(|(_, v)| v.into_owned())
        .context("the consent url carries no scope parameter")?;
    let scopes: std::collections::HashSet<&str> =
        scope.split([' ', ',']).filter(|s| !s.is_empty()).collect();
    for cover in &covers {
        anyhow::ensure!(
            scopes.contains(cover.as_str()),
            "the consent must carry the app's fixed permission '{cover}'; its scopes: {scope}"
        );
    }

    let err = disp
        .post_json::<Value>(
            "/access/connect/begin",
            &json!({ "spec": spec, "door": "shared", "shared_app": "no-such-app",
                     "permissions": [], "project_id": null,
                     "upgrade_grant_id": null, "registration": null }),
        )
        .await
        .err()
        .context("an unknown app label must be refused")?;
    anyhow::ensure!(format!("{err:#}").contains("labelled"), "unexpected refusal: {err:#}");
    Ok(())
}
