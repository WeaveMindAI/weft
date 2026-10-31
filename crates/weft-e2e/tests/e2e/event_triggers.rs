//! Provider event triggers, end to end on a real install: the BOT
//! (push) pattern, proven with a SIGNED synthetic Slack event so no
//! real Slack is involved.
//!
//! Env (skipped loudly when unset; the values must match the
//! install's own `access-apps.json`, which is what verifies the
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
use weft_task_store::db::{self, Client as _, Pool};

/// Sign a synthetic Slack push exactly as Slack signs (the v0 scheme
/// over `v0:{timestamp}:{body}`), with the shipped recipe's own scheme.
fn slack_signature(signing_secret: &str, timestamp: i64, body: &str) -> String {
    let kind: weft_core::access::events::VerifyKind = serde_json::from_value(json!({
        "kind": "hmac",
        "signature": { "in": "header:X-Slack-Signature", "pair": "=", "key": "v0" },
        "timestamp": { "in": "header:X-Slack-Request-Timestamp" },
        "concat": "v0:{timestamp}:{body}"
    }))
    .expect("the slack scheme parses");
    let digest = weft_core::access::verify::hmac_digest(
        &kind,
        signing_secret,
        Some(&timestamp.to_string()),
        &weft_core::access::verify::PushParts { body: body.as_bytes(), headers: &[], url: None, method: "POST" },
    )
    .expect("no unresolvable part in the slack concat");
    format!("v0={digest}")
}

/// Seed the connection an inbound event routes to: workspace T0E2E,
/// made through the operator's app (the client_id pin), and the
/// service recipe the receiver reads (normally recorded when a
/// project's access node renders; the seed writes it directly).
async fn seed_slack_grant(
    pool: &Pool,
    client_id: &str,
) -> Result<(uuid::Uuid, SeededGrant)> {
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
    pool.execute(
        "INSERT INTO access_grant
           (id, tenant_id, service, registration_sealed, client_id, project_id, spec,
            events_recipe_hash, values_sealed, granted_scopes, permissions_verified,
            identity, provider_account, spec_version, exclusive)
         VALUES ($1, 'local', 'slack', $2, $3, NULL, $4, $5, $6, '[]', FALSE, 'events-e2e',
                 'T0E2E', $7, $8)",
        &[
            &grant_id,
            &weft_access_store::seal_json(&json!({ "label": "E2E app", "client_id": client_id }))?,
            &client_id,
            &db::JsonText(&spec),
            &recipe_hash,
            &weft_access_store::seal_json(&json!({ "token": "xoxb-never-used", "teamId": "T0E2E" }))?,
            &weft_core::AccessSpec::STORED.version(),
            &(spec.get("grants") == Some(&json!("exclusive"))),
        ],
    )
    .await?;
    let seeded = SeededGrant::new(pool.clone(), grant_id)?;
    pool.execute(
        "INSERT INTO service_events_recipe (service, recipe_hash, events_json, events_json_version)
         VALUES ('slack', $1, $2, $3)
         ON CONFLICT (service, recipe_hash) DO UPDATE SET updated_at = now()",
        &[&recipe_hash, &db::JsonText(&events), &weft_core::access::events::STORED_EVENTS.version()],
    )
    .await?;
    Ok((grant_id, seeded))
}

/// POST a body to a topic's events receiver, signed exactly as Slack
/// signs, and return the response status.
async fn post_signed(
    http: &reqwest::Client,
    events_url: &str,
    signing_secret: &str,
    body: &str,
) -> Result<reqwest::StatusCode> {
    let timestamp = chrono::Utc::now().timestamp();
    let signature = slack_signature(signing_secret, timestamp, body);
    let resp = http
        .post(events_url)
        .header("X-Slack-Request-Timestamp", timestamp.to_string())
        .header("X-Slack-Signature", &signature)
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .context("POST the signed event")?;
    Ok(resp.status())
}

/// A signed synthetic Slack event lands at the public events
/// receiver: it is verified against the operator's app, routed to the
/// seeded connection's workspace, filtered by the node's channel, and
/// fires a run carrying the message's named fields. A
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
    let pool = weft_e2e::platform::database(&db_url, 2).await.context("connect WEFT_E2E_DATABASE_URL")?;

    let (grant_id, seeded) = seed_slack_grant(&pool, &client_id).await?;

    // The trigger project: fires on messages in channel C0E2E.
    let mut project = Project::prepare("slack_receive", disp.clone()).await?;
    let pid = project.id();
    weft_e2e::access::set_account(&project, "ws", &json!({ "id": grant_id.to_string() })).await?;
    project.activate().await?;
    let before = run::runs(&disp, &pid).await?;

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
    let base = disp.base();
    let http = weft_core::net::http_client().build()?;
    let events_url = format!("{base}/events/slack/messages");
    let status = post_signed(&http, &events_url, &signing_secret, &body).await?;
    anyhow::ensure!(status.is_success(), "the signed push was refused: {status}");

    // The run fires with the message's named fields.
    let run_id =
        run::wait_for_triggered_run(&disp, &pid, &before, Duration::from_secs(60)).await?;
    let settled = SettledRun::observe(&disp, run_id).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!("hello from the events e2e"))?;

    // A tampered copy (one byte changed, signature computed over the
    // ORIGINAL bytes) is refused and fires nothing.
    let timestamp = chrono::Utc::now().timestamp();
    let stale_signature = slack_signature(&signing_secret, timestamp, &body);
    let tampered = body.replace("hello", "HELLO");
    let resp = http
        .post(&events_url)
        .header("X-Slack-Request-Timestamp", timestamp.to_string())
        .header("X-Slack-Signature", &stale_signature)
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
    // pipeline is ordered, so its run arriving proves everything
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
    let status = post_signed(&http, &events_url, &signing_secret, &body2).await?;
    anyhow::ensure!(status.is_success(), "the second signed push was refused: {status}");
    let mut known = before.clone();
    known.insert(run_id);
    let run_id2 =
        run::wait_for_triggered_run(&disp, &pid, &known, Duration::from_secs(60)).await?;
    SettledRun::observe(&disp, run_id2).await?.completed()?;

    // Exactly the two valid pushes fired: the baseline plus two.
    let after = run::runs(&disp, &pid).await?;
    anyhow::ensure!(
        after.len() == before.len() + 2,
        "the tampered push must fire nothing: saw {} runs, expected {} (the two \
         validly signed pushes only)",
        after.len(),
        before.len() + 2
    );

    seeded.finish().await?;
    project.finish().await
}

/// The awaited-push path end to end: a run parks mid-flow on the
/// `interactions` topic (a Slack-button wait), a SIGNED synthetic
/// interactivity payload (form-encoded `payload=`, exactly as Slack
/// posts it) arrives at the receiver, and the run RESUMES carrying
/// the clicked action. This is the machinery under SlackAwaitAction.
#[tokio::test]
async fn a_signed_interactivity_push_resumes_the_parked_run() -> Result<()> {
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
    let pool = weft_e2e::platform::database(&db_url, 2).await.context("connect WEFT_E2E_DATABASE_URL")?;
    let (grant_id, seeded) = seed_slack_grant(&pool, &client_id).await?;

    let mut project = Project::prepare("slack_await", disp.clone()).await?;
    let pid = project.id();
    weft_e2e::access::set_account(&project, "ws", &json!({ "id": grant_id.to_string() })).await?;

    // Start the run and wait until the node has PARKED: its awaited
    // resume signal row appearing in the journal is exactly that
    // moment (posting the push before it would race the park). The
    // signal-token enumeration can NOT detect this park: provider-push
    // signals render no consumer payload (nothing for a human to see
    // or fire; the provider answers them), so they never enumerate.
    // The row itself is the park marker, read through the same pool
    // the grant was seeded with.
    let run_id = run::start(&mut project).await?;
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let parked: Option<String> = pool
            .scalar_opt("SELECT token FROM signal WHERE project_id = $1 AND node_id = 'wait' AND is_resume", &[&pid])
            .await
            .context("poll for the parked resume signal")?;
        if parked.is_some() {
            break;
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "the run never parked on its awaited interactions signal"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // The synthetic block_actions payload, form-encoded and signed
    // over the RAW form bytes, exactly as Slack delivers it.
    let payload = json!({
        "type": "block_actions",
        "team": { "id": "T0E2E" },
        "user": { "id": "U9", "username": "approver" },
        "channel": { "id": "C0E2E" },
        "message": {
            "ts": "1700000000.000400",
            "metadata": { "event_type": "weft_await_action",
                          "event_payload": { "callback": "cb-e2e" } }
        },
        "actions": [ { "action_id": "approve", "value": "approve" } ],
        "response_url": "https://hooks.slack.invalid/never-used"
    });
    let body: String = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("payload", &serde_json::to_string(&payload)?)
        .finish();
    let base = disp.base();
    let http = weft_core::net::http_client().build()?;
    let events_url = format!("{base}/events/slack/interactions");
    let timestamp = chrono::Utc::now().timestamp();
    let signature = slack_signature(&signing_secret, timestamp, &body);
    let resp = http
        .post(&events_url)
        .header("X-Slack-Request-Timestamp", timestamp.to_string())
        .header("X-Slack-Signature", &signature)
        .header("content-type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .context("POST the signed interactivity payload")?;
    anyhow::ensure!(
        resp.status().is_success(),
        "the signed interactivity push was refused: {} {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    // The SAME run resumes (no new run) and completes with the
    // clicked action mapped through the topic's fields.
    let settled = SettledRun::observe(&disp, run_id).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!("approve"))?;

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
        json!({ "token": bot_token, "appToken": app_token }),
    )
    .await?;

    let mut project = Project::prepare("slack_app_messages", disp.clone()).await?;
    let pid = project.id();
    weft_e2e::access::set_account(&project, "ws", conn.handle()).await?;
    // The proof posts AS THE BOT below, and bot messages are ignored
    // by default (a workflow must not trigger itself unasked).
    project.set_node_config("recv", "includeBots", "true")?;
    project.activate().await?;
    let before = run::runs(&disp, &pid).await?;

    // Wait until the listener has actually dialed the socket (its own
    // log line), then post a real message AS THE BOT (its own posts
    // ride the firehose too). Generous window: the
    // whole suite may be running at once.
    Platform::connect(&disp)
        .await?
        .wait_for_runtime_log(
            "the listener to dial the app socket ('socket connected' in its log)",
            "socket connected",
            Duration::from_secs(240),
        )
        .await?;
    let marker = format!("weft-e2e-app-{}", uuid::Uuid::new_v4());
    let resp: Value = weft_core::net::http_client()
        .build()?
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

    let run_id =
        run::wait_for_triggered_run(&disp, &pid, &before, Duration::from_secs(90)).await?;
    let settled = SettledRun::observe(&disp, run_id).await?;
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
/// `appToken`, and the register-time resolve refuses to arm it, so
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
    weft_e2e::access::set_account(&project, "ws", conn.handle()).await?;
    let output = project.activate_refused().await?;
    anyhow::ensure!(
        output.contains("needs the connection's 'appToken'"),
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

    if let Some(blocked) = doors.get("consentBlocked").and_then(Value::as_str) {
        anyhow::ensure!(
            blocked.contains("https"),
            "the consent block must teach the https fix: {blocked}"
        );
        eprintln!("SKIPPED consent-url half: no https address on this weft ({blocked})");
        return Ok(());
    }

    let apps = doors
        .get("sharedApps")
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
            &json!({ "spec": spec, "door": "shared", "sharedApp": label,
                     "permissions": [], "projectId": null,
                     "upgradeGrantId": null, "registration": null }),
        )
        .await?;
    let consent = started
        .get("consentUrl")
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
            &json!({ "spec": spec, "door": "shared", "sharedApp": "no-such-app",
                     "permissions": [], "projectId": null,
                     "upgradeGrantId": null, "registration": null }),
        )
        .await
        .err()
        .context("an unknown app label must be refused")?;
    anyhow::ensure!(format!("{err:#}").contains("labelled"), "unexpected refusal: {err:#}");
    Ok(())
}
