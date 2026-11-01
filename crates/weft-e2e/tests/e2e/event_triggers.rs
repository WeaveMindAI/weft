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
//! Two tests: a signed message push fires a run, and a signed
//! interactivity push resumes a run parked on it.
#![cfg(feature = "e2e")]

use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::{json, Value};

use weft_e2e::access::{catalog_spec, SeededGrant};
use weft_e2e::ensure::{self, env_group_or_skip};
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
    let pool = weft_e2e::platform::database(&db_url).await.context("connect WEFT_E2E_DATABASE_URL")?;

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
    let pool = weft_e2e::platform::database(&db_url).await.context("connect WEFT_E2E_DATABASE_URL")?;
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
    let status = resp.status();
    let refusal = resp.text().await.context("read the interactivity push's answer")?;
    anyhow::ensure!(status.is_success(), "the signed interactivity push was refused: {status} {refusal}");

    // The SAME run resumes (no new run) and completes with the
    // clicked action mapped through the topic's fields.
    let settled = SettledRun::observe(&disp, run_id).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!("approve"))?;

    seeded.finish().await?;
    project.finish().await
}
