//! Email end to end: the raw-pipe listener dialling a REAL IMAP
//! server (TLS, sign-in dialogue, idle, fire on arrival) and the SMTP
//! action sending a real message, proven as one round trip: the run
//! mails the mailbox it watches and the trigger fires on its own
//! message. Plus the capability gates that need no mailbox at all.
//!
//! Env (skipped loudly when unset). Any mailbox serving IMAP + SMTP
//! works; for Gmail use an app password:
//!
//!   WEFT_E2E_EMAIL_USER       the address, also the round trip's recipient
//!   WEFT_E2E_EMAIL_PASSWORD   its password (Gmail/Outlook: an app password)
//!   WEFT_E2E_EMAIL_IMAP_HOST  e.g. imap.gmail.com
//!   WEFT_E2E_EMAIL_IMAP_PORT  e.g. 993
//!   WEFT_E2E_EMAIL_SMTP_HOST  e.g. smtp.gmail.com
//!   WEFT_E2E_EMAIL_SMTP_PORT  e.g. 465
#![cfg(feature = "e2e")]

use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::json;

use weft_e2e::access::{catalog_spec, connect_direct, set_account};
use weft_e2e::platform::Platform;
use weft_e2e::ensure::{self, env_group_or_skip};
use weft_e2e::{project::Project, run, SettledRun};

/// The full round trip on one mailbox: connect with both protocol
/// halves, arm the arrival trigger (subject-filtered on a unique
/// marker), send a real mail to the same address through the SMTP
/// action, and watch the trigger fire with that subject.
#[tokio::test]
async fn email_roundtrip_sends_over_smtp_and_the_imap_trigger_fires() -> Result<()> {
    let Some(env) = env_group_or_skip(
        "email",
        &[
            "WEFT_E2E_EMAIL_USER",
            "WEFT_E2E_EMAIL_PASSWORD",
            "WEFT_E2E_EMAIL_IMAP_HOST",
            "WEFT_E2E_EMAIL_IMAP_PORT",
            "WEFT_E2E_EMAIL_SMTP_HOST",
            "WEFT_E2E_EMAIL_SMTP_PORT",
        ],
    ) else {
        return Ok(());
    };
    let [user, password, imap_host, imap_port, smtp_host, smtp_port] =
        <[String; 6]>::try_from(env).expect("six vars requested");
    let disp = ensure::up().await?;
    let conn = connect_direct(
        &disp,
        catalog_spec("email", "access")?,
        "own",
        json!({
            "user": user, "password": password,
            "imap_host": imap_host, "imap_port": imap_port,
            "smtp_host": smtp_host, "smtp_port": smtp_port,
        }),
    )
    .await?;
    let marker = format!("weft-e2e-{}", uuid::Uuid::new_v4());

    // The watcher first, so the mailbox is idling before the send.
    let mut receiver = Project::prepare("email_receive", disp.clone()).await?;
    let rid = receiver.id();
    set_account(&receiver, "mb", "account", conn.handle())?;
    receiver.set_node_config("recv", "subjectContains", &format!("{marker:?}"))?;
    receiver.activate().await?;
    let before = run::execution_colors(&disp, &rid).await?;

    let mut sender = Project::prepare("email_send", disp.clone()).await?;
    set_account(&sender, "mb", "account", conn.handle())?;
    sender.set_node_config("send", "to", &format!("{user:?}"))?;
    sender.set_node_config("send", "subject", &format!("{marker:?}"))?;
    let sent = run::run_and_settle(&mut sender).await?;
    sent.completed()?;

    // Provider delivery to the same mailbox is usually seconds; the
    // generous window absorbs a slow one.
    let color =
        run::wait_for_triggered_execution(&disp, &rid, &before, Duration::from_secs(180)).await?;
    let settled = SettledRun::observe(&disp, color).await?;
    settled.completed()?;
    let subject = settled
        .input_of("out")
        .and_then(|i| i.get("data").and_then(serde_json::Value::as_str).map(str::to_string))
        .unwrap_or_default();
    anyhow::ensure!(subject.contains(&marker), "the fired subject was: {subject:?}");

    sender.finish().await?;
    receiver.finish().await?;
    conn.finish().await
}

/// The value gate on the trigger, no mailbox needed: a connection
/// holding only the SMTP half arms nothing; the listener refuses the
/// trigger naming the missing IMAP value (a fatal prepare, visible in
/// the listener's log; no dial is ever attempted).
#[tokio::test]
async fn a_send_only_connection_cannot_arm_the_mail_trigger() -> Result<()> {
    let disp = ensure::up().await?;
    let conn = connect_direct(
        &disp,
        catalog_spec("email", "access")?,
        "own",
        json!({
            "user": "e2e@example.invalid", "password": "unused",
            "smtp_host": "smtp.example.invalid", "smtp_port": "465",
        }),
    )
    .await?;

    let mut project = Project::prepare("email_receive", disp.clone()).await?;
    set_account(&project, "mb", "account", conn.handle())?;
    project.activate().await?;

    // Generous window: when this is the suite's FIRST trigger after a
    // bring-up rollout, the pooled listener pod is spawned from
    // nothing before any prepare (and its refusal) can run.
    let platform = Platform::connect().await?;
    platform
        .wait_for_listener_log(
            "the listener to refuse the send-only connection (imap_host shortfall in its log)",
            "needs the connection's 'imap_host'",
            Duration::from_secs(240),
        )
        .await?;

    project.finish().await?;
    conn.finish().await
}

/// The capability gate at connect, no mailbox needed: a mailbox with
/// NEITHER protocol half is refused outright, naming both groups.
#[tokio::test]
async fn a_mailbox_with_neither_protocol_half_is_refused_at_connect() -> Result<()> {
    let disp = ensure::up().await?;
    let err = connect_direct(
        &disp,
        catalog_spec("email", "access")?,
        "own",
        json!({ "user": "e2e@example.invalid", "password": "unused" }),
    )
    .await
    .err()
    .context("a connection filling no capability group must be refused")?;
    let text = format!("{err:#}");
    anyhow::ensure!(
        text.contains("able to do nothing"),
        "the refusal did not explain the empty capability set: {text}"
    );
    Ok(())
}
