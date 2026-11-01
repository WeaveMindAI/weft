//! Email end to end: the raw-pipe listener dialling a REAL IMAP
//! server (TLS, sign-in dialogue, idle, fire on arrival) and the SMTP
//! action sending a real message, proven as one round trip: the run
//! mails the mailbox it watches and the trigger fires on its own
//! message.
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

use anyhow::Result;
use serde_json::json;

use weft_e2e::access::{catalog_spec, connect_direct, set_account};
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
            "imapHost": imap_host, "imapPort": imap_port,
            "smtpHost": smtp_host, "smtpPort": smtp_port,
        }),
    )
    .await?;
    let marker = format!("weft-e2e-{}", uuid::Uuid::new_v4());

    // The watcher first, so the mailbox is idling before the send.
    let mut receiver = Project::prepare("email_receive", disp.clone()).await?;
    let rid = receiver.id();
    set_account(&receiver, "mb", conn.handle()).await?;
    receiver.set_node_config("recv", "subjectContains", &format!("{marker:?}"))?;
    receiver.activate().await?;
    let before = run::runs(&disp, &rid).await?;

    let mut sender = Project::prepare("email_send", disp.clone()).await?;
    set_account(&sender, "mb", conn.handle()).await?;
    sender.set_node_config("send", "to", &format!("{user:?}"))?;
    sender.set_node_config("send", "subject", &format!("{marker:?}"))?;
    let sent = run::run_and_settle(&mut sender).await?;
    sent.completed()?;

    // Provider delivery to the same mailbox is usually seconds; the
    // generous window absorbs a slow one.
    let run_id =
        run::wait_for_triggered_run(&disp, &rid, &before, Duration::from_secs(180)).await?;
    let settled = SettledRun::observe(&disp, run_id).await?;
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
