//! What the broker tells the dispatcher after a batch of records commits:
//! which runs got rows, by project (`weft_journal::RUN_LOG_CHANNEL`, for
//! the live view) and which runs ended with somebody to tell
//! (`weft_journal::RUN_ENDED_CHANNEL`). Never inside the write: a
//! `NOTIFY` in a transaction takes a lock that makes commits go one at a
//! time server-wide, so the broker gathers what it has to say over a few
//! milliseconds and says it in a statement of its own. These are wake-ups
//! only: the dispatcher also rescans for what they announce, so one lost
//! in a crash only delays the work.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sqlx::PgPool;

/// How long the broker gathers before it speaks.
const GATHER: Duration = Duration::from_millis(10);

/// The broker's pending notifications, and the task that sends them.
pub struct Notices {
    pending: Mutex<Pending>,
    wake: tokio::sync::Notify,
}

#[derive(Default)]
struct Pending {
    run_log: BTreeMap<uuid::Uuid, BTreeSet<weft_core::ExecutionId>>,
    run_ended: BTreeSet<weft_core::ExecutionId>,
}

impl Notices {
    /// The notices, sent on `pool` by a task of their own.
    pub fn start(pool: PgPool) -> Arc<Self> {
        let notices = Arc::new(Self { pending: Mutex::new(Pending::default()), wake: tokio::sync::Notify::new() });
        tokio::spawn(send_loop(notices.clone(), pool));
        notices
    }

    /// After a batch committed: `logged` runs (each with its project) got
    /// rows, `ended` ended with somebody to tell.
    pub fn tell(
        &self,
        logged: impl IntoIterator<Item = (uuid::Uuid, weft_core::ExecutionId)>,
        ended: impl IntoIterator<Item = weft_core::ExecutionId>,
    ) {
        let mut pending = self.pending.lock().expect("broker notices");
        for (project, run) in logged {
            pending.run_log.entry(project).or_default().insert(run);
        }
        pending.run_ended.extend(ended);
        let any = !pending.run_log.is_empty() || !pending.run_ended.is_empty();
        drop(pending);
        if any {
            self.wake.notify_one();
        }
    }
}

async fn send_loop(notices: Arc<Notices>, pool: PgPool) {
    loop {
        notices.wake.notified().await;
        tokio::time::sleep(GATHER).await;
        let Pending { run_log, run_ended } = std::mem::take(&mut *notices.pending.lock().expect("broker notices"));
        let mut channels = Vec::new();
        let mut payloads = Vec::new();
        for (project, runs) in &run_log {
            for payload in weft_journal::run_log_payloads(project, runs) {
                channels.push(weft_journal::RUN_LOG_CHANNEL);
                payloads.push(payload);
            }
        }
        for payload in weft_journal::run_ended_payloads(&run_ended) {
            channels.push(weft_journal::RUN_ENDED_CHANNEL);
            payloads.push(payload);
        }
        if channels.is_empty() {
            continue;
        }
        if let Err(e) = sqlx::query("SELECT pg_notify(c, p) FROM unnest($1::text[], $2::text[]) AS t(c, p)")
            .bind(&channels)
            .bind(&payloads)
            .execute(&pool)
            .await
        {
            // A wake-up lost: the dispatcher's rescan finds what it was about.
            tracing::warn!(target: "weft_broker::notices", error = %e, "could not tell the dispatcher about records just written; its rescan will find them");
        }
    }
}
