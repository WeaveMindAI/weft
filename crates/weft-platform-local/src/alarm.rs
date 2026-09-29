//! Wakes on a local install: kept in Postgres (`weft_task_store::alarm`),
//! delivered by a loop in the runtime process.
//!
//! The loop sleeps until the next wake is due (or until a new one is set),
//! claims what is due, and posts each to its role the way a cloud's queue
//! would: `WakeCall` as the body, the install's own identity as the
//! bearer. A receiver that answers with an error gets the wake again
//! later, backing off; a wake is never dropped for failing.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sqlx::PgPool;
use weft_platform_traits::{Alarm, CoreRole, IdentityTokens, RoleAddresses, Wake, WakeCall};

/// How long a claimed wake is this loop's before it comes due again (a
/// delivery that never finished, the process gone mid-call).
const LEASE_MS: i64 = 5 * 60 * 1000;

/// How many due wakes one pass claims.
const BATCH: i64 = 64;

/// The longest the loop sleeps without looking again, so a wake another
/// process stored is not missed for long.
const LOOK_AGAIN: Duration = Duration::from_secs(60);

/// The most a failing wake waits before its next try.
const MAX_BACKOFF_MS: i64 = 5 * 60 * 1000;

pub struct LocalAlarm {
    pool: PgPool,
    changed: Arc<tokio::sync::Notify>,
}

impl LocalAlarm {
    pub fn new(pool: PgPool) -> Self {
        Self { pool, changed: Arc::new(tokio::sync::Notify::new()) }
    }

    /// Deliver wakes for as long as the process lives.
    pub async fn run(&self, deliver: Arc<dyn Deliver>) -> anyhow::Result<()> {
        loop {
            let delivered = self.deliver_due(&deliver).await?;
            if delivered as i64 >= BATCH {
                continue;
            }
            let wait = match weft_task_store::alarm::next_due(&self.pool).await? {
                Some(at) => Duration::from_millis((at - now_ms()).max(0) as u64).min(LOOK_AGAIN),
                None => LOOK_AGAIN,
            };
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = self.changed.notified() => {}
            }
        }
    }

    /// Claim what is due now and deliver it. Returns how many were due.
    pub async fn deliver_due(&self, deliver: &Arc<dyn Deliver>) -> anyhow::Result<usize> {
        let now = now_ms();
        let due = weft_task_store::alarm::claim_due(&self.pool, now, LEASE_MS, BATCH).await?;
        let count = due.len();
        let mut running = tokio::task::JoinSet::new();
        for row in due {
            let deliver = deliver.clone();
            let pool = self.pool.clone();
            running.spawn(async move {
                let role = match CoreRole::parse(&row.role) {
                    Ok(role) => role,
                    Err(e) => {
                        // A row no role can take is dropped loudly: no
                        // retry would ever deliver it.
                        tracing::error!(target: "weft_platform_local::alarm", wake = %row.name, error = %e, "dropping a wake for no known role");
                        return weft_task_store::alarm::delete(&pool, &row.name).await;
                    }
                };
                let call = WakeCall { at_unix_ms: row.set_for_unix_ms, body: row.body.clone() };
                match deliver.deliver(role, &row.path, call).await {
                    Ok(()) => weft_task_store::alarm::delete(&pool, &row.name).await,
                    Err(e) => {
                        let next = now_ms() + backoff_ms(row.attempts);
                        tracing::warn!(
                            target: "weft_platform_local::alarm",
                            wake = %row.name, key = %row.key, attempt = row.attempts, error = %format!("{e:#}"),
                            "a wake was not taken; trying again later"
                        );
                        weft_task_store::alarm::retry_at(&pool, &row.name, next).await
                    }
                }
            });
        }
        while let Some(done) = running.join_next().await {
            done??;
        }
        Ok(count)
    }
}

#[async_trait]
impl Alarm for LocalAlarm {
    async fn set(&self, wake: Wake) -> anyhow::Result<()> {
        weft_task_store::alarm::set(
            &self.pool,
            &wake.name(),
            &wake.key,
            wake.at_unix_ms,
            wake.role.as_str(),
            &wake.path,
            &wake.body,
        )
        .await?;
        self.changed.notify_one();
        Ok(())
    }
}

/// Hands a due wake to its role.
#[async_trait]
pub trait Deliver: Send + Sync {
    async fn deliver(&self, role: CoreRole, path: &str, call: WakeCall<serde_json::Value>) -> anyhow::Result<()>;
}

/// Posts the wake to the role's internal address with the install's own
/// identity.
pub struct HttpDeliver {
    roles: RoleAddresses,
    tokens: Arc<dyn IdentityTokens>,
    http: reqwest::Client,
}

impl HttpDeliver {
    pub fn new(roles: RoleAddresses, tokens: Arc<dyn IdentityTokens>) -> Self {
        Self { roles, tokens, http: reqwest::Client::new() }
    }
}

#[async_trait]
impl Deliver for HttpDeliver {
    async fn deliver(&self, role: CoreRole, path: &str, call: WakeCall<serde_json::Value>) -> anyhow::Result<()> {
        let base = self.roles.of(role)?;
        let token = self.tokens.token_for(base).await?;
        self.http
            .post(format!("{base}{path}"))
            .bearer_auth(token)
            .json(&call)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}

/// How long a wake that failed `attempts` times waits before the next try:
/// doubling from a second, at most [`MAX_BACKOFF_MS`].
fn backoff_ms(attempts: i32) -> i64 {
    let exp = attempts.clamp(1, 20) as u32 - 1;
    (1000i64 << exp).min(MAX_BACKOFF_MS)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock past UNIX_EPOCH")
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_failing_wake_backs_off_doubling_up_to_the_cap() {
        assert_eq!(backoff_ms(1), 1000);
        assert_eq!(backoff_ms(2), 2000);
        assert_eq!(backoff_ms(4), 8000);
        assert_eq!(backoff_ms(40), MAX_BACKOFF_MS);
    }
}
