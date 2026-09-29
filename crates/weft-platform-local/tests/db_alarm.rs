//! The local alarm against a real Postgres: a wake set before a restart
//! is delivered after it, a wake the receiver refuses comes back, and the
//! same wake set twice is delivered once.
#![cfg(feature = "db-tests")]

use std::sync::Arc;

use async_trait::async_trait;
use sqlx::PgPool;
use weft_platform_local::{Deliver, LocalAlarm};
use weft_platform_traits::{Alarm, CoreRole, Wake, WakeCall};

#[derive(Default)]
struct Recorder {
    got: parking_lot::Mutex<Vec<(CoreRole, String, WakeCall<serde_json::Value>)>>,
    refuse: parking_lot::Mutex<bool>,
}

#[async_trait]
impl Deliver for Recorder {
    async fn deliver(&self, role: CoreRole, path: &str, call: WakeCall<serde_json::Value>) -> anyhow::Result<()> {
        if *self.refuse.lock() {
            anyhow::bail!("not now");
        }
        self.got.lock().push((role, path.to_string(), call));
        Ok(())
    }
}

async fn schema(pool: &PgPool) {
    weft_task_store::apply_groups(pool, &[&weft_task_store::alarm::GROUP]).await.unwrap();
}

fn wake(at: i64) -> Wake {
    Wake { key: "signal:t".into(), at_unix_ms: at, role: CoreRole::Listener, path: "/wake".into(), body: serde_json::json!({ "token": "t" }) }
}

fn past() -> i64 {
    now_ms() - 1_000
}

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64
}

#[sqlx::test]
async fn a_wake_set_before_a_restart_is_delivered_after_it_once(pool: PgPool) {
    schema(&pool).await;
    let at = past();
    LocalAlarm::new(pool.clone()).set(wake(at)).await.unwrap();
    LocalAlarm::new(pool.clone()).set(wake(at)).await.unwrap();

    // A new process: nothing in memory, the row is all there is.
    let recorder = Arc::new(Recorder::default());
    let deliver: Arc<dyn Deliver> = recorder.clone();
    let after_restart = LocalAlarm::new(pool.clone());
    assert_eq!(after_restart.deliver_due(&deliver).await.unwrap(), 1, "the same wake set twice is one wake");
    let got = recorder.got.lock().clone();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].0, CoreRole::Listener);
    assert_eq!(got[0].1, "/wake");
    assert_eq!(got[0].2.at_unix_ms, at, "the receiver hears the moment it was set for");
    assert_eq!(after_restart.deliver_due(&deliver).await.unwrap(), 0, "delivered wakes are gone");
}

#[sqlx::test]
async fn a_refused_wake_comes_back_later(pool: PgPool) {
    schema(&pool).await;
    let alarm = LocalAlarm::new(pool.clone());
    alarm.set(wake(past())).await.unwrap();
    let recorder = Arc::new(Recorder::default());
    *recorder.refuse.lock() = true;
    let deliver: Arc<dyn Deliver> = recorder.clone();
    assert_eq!(alarm.deliver_due(&deliver).await.unwrap(), 1);
    assert_eq!(alarm.deliver_due(&deliver).await.unwrap(), 0, "backing off, not due again at once");
    let (at, attempts): (i64, i32) = sqlx::query_as("SELECT at_unix_ms, attempts FROM alarm").fetch_one(&pool).await.unwrap();
    assert!(at > now_ms(), "set to try again later");
    assert_eq!(attempts, 1);

    sqlx::query("UPDATE alarm SET at_unix_ms = 0").execute(&pool).await.unwrap();
    *recorder.refuse.lock() = false;
    assert_eq!(alarm.deliver_due(&deliver).await.unwrap(), 1);
    assert_eq!(recorder.got.lock().len(), 1, "taken on the retry");
}
