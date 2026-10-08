//! The schema every database test in this crate stands up: the crate's own
//! task and worker-process groups.

use std::sync::Arc;

use sqlx::PgPool;
use weft_task_store::pg_signal::PgSignalWatch;
use weft_task_store::tasks;

/// Every channel this crate's writes notify on. (Each test file
/// compiles this module; the ones that never wait leave these unused.)
#[allow(dead_code)]
pub const CHANNELS: &[&str] = &[tasks::TASK_READY_CHANNEL, weft_task_store::terminal::TERMINAL_CHANNEL];

/// A signal watch on the test database, listening on [`CHANNELS`].
#[allow(dead_code)]
pub async fn signals(pool: &PgPool) -> Arc<PgSignalWatch> {
    PgSignalWatch::start(&pool.connect_options(), CHANNELS).await.expect("start the signal watch")
}

/// Apply the task and worker-process schema to a fresh test database.
pub async fn setup(pool: &PgPool) {
    weft_task_store::apply_groups(pool, &[&weft_task_store::announce::GROUP, &weft_task_store::worker_door::GROUP, &tasks::GROUP])
        .await
        .expect("tasks schema");
}
