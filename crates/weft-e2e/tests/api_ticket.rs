//! A live caller's TICKET: what asking for a connection buys you, and
//! for how long.
//!
//! Asking at `/connect/...` picks one worker and hands back a URL
//! naming it. Nothing is queued at that moment, on purpose, so a caller
//! who never comes leaves nothing behind. The cost of that choice is
//! that nothing else in the system knows the caller is expected, which
//! is what the worker's promise (`held_until_unix`) exists to fix.
//!
//! Elapsed time against a live cluster is the thing under test, so it runs
//! in a cell whose clock runs four times faster: the ticket, the worker's
//! idle exit and the waits below are all the real-time figures, compressed
//! together, so the story is the same three minutes told in 45 seconds.
//! Not [`Cell::FAST`]: the ticket's life starts before the worker it names
//! is ready, and that part is real work that no clock compresses. A tenth
//! of two minutes left too little of the ticket once a busy cluster had
//! spent a few seconds starting the worker.
#![cfg(feature = "e2e")]

use reqwest::Method;
use serde_json::{json, Value};
use weft_e2e::{live, project::Project, Cell};

/// A ticket is a promise, and the worker keeps it.
///
/// Asking for a connection picks ONE worker and hands the caller a
/// ticket naming it. Nothing is queued at that moment on purpose, so
/// every query in the system says that worker has nothing to do, and a
/// worker with nothing to do shuts itself down in half a minute. A
/// caller on a slow phone would follow the redirect into a machine that
/// had already gone, holding a ticket still good for another minute and
/// a half.
///
/// Two callers, one after the other, both real:
///
///   - a minute late, which is well inside the ticket's life and well
///     past the half minute an idle worker survives: served normally.
///     If the promise were not kept this is the one that breaks, and it
///     breaks as a connection failure rather than an answer.
///   - three minutes late, past the ticket entirely: refused, in words
///     that say to ask again. The worker is deliberately kept a while
///     longer than the ticket so it is there to say this, instead of
///     the caller meeting a socket that does not answer.
///
/// The minutes below are the cell's: three of them pass in 45 real
/// seconds. The rule it rests on is pinned fast in the dispatcher's
/// database tests; this proves the whole path.
#[tokio::test]
async fn a_late_caller_is_served_and_a_much_later_one_is_told_to_ask_again() -> anyhow::Result<()> {
    let cell = Cell::start(0.25).await?;
    let disp = cell.dispatcher();
    let mut project = Project::prepare("api_reply", disp.clone()).await?;
    let base = project.unique_live_path()?;
    project.activate().await?;

    // Both tickets are taken now, so both are promised the same worker
    // from the same moment and only the arrival times differ.
    let lookup = format!("{base}/users/42");
    let soon = live::ticket(&disp, Method::GET, &lookup, &[], None).await?;
    let late = live::ticket(&disp, Method::GET, &lookup, &[], None).await?;
    anyhow::ensure!(soon != late, "each caller gets their own ticket");

    // A minute of nothing at all: no calls, no runs, nobody touching
    // the project. The worker's own idle timer is half that.
    tokio::time::sleep(cell.scaled(std::time::Duration::from_secs(60))).await;

    let (status, body) = live::follow(&soon, Method::GET, &[], None).await?;
    anyhow::ensure!(
        status == 200,
        "a caller a minute late must still find their worker, got HTTP {status}: {}",
        String::from_utf8_lossy(&body)
    );
    let answered: Value = serde_json::from_slice(&body)?;
    anyhow::ensure!(answered["name"] == json!("Ada"), "and be served properly: {answered}");

    // Two more minutes. The first caller's run finished long ago, so
    // the worker is idle again and only the promise is holding it up.
    tokio::time::sleep(cell.scaled(std::time::Duration::from_secs(120))).await;

    let (status, body) = live::follow(&late, Method::GET, &[], None).await?;
    let said = String::from_utf8_lossy(&body).into_owned();
    anyhow::ensure!(status == 401, "a spent ticket opens nothing, got HTTP {status}: {said}");
    anyhow::ensure!(said.contains("expired"), "the caller is told why: {said}");
    anyhow::ensure!(
        said.contains("Ask for a new one"),
        "and what to do about it, rather than being left guessing: {said}"
    );

    // And asking again works, which is the whole of the advice.
    let fresh = live::ticket(&disp, Method::GET, &lookup, &[], None).await?;
    let (status, body) = live::follow(&fresh, Method::GET, &[], None).await?;
    anyhow::ensure!(status == 200, "{}", String::from_utf8_lossy(&body));

    project.finish().await?;
    cell.finish().await
}
