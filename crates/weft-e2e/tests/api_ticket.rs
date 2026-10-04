//! A browser's socket TICKET: what asking for a socket buys you, and for
//! how long.
//!
//! A browser cannot put a credential on a WebSocket's opening request, so
//! it asks with a plain request first. That checks the caller, gives birth
//! to their run and hands back a URL on the live door carrying a signed
//! ticket: which run, which program to reach and until when. The run waits
//! for the caller that long; one whose caller never came is erased once the
//! ticket expires. When the caller does come, the door starts a worker of
//! that program if none is running and passes the socket to it, and that
//! worker claims the run.
//!
//! Elapsed time against a live install is the thing under test, so it runs
//! in a cell whose clock runs four times faster: the ticket's life and the
//! waits below are the real-time figures, compressed together, so the
//! story is the same three minutes told in 45 seconds. No faster: a late
//! caller may have to wait for a worker to start, and that is real
//! work no clock compresses.
#![cfg(feature = "e2e")]

use serde_json::{json, Value};
use weft_e2e::{live, project::Project, Cell};

/// A ticket is good for its whole life, and not a moment longer.
///
/// Two callers, one after the other, both real:
///
///   - a minute late, well inside the ticket's life: served normally,
///     whatever happened to the project's workers in the meantime.
///   - three minutes late, past the ticket entirely: refused, in words
///     that say to ask again.
///
/// The minutes below are the cell's: three of them pass in 45 real
/// seconds. The rule it rests on is pinned fast in the dispatcher's
/// database tests; this proves the whole path.
#[tokio::test]
async fn a_late_caller_is_served_and_a_much_later_one_is_told_to_ask_again() -> anyhow::Result<()> {
    let cell = Cell::start(0.25).await?;
    let disp = cell.dispatcher();
    let mut project = Project::prepare("live_chat", disp.clone()).await?;
    let path = project.unique_live_path()?;
    project.activate().await?;

    // Both tickets are taken now, so only the arrival times differ.
    let soon = live::ticket(&disp, &path, &[]).await?;
    let late = live::ticket(&disp, &path, &[]).await?;
    anyhow::ensure!(soon != late, "each caller gets their own ticket");

    // A minute of nothing at all: no calls, no runs, nobody touching
    // the project.
    tokio::time::sleep(cell.scaled(std::time::Duration::from_secs(60))).await;

    let mut ws = match live::open_socket_at(&soon).await? {
        Ok(ws) => ws,
        Err((status, said)) => anyhow::bail!("a caller a minute late must still be served, got HTTP {status}: {said}"),
    };
    ws.send_json(&json!("hello")).await?;
    let answered: Value = ws.recv_json(cell.scaled(std::time::Duration::from_secs(60))).await?;
    anyhow::ensure!(answered["echo"] == json!("hello"), "and be served properly: {answered}");
    ws.close().await?;

    // Two more minutes, which is past the second ticket's life.
    tokio::time::sleep(cell.scaled(std::time::Duration::from_secs(120))).await;

    let Err((status, said)) = live::open_socket_at(&late).await? else {
        anyhow::bail!("a spent ticket must open nothing");
    };
    anyhow::ensure!(status == 401, "a spent ticket opens nothing, got HTTP {status}: {said}");
    anyhow::ensure!(said.contains("expired"), "the caller is told why: {said}");
    anyhow::ensure!(
        said.contains("Ask for a new one"),
        "and what to do about it, rather than being left guessing: {said}"
    );

    // And asking again works, which is the whole of the advice.
    let fresh = live::ticket(&disp, &path, &[]).await?;
    let Ok(ws) = live::open_socket_at(&fresh).await? else {
        anyhow::bail!("a fresh ticket opens a socket");
    };
    ws.close().await?;

    project.finish().await?;
    cell.finish().await
}
