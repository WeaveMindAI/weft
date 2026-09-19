//! Streaming with zero custom Rust: a bus behind a route, framed as
//! server-sent events and as ndjson, read whole by the rig. Plus the
//! lifetime of a streaming run: a caller who hangs up on a feed that has
//! gone quiet still ends it.
#![cfg(feature = "e2e")]

use reqwest::Method;
use weft_e2e::{ensure, live, project::Project};

fn header<'a>(headers: &'a reqwest::header::HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

#[tokio::test]
async fn a_bus_streams_as_sse_and_ndjson() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("api_stream", disp.clone()).await?;
    let base = project.unique_live_path()?;
    project.activate().await?;

    let (status, headers, body) =
        live::http_request(&disp, Method::GET, &format!("{base}/sse"), &[], None).await?;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    assert_eq!(header(&headers, "content-type"), Some("text/event-stream"));
    assert_eq!(String::from_utf8_lossy(&body), "data: t1\n\ndata: t2\n\ndata: t3\n\n");

    let (status, headers, body) =
        live::http_request(&disp, Method::GET, &format!("{base}/lines"), &[], None).await?;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    assert_eq!(header(&headers, "content-type"), Some("application/x-ndjson"));
    assert_eq!(header(&headers, "x-run"), Some("e2e"), "the Stream's own headers ride the head");
    assert_eq!(String::from_utf8_lossy(&body), "\"t1\"\n\"t2\"\n\"t3\"\n");

    project.finish().await
}

/// A feed that says one thing and then goes quiet, and a caller who hangs
/// up. The run behind the route is cancelled without the program writing
/// another byte: the worker watches the connection itself, and the
/// keepalive the framing ignores is the second way it finds out. Before
/// this, only a real write could discover the caller had left, so a
/// watched table nobody touched held its run open for ever, one per
/// connection.
#[tokio::test]
async fn a_caller_leaving_a_quiet_stream_ends_its_run() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("api_stream", disp.clone()).await?;
    let base = project.unique_live_path()?;
    project.activate().await?;
    let before = weft_e2e::run::execution_colors(&disp, &project.id()).await?;

    let first = live::stream_first_chunk_then_hang_up(&disp, &format!("{base}/quiet")).await?;
    assert_eq!(String::from_utf8_lossy(&first), "data: first\n\n");

    // The caller is gone and the program has nothing more to say. The run
    // must end anyway, and end as the caller's doing.
    let colors = weft_e2e::run::wait_for_triggered_executions(
        &disp,
        &project.id(),
        &before,
        1,
        std::time::Duration::from_secs(60),
    )
    .await?;
    let settled = weft_e2e::run::SettledRun::observe_within(
        &disp,
        colors[0],
        std::time::Duration::from_secs(120),
    )
    .await?;
    anyhow::ensure!(
        settled.status == "cancelled",
        "the run should end with its caller, got {} ({:?})",
        settled.status,
        settled.cancel_reason()
    );

    project.finish().await
}

/// A caller that is THERE but has stopped reading: the socket stays
/// open and its machine keeps acknowledging, it just never consumes.
///
/// Deliberately NOT asserting the run ends. This is the case the
/// socket-level bound cannot see and should not: the far side is
/// answering, so by every measure available it is alive, and cutting it
/// would be cutting a slow reader. What this pins is that the exchange
/// survives it rather than erroring, and the run ends the moment the
/// caller actually goes.
///
/// The case that DOES matter, a caller whose machine stops answering,
/// cannot be produced from a healthy local socket: the kernel on this
/// side keeps acknowledging whatever we do in user space. Proving it
/// needs the packets dropped underneath us (a firewall rule, a severed
/// network), which is a harness this rig does not have. That gap is
/// real and worth closing; it is not something this test can fake.
#[tokio::test]
async fn a_caller_that_stops_reading_does_not_break_the_exchange() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("api_stream", disp.clone()).await?;
    let base = project.unique_live_path()?;
    project.activate().await?;
    let before = weft_e2e::run::execution_colors(&disp, &project.id()).await?;

    {
        // Held, then dropped: while held the caller is present and
        // silent, and dropping it is the caller finally leaving.
        let _reading_nothing =
            live::stream_first_chunk_then_stop_reading(&disp, &format!("{base}/quiet")).await?;
    }

    let colors = weft_e2e::run::wait_for_triggered_executions(
        &disp,
        &project.id(),
        &before,
        1,
        std::time::Duration::from_secs(60),
    )
    .await?;
    let settled = weft_e2e::run::SettledRun::observe_within(
        &disp,
        colors[0],
        std::time::Duration::from_secs(120),
    )
    .await?;
    anyhow::ensure!(
        settled.status == "cancelled",
        "the run ends once the caller really goes, got {} ({:?})",
        settled.status,
        settled.cancel_reason()
    );

    project.finish().await
}

/// Many callers arriving and abandoning the same feed in a row, which is
/// what a person refreshing a page does. The count that matters is not
/// whether ONE run ended, it is whether the standing population comes
/// back to nothing: a leak of one per connection is invisible in a
/// single-run test and obvious here.
#[tokio::test]
async fn abandoned_streams_do_not_pile_up() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("api_stream", disp.clone()).await?;
    let base = project.unique_live_path()?;
    project.activate().await?;
    let before = weft_e2e::run::execution_colors(&disp, &project.id()).await?;

    const CALLERS: usize = 4;
    for _ in 0..CALLERS {
        let first =
            live::stream_first_chunk_then_hang_up(&disp, &format!("{base}/quiet")).await?;
        anyhow::ensure!(!first.is_empty(), "each caller got its first event");
    }

    let colors = weft_e2e::run::wait_for_triggered_executions(
        &disp,
        &project.id(),
        &before,
        CALLERS,
        std::time::Duration::from_secs(120),
    )
    .await?;
    for color in colors {
        let settled = weft_e2e::run::SettledRun::observe_within(
            &disp,
            color,
            std::time::Duration::from_secs(120),
        )
        .await?;
        anyhow::ensure!(
            settled.status == "cancelled",
            "every abandoned stream must end, {color} is {} ({:?})",
            settled.status,
            settled.cancel_reason()
        );
    }

    project.finish().await
}
