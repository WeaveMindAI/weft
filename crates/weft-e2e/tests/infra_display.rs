//! An infra node's display, through the doors an app built on a weft program
//! uses: the signal-token listing, the read, and the button press. The trigger
//! side of these doors is covered by `web_trigger`; this is the side with a
//! button that does something.
#![cfg(feature = "e2e")]

use std::time::Duration;

use weft_e2e::{display, ensure, infra, poll_until, project::Project};

#[tokio::test]
async fn a_token_reads_an_infra_display_and_presses_its_button() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("infra_display", disp).await?;
    infra::start_and_wait_running(&mut project, "panel").await?;
    let pid = project.id();

    // A token granted this one display finds it, as an infra display.
    let token =
        display::mint_display_token(project.dispatcher(), &pid, "weft-e2e-panel", &["panel"], false)
            .await?;
    let listed = display::list_for_token(project.dispatcher(), &token).await?;
    anyhow::ensure!(
        listed.len() == 1 && listed[0]["node"] == "panel" && listed[0]["kind"] == "infra",
        "the token lists the one display it was granted: {listed:?}"
    );

    // It reads what the container serves, and presses its button. A
    // Service answers a moment after its pod reports ready, and the read
    // door answers 502 in that gap (a panel polls, so its next look
    // lands), so the first read waits for the door to answer.
    poll_until("the panel's first answer", Duration::from_secs(30), Duration::from_millis(500), || async {
        let (status, _) = project
            .dispatcher()
            .get_raw_bearer(&format!("/signal-token/displays/{pid}/panel"), &token)
            .await?;
        Ok(status.is_success().then_some(()))
    })
    .await?;
    let shown = display::as_token(project.dispatcher(), &token, &pid, "panel").await?;
    anyhow::ensure!(shown.expect_text("Presses")? == "0", "nothing pressed yet: {shown:?}");
    let (status, body) = display::press_status(project.dispatcher(), &token, &pid, "panel", "press").await?;
    anyhow::ensure!(status.is_success(), "the press reaches the container: {status} {body}");
    let shown = display::as_token(project.dispatcher(), &token, &pid, "panel").await?;
    anyhow::ensure!(shown.expect_text("Presses")? == "1", "the press changed what it shows: {shown:?}");

    // A button the container does not have is the caller's mistake, said as one.
    let (status, body) = display::press_status(project.dispatcher(), &token, &pid, "panel", "explode").await?;
    anyhow::ensure!(
        status == reqwest::StatusCode::BAD_REQUEST,
        "an unknown button is a 400 naming the container's answer, got {status}: {body}"
    );

    // A token granted no display reaches neither the read nor the button.
    let blind =
        display::mint_display_token(project.dispatcher(), &pid, "weft-e2e-blind", &[], false).await?;
    let status = display::read_status(project.dispatcher(), &blind, &pid, "panel").await?;
    anyhow::ensure!(!status.is_success(), "a token with no display grant reads nothing, got {status}");
    let (status, _) = display::press_status(project.dispatcher(), &blind, &pid, "panel", "press").await?;
    anyhow::ensure!(!status.is_success(), "nor presses anything, got {status}");
    let shown = display::as_token(project.dispatcher(), &token, &pid, "panel").await?;
    anyhow::ensure!(shown.expect_text("Presses")? == "1", "the refused press did nothing: {shown:?}");

    infra::terminate_and_wait_gone(&project, "panel").await?;
    project.finish().await
}
