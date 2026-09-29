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
    // unit's port answers a moment after it reports ready, and the read
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

/// A member's display says where their copy stands: the listing a member
/// token reads gives the copy's `status`, and once the copy is stopped the
/// read door answers 404 naming it, instead of a connection error from a
/// copy scaled to nothing.
#[tokio::test]
async fn a_members_display_says_where_their_copy_stands() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("infra_display", disp.clone()).await?;
    project.set_main("panel = DisplayService {\n  @per_member\n}\n")?;
    let pid = project.id();
    let status_of = |listed: &[serde_json::Value]| {
        listed.iter().find(|d| d["node"] == "panel").and_then(|d| d["status"].as_str().map(str::to_string))
    };

    project.weft(&["infra", "start", "--member", "ada"]).await?;
    let minted: serde_json::Value = serde_json::from_str(
        project.weft(&["token", "mint", "--member", "ada", "--expires", "1d", "--display", "panel", "--json"]).await?.trim(),
    )?;
    let token = minted["token"].as_str().expect("a token").to_string();
    let read = format!("/signal-token/displays/{pid}/panel");
    poll_until("ada's panel to answer", Duration::from_secs(300), Duration::from_millis(750), || async {
        let listed = display::list_for_token(&disp, &token).await?;
        anyhow::ensure!(status_of(&listed).as_deref() != Some("failed"), "ada's copy failed: {listed:?}");
        if status_of(&listed).as_deref() != Some("running") {
            return Ok(None);
        }
        let (status, _) = disp.get_raw_bearer(&read, &token).await?;
        Ok(status.is_success().then_some(()))
    })
    .await?;

    project.weft(&["infra", "node-stop", "panel", "--force", "--member", "ada"]).await?;
    poll_until("ada's copy to stop", Duration::from_secs(300), Duration::from_millis(750), || async {
        Ok((status_of(&display::list_for_token(&disp, &token).await?).as_deref() == Some("stopped")).then_some(()))
    })
    .await?;
    let (status, body) = disp.get_raw_bearer(&read, &token).await?;
    anyhow::ensure!(
        status == reqwest::StatusCode::NOT_FOUND && body.contains("stopped"),
        "a stopped copy's display is a 404 naming its state: {status} {body}"
    );

    project.weft(&["infra", "node-terminate", "panel", "--member", "ada"]).await?;
    poll_until("ada's copy to go", Duration::from_secs(300), Duration::from_millis(750), || async {
        Ok((status_of(&display::list_for_token(&disp, &token).await?).as_deref() == Some("absent")).then_some(()))
    })
    .await?;
    project.finish().await
}
