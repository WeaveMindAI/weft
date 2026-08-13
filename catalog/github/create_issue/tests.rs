//! GitHubCreateIssue self-tests: the one authenticated POST.

use serde_json::json;

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use super::GitHubCreateIssueNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("creates_the_issue_and_emits_url_and_number", creates_issue),
        NodeTest::fake("a_github_refusal_fails_loud_with_its_message", refused),
        NodeTest::live("one_real_issue_on_the_fixture_repo", "github", live_create).with_fixture(
            fixture_spec(
                "GITHUB_REPO",
                "Repository",
                "The owner/repo the test opens (then closes) its issue on; dedicate \
                 a repo to these tests.",
            ),
        ),
    ]
}

/// Open one issue on the fixture repo (a repo dedicated to these
/// tests), then close it through the test's own connection so
/// repeated runs never pile open issues into the repo. GitHub offers
/// no issue deletion, so closed is as clean as it gets.
async fn live_create(rig: LiveRig) -> WeftResult<()> {
    let repo = rig.fixture("GITHUB_REPO")?;
    let outcome = rig
        .run(
            &GitHubCreateIssueNode,
            json!({
                "account": rig.access("github"),
                "repo": repo,
                "title": "weft-node-tests",
                "body": "Opened by the GitHubCreateIssue live node test; closed by it too.",
            }),
        )
        .await
        .ok()?;
    // Harvest, then CLOSE, then assert: an assertion between the create
    // and the close would leave the issue open on the repo when it
    // fails. The close is the cleanup, so it must be unskippable.
    let number = outcome.output("number")?.as_i64().expect("issue number");
    let url = outcome.output("url")?.as_str().expect("issue url").to_string();

    let conn = rig.connect().await?;
    let closed = weft::access::client::json_call(
        conn.client()
            .patch(format!("https://api.github.com/repos/{repo}/issues/{number}"))
            .header("Accept", "application/vnd.github+json")
            .json(&json!({ "state": "closed" })),
        "close the test issue",
    )
    .await?;

    assert!(number > 0);
    assert!(url.contains(&repo), "the url names the fixture repo");
    assert_eq!(closed["state"], json!("closed"), "the issue ends closed");
    Ok(())
}

async fn creates_issue(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/repos/acme/widgets/issues",
        json!({ "html_url": "https://github.com/acme/widgets/issues/7", "number": 7 }),
    );
    let outcome = rig
        .run(
            &GitHubCreateIssueNode,
            json!({
                "account": rig.access("github"),
                "repo": "acme/widgets",
                "title": "It breaks",
                "body": "Steps: ...",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["url"], json!("https://github.com/acme/widgets/issues/7"));
    assert_eq!(outcome.outputs["number"], json!(7));
    let body = rig.requests()[0].body.clone().expect("issue body");
    assert_eq!(body, json!({ "title": "It breaks", "body": "Steps: ..." }));
    Ok(())
}

async fn refused(rig: FakeRig) -> WeftResult<()> {
    rig.respond_status(
        "POST",
        "/repos/acme/widgets/issues",
        404,
        json!({ "message": "Not Found" }),
    );
    let outcome = rig
        .run(
            &GitHubCreateIssueNode,
            json!({ "account": rig.access("github"), "repo": "acme/widgets", "title": "t" }),
        )
        .await;
    let err = outcome.result.expect_err("a 404 must refuse").to_string();
    assert!(err.contains("Not Found"), "{err}");
    Ok(())
}
