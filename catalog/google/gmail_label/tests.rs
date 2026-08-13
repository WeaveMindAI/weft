//! GmailLabel self-tests: names resolve to ids, an unknown add-label
//! is created, an unknown remove-label refuses.

use serde_json::json;

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use super::super::gmail_send::GmailSendNode;
use super::GmailLabelNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("resolves_names_and_creates_the_missing_add_label", labels),
        NodeTest::fake("an_unknown_remove_label_refuses", unknown_remove),
        NodeTest::live("one_real_add_then_remove", "google", live_label).with_fixture(
            fixture_spec(
                "GMAIL_TO",
                "Recipient address",
                "The address the test sends to; the connected account's own address \
                 keeps the send self-contained.",
            ),
        ),
    ]
}

fn canned_listing(rig: &FakeRig) {
    rig.respond(
        "GET",
        "/gmail/v1/users/me/labels",
        json!({ "labels": [
            { "id": "L1", "name": "Invoices" },
            { "id": "L2", "name": "Todo" },
        ]}),
    );
}

async fn labels(rig: FakeRig) -> WeftResult<()> {
    canned_listing(&rig);
    rig.respond("POST", "/gmail/v1/users/me/labels", json!({ "id": "L9", "name": "Fresh" }));
    rig.respond(
        "POST",
        "/gmail/v1/users/me/messages/m1/modify",
        json!({ "labelIds": ["L1", "L9"] }),
    );
    let outcome = rig
        .run(
            &GmailLabelNode,
            json!({
                "account": rig.access("google"),
                "id": "m1",
                "add": ["invoices", "Fresh"],
                "remove": "Todo",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["labels"], json!(["L1", "L9"]));
    let sent = rig.requests();
    assert_eq!(
        sent[1].body.as_ref().expect("create body"),
        &json!({ "name": "Fresh" }),
        "the unknown add-label is created on the fly"
    );
    assert_eq!(
        sent[2].body.as_ref().expect("modify body"),
        &json!({ "addLabelIds": ["L1", "L9"], "removeLabelIds": ["L2"] }),
        "names resolved case-insensitively to ids"
    );
    Ok(())
}

async fn unknown_remove(rig: FakeRig) -> WeftResult<()> {
    canned_listing(&rig);
    let outcome = rig
        .run(
            &GmailLabelNode,
            json!({ "account": rig.access("google"), "id": "m1", "remove": "Nope" }),
        )
        .await;
    let err = outcome.result.expect_err("unknown remove must refuse").to_string();
    assert!(err.contains("no label named 'Nope'"), "{err}");
    Ok(())
}

async fn live_label(rig: LiveRig) -> WeftResult<()> {
    // A message id cannot be guessed, so the test self-provisions one
    // by sending a mail to the fixture address, then labels it and
    // reverts the label. The 'weft-node-tests' LABEL persists in the
    // account (created on the first run, reused ever after); the sent
    // mail stays (the artifact IS the proof).
    let to = rig.fixture("GMAIL_TO")?;
    let sent = rig
        .run(
            &GmailSendNode,
            json!({
                "account": rig.access("google"),
                "to": to,
                "subject": "weft-node-tests",
                "text": "sent by the GmailLabel live self-test",
            }),
        )
        .await
        .ok()?;
    let id = sent.output("id")?.as_str().expect("message id").to_string();

    let added = rig
        .run(
            &GmailLabelNode,
            json!({ "account": rig.access("google"), "id": id, "add": "weft-node-tests" }),
        )
        .await
        .ok()?;
    let after_add = added.output("labels")?.as_array().expect("label ids").len();

    let removed = rig
        .run(
            &GmailLabelNode,
            json!({ "account": rig.access("google"), "id": id, "remove": "weft-node-tests" }),
        )
        .await
        .ok()?;
    let after_remove = removed.output("labels")?.as_array().expect("label ids").len();
    assert_eq!(
        after_add,
        after_remove + 1,
        "the add put exactly one label on and the remove took it back off"
    );
    Ok(())
}
