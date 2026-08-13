//! SendEmail self-tests: everything that must refuse BEFORE the SMTP
//! submission (fake; the fake rig has no sockets), and one real
//! send-to-self (live).

use serde_json::json;

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use super::SendEmailNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("an_unparseable_address_refuses_before_sending", bad_address),
        NodeTest::fake("a_non_numeric_smtp_port_refuses_before_sending", bad_port),
        NodeTest::live("one_real_send_to_self", "email", live_send).with_fixture(fixture_spec(
            "EMAIL_TO",
            "Recipient address",
            "The address the test sends to; the connected mailbox's own address \
             keeps the send self-contained.",
        )),
    ]
}

fn connection(rig: &FakeRig) {
    rig.connection_value("email", "user", "sender@example.com");
    rig.connection_value("email", "password", "secret");
    rig.connection_value("email", "smtp_host", "smtp.example.com");
}

async fn bad_address(rig: FakeRig) -> WeftResult<()> {
    connection(&rig);
    rig.connection_value("email", "smtp_port", "587");
    let outcome = rig
        .run(
            &SendEmailNode,
            json!({
                "account": rig.access("email"),
                "to": "not an address",
                "subject": "s",
                "body": "b",
            }),
        )
        .await;
    let err = outcome.result.expect_err("a bad To must refuse").to_string();
    assert!(err.contains("To address"), "{err}");
    assert!(rig.requests().is_empty(), "nothing was sent");
    Ok(())
}

async fn bad_port(rig: FakeRig) -> WeftResult<()> {
    connection(&rig);
    rig.connection_value("email", "smtp_port", "not-a-port");
    let outcome = rig
        .run(
            &SendEmailNode,
            json!({
                "account": rig.access("email"),
                "to": "someone@example.com",
                "subject": "s",
                "body": "b",
            }),
        )
        .await;
    let err = outcome.result.expect_err("a bad port must refuse").to_string();
    assert!(err.contains("not a number"), "{err}");
    Ok(())
}

async fn live_send(rig: LiveRig) -> WeftResult<()> {
    // A mailbox cannot be created from here: the recipient (the
    // connected account's own address) is a fixture.
    let to = rig.fixture("EMAIL_TO")?;
    let outcome = rig
        .run(
            &SendEmailNode,
            json!({
                "account": rig.access("email"),
                "to": to,
                "subject": "weft node test",
                "body": "sent by the SendEmail live self-test",
            }),
        )
        .await
        .ok()?;
    let id = outcome.output("messageId")?.as_str().expect("message id");
    assert!(!id.is_empty(), "the send minted a Message-ID");
    Ok(())
}
