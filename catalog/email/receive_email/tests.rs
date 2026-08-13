//! ReceiveEmail self-tests: the registered IMAP watch dialogue (the
//! run body opens a raw IMAP socket, which no rig fakes; the watch
//! registration is the trigger's language-facing contract).

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::ReceiveEmailNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("setup_registers_the_imap_idle_watch", setup_registers)]
}

async fn setup_registers(rig: FakeRig) -> WeftResult<()> {
    rig.run_setup_trigger(&ReceiveEmailNode, json!({ "account": rig.access("email") }))
        .await
        .ok()?;
    let registered = rig.registered_signals();
    assert_eq!(registered.len(), 1, "one watch signal");
    let spec = &registered[0].0;
    assert_eq!(spec.kind, "stream_listen");
    let config = &spec.config;
    assert_eq!(
        config["address"],
        json!("{imap_host}:{imap_port}"),
        "the address resolves from the connection's stored values"
    );
    assert_eq!(
        config["fire"],
        json!(r"^\* \d+ (EXISTS|RECENT)"),
        "fires on the server's arrival announcement"
    );
    // The connect dialogue: sign in, open the inbox, hold an IDLE.
    let script = config["script"].as_array().expect("connect script");
    assert_eq!(script.len(), 3);
    assert!(
        script[0]["send"]["body"].as_str().expect("login frame").contains("LOGIN"),
        "{script:?}"
    );
    assert!(
        script[1]["send"]["body"].as_str().expect("select frame").contains("SELECT INBOX"),
        "{script:?}"
    );
    assert!(
        script[2]["send"]["body"].as_str().expect("idle frame").contains("IDLE"),
        "{script:?}"
    );
    // The heartbeat re-issues the IDLE inside RFC 2177's 29-minute window.
    assert!(
        config["heartbeat"]["body"].as_str().expect("heartbeat frame").contains("IDLE"),
        "{config}"
    );
    assert_eq!(config["heartbeat_secs"], json!(25 * 60));
    Ok(())
}
