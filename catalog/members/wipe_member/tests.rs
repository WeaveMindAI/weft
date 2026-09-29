//! WipeMember self-tests: every piece of the member goes, in the order
//! that leaves nothing half-attached, and this run is stopped only at
//! the end and only when asked.

use serde_json::json;

use weft::member::MemberId;
use weft::program::ProgramCall;
use weft::storage::StorageScope;
use weft::{FakeRig, NodeTest, StopSelf, WeftResult};

use super::WipeMemberNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("everything_of_the_member_goes_in_order", order)]
}

async fn order(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.connections.forget", json!({ "forgotten": 2 }));
    rig.answer_program_call("weft.tokens.revoke", json!({ "revoked": 1 }));
    rig.answer_program_call("weft.runs.clean", json!({ "deleted": 3, "cancelled": 0, "left_running": 0 }));
    let ada = StorageScope::member_of(MemberId::new("ada").expect("a valid id"));
    let grace = StorageScope::member_of(MemberId::new("grace").expect("a valid id"));
    let theirs = rig.store_file_in(&ada, "notes.txt", "text/plain", "ada's notes");
    let other = rig.store_file_in(&grace, "notes.txt", "text/plain", "grace's notes");
    rig.run(&WipeMemberNode, json!({ "member": "ada", "infra": ["bridge"], "includeSelf": true })).await.ok()?;
    let key = |file: &serde_json::Value| weft::storage::StoredFile::from_value(file).map(|f| f.key);
    assert!(rig.stored_meta(&key(&theirs)?).is_err(), "the member's files go too");
    assert!(rig.stored_meta(&key(&other)?).is_ok(), "another member's files stay");
    let calls = rig.program_calls();
    let names: Vec<&str> = calls.iter().map(|(c, _)| c.journal_name()).collect();
    assert_eq!(
        names,
        vec![
            "weft.triggers.deactivate",
            "weft.infra.terminate",
            "weft.values.forget",
            "weft.connections.forget",
            "weft.tokens.revoke",
            "weft.runs.clean",
        ]
    );
    let selves: Vec<StopSelf> = calls.iter().map(|(_, s)| *s).collect();
    assert_eq!(selves, vec![StopSelf::Keep, StopSelf::Keep, StopSelf::Keep, StopSelf::Keep, StopSelf::Keep, StopSelf::Include]);
    match &calls[1].0 {
        ProgramCall::InfraTerminate { node, disks, .. } => {
            assert_eq!(node, "bridge");
            assert_eq!(*disks, weft::infra::TerminateDisks::DeleteAll, "the kept disks go too");
        }
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}
