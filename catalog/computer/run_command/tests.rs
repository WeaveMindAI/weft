//! RunCommand self-tests: a quick command answered in the call, a long
//! one parked on its status route and not run again when the body runs
//! again after the wait, the
//! whole output read off the computer piece by piece into a stored file,
//! the session passed along, and the ways a command fails to come back.

use base64::Engine;
use serde_json::json;

use weft::signal::Predicate;
use weft::storage::StoredFile;
use weft::{FakeRig, NodeTest, WeftResult};

use super::super::command::{finished, piece, Finished};
use super::super::testing::{answers, asked, computer};
use super::{RunCommandNode, RESTARTED};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("a_finished_command_reads_as_its_exit_code_and_size", finished_reads),
        NodeTest::basic("a_lost_command_fails_with_the_computers_words", lost_fails),
        NodeTest::basic("a_piece_of_output_reads_or_fails_loud", piece_reads),
        NodeTest::fake("a_quick_command_hands_back_its_whole_output_as_a_file", quick),
        NodeTest::fake("a_long_output_is_read_in_pieces", in_pieces),
        NodeTest::fake("pieces_out_of_order_fail_loud", out_of_order),
        NodeTest::fake("a_long_command_parks_and_is_not_run_again_on_resume", long),
        NodeTest::fake("a_restart_alone_says_so_in_the_output", restart_alone),
        NodeTest::fake("a_command_lost_while_parked_reaches_error", lost_while_parked),
        NodeTest::fake("a_busy_session_reaches_error", busy),
    ]
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn finished_reads() -> WeftResult<()> {
    assert_eq!(
        finished(&json!({ "status": "done", "id": "j1", "exitCode": 2, "size": 40 }))?,
        Finished { id: "j1".into(), exit_code: 2, size: 40 }
    );
    let error = finished(&json!({ "status": "done", "exitCode": 2, "size": 40 })).expect_err("no id").to_string();
    assert!(error.contains("finished command with no id"), "{error}");
    let error = finished(&json!({ "status": "done", "id": "j1", "exitCode": 0 })).expect_err("no size").to_string();
    assert!(error.contains("no output size"), "{error}");
    let error = finished(&json!({ "status": "running", "id": "j1" })).expect_err("not finished").to_string();
    assert!(error.contains("not a finished command"), "{error}");
    Ok(())
}

fn lost_fails() -> WeftResult<()> {
    let error = finished(&json!({ "status": "lost", "error": "no command j1 on this computer" })).expect_err("lost").to_string();
    assert!(error.contains("no command j1 on this computer"), "{error}");
    let error = finished(&json!({ "status": "lost" })).expect_err("lost, saying nothing").to_string();
    assert!(error.contains("said nothing more"), "{error}");
    Ok(())
}

fn piece_reads() -> WeftResult<()> {
    let (bytes, next, end) = piece(&json!({ "data": b64(b"hi\n"), "next": 3, "end": true }))?;
    assert_eq!((bytes.as_slice(), next, end), (&b"hi\n"[..], 3, true));
    let error = piece(&json!({ "data": "%%%", "next": 3, "end": true })).expect_err("not base64").to_string();
    assert!(error.contains("not base64"), "{error}");
    let error = piece(&json!({ "data": b64(b"x"), "next": 1 })).expect_err("no end").to_string();
    assert!(error.contains("whether it was the last"), "{error}");
    Ok(())
}

fn output_of(rig: &FakeRig, file: &serde_json::Value) -> WeftResult<Vec<u8>> {
    Ok(rig.stored_bytes(&StoredFile::from_value(file)?.key)?.to_vec())
}

async fn quick(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({ "status": "done", "id": "j1", "exitCode": 0, "size": 3 }));
    answers(&rig, json!({ "data": b64(b"hi\n"), "next": 3, "end": true }));
    let outcome = rig
        .run(&RunCommandNode, json!({ "computer": computer, "args": { "command": "echo hi" }, "session": "run-1" }))
        .await
        .ok()?;
    assert_eq!(output_of(&rig, &outcome.outputs["output"])?, b"hi\n");
    assert_eq!(StoredFile::from_value(&outcome.outputs["output"])?.mime_type, "text/plain");
    assert_eq!(outcome.outputs["exitCode"], json!(0));
    assert_eq!(
        asked(&rig),
        vec![
            ("exec".to_string(), json!({ "command": "echo hi", "session": "run-1" })),
            ("output".to_string(), json!({ "id": "j1", "from": 0 })),
        ]
    );
    assert!(rig.awaited_signals().is_empty(), "a command done in the call parks on nothing");
    Ok(())
}

async fn in_pieces(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({ "status": "done", "id": "j1", "exitCode": 1, "size": 9 }));
    answers(&rig, json!({ "data": b64(b"abcd"), "next": 4, "end": false }));
    answers(&rig, json!({ "data": b64(b"efghi"), "next": 9, "end": true }));
    let outcome = rig.run(&RunCommandNode, json!({ "computer": computer, "args": { "command": "make" } })).await.ok()?;
    assert_eq!(output_of(&rig, &outcome.outputs["output"])?, b"abcdefghi", "every piece, in order, nothing cut");
    assert_eq!(outcome.outputs["exitCode"], json!(1), "a failing command still succeeds here");
    let reads: Vec<_> = asked(&rig).into_iter().filter(|(action, _)| action == "output").map(|(_, p)| p["from"].clone()).collect();
    assert_eq!(reads, vec![json!(0), json!(4)]);
    assert_eq!(asked(&rig)[0].1, json!({ "command": "make" }), "no session named, none sent: the computer's default");
    Ok(())
}

async fn out_of_order(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({ "status": "done", "id": "j1", "exitCode": 0, "size": 9 }));
    answers(&rig, json!({ "data": b64(b"abcd"), "next": 7, "end": false }));
    let outcome = rig.run(&RunCommandNode, json!({ "computer": computer, "args": { "command": "make" } })).await;
    let error = outcome.result.expect_err("a gap in the output").to_string();
    assert!(error.contains("out of order"), "{error}");
    Ok(())
}

async fn long(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({ "status": "running", "id": "j7" }));
    answers(&rig, json!({ "data": b64(b"built\n"), "next": 6, "end": true }));
    let input = json!({ "computer": computer, "args": { "command": "make", "timeoutSecs": 600 } });
    // The first run pauses in its wait (no status yet): the state holds
    // the started command.
    rig.signal_paused();
    let paused = rig.run(&RunCommandNode, input.clone()).await;
    assert!(paused.paused(), "the first run pauses in its wait: {:?}", paused.result);
    assert_eq!(rig.saved_state(&RunCommandNode), Some(json!({ "started": { "status": "running", "id": "j7" }, "ended": null })));
    // The body runs again from its top: the start is in its state, so
    // the command is not sent to the computer again, and the run comes
    // back to the wait on the same command.
    rig.signal(json!({ "status": "done", "id": "j7", "exitCode": 0, "size": 6 }));
    let outcome = rig.run(&RunCommandNode, input).await.ok()?;
    assert_eq!(output_of(&rig, &outcome.outputs["output"])?, b"built\n");
    assert_eq!(outcome.outputs["exitCode"], json!(0));
    let starts = asked(&rig).into_iter().filter(|(action, _)| action == "exec").count();
    assert_eq!(starts, 1, "the command ran once: {:?}", asked(&rig));
    let awaited = rig.awaited_signals();
    assert_eq!(awaited.len(), 2, "the paused wait, then the same wait answered");
    assert_eq!(awaited[0].config, awaited[1].config, "both on the one command's status");
    assert_eq!(awaited[1].kind, "poll_endpoint");
    assert_eq!(awaited[1].config["url"], json!("http://computer.example:8000/jobs/j7"));
    assert_eq!(awaited[1].config["interval_secs"], json!(5));
    assert_eq!(awaited[1].match_predicates, vec![Predicate::neq("status", "running")]);
    Ok(())
}

async fn restart_alone(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({ "status": "done", "restarted": true }));
    let outcome = rig
        .run(&RunCommandNode, json!({ "computer": computer, "args": { "restart": true }, "session": "run-1" }))
        .await
        .ok()?;
    assert_eq!(output_of(&rig, &outcome.outputs["output"])?, RESTARTED.as_bytes());
    assert_eq!(asked(&rig), vec![("exec".to_string(), json!({ "restart": true, "session": "run-1" }))]);
    Ok(())
}

async fn lost_while_parked(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({ "status": "running", "id": "j7" }));
    rig.signal(json!({ "status": "lost", "error": "no command j7 on this computer: it restarted since the command began" }));
    rig.wire_output("_error");
    let outcome = rig.run(&RunCommandNode, json!({ "computer": computer, "args": { "command": "make" } })).await.ok()?;
    let error = outcome.output("_error")?.as_str().expect("the error is text").to_string();
    assert!(error.contains("it restarted since the command began"), "{error}");
    Ok(())
}

async fn busy(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({ "error": "a command is still running in session 'run-1' (40 seconds so far): 'sleep 99'" }));
    rig.wire_output("_error");
    let outcome = rig
        .run(&RunCommandNode, json!({ "computer": computer, "args": { "command": "ls" }, "session": "run-1" }))
        .await
        .ok()?;
    let error = outcome.output("_error")?.as_str().expect("the error is text").to_string();
    assert!(error.contains("exec: a command is still running in session 'run-1'"), "{error}");
    Ok(())
}
