//! A run's events as its record stores them: one row of `run_log` per
//! write of the run (its `seq`), holding that write's events in order as a
//! JSON array, compressed with zstd by whoever writes it. The events leave
//! out their run (`RunEvent`'s `run_id`, which the row carries);
//! decoding sets it back.
//!
//! Postgres does not compress a value under about 2 KB, and a ping's events
//! are about that, so the writer compresses them: the database stores and
//! the line carries a third of the bytes, and only the readers (which are
//! few and rarely hot) pay for decompressing.

use weft_core::RunId;

use crate::events::RunEvent;

/// zstd's fastest level: a row is written on every run and read rarely.
pub const LEVEL: i32 = 1;

/// `events`, all of one run, as the row a write of them is stored as.
pub fn encode(events: &[RunEvent]) -> Vec<u8> {
    let json = serde_json::to_vec(events).expect("an event serializes");
    zstd::bulk::compress(&json, LEVEL).expect("zstd compresses a buffer it holds")
}

/// A row of a run's record as `run_log.events` holds it, once
/// decompressed: its events, in order, each without its run.
pub const STORED_EVENTS: weft_core::stored::Kind<Vec<RunEvent>> = weft_core::stored::Kind::new("run record row", STEPS);

/// How a row's events changed shape, version N to N+1.
const STEPS: &[&[weft_core::stored::Op]] = &[
    // 1 -> 2: a node keeps a saved state instead of replaying its body.
    // A `ctx.run` step's answer (`run_output`) is no longer recorded, so
    // its rows go; a wait is numbered among the firing's waits alone
    // (`wait_index`, which was `call_index`). A run parked before this
    // cannot resume (its nodes saved no state, and the engine refuses
    // it, naming the run); what it recorded still reads.
    &[
        weft_core::stored::Op::Code { at: "", run: drop_run_outputs },
        weft_core::stored::Op::RenameKey { at: "*", from: "call_index", to: "wait_index" },
    ],
    // 2 -> 3: a live caller's gate names its check
    // (`weft_core::primitive::SIGNAL_SPEC_V2`), in each spec a row keeps:
    // a captured trigger's, a wait's, and a fired run's stand-in.
    &[
        weft_core::stored::Op::Within { at: "*.spec", ops: weft_core::primitive::SIGNAL_SPEC_V2 },
        weft_core::stored::Op::Within { at: "*.stand_in", ops: weft_core::primitive::SIGNAL_SPEC_V2 },
    ],
    // 3 -> 4: a name leaving a bus says how its body returned (`end`),
    // which is how a bus nobody is on ends once no node can still join
    // it. Before, the last one leaving ended the bus in the same breath,
    // its `bus_closed` row right after, so every older leave reads as
    // finished.
    &[weft_core::stored::Op::Code { at: "", run: bus_left_finished }],
];

/// Every `bus_left` event of a row says it left finished (version 3 to 4).
fn bus_left_finished(row: &mut serde_json::Value) -> Result<(), String> {
    let serde_json::Value::Array(events) = row else {
        return Err(format!("a record row should be a list of events, and is {row}"));
    };
    for event in events.iter_mut().filter(|event| event.get("kind").and_then(serde_json::Value::as_str) == Some("bus_left")) {
        let serde_json::Value::Object(fields) = event else { continue };
        if fields.contains_key("end") {
            return Err(format!("a bus_left event of a version 3 row already says how it ended: {event}"));
        }
        fields.insert("end".into(), serde_json::json!({ "kind": "finished" }));
    }
    Ok(())
}

/// Drop every `run_output` event of a row (version 1 to 2).
fn drop_run_outputs(row: &mut serde_json::Value) -> Result<(), String> {
    let serde_json::Value::Array(events) = row else {
        return Err(format!("a record row should be a list of events, and is {row}"));
    };
    events.retain(|event| event.get("kind").and_then(serde_json::Value::as_str) != Some("run_output"));
    Ok(())
}

/// The events of one stored row of `run_id`, written at `version`
/// (`run_log.events_version`), in order, its id set back on each. A row
/// that does not decode fails with the one wording every reader shares,
/// which names the run and how to remove it.
pub fn decode(run_id: RunId, row: &[u8], version: weft_core::stored::Version) -> Result<Vec<RunEvent>, String> {
    let json = zstd::stream::decode_all(row).map_err(|e| undecodable(run_id, &e))?;
    let mut events = STORED_EVENTS.decode(&json[..], version).map_err(|e| undecodable(run_id, &e))?;
    for event in &mut events {
        *event.run_id_mut() = run_id;
    }
    Ok(events)
}

/// The wording for a row that does not decode, whoever reads it (the
/// dispatcher's strict and lossy reads, the engine's resume fold): the
/// run, the reason, and the recovery.
pub fn undecodable(run_id: RunId, why: &dyn std::fmt::Display) -> String {
    format!(
        "a record row of run {run_id} does not decode ({why}): it is damaged, and the run cannot be \
         read. `weft clean {run_id}` removes it."
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run() -> RunId {
        RunId::from_u128(7)
    }

    fn events(run_id: RunId) -> Vec<RunEvent> {
        vec![
            RunEvent::NodeStarted { run_id, node_id: "a".into(), frames: Vec::new(), at_unix: 1 },
            RunEvent::LogLine {
                run_id,
                node_id: "a".into(),
                frames: Vec::new(),
                level: "info".into(),
                message: "hello".into(),
                at_unix_ms: Some(1_000),
                seq: Some(0),
                at_unix: 1,
            },
            RunEvent::RunCompleted { run_id, at_unix: 2 },
        ]
    }

    /// A row decodes to the events written, each with its run set back,
    /// and the run is nowhere in what is stored.
    #[test]
    fn a_row_decodes_to_its_events_with_their_run() {
        let written = events(run());
        let row = encode(&written);
        let json = String::from_utf8(zstd::stream::decode_all(&row[..]).unwrap()).unwrap();
        assert!(!json.contains("run_id"), "{json}");
        let read = decode(run(), &row, STORED_EVENTS.version()).unwrap();
        assert_eq!(serde_json::to_value(&read).unwrap(), serde_json::to_value(&written).unwrap());
        assert!(read.iter().all(|event| event.run_id() == run()));
    }

    /// A row that does not decode names its run and the way out.
    #[test]
    fn a_damaged_row_names_its_run_and_the_way_out() {
        let why = decode(run(), b"not zstd", STORED_EVENTS.version()).unwrap_err();
        assert!(why.contains(&run().to_string()) && why.contains("weft clean"), "{why}");
    }
}
