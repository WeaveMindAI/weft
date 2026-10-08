//! A run's events as its record stores them: one row of `run_log` per
//! write of the run (its `seq`), holding that write's events in order as a
//! JSON array, compressed with zstd by whoever writes it. The events leave
//! out their run (`ExecEvent`'s `execution_id`, which the row carries) and
//! the run's selection (written as its digest, kept once in
//! `run_selection`); decoding sets both back.
//!
//! Postgres does not compress a value under about 2 KB, and a ping's events
//! are about that, so the writer compresses them: the database stores and
//! the line carries a third of the bytes, and only the readers (which are
//! few and rarely hot) pay for decompressing.

use weft_core::project::selection::RecordedSelection;
use weft_core::ExecutionId;

use crate::events::ExecEvent;

/// zstd's fastest level: a row is written on every run and read rarely.
const LEVEL: i32 = 1;

/// `events`, all of one run, as the row a write of them is stored as.
pub fn encode(events: &[ExecEvent]) -> Vec<u8> {
    let json = serde_json::to_vec(events).expect("an event serializes");
    zstd::bulk::compress(&json, LEVEL).expect("zstd compresses a buffer it holds")
}

/// The events of one stored row of `execution_id`, in order, its id set
/// back on each. A birth in it names its selection by digest, resolved
/// against `selection` (the run's, read with it). A row that does not
/// decode fails with the one wording every reader shares, which names the
/// run and how to remove it.
pub fn decode(execution_id: ExecutionId, selection: Option<&RecordedSelection>, row: &[u8]) -> Result<Vec<ExecEvent>, String> {
    let json = zstd::stream::decode_all(row).map_err(|e| undecodable(execution_id, &e))?;
    let mut events: Vec<ExecEvent> = RecordedSelection::resolving(selection.map(std::slice::from_ref).unwrap_or_default(), || {
        serde_json::from_slice(&json)
    })
    .map_err(|e| undecodable(execution_id, &e))?;
    for event in &mut events {
        *event.execution_id_mut() = execution_id;
    }
    Ok(events)
}

/// The wording for a row that does not decode, whoever reads it (the
/// dispatcher's strict and lossy reads, the engine's resume fold): the
/// run, the reason, and the recovery.
pub fn undecodable(execution_id: ExecutionId, why: &dyn std::fmt::Display) -> String {
    format!(
        "a record row of run {execution_id} does not decode ({why}): it was written by an earlier weft or \
         is damaged, and the run cannot be read. `weft clean {execution_id}` removes it."
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run() -> ExecutionId {
        ExecutionId::from_u128(7)
    }

    fn events(execution_id: ExecutionId) -> Vec<ExecEvent> {
        vec![
            ExecEvent::NodeStarted { execution_id, node_id: "a".into(), frames: Vec::new(), at_unix: 1 },
            ExecEvent::LogLine {
                execution_id,
                node_id: "a".into(),
                frames: Vec::new(),
                level: "info".into(),
                message: "hello".into(),
                at_unix_ms: Some(1_000),
                seq: Some(0),
                at_unix: 1,
            },
            ExecEvent::ExecutionCompleted { execution_id, at_unix: 2 },
        ]
    }

    /// A row decodes to the events written, each with its run set back,
    /// and the run is nowhere in what is stored.
    #[test]
    fn a_row_decodes_to_its_events_with_their_run() {
        let written = events(run());
        let row = encode(&written);
        let json = String::from_utf8(zstd::stream::decode_all(&row[..]).unwrap()).unwrap();
        assert!(!json.contains("execution_id"), "{json}");
        let read = decode(run(), None, &row).unwrap();
        assert_eq!(serde_json::to_value(&read).unwrap(), serde_json::to_value(&written).unwrap());
        assert!(read.iter().all(|event| event.execution_id() == run()));
    }

    /// A row that does not decode names its run and the way out.
    #[test]
    fn a_damaged_row_names_its_run_and_the_way_out() {
        let why = decode(run(), None, b"not zstd").unwrap_err();
        assert!(why.contains(&run().to_string()) && why.contains("weft clean"), "{why}");
    }
}
