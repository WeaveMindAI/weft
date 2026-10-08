//! A batch of records as one call carries it, from a worker's writer lane
//! to the broker: a small header saying, run by run, where its rows go and
//! what its row learns from them, then the rows themselves, compressed
//! (`crate::stored`), back to back. One call on the worker's line, one
//! statement in the database (`crate::record::record_batch`).
//!
//! ```text
//!   [u32 header length, big-endian][header JSON][row][row][row]...
//! ```

use serde::{Deserialize, Serialize};
use weft_core::ExecutionId;

use crate::record::{Born, Fate, StoredSelection, Written};

/// What a batch says besides its rows.
// SYNC: BatchHead <-> crate::record::record_batch (what it binds)
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BatchHead {
    /// Somebody waits on this batch being on disk (a durable run at a
    /// commit point): it commits synced. A batch of fast runs does not
    /// wait for the disk.
    pub durable: bool,
    /// The lane that sent it, which keeps its version counts on a row of
    /// its own (`version_runs.lane`).
    pub lane: u16,
    pub runs: Vec<RunHead>,
    /// The selections the batch's births name that the lane has not sent
    /// before; the record keeps each once.
    pub selections: Vec<StoredSelection>,
}

/// One run's share of a batch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunHead {
    pub execution_id: ExecutionId,
    /// The epoch the run's writer holds it under (1 for a run it bore,
    /// what its claim answered for one it claimed).
    pub epoch: i32,
    /// The `seq` of its first row here; its rows follow on, one `seq`
    /// each. 0 for a run that starts here.
    pub first_seq: i32,
    /// The size of each of its rows, in order.
    pub row_sizes: Vec<u32>,
    /// The row it is born with, for a run that starts here.
    pub born: Option<Born>,
    /// What its rows here say about it.
    pub written: Written,
    /// It stored files of its own (so far), which its ending's sweep
    /// reclaims.
    pub wrote_files: bool,
}

/// A batch read off the wire: its head, and its rows in order, borrowed
/// from the bytes they came in.
#[derive(Debug, Clone)]
pub struct Batch<'a> {
    pub head: BatchHead,
    pub rows: Vec<&'a [u8]>,
}

/// `head` and `rows` (every run's rows, run by run, in the head's order)
/// as one call's body.
pub fn encode<'r>(head: &BatchHead, rows: impl IntoIterator<Item = &'r [u8]>) -> Vec<u8> {
    let header = serde_json::to_vec(head).expect("a batch head serializes");
    let mut body = Vec::with_capacity(4 + header.len() + head.runs.iter().flat_map(|run| &run.row_sizes).map(|size| *size as usize).sum::<usize>());
    body.extend_from_slice(&u32::try_from(header.len()).expect("a batch head is under 4 GiB").to_be_bytes());
    body.extend_from_slice(&header);
    for row in rows {
        body.extend_from_slice(row);
    }
    body
}

/// A batch's body read back, its rows checked against the sizes its head
/// gives them.
pub fn decode(body: &[u8]) -> anyhow::Result<Batch<'_>> {
    anyhow::ensure!(body.len() >= 4, "a batch of records is shorter than its header's length");
    let header_len = u32::from_be_bytes(body[..4].try_into().expect("four bytes")) as usize;
    let rest = &body[4..];
    anyhow::ensure!(rest.len() >= header_len, "a batch of records is shorter than its header");
    let head: BatchHead = serde_json::from_slice(&rest[..header_len])?;
    let mut rows = Vec::new();
    let mut at = header_len;
    for size in head.runs.iter().flat_map(|run| &run.row_sizes) {
        let end = at + *size as usize;
        anyhow::ensure!(end <= rest.len(), "a batch of records is shorter than the rows its header names");
        rows.push(&rest[at..end]);
        at = end;
    }
    anyhow::ensure!(at == rest.len(), "a batch of records carries {} bytes past the rows its header names", rest.len() - at);
    Ok(Batch { head, rows })
}

/// What the broker answers a batch: each run's fate, in the batch's order.
// SYNC: BatchAnswer <-> crates/weft-broker/src/handlers.rs (journal_record), crates/weft-engine/src/journal_writer.rs (what a lane reads)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchAnswer {
    pub fates: Vec<Fate>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn head(runs: &[(u128, &[u32])]) -> BatchHead {
        BatchHead {
            durable: true,
            lane: 2,
            runs: runs
                .iter()
                .map(|(id, sizes)| RunHead {
                    execution_id: ExecutionId::from_u128(*id),
                    epoch: 1,
                    first_seq: 0,
                    row_sizes: sizes.to_vec(),
                    born: None,
                    written: Written::default(),
                    wrote_files: false,
                })
                .collect(),
            selections: Vec::new(),
        }
    }

    /// Any set of runs and rows comes back as it went: the head whole, and
    /// each row where its size puts it.
    #[test]
    fn a_batch_comes_back_as_it_went() {
        let head = head(&[(1, &[3, 0]), (2, &[]), (3, &[5])]);
        let rows: [&[u8]; 3] = [b"abc", b"", b"vwxyz"];
        let body = encode(&head, rows);
        let batch = decode(&body).unwrap();
        assert_eq!(batch.head, head);
        assert_eq!(batch.rows, rows);
    }

    /// A body cut short, or carrying bytes no row accounts for, is refused.
    #[test]
    fn a_body_that_does_not_match_its_head_is_refused() {
        let head = head(&[(1, &[3])]);
        let body = encode(&head, [&b"abc"[..]]);
        assert!(decode(&body[..body.len() - 1]).is_err());
        let mut longer = body.clone();
        longer.push(0);
        assert!(decode(&longer).is_err());
        assert!(decode(&body[..2]).is_err());
    }
}
