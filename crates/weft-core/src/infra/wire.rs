//! What the install's infra endpoints answer and the CLI reads: one
//! Rust type per answer, so the two ends cannot drift.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::member::MemberId;

/// `GET /projects/{id}/infra/doors`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DoorsResponse {
    /// Every door a copy has serving, at the address its host gave it.
    pub doors: Vec<Door>,
    /// Copies still being applied: their doors (if they declare any) get
    /// an address only once the apply lands, so they are named here
    /// rather than left out as if they had none.
    pub applying: Vec<CopyRef>,
}

/// One copy of an infra node: the shared one, or a member's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CopyRef {
    /// The node as the program spells it.
    pub node: String,
    /// Whose copy: absent for the shared one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member: Option<MemberId>,
}

/// One door serving right now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Door {
    #[serde(flatten)]
    pub copy: CopyRef,
    pub endpoint: String,
    /// Where the door answers, exactly as its host wrote it (`host:port`):
    /// the machine's loopback on a local install, the unit's private
    /// address on the install's network on a cloud one. Not a URL: a door
    /// carries whatever protocol the endpoint speaks, and most of them
    /// are not HTTP.
    pub address: String,
}

/// One line a container wrote, with the instant its runtime stamped on
/// it (nanosecond precision, what `docker logs --timestamps` prints) and
/// the pipe it came out of.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogLine {
    pub at: DateTime<Utc>,
    pub pipe: Pipe,
    pub text: String,
}

/// Which of a container's two outputs a line was written to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Pipe {
    Stdout,
    Stderr,
}

/// Which lines of a unit's containers a read wants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogsFrom {
    /// The last N lines of each container.
    Tail(usize),
    /// Every line after the mark of its container (keyed by
    /// `LogStream::source`); all of a container that has no mark yet
    /// (one that appeared since the follow started).
    After(BTreeMap<String, LogMark>),
}

/// Where a follower stopped reading one container. A timestamp alone is
/// not a cursor: one write of several lines stamps them all with the same
/// instant, so the mark also counts how many lines AT that instant were
/// already delivered. Counted per pipe: the runtime hands stdout and
/// stderr back apart, so where the lines of one instant fall between the
/// two is not an order that holds from one read to the next.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogMark {
    pub at: DateTime<Utc>,
    pub stdout_seen: usize,
    pub stderr_seen: usize,
}

impl LogMark {
    /// The mark of a read that delivered nothing yet, taken at `at` by the
    /// clock that stamps the lines (the container runtime's, never the
    /// caller's: a line stamped by a clock running behind the caller's
    /// would fall before a caller-made mark and be dropped).
    pub fn start(at: DateTime<Utc>) -> Self {
        LogMark { at, stdout_seen: 0, stderr_seen: 0 }
    }

    fn seen(&mut self, pipe: Pipe) -> &mut usize {
        match pipe {
            Pipe::Stdout => &mut self.stdout_seen,
            Pipe::Stderr => &mut self.stderr_seen,
        }
    }

    /// The lines of `lines` (in the order written, read from `self.at`
    /// inclusive) that were not delivered yet.
    pub fn unseen(&self, lines: Vec<LogLine>) -> Vec<LogLine> {
        let mut skip = *self;
        lines
            .into_iter()
            .filter(|l| {
                if l.at < self.at {
                    return false;
                }
                let skip = skip.seen(l.pipe);
                if l.at == self.at && *skip > 0 {
                    *skip -= 1;
                    return false;
                }
                true
            })
            .collect()
    }

    /// The mark after delivering `delivered` on top of `self` (the
    /// previous mark, or `LogMark::start` of this read). Every read yields
    /// a mark, even one that delivered nothing: a follower whose first
    /// read was empty (`--tail 0`, a quiet container) must next ask for
    /// what came after that read, never for the whole history.
    pub fn advance(self, delivered: &[LogLine]) -> LogMark {
        let Some(last) = delivered.last() else {
            return self;
        };
        let mut next = if self.at == last.at { self } else { LogMark::start(last.at) };
        for l in delivered.iter().filter(|l| l.at == last.at) {
            *next.seen(l.pipe) += 1;
        }
        next
    }
}

/// What one container of a unit wrote, and the mark a follower resumes
/// from (made by the host that owns the container's clock).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogStream {
    /// The container, as its unit names it.
    pub source: String,
    pub lines: Vec<LogLine>,
    pub mark: LogMark,
}

/// `GET /projects/{id}/infra/logs`: each container's lines, headed by
/// what wrote them, plus the cursor a follower hands back as `after` to
/// get exactly what came next.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InfraLogs {
    pub blocks: Vec<LogBlock>,
    pub cursor: LogCursor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogBlock {
    #[serde(flatten)]
    pub copy: CopyRef,
    pub unit: String,
    pub stream: LogStream,
}

/// Every container's mark, keyed by `LogCursor::key`. Opaque to the
/// CLI: it only hands it back.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogCursor(pub BTreeMap<String, LogMark>);

impl LogCursor {
    /// One container of one unit of one copy (`instance` is the copy's
    /// stable id, so two members' copies never share a key).
    pub fn key(instance: &str, unit: &str, source: &str) -> String {
        format!("{instance}/{unit}/{source}")
    }

    /// The marks of one unit, keyed by source, as a host reads them.
    pub fn for_unit(&self, instance: &str, unit: &str) -> BTreeMap<String, LogMark> {
        let prefix = format!("{instance}/{unit}/");
        self.0
            .iter()
            .filter_map(|(k, m)| k.strip_prefix(&prefix).map(|source| (source.to_string(), *m)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(nanos: u32) -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, nanos).unwrap()
    }

    fn line(nanos: u32, text: &str) -> LogLine {
        LogLine { at: at(nanos), pipe: Pipe::Stdout, text: text.into() }
    }

    fn err_line(nanos: u32, text: &str) -> LogLine {
        LogLine { at: at(nanos), pipe: Pipe::Stderr, text: text.into() }
    }

    fn texts(lines: &[LogLine]) -> Vec<&str> {
        lines.iter().map(|l| l.text.as_str()).collect()
    }

    #[test]
    fn lines_sharing_an_instant_are_neither_repeated_nor_dropped() {
        // First read: a, b, c where b and c came from one write.
        let first = vec![line(1, "a"), line(5, "b"), line(5, "c")];
        let mark = LogMark::start(at(0)).advance(&first);
        assert_eq!(mark, LogMark { at: at(5), stdout_seen: 2, stderr_seen: 0 });
        // Next read starts at the mark's instant inclusive, and a fourth
        // line of that same write arrived since.
        let again = vec![line(5, "b"), line(5, "c"), line(5, "d"), line(9, "e")];
        let fresh = mark.unseen(again);
        assert_eq!(texts(&fresh), ["d", "e"]);
        assert_eq!(mark.advance(&fresh), LogMark { at: at(9), stdout_seen: 1, stderr_seen: 0 });
    }

    #[test]
    fn a_read_ending_on_the_marks_instant_carries_its_count() {
        let mark = LogMark { at: at(5), stdout_seen: 2, stderr_seen: 0 };
        let fresh = mark.unseen(vec![line(5, "b"), line(5, "c"), line(5, "d")]);
        assert_eq!(mark.advance(&fresh), LogMark { at: at(5), stdout_seen: 3, stderr_seen: 0 });
        assert_eq!(mark.advance(&[]), mark);
    }

    /// The two pipes of one instant come back in whatever order the
    /// runtime merges them: a stderr line arriving at an instant whose
    /// stdout lines were delivered is new, however the merge interleaves.
    #[test]
    fn lines_of_one_instant_are_counted_per_pipe() {
        let first = vec![line(5, "out1"), err_line(5, "err1")];
        let mark = LogMark::start(at(0)).advance(&first);
        assert_eq!(mark, LogMark { at: at(5), stdout_seen: 1, stderr_seen: 1 });
        // The next read merges the pipes the other way round, and one
        // more stderr line of that instant arrived.
        let again = vec![err_line(5, "err1"), err_line(5, "err2"), line(5, "out1")];
        assert_eq!(texts(&mark.unseen(again)), ["err2"]);
    }

    #[test]
    fn an_empty_first_read_marks_its_own_start() {
        // `--tail 0`: the first read delivers nothing, and the next one
        // must start at that read, not at the container's first line.
        let mark = LogMark::start(at(5)).advance(&[]);
        assert_eq!(mark, LogMark::start(at(5)));
        let fresh = mark.unseen(vec![line(1, "old"), line(5, "same instant"), line(7, "new")]);
        assert_eq!(texts(&fresh), ["same instant", "new"]);
    }

    #[test]
    fn a_cursor_hands_each_unit_only_its_own_marks() {
        let mark = LogMark::start(at(1));
        let mut cursor = LogCursor::default();
        cursor.0.insert(LogCursor::key("i1", "main", "app"), mark);
        cursor.0.insert(LogCursor::key("i1", "side", "app"), mark);
        assert_eq!(cursor.for_unit("i1", "main").into_keys().collect::<Vec<_>>(), ["app"]);
        assert!(cursor.for_unit("i2", "main").is_empty());
    }
}
