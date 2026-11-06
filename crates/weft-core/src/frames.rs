//! Frame stack for pulse keying. Each pulse carries a stack of frames
//! that says where in the program's dynamic structure the firing sits:
//! which iteration of which (nested) loop, and which call of which
//! included file. Empty stack = a firing at the root. Used by
//! `ready::find_ready_nodes` to match pulses across required ports at
//! the same frame stack.
//!
//! A loop body and an included file are each compiled ONCE. What tells
//! two runs through the same body apart is this stack: a loop pushes an
//! iteration frame per launch, a call site pushes a call frame per use,
//! and the body's nodes fire under whatever stack reaches them. Nesting
//! is the stack itself: `[Loop 2, Call auth, Loop 0]` is the first
//! iteration of a loop inside the include `auth` uses, inside the
//! third iteration of an outer loop.

use serde::{Deserialize, Serialize};

/// One level of the dynamic structure a firing is inside: an iteration
/// of a loop, or a call of an included file through the site named.
/// The site is the call site's own id in the program (`auth`, or
/// `Auth.billing.inner` for a site inside a body), so the stack alone
/// says which use of the body this is.
// SYNC: Frame <-> packages/weft-graph/src/protocol.ts Frame
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Frame {
    Loop { index: u32 },
    Call { site: String },
}

impl Frame {
    /// The iteration index of a loop frame; a call frame has none.
    pub fn loop_index(&self) -> Option<u32> {
        match self {
            Frame::Loop { index } => Some(*index),
            Frame::Call { .. } => None,
        }
    }

    /// The site of a call frame; a loop frame has none.
    pub fn call_site(&self) -> Option<&str> {
        match self {
            Frame::Call { site } => Some(site),
            Frame::Loop { .. } => None,
        }
    }

    /// One frame as text: `3` for an iteration, `@auth` for a call.
    pub fn text(&self) -> String {
        match self {
            Frame::Loop { index } => index.to_string(),
            Frame::Call { site } => format!("@{site}"),
        }
    }
}

/// The frame stack a firing is inside. Empty = the root. `[3]` = the
/// fourth iteration of one loop; `[3, 0]` = the first iteration of an
/// inner loop inside it; `[@auth]` = the body of the file the site
/// `auth` includes, called from the root.
pub type LoopFrames = Vec<Frame>;

/// The stack as one stable text key, frames joined by `.`: `3.@auth.0`,
/// empty at the root. What derived emission ids, log suffixes and the
/// CLI's `#` display are built from.
pub fn frames_text(frames: &LoopFrames) -> String {
    frames.iter().map(Frame::text).collect::<Vec<_>>().join(".")
}

/// The iteration indices on the stack, outermost first, call frames
/// left out: what a loop-only reader (the loop rig's assertions) sees.
pub fn loop_indices(frames: &LoopFrames) -> Vec<u32> {
    frames.iter().filter_map(Frame::loop_index).collect()
}

/// The call sites on the stack, outermost first: which use of which
/// included file the firing belongs to, loop frames left out.
pub fn call_path(frames: &LoopFrames) -> Vec<&str> {
    frames.iter().filter_map(Frame::call_site).collect()
}

/// One node of a run, at one place: its id in the compiled definition
/// and the call path it runs under (the sites from the top of the
/// program down, outermost first; empty outside every included file).
/// A body is compiled once and reached through every site that
/// includes it, so `Clean.strip` under `one` and under `two` are two
/// places in a run, with their own gates, wires, handed values and
/// results: what a cut, a kick plan and a seed are keyed by. The loop
/// iterations a place runs in are not part of it (see
/// `FiringLocation` for one run), so the frames a place fires at are the
/// program's to say (`crate::project::ProgramIndex::frames_at`).
///
/// Written as one string, `one/Clean.strip` (the path, then the id,
/// `/`-joined; a top-level place is its id), so it keys a JSON object.
// SYNC: Located <-> packages/weft-graph/src/protocol.ts locatedKey
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
pub struct Located {
    pub id: String,
    pub path: Vec<String>,
}

impl Located {
    pub fn new(id: impl Into<String>, path: Vec<String>) -> Self {
        Self { id: id.into(), path }
    }

    /// A place outside every included file.
    pub fn top(id: impl Into<String>) -> Self {
        Self { id: id.into(), path: Vec::new() }
    }

    /// The place a firing at `frames` belongs to: the node under the
    /// call sites on the stack, iterations left out.
    pub fn at(id: impl Into<String>, frames: &LoopFrames) -> Self {
        Self { id: id.into(), path: call_path(frames).into_iter().map(str::to_string).collect() }
    }

    /// The same place one site deeper: inside the body `site` calls.
    pub fn into_call(mut self, site: &str) -> Self {
        self.path.push(site.to_string());
        self
    }

    /// The same id one site shallower, in the caller of the innermost
    /// call; `None` at the top.
    pub fn out_of_call(&self) -> Option<Self> {
        let (_, above) = self.path.split_last()?;
        Some(Self { id: self.id.clone(), path: above.to_vec() })
    }

    /// The innermost site on the path, if any.
    pub fn site(&self) -> Option<&str> {
        self.path.last().map(String::as_str)
    }
}

impl From<Located> for String {
    fn from(located: Located) -> String {
        located.to_string()
    }
}

impl std::fmt::Display for Located {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for site in &self.path {
            write!(f, "{site}/")?;
        }
        write!(f, "{}", self.id)
    }
}

impl TryFrom<String> for Located {
    type Error = String;

    fn try_from(text: String) -> Result<Self, String> {
        let mut segments: Vec<&str> = text.split('/').collect();
        let id = segments.pop().filter(|id| !id.is_empty()).ok_or_else(|| format!("'{text}' names no node"))?;
        if segments.iter().any(|site| site.is_empty()) {
            return Err(format!("'{text}' has an empty call site"));
        }
        Ok(Self { id: id.to_string(), path: segments.into_iter().map(str::to_string).collect() })
    }
}

/// One FIRING: a node id plus the loop frames it fires under
/// (a node inside a loop runs as distinct runs, one per frame).
/// The key for per-run state: the engine's stuck-check lanes
/// (see `liveness`) and the persisted `awaited_sequences` on
/// `NodeRunState`. Lives here, not in `liveness`, because it is a
/// pure identity the parse-only build needs too.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FiringLocation {
    pub node_id: String,
    pub frames: LoopFrames,
}

impl FiringLocation {
    pub fn new(node_id: impl Into<String>, frames: LoopFrames) -> Self {
        Self { node_id: node_id.into(), frames }
    }

    /// This firing's place in the run `run`, as one opaque text: the same
    /// every time its body runs again (after a pause or a crash), and
    /// different for every other firing, every loop iteration and every
    /// other run, because a firing is exactly a node under its frames in
    /// one run. What a node hands a service that must refuse a duplicate
    /// (`crate::context::RunContext::firing_key`).
    ///
    /// A SHA-256 in hex (64 characters) of the run id, the node id and
    /// the frames, so it has one length whatever the nesting, fits the
    /// idempotency keys services take, and shows no name of the program.
    pub fn key(&self, run: crate::RunId) -> String {
        use sha2::{Digest, Sha256};
        let mut hash = Sha256::new();
        hash.update(b"weft-firing-key-v1\n");
        hash.update(run.as_bytes());
        // JSON keeps the parts apart: no node id and frames can spell
        // another pair's text.
        hash.update(serde_json::to_vec(&(&self.node_id, &self.frames)).expect("an id and frames always write as JSON"));
        crate::project::hash::hex(&hash.finalize())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A firing's key is the same for the same run, node and frames, and
    /// differs when any of them does: another loop iteration, another
    /// call site, another node, another run.
    #[test]
    fn a_firing_key_names_one_firing_of_one_run() {
        let run = crate::RunId::new_v4();
        let at = |id: &str, frames: LoopFrames| FiringLocation::new(id, frames);
        let first = at("ask", vec![Frame::Loop { index: 0 }]);
        assert_eq!(first.key(run), at("ask", vec![Frame::Loop { index: 0 }]).key(run), "run again, the same key");
        assert_eq!(first.key(run).len(), 64);
        let others = [
            at("ask", vec![Frame::Loop { index: 1 }]),
            at("ask", vec![Frame::Loop { index: 0 }, Frame::Loop { index: 0 }]),
            at("ask", vec![Frame::Call { site: "one".into() }, Frame::Loop { index: 0 }]),
            at("ask", vec![]),
            at("ask2", vec![Frame::Loop { index: 0 }]),
        ];
        for other in &others {
            assert_ne!(first.key(run), other.key(run), "{other:?}");
        }
        assert_ne!(first.key(run), first.key(crate::RunId::new_v4()), "another run");
    }
}
