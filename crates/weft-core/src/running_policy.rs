//! What to do with in-flight Fire-phase executions of a targeted
//! project when an operation needs the running set out of the way:
//! a lifecycle verb (stop / terminate / deactivate) on the
//! broker/dispatcher side, or the CLI's stale-binary build gate.
//! One definition for every crate; the serde derive is the single
//! source of truth for the `{cancel, wait}` wire spelling (TEXT
//! column, JSON string field, and CLI flag value all agree).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunningPolicy {
    /// Cancel running execs immediately, then proceed.
    Cancel,
    /// Wait until running_count reaches 0, then proceed. New fires are
    /// gated per the project's lifecycle axes (set by the
    /// trigger-deactivate / park step).
    Wait,
}

impl RunningPolicy {
    /// Wire string. Must match the serde rename above; the round-trip
    /// test below pins them together.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cancel => "cancel",
            Self::Wait => "wait",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "cancel" => Some(Self::Cancel),
            "wait" => Some(Self::Wait),
            _ => None,
        }
    }

    pub const VARIANTS: &'static [Self] = &[Self::Cancel, Self::Wait];
}

impl Default for RunningPolicy {
    fn default() -> Self {
        Self::Wait
    }
}

impl std::fmt::Display for RunningPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_roundtrip() {
        for v in RunningPolicy::VARIANTS {
            assert_eq!(RunningPolicy::parse(v.as_str()), Some(*v));
            let json = serde_json::to_string(v).expect("serialize");
            assert_eq!(json, format!("\"{}\"", v.as_str()));
            let back: RunningPolicy = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, *v);
        }
    }
}
