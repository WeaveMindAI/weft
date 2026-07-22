use thiserror::Error;

#[derive(Debug, Error)]
pub enum WeftError {
    #[error("config error: {0}")]
    Config(String),

    #[error("input error: {0}")]
    Input(String),

    #[error("type error: {0}")]
    Type(String),

    #[error("node execution failed: {0}")]
    NodeExecution(String),

    /// The node called a suspension primitive; the worker should
    /// record the suspension (already done via the ContextHandle)
    /// and exit. On wake, a new worker picks up from the resume
    /// payload.
    #[error("suspended: token {token}")]
    Suspended { token: String },

    #[error("suspension setup failed: {0}")]
    Suspension(String),

    #[error("cancelled")]
    Cancelled,

    #[error("runtime error: {0}")]
    Runtime(#[from] anyhow::Error),
}

pub type WeftResult<T> = Result<T, WeftError>;

/// The one door for wrapping an OUTSIDE error (an HTTP library, a JSON
/// parser, any non-weft `Result`) into a node failure with context. A
/// node body never names a `WeftError` variant: the ctx accessors stamp
/// input/config errors themselves, every ctx handle already returns
/// `WeftResult`, and everything else goes through `.node_err("doing X")`.
pub trait NodeErrExt<T> {
    /// Map the error to a node failure reading "`context`: `error`".
    fn node_err(self, context: impl std::fmt::Display) -> WeftResult<T>;
}

impl<T, E: std::fmt::Display> NodeErrExt<T> for Result<T, E> {
    fn node_err(self, context: impl std::fmt::Display) -> WeftResult<T> {
        self.map_err(|e| WeftError::NodeExecution(format!("{context}: {e}")))
    }
}

impl<T> NodeErrExt<T> for Option<T> {
    /// `None` becomes a node failure reading exactly `context`, so the
    /// message should state what was missing and why it matters.
    fn node_err(self, context: impl std::fmt::Display) -> WeftResult<T> {
        self.ok_or_else(|| WeftError::NodeExecution(context.to_string()))
    }
}

#[cfg(test)]
mod node_err_tests {
    use super::*;

    #[test]
    fn node_err_wraps_with_context_and_passes_ok_through() {
        let ok: Result<u32, std::io::Error> = Ok(7);
        assert_eq!(ok.node_err("reading").unwrap(), 7);

        let err: Result<u32, &str> = Err("boom");
        let e = err.node_err("sending to bridge").unwrap_err();
        assert!(matches!(&e, WeftError::NodeExecution(m) if m == "sending to bridge: boom"), "{e}");
    }

    #[test]
    fn node_err_turns_none_into_a_failure_with_the_message_verbatim() {
        assert_eq!(Some(7).node_err("unused").unwrap(), 7);

        let e = None::<u32>.node_err("no messageId on a media message").unwrap_err();
        assert!(matches!(&e, WeftError::NodeExecution(m) if m == "no messageId on a media message"), "{e}");
    }
}

#[cfg(feature = "runtime")]
impl From<crate::caller::CallerError> for WeftError {
    /// A caller-connection failure surfaces as a node failure (fails
    /// loud, visible in the UI), EXCEPT a disconnect under the
    /// `cancel-on-disconnect` policy, which IS a cancellation of this
    /// execution and maps to `Cancelled` so the engine's existing
    /// cancel short-circuit handles it uniformly. The connection layer
    /// fires the per-execution cancel flag independently; this mapping
    /// only governs how the awaiting node's `?` propagates.
    fn from(e: crate::caller::CallerError) -> Self {
        use crate::caller::CallerError;
        match e {
            // A disconnect always means the run is being cancelled (the
            // keep-running policy is a silent `Ok(())`, never this error), so
            // it maps cleanly to `Cancelled` with no action to disambiguate.
            CallerError::Disconnected => WeftError::Cancelled,
            other => WeftError::NodeExecution(other.to_string()),
        }
    }
}
