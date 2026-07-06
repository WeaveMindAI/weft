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
