use thiserror::Error;

#[derive(Debug, Error)]
pub enum WeftError {
    #[error("config error: {0}")]
    Config(String),

    #[error("input error: {0}")]
    Input(String),

    #[error("type error: {0}")]
    Type(String),

    #[error("node failed: {0}")]
    NodeFailed(String),

    /// Something the node's package declares it needs from the machine
    /// (a `[[needs]]` entry of its `deps.toml`, by its `name`) is missing
    /// where the worker runs: [`need_missing`]. The engine says it with
    /// the node's type and the way to meet it on that platform
    /// (`crate::context::failure_message`). Like any outcome of the
    /// firing, a wired `_error` catches it: the program was checked
    /// before it started, so this is the machine changing under a run.
    #[error("the machine this worker runs on lacks {0}, which this node needs")]
    NeedMissing(String),

    /// The firing paused, its state saved (`crate::context::Kept`): on
    /// the wait `token` registered, which its answer resumes, or, with
    /// no token, inside the run (a bus read, a stream pull, a yield)
    /// while the whole run paused, which it resumes with. Returned by
    /// the call the body paused in; the body hands it back with `?`.
    #[error("{}", match token { Some(token) => format!("suspended: token {token}"), None => "paused with its run".to_string() })]
    Suspended { token: Option<String> },

    #[error("suspension setup failed: {0}")]
    Suspension(String),

    /// A wait the run could not pause on, held in its worker, ran out of
    /// its hold (`holdSecs`) with nothing moving in the run, or was asked
    /// for where the hold is zero. Returned by the waiting `ctx` call, so
    /// the node may handle it like any outcome of its firing.
    #[error("{0}")]
    WaitGaveUp(String),

    /// A person skipped what the waiting `ctx` call waited on (a form's
    /// skip): the node does not run on, its firing ends skipped with every
    /// output closed, the way a node whose input closed does. Returned by the
    /// waiting call, so a body passing errors on with `?` needs no code
    /// of its own for it.
    #[error("{0}")]
    WaitSkipped(String),

    #[error("cancelled")]
    Cancelled,

    #[error("runtime error: {0}")]
    Runtime(#[from] anyhow::Error),
}

impl WeftError {
    /// The run's own control flow, never a failure of the node's: a pause,
    /// a cancel, a person skipping the wait, a wait given up (which the run
    /// reads by its variant, to say it gave up). Whatever wraps errors
    /// passes these through unchanged.
    pub fn is_run_control(&self) -> bool {
        matches!(self, Self::Suspended { .. } | Self::Cancelled | Self::WaitSkipped(_) | Self::WaitGaveUp(_))
    }
}

pub type WeftResult<T> = Result<T, WeftError>;

/// A node failure carrying exactly `message`. The expression door for
/// node code that detects a bad condition itself: use it inside
/// `map_err`/`ok_or_else` closures that build a rich message, or reach
/// for [`node_bail!`](crate::node_bail) to return it in one statement.
/// The ctx accessors stamp input/config errors themselves, every ctx
/// handle already returns `WeftResult`, outside errors go through
/// `.node_err("doing X")`, and a failure of the work the node worked out
/// itself goes through `node_bail!` / `node_error`, which a program can
/// catch on `_error`. A node names a variant itself only when it spots a
/// mistake in the program (SQL that does not parse, an input value of the
/// wrong shape): `WeftError::Input`, which fails the run even with
/// `_error` wired, because the fix is in the program.
pub fn node_error(message: impl std::fmt::Display) -> WeftError {
    WeftError::NodeFailed(message.to_string())
}

/// The failure of a node that finds a need of its own missing as it runs
/// (a program it launches is not there), named as its `deps.toml` names it
/// (`[[needs]] name`). The node says only what is missing; how to meet it
/// is the platform's to say ([`WeftError::NeedMissing`]).
pub fn need_missing(need: impl std::fmt::Display) -> WeftError {
    WeftError::NeedMissing(need.to_string())
}

/// Fail the node now with this message. The statement door for a
/// node-detected bad condition: expands to
/// `return Err(node_error(format!(...)))`.
#[macro_export]
macro_rules! node_bail {
    ($($arg:tt)*) => {
        return Err($crate::error::node_error(format!($($arg)*)))
    };
}

/// Fail the node now with an input error carrying this message: the
/// statement door for a mistake in the program the node spotted (a value
/// of the wrong shape, SQL that does not parse). Expands to
/// `return Err(WeftError::Input(format!(...)))`. Unlike [`node_bail!`],
/// a wired `_error` does not catch it: the run fails, because the fix is
/// in the program.
#[macro_export]
macro_rules! input_bail {
    ($($arg:tt)*) => {
        return Err($crate::error::WeftError::Input(format!($($arg)*)))
    };
}

/// The one door for wrapping an OUTSIDE error (an HTTP library, a JSON
/// parser, any non-weft `Result` or absent `Option`) into a node
/// failure with context.
pub trait NodeErrExt<T> {
    /// Map the error to a node failure reading "`context`: `error`".
    fn node_err(self, context: impl std::fmt::Display) -> WeftResult<T>;
}

impl<T, E: std::fmt::Display + 'static> NodeErrExt<T> for Result<T, E> {
    /// A weft error that is the run's own control flow
    /// ([`WeftError::is_run_control`]) passes through unchanged: wrapped as
    /// a failure, a pause inside a bus read or a wait would fail the node
    /// instead of pausing it, and a skipped wait would fail it instead of
    /// skipping it.
    fn node_err(self, context: impl std::fmt::Display) -> WeftResult<T> {
        self.map_err(|e| {
            let e: Box<dyn std::any::Any> = Box::new(e);
            match e.downcast::<WeftError>() {
                Ok(weft) if weft.is_run_control() => *weft,
                Ok(weft) => WeftError::NodeFailed(format!("{context}: {weft}")),
                Err(other) => {
                    let other = other.downcast::<E>().expect("the boxed error is the one just boxed");
                    WeftError::NodeFailed(format!("{context}: {other}"))
                }
            }
        })
    }
}

impl<T> NodeErrExt<T> for Option<T> {
    /// `None` becomes a node failure reading exactly `context`, so the
    /// message should state what was missing and why it matters.
    fn node_err(self, context: impl std::fmt::Display) -> WeftResult<T> {
        self.ok_or_else(|| WeftError::NodeFailed(context.to_string()))
    }
}

#[cfg(feature = "runtime")]
impl From<crate::caller::CallerError> for WeftError {
    /// A caller-connection failure surfaces as a node failure (fails
    /// loud, visible in the UI), EXCEPT a disconnect under the
    /// `cancel-on-disconnect` policy, which IS a cancellation of this
    /// run and maps to `Cancelled` so the engine's existing
    /// cancel short-circuit handles it uniformly. The connection layer
    /// fires the per-run cancel flag independently; this mapping
    /// only governs how the awaiting node's `?` propagates.
    fn from(e: crate::caller::CallerError) -> Self {
        use crate::caller::CallerError;
        match e {
            // A disconnect always means the run is being cancelled (the
            // keep-running policy is a silent `Ok(())`, never this error), so
            // it maps cleanly to `Cancelled` with no action to disambiguate.
            CallerError::Disconnected => WeftError::Cancelled,
            other => WeftError::NodeFailed(other.to_string()),
        }
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
        assert!(matches!(&e, WeftError::NodeFailed(m) if m == "sending to bridge: boom"), "{e}");
    }

    #[test]
    fn node_bail_and_node_error_fail_with_the_message_verbatim() {
        fn bails() -> WeftResult<()> {
            crate::node_bail!("bridge rejected the send: {}", "no session");
        }
        let e = bails().unwrap_err();
        assert!(
            matches!(&e, WeftError::NodeFailed(m) if m == "bridge rejected the send: no session"),
            "{e}"
        );
        assert!(matches!(node_error("boom"), WeftError::NodeFailed(m) if m == "boom"));
    }

    #[test]
    fn input_bail_fails_with_an_input_error_carrying_the_message() {
        fn bails() -> WeftResult<()> {
            crate::input_bail!("`count` is {}, and it takes at most 10", 12);
        }
        let e = bails().unwrap_err();
        assert!(matches!(&e, WeftError::Input(m) if m == "`count` is 12, and it takes at most 10"), "{e}");
    }

    /// The run's own control flow passes through a `node_err` unchanged:
    /// a pause still pauses, a skip still skips.
    #[test]
    fn node_err_passes_the_runs_control_flow_through() {
        let controls = || {
            [
                WeftError::Suspended { token: None },
                WeftError::Cancelled,
                WeftError::WaitSkipped("skipped".into()),
                WeftError::WaitGaveUp("gave up".into()),
            ]
        };
        for (control, same) in controls().into_iter().zip(controls()) {
            let passed = Err::<(), _>(control).node_err("waiting").unwrap_err();
            assert_eq!(passed.to_string(), same.to_string(), "{same:?}");
            assert!(passed.is_run_control());
        }
        let failure = Err::<(), _>(WeftError::NodeFailed("boom".into())).node_err("waiting").unwrap_err();
        assert!(matches!(&failure, WeftError::NodeFailed(m) if m.starts_with("waiting: ")), "{failure}");
    }

    #[test]
    fn node_err_turns_none_into_a_failure_with_the_message_verbatim() {
        assert_eq!(Some(7).node_err("unused").unwrap(), 7);

        let e = None::<u32>.node_err("no messageId on a media message").unwrap_err();
        assert!(matches!(&e, WeftError::NodeFailed(m) if m == "no messageId on a media message"), "{e}");
    }
}
