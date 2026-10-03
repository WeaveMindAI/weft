//! What a listener is started with.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListenerConfig {
    /// This process's replica id: what its broker calls are made as, the
    /// name its claims on held signals are taken in, and what keeps its
    /// shared provider sockets apart from another in-process listener's
    /// (tests).
    pub replica: String,
    /// Broker base URL: the listener's only door to the durable `signal`
    /// table (loading a signal, writing its kind state, claiming held
    /// ones) and to the task queue its fires ride.
    pub broker_url: String,
    /// Whether this process holds the signals that keep a connection open
    /// (a local install's one process, or a holder): it runs the hold loop
    /// (`crate::hold`) and brings such a signal up itself. A listener that
    /// scales to zero does not; the holders do.
    pub holds_here: bool,
    /// Whether a subscription a provider can push to this install takes
    /// the push over a connection held open: where holding costs money
    /// (a cloud's holders run only while something needs one) and the
    /// install's address is the internet's for good. A local install keeps
    /// dialing out, since its one process is up anyway and its tunnel's
    /// address can change.
    pub prefer_push: bool,
}
