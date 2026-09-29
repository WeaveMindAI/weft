//! What a listener is started with.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListenerConfig {
    /// This process instance's id: what its broker calls are made as, and
    /// what keeps its shared provider sockets apart from another
    /// in-process listener's (tests).
    pub instance: String,
    /// Broker base URL: the listener's only door to the durable `signal`
    /// table (loading a signal, writing its kind state) and to the task
    /// queue its fires ride.
    pub broker_url: String,
    /// Where the listener runs. On the machine it holds connections open
    /// between fires; placed serverless it cannot, and refuses a signal
    /// that needs one.
    pub placement: weft_platform_traits::Placement,
}
