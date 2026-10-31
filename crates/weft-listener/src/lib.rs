//! The listener role: the kind-aware processor for signals.
//!
//! One logical service: a module of a local install's one process, or a
//! service of its own that scales to zero. The durable `signal` table
//! (read through the broker) is the truth about which signals exist; the
//! listener reads a signal from its row on every call, so any copy of it
//! answers for any signal.
//!
//! What a signal needs between fires decides where it runs
//! ([`kinds::BetweenFires`]): one the outside calls in to needs nothing,
//! one that wakes at times hands each next wake to the platform's
//! [`weft_platform_traits::Alarm`], and one that holds a connection open
//! is held by a holder: this same code, run where something stays up,
//! claiming the signals it holds through the broker ([`hold`]).
//!
//! Endpoints (internal, platform identity required):
//!   POST /prepare, /start, /unregister, /process, /match_push,
//!   /wake_by_hand, /live, /cancel_call, /rehydrate, /wake; GET /signals,
//!   /health.
//! A fire the listener raises itself (a timer's tick, an event on a held
//! connection) goes straight to its project's worker door when it is an
//! entry's, and as a `FireSignal` task through the broker otherwise
//! (`fire_sink`).

pub mod config;
pub mod event_context;
pub mod fire_sink;
pub mod hold;
pub mod kinds;
pub mod listener_access;
pub mod load;
pub mod reach;
pub mod registry;
pub mod router;
pub mod socket_engine;
pub mod stream_engine;

pub use config::{Holders, ListenerConfig};
pub use router::router;

use std::sync::Arc;

use weft_broker_client::{BrokerLink, BrokerSignalClient};
use weft_platform_traits::{Alarm, HolderLiveness, HolderNews};
use weft_task_store::TaskStoreClient;

use crate::fire_sink::FireSignalSink;
use crate::registry::Registry;

#[derive(Clone)]
pub struct ListenerState {
    pub config: Arc<ListenerConfig>,
    pub registry: Arc<Registry>,
    /// Where held-event kinds send their fires.
    pub fire_sink: FireSignalSink,
    /// The durable signal rows: loading one, listing the held ones,
    /// writing a kind's state.
    pub signals: Arc<BrokerSignalClient>,
    /// The broker's event-serving surface: connection resolution and
    /// provider subscriptions for the kinds that act as a connection.
    pub events_broker: Arc<weft_broker_client::BrokerEventsClient>,
    /// Wakes the listener at a time: every `Wakes` kind's next moment.
    pub alarm: Arc<dyn Alarm>,
    /// What this process holds and has said about it, when it holds
    /// (`ListenerConfig::holds_here`).
    pub holding: Arc<hold::Holding>,
    /// What holders hear, and how a dead one is noticed.
    pub holders: HolderPlatform,
    /// Where this process connects and as whom, as the broker last said.
    pub reach: Arc<reach::Reach>,
}

/// The platform's side of holding (`weft_platform_traits::holders`).
#[derive(Clone)]
pub struct HolderPlatform {
    pub news: Arc<dyn HolderNews>,
    pub liveness: Arc<dyn HolderLiveness>,
}

impl ListenerState {
    pub fn new(
        config: ListenerConfig,
        tasks: Arc<dyn TaskStoreClient>,
        link: BrokerLink,
        alarm: Arc<dyn Alarm>,
        doors: Arc<dyn fire_sink::WorkerDoors>,
        holders: HolderPlatform,
    ) -> Self {
        let signals = BrokerSignalClient::new(link.clone());
        let registry = Arc::new(Registry::new());
        let holding = Arc::<hold::Holding>::default();
        let reach = Arc::<reach::Reach>::default();
        let fire_sink = FireSignalSink::new(tasks, signals.clone(), registry.clone(), doors, holding.changed.clone(), reach.clone());
        let events_broker = weft_broker_client::BrokerEventsClient::new(link);
        Self {
            config: Arc::new(config),
            registry,
            fire_sink,
            signals,
            events_broker,
            alarm,
            holding,
            holders,
            reach,
        }
    }
}
