//! The listener role: the kind-aware processor for signals.
//!
//! One logical service: a module of the machine's process, or a service
//! of its own that scales to zero. The durable `signal` table (read
//! through the broker) is the truth about which signals exist; the
//! listener keeps what it has seen in memory and loads a signal it has
//! not seen yet on first use, so any copy of it answers for any signal.
//!
//! What a kind needs between fires decides where it can run
//! ([`kinds::BetweenFires`]): a kind the outside calls in to needs
//! nothing, a kind that wakes at times hands each next wake to the
//! platform's [`weft_platform_traits::Alarm`], and a kind that holds a
//! connection open needs the listener placed on the machine.
//!
//! Endpoints (internal, platform identity required):
//!   POST /prepare, /start, /unregister, /process, /match_push,
//!   /wake_by_hand, /live, /rehydrate, /wake; GET /signals, /health.
//! A fire the listener raises itself (a timer's tick, an event on a held
//! connection) is enqueued as a `FireSignal` task through the broker; the
//! dispatcher runs it back through `/process` like any other fire.

pub mod config;
pub mod event_context;
pub mod fire_sink;
pub mod infra_address;
pub mod kinds;
pub mod listener_access;
pub mod registry;
pub mod router;
pub mod socket_engine;
pub mod stream_engine;

pub use config::ListenerConfig;
pub use router::router;

use std::sync::Arc;

use weft_broker_client::{BrokerSignalClient, TokenSource};
use weft_platform_traits::Alarm;
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
}

impl ListenerState {
    pub fn new(
        config: ListenerConfig,
        tasks: Arc<dyn TaskStoreClient>,
        token_source: TokenSource,
        alarm: Arc<dyn Alarm>,
    ) -> Self {
        let signals = BrokerSignalClient::new(config.broker_url.clone(), token_source.clone());
        let fire_sink = FireSignalSink::new(tasks, signals.clone());
        let events_broker = weft_broker_client::BrokerEventsClient::new(config.broker_url.clone(), token_source);
        Self {
            config: Arc::new(config),
            registry: Arc::new(Registry::new()),
            fire_sink,
            signals,
            events_broker,
            alarm,
        }
    }
}
