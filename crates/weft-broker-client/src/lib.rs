//! HTTP client crate for `weft-broker`. Holds the wire protocol
//! (`protocol`) and the trait implementations (`client`) that
//! workers / listeners use as drop-in replacements for direct
//! Postgres clients.
//!
//! Every call of a process travels on its one line to the broker
//! (`line::BrokerLink`). Authentication: every call carries the caller's
//! platform identity for the broker's address and the id of the calling
//! process replica (see `token::TokenSource`).

pub mod activation;
pub mod client;
pub mod line;
pub mod lifecycle_command;
pub mod protocol;
pub mod token;

pub use client::{
    BrokerAccessClient, BrokerDoorClient, BrokerEventsClient, BrokerExecutionClient, BrokerInfraClient,
    BrokerInfraStateClient, BrokerProjectClient, BrokerRecordClient, BrokerRefused, BrokerRunClient, BrokerSignalClient,
    BrokerSupervisorClient, BrokerTaskStoreClient, WriteOutcome,
};
pub use line::BrokerLink;
pub use token::TokenSource;
