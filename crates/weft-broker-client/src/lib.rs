//! HTTP client crate for `weft-broker`. Holds the wire protocol
//! (`protocol`) and the trait implementations (`client`) that
//! workers / listeners use as drop-in replacements for direct
//! Postgres clients.
//!
//! Authentication: every call carries the caller's platform identity for
//! the broker's address and the id of the calling process replica (see
//! `token::TokenSource`).

pub mod activation;
pub mod client;
pub mod lifecycle_command;
pub mod protocol;
pub mod token;

pub use client::{
    BrokerAccessClient, BrokerEventsClient, BrokerExecutionClient, BrokerInfraClient,
    BrokerInfraStateClient, BrokerJournalClient, BrokerProjectClient, BrokerSignalClient,
    BrokerSupervisorClient, BrokerRefused, BrokerTaskStoreClient, WriteOutcome,
};
pub use token::TokenSource;
