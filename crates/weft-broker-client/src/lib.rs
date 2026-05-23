//! HTTP client crate for `weft-broker`. Holds the wire protocol
//! (`protocol`) and the trait implementations (`client`) that
//! workers / listeners use as drop-in replacements for direct
//! Postgres clients.
//!
//! Authentication: the client reads its bearer token from a fixed
//! filesystem path on every call (the kubelet rotates the projected
//! token periodically; reading on each call keeps the auth fresh
//! without any in-process refresh logic).

pub mod client;
pub mod lifecycle_command;
pub mod protocol;
pub mod token;

pub use client::{
    BrokerInfraClient, BrokerInfraStateClient, BrokerJournalClient, BrokerSignalClient,
    BrokerSupervisorClient, BrokerTaskStoreClient, BrokerWorkerPodClient, WriteOutcome,
};
pub use token::TokenSource;
