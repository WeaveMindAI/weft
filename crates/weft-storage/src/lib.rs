//! The Weft storage plane: the per-tenant storage box service
//! (chunk placement over plain-PVC backing disks, identity-walled
//! HTTP surface, capability authority), the resize watcher, and the
//! consumer-side `StorageOps` client + fakes.

pub mod auth;
pub mod boxstate;
pub mod capability;
pub mod client;
pub mod config;
pub mod disk;
pub mod index;
pub mod key;
pub mod protocol;
pub mod resize;
pub mod service;
pub mod store;

#[cfg(any(test, feature = "test-helpers"))]
pub mod testing;
