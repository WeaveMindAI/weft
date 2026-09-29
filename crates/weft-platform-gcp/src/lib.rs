//! Google Cloud's implementation of the translation layer
//! (`weft-platform-traits`).
//!
//! - `metadata`: this process's own identity (all a worker links).
//! - behind `control`: the runtime's side, each trait on Google's service
//!   for it: workers on Cloud Run (`runner`), infra on Compute Engine
//!   (`infra_host`), images on Cloud Build and Artifact Registry
//!   (`images`), wakes on Cloud Tasks (`alarm`), and caller identities
//!   verified against Google's keys (`identity`). `api` is the one REST
//!   client they share, `names` what they call things, `accounts` the
//!   project's own service account.

pub mod metadata;

#[cfg(feature = "control")]
pub mod accounts;
#[cfg(feature = "control")]
pub mod alarm;
#[cfg(feature = "control")]
pub mod api;
#[cfg(feature = "control")]
pub mod identity;
#[cfg(feature = "control")]
pub mod images;
#[cfg(feature = "control")]
pub mod infra_host;
#[cfg(feature = "control")]
pub mod names;
#[cfg(feature = "control")]
pub mod runner;

pub use metadata::MetadataTokens;
#[cfg(feature = "control")]
pub use {
    alarm::CloudTasksAlarm, api::Google, identity::GoogleIdentity, images::CloudBuildImages,
    infra_host::ComputeInfraHost, runner::CloudRunRunner,
};
