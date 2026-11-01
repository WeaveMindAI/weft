//! Google Cloud's implementation of the translation layer
//! (`weft-platform-traits`).
//!
//! - `metadata`: this process's own identity (all a worker links).
//! - behind `control`: the runtime's side, each trait on Google's service
//!   for it: workers on Cloud Run (`runner`), infra on Compute Engine
//!   (`infra_host`), images on Cloud Build and Artifact Registry
//!   (`images`), wakes on Cloud Tasks (`alarm`), and caller identities
//!   verified against Google's keys (`identity`), the object store on
//!   Cloud Storage (`storage`), frontends on Cloud Run (`frontends`), the
//!   door in front of the install's domains on a load balancer
//!   (`domains`), the holders on a worker pool (`holders`), what happened
//!   to a run told across copies on Pub/Sub (`news`), and what holders are
//!   told on a Pub/Sub topic of their own (`holder_news`), and which of a
//!   node's needs its worker image can carry (`needs`). `api` is the one REST
//!   client they share, `names` what they call things, `accounts` the
//!   project's own service account, `alert_line` the log line the
//!   install's alert policy matches.

pub mod metadata;

#[cfg(feature = "control")]
pub mod accounts;
#[cfg(feature = "control")]
pub mod alarm;
#[cfg(feature = "control")]
pub mod alert_line;
#[cfg(feature = "control")]
pub mod api;
#[cfg(feature = "control")]
pub mod domains;
#[cfg(feature = "control")]
pub mod frontends;
#[cfg(feature = "control")]
pub mod holder_news;
#[cfg(feature = "control")]
pub mod holders;
#[cfg(feature = "control")]
pub mod identity;
#[cfg(feature = "control")]
pub mod images;
#[cfg(feature = "control")]
pub mod infra_host;
#[cfg(feature = "control")]
pub mod names;
#[cfg(feature = "control")]
pub mod needs;
#[cfg(feature = "control")]
pub mod news;
#[cfg(feature = "control")]
pub mod runner;
#[cfg(feature = "control")]
pub mod storage;

pub use metadata::{identity_from_env, MetadataTokens};
#[cfg(feature = "control")]
pub use {
    alarm::CloudTasksAlarm, api::Google, domains::LoadBalancerDomains, frontends::CloudRunFrontends, holder_news::PubSubHolderNews, holders::WorkerPoolHolders,
    identity::GoogleIdentity, images::CloudBuildImages, infra_host::ComputeInfraHost, needs::ImageNeeds, news::PubSubNews, runner::{CloudRunRunner, REQUEST_CAP, STOP_GRACE},
    storage::GcsObjectStore,
};
