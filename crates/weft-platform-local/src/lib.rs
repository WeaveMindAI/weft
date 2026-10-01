//! The local implementation of the translation layer
//! (`weft-platform-traits`): one process for weft's roles and the local
//! Docker daemon for everything else.
//!
//! - `docker`: the Docker daemon, behind a trait so the rest is tested
//!   against a fake.
//! - `runner`: a project's workers, one container per project and image.
//! - `infra_host`: infra units, each a group of containers.
//! - `images`: `docker build`, into the daemon's own store.
//! - `alarm`: wakes kept in Postgres, delivered by the runtime.
//! - `identity`: identities the install signs with its own key.

pub mod alarm;
pub mod docker;
pub mod identity;
pub mod images;
pub mod infra_host;
pub mod runner;

pub use alarm::{Deliver, HttpDeliver, LocalAlarm, NotTaken};
pub use docker::{Docker, DockerCli};
pub use identity::LocalIdentity;
pub use images::{bound_build_cache, DockerImageBuilder};
pub use infra_host::{DiskBacking, GpuAccess, LocalInfraHost, LocalInfraHostConfig, Publish};
pub use runner::{LocalRunner, LocalRunnerConfig};
