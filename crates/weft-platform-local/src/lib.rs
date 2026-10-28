//! The local implementation of the translation layer
//! (`weft-platform-traits`): one process for weft's roles, the projects'
//! workers as processes beside it, and the local Docker daemon for infra.
//!
//! - `docker`: the Docker daemon, behind a trait so the rest is tested
//!   against a fake.
//! - `processes`: the machine's processes, behind a trait the same way.
//! - `runner`: a project's workers, one process per project and program.
//! - `registry`: the worker processes each life of the runtime started.
//! - `infra_host`: infra units, each a group of containers.
//! - `images`: workers compiled on the machine (`host_build`, in the
//!   machine's compile `lanes`), infra images with `docker build` into the
//!   daemon's own store.
//! - `layout`: where the machine's and each install's files are.
//! - `needs`: which of a node's needs the machine lacks.
//! - `alarm`: wakes kept in Postgres, delivered by the runtime.
//! - `identity`: identities the install signs with its own key.
//! - `news`: nothing to carry, the database being listened to for good.
//! - `folders`: every folder working copy kept in sync with its folder.

pub mod alarm;
pub mod cargo_cache;
pub mod docker;
pub mod domains;
pub mod folders;
pub mod frontends;
pub mod holders;
pub mod host_build;
pub mod identity;
pub mod images;
pub mod infra_host;
pub mod lanes;
pub mod layout;
pub mod needs;
pub mod news;
pub mod processes;
pub mod registry;
pub mod runner;

pub use alarm::{Deliver, EarlierDeliveries, HttpDeliver, LocalAlarm, NotTaken};
pub use docker::{Docker, DockerCli};
pub use domains::NoDomains;
pub use frontends::NoFrontendHosting;
pub use holders::{DatabaseHolderNews, OneProcessHolds};
pub use identity::LocalIdentity;
pub use host_build::MachineToolchain;
pub use images::{bound_build_cache, LocalImageBuilder};
pub use layout::{InstallLayout, MachineLayout};
pub use needs::LocalNeeds;
pub use processes::MachineProcesses;
pub use infra_host::{AgentBroker, AgentIdentity, DiskBacking, GpuAccess, LocalInfraHost, LocalInfraHostConfig, Publish};
pub use news::DatabaseNews;
pub use runner::{LocalRunner, LocalRunnerConfig};
