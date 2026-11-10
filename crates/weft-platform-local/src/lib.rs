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
//! - `process_units`: infra units that run one process on the machine,
//!   each under an agent of its own; `machine_host` routes a local
//!   install's units to the one of the two that runs them.
//! - `kept_ports`: the host port each of those units' ports keeps on the
//!   machine, from its first start until it is removed.
//! - `probe`: one network check of a unit's port.
//! - `infra_names`: what those containers and their volumes are called,
//!   and what happens to a disk named before the names carried the install
//!   (`disk_claims`: which install a person said takes one).
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
pub mod disk_claims;
pub mod docker;
pub mod domains;
pub mod folders;
pub mod frontends;
pub mod holders;
pub mod host_build;
pub mod identity;
pub mod images;
pub mod infra_host;
pub mod infra_names;
pub mod kept_ports;
pub mod lanes;
pub mod layout;
pub mod machine_host;
pub mod needs;
pub mod news;
pub mod probe;
pub mod process_units;
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
pub use kept_ports::{KeptPorts, MachinePorts};
pub use layout::{InstallLayout, MachineLayout};
pub use needs::LocalNeeds;
pub use machine_host::MachineInfraHost;
pub use process_units::{ProcessUnits, ProcessUnitsConfig};
pub use processes::MachineProcesses;
pub use infra_host::{AgentBroker, AgentIdentity, DiskBacking, GpuAccess, LocalInfraHost, LocalInfraHostConfig, Publish};
pub use news::DatabaseNews;
pub use runner::{LocalRunner, LocalRunnerConfig};
