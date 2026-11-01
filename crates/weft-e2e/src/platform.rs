//! The platform layer: a host-side, test-only window into what the SYSTEM does
//! underneath a running program (which infra containers run, which worker
//! programs the install holds), plus the levers a test needs to drive it:
//! faking a worker crash or a platform stop.
//!
//! ## Why this reaches behind the public API
//!
//! The program layer (the rest of this crate) asserts through the dispatcher's
//! public HTTP API, exactly as the outside world does. But platform facts
//! (which worker processes run a project, which containers back its infra)
//! are NOT on that surface, by design: exposing them would add privileged
//! endpoints to the shipped system for a need only tests have. So the
//! platform layer reaches BEHIND the API, the way an operator of a local
//! install can: it signals the worker processes its runtime started (from
//! the install's worker registry), lists the worker programs held on the
//! machine, lists infra containers with Docker, and opens a database
//! directly ([`database`]) for a test that seeds rows. This is host-side
//! TEST code only, compiled solely under the `e2e` feature, never into
//! anything shipped.

use anyhow::{Context, Result};
use std::time::Duration;
use uuid::Uuid;
use weft_task_store::db::{self, Pool};

use crate::client::Dispatcher;

/// Host-side handle to one install's platform state, aimed at the install
/// the dispatcher it was made from belongs to (the default one, or a
/// [`crate::cell::Cell`]).
pub struct Platform {
    install: weft_core::infra::Install,
}

/// A small pool (two connections) on the install database at `url`: what
/// a test that seeds rows directly (a connection, a recipe) writes
/// through.
pub async fn database(url: &str) -> Result<Pool> {
    let pool = Pool::open(db::config(url)?, 2, Duration::from_secs(10), db::Waits::Expected);
    // A first connection now, so an install whose database does not answer
    // fails here rather than at the test's first read.
    pool.get().await.context("connect to the install's Postgres")?;
    Ok(pool)
}

impl Platform {
    /// Aim at `disp`'s install.
    pub async fn connect(disp: &Dispatcher) -> Result<Self> {
        Ok(Self { install: disp.install().clone() })
    }

    /// The worker processes running `project_id`'s program right now, from
    /// the install's worker registry: each one still the process its
    /// runtime started (a pid another program took since is not one).
    async fn workers_of(&self, project_id: &Uuid) -> Result<Vec<weft_platform_local::registry::Entry>> {
        use weft_platform_local::registry;
        let mut running = Vec::new();
        for entry in registry::Registry::in_dir(&weft_platform_local::InstallLayout::new(self.install.dir()).workers()).load()?.into_iter().filter(|e| &e.project == project_id) {
            if registry::still_runs(&weft_platform_local::MachineProcesses, &entry).await? {
                running.push(entry);
            }
        }
        Ok(running)
    }

    /// The running infra containers of `project_id`.
    pub async fn infra_containers_for_project(&self, project_id: &Uuid) -> Result<Vec<String>> {
        use weft_platform_local::docker::{labels, roles};
        let out = tokio::process::Command::new("docker")
            .args([
                "ps",
                "--filter",
                &format!("label={}={}", weft_core::infra::INSTALL_LABEL, self.install.label_value()),
                "--filter",
                &format!("label={}={project_id}", labels::PROJECT),
                "--filter",
                &format!("label={}={}", labels::ROLE, roles::INFRA),
                "--format",
                "{{.Names}}",
            ])
            .output()
            .await
            .context("docker ps")?;
        anyhow::ensure!(out.status.success(), "docker ps failed: {}", String::from_utf8_lossy(&out.stderr));
        Ok(String::from_utf8_lossy(&out.stdout).lines().map(str::to_string).filter(|l| !l.is_empty()).collect())
    }

    /// The binary hashes of the worker programs this install holds on the
    /// machine.
    pub fn worker_programs(&self) -> Result<Vec<String>> {
        weft_platform_local::images::held_workers(&weft_platform_local::MachineLayout::here().workers(), self.install.label_value())
    }

    /// Fake a worker crash: kill every worker process of `project_id` at
    /// once (`SIGKILL`), with no chance to finish what it drives. Returns
    /// the ones it killed; an empty answer means nothing was running, which
    /// a resume test treats as a setup miss.
    pub async fn kill_workers(&self, project_id: &Uuid) -> Result<Vec<String>> {
        self.signal_workers(project_id, weft_platform_local::processes::Signal::Kill).await
    }

    /// Tell every worker process of `project_id` to stop the way a
    /// platform does (`SIGTERM`), leaving it to wind down on its own.
    /// Returns the ones it told; an empty answer means nothing was running.
    pub async fn stop_workers(&self, project_id: &Uuid) -> Result<Vec<String>> {
        self.signal_workers(project_id, weft_platform_local::processes::Signal::Term).await
    }

    async fn signal_workers(&self, project_id: &Uuid, signal: weft_platform_local::processes::Signal) -> Result<Vec<String>> {
        use weft_platform_local::processes::Processes as _;
        let running = self.workers_of(project_id).await?;
        for entry in &running {
            weft_platform_local::MachineProcesses.signal(entry.pid, signal)?;
        }
        Ok(running.into_iter().map(|e| e.name).collect())
    }
}
