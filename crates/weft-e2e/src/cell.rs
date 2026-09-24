//! A cell: a whole weft install of a test's own, beside the default one.
//!
//! Most tests share the default install and stay out of each other's way,
//! because each only touches the projects it made. A few cannot: the ones
//! that test the POOLS (the listener and supervisor pods every project on an
//! install shares) watch pods move and shrink, and a neighbour's triggers on
//! the same pool would move with them. Those tests start a cell.
//!
//! A cell is a named install (`weft_core::infra::Instance`) in the same kind
//! cluster: its own dispatcher, Postgres, broker and pools, in namespaces of
//! its own, sharing only the node, the front door's gateway, the object store
//! and the images. It starts from the images the default install already
//! has, so nothing is built.
//!
//! A cell also chooses how fast its own timers run
//! (`weft_core::time_scale`): at [`Cell::FAST`], a scale-down that waits a
//! minute of real time comes round in six seconds, and so does every
//! heartbeat, lease and silence window it depends on, together. Budgets
//! for real work (a pod's spawn grace, a boot) keep their real length.
//!
//! Like a project, a cell is removed when the test passes
//! ([`Cell::finish`]) and kept when it fails, so what the test saw is still
//! there to look at.

use std::time::Duration;

use anyhow::{Context, Result};

use crate::client::{poll_until, Dispatcher};

/// Every cell's name starts with this, so `scripts/run-e2e.sh --clean` can
/// find the cells failed runs kept without touching any other install.
pub const CELL_NAME_PREFIX: &str = "e2e";

/// A cell of a test's own. See the module docs.
pub struct Cell {
    instance: weft_core::infra::Instance,
    /// `None` only while [`Self::start`] is still bringing it up.
    dispatcher: Option<Dispatcher>,
    time_scale: f64,
    finished: bool,
}

impl Cell {
    /// The pace for a test that waits on the runtime's own timers: ten
    /// times real time. Faster shrinks the shortest silence window (a
    /// worker counts as dead after three missed heartbeats, 30 seconds at
    /// real time) below a few seconds, where a busy machine's own pauses
    /// start to look like deaths.
    pub const FAST: f64 = 0.1;

    /// Start a cell whose own timers run at `time_scale` times real time
    /// (`1.0` for real time, [`Self::FAST`] for a test that waits on
    /// them), and wait until its dispatcher answers.
    pub async fn start(time_scale: f64) -> Result<Self> {
        let root = crate::ensure::repo_root()?;
        // Fits `weft_core::infra::MAX_INSTANCE_NAME`: the prefix and nine
        // characters of a fresh id.
        let name = format!("{CELL_NAME_PREFIX}{}", &uuid::Uuid::new_v4().simple().to_string()[..9]);
        let instance =
            weft_core::infra::Instance::named(&name).map_err(anyhow::Error::msg)?;
        let started = std::time::Instant::now();
        let out = tokio::process::Command::new("weft")
            .args(["daemon", "start"])
            .current_dir(&root)
            .env("WEFT_REPO_ROOT", &root)
            .env(weft_core::infra::INSTANCE_ENV, &name)
            .env(weft_core::time_scale::TIME_SCALE_ENV, time_scale.to_string())
            .stdin(std::process::Stdio::null())
            .output()
            .await
            .context("spawn `weft daemon start` for a cell")?;
        eprintln!("[e2e] {:.1}s: weft daemon start (cell {name})", started.elapsed().as_secs_f64());
        // From here on the cell exists, whole or in part: the guard keeps
        // it for inspection if anything below fails.
        let mut cell = Self { instance: instance.clone(), dispatcher: None, time_scale, finished: false };
        anyhow::ensure!(
            out.status.success(),
            "starting the cell {name} failed (exit {:?})\n--- stdout ---\n{}\n--- stderr ---\n{}",
            out.status.code(),
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        let port = door_port(&instance).await?;
        let dispatcher = Dispatcher::for_install(&format!("http://127.0.0.1:{port}"), instance)?;
        crate::ensure::wait_healthy(&dispatcher).await?;
        cell.dispatcher = Some(dispatcher);
        Ok(cell)
    }

    /// The cell's dispatcher: hand it to [`crate::project::Project::prepare`]
    /// and every project, CLI call and platform probe follows it here.
    pub fn dispatcher(&self) -> Dispatcher {
        self.dispatcher.clone().expect("a started cell has a dispatcher")
    }

    /// How fast the cell's own timers run.
    pub fn time_scale(&self) -> f64 {
        self.time_scale
    }

    /// `real` at the cell's pace: what a test waits for one of the
    /// runtime's own timers to come round.
    pub fn scaled(&self, real: Duration) -> Duration {
        weft_core::time_scale::scaled_by(real, self.time_scale)
    }

    /// Remove the cell: call as the last line of a PASSING test, after every
    /// project in it finished. A failing test never gets here, and the cell
    /// stays for inspection.
    pub async fn finish(mut self) -> Result<()> {
        let name = self.name().to_string();
        let started = std::time::Instant::now();
        let out = tokio::process::Command::new("weft")
            .args(["daemon", "remove"])
            .env(weft_core::infra::INSTANCE_ENV, &name)
            .stdin(std::process::Stdio::null())
            .output()
            .await
            .context("spawn `weft daemon remove` for a cell")?;
        eprintln!("[e2e] {:.1}s: weft daemon remove (cell {name})", started.elapsed().as_secs_f64());
        anyhow::ensure!(
            out.status.success(),
            "removing the cell {name} failed (exit {:?})\n--- stdout ---\n{}\n--- stderr ---\n{}",
            out.status.code(),
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        self.finished = true;
        Ok(())
    }

    fn name(&self) -> &str {
        self.instance.name().expect("a cell is a named install")
    }
}

impl Drop for Cell {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // Same policy as a project's teardown guard: Drop cannot await, and a
        // cell a test did not finish is exactly what the reader wants to see.
        let at = self.dispatcher.as_ref().map(|d| format!(" at {}", d.base())).unwrap_or_default();
        eprintln!(
            "weft-e2e: cell '{name}' NOT finished (test ended early); keeping it for \
             inspection{at}. Its namespaces are {system} and {db}. Remove it with \
             `WEFT_INSTANCE={name} weft daemon remove`, or every kept cell with \
             `scripts/run-e2e.sh --clean`.",
            name = self.name(),
            system = self.instance.system_namespace(),
            db = self.instance.db_namespace(),
        );
    }
}

/// The node port the apiserver gave the cell's door
/// (`deploy/k8s/instance-door.yaml`).
async fn door_port(instance: &weft_core::infra::Instance) -> Result<u16> {
    let namespace = instance.system_namespace();
    poll_until(
        &format!("the node port of {namespace}'s door"),
        Duration::from_secs(30),
        Duration::from_millis(250),
        || {
            let namespace = namespace.clone();
            async move {
                let out = tokio::process::Command::new("kubectl")
                    .args([
                        "-n",
                        &namespace,
                        "get",
                        "service",
                        "weft-dispatcher-node-port",
                        "-o",
                        "jsonpath={.spec.ports[0].nodePort}",
                    ])
                    .output()
                    .await
                    .context("spawn kubectl")?;
                Ok(String::from_utf8_lossy(&out.stdout).trim().parse::<u16>().ok())
            }
        },
    )
    .await
}
