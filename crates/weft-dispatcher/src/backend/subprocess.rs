//! Subprocess worker backend. Spawns the weft-runner binary as a
//! child process per execution. Appropriate for local dev
//! (`weft start`); cloud uses a different backend that spawns
//! isolated pods.

use std::path::Path;
use std::process::Stdio;

use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

use crate::backend::{WakeContext, WorkerBackend, WorkerHandle};

pub struct SubprocessWorkerBackend {
    /// Path to the `weft-runner` binary. Default discovery: look for
    /// `weft-runner` on PATH.
    runner_path: String,
    /// Base URL of this dispatcher, so the spawned runner can call
    /// back with cost reports and suspension requests.
    dispatcher_url: String,
}

impl SubprocessWorkerBackend {
    pub fn new(runner_path: impl Into<String>, dispatcher_url: impl Into<String>) -> Self {
        Self {
            runner_path: runner_path.into(),
            dispatcher_url: dispatcher_url.into(),
        }
    }
}

#[async_trait]
impl WorkerBackend for SubprocessWorkerBackend {
    async fn spawn_worker(
        &self,
        binary_path: &Path,
        wake: WakeContext,
    ) -> anyhow::Result<WorkerHandle> {
        // For the subprocess backend, `binary_path` is the path to
        // the compiled ProjectDefinition JSON (we don't codegen per
        // project; we hand the runner the project + wake context).
        let mut cmd = Command::new(&self.runner_path);
        cmd.arg("--project").arg(binary_path);
        cmd.arg("--color").arg(wake.color.to_string());
        cmd.arg("--entry-node").arg(&wake.resume_node);
        let payload = serde_json::to_string(&wake.resume_value)?;
        cmd.arg("--entry-payload").arg(payload);
        cmd.arg("--dispatcher").arg(&self.dispatcher_url);
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

        let mut child = cmd.spawn().map_err(|e| anyhow::anyhow!("spawn runner: {e}"))?;
        let id = child.id().map(|p| p.to_string()).unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        // Drain stdout/stderr so the child isn't blocked. For phase
        // A2, logs go to the dispatcher's tracing stream; a later
        // iteration will ship them into the journal so the dashboard
        // can show them.
        if let Some(stdout) = child.stdout.take() {
            tokio::spawn(async move {
                let mut lines = BufReader::new(stdout).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::info!(target: "weft_dispatcher::worker", "{}", line);
                }
            });
        }
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::info!(target: "weft_dispatcher::worker", "{}", line);
                }
            });
        }

        // Fire-and-forget the reap: runner exits when the execution
        // completes or suspends. We capture the exit status in the
        // background; the dispatcher doesn't block on it.
        let handle_id = id.clone();
        tokio::spawn(async move {
            match child.wait().await {
                Ok(status) => tracing::info!(
                    target: "weft_dispatcher::worker",
                    id = %handle_id, status = ?status, "worker exited"
                ),
                Err(e) => tracing::error!(
                    target: "weft_dispatcher::worker",
                    id = %handle_id, error = %e, "worker wait failed"
                ),
            }
        });

        Ok(WorkerHandle { id })
    }

    async fn kill_worker(&self, handle: WorkerHandle) -> anyhow::Result<()> {
        // Phase A2: send SIGTERM via PID. Subprocess pids are in the
        // handle's id string (when the runner is directly spawned).
        // Skipping implementation until cancellation end-to-end lands.
        tracing::debug!(target: "weft_dispatcher::worker", id = %handle.id, "kill_worker no-op");
        Ok(())
    }
}
