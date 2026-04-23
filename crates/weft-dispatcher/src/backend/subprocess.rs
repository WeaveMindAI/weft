//! Subprocess worker backend. Spawns the project's compiled binary
//! as a subprocess per wake. The binary's location comes from the
//! project registry (`ProjectSummary.binary_path`), populated by
//! `weft build` / `weft run`. The binary itself is emitted by
//! `weft-compiler::codegen`.
//!
//! Phase A Slice 3: the worker connects back to the dispatcher via
//! WebSocket at `${dispatcher_url}/ws/executions/{color}`. All state
//! transfer (start wake, queued deliveries, suspension tokens, cost,
//! logs, node events, snapshot on stall) happens over that socket.
//! The subprocess just needs the color and dispatcher URL; no CLI
//! args carry wake data.

use std::path::Path;
use std::process::Stdio;

use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

use crate::backend::{WakeContext, WorkerBackend, WorkerHandle};

pub struct SubprocessWorkerBackend {
    dispatcher_url: String,
}

impl SubprocessWorkerBackend {
    pub fn new(_legacy_runner_path: impl Into<String>, dispatcher_url: impl Into<String>) -> Self {
        // The first argument is legacy; callers still pass it so
        // `main.rs` doesn't need to change. It is ignored.
        Self { dispatcher_url: dispatcher_url.into() }
    }
}

#[async_trait]
impl WorkerBackend for SubprocessWorkerBackend {
    async fn spawn_worker(
        &self,
        binary_path: &Path,
        wake: WakeContext,
    ) -> anyhow::Result<WorkerHandle> {
        if !binary_path.exists() {
            anyhow::bail!(
                "project binary not found at {}. Run `weft build` first.",
                binary_path.display()
            );
        }

        let mut cmd = Command::new(binary_path);
        cmd.arg("--color").arg(wake.color.to_string());
        cmd.arg("--dispatcher").arg(&self.dispatcher_url);
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = cmd.spawn().map_err(|e| anyhow::anyhow!("spawn worker: {e}"))?;
        let id = child
            .id()
            .map(|p| p.to_string())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

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
        tracing::debug!(target: "weft_dispatcher::worker", id = %handle.id, "kill_worker no-op");
        Ok(())
    }
}
