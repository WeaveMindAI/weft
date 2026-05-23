//! Bearer token source. Reads the projected SA token from a
//! filesystem path on every call so the kubelet's automatic token
//! rotation propagates without any in-process refresh logic. The
//! OS page cache covers the per-call cost.
//!
//! The path is the same conventional path k8s uses for projected
//! tokens (`/var/run/secrets/...`), but configurable via env so a
//! dev binary can point at any token file.

use std::path::PathBuf;

#[derive(Clone)]
pub struct TokenSource {
    path: PathBuf,
}

impl TokenSource {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Default path for in-cluster pods; the dispatcher mounts the
    /// projected SA token at this location for listener / worker /
    /// infra pods.
    pub fn default_path() -> PathBuf {
        PathBuf::from("/var/run/weft/sa/token")
    }

    /// Read the token. Reads the file every call; the kubelet
    /// rewrites the file in place during rotation, so a fresh read
    /// is the simplest way to stay current. Atomic-rename rotation
    /// in k8s makes the read race-free; if a transient `NotFound`
    /// shows up (kubelet mid-rotation, volume remount), retry once
    /// after a short sleep before propagating.
    pub async fn read(&self) -> anyhow::Result<String> {
        match self.read_once().await {
            Ok(t) => Ok(t),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound
                || e.kind() == std::io::ErrorKind::Interrupted =>
            {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                self.read_once()
                    .await
                    .map_err(|e| anyhow::anyhow!("read token at {}: {e}", self.path.display()))
            }
            Err(e) => Err(anyhow::anyhow!(
                "read token at {}: {e}",
                self.path.display()
            )),
        }
    }

    async fn read_once(&self) -> std::io::Result<String> {
        let bytes = tokio::fs::read(&self.path).await?;
        let text = String::from_utf8(bytes).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "token file is not utf8",
            )
        })?;
        Ok(text.trim().to_string())
    }
}
