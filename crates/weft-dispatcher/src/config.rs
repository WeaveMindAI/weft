use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DispatcherConfig {
    /// TCP port to bind the HTTP server. Default 9999 for local dev.
    pub http_port: u16,

    /// Where to store persistent state (embedded restate data, local
    /// project binaries, etc). Defaults to `~/.weft` on unix.
    pub data_dir: PathBuf,

    /// Which worker backend to use. Values: "subprocess" (local
    /// default), "kubernetes" (cloud / BYOC). The closed-source
    /// weavemind repo adds cloud-specific backends on top.
    pub worker_backend: String,

    /// Which infra backend to use. Values: "kind" (local),
    /// "kubernetes" (cloud / BYOC).
    pub infra_backend: String,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        let data_dir = std::env::var_os("WEFT_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                dirs_data_dir()
                    .unwrap_or_else(|| PathBuf::from("."))
                    .join("weft")
            });
        Self {
            http_port: 9999,
            data_dir,
            worker_backend: "subprocess".into(),
            infra_backend: "kind".into(),
        }
    }
}

fn dirs_data_dir() -> Option<PathBuf> {
    // Phase A2: replace with a proper dirs crate. Avoid adding the
    // dep during scaffolding to keep workspace minimal.
    std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/share"))
}
