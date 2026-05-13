//! Shared filesystem walk for hash + image-stamp inputs.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// Recursive directory walk that returns every regular file under
/// `root`, skipping the standard build/cache directories. Order is
/// not stable; callers that need deterministic order sort the
/// returned vec themselves.
pub fn walk_dir(root: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir)
            .with_context(|| format!("read_dir {}", dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            if is_ignored(&path) {
                continue;
            }
            if path.is_dir() {
                stack.push(path);
            } else {
                out.push(path);
            }
        }
    }
    Ok(out)
}

fn is_ignored(path: &Path) -> bool {
    let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
    matches!(name, "target" | "node_modules" | ".git" | ".weft")
}
