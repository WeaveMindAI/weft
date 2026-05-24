//! Shared filesystem walk for hash + image-stamp inputs.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use weft_catalog::is_node_tree_excluded;

/// Recursive directory walk that returns every regular file under
/// `root`, skipping the shared node-tree exclude set
/// (`weft_catalog::NODE_TREE_EXCLUDE`) and never following symlinks.
/// This is the hash side of the one node-tree walk policy: it must
/// see exactly the bytes the build stages, or a missed/extra file
/// silently de/over-syncs the worker-image hash. Order is not stable;
/// callers that need deterministic order sort the returned vec.
pub fn walk_dir(root: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir)
            .with_context(|| format!("read_dir {}", dir.display()))?
        {
            let entry = entry?;
            if is_node_tree_excluded(&entry.file_name().to_string_lossy()) {
                continue;
            }
            // `file_type()` does not follow symlinks: a loop under
            // user-authored `nodes/` must not send the walk infinite.
            let ft = entry.file_type()?;
            if ft.is_symlink() {
                continue;
            }
            let path = entry.path();
            if ft.is_dir() {
                stack.push(path);
            } else if ft.is_file() {
                out.push(path);
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn skips_excluded_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        fs::write(root.join("keep.rs"), "x").unwrap();
        fs::create_dir(root.join("target")).unwrap();
        fs::write(root.join("target/junk.rs"), "x").unwrap();
        fs::create_dir(root.join("node_modules")).unwrap();
        fs::write(root.join("node_modules/dep.js"), "x").unwrap();

        let files = walk_dir(root).unwrap();
        let names: Vec<_> = files
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert!(names.contains(&"keep.rs".to_string()));
        assert!(!names.iter().any(|n| n == "junk.rs"), "target/ must be skipped");
        assert!(!names.iter().any(|n| n == "dep.js"), "node_modules/ must be skipped");
    }

    /// A symlink loop under the walked tree must not send the walk
    /// infinite. The walk must terminate and not descend the link.
    #[test]
    #[cfg(unix)]
    fn does_not_follow_symlink_loop() {
        use std::os::unix::fs::symlink;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        fs::write(root.join("real.rs"), "x").unwrap();
        let sub = root.join("sub");
        fs::create_dir(&sub).unwrap();
        // sub/loop -> root: following it would recurse forever.
        symlink(root, sub.join("loop")).unwrap();

        let files = walk_dir(root).unwrap(); // must terminate
        assert!(files.iter().any(|p| p.ends_with("real.rs")));
        assert!(
            !files.iter().any(|p| p.to_string_lossy().contains("loop")),
            "symlink must not be followed",
        );
    }
}
