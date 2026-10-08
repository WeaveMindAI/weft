//! Records the commit this CLI is built from as `WEFT_CLI_COMMIT`, so every
//! call can tell the install which weft sent it (the install warns when it
//! runs another one). A build outside a git checkout (or without git) sets
//! nothing, and the CLI then sends no commit.

use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    let dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("cargo sets CARGO_MANIFEST_DIR"));
    let Some(commit) = git(&dir, &["rev-parse", "HEAD"]) else {
        return;
    };
    // Rebuild when HEAD moves: a checkout rewrites HEAD, a commit rewrites
    // the branch's ref (or `packed-refs` once refs are packed). `--git-path`
    // answers the right file in a worktree too, where `.git` is a file and
    // refs live in the main repository's folder.
    watch(&dir, "HEAD");
    watch(&dir, "packed-refs");
    if let Some(reference) = git(&dir, &["symbolic-ref", "-q", "HEAD"]) {
        if let Some(path) = git_path(&dir, &reference) {
            // A branch whose ref is only packed has no loose file yet; its
            // folder changes when the next commit writes one.
            if path.exists() {
                println!("cargo:rerun-if-changed={}", path.display());
            } else if let Some(parent) = path.parent().filter(|p| p.exists()) {
                println!("cargo:rerun-if-changed={}", parent.display());
            }
        }
    }
    println!("cargo:rustc-env=WEFT_CLI_COMMIT={commit}");
}

/// What a git command prints, trimmed; `None` when git is missing, fails,
/// or prints nothing.
fn git(dir: &Path, args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).current_dir(dir).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8(out.stdout).ok()?.trim().to_string();
    (!text.is_empty()).then_some(text)
}

/// The file git keeps `name` in, as an absolute path.
fn git_path(dir: &Path, name: &str) -> Option<PathBuf> {
    let path = PathBuf::from(git(dir, &["rev-parse", "--git-path", name])?);
    Some(if path.is_absolute() { path } else { dir.join(path) })
}

/// Rebuild when git's file `name` changes, when it exists (a missing file
/// would make cargo rerun this script on every build).
fn watch(dir: &Path, name: &str) {
    if let Some(path) = git_path(dir, name).filter(|p| p.exists()) {
        println!("cargo:rerun-if-changed={}", path.display());
    }
}
