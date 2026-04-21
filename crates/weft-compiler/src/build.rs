//! Invoke cargo on the codegen output to produce the final binary.

use std::path::Path;
use std::process::Command;

use crate::error::{CompileError, CompileResult};

pub fn invoke_cargo(target_root: &Path, release: bool) -> CompileResult<()> {
    let mut cmd = Command::new("cargo");
    cmd.arg("build");
    if release {
        cmd.arg("--release");
    }
    cmd.current_dir(target_root);
    let status = cmd.status().map_err(CompileError::Io)?;
    if !status.success() {
        return Err(CompileError::Build(format!("cargo exited with {}", status)));
    }
    Ok(())
}
