//! `weft start`: boot the local dispatcher daemon if it isn't
//! already running. Uses a PID file under the weft data dir.

use std::fs;
use std::path::PathBuf;
use std::process::Stdio;

use anyhow::Context;

use super::Ctx;
use crate::client::resolve_dispatcher_url;

pub async fn run(ctx: Ctx) -> anyhow::Result<()> {
    let url = resolve_dispatcher_url(ctx.dispatcher.as_deref());
    let pid_file = pid_file_path();

    if pid_file.exists() {
        if let Ok(pid_str) = fs::read_to_string(&pid_file) {
            if let Ok(pid) = pid_str.trim().parse::<i32>() {
                if process_alive(pid) {
                    println!("dispatcher already running (pid {pid}) at {url}");
                    return Ok(());
                }
            }
        }
    }

    let binary = std::env::var("WEFT_DISPATCHER_PATH").unwrap_or_else(|_| "weft-dispatcher".to_string());
    let child = std::process::Command::new(binary)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("spawn weft-dispatcher")?;
    let pid = child.id();
    if let Some(parent) = pid_file.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&pid_file, pid.to_string())?;
    println!("dispatcher started (pid {pid}) at {url}");
    Ok(())
}

pub fn pid_file_path() -> PathBuf {
    let home = std::env::var_os("HOME").map(PathBuf::from).unwrap_or_default();
    home.join(".local/share/weft/dispatcher.pid")
}

fn process_alive(pid: i32) -> bool {
    // On unix, sending signal 0 checks the process exists without
    // affecting it. Portable enough for the local-dev use case.
    #[cfg(unix)]
    unsafe {
        extern "C" { fn kill(pid: i32, sig: i32) -> i32; }
        kill(pid, 0) == 0
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
}
