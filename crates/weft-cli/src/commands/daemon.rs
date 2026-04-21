//! `weft daemon start|stop|status|restart|logs`. Lifecycle of the
//! local dispatcher, the long-lived HTTP daemon that owns projects,
//! executions, and infra. PID + log files live under
//! `~/.local/share/weft/`.

use std::fs::{self, OpenOptions};
use std::io::SeekFrom;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use anyhow::Context;
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, BufReader};
use tokio::time::sleep;

use super::Ctx;
use crate::client::resolve_dispatcher_url;

pub enum DaemonAction {
    Start,
    Stop,
    Status,
    Restart,
    Logs { tail: usize, follow: bool },
}

pub async fn run(ctx: Ctx, action: DaemonAction) -> anyhow::Result<()> {
    match action {
        DaemonAction::Start => start(&ctx).await,
        DaemonAction::Stop => stop().await,
        DaemonAction::Status => status(&ctx).await,
        DaemonAction::Restart => restart(&ctx).await,
        DaemonAction::Logs { tail, follow } => logs(tail, follow).await,
    }
}

pub fn data_dir() -> PathBuf {
    let home = std::env::var_os("HOME").map(PathBuf::from).unwrap_or_default();
    home.join(".local/share/weft")
}

pub fn pid_file_path() -> PathBuf {
    data_dir().join("dispatcher.pid")
}

pub fn log_file_path() -> PathBuf {
    data_dir().join("dispatcher.log")
}

async fn start(ctx: &Ctx) -> anyhow::Result<()> {
    let url = resolve_dispatcher_url(ctx.dispatcher.as_deref());
    let pid_file = pid_file_path();
    if let Some(pid) = read_pid(&pid_file) {
        if process_alive(pid) {
            println!("daemon already running (pid {pid}) at {url}");
            return Ok(());
        }
        let _ = fs::remove_file(&pid_file);
    }

    fs::create_dir_all(data_dir())?;
    let log_file = log_file_path();
    let out = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_file)
        .with_context(|| format!("open {}", log_file.display()))?;
    let err = out.try_clone()?;

    let binary = std::env::var("WEFT_DISPATCHER_PATH")
        .unwrap_or_else(|_| "weft-dispatcher".to_string());
    let child = std::process::Command::new(&binary)
        .stdin(Stdio::null())
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err))
        .spawn()
        .with_context(|| format!("spawn {binary}"))?;

    let pid = child.id();
    fs::write(&pid_file, pid.to_string())?;
    println!("daemon started (pid {pid}) at {url}");
    println!("logs: {}", log_file.display());
    Ok(())
}

async fn stop() -> anyhow::Result<()> {
    let pid_file = pid_file_path();
    let Some(pid) = read_pid(&pid_file) else {
        println!("daemon not running");
        return Ok(());
    };
    signal_term(pid)?;
    let _ = fs::remove_file(&pid_file);
    println!("daemon stopped (pid {pid})");
    Ok(())
}

async fn status(ctx: &Ctx) -> anyhow::Result<()> {
    let url = resolve_dispatcher_url(ctx.dispatcher.as_deref());
    let pid_file = pid_file_path();
    let pid = read_pid(&pid_file);
    let alive = pid.map(process_alive).unwrap_or(false);

    match ctx.client().get_json("/projects").await {
        Ok(value) => {
            let arr = value.as_array().cloned().unwrap_or_default();
            println!(
                "daemon: running (pid {}) at {url}, {} project(s)",
                pid.map(|p| p.to_string()).unwrap_or_else(|| "?".into()),
                arr.len()
            );
        }
        Err(e) => {
            if alive {
                println!(
                    "daemon: process alive (pid {}) but dispatcher unreachable at {url}: {e}",
                    pid.unwrap()
                );
            } else if pid.is_some() {
                println!("daemon: stale pid file at {}, process dead", pid_file.display());
            } else {
                println!("daemon: not running");
            }
        }
    }
    Ok(())
}

async fn restart(ctx: &Ctx) -> anyhow::Result<()> {
    stop().await?;
    // Give the OS a moment to release the port.
    sleep(Duration::from_millis(400)).await;
    start(ctx).await
}

async fn logs(tail: usize, follow: bool) -> anyhow::Result<()> {
    let log_file = log_file_path();
    if !log_file.exists() {
        println!("(no logs at {})", log_file.display());
        return Ok(());
    }

    // Read the last `tail` lines.
    let text = tokio::fs::read_to_string(&log_file).await?;
    let lines: Vec<&str> = text.lines().collect();
    let start = lines.len().saturating_sub(tail);
    for line in &lines[start..] {
        println!("{line}");
    }

    if !follow {
        return Ok(());
    }

    // Stream new data as it's appended. Reopen at current EOF and
    // poll with short sleeps; simpler than inotify and good enough
    // for local dev.
    let mut file = tokio::fs::OpenOptions::new()
        .read(true)
        .open(&log_file)
        .await?;
    file.seek(SeekFrom::End(0)).await?;
    let mut reader = BufReader::new(file);
    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            sleep(Duration::from_millis(400)).await;
            continue;
        }
        print!("{line}");
    }
}

fn read_pid(pid_file: &PathBuf) -> Option<i32> {
    fs::read_to_string(pid_file).ok()?.trim().parse().ok()
}

fn process_alive(pid: i32) -> bool {
    #[cfg(unix)]
    unsafe {
        extern "C" {
            fn kill(pid: i32, sig: i32) -> i32;
        }
        kill(pid, 0) == 0
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
}

fn signal_term(pid: i32) -> anyhow::Result<()> {
    #[cfg(unix)]
    unsafe {
        extern "C" {
            fn kill(pid: i32, sig: i32) -> i32;
        }
        if kill(pid, 15) != 0 {
            return Err(anyhow::anyhow!("kill SIGTERM failed"));
        }
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        anyhow::bail!("not supported on non-unix");
    }
    Ok(())
}
