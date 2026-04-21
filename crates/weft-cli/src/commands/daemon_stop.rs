use std::fs;

use super::Ctx;
use super::start::pid_file_path;

pub async fn run(_ctx: Ctx) -> anyhow::Result<()> {
    let pid_file = pid_file_path();
    if !pid_file.exists() {
        println!("dispatcher not running");
        return Ok(());
    }
    let pid: i32 = fs::read_to_string(&pid_file)?.trim().parse()?;
    signal_term(pid)?;
    let _ = fs::remove_file(&pid_file);
    println!("dispatcher stopped (pid {pid})");
    Ok(())
}

fn signal_term(pid: i32) -> anyhow::Result<()> {
    #[cfg(unix)]
    unsafe {
        extern "C" { fn kill(pid: i32, sig: i32) -> i32; }
        // SIGTERM = 15
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
