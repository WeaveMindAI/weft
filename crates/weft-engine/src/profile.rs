//! A worker's own CPU profile, for measuring where its time goes: set
//! `WEFT_PROFILE` to a number of seconds and the worker samples every one of
//! its threads for that long from the moment it starts, then writes a flame
//! graph to `WEFT_PROFILE_OUT` (`/tmp/weft-worker-profile.svg` unless set).
//! Sampled with signals, so it needs no `perf` on the machine. Unset, nothing
//! is sampled.

/// The variable naming how many seconds to sample.
pub const PROFILE_ENV: &str = "WEFT_PROFILE";

/// Where the flame graph is written.
pub const PROFILE_OUT_ENV: &str = "WEFT_PROFILE_OUT";

const DEFAULT_OUT: &str = "/tmp/weft-worker-profile.svg";

/// How often each thread is sampled.
const SAMPLES_PER_SECOND: i32 = 997;

/// Start the profile `WEFT_PROFILE` asks for, on a task of its own. A value
/// that is not a number of seconds is refused, rather than run unprofiled.
pub fn start_from_env() -> anyhow::Result<()> {
    let Ok(secs) = std::env::var(PROFILE_ENV) else { return Ok(()) };
    let secs: u64 = secs.trim().parse().map_err(|e| anyhow::anyhow!("{PROFILE_ENV} is not a number of seconds: {e}"))?;
    let out = std::env::var(PROFILE_OUT_ENV).unwrap_or_else(|_| DEFAULT_OUT.to_string());
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(SAMPLES_PER_SECOND)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .map_err(|e| anyhow::anyhow!("the CPU profile could not start: {e}"))?;
    tracing::info!(target: "weft_engine::profile", secs, %out, "sampling this worker's CPU");
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
        let written = guard.report().build().map_err(anyhow::Error::from).and_then(|report| {
            let file = std::fs::File::create(&out)?;
            report.flamegraph(file)?;
            Ok(())
        });
        match written {
            Ok(()) => tracing::info!(target: "weft_engine::profile", %out, "the CPU profile is written"),
            Err(e) => tracing::error!(target: "weft_engine::profile", %out, error = %format!("{e:#}"), "the CPU profile could not be written"),
        }
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A worker started with `WEFT_PROFILE=1` writes its flame graph once the
    /// second is over; one asking for nonsense is refused.
    #[tokio::test]
    async fn a_profile_asked_for_is_written_when_its_time_is_up() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("profile.svg");
        // SAFETY: this test is the only one in this binary that sets these
        // variables, and it sets them before anything reads them.
        unsafe {
            std::env::set_var(PROFILE_ENV, "a while");
        }
        assert!(start_from_env().is_err(), "a duration that is not seconds is refused");
        unsafe {
            std::env::set_var(PROFILE_ENV, "1");
            std::env::set_var(PROFILE_OUT_ENV, &out);
        }
        start_from_env().expect("the profile starts");
        // Something to sample: a profile of a process doing nothing has no
        // stacks to draw.
        let busy = std::thread::spawn(|| {
            let until = std::time::Instant::now() + std::time::Duration::from_millis(1500);
            let mut x = 0u64;
            while std::time::Instant::now() < until {
                x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
            }
            x
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !out.exists() || std::fs::metadata(&out).unwrap().len() == 0 {
            assert!(std::time::Instant::now() < deadline, "the profile was never written");
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(std::fs::read_to_string(&out).unwrap().contains("<svg"));
        busy.join().unwrap();
    }
}

