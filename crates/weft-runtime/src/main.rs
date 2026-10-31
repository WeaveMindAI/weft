//! `weft-runtime`: see the crate docs (`lib.rs`).

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::Layer as _;

#[derive(Debug, Parser)]
#[command(name = "weft-runtime", version)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the install's roles: every role placed on the machine, or with
    /// `--role` the one serverless role this process is.
    Serve {
        /// The install config.
        #[arg(long, env = "WEFT_CONFIG")]
        config: PathBuf,
        /// Run only this role, placed in a process of its own.
        #[arg(long)]
        role: Option<String>,
        /// Write the log to this file, rolled over at a bounded size,
        /// instead of stdout.
        #[arg(long)]
        log: Option<PathBuf>,
    },
    /// The agent beside an infra unit.
    UnitAgent {
        #[command(subcommand)]
        command: AgentCommand,
    },
}

#[derive(Debug, Subcommand)]
enum AgentCommand {
    /// Hold the unit's network and answer its checks.
    Serve,
    /// Hand these directories to group `gid` (a unit's `fsGroup`).
    Own { gid: u32, paths: Vec<PathBuf> },
    /// Run the unit this cloud machine was given, and answer about it.
    Host,
}

// jemalloc hands freed memory back to the system, as it does for every
// worker (`weft_engine::Jemalloc`): with the system allocator a daemon
// that folded a few huge records kept every peak (gigabytes, until a
// restart).
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    weft_core::net::install_crypto_provider();
    let args = Args::parse();
    let config = match &args.command {
        Command::Serve { config, .. } => Some(weft_platform_traits::InstallConfig::load(config)?),
        Command::UnitAgent { .. } => None,
    };
    // Every weft crate at info, and the database libraries' own warnings
    // (`tokio_postgres`, and `deadpool`, which also covers
    // `deadpool_postgres`), never their per-statement lines. A slow wait
    // for a connection is the pools' own to count
    // (`weft_task_store::throttle`), said once a minute. The alert line is
    // the platform's to write (`platform::alert_lines`), never the
    // ordinary log's.
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "weft=info,tokio_postgres=warn,deadpool=warn".into())
        .add_directive(format!("{}=off", weft_platform_traits::ALERT_TARGET).parse()?);
    let lines = match &args.command {
        // A local install's runtime keeps its own log, bounded; anywhere
        // else the platform collects stdout.
        Command::Serve { log: Some(path), .. } => {
            let file = weft_runtime::log_file::LogFile::open(path, weft_runtime::log_file::MAX_BYTES)?;
            tracing_subscriber::fmt::layer().with_ansi(false).with_writer(file).boxed()
        }
        // Colours only on a terminal: a platform's log collector (Cloud
        // Run's) keeps the escape codes as text.
        _ => tracing_subscriber::fmt::layer().with_ansi(std::io::IsTerminal::is_terminal(&std::io::stdout())).boxed(),
    };
    tracing_subscriber::registry()
        .with(config.as_ref().and_then(weft_runtime::platform::alert_lines))
        .with(lines.with_filter(filter))
        .init();
    match args.command {
        Command::Serve { role, .. } => {
            weft_core::time_scale::announce();
            let config = config.expect("the config of `serve` is read above");
            let role = role.as_deref().map(weft_platform_traits::CoreRole::parse).transpose().map_err(anyhow::Error::msg)?;
            weft_runtime::serve(config, role).await
        }
        Command::UnitAgent { command } => match command {
            AgentCommand::Serve => weft_runtime::unit_agent::serve().await,
            AgentCommand::Own { gid, paths } => weft_runtime::unit_agent::own(gid, &paths),
            AgentCommand::Host => weft_runtime::unit_agent::host().await,
        },
    }
}
