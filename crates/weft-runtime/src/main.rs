//! `weft-runtime`: see the crate docs (`lib.rs`).

use std::path::PathBuf;

use clap::{Parser, Subcommand};

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    weft_core::net::install_crypto_provider();
    let args = Args::parse();
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        "weft_runtime=info,weft_dispatcher=info,weft_broker=info,weft_listener=info,weft_infra_supervisor=info,weft_platform_local=info,weft_platform_gcp=info".into()
    });
    match &args.command {
        // A local install's runtime keeps its own log, bounded; anywhere
        // else the platform collects stdout.
        Command::Serve { log: Some(path), .. } => {
            let file = weft_runtime::log_file::LogFile::open(path, weft_runtime::log_file::MAX_BYTES)?;
            tracing_subscriber::fmt().with_env_filter(filter).with_ansi(false).with_writer(file).init();
        }
        _ => tracing_subscriber::fmt().with_env_filter(filter).init(),
    }
    match args.command {
        Command::Serve { config, role, .. } => {
            weft_core::time_scale::announce();
            let config = weft_platform_traits::InstallConfig::load(&config)?;
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
