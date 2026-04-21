//! The weft-runner binary. One binary per weft install; it takes a
//! compiled ProjectDefinition at runtime plus a wake context and
//! runs the execution. The dispatcher spawns a runner per wake.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use tokio::sync::Notify;

use weft_core::ProjectDefinition;
use weft_runner::run_loop;
use weft_stdlib::StdlibCatalog;

#[derive(Debug, Parser)]
#[command(name = "weft-runner", version)]
struct Args {
    /// Path to the compiled project JSON.
    #[arg(long)]
    project: PathBuf,

    /// Color for this execution. New run = fresh color; resume = the
    /// suspended color we're resuming.
    #[arg(long)]
    color: Option<String>,

    /// Entry node id to kick off a fresh run. Mutually exclusive with
    /// --resume-node.
    #[arg(long)]
    entry_node: Option<String>,

    /// Payload for the entry node's first pulse (JSON).
    #[arg(long)]
    entry_payload: Option<String>,

    /// Node where a suspension is resuming.
    #[arg(long)]
    resume_node: Option<String>,

    /// Resume value (JSON) for the suspension primitive.
    #[arg(long)]
    resume_value: Option<String>,

    /// Dispatcher URL (for journal writes, cost reports, suspension
    /// tokens). If absent, the runner runs in "detached" mode: no
    /// journal, no suspensions. Pure programs complete and print
    /// output.
    #[arg(long, env = "WEFT_DISPATCHER_URL")]
    dispatcher: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "weft_runner=info,weft_core=info".into()),
        )
        .init();

    let args = Args::parse();
    let project: ProjectDefinition = {
        let raw = std::fs::read_to_string(&args.project)
            .with_context(|| format!("read {}", args.project.display()))?;
        serde_json::from_str(&raw).context("parse project json")?
    };

    let color = match args.color {
        Some(s) => s.parse::<uuid::Uuid>().context("color uuid")?,
        None => uuid::Uuid::new_v4(),
    };

    let entry_value = match args.entry_payload {
        Some(s) => serde_json::from_str(&s).context("entry payload json")?,
        None => serde_json::Value::Null,
    };

    let catalog = Arc::new(StdlibCatalog) as Arc<dyn weft_core::NodeCatalog>;
    let cancellation = Arc::new(Notify::new());

    run_loop(
        project,
        catalog,
        color,
        args.entry_node.as_deref(),
        entry_value,
        args.dispatcher.as_deref(),
        cancellation,
    )
    .await?;

    Ok(())
}
