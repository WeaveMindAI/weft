//! The weft-runner binary. One binary per weft install; it takes a
//! compiled ProjectDefinition at runtime plus a wake context and
//! runs the execution. The dispatcher spawns a runner per wake.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use tokio::sync::Notify;

use weft_core::ProjectDefinition;
use weft_runner::{run_loop, EntryMode, LoopOutcome};
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

    /// Entry node for a fresh run or resume.
    #[arg(long)]
    entry_node: Option<String>,

    /// Entry payload (JSON) for a fresh run.
    #[arg(long)]
    entry_payload: Option<String>,

    /// Resume value (JSON) for a suspended run.
    #[arg(long)]
    resume_value: Option<String>,

    /// Dispatcher URL (for cost reports, suspension tokens, status
    /// reporting, log shipping).
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

    let (entry_value, entry_mode) = match (args.resume_value, args.entry_payload) {
        (Some(s), _) => (
            serde_json::from_str(&s).context("resume value json")?,
            EntryMode::Resume,
        ),
        (None, Some(s)) => (
            serde_json::from_str(&s).context("entry payload json")?,
            EntryMode::Fresh,
        ),
        (None, None) => (serde_json::Value::Null, EntryMode::Fresh),
    };

    let catalog = Arc::new(StdlibCatalog) as Arc<dyn weft_core::NodeCatalog>;
    let cancellation = Arc::new(Notify::new());

    let http = reqwest::Client::new();
    if let (Some(dispatcher), Some(entry)) = (&args.dispatcher, &args.entry_node) {
        let url = format!("{dispatcher}/executions/{color}/status");
        let body = serde_json::json!({
            "kind": "started",
            "entry_node": entry,
        });
        let _ = http.post(&url).json(&body).send().await;
    }

    let outcome = run_loop(
        project,
        catalog,
        color,
        args.entry_node.as_deref(),
        entry_value,
        entry_mode,
        args.dispatcher.as_deref(),
        cancellation,
    )
    .await?;

    if let Some(dispatcher) = &args.dispatcher {
        let url = format!("{dispatcher}/executions/{color}/status");
        let body = match &outcome {
            LoopOutcome::Completed { outputs } => serde_json::json!({ "kind": "completed", "outputs": outputs }),
            LoopOutcome::Failed { error } => serde_json::json!({ "kind": "failed", "error": error }),
            LoopOutcome::Stuck => serde_json::json!({ "kind": "failed", "error": "execution stuck: pending pulses with no ready nodes" }),
            LoopOutcome::Suspended { .. } => {
                // No terminal status emitted on suspend; the resume
                // worker reports whatever happens next.
                return Ok(());
            }
        };
        let _ = http.post(&url).json(&body).send().await;
    }

    Ok(())
}
