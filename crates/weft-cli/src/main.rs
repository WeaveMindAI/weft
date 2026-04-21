//! The `weft` CLI. A thin HTTP client of the dispatcher plus a
//! front-end for `weft build`. Every lifecycle command maps to an
//! HTTP call. The CLI never owns execution state.

use clap::{Parser, Subcommand};

mod client;
mod commands;

#[derive(Debug, Parser)]
#[command(name = "weft", version, about = "Weft CLI")]
struct Cli {
    #[command(subcommand)]
    command: Cmd,

    /// Override the dispatcher URL. Defaults to the value in
    /// `weft.toml` or `http://localhost:9999`.
    #[arg(long, env = "WEFT_DISPATCHER_URL", global = true)]
    dispatcher: Option<String>,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Scaffold a new project (git init, main.weft, weft.toml).
    New { name: String },
    /// Compile the current project to a native rust binary.
    Build,
    /// Run the current project via the dispatcher. Streams logs until
    /// completion or suspension unless `--detach` is set.
    Run {
        #[arg(long)]
        detach: bool,
    },
    /// Subscribe to the dispatcher's SSE stream for a project or a
    /// specific execution color.
    Follow { target: String },
    /// Cancel an execution by color.
    Stop { color: String },
    /// Activate a registered project (mint trigger URLs).
    Activate { project: String },
    /// Deactivate a registered project (kill trigger URLs, cancel
    /// pending suspensions).
    Deactivate { project: String },
    /// List every project registered with the dispatcher.
    Ps,
    /// Unregister a project entirely (journal gone, logs gone).
    Rm { project: String },
    /// Tail historical + live logs for a project or execution.
    Logs { target: String },
    /// Manage the local dispatcher daemon (start, stop, status,
    /// restart, logs). The dispatcher is the long-lived process that
    /// owns projects, executions, and infra; `weft run` and the
    /// VS Code extension talk to it over HTTP.
    #[command(visible_alias = "d")]
    Daemon {
        #[command(subcommand)]
        action: DaemonAction,
    },
    /// Provision or tear down infra nodes for the current project.
    Infra {
        #[command(subcommand)]
        action: InfraAction,
    },
    /// Add an external node package (git-backed).
    Add { source: String },
    /// Print the per-project catalog as JSON (for Tangle, VS Code,
    /// dashboard introspection).
    DescribeNodes,
    /// Manage extension tokens (browser extension auth).
    Token {
        #[command(subcommand)]
        action: TokenAction,
    },
    /// List past executions for any project (newest first).
    Executions {
        #[arg(long, default_value_t = 50)]
        limit: u32,
    },
    /// Print a past execution's node events in order. Use for
    /// offline inspection; `weft replay <color>` drives the graph
    /// view animation.
    Events { color: String },
    /// Remove journal data. With no color: all executions older than
    /// `--keep` days (default 30). With a color: only that execution.
    Clean {
        #[arg(value_name = "color")]
        color: Option<String>,
        #[arg(long, default_value_t = 30)]
        keep_days: u32,
        #[arg(long, default_value_t = false)]
        all: bool,
    },
}

#[derive(Debug, Subcommand)]
enum TokenAction {
    /// Mint a new extension token.
    Mint {
        #[arg(long)]
        name: Option<String>,
    },
    /// List existing extension tokens.
    Ls,
    /// Revoke an extension token.
    Revoke { token: String },
}

impl From<TokenAction> for commands::token::TokenAction {
    fn from(value: TokenAction) -> Self {
        match value {
            TokenAction::Mint { name } => commands::token::TokenAction::Mint { name },
            TokenAction::Ls => commands::token::TokenAction::Ls,
            TokenAction::Revoke { token } => commands::token::TokenAction::Revoke { token },
        }
    }
}

#[derive(Debug, Subcommand)]
enum InfraAction {
    Up,
    Down,
}

#[derive(Debug, Subcommand)]
enum DaemonAction {
    /// Start the daemon in the background.
    Start,
    /// Stop the running daemon.
    Stop,
    /// Report whether the daemon is reachable.
    Status,
    /// Stop then start the daemon.
    Restart,
    /// Tail the daemon's stderr log.
    Logs {
        /// Number of lines to print.
        #[arg(long, default_value_t = 100)]
        tail: usize,
        /// Keep streaming new lines as the daemon writes them.
        #[arg(long, short = 'f', default_value_t = false)]
        follow: bool,
    },
}

impl From<InfraAction> for commands::infra::InfraAction {
    fn from(value: InfraAction) -> Self {
        match value {
            InfraAction::Up => commands::infra::InfraAction::Up,
            InfraAction::Down => commands::infra::InfraAction::Down,
        }
    }
}

impl From<DaemonAction> for commands::daemon::DaemonAction {
    fn from(value: DaemonAction) -> Self {
        match value {
            DaemonAction::Start => commands::daemon::DaemonAction::Start,
            DaemonAction::Stop => commands::daemon::DaemonAction::Stop,
            DaemonAction::Status => commands::daemon::DaemonAction::Status,
            DaemonAction::Restart => commands::daemon::DaemonAction::Restart,
            DaemonAction::Logs { tail, follow } => {
                commands::daemon::DaemonAction::Logs { tail, follow }
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let cli = Cli::parse();
    let ctx = commands::Ctx { dispatcher: cli.dispatcher };

    match cli.command {
        Cmd::New { name } => commands::new::run(ctx, name).await,
        Cmd::Build => commands::build::run(ctx).await,
        Cmd::Run { detach } => commands::run::run(ctx, detach).await,
        Cmd::Follow { target } => commands::follow::run(ctx, target).await,
        Cmd::Stop { color } => commands::stop::run(ctx, color).await,
        Cmd::Activate { project } => commands::activate::run(ctx, project).await,
        Cmd::Deactivate { project } => commands::deactivate::run(ctx, project).await,
        Cmd::Ps => commands::ps::run(ctx).await,
        Cmd::Rm { project } => commands::rm::run(ctx, project).await,
        Cmd::Logs { target } => commands::logs::run(ctx, target).await,
        Cmd::Daemon { action } => commands::daemon::run(ctx, action.into()).await,
        Cmd::Infra { action } => commands::infra::run(ctx, action.into()).await,
        Cmd::Add { source } => commands::add::run(ctx, source).await,
        Cmd::DescribeNodes => commands::describe_nodes::run(ctx).await,
        Cmd::Token { action } => commands::token::run(ctx, action.into()).await,
        Cmd::Executions { limit } => commands::executions::list(ctx, limit).await,
        Cmd::Events { color } => commands::executions::events(ctx, color).await,
        Cmd::Clean { color, keep_days, all } => {
            commands::executions::clean(ctx, color, keep_days, all).await
        }
    }
}
