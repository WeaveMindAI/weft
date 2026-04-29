//! The `weft` CLI. A thin HTTP client of the dispatcher plus a
//! front-end for `weft build`. Every lifecycle command maps to an
//! HTTP call. The CLI never owns execution state.

use clap::{Parser, Subcommand};

mod client;
mod commands;
pub mod images;

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
    /// Activate a project. Without a project id, discovers the cwd
    /// project, compiles + registers it first, then activates.
    Activate { project: Option<String> },
    /// Deactivate a registered project (kill trigger URLs, cancel
    /// pending suspensions).
    Deactivate { project: Option<String> },
    /// List every project registered with the dispatcher.
    Ps,
    /// Remove a project at the level you ask for. No flags → the
    /// cwd project is deactivated + unregistered on the
    /// dispatcher. Add flags to escalate: `--infra` terminates
    /// sidecars, `--journal` drops execution history,
    /// `--image` removes the worker image from docker + kind,
    /// `--local` wipes `.weft/target/` on the host, `--all`
    /// implies every flag. An explicit project id overrides the
    /// cwd discovery.
    Rm {
        #[arg(value_name = "project")]
        project: Option<String>,
        #[arg(long)]
        infra: bool,
        #[arg(long)]
        journal: bool,
        #[arg(long)]
        image: bool,
        #[arg(long)]
        local: bool,
        #[arg(long)]
        all: bool,
    },
    /// Tail logs. No arg → latest execution of the cwd project.
    /// UUID arg → that specific execution.
    Logs {
        #[arg(value_name = "color")]
        target: Option<String>,
    },
    /// Print a summary of the cwd project's current state.
    /// Registration, listener, infra per-node, recent executions.
    Status,
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
    /// Inspect every active listener: per-tenant, prints the
    /// journal's signal count alongside the listener's local
    /// registry. Drift between the two means cleanup went wrong.
    /// Operator command for diagnosing stuck listeners.
    Listener {
        #[command(subcommand)]
        action: ListenerAction,
    },
    /// Remove stale state. Default subject is the journal: with no
    /// flags, deletes executions older than --keep-days (30). Pass
    /// a positional UUID to target one execution. Other subjects
    /// are flag-driven and combinable.
    ///
    /// Subjects:
    ///   (default)         journal cleanup (executions older than --keep-days)
    ///   <UUID>            one execution
    ///   --images          dangling worker images for the cwd project
    ///                     (use --all to span every project)
    ///   --build-cache     docker buildkit cache prune
    ///   --all             with the journal subject: nuke every execution
    ///                     with --images: every project's images
    #[command(verbatim_doc_comment)]
    Clean {
        /// Single execution UUID to delete. Mutually exclusive with --images / --build-cache.
        #[arg(value_name = "color")]
        color: Option<String>,
        /// Bulk-delete journal cutoff in days.
        #[arg(long, default_value_t = 30)]
        keep_days: u32,
        /// Wipe ALL executions (with no other flags) OR span every project (with --images).
        #[arg(long, default_value_t = false)]
        all: bool,
        /// Reclaim dangling worker images. Cwd-scoped unless --all.
        #[arg(long, default_value_t = false)]
        images: bool,
        /// Prune docker BuildKit cache (heavy: invalidates cargo dep cache).
        #[arg(long, default_value_t = false)]
        build_cache: bool,
    },
}

#[derive(Debug, Subcommand)]
enum TokenAction {
    /// Mint a new extension token. Default token shape is the
    /// friendly `wm_tk_<adj>-<noun>-<NN>` (e.g. `wm_tk_swift-
    /// falcon-23`); pass `--hard` for a uuid-backed token when
    /// the dispatcher is exposed beyond localhost.
    Mint {
        /// Optional human label, separate from the token itself.
        /// Shown by `weft token ls`. Defaults to the token's own
        /// readable suffix when omitted.
        #[arg(long)]
        name: Option<String>,
        /// Use a high-entropy token (`wm_tk_<32-hex>`) instead of
        /// the friendly default. Pick this when the dispatcher
        /// isn't on localhost-only.
        #[arg(long)]
        hard: bool,
    },
    /// List existing extension tokens.
    Ls,
    /// Revoke an extension token.
    Revoke { token: String },
}

impl From<TokenAction> for commands::token::TokenAction {
    fn from(value: TokenAction) -> Self {
        match value {
            TokenAction::Mint { name, hard } => commands::token::TokenAction::Mint { name, hard },
            TokenAction::Ls => commands::token::TokenAction::Ls,
            TokenAction::Revoke { token } => commands::token::TokenAction::Revoke { token },
        }
    }
}

#[derive(Debug, Subcommand)]
enum InfraAction {
    /// Provision the project's sidecars (first time) or scale them
    /// back to 1 (after stop). Errors if infra is already running.
    Start,
    /// Scale sidecars to 0 replicas. PVC + Service stay so the
    /// next `start` resumes the same instance with its persisted
    /// state (auth, credentials, etc).
    Stop,
    /// Delete every k8s resource the sidecars own, PVC included.
    /// Irreversible: the next `start` is a fresh provision.
    Terminate,
    /// Print the current lifecycle state of each infra node.
    Status,
}

#[derive(Debug, Subcommand)]
enum ListenerAction {
    /// Pretty-print every active listener: tenant, journal signal
    /// count, listener registry. Drift highlights where cleanup
    /// went wrong.
    Inspect,
}

#[derive(Debug, Subcommand)]
enum DaemonAction {
    /// Start the daemon. Ensures the kind cluster, ingress, images,
    /// and dispatcher Deployment exist, then opens a port-forward
    /// so the CLI can talk to it on localhost.
    Start {
        /// Force-rebuild the dispatcher and listener images.
        #[arg(long)]
        rebuild: bool,
    },
    /// Stop the running daemon. Scales the dispatcher Deployment to
    /// 0 and tears down the local port-forward. The kind cluster
    /// and persistent state stay intact.
    Stop,
    /// Report whether the daemon is reachable.
    Status,
    /// Stop then start the daemon.
    Restart {
        #[arg(long)]
        rebuild: bool,
    },
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
            InfraAction::Start => commands::infra::InfraAction::Start,
            InfraAction::Stop => commands::infra::InfraAction::Stop,
            InfraAction::Terminate => commands::infra::InfraAction::Terminate,
            InfraAction::Status => commands::infra::InfraAction::Status,
        }
    }
}

impl From<DaemonAction> for commands::daemon::DaemonAction {
    fn from(value: DaemonAction) -> Self {
        match value {
            DaemonAction::Start { rebuild } => commands::daemon::DaemonAction::Start { rebuild },
            DaemonAction::Stop => commands::daemon::DaemonAction::Stop,
            DaemonAction::Status => commands::daemon::DaemonAction::Status,
            DaemonAction::Restart { rebuild } => commands::daemon::DaemonAction::Restart { rebuild },
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
        Cmd::Rm { project, infra, journal, image, local, all } => {
            commands::rm::run(
                ctx,
                commands::rm::RmArgs { project, infra, journal, image, local, all },
            )
            .await
        }
        Cmd::Logs { target } => commands::logs::run(ctx, target).await,
        Cmd::Status => commands::status::run(ctx).await,
        Cmd::Daemon { action } => commands::daemon::run(ctx, action.into()).await,
        Cmd::Infra { action } => commands::infra::run(ctx, action.into()).await,
        Cmd::Add { source } => commands::add::run(ctx, source).await,
        Cmd::DescribeNodes => commands::describe_nodes::run(ctx).await,
        Cmd::Token { action } => commands::token::run(ctx, action.into()).await,
        Cmd::Executions { limit } => commands::executions::list(ctx, limit).await,
        Cmd::Events { color } => commands::executions::events(ctx, color).await,
        Cmd::Listener { action } => match action {
            ListenerAction::Inspect => commands::listener::inspect(ctx).await,
        },
        Cmd::Clean { color, keep_days, all, images, build_cache } => {
            commands::executions::clean(ctx, color, keep_days, all, images, build_cache).await
        }
    }
}
