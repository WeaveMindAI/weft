//! The `weft` CLI. A thin HTTP client of the dispatcher plus a
//! front-end for `weft build`. Every lifecycle command maps to an
//! HTTP call. The CLI never owns execution state.

use clap::{Parser, Subcommand};

mod client;
mod commands;
pub mod images;
pub mod progress;
pub mod prompt;

#[derive(Debug, Parser)]
#[command(name = "weft", version, about = "Weft CLI")]
struct Cli {
    #[command(subcommand)]
    command: Cmd,

    /// Override the dispatcher URL. Defaults to the value in
    /// `weft.toml` or `http://localhost:9999`.
    #[arg(long, env = "WEFT_DISPATCHER_URL", global = true)]
    dispatcher: Option<String>,

    /// Emit JSON progress events to stdout (one object per line)
    /// instead of human-readable output. Used by the VS Code
    /// extension to drive its action bar from CLI output. Each
    /// line is a {"phase": ..., "detail": ...} object.
    #[arg(long, global = true)]
    json: bool,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Scaffold a new project (git init, main.weft, weft.toml).
    New { name: String },
    /// Compile the current project to a native rust binary.
    Build,
    /// Build (if stale) the shared worker builder-base image and print its
    /// content-addressed tag. The base bakes the precompiled engine + deps that
    /// every per-project worker build reuses; this is the same base-ensure a
    /// `weft build` runs, exposed so a cluster setup can build + load it into its
    /// registry. `--quiet` prints ONLY the tag (for scripting).
    BuildBase {
        #[arg(long)]
        quiet: bool,
    },
    /// Run the current project via the dispatcher. Streams logs until
    /// completion or suspension unless `--detach` is set.
    ///
    /// Rebuilding while executions are in flight is non-disruptive:
    /// in-flight work finishes on workers baked from its own image,
    /// and this run lands on a fresh current-image worker.
    Run {
        #[arg(long)]
        detach: bool,
    },
    /// Subscribe to the dispatcher's SSE stream for a project.
    Follow { project: String },
    /// Cancel an execution by color.
    Stop { color: String },
    /// Activate a project. Without a project id, discovers the cwd
    /// project, compiles + registers it first, then activates.
    ///
    /// `--reactivate-choice` is forwarded to the dispatcher when the
    /// project is in hibernate/park: one of
    /// `execute_parked_keep_suspended`, `keep_suspended_only`,
    /// `wipe_all`. Without it the human-terminal prompt fires;
    /// `--json` mode requires it explicitly when there is preserved
    /// state.
    Activate {
        project: Option<String>,
        #[arg(long = "reactivate-choice", value_name = "choice")]
        reactivate_choice: Option<String>,
    },
    /// Deactivate a registered project. By default WIPEs (drops
    /// signals + cancels suspended runs); pass `--mode hibernate`
    /// or `--mode park` to preserve in-flight HumanQuery work
    /// across the inactive window.
    ///
    /// `--running-policy` controls how in-flight executions are
    /// handled: `wait` (default) leaves running executions to
    /// drain, parking new fires meanwhile; `cancel` kills running
    /// executions and flips the project straight to inactive.
    Deactivate {
        project: Option<String>,
        #[command(flatten)]
        opts: TriggerDeactivationOpts,
    },
    /// Cancel an in-flight `activate` (status=Activating). Wipes
    /// every signal row registered so far, cancels the
    /// TriggerSetup color, flips the project to Inactive. 412 if
    /// the project isn't Activating.
    #[command(name = "cancel-activate")]
    CancelActivate {
        project: Option<String>,
    },
    /// Cancel an in-flight build (transition=building).
    /// The dispatcher pod driving the build interrupts the builder
    /// job; the verb that was building errs "cancelled". 412 if no
    /// build is in flight.
    #[command(name = "cancel-build")]
    CancelBuild {
        project: Option<String>,
    },
    /// Force-cancel running executions while a deactivate-with-wait
    /// is draining. Idempotent: if the project isn't currently in
    /// `deactivating`, this is a no-op.
    #[command(name = "cancel-running")]
    CancelRunning {
        project: Option<String>,
    },
    /// Atomic deactivate-then-activate against a fresh worker image.
    /// Used after editing the trigger or fire subgraph: drops live
    /// signals, rebuilds if needed, re-registers everything against
    /// the new binary in one shot.
    Resync {
        /// Trigger-deactivation choice (mode / grace / running-policy /
        /// drain cap): resync deactivates with YOUR spec before
        /// re-registering, exactly like the standalone Deactivate.
        /// Missing flags prompt on a TTY or error in `--json`.
        #[command(flatten)]
        opts: TriggerDeactivationOpts,
    },
    /// List every project registered with the dispatcher.
    Ps,
    /// Remove a project at the level you ask for. No flags → the
    /// cwd project is deactivated + unregistered on the
    /// dispatcher. Add flags to escalate: `--infra` terminates
    /// infra pods, `--journal` drops execution history,
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
        /// Skip the supervisor terminate-wait window. Use when the
        /// supervisor pod is wedged or the cluster is unreachable
        /// and the user wants the project gone NOW.
        #[arg(long)]
        force: bool,
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
    /// Print the per-project catalog as JSON (for editor / tooling
    /// introspection).
    DescribeNodes {
        /// Describe the bundled stdlib catalog instead of a project's
        /// `nodes/`. Needs no project on disk; used to produce the browser
        /// parser's catalog asset.
        #[arg(long)]
        stdlib: bool,
    },
    /// Parse weft source (read from stdin) against the project's
    /// `nodes/` catalog and print the project + referenced catalog +
    /// diagnostics as JSON. The editor's live graph feedback. Lenient:
    /// unknown node types become placeholders rather than errors.
    Parse {
        /// Path of the source file (the stdin content's origin). Its
        /// directory is the base for `@file`/`@include` resolution, so
        /// relative paths resolve against the file's own location, not the
        /// project root. Omit when parsing a detached buffer.
        #[arg(long)]
        file: Option<std::path::PathBuf>,
    },
    /// Validate weft source (read from stdin) against the project's
    /// `nodes/` catalog and print diagnostics as JSON. The editor's
    /// Problems-panel feedback. Strict: the full compile + enrich +
    /// validate pipeline.
    Validate {
        /// Path of the source file (see `parse --file`).
        #[arg(long)]
        file: Option<std::path::PathBuf>,
    },
    /// Long-lived parse server for the editor: reads one JSON request per
    /// line on stdin, writes one JSON response per line on stdout. Holds the
    /// node catalog warm in memory so each parse/validate is parse-cost, not
    /// catalog-discovery cost. The VS Code extension spawns one on activate
    /// and kills it on deactivate. Serves both `parse` and `validate` kinds.
    #[command(name = "parse-server")]
    ParseServer,
    /// Manage the project's base node catalog (the stdlib mirror
    /// under `nodes/base_catalog/`).
    Catalog {
        #[command(subcommand)]
        action: CatalogAction,
    },
    /// Manage signal tokens: scoped credentials that let an external
    /// client listen for + reply to a project's waiting nodes.
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
    /// Browse + manage stored files (the tenant's runtime storage):
    /// project files, shared spaces, past-execution survivors.
    Files {
        #[command(subcommand)]
        action: FilesAction,
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
enum CatalogAction {
    /// Re-sync `nodes/base_catalog/` from the installed weft's
    /// bundled catalog. Wipes and recopies: picks up edited node
    /// source, added nodes, and removed nodes. Your own nodes
    /// (anywhere else under `nodes/`) are untouched. Anything you
    /// edited in place under `base_catalog/` IS overwritten; copy a
    /// node out of `base_catalog/` first if you want to keep changes.
    Update,
}

#[derive(Debug, Subcommand)]
enum TokenAction {
    /// Mint a new signal token. A signal token grants scoped access
    /// to the dispatcher's signal enumeration + reply surface. All
    /// scope flags are optional; an unscoped token sees every signal
    /// in the tenant.
    Mint {
        /// Optional human label, pure metadata (never part of the
        /// token value). Shown by `weft token ls`.
        #[arg(long)]
        name: Option<String>,
        /// Restrict to specific project ids. Repeat for multiple.
        /// Empty = any project in the tenant.
        #[arg(long, value_name = "uuid")]
        projects: Vec<String>,
        /// Restrict to signals carrying any of these tags. Repeat
        /// for multiple. Empty = any tag (including untagged).
        /// Tag charset: [A-Za-z0-9_-]{1,64}.
        #[arg(long, value_name = "tag")]
        tags: Vec<String>,
    },
    /// List existing signal tokens (metadata + recognizer; the full
    /// value is shown only once, at mint).
    Ls,
    /// Revoke a signal token by its id (from mint output or `ls`).
    Revoke { id: String },
}

impl From<TokenAction> for commands::token::TokenAction {
    fn from(value: TokenAction) -> Self {
        match value {
            TokenAction::Mint { name, projects, tags } => {
                commands::token::TokenAction::Mint { name, projects, tags }
            }
            TokenAction::Ls => commands::token::TokenAction::Ls,
            TokenAction::Revoke { id } => commands::token::TokenAction::Revoke { id },
        }
    }
}

/// Shared trigger-deactivation flags, used by every verb that takes
/// triggers down (the standalone `weft deactivate` and every infra
/// verb that deactivates as a side effect: stop, terminate, upgrade).
///
/// Missing flags prompt the user on a TTY; in `--json` mode missing
/// required flags become errors so the extension always passes them.
#[derive(Debug, clap::Args, Default, Clone)]
struct TriggerDeactivationOpts {
    /// Preservation mode for active triggers: wipe | hibernate | park.
    #[arg(long, value_name = "wipe|hibernate|park")]
    mode: Option<String>,
    /// Hibernate grace window in minutes (only meaningful with
    /// --mode hibernate). Default 15.
    #[arg(long, value_name = "minutes")]
    grace: Option<u32>,
    /// What to do with in-flight executions: wait | cancel.
    #[arg(long = "running-policy", value_name = "wait|cancel")]
    running_policy: Option<String>,
    /// Cap in seconds on a `--running-policy wait` drain ("wait at
    /// most N, then proceed anyway": the deactivation cancels the
    /// stragglers; a worker replacement kills them with the old
    /// workers). Default: the server's 600s.
    #[arg(long = "drain-timeout", value_name = "seconds")]
    drain_timeout: Option<u64>,
}

/// The bare drain cap for `weft infra start` (which takes no
/// trigger-deactivation flags: infra start fires when the project is
/// inactive, but its worker reconciliation can still drain).
#[derive(Debug, clap::Args)]
struct DrainOpts {
    /// Cap in seconds on a `--running-policy wait` drain before the
    /// operation proceeds anyway. Default: the server's 600s.
    #[arg(long = "drain-timeout", value_name = "seconds")]
    drain_timeout: Option<u64>,
}

#[derive(Debug, Subcommand)]
enum InfraAction {
    /// Run the InfraSetup subworkflow: per-node either skip-apply
    /// (hash match) or fresh apply. Use when starting infra from
    /// scratch.
    Start {
        #[command(flatten)]
        drain: DrainOpts,
    },
    /// Re-apply against current images / sources (stop then start).
    /// When the project is Active, triggers deactivate (same picker as
    /// `weft deactivate`) for the duration. The project is left
    /// deactivated afterward; click Activate when ready.
    Upgrade {
        #[command(flatten)]
        opts: TriggerDeactivationOpts,
    },
    /// Scale infra workloads to 0 (PVCs preserved). When the project
    /// is Active, triggers deactivate via the standard picker.
    Stop {
        #[command(flatten)]
        opts: TriggerDeactivationOpts,
    },
    /// Delete every infra resource (PVCs included unless preserved by
    /// the node's InfraSpec). When the project is Active, triggers
    /// deactivate via the standard picker.
    Terminate {
        #[command(flatten)]
        opts: TriggerDeactivationOpts,
    },
    /// Print the current lifecycle state of each infra node.
    Status,
    /// Cancel in-flight infra work: halt claimed lifecycle commands
    /// (the supervisor stops between kubectl steps), cancel unclaimed
    /// ones outright, interrupt the provisioning execution. HALT, not
    /// rollback: per-node partial state stays visible; terminate or
    /// retry per-node from where it stopped. 412 if nothing is in
    /// flight.
    Cancel,
    /// Per-node stop. Targets one infra node by id, leaves the rest
    /// of the project's infra untouched. Used from the graph's per-
    /// node menu (the trash icon's siblings).
    NodeStop {
        #[arg(value_name = "node_id")]
        node_id: String,
        /// Force scale-to-zero every unit, ignoring each unit's
        /// `on_stop`. Takes down units that would normally stay up
        /// (NoOp) so you can update them on the next start. You accept
        /// the downtime (and any slow re-warmup) by passing this.
        #[arg(long)]
        force: bool,
    },
    /// Per-node terminate. Same scope as `node-stop` but deletes
    /// resources instead of scaling to 0.
    NodeTerminate {
        #[arg(value_name = "node_id")]
        node_id: String,
    },
}

#[derive(Debug, Subcommand)]
enum ListenerAction {
    /// Pretty-print every active listener: tenant, journal signal
    /// count, listener registry. Drift highlights where cleanup
    /// went wrong.
    Inspect,
}

#[derive(Debug, Subcommand)]
enum FilesAction {
    /// List stored files, organized by space (project files, shared
    /// spaces, past-execution survivors). Optional prefix filter.
    Ls {
        #[arg(value_name = "prefix")]
        prefix: Option<String>,
    },
    /// Show one file's full metadata.
    Inspect { key: String },
    /// Download a file: handshake with the dispatcher for a presigned URL,
    /// then stream the bytes DIRECTLY from the storage bucket.
    Download {
        key: String,
        /// Output path; defaults to the stored filename.
        #[arg(short, long)]
        output: Option<String>,
    },
    /// Remove a file (full key) or a whole space (prefix ending in `/`).
    Rm {
        #[arg(value_name = "key-or-space/")]
        target: String,
        /// Delete without the interactive confirmation. Required to
        /// delete non-interactively (piped stdin), since a prefix wipe
        /// can remove kept files you deliberately persisted.
        #[arg(short, long)]
        yes: bool,
    },
    /// Stored bytes + file count of the tenant's runtime storage.
    Usage,
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

impl InfraAction {
    fn split(self) -> (commands::infra::InfraAction, commands::infra::InfraOpts) {
        let opts_from = |t: TriggerDeactivationOpts| commands::infra::InfraOpts {
            mode: t.mode,
            grace: t.grace,
            running_policy: t.running_policy,
            drain_timeout: t.drain_timeout,
        };
        match self {
            InfraAction::Start { drain } => (
                commands::infra::InfraAction::Start,
                commands::infra::InfraOpts {
                    drain_timeout: drain.drain_timeout,
                    ..Default::default()
                },
            ),
            InfraAction::Stop { opts } => {
                (commands::infra::InfraAction::Stop, opts_from(opts))
            }
            InfraAction::Terminate { opts } => {
                (commands::infra::InfraAction::Terminate, opts_from(opts))
            }
            InfraAction::Upgrade { opts } => {
                (commands::infra::InfraAction::Upgrade, opts_from(opts))
            }
            InfraAction::Status => (commands::infra::InfraAction::Status, Default::default()),
            InfraAction::Cancel => (commands::infra::InfraAction::Cancel, Default::default()),
            InfraAction::NodeStop { node_id, force } => (
                commands::infra::InfraAction::NodeStop { node_id, force },
                Default::default(),
            ),
            InfraAction::NodeTerminate { node_id } => (
                commands::infra::InfraAction::NodeTerminate { node_id },
                Default::default(),
            ),
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
    // Provider keys (`<PROVIDER>_API_KEY`) and local overrides come from
    // the nearest `.env` up from the invoking directory. Real env vars
    // win over the file; no file is normal; a malformed file fails loud.
    match dotenvy::dotenv() {
        Ok(_) => {}
        Err(e) if e.not_found() => {}
        Err(e) => anyhow::bail!("failed to load .env: {e}"),
    }

    // Logs go to stderr so stdout stays a clean channel for
    // machine-readable output (notably `--json`). The extension
    // reads stdout-only and parses JSON; without this, tracing
    // warnings prepend themselves and the parse explodes.
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    let ctx = commands::Ctx::new(cli.dispatcher, cli.json);

    match cli.command {
        Cmd::New { name } => commands::new::run(ctx, name).await,
        Cmd::Build => commands::build::run(ctx).await,
        Cmd::BuildBase { quiet } => commands::build::run_build_base(quiet).await,
        Cmd::Run { detach } => commands::run::run(ctx, detach).await,
        Cmd::Follow { project } => commands::follow::run(ctx, project).await,
        Cmd::Stop { color } => commands::stop::run(ctx, color).await,
        Cmd::Activate { project, reactivate_choice } => {
            commands::activate::run(ctx, project, reactivate_choice).await
        }
        Cmd::Deactivate { project, opts } => {
            commands::deactivate::run(
                ctx,
                project,
                opts.mode,
                opts.grace,
                opts.running_policy,
                opts.drain_timeout,
            )
            .await
        }
        Cmd::CancelActivate { project } => {
            commands::cancel_activate::run(ctx, project).await
        }
        Cmd::CancelBuild { project } => {
            commands::cancel_build::run(ctx, project).await
        }
        Cmd::CancelRunning { project } => {
            commands::cancel_running::run(ctx, project).await
        }
        Cmd::Resync { opts } => {
            commands::resync::run(
                ctx,
                commands::infra::InfraOpts {
                    mode: opts.mode,
                    grace: opts.grace,
                    running_policy: opts.running_policy,
                    drain_timeout: opts.drain_timeout,
                },
            )
            .await
        }
        Cmd::Ps => commands::ps::run(ctx).await,
        Cmd::Rm { project, infra, journal, image, local, all, force } => {
            commands::rm::run(
                ctx,
                commands::rm::RmArgs { project, infra, journal, image, local, all, force },
            )
            .await
        }
        Cmd::Logs { target } => commands::logs::run(ctx, target).await,
        Cmd::Status => commands::status::run(ctx).await,
        Cmd::Daemon { action } => commands::daemon::run(ctx, action.into()).await,
        Cmd::Infra { action } => {
            let (verb, opts) = action.split();
            commands::infra::run(ctx, verb, opts).await
        }
        Cmd::Add { source } => commands::add::run(ctx, source).await,
        Cmd::DescribeNodes { stdlib } => commands::describe_nodes::run(ctx, stdlib).await,
        Cmd::Parse { file } => commands::parse::parse(file).await,
        Cmd::Validate { file } => commands::parse::validate(ctx, file).await,
        Cmd::ParseServer => commands::parse::serve(ctx).await,
        Cmd::Catalog { action } => match action {
            CatalogAction::Update => commands::catalog::update(ctx).await,
        },
        Cmd::Token { action } => commands::token::run(ctx, action.into()).await,
        Cmd::Executions { limit } => commands::executions::list(ctx, limit).await,
        Cmd::Events { color } => commands::executions::events(ctx, color).await,
        Cmd::Listener { action } => match action {
            ListenerAction::Inspect => commands::listener::inspect(ctx).await,
        },
        Cmd::Files { action } => match action {
            FilesAction::Ls { prefix } => commands::files::ls(ctx, prefix).await,
            FilesAction::Inspect { key } => commands::files::inspect(ctx, key).await,
            FilesAction::Download { key, output } => {
                commands::files::download(ctx, key, output).await
            }
            FilesAction::Rm { target, yes } => commands::files::rm(ctx, target, yes).await,
            FilesAction::Usage => commands::files::usage(ctx).await,
        },
        Cmd::Clean { color, keep_days, all, images, build_cache } => {
            commands::executions::clean(ctx, color, keep_days, all, images, build_cache).await
        }
    }
}
