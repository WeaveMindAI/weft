//! The `weft` CLI. A thin HTTP client of the dispatcher plus a
//! front-end for `weft build`. Every lifecycle command maps to an
//! HTTP call. The CLI never owns execution state.

use clap::{Parser, Subcommand};

mod client;
mod commands;
pub mod images;
pub mod progress;
pub mod prompt;
pub mod user_config;

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
    /// Scaffold a new project (git init, main.weft, weft.toml). With
    /// `--assistant <name>` also install the Tangle assistant persona for
    /// that AI coding assistant, symlinked from this weft checkout's
    /// `tangle/<name>/`, so a later `git pull` + `./setup.sh` of the
    /// checkout refreshes Tangle in every such project at once. The choice
    /// is remembered: later runs of `weft new` install the same assistants
    /// with no flag; `--assistant none` clears it.
    New {
        name: String,
        /// Install the Tangle persona for this AI coding assistant
        /// (e.g. `claude-code`/`cc` or `kilo-code`/`kc`), symlinked from the weft
        /// checkout so updating weft updates Tangle. Repeatable, and
        /// remembered as the default for future projects; `none` opts out.
        #[arg(long = "assistant", value_name = "NAME")]
        assistants: Vec<String>,
    },
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
    /// Make every shared image exist locally: the four system images
    /// (dispatcher, listener, broker, infra-supervisor) plus the worker
    /// builder-base, each under its content-addressed registry ref
    /// (present -> pull -> build). `--push` then pushes every ref to
    /// its registry; the release workflow runs this on each push to the
    /// release branch, which is what lets every install pull instead of
    /// compile.
    #[command(name = "build-images")]
    BuildImages {
        /// Push each ensured ref to its registry (requires a prior
        /// `docker login`).
        #[arg(long, conflicts_with = "push_suffix")]
        push: bool,
        /// Push each ref under `<ref><suffix>` instead of its bare
        /// name (implies --push). The release workflow builds each
        /// architecture on its native runner with `-amd64` / `-arm64`
        /// here, then stitches the bare ref into one multi-arch
        /// manifest list, so every machine pulls its own architecture.
        /// `allow_hyphen_values`: the suffixes start with a hyphen
        /// (`-amd64`), which clap would otherwise read as flags.
        #[arg(long, value_name = "SUFFIX", allow_hyphen_values = true)]
        push_suffix: Option<String>,
        /// Only print the refs this tree resolves to, one per line,
        /// touching nothing (no ensure, no docker). What setup.sh
        /// keys its engine-change sweep on.
        #[arg(long, conflicts_with_all = ["push", "push_suffix"])]
        print: bool,
    },
    /// Manage a node's service connection from the terminal: list the
    /// stored connections and pick one, connect a new account (paste a
    /// key, browser sign-in, shared app), upgrade or forget one, or
    /// disconnect the node. Interactive by default; every choice has a
    /// flag for scripts.
    Connect {
        #[command(flatten)]
        opts: commands::connect::ConnectOpts,
    },
    /// Run node self-tests. Without a target: every package. Without
    /// --tier: the basic + fake tiers (compiled + run locally, no
    /// cluster needed). With a package name or node type: just that
    /// scope. `--tier live` adds the live tier: real credentials
    /// through the production access path, as a test pod in the
    /// cluster; it can spend money, so it confirms first (persist
    /// "don't ask again" when prompted, or pass --yes).
    #[command(name = "test-node")]
    TestNode {
        /// A package name or a node type; absent = every package.
        target: Option<String>,
        /// Run only the test with this name.
        #[arg(long)]
        test: Option<String>,
        /// Tiers to run (repeatable). Default: basic and fake.
        #[arg(long = "tier", value_enum)]
        tiers: Vec<commands::test_node::TierArg>,
        /// Live: paste a throwaway key for this service (repeatable);
        /// each field is read from `WEFT_NODE_TEST_<SERVICE>_<FIELD>`
        /// when set, else prompted on stdin, never an argument. The
        /// grant is deleted afterwards.
        #[arg(long, value_name = "service")]
        key: Vec<String>,
        /// Live: use this exact connection (grant) id, as
        /// `<service>=<grant id>` (repeatable; a bare id works when
        /// the selected tests need exactly one service).
        #[arg(long, value_name = "service=grant-id")]
        connection: Vec<String>,
        /// Skip the live-run confirmation for this invocation.
        #[arg(long)]
        yes: bool,
        /// Run tests concurrently: bare `--parallel` runs everything at
        /// once, `--parallel N` caps in-flight tests at N. Applies to
        /// the local tiers and to live pod runs alike.
        #[arg(long, num_args = 0..=1, default_missing_value = "0", value_name = "N")]
        parallel: Option<usize>,
    },
    /// Print the content hash naming a package's node-test OUTCOME
    /// inputs: the package's own sources plus the catalog's type
    /// registry. Deliberately narrower than the test image's tag: an
    /// engine or image-recipe edit rebuilds the image but does not
    /// move this hash, so a recorded live pass (live tests spend real
    /// provider money) survives a rebuild. A scripted runner records
    /// it after a fully green live run to skip unchanged packages.
    /// With no target, prints `<package> <hash>` for every
    /// test-declaring package.
    #[command(name = "node-test-hash")]
    NodeTestHash {
        /// A package name or a node type; absent = every package.
        target: Option<String>,
    },
    /// Run the current project via the dispatcher. Streams logs until
    /// completion, including across waits, unless `--detach` is set.
    ///
    /// Rebuilding while executions are in flight is non-disruptive:
    /// in-flight work finishes on workers baked from its own image,
    /// and this run lands on a fresh current-image worker.
    Run {
        #[arg(long)]
        detach: bool,
        /// Run only this node and what it needs (repeatable: several
        /// targets run the union of what each needs). A run normally
        /// kicks every root of the graph; an aimed run is held to the
        /// targets' upstream, so a root shared with another branch
        /// never drags that branch in. Any node can be a target.
        #[arg(long, value_name = "node-id")]
        target: Vec<String>,
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
    /// cwd project is unregistered: the dispatcher deactivates it,
    /// terminates its infra pods, and reclaims its stored data.
    /// Add flags to escalate: `--journal` drops execution history,
    /// `--local` wipes `.weft/target/` on the host, `--all`
    /// implies every flag. An explicit project id overrides the
    /// cwd discovery.
    Rm {
        #[arg(value_name = "project")]
        project: Option<String>,
        #[arg(long)]
        journal: bool,
        #[arg(long)]
        local: bool,
        #[arg(long)]
        all: bool,
        /// Skip the supervisor terminate-wait window. Use when the
        /// supervisor pod is wedged or the cluster is unreachable
        /// and the user wants the project gone NOW.
        #[arg(long)]
        force: bool,
        /// Answer the confirmation. Required when there is no terminal
        /// to ask on: removing a project wipes its triggers, cancels
        /// its runs, terminates its infra and reclaims its stored
        /// data, so nothing does that on a bare command.
        #[arg(long)]
        yes: bool,
    },
    /// Tail logs. No arg → latest execution of the cwd project.
    /// UUID arg → that specific execution.
    Logs {
        #[arg(value_name = "color")]
        target: Option<String>,
        /// How many lines, counted from the END of the log: a run that
        /// wrote more than this shows its last lines, and says so.
        /// Unset, the dispatcher's own default applies.
        #[arg(long)]
        limit: Option<u32>,
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
    /// Print the per-project catalog: `--list` for one line per node
    /// type (the way to find a node), `--node <Type> --compact` for
    /// how one node wires, the bare form for the whole catalog as
    /// JSON (the editor's palette; large).
    DescribeNodes {
        /// One line per node type: the type, its tags, and its
        /// one-line description. The cheap first look at a catalog;
        /// pick a type, then `--node <Type> --compact` for its ports.
        /// `--json` prints the same as an array.
        #[arg(long, conflicts_with_all = ["node", "compact"])]
        list: bool,
        /// Describe the bundled stdlib catalog instead of a project's
        /// `nodes/`. Needs no project on disk; used to produce the browser
        /// parser's catalog asset.
        #[arg(long)]
        stdlib: bool,
        /// Print only this node TYPE's metadata (resolved). Unknown
        /// type is an error naming the fix.
        #[arg(long)]
        node: Option<String>,
        /// The wiring-only view: presentation (labels, icons, tags,
        /// placeholders, form render hints) and authoring machinery
        /// (connect recipes, image lists, graph-body display) stripped,
        /// so an AI reading the catalog burns tokens only on what
        /// decides how nodes connect. Without `--node`, the whole
        /// catalog compacted.
        #[arg(long)]
        compact: bool,
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
    /// List past executions (newest first): one line per run with its
    /// status, phase, start time, entry node and tags. `--json` prints
    /// the page as the dispatcher returns it.
    Executions {
        #[arg(long, default_value_t = 50)]
        limit: u32,
        /// Only this project's runs (a project id). Without it, every
        /// project's.
        #[arg(long)]
        project: Option<String>,
        /// Only runs of this phase: `fire` (a trigger fired, or a
        /// manual run), `trigger_setup` or `infra_setup` (the runs an
        /// activate, resync or infra start makes). "Has my trigger
        /// fired since the change" is `--phase fire`.
        #[arg(long, value_parser = parse_phase)]
        phase: Option<weft_core::context::Phase>,
    },
    /// Print a past execution's events in order, one line each:
    /// time, kind, node, and a short summary of the value or error.
    /// Values are truncated so a long run stays readable; `--node`
    /// and `--kind` narrow it, `--full` opens the values, and `--json`
    /// prints the replay rows the graph view reads.
    Events {
        color: String,
        /// Only events of this node (its id in the source).
        #[arg(long)]
        node: Option<String>,
        /// Only events of this kind (`node_failed`, `node_completed`,
        /// `node_skipped`, `execution_cancelled`, ...), or of every
        /// kind containing it (`failed` matches both failure kinds).
        #[arg(long)]
        kind: Option<String>,
        /// Print every value in full instead of the truncated summary.
        /// Only the human output truncates, so this changes nothing
        /// under `--json`, which always carries the whole row.
        #[arg(long)]
        full: bool,
    },
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
    ///   --images          unreferenced worker images for the cwd project
    ///                     (use --all to span every project)
    ///   --build-cache     docker buildkit cache prune
    ///   --all             with the journal subject: nuke every execution
    ///                     with --images: every project's images
    ///   --project <id>    that project's runs, all of them. They
    ///                     outlive the project, so this is how the
    ///                     history of a project you already removed is
    ///                     erased.
    #[command(verbatim_doc_comment)]
    Clean {
        /// Single execution UUID to delete. Mutually exclusive with --images / --build-cache.
        #[arg(value_name = "color")]
        color: Option<String>,
        /// Age cutoff in days for a sweep that names no subject
        /// (default 30). Naming a subject means you mean all of it, so
        /// this only applies to `--project` when you ask for it.
        #[arg(long, value_name = "days")]
        keep_days: Option<u32>,
        /// Wipe ALL executions (with no other flags) OR span every project (with --images).
        #[arg(long, default_value_t = false)]
        all: bool,
        /// This project's executions. A project's runs outlive it
        /// (removing a project leaves its history in the journal), so
        /// this is how a removed project's history is erased. Takes
        /// all of them, like naming one execution does; add
        /// --keep-days to spare the recent ones.
        #[arg(long, value_name = "project-id")]
        project: Option<String>,
        /// Reclaim unreferenced worker images. Cwd-scoped unless
        /// --all, which spans every project AND drops old builder-base
        /// images (~1.4GB each; the next build re-makes the base).
        #[arg(long, default_value_t = false)]
        images: bool,
        /// Prune docker BuildKit cache (heavy: invalidates cargo dep cache).
        #[arg(long, default_value_t = false)]
        build_cache: bool,
        /// Answer the confirmation a journal deletion asks for.
        /// Required when there is no terminal to ask on. Image and
        /// build-cache sweeps ask nothing: the next build re-makes
        /// what they drop.
        #[arg(long)]
        yes: bool,
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
    /// Print what the project's infra containers wrote: every unit of
    /// every infra node, or one node's. Lines are prefixed with the pod
    /// and container they came from.
    Logs {
        /// The infra node to read; unset reads every infra node of the project.
        #[arg(value_name = "node_id")]
        node_id: Option<String>,
        /// Number of lines to print, counted from the end.
        #[arg(long, default_value_t = 200)]
        tail: usize,
        /// Keep streaming new lines as the containers write them.
        #[arg(long, short = 'f', default_value_t = false)]
        follow: bool,
    },
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
    /// Bring the daemon to the desired state: ensure the kind cluster,
    /// ingress, images and dispatcher exist, roll whatever changed,
    /// and open the port-forwards so the CLI can talk to it on
    /// localhost. Idempotent, so it is both the first boot and the
    /// refresh; `restart` is an alias for the same reconcile.
    #[command(visible_alias = "restart")]
    Start {
        /// Force-rebuild every shared image (the four system images
        /// and the worker builder base), skipping the
        /// present-and-pull check. For when a local image is corrupt
        /// or hand-modified.
        #[arg(long)]
        rebuild: bool,
        /// Allow rebuilding the kind NODE when its shape changed (a
        /// config or kind-version change). The system database survives
        /// (its files live on the host); every project's own database
        /// (a PostgresDatabase infra node's volume) lives inside the
        /// node and is destroyed with it. Without this flag a shape
        /// change refuses and explains.
        #[arg(long)]
        rebuild_cluster: bool,
        /// Expose the PUBLIC TRIGGER SURFACE (/events/..., /signal/...)
        /// to the internet through an outbound tunnel + filtering
        /// proxy, so providers can deliver event pushes to this local
        /// install. Persisted until --no-public-url.
        #[arg(long, overrides_with = "no_public_url")]
        public_url: bool,
        /// Close the public trigger surface (tear the tunnel down).
        #[arg(long)]
        no_public_url: bool,
    },
    /// Stop the running daemon. Scales the dispatcher Deployment to
    /// 0 and tears down the local port-forward. The kind cluster
    /// and persistent state stay intact.
    Stop,
    /// Report whether the daemon is reachable.
    Status,
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
            InfraAction::Logs { node_id, tail, follow } => (
                commands::infra::InfraAction::Logs { node_id, tail, follow },
                Default::default(),
            ),
        }
    }
}

/// The tri-state the daemon persists: flagged on, flagged off, or
/// unflagged (keep the persisted choice).
fn public_url_choice(on: bool, off: bool) -> Option<bool> {
    match (on, off) {
        (true, _) => Some(true),
        (_, true) => Some(false),
        (false, false) => None,
    }
}

impl From<DaemonAction> for commands::daemon::DaemonAction {
    fn from(value: DaemonAction) -> Self {
        match value {
            DaemonAction::Start { rebuild, rebuild_cluster, public_url, no_public_url } => {
                commands::daemon::DaemonAction::Start {
                    rebuild,
                    rebuild_cluster,
                    public_url: public_url_choice(public_url, no_public_url),
                }
            }
            DaemonAction::Stop => commands::daemon::DaemonAction::Stop,
            DaemonAction::Status => commands::daemon::DaemonAction::Status,
            DaemonAction::Logs { tail, follow } => {
                commands::daemon::DaemonAction::Logs { tail, follow }
            }
        }
    }
}

/// `--phase` as the phase itself, so the set of names has ONE
/// definition (`Phase::as_str`) instead of a copy in the flag.
fn parse_phase(text: &str) -> Result<weft_core::context::Phase, String> {
    weft_core::context::Phase::from_tag(text).ok_or_else(|| {
        format!(
            "unknown phase '{text}': one of {}",
            weft_core::context::Phase::ALL
                .iter()
                .map(|p| p.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    weft_core::net::install_crypto_provider();
    // Local overrides (the sealing key, ports, paths) come from the
    // nearest `.env` up from the invoking directory. Real env vars win
    // over the file; no file is normal; a malformed file fails loud.
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
        Cmd::New { name, assistants } => commands::new::run(ctx, name, assistants).await,
        Cmd::Build => commands::build::run(ctx).await,
        Cmd::BuildBase { quiet } => commands::build::run_build_base(quiet).await,
        Cmd::BuildImages { push, push_suffix, print } => {
            commands::build::run_build_images(push, push_suffix, print).await
        }
        Cmd::Connect { opts } => commands::connect::run(ctx, opts).await,
        Cmd::TestNode { target, test, tiers, key, connection, yes, parallel } => {
            commands::test_node::run(
                ctx,
                commands::test_node::TestNodeArgs {
                    target,
                    test,
                    tiers,
                    key,
                    connection,
                    yes,
                    parallel,
                },
            )
            .await
        }
        Cmd::NodeTestHash { target } => commands::test_node::hash(ctx, target),
        Cmd::Run { detach, target } => commands::run::run(ctx, detach, target).await,
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
        Cmd::Rm { project, journal, local, all, force, yes } => {
            commands::rm::run(
                ctx,
                commands::rm::RmArgs { project, journal, local, all, force, yes },
            )
            .await
        }
        Cmd::Logs { target, limit } => commands::logs::run(ctx, target, limit).await,
        Cmd::Status => commands::status::run(ctx).await,
        Cmd::Daemon { action } => commands::daemon::run(ctx, action.into()).await,
        Cmd::Infra { action } => {
            let (verb, opts) = action.split();
            commands::infra::run(ctx, verb, opts).await
        }
        Cmd::DescribeNodes { list, stdlib, node, compact } => {
            commands::describe_nodes::run(ctx, stdlib, node, compact, list).await
        }
        Cmd::Parse { file } => commands::parse::parse(file).await,
        Cmd::Validate { file } => commands::parse::validate(ctx, file).await,
        Cmd::ParseServer => commands::parse::serve(ctx).await,
        Cmd::Catalog { action } => match action {
            CatalogAction::Update => commands::catalog::update(ctx).await,
        },
        Cmd::Token { action } => commands::token::run(ctx, action.into()).await,
        Cmd::Executions { limit, project, phase } => {
            commands::executions::list(ctx, limit, project, phase).await
        }
        Cmd::Events { color, node, kind, full } => {
            commands::executions::events(
                ctx,
                color,
                commands::executions::EventsFilter { node, kind, full },
            )
            .await
        }
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
        Cmd::Clean { color, keep_days, all, images, build_cache, project, yes } => {
            commands::executions::clean(
                ctx, color, keep_days, all, images, build_cache, project, yes,
            )
            .await
        }
    }
}
