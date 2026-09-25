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

    /// Machine-readable output instead of human text. The long
    /// commands (build, run, activate, deactivate, resync, infra, rm,
    /// the cancels) stream progress as one {"phase", "detail"} object
    /// per line, which is how the VS Code extension drives its action
    /// bar; the readers (status, ps, executions, events, logs, files,
    /// listener inspect, token, stop, connect) print what the
    /// dispatcher answered; test-node prints its reports as one JSON
    /// array. The rest (new, follow, daemon, catalog, tangle, clean)
    /// ignore it; describe-nodes, parse and validate are JSON already.
    #[arg(long, global = true)]
    json: bool,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Scaffold a new project (git init, main.weft, weft.toml). With
    /// `--assistant <name>` also install the Tangle assistant persona for
    /// that AI coding assistant, COPIED from this weft checkout's
    /// `tangle/<name>/`, so the project keeps the version that created it
    /// and `weft tangle update` is what refreshes it. The choice is
    /// remembered: later runs of `weft new` install the same assistants
    /// with no flag; `--assistant none` clears it.
    New {
        name: String,
        /// Install the Tangle persona for this AI coding assistant
        /// (e.g. `claude-code`/`cc` or `kilo-code`/`kc`), copied from the weft
        /// checkout so the project keeps what it was created with
        /// (`weft tangle update` refreshes it). Repeatable, and
        /// remembered as the default for future projects; `none` opts out.
        #[arg(long = "assistant", value_name = "NAME")]
        assistants: Vec<String>,
    },
    /// Compile the current project into its worker image and register it
    /// with the dispatcher, without running anything. Also how you put
    /// back code the dispatcher no longer has: registering records the
    /// compiled program under its own hash, which is the hash a past run
    /// names, so unchanged files make that run readable again. It adds
    /// nothing to the version tree; `weft checkpoint` does that.
    Build {
        /// Compile only nodes used by the graph. By default every catalog
        /// node is compiled, so adding an unchanged node needs no rebuild.
        #[arg(long)]
        referenced: bool,
    },
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
    /// builder-base and full-library worker, each under its content-addressed registry ref
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
    /// Every run records the code it ran as a version in the project's
    /// tree (`weft tree`).
    ///
    /// Rebuilding while executions are in flight is non-disruptive:
    /// in-flight work finishes on workers baked from its own image,
    /// and this run lands on a fresh current-image worker.
    Run {
        /// Load `examples/<name>.json`; explicit run flags edit its settings.
        spec: Option<String>,
        #[arg(long)]
        detach: bool,
        /// Compile only nodes used by the graph. Default: the whole catalog.
        #[arg(long)]
        referenced: bool,
        /// Reuse from head's run every node whose slice of the program
        /// did not change (and completed), re-running only the rest.
        #[arg(long)]
        seed: bool,
        /// With --seed: reuse through this node, inclusively. Repeatable.
        #[arg(long, value_name = "node-id")]
        seed_until: Vec<String>,
        /// With --seed: reuse up to this node, excluding it. Repeatable.
        #[arg(long, value_name = "node-id")]
        seed_before: Vec<String>,
        /// Start a new tree: the version parents on nothing and nothing
        /// is seeded.
        #[arg(long)]
        root: bool,
        /// Start the cut at this node, optionally supplying backup inputs
        /// as node='{"port": value}'. Repeatable; real input values win,
        /// and with --seed an unchanged start is reused like any other node.
        #[arg(long, value_name = "node[=json]")]
        from: Vec<String>,
        /// Run only this node and what it needs (repeatable: several
        /// targets run the union of what each needs). Composes with
        /// --from: from a plus target z is the path between.
        #[arg(long, value_name = "node-id")]
        target: Vec<String>,
        /// Run what this node needs and NOT the node (repeatable). The
        /// way to run a program up to the act you do not want performed:
        /// `--before post` runs everything the posting step needs and
        /// stops there.
        #[arg(long, value_name = "node-id")]
        before: Vec<String>,
        /// Run one group (or `@include`d file, by its alias) alone, with
        /// its boundary inputs provided.
        #[arg(long, value_name = "group[=json]")]
        group: Option<String>,
        /// Also run what feeds this start (a --from node or the --group):
        /// for each of its inputs you did not hand a value, the node that
        /// feeds it, through any group or include doors on the way, and
        /// nothing above that node. Repeatable.
        #[arg(long, value_name = "start")]
        feed: Vec<String>,
        /// Fire exactly one trigger with this wake payload, using a matching
        /// bake without activating listeners: `trigger=<json>`.
        #[arg(long, value_name = "trigger=json")]
        fire: Vec<String>,
        /// Stand in for a node: node='{"port": value}' declares what it
        /// would have emitted, and the node itself does not run. The way
        /// past a step whose act you do not want repeated (a trigger you
        /// have not activated, an infra step), to get at what comes
        /// after it.
        #[arg(long, value_name = "node=json")]
        emit: Vec<String>,
        /// Write the scope flags as `examples/<name>.json` before running.
        #[arg(long, value_name = "name")]
        save: Option<String>,
        /// Clear a saved setting before applying explicit flags: from,
        /// emit, target, before, group, feed, or fire. Repeatable.
        #[arg(long, value_name = "field")]
        clear: Vec<String>,
    },
    /// Record the project's files as a version under head, with no run
    /// and no build: a point to branch back to.
    Checkpoint {
        label: Option<String>,
        /// Start a new tree: the version parents on nothing.
        #[arg(long)]
        root: bool,
    },
    /// Restore a version's files (a color means its version) and move
    /// head there. A color sets the run the next `--seed` inherits from.
    Branch {
        /// A version id or a run color, or the start of one.
        reference: String,
        /// Throw away uncommitted changes instead of refusing.
        #[arg(long)]
        discard: bool,
    },
    /// The project's version tree: every version with what changed
    /// against its parent, its runs beneath, head marked.
    Tree,
    /// Compare what two runs put on their wires. A ref is a color (or
    /// the start of one) or `example:<name>`.
    Diff {
        left: String,
        right: String,
        /// Every differing wire with both values, instead of per node.
        #[arg(long)]
        full: bool,
    },
    /// Write `examples/<name>.json` from a run (default: head's run):
    /// its starting parameters, the answers people gave, and outputs
    /// accepted for review. Replaces an existing example in full.
    Freeze {
        name: String,
        color: Option<String>,
        /// Emphasize these output nodes in later diffs. Repeat for several nodes.
        #[arg(long = "expect")]
        expect: Vec<String>,
    },
    /// List saved examples, their frozen status, and their latest run.
    Examples,
    /// Delete a version, every version under it, and all their runs.
    Prune {
        /// A version id, or the start of one.
        version: String,
        #[arg(long)]
        yes: bool,
    },
    /// Resolve a pure time wait now. A wait that expects a value is
    /// refused, naming its kind.
    Wake {
        color: String,
        node: String,
    },
    /// Subscribe to the dispatcher's SSE stream for a project.
    Follow { project: String },
    /// Cancel an execution by color.
    Stop { color: String },
    /// Prepare and save trigger settings without listening for events.
    Bake {
        project: Option<String>,
        /// Compile only nodes used by the graph, matching `run --referenced`.
        #[arg(long, conflicts_with = "project")]
        referenced: bool,
        #[command(flatten)]
        running: RunningChoiceOpts,
    },
    /// Activate a project. Without a project id, discovers the cwd
    /// project, compiles + registers it first, then activates.
    ///
    /// `--reactivate-choice` is forwarded to the dispatcher when the
    /// project is in hibernate/park: one of
    /// `execute_parked_keep_suspended`, `keep_suspended_only`,
    /// `wipe_all`. Without it the human-terminal prompt fires;
    /// `--json` mode requires it explicitly when there is preserved
    /// state.
    ///
    /// A worker built from an older image than the one being
    /// activated is replaced: `--running-policy cancel` (the default)
    /// cancels what it runs and replaces it now; `wait` lets its
    /// in-flight executions land first, up to `--drain-timeout`.
    Activate {
        project: Option<String>,
        #[arg(long = "reactivate-choice", value_name = "choice")]
        reactivate_choice: Option<String>,
        #[command(flatten)]
        running: RunningChoiceOpts,
    },
    /// Deactivate a registered project. Choose --mode wipe, hibernate,
    /// or park explicitly in scripts; a terminal prompts when it is omitted.
    /// Wipe cancels suspended work; hibernate and park preserve it.
    ///
    /// `--running-policy` controls how in-flight executions are
    /// handled: `cancel` (the default) kills running executions and
    /// flips the project straight to inactive; `wait` leaves them to
    /// drain, parking new fires meanwhile, up to `--drain-timeout`.
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
    /// `--local` wipes this project's build artifacts, `--all`
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
    /// Manage the project's Tangle persona: the AI assistant prompts,
    /// skills and commands copied in by `weft new --assistant`.
    Tangle {
        #[command(subcommand)]
        action: TangleAction,
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
        /// Only runs this node started. The filter that finds YOUR run
        /// in a project answering thousands: everything else about a
        /// run is shared by every run beside it. Spelled the way the
        /// program spells the node (`cards.post`).
        #[arg(long)]
        node: Option<String>,
        /// Only runs that started within the last <duration>: `10m`,
        /// `2h`, `3d`. The other half of finding your own run, when you
        /// know roughly when you made it.
        #[arg(long, value_parser = parse_duration_ago)]
        since: Option<u64>,
        /// Skip this many runs before the page. With `--limit`, how you
        /// walk past the first page.
        #[arg(long, default_value_t = 0)]
        offset: u32,
        /// Only runs that ended this way: `completed`, `failed`,
        /// `cancelled`, or `running` for the ones still going. "Which
        /// of mine broke" is `--status failed`.
        #[arg(long, value_parser = parse_run_status)]
        status: Option<String>,
    },
    /// Print a past execution's events in order, one line each:
    /// time, kind, node, and a short summary of the value or error.
    /// Values are truncated so a long run stays readable; `--node`
    /// and `--kind` narrow it, `--full` opens the values, and `--json`
    /// prints the replay rows the graph view reads.
    Events {
        color: String,
        /// Only events of this node, spelled the way the source reads:
        /// `auth.check` is the node `check` of the file the site `auth`
        /// includes, and only that use of it.
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
    /// Inspect every live listener pod: how many signals the
    /// dispatcher placed on it alongside what the pod holds in RAM.
    /// Drift between the two means cleanup went wrong. Operator
    /// command for diagnosing stuck listeners.
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
    ///   --build-cache     drop the docker buildkit cache and the node-test cache
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
        /// Drop every build cache: the docker BuildKit cache and the
        /// host's node-test cache (heavy: the next build and the next
        /// `weft test-node` compile the engine and every dependency again).
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
enum TangleAction {
    /// Re-copy this project's Tangle files from the installed weft.
    /// Every file the template owns is replaced, so edits you made to
    /// the personas, skills or commands in place ARE overwritten;
    /// anything the assistant wrote next to them is left alone. With
    /// no flag it refreshes the assistants the project already has;
    /// `--assistant <name>` also installs one that is not here yet.
    Update {
        /// Assistant to install or refresh (repeatable). Same names
        /// and shorthands as `weft new --assistant`.
        #[arg(long = "assistant", value_name = "NAME")]
        assistants: Vec<String>,
    },
}

#[derive(Debug, Subcommand)]
enum TokenAction {
    /// Mint a new signal token. A signal token grants scoped access
    /// to the dispatcher's signal enumeration + reply surface, and to
    /// what a node is showing. An unscoped token sees every signal in
    /// the tenant and no node display.
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
        /// Let the token reach one node's display (the bridge's QR
        /// code, a database's minted credential): read it, and press
        /// the buttons its items carry. Takes the node the way you
        /// write it anywhere else (`whatsapp`, `test.whatsapp` for a
        /// node of an included file), resolved against the project you
        /// are standing in. Repeat for several. Unlike the flags
        /// above, saying nothing here grants nothing: a display can be
        /// a credential.
        #[arg(long = "display", value_name = "node")]
        displays: Vec<String>,
        /// Let the token reach EVERY node display in its projects.
        #[arg(long = "displays")]
        all_displays: bool,
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
            TokenAction::Mint { name, projects, tags, displays, all_displays } => {
                commands::token::TokenAction::Mint {
                    name,
                    projects,
                    tags,
                    displays,
                    all_displays,
                }
            }
            TokenAction::Ls => commands::token::TokenAction::Ls,
            TokenAction::Revoke { id } => commands::token::TokenAction::Revoke { id },
        }
    }
}

/// Shared trigger-deactivation flags, used by every verb that takes
/// triggers down (the standalone `weft deactivate`, `weft resync`, and
/// every infra verb that deactivates as a side effect: stop, terminate,
/// upgrade). The running-work pair rides along: the same flags, read
/// the same way, whether or not the verb ends up showing the picker.
///
/// A missing `--mode` prompts on a TTY; off one (and under `--json`)
/// it is `wipe`, the standing answer while building. A missing
/// `--running-policy` is `cancel` everywhere: a wait is asked for.
#[derive(Debug, clap::Args, Default, Clone)]
struct TriggerDeactivationOpts {
    /// Preservation mode for active triggers: wipe | hibernate | park.
    #[arg(long, value_name = "wipe|hibernate|park")]
    mode: Option<String>,
    /// Hibernate grace window in minutes (only meaningful with
    /// --mode hibernate). Default 15.
    #[arg(long, value_name = "minutes")]
    grace: Option<u32>,
    #[command(flatten)]
    running: RunningChoiceOpts,
}

/// The running-work choice: what happens to the executions running
/// right now when a verb needs them out of the way (a stale worker
/// replaced by `weft activate`, `weft bake` or `weft infra start`; the
/// infra an execution uses taken down by a stop, terminate or per-node
/// verb; the triggers taken down by a deactivate). The pair travels
/// together: the cap only ever bounds a wait, and a verb that took one
/// without the other could only refuse it.
#[derive(Debug, clap::Args, Default, Clone)]
struct RunningChoiceOpts {
    /// What to do with the running executions: cancel (default) | wait.
    #[arg(long = "running-policy", value_name = "cancel|wait")]
    running_policy: Option<String>,
    /// Cap in seconds on a `--running-policy wait` before the operation
    /// proceeds anyway (what is still running is cancelled). Only
    /// beside `wait`. Default: the server's 60s.
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
        running: RunningChoiceOpts,
    },
    /// Re-apply against current images / sources (stop then start).
    /// When the project is Active, triggers deactivate (same picker as
    /// `weft deactivate`) for the duration. The project is left
    /// deactivated afterward; click Activate when ready.
    Upgrade {
        #[command(flatten)]
        opts: TriggerDeactivationOpts,
    },
    /// The doors this project's infrastructure has: which pieces of it
    /// a client on this machine can reach, and at what address. A door
    /// is declared by the node (its endpoint says so), so this only
    /// ever reports; nothing here opens or closes one.
    ListDoors,
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
    /// (the supervisor stops between cluster calls), cancel unclaimed
    /// ones outright, interrupt the provisioning execution. HALT, not
    /// rollback: per-node partial state stays visible; terminate or
    /// retry per-node from where it stopped. 412 if nothing is in
    /// flight.
    Cancel,
    /// Print what the project's infra containers wrote: every unit of
    /// every infra node, or one node's. Lines are prefixed with the pod
    /// and container they came from.
    Logs {
        /// The infra instance to read, named as `weft infra status` lists
        /// it (`db`, or `one.db` for the `db` inside the file the site
        /// `one` includes); unset reads every infra node of the project.
        #[arg(value_name = "node")]
        node: Option<String>,
        /// Number of lines to print, counted from the end.
        #[arg(long, default_value_t = 200)]
        tail: usize,
        /// Keep streaming new lines as the containers write them.
        #[arg(long, short = 'f', default_value_t = false)]
        follow: bool,
    },
    /// Per-instance stop. Targets one infra instance, named as `weft
    /// infra status` lists it (`db`, or `one.db` for the `db` inside
    /// the file the site `one` includes), and leaves the rest of the
    /// project's infra untouched. Used from the graph's per-node menu
    /// (the trash icon's siblings).
    ///
    /// Nothing records which running executions use this one piece, so
    /// `--running-policy cancel` (the default) cancels every running
    /// execution of the project before the piece goes; `wait` lets
    /// them land first, up to `--drain-timeout`.
    NodeStop {
        #[arg(value_name = "node")]
        node: String,
        /// Force scale-to-zero every unit, ignoring each unit's
        /// `on_stop`. Takes down units that would normally stay up
        /// (NoOp) so you can update them on the next start. You accept
        /// the downtime (and any slow re-warmup) by passing this.
        #[arg(long)]
        force: bool,
        #[command(flatten)]
        running: RunningChoiceOpts,
    },
    /// Per-instance terminate. Same scope as `node-stop` but deletes
    /// resources instead of scaling to 0, and the same running-policy
    /// rule.
    NodeTerminate {
        #[arg(value_name = "node")]
        node: String,
        #[command(flatten)]
        running: RunningChoiceOpts,
    },
}

#[derive(Debug, Subcommand)]
enum ListenerAction {
    /// Print every live listener pod: placed signal count and the
    /// pod's registry. Drift highlights where cleanup went wrong.
    /// `--json` prints the rows as the dispatcher returns them.
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
    /// Bring the daemon to the desired state: ensure the kind cluster
    /// (with its loopback port mappings, so the CLI talks to the
    /// dispatcher on localhost), ingress, images and dispatcher exist,
    /// and roll whatever changed. Idempotent, so it is both the first
    /// boot and the refresh; `restart` is an alias for the same
    /// reconcile.
    #[command(visible_alias = "restart")]
    Start {
        /// Force-rebuild every shared image (the four system images
        /// and the worker builder base and full-library worker), skipping the
        /// present-and-pull check. For when a local image is corrupt
        /// or hand-modified.
        #[arg(long)]
        rebuild: bool,
        /// Rebuild the kind NODE even when its shape did not change (a
        /// shape change, a config or kind-version change, rebuilds on
        /// its own). The system database survives (its files live on
        /// the host); every project's own database (a PostgresDatabase
        /// infra node's volume) lives inside the node and is destroyed
        /// with it.
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
        /// Empty the shared-credentials secret when the checkout has
        /// no `access-apps.json`. Without this flag an absent file
        /// keeps whatever keys the cluster already holds.
        #[arg(long)]
        clear_access_apps: bool,
    },
    /// Stop the running daemon. Scales the dispatcher to 0. The kind
    /// cluster and persistent state stay intact.
    Stop,
    /// Take a NAMED install (`WEFT_INSTANCE`) off the cluster, leaving
    /// nothing of it: its namespaces and every project in them, its
    /// database, its bucket. A named install is one that lives beside
    /// the default install in the same cluster, like a test cell;
    /// `WEFT_INSTANCE=<name> weft daemon start` brings one up (add
    /// `WEFT_TIME_SCALE` to run its own timers faster). Refused for the
    /// default install, which `./setup.sh --uninstall` removes.
    Remove,
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
            running_policy: t.running.running_policy,
            drain_timeout: t.running.drain_timeout,
        };
        match self {
            InfraAction::Start { running } => (
                commands::infra::InfraAction::Start,
                commands::infra::InfraOpts {
                    running_policy: running.running_policy,
                    drain_timeout: running.drain_timeout,
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
            InfraAction::ListDoors => {
                (commands::infra::InfraAction::ListDoors, Default::default())
            }
            InfraAction::Cancel => (commands::infra::InfraAction::Cancel, Default::default()),
            InfraAction::NodeStop { node, force, running } => (
                commands::infra::InfraAction::NodeStop { node, force },
                commands::infra::InfraOpts {
                    running_policy: running.running_policy,
                    drain_timeout: running.drain_timeout,
                    ..Default::default()
                },
            ),
            InfraAction::NodeTerminate { node, running } => (
                commands::infra::InfraAction::NodeTerminate { node },
                commands::infra::InfraOpts {
                    running_policy: running.running_policy,
                    drain_timeout: running.drain_timeout,
                    ..Default::default()
                },
            ),
            InfraAction::Logs { node, tail, follow } => (
                commands::infra::InfraAction::Logs { node, tail, follow },
                Default::default(),
            ),
        }
    }
}

/// Full builds are the default; --referenced opts into a graph-specific image.
fn node_set(referenced: bool) -> weft_compiler::codegen::NodeSet {
    if referenced {
        weft_compiler::codegen::NodeSet::Referenced
    } else {
        weft_compiler::codegen::NodeSet::Full
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
            DaemonAction::Start { rebuild, rebuild_cluster, public_url, no_public_url, clear_access_apps } => {
                commands::daemon::DaemonAction::Start {
                    rebuild,
                    rebuild_cluster,
                    public_url: public_url_choice(public_url, no_public_url),
                    clear_access_apps,
                }
            }
            DaemonAction::Stop => commands::daemon::DaemonAction::Stop,
            DaemonAction::Remove => commands::daemon::DaemonAction::Remove,
            DaemonAction::Status => commands::daemon::DaemonAction::Status,
            DaemonAction::Logs { tail, follow } => {
                commands::daemon::DaemonAction::Logs { tail, follow }
            }
        }
    }
}

/// `--status` against the names a run can actually end with, so a
/// typo is a refusal naming the set rather than an empty listing that
/// reads as "nothing matched".
fn parse_run_status(text: &str) -> Result<String, String> {
    const ENDINGS: [&str; 4] = ["completed", "failed", "cancelled", "running"];
    let text = text.trim().to_ascii_lowercase();
    if ENDINGS.contains(&text.as_str()) {
        return Ok(text);
    }
    Err(format!("'{text}' is not how a run ends; use one of: {}", ENDINGS.join(", ")))
}

/// `--since 10m` as the unix second that long ago. A duration rather
/// than a timestamp, because the question is always "the run I made a
/// moment ago" and nobody wants to work out what time that was.
fn parse_duration_ago(text: &str) -> Result<u64, String> {
    let text = text.trim();
    let (count, unit_secs) = match text.chars().last() {
        Some('s') => (&text[..text.len() - 1], 1u64),
        Some('m') => (&text[..text.len() - 1], 60),
        Some('h') => (&text[..text.len() - 1], 3600),
        Some('d') => (&text[..text.len() - 1], 86_400),
        _ => {
            return Err(format!(
                "'{text}' has no unit; say how long ago in seconds, minutes, hours or days \
                 (`30s`, `10m`, `2h`, `3d`)"
            ))
        }
    };
    let count: u64 = count
        .parse()
        .map_err(|_| format!("'{text}' is not a whole number of units (`10m`, `2h`, `3d`)"))?;
    let ago = count.saturating_mul(unit_secs);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| format!("the machine clock is before 1970: {e}"))?
        .as_secs();
    Ok(now.saturating_sub(ago))
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
async fn main() -> std::process::ExitCode {
    // Tab completion. The shell's completion script runs `weft` again
    // with `COMPLETE=<shell>` set and the words typed so far; this
    // answers with the candidates and exits, before anything else
    // runs. Without the variable it returns and `weft` runs normally.
    // `COMPLETE=bash weft` (or zsh, fish, ...) prints that script.
    clap_complete::CompleteEnv::with_factory(<Cli as clap::CommandFactory>::command).complete();
    // The flags are parsed out here so a failure anywhere below can be
    // reported the way the caller asked for it: `--json` gets one
    // error event on stdout, a terminal gets one `error:` line.
    if let Err(e) = prelude() {
        progress::report_plain_error(false, &e);
        return std::process::ExitCode::FAILURE;
    }
    let cli = Cli::parse();
    let json = cli.json;
    match run(cli).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        // A verb's progress channel already reported this one; a
        // second report would be the same message again.
        Err(e) if e.is::<progress::Reported>() => std::process::ExitCode::FAILURE,
        Err(e) => {
            progress::report_plain_error(json, &e);
            std::process::ExitCode::FAILURE
        }
    }
}

/// Everything that has to happen before the flags are read: the
/// `.env` feeds the flags that read the environment.
fn prelude() -> anyhow::Result<()> {
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
    Ok(())
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    let ctx = commands::Ctx::new(cli.dispatcher, cli.json);

    match cli.command {
        Cmd::New { name, assistants } => commands::new::run(ctx, name, assistants).await,
        Cmd::Build { referenced } => commands::build::run(ctx, node_set(referenced)).await,
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
        Cmd::Run { spec, detach, referenced, seed, seed_until, seed_before, root, from, target, before, group, feed, fire, emit, save, clear } => {
            commands::run::run(
                ctx,
                commands::run::RunArgs {
                    spec,
                    detach,
                    node_set: Some(node_set(referenced)),
                    seed,
                    seed_until,
                    seed_before,
                    root,
                    flags: commands::versions::RunFlags { from, target, before, group, feed, fire, emit, clear },
                    save,
                },
            )
            .await
        }
        Cmd::Checkpoint { label, root } => commands::checkpoint::run(ctx, label, root).await,
        Cmd::Branch { reference, discard } => commands::branch::run(ctx, reference, discard).await,
        Cmd::Tree => commands::tree::run(ctx).await,
        Cmd::Diff { left, right, full } => commands::diff::run(ctx, left, right, full).await,
        Cmd::Freeze { name, color, expect } => commands::freeze::run(ctx, name, color, expect).await,
        Cmd::Examples => commands::examples::run(ctx).await,
        Cmd::Prune { version, yes } => commands::prune::run(ctx, version, yes).await,
        Cmd::Wake { color, node } => commands::wake::run(ctx, color, node).await,
        Cmd::Follow { project } => commands::follow::run(ctx, project).await,
        Cmd::Stop { color } => commands::stop::run(ctx, color).await,
        Cmd::Bake { project, referenced, running } => {
            commands::bake::run(ctx, project, node_set(referenced), running.running_policy, running.drain_timeout)
                .await
        }
        Cmd::Activate { project, reactivate_choice, running } => {
            commands::activate::run(
                ctx,
                project,
                reactivate_choice,
                running.running_policy,
                running.drain_timeout,
            )
            .await
        }
        Cmd::Deactivate { project, opts } => {
            commands::deactivate::run(
                ctx,
                project,
                opts.mode,
                opts.grace,
                opts.running.running_policy,
                opts.running.drain_timeout,
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
                    running_policy: opts.running.running_policy,
                    drain_timeout: opts.running.drain_timeout,
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
        Cmd::Tangle { action } => match action {
            TangleAction::Update { assistants } => commands::tangle::update(ctx, assistants).await,
        },
        Cmd::Token { action } => commands::token::run(ctx, action.into()).await,
        Cmd::Executions { limit, project, phase, node, since, offset, status } => {
            commands::executions::list(
                ctx,
                commands::executions::ListFilter {
                    limit, offset, project, phase, node, since, status,
                },
            )
            .await
        }
        Cmd::Events { color, node, kind, full } => {
            commands::executions::events(
                ctx,
                color,
                commands::executions::EventsFilter { node, call_path: Vec::new(), kind, full },
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

#[cfg(test)]
mod command_contract_tests {
    use super::*;

    #[test]
    fn builds_use_the_full_catalog_unless_referenced_is_requested() {
        for command in ["run", "build", "bake"] {
            for referenced in [false, true] {
                let mut args = vec!["weft", command];
                if referenced { args.push("--referenced"); }
                let cli = Cli::try_parse_from(args).unwrap();
                let flag = match cli.command {
                    Cmd::Run { referenced, .. } | Cmd::Build { referenced } | Cmd::Bake { referenced, .. } => referenced,
                    _ => panic!("build command"),
                };
                assert_eq!(node_set(flag), if referenced { weft_compiler::codegen::NodeSet::Referenced } else { weft_compiler::codegen::NodeSet::Full });
            }
            assert!(Cli::try_parse_from(["weft", command, "--full"]).is_err());
        }
    }

    #[test]
    fn obsolete_check_input_and_rerun_commands_are_rejected() {
        for args in [
            vec!["weft", "check"],
            vec!["weft", "run", "--input", "node.port=1"],
            vec!["weft", "run", "--rerun", "node"],
        ] {
            assert!(Cli::try_parse_from(&args).is_err(), "{args:?}");
        }
    }

    #[test]
    fn frozen_review_focus_is_repeatable() {
        let cli = Cli::try_parse_from(["weft", "freeze", "accepted", "--expect", "answer", "--expect", "summary"]).unwrap();
        let Cmd::Freeze { name, expect, .. } = cli.command else { panic!("freeze command") };
        assert_eq!(name, "accepted");
        assert_eq!(expect, ["answer", "summary"]);
    }
}
