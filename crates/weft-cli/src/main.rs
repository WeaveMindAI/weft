//! The `weft` CLI. A thin HTTP client of the dispatcher plus a
//! front-end for `weft build`. Every lifecycle command maps to an
//! HTTP call. The CLI never owns run state.

use clap::{Parser, Subcommand};

mod client;
mod credentials;
mod commands;
mod host_programs;
mod image_refs;
mod startup;
pub mod images;
pub mod progress;
pub mod prompt;
mod spinner;
pub mod user_config;

#[derive(Debug, Parser)]
#[command(name = "weft", version, about = "Weft CLI")]
struct Cli {
    #[command(subcommand)]
    command: Cmd,

    /// Act on the install at this raw address instead of a named
    /// target (for tooling; people use `--on`). Unset, a non-empty
    /// WEFT_DISPATCHER_URL stands in for it; read in `run`, not through
    /// clap's `env`, because the verbs bound to this machine refuse the
    /// flag but ignore the variable.
    #[arg(long, global = true)]
    dispatcher: Option<String>,

    /// Act on the named target of this project (`[targets.<name>]` in
    /// weft.toml, e.g. `--on prod`). Without it every command acts on
    /// the local install: no setting makes a remote target the default,
    /// so a shared install is only ever touched when named.
    #[arg(long = "on", value_name = "TARGET", global = true)]
    on: Option<String>,

    /// Machine-readable output instead of human text. The long
    /// commands (build, run, activate, deactivate, resync, infra, rm,
    /// the cancels) stream progress as one {"phase", "detail"} object
    /// per line, which is how the VS Code extension drives its action
    /// bar; the readers (status, ps, runs, events, logs, files,
    /// token, stop, connect) print what the
    /// dispatcher answered; test-node prints its reports as one JSON
    /// array. new never asks under it (no assistant given and none
    /// remembered is then an error). The rest (follow, daemon, catalog,
    /// tangle, clean) ignore it; describe-nodes, parse and validate are
    /// JSON already.
    #[arg(long, global = true)]
    json: bool,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Scaffold a new project (git init, main.weft, weft.toml) with the
    /// Tangle assistant persona for the AI coding assistant you will open
    /// it in, COPIED from this weft checkout's `tangle/<name>/`, so the
    /// project keeps the version that created it and `weft tangle update`
    /// is what refreshes it. The assistant comes from `--assistant`, else
    /// the default `--remember` saved, else you are asked in the terminal
    /// (the assistants found on this machine listed first). With no
    /// terminal to ask in, or `--json`, it refuses: pass `--assistant`,
    /// or `--assistant none` for a project without Tangle. Ends by
    /// printing the command that opens the project in that assistant.
    New {
        name: String,
        /// Install the Tangle persona for this AI coding assistant
        /// (e.g. `claude-code`/`cc` or `kilo-code`/`kc`), copied from the weft
        /// checkout so the project keeps what it was created with
        /// (`weft tangle update` refreshes it). Repeatable; `none` installs
        /// nothing, and is the only way to make a project without Tangle.
        /// For this project only, unless `--remember`.
        #[arg(long = "assistant", value_name = "NAME")]
        assistants: Vec<String>,
        /// Also make the `--assistant` choice the default for future
        /// `weft new` calls (`--assistant none --remember` clears it, so
        /// the next one asks again).
        #[arg(long, requires = "assistants")]
        remember: bool,
        /// Also write the GitHub Actions workflow that deploys the
        /// project to its install on this cloud (same as `weft ci add`).
        #[arg(long, value_name = "CLOUD")]
        ci: Option<commands::ci::Cloud>,
    },
    /// Have the install record this folder's files as a version, build it
    /// (compiled there: on a local install its worker compiled on the
    /// machine and its infra images built by Docker, on the cloud its
    /// images built by Cloud Build) and register it, without running
    /// anything. Also how you put back code
    /// the dispatcher no longer has: registering records the compiled
    /// program under its own hash, which is the hash a past run names, so
    /// unchanged files make that run readable again.
    Build {
        /// Compile only nodes used by the graph. By default every catalog
        /// node is compiled, so adding an unchanged node needs no rebuild.
        #[arg(long)]
        referenced: bool,
    },
    /// Compile the standard worker (every standard-library node) on this
    /// machine and pack it as one gzipped tar at `--out`, printing its
    /// binary hash: the program the release publishes per machine type,
    /// which setup.sh unpacks so a local install's first project compiles
    /// nothing.
    #[command(name = "build-standard-worker", hide = true)]
    BuildStandardWorker {
        #[arg(long)]
        out: std::path::PathBuf,
    },
    /// For setup.sh: remove from a cargo build folder (a checkout's
    /// `target/`) every compiled piece a newer build of the same piece
    /// replaced, keeping one copy of each way it is built (tests, clippy,
    /// release), so the next build of any of them compiles nothing more.
    #[command(name = "trim-cargo-target", hide = true)]
    TrimCargoTarget { dir: std::path::PathBuf },
    /// Make every image the release publishes exist locally: the runtime
    /// image (every role weft has, in one), and for the cloud the worker
    /// builder base and the full-library worker image, each under its
    /// content-addressed registry ref (present -> pull -> build).
    /// `--push` then pushes every ref to its registry; the release
    /// workflow runs this on each push to the release branch, so a local
    /// install pulls its runtime image instead of compiling it, and a
    /// cloud install's worker images compile on a ready base.
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
        /// touching nothing (no ensure, no docker).
        #[arg(long, conflicts_with_all = ["push", "push_suffix", "runtime_binary"])]
        print: bool,
        /// Put this `weft-runtime` binary in the runtime image instead of
        /// compiling one inside Docker. It must be built from this same
        /// checkout, for this machine, against a glibc no newer than
        /// Debian trixie's. The release workflow passes the binary its
        /// CLI job built, so the runtime compiles once per release.
        #[arg(long, value_name = "PATH")]
        runtime_binary: Option<std::path::PathBuf>,
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
    /// List the choices a node's searchable field offers (a
    /// `remote_select`, like a model or a spreadsheet picker): the same
    /// list the graph editor shows when you search that field, through
    /// the connection picked for the node. One `id  label` line each;
    /// the id is what goes in the source.
    Options {
        /// The node, by its id as written in the source (or
        /// `path/to/file.weft:id` when the id exists in several files).
        node: String,
        /// The field: the node's input port name.
        field: String,
        /// Narrow the list to this text, as typed in the editor's search box.
        #[arg(long)]
        search: Option<String>,
    },
    /// Run node self-tests. Without a target: every package. Without
    /// --tier: the basic + fake tiers (compiled + run locally, no
    /// install needed). With a package name or node type: just that
    /// scope. `--tier live` adds the live tier: real credentials
    /// through the production access path, as a test run in the
    /// install; it can spend money, so it confirms first (persist
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
        /// the local tiers and to live runs alike.
        #[arg(long, num_args = 0..=1, default_missing_value = "0", value_name = "N")]
        parallel: Option<usize>,
    },
    /// Print the content hash naming a package's node-test OUTCOME
    /// inputs: the package's own sources plus the catalog's type
    /// registry. Deliberately narrower than the test server's tag: an
    /// engine edit recompiles the test server but does not move this
    /// hash, so a recorded live pass (live tests spend real provider
    /// money) survives a rebuild. A scripted runner records
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
    /// Rebuilding while runs are in flight is non-disruptive:
    /// in-flight work finishes on workers of the build it started on,
    /// and this run lands on a fresh worker of the current build.
    ///
    /// Following the run to its end, it exits with the code `weft wait`
    /// gives that ending (see `weft wait --help`): 0 when the run
    /// completed, 10 when it failed or was cancelled. With `--detach` or
    /// `--json` it only starts the run and exits 0.
    Run {
        /// Load `examples/<name>.json`; explicit run flags edit its settings.
        spec: Option<String>,
        #[arg(long)]
        detach: bool,
        /// Compile only nodes used by the graph. Default: the whole catalog.
        #[arg(long)]
        referenced: bool,
        /// Reuse every node whose slice of the program did not change (and
        /// completed), re-running only the rest: from this folder's head run, or from
        /// the run named (`--seed=<run>`: any run of the project, one a
        /// trigger started or one that failed included). A node that runs
        /// again and waited there replays the answers it was given, when
        /// its code did not change. The run is named after `=`, so a bare
        /// word after `--seed` stays the example name.
        #[arg(long, value_name = "run", num_args = 0..=1, require_equals = true, default_missing_value = "")]
        seed: Option<String>,
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
        /// Start the cut at this node, optionally handing it inputs as
        /// node='{"port": value}': a handed port takes that value, and no
        /// wire into it runs. Repeatable; with --seed an unchanged start is
        /// reused like any other node.
        #[arg(long, value_name = "node[=json]")]
        from: Vec<String>,
        /// Run only this node and what it needs (repeatable: several
        /// targets run the union of what each needs). Composes with
        /// --from: from a plus target z is the path between.
        #[arg(long, value_name = "node-id")]
        target: Vec<String>,
        /// Run what this node needs and NOT the node (repeatable). The
        /// way to run a program up to the act you do not want performed:
        /// `--before post` runs everything the posting node needs and
        /// stops there.
        #[arg(long, value_name = "node-id")]
        before: Vec<String>,
        /// Run one group (or `@include`d file, by its alias) alone, with
        /// the values its inputs take handed as JSON.
        #[arg(long, value_name = "group[=json]")]
        group: Option<String>,
        /// Also run what feeds this start (a --from node or the --group):
        /// for each of its inputs you did not hand a value, the node that
        /// feeds it, through any groups and included files on the way,
        /// and nothing above that node. Repeatable.
        #[arg(long, value_name = "start")]
        feed: Vec<String>,
        /// Fire exactly one trigger with this wake payload, using a matching
        /// bake without activating listeners: `trigger=<json>`.
        #[arg(long, value_name = "trigger=json")]
        fire: Vec<String>,
        /// Stand in for a node: node='{"port": value}' declares what it
        /// would have emitted, and the node itself does not run. The way
        /// past a node whose act you do not want repeated (a trigger you
        /// have not activated, an infra node), to get at what comes
        /// after it.
        #[arg(long, value_name = "node=json")]
        emit: Vec<String>,
        /// Run inside this instance of the program: needed when the run
        /// reaches a node that exists once per instance (`@per_instance`,
        /// or anything reading from one).
        #[arg(long, value_name = "id")]
        instance: Option<weft_core::instance::InstanceId>,
        /// Write the scope flags as `examples/<name>.json` before running.
        #[arg(long, value_name = "name")]
        save: Option<String>,
        /// Only check that the run could start (its targets, starts and
        /// stand-ins resolve against the program): nothing is built or
        /// run. Exits 0 when it could, 1 with the refusal when not; with
        /// `--json`, prints `{"ok": true}` or `{"ok": false, "refusal":
        /// "..."}` and exits 0 either way.
        #[arg(long, conflicts_with_all = ["save", "detach", "seed"])]
        check: bool,
        /// Clear a saved setting before applying explicit flags: from,
        /// emit, target, before, group, feed, fire, instance, keeping
        /// (what `--durable` or `--fast` saved), keep_for or hold_secs.
        /// Repeatable.
        #[arg(long, value_name = "field")]
        clear: Vec<String>,
        /// Carry the run on in another worker if its worker dies (a node
        /// caught halfway is failed, not run twice): what the run did is
        /// written down before each node starts (except a node marked pure,
        /// which does nothing outside the run), which makes the run slower. Unset, a run that fires a trigger is kept the way the
        /// trigger says, and any other run is fast.
        #[arg(long, conflicts_with = "fast")]
        durable: bool,
        /// Run fast even when the fired trigger is durable: the run waits
        /// for its record only to pause or before it asks weft for
        /// something on its behalf, and it ends if its worker dies.
        #[arg(long)]
        fast: bool,
        /// How long the run is kept once it ends: a whole number and a
        /// unit (`30m`, `12h`, `7d`), or `forever`. Unset, the fired
        /// trigger's `keepRunsFor`, else the project's `[runs] keep_for`
        /// in weft.toml, else a week.
        #[arg(long, value_name = "duration")]
        keep_for: Option<weft_core::run_settings::KeepFor>,
        /// How long, in seconds, a wait holds the run's worker while the
        /// run cannot pause (a bus between its nodes is open), counted from
        /// the last time anything moved in the run; then the waiting call
        /// fails. 0 fails such a wait at once; at most 30 days. Unset, the
        /// fired trigger's `holdSecs`, else 60.
        #[arg(long, value_name = "seconds")]
        hold_secs: Option<u32>,
    },
    /// Record this folder's files as a version under its head, with no
    /// run and no build: a point to branch back to.
    Checkpoint {
        label: Option<String>,
        /// Start a new tree: the version parents on nothing.
        #[arg(long)]
        root: bool,
    },
    /// Move this folder to a version and write its files here. Given a
    /// run, the version it ran, and that run becomes the one the next
    /// `--seed` inherits from. Where the folder was is kept as a version
    /// first, and the command says how to come back to it.
    Branch {
        /// A version id or a run id, or the start of either.
        reference: String,
        /// Move another working copy instead of this folder's: `program`
        /// for the copy programs write into, a folder's path, or a working
        /// copy's id (`weft tree --json` lists them).
        #[arg(long, value_name = "copy")]
        copy: Option<String>,
    },
    /// Keep a working copy's files as a version and remove the copy: a
    /// folder that is gone, or the program copy, when its head keeps
    /// versions from `weft prune`. Says the version its files are now.
    ForgetCopy {
        /// `program`, a folder's path, or a working copy's id.
        copy: String,
    },
    /// Settle the conflict copies in this folder (written when a file
    /// changed both here and in the install): keep this folder's files and
    /// delete the copies, or put the install's in place.
    Resolve {
        /// The files to settle, or one of their conflict copies; none:
        /// every conflict copy in the folder.
        files: Vec<String>,
        /// `folder`: keep this folder's file. `incoming`: put the install's
        /// in its place.
        #[arg(long, value_enum)]
        keep: commands::resolve::Keep,
    },
    /// Write the files of the program the install holds (its last build)
    /// into a new folder; `--on <target>` reads another install's.
    RunningSource {
        /// The folder to create.
        dir: std::path::PathBuf,
    },
    /// The project's version tree: every version with what changed
    /// against its parent, its runs beneath, and every working copy (this
    /// folder's among them) with its head marked.
    Tree,
    /// Compare what two runs put on their wires. A ref is a run (or
    /// the start of one) or `example:<name>`.
    Diff {
        left: String,
        right: String,
        /// Every differing wire with both values, instead of per node.
        #[arg(long)]
        full: bool,
    },
    /// Write `examples/<name>.json` from a run (default: this folder's head run):
    /// its starting parameters, the answers people gave, and outputs
    /// accepted for review. Replaces an existing example in full.
    Freeze {
        name: String,
        run_id: Option<String>,
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
        run_id: String,
        /// The waiting node, named the way the program the run ran spells
        /// it, which a move or a rename since does not change.
        node: String,
        /// For a node inside a loop waiting in several iterations at once,
        /// which one: counted from 0, outermost loop first (`3`, `3.0`).
        #[arg(long, value_name = "n")]
        iteration: Option<String>,
    },
    /// The project's own worker levers: copies kept warm, the most copies,
    /// runs per copy, CPU and memory. What the project leaves unset follows
    /// the install; a change applies to the running workers at once.
    Workers {
        #[command(subcommand)]
        action: Option<WorkersAction>,
    },
    /// The domains the install answers at: its own, or a project's
    /// frontend or API. The install answers at its own address for free;
    /// a domain needs a door in front of it, which the first domain makes
    /// and the last one takes down. Adding one prints the DNS record to set
    /// and waits until it points at the door, which then gets its
    /// certificate.
    Domain {
        #[command(subcommand)]
        action: DomainAction,
    },
    /// Switch everything on the install off while weft itself is updated,
    /// so an older and a newer weft never run side by side: `on` takes down
    /// every trigger that is on and stops every infra copy that runs, and
    /// nothing is turned on until `off` builds those projects again on the
    /// new weft, starts those copies on that build and then brings those
    /// triggers back. Running `on` again adds what is on now;
    /// nothing is forgotten until `off`. Plain, it says where the install
    /// stands and what `off` brings back.
    Maintenance {
        #[command(subcommand)]
        action: Option<MaintenanceAction>,
    },
    /// The project's frontends: websites that call the install for their
    /// visitors, each with a token of its own. With `--repo`, the install
    /// hosts one and lets that repository deploy it; without, it runs
    /// wherever you run it.
    Frontend {
        #[command(subcommand)]
        action: FrontendAction,
    },
    /// Subscribe to the dispatcher's SSE stream for a project.
    Follow { project: String },
    /// Stop a run that is still going, named by its id or the start of it.
    Stop { run_id: String },
    /// Wait until a run ends, then say how it ended, in one line and in
    /// the exit code.
    ///
    /// The run is named by its id or the start of it. A run that already
    /// ended is answered at once. A timeout only stops the waiting: the
    /// run carries on, and calling `weft wait` again picks it back up.
    #[command(after_long_help = commands::wait::exit_codes_help())]
    Wait {
        run_id: String,
        /// How long to wait before giving up on hearing back: a whole
        /// number and a unit (`30s`, `10m`), at most 10 minutes.
        #[arg(long, value_name = "duration", default_value = "10m", value_parser = commands::wait::parse_timeout)]
        timeout: std::time::Duration,
        /// Also return as soon as the run parks: every node still open
        /// waits on something (a form, a timer, an outside job), which
        /// `weft runs` shows as `waiting_for_input`.
        #[arg(long)]
        waiting_for_input: bool,
    },
    /// Where one node of a run stands: pending, running, waiting (with
    /// the raw last answer its wait saw, like an outside job's status),
    /// completed, failed (with its error), skipped or cancelled.
    ///
    /// The run is named by its id or the start of it; the node the way
    /// `weft run --from` takes it (`render`, `one.render`). A node inside
    /// a loop runs once per iteration and is refused: name the loop.
    NodeState { run_id: String, node: String },
    /// Prepare and save trigger settings without listening for events.
    Bake {
        project: Option<String>,
        /// Compile only nodes used by the graph, matching `run --referenced`.
        #[arg(long, conflicts_with = "project")]
        referenced: bool,
        #[command(flatten)]
        scope: ScopeOpts,
    },
    /// Turn triggers on. Without a project id, discovers the cwd project,
    /// builds and registers it first.
    ///
    /// It turns on every trigger it names that is off, new ones included,
    /// and leaves the ones that are on alone: with nothing off it succeeds
    /// and says so, so a script can always run it. Plain, it is every
    /// shared trigger; `--instance <id>` (repeatable) names instances'
    /// triggers, `--all-instances` every instance's, and `--trigger`
    /// (repeatable) narrows either. It never starts infra and never
    /// touches a run going now: a trigger whose setup reads infra that is
    /// not running is refused, naming the infra to start; any other
    /// trigger is turned on, with a note naming the program's infra that
    /// is not running.
    ///
    /// `--held run` (the default) releases the work the triggers held while
    /// they were down, paced at half each trigger's own limits so new
    /// arrivals still get through; `--held drop` deletes the held events
    /// and cancels the runs paused on them.
    Activate {
        project: Option<String>,
        /// What happens to the work the triggers held while they were down.
        #[arg(long, value_name = "run|drop", default_value = "run")]
        held: weft_core::ReleaseHeld,
        /// On this machine, the port the project's own address opens on,
        /// kept for later activates. If another program or project holds
        /// it, nothing is activated. Unset, the project keeps the port it
        /// has, or takes a free one when it has none or another program
        /// took it.
        #[arg(long, value_name = "port")]
        port: Option<u16>,
        #[command(flatten)]
        scope: ScopeOpts,
    },
    /// Take triggers down. Plain, it is every shared trigger that is on,
    /// and it says how many instances still have theirs on;
    /// `--instance <id>` (repeatable) takes those instances' down,
    /// `--all-instances` every instance's, and `--trigger` (repeatable)
    /// narrows either. `weft activate` brings them back.
    ///
    /// Three answers, each with a default, applied in this order:
    /// `--door take|hide|refuse` (default take) is what happens to work
    /// arriving while they are down (held for when they are back, held out
    /// of sight, or refused; `--door-for <minutes>` refuses once that long
    /// has passed); `--held keep|drop` (default keep) is what happens to the
    /// work already held (drop deletes it and cancels the runs paused on
    /// it); `--running wait|cancel` (default wait) is what happens to the
    /// runs going now: wait lets them finish on their own version and
    /// answers once they have (Ctrl+C stops waiting, the triggers stay
    /// down), cancel stops them. It prints what it did.
    Deactivate {
        project: Option<String>,
        #[command(flatten)]
        take_down: TakeDownOpts,
        #[command(flatten)]
        scope: ScopeOpts,
    },
    /// Cancel an in-flight `activate`: cancels the setup run of each
    /// trigger still activating, wipes every signal it registered so
    /// far, and leaves those triggers down. 412 if none of them is
    /// activating.
    #[command(name = "cancel-activate")]
    CancelActivate {
        project: Option<String>,
        #[command(flatten)]
        scope: ScopeOpts,
    },
    /// Cancel an in-flight build (transition=building).
    /// The dispatcher driving the build stops it; the verb that was
    /// building errs "cancelled". 412 if no build is in flight.
    #[command(name = "cancel-build")]
    CancelBuild {
        project: Option<String>,
    },
    /// Build, then bring every trigger that is on and behind the build
    /// onto it: each taken down, its paused runs moved onto the new build
    /// when `--held keep` (the default), the runs going on the old version
    /// left to finish (`--running wait`, the default, however long they
    /// take) or cancelled (`--running cancel`), and only then back on the
    /// new version, which takes over from the old. A trigger
    /// that is off is never turned on: the answer names it with the `weft
    /// activate` that would. Plain, it is the shared triggers;
    /// `--instance <id>` (repeatable) names instances', `--all-instances`
    /// every instance's, `--all` the shared ones and every instance's (what
    /// the deploy workflow `weft ci add` writes runs), and `--trigger`
    /// (repeatable) narrows any of them. `--door`, `--held` and
    /// `--running wait|cancel` answer as on `weft deactivate`, with the
    /// same defaults.
    Resync {
        #[command(flatten)]
        take_down: TakeDownOpts,
        #[command(flatten)]
        scope: ScopeOpts,
        /// Take up again the resync this project holds (one that stopped
        /// with triggers down), where it stopped, with the answers and the
        /// build it began with: nothing is built and no new resync starts.
        /// Takes no other flag.
        #[arg(long)]
        resume: bool,
    },
    /// The project's instances: each one weft holds anything for, its
    /// triggers on, its infra copies, and its last run. The same list
    /// the `ListInstances` node reads.
    Instances {
        project: Option<String>,
    },
    /// List every project registered with the dispatcher.
    Ps,
    /// Remove a project at the level you ask for. No flags → the
    /// cwd project is unregistered: the dispatcher deactivates it,
    /// terminates its infrastructure, erases its runs, and reclaims its
    /// stored data.
    /// Add flags to escalate: `--journal` drops run history,
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
        /// Skip the supervisor terminate-wait window, and go on past
        /// cloud leftovers (named in the answer). Use when the supervisor
        /// is wedged and the user wants the project gone NOW. Workers
        /// that cannot be killed still stop the removal.
        #[arg(long)]
        force: bool,
        /// Answer the confirmation. Required when there is no terminal
        /// to ask on: removing a project wipes its triggers, cancels
        /// its runs, terminates its infra and reclaims its stored
        /// data, so nothing does that on a bare command.
        #[arg(long)]
        yes: bool,
    },
    /// Tail logs. No arg → latest run of the cwd project.
    /// UUID arg → that specific run.
    Logs {
        #[arg(value_name = "run-id")]
        target: Option<String>,
        /// How many lines, counted from the END of the log: a run that
        /// wrote more than this shows its last lines, and says so.
        /// Unset, the dispatcher's own default applies.
        #[arg(long)]
        limit: Option<u32>,
    },
    /// Print a summary of the cwd project's current state.
    /// Registration, listener, infra per-node, recent runs.
    Status,
    /// Manage a local install (start, stop, status, restart, logs,
    /// remove, which): the runtime that owns projects, runs, and
    /// infra; `weft run` and the VS Code extension talk to it over HTTP.
    /// Which install is where you stand: the branch of the weft checkout
    /// you are in (`main` is the default install), the default install
    /// outside any, or the one WEFT_INSTALL names.
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
    /// Install on this machine what nodes need from it (the programs
    /// they launch, what their packages need to compile), with the
    /// system's package manager. With no node type: what the program in
    /// this folder uses and the machine lacks.
    DepsInstall {
        /// The node types whose needs to install (several at once).
        #[arg(value_name = "NODE_TYPE")]
        node_types: Vec<String>,
        /// Every node type of the project's catalog, used or not.
        #[arg(long)]
        all: bool,
        /// Only the needs of one stage: `run` (what a node launches) or
        /// `build` (what its package needs to compile).
        #[arg(long, value_parser = need_stage)]
        stage: Option<weft_core::needs::NeedStage>,
        /// Run the install command without asking.
        #[arg(long, conflicts_with = "print")]
        yes: bool,
        /// Only print the install command.
        #[arg(long)]
        print: bool,
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
        /// nodes. Needs no project on disk; used to produce the browser
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
    /// node catalog and print the project + referenced catalog +
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
    /// node catalog and print diagnostics as JSON. The full check
    /// before a project ships: the strict compile + enrich + validate
    /// pipeline, runtime rules included (the editor runs the same
    /// before Run, Activate and Resync).
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
    /// What one instance of the program holds for the fields it writes
    /// `@instance_filled`: lists them with the instance's values, or
    /// changes them (`--set`, `--clear`), exactly as that instance's own
    /// page would, re-arming its live triggers that read a changed
    /// value. Acts through an instance token minted for the command and
    /// revoked after.
    #[command(name = "instance-values")]
    InstanceValues {
        /// The instance, by the id the program gives it.
        #[arg(long)]
        instance: weft_core::instance::InstanceId,
        /// Give a value: `<node>.<field>=<value>` (JSON when it reads as
        /// JSON, a string otherwise). Repeatable; all are stored at once.
        #[arg(long = "set", value_name = "NODE.FIELD=VALUE")]
        set: Vec<String>,
        /// Forget a value, so the field falls back as if never given:
        /// `<node>.<field>`. Repeatable.
        #[arg(long = "clear", value_name = "NODE.FIELD")]
        clear: Vec<String>,
    },
    /// Copy weft's connect library into the project's frontend, so its
    /// pages show the same connection pickers the editor shows, and
    /// each instance of the program gets its own connect page. Lands in
    /// `front/src/lib/weft-connect` (the default SvelteKit `$lib`)
    /// unless `--into` names another folder; that folder is the
    /// library's, replaced whole each time. Run it again after updating
    /// weft.
    ConnectLib {
        /// The folder to copy it into, relative to the project root.
        #[arg(long, value_name = "dir")]
        into: Option<std::path::PathBuf>,
    },
    /// The installs this project deploys to (`[targets.<name>]` in
    /// weft.toml). Every command acts on one with `--on <name>`; without
    /// it, on the local install.
    Target {
        #[command(subcommand)]
        action: TargetCmd,
    },
    /// The GitHub Actions workflow that deploys this project.
    Ci {
        #[command(subcommand)]
        action: CiCmd,
    },
    /// Store your operator key for a target, after checking the install
    /// accepts it. Kept in ~/.config/weft/credentials.toml (only you can
    /// read it), never in the project. CI passes WEFT_OPERATOR_KEY instead.
    Login {
        /// The target's name, as in `[targets.<name>]`.
        target: String,
        /// Read the key from stdin instead of a hidden prompt.
        #[arg(long)]
        key_stdin: bool,
    },
    /// Forget your stored operator key for a target.
    Logout {
        target: String,
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
    /// List past runs (newest first): one line per run with its
    /// status, phase, start time, entry node and tags. `--json` prints
    /// the page as the dispatcher returns it.
    Runs {
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
        /// Only runs standing this way: `completed`, `failed`,
        /// `cancelled`, `running` for the ones still going (a run parked
        /// on a wait included), or `waiting_for_input` for only those.
        /// "Which of mine broke" is `--status failed`.
        #[arg(long, value_parser = parse_run_status)]
        status: Option<weft_core::program::RunStatus>,
        /// Only runs for this instance.
        #[arg(long, value_name = "id")]
        instance: Option<weft_core::instance::InstanceId>,
        /// Only runs carrying this tag (`ctx.tag_run`).
        #[arg(long)]
        tag: Option<String>,
        /// Only finished runs in which this node fired, wherever it sits
        /// in the program (`--node` is the node that started the run).
        /// Spelled the way `weft events` names nodes. Like `--search`, it
        /// finds a run a moment after it ends, and never one still going.
        #[arg(long, value_name = "node")]
        through: Option<String>,
        /// Only finished runs that carried every one of these words
        /// somewhere in what they recorded: the trigger's input, what
        /// every node sent on, an error, a log line. An email, an order
        /// id, a phrase in quotes. A run is searchable a moment after it
        /// ends (longer while the install is busy writing records); one
        /// still going is not searched yet.
        #[arg(long, value_name = "words")]
        search: Option<String>,
    },
    /// Print a past run's events in order, one line each:
    /// time, kind, node, and a short summary of the value or error.
    /// Values are truncated so a long run stays readable; `--node`
    /// and `--kind` narrow it, `--full` opens the values, and `--json`
    /// prints the rows. Nodes are named the way the program the run ran
    /// spells them, whatever this folder holds now.
    Events {
        run_id: String,
        /// Only events of this node (or group, or loop), spelled the way
        /// the program the run ran reads: `auth.check` is the node `check`
        /// of the file the site `auth` includes, and only that use of it.
        /// A node renamed or moved since is named as it was in that run.
        #[arg(long)]
        node: Option<String>,
        /// Only events of this kind (`node_failed`, `node_completed`,
        /// `node_skipped`, `run_cancelled`, ...), or of every
        /// kind containing it (`failed` matches both failure kinds).
        #[arg(long)]
        kind: Option<String>,
        /// Only events fired inside this loop iteration, counted from 0:
        /// `--iteration 3` is the fourth time round, and `3.0` the first
        /// iteration of a loop inside it (outermost first). An inner
        /// loop's events count as part of the outer iteration they ran in.
        #[arg(long, value_name = "n")]
        iteration: Option<String>,
        /// Print every value in full instead of the truncated summary.
        /// Only the human output truncates, so this changes nothing
        /// under `--json`, which always carries the whole row.
        #[arg(long)]
        full: bool,
    },
    /// Copy saved connections or stored files from the install this
    /// command acts on to another (`--to <target>`), so moving an app to
    /// another install never means making them again. Name what to copy:
    /// `--all`, or `--id`, `--since`/`--until`, and `--service`/`--project`
    /// (connections) or `--prefix` (files). `--dry-run` copies nothing.
    Migrate {
        #[command(subcommand)]
        action: commands::migrate::MigrateAction,
    },
    /// Browse + manage stored files (the tenant's runtime storage):
    /// run, project, instance, shared and asset spaces.
    Files {
        #[command(subcommand)]
        action: FilesAction,
    },
    /// Remove stale state. Default subject is the journal: with no
    /// flags, deletes runs older than --keep-days (30). Pass
    /// a positional UUID to target one run. Other subjects
    /// are flag-driven and combinable.
    ///
    /// Subjects:
    ///   (default)         journal cleanup (runs older than --keep-days)
    ///   <UUID>            one run
    ///   --images          unreferenced workers and infra images for the cwd
    ///                     project (use --all to span every project, and to
    ///                     drop the shared images, worker programs and
    ///                     compile caches no install on this machine uses,
    ///                     and everything installs no longer on it left
    ///                     behind)
    ///   --images --build-plane
    ///                     the worker programs no install on this machine
    ///                     uses, the shared build crates no program on it
    ///                     was built from, and the builder bases and
    ///                     node-test images an older weft left
    ///   --build-cache     drop the docker buildkit cache, the node-test cache
    ///                     and the worker compile caches
    ///   --all             with the journal subject: nuke every run
    ///                     with --images: every project's images
    ///   --project <id>    that project's runs, all of them.
    #[command(verbatim_doc_comment)]
    Clean {
        /// Single run UUID to delete. Mutually exclusive with --images / --build-cache.
        #[arg(value_name = "run-id")]
        run_id: Option<String>,
        /// Age cutoff in days for a sweep that names no subject
        /// (default 30). Naming a subject means you mean all of it, so
        /// this only applies to `--project` when you ask for it.
        #[arg(long, value_name = "days")]
        keep_days: Option<u32>,
        /// Wipe ALL runs (with no other flags) OR span every project (with --images).
        #[arg(long, default_value_t = false)]
        all: bool,
        /// This project's runs. Takes all of them, like naming
        /// one run does; add --keep-days to spare the recent ones.
        #[arg(long, value_name = "project-id")]
        project: Option<String>,
        /// Only runs for this instance.
        #[arg(long, value_name = "id")]
        instance: Option<weft_core::instance::InstanceId>,
        /// Only runs standing this way (`completed`, `failed`,
        /// `cancelled`, `running`, `waiting_for_input`).
        #[arg(long, value_parser = parse_run_status)]
        status: Option<weft_core::program::RunStatus>,
        /// Only runs started by this node (a trigger that fired, or the
        /// node a run was started from).
        #[arg(long, value_name = "node")]
        node: Option<String>,
        /// Only runs carrying this tag.
        #[arg(long, value_name = "tag")]
        tag: Option<String>,
        /// Cancel matching runs that are still going (their rows go with
        /// the next clean). Without it they are left to finish.
        #[arg(long)]
        cancel_running: bool,
        /// Reclaim unreferenced workers and infra images. Cwd-scoped
        /// unless --all, which spans every project AND drops the runtime
        /// images and worker programs no install on this machine uses,
        /// and what an older weft left that none uses (builder bases,
        /// node-test images, standard worker images, worker image compile
        /// caches); each install records its own at `weft daemon start`,
        /// and while one has not, that kind is kept whole.
        #[arg(long, default_value_t = false)]
        images: bool,
        /// With --images: only the build plane, the worker programs no
        /// install on this machine uses, the shared build crates no program
        /// on it was built from, and the builder bases and node-test images
        /// an older weft left that no install uses (kept whole while an
        /// install has not recorded its own). Needs no running install;
        /// setup.sh runs it after every start.
        #[arg(long, requires = "images", conflicts_with_all = ["all", "build_cache", "run_id"])]
        build_plane: bool,
        /// Drop every build cache: the docker BuildKit cache, the
        /// machine's worker compile caches (every lane and the seed, and
        /// the shared build crates) and the node-test cache (heavy: the
        /// next build and the next `weft test-node` compile the engine
        /// and every dependency again).
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
    /// Store this weft's standard library in an install's files ahead of
    /// any project, so a project's first deploy there has none of its
    /// `nodes/base_catalog/` files left to upload. A local install does
    /// it each time it starts; the cloud install workflow runs this.
    Preload {
        /// Read the operator key from stdin rather than from the keys
        /// `weft login` stored, for a script that has it from elsewhere.
        #[arg(long)]
        key_stdin: bool,
    },
}

#[derive(Debug, Subcommand)]
enum TargetCmd {
    /// Name an install: `weft target add prod https://weft.example.com`.
    /// Replaces the url when the name exists.
    Add { name: String, url: String },
    /// List the targets, and whether you are logged in to each.
    #[command(alias = "ls")]
    List,
    /// Where a target's install lives: its address, and on a cloud the
    /// project and region it runs in.
    Show { name: String },
    /// Remove a target from weft.toml.
    #[command(alias = "rm")]
    Remove { name: String },
    /// For tooling (the editor's install switch): the address and the
    /// operator key a request carries with the same `--on`, as JSON.
    #[command(hide = true)]
    Key,
    /// Hand this project's GitHub repository what its deploy workflow
    /// needs to reach a cloud install: sets the repository variables and
    /// secrets with `gh`, minting a CI operator key and a frontend caller
    /// token on the install. Without `--github`, prints them instead.
    Export {
        name: String,
        /// Set them on the repository with `gh` (logged in, run inside
        /// the repository).
        #[arg(long)]
        github: bool,
        /// The frontend server's own environment, a dotenv file (what
        /// `weft infra env <node> --on <target> --into <file> --set ...` writes, plus any
        /// secret of the site's own), sent as the WEFT_FRONT_ENV secret.
        #[arg(long, value_name = "file")]
        front_env: Option<std::path::PathBuf>,
        /// The frontend (`weft frontend add`) the workflow deploys, when
        /// the install hosts several this repository could.
        #[arg(long, value_name = "name")]
        frontend: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum CiCmd {
    /// Write .github/workflows/deploy.yml for a cloud. Running it again
    /// replaces a file weft wrote and nobody edited, and refuses one that
    /// was edited.
    Add {
        #[arg(long, value_name = "CLOUD")]
        cloud: commands::ci::Cloud,
    },
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
        /// Make an instance token: it acts inside this one instance of
        /// the project you are standing in, and nothing else. It starts
        /// the instance's runs (through the project's live routes),
        /// answers its waits, shows its displays and connects its
        /// accounts. Needs --expires.
        #[arg(long, value_name = "id")]
        instance: Option<weft_core::instance::InstanceId>,
        /// How long the token works (`30m`, `12h`, `7d`). Required with
        /// --instance; without it a token works until revoked.
        #[arg(long, value_name = "duration", value_parser = parse_duration_secs)]
        expires: Option<u64>,
        /// Mint an operator key instead: full admin of the install
        /// (every CLI and editor verb), for a teammate or CI
        /// (`WEFT_OPERATOR_KEY`). Takes no scope and acts in no instance.
        /// Never put one in a frontend: its server holds a caller token.
        #[arg(long, conflicts_with_all = ["projects", "tags", "displays", "all_displays", "instance"])]
        operator: bool,
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
            TokenAction::Mint { name, projects, tags, displays, all_displays, instance, expires, operator } => {
                commands::token::TokenAction::Mint {
                    name,
                    projects,
                    tags,
                    displays,
                    all_displays,
                    instance,
                    expires_in_secs: expires,
                    operator,
                }
            }
            TokenAction::Ls => commands::token::TokenAction::Ls,
            TokenAction::Revoke { id } => commands::token::TokenAction::Revoke { id },
        }
    }
}

/// Which activations a lifecycle verb acts on: every shared trigger when
/// nothing is named; `--trigger` narrows to those triggers; `--instance`
/// turns to that instance's activations of the triggers that exist once
/// per instance.
#[derive(Debug, clap::Args, Default)]
struct ScopeOpts {
    /// Only this trigger, named the way the program writes it
    /// (`receive`, `one.receive`). Repeat for several.
    #[arg(long = "trigger", value_name = "node")]
    triggers: Vec<String>,
    #[command(flatten)]
    instances: InstanceOpts,
}

impl TryFrom<ScopeOpts> for weft_core::activation::ActivationScope {
    type Error = anyhow::Error;

    fn try_from(opts: ScopeOpts) -> anyhow::Result<Self> {
        Ok(Self { triggers: opts.triggers, instances: opts.instances.try_into()? })
    }
}

/// Whose triggers or infra copies a verb acts on: the shared ones when no
/// flag is given.
#[derive(Debug, clap::Args, Default, Clone)]
struct InstanceOpts {
    /// That instance's (repeat for several): its triggers that exist once
    /// per instance, its copies of the infra marked `@per_instance`.
    #[arg(long = "instance", value_name = "id")]
    instances: Vec<weft_core::instance::InstanceId>,
    /// Every instance weft holds anything for. A shared thing an
    /// instance's depends on is never needed here: shared infra taken
    /// down takes every trigger whose cut contains it down, whoever's.
    #[arg(long = "all-instances", conflicts_with = "instances")]
    all_instances: bool,
    /// The program's own and every instance's, in one command (what a
    /// deploy runs).
    #[arg(long = "all", conflicts_with_all = ["instances", "all_instances"])]
    all: bool,
}

impl TryFrom<InstanceOpts> for weft_core::instance::InstanceTarget {
    type Error = anyhow::Error;

    fn try_from(opts: InstanceOpts) -> anyhow::Result<Self> {
        Self::from_flags(opts.instances, opts.all_instances, opts.all).map_err(anyhow::Error::msg)
    }
}

/// How triggers go down, for every verb that takes them down (`weft
/// deactivate`, `weft resync`, and `weft infra stop | terminate | upgrade`
/// for the triggers whose cut contains the infra). Each answer left out
/// takes the verb's default; nothing is ever asked.
#[derive(Debug, clap::Args, Default, Clone)]
struct TakeDownOpts {
    /// What happens to work arriving while they are down: held for when
    /// they are back (take, the default), held out of sight of the people
    /// who answer questions (hide), or refused (refuse).
    #[arg(long, value_name = "take|hide|refuse")]
    door: Option<weft_core::Door>,
    /// With `--door take` or `hide`: refuse once this many minutes have
    /// passed.
    #[arg(long = "door-for", value_name = "minutes")]
    door_for: Option<u32>,
    /// What happens to the work already held (events held while they were
    /// down, runs paused on them): kept for when they are back (keep, the
    /// default), or deleted, the paused runs cancelled (drop).
    #[arg(long, value_name = "keep|drop")]
    held: Option<weft_core::Held>,
    /// What happens to the runs going now: they finish on their own
    /// version and the command goes on once they have, however long that
    /// takes (wait, the default: the infra stays up, the new version takes
    /// over, the deactivate answers), or they are cancelled (cancel).
    #[arg(long, value_name = "wait|cancel")]
    running: Option<weft_core::Running>,
}

impl From<TakeDownOpts> for weft_core::TakeDownChoice {
    fn from(opts: TakeDownOpts) -> Self {
        Self { door: opts.door, door_for_minutes: opts.door_for, held: opts.held, running: opts.running }
    }
}

#[derive(Debug, Subcommand)]
enum InfraAction {
    /// Start what is not running and nothing else. With `--infra X`
    /// (repeatable), X and anything upstream it needs that is not
    /// running; what runs already hands over its outputs without its
    /// setup running again. The triggers a stop of this infra took down
    /// come back on by themselves once it runs; no other trigger is
    /// touched. Plain, the shared infra;
    /// `--instance <id>` (repeatable) or `--all-instances` the copies of
    /// the nodes marked `@per_instance`.
    Start {
        #[command(flatten)]
        infra: InfraNodesOpts,
        #[command(flatten)]
        instances: InstanceOpts,
    },
    /// Build first, then replace with the build only what is running on
    /// another one: nothing stopped is started, nothing missing is made
    /// (`weft infra start` does both), and with nothing to replace it does
    /// nothing. The triggers whose cut
    /// contains what is replaced go down for it (`--door`, `--held`,
    /// `--running wait|cancel`, as on `weft infra stop`) and come back on
    /// by themselves once it runs again.
    Upgrade {
        #[command(flatten)]
        infra: InfraNodesOpts,
        #[command(flatten)]
        take_down: TakeDownOpts,
        #[command(flatten)]
        instances: InstanceOpts,
    },
    /// The doors this project's infrastructure has: which pieces of it
    /// a client on this machine can reach, and at what address. A door
    /// is declared by the node (its endpoint says so), so this only
    /// ever reports; nothing here opens or closes one.
    ListDoors,
    /// Stop the infra containers; their disks stay. Plain, every shared
    /// infra node; `--infra X` (repeatable) only those (a node the program
    /// no longer declares that still runs included). Shared infra reaches
    /// every trigger and run whose cut contains it, whoever's; an
    /// instance's copy (`--instance <id>`, `--all-instances`) only that
    /// instance's.
    ///
    /// The triggers whose cut contains it go down first and come back on by
    /// themselves once it runs again: `--door take|hide|refuse` and
    /// `--held keep|drop` as on `weft deactivate`; `--running wait` (the
    /// default) keeps the infra up until the runs reaching it end, however
    /// long that takes, `--running cancel` stops them.
    Stop {
        #[command(flatten)]
        infra: InfraNodesOpts,
        #[command(flatten)]
        take_down: TakeDownOpts,
        #[command(flatten)]
        instances: InstanceOpts,
        /// Scale every unit to zero, ignoring each unit's `on_stop`: units
        /// that would normally stay up come down too, so you can update
        /// them on the next start. You accept the downtime (and any slow
        /// re-warmup) by passing this.
        #[arg(long)]
        force: bool,
    },
    /// Delete the infra containers and their disks, except the disks the
    /// node lists in `keepOnTerminate`. Reaches what `weft infra stop`
    /// reaches, and takes the same flags.
    Terminate {
        #[command(flatten)]
        infra: InfraNodesOpts,
        #[command(flatten)]
        take_down: TakeDownOpts,
        #[command(flatten)]
        instances: InstanceOpts,
        /// Skip the are-you-sure prompt (required off a terminal and
        /// under --json).
        #[arg(long)]
        yes: bool,
    },
    /// Write what an infra node's card shows, and where its doors answer,
    /// into an env file, every name in one go: `--set DATABASE_USER=User
    /// --set DATABASE_PASSWORD=Password --door DATABASE_ADDRESS=pg`. A
    /// `secret` item's value is never printed; the other values are, in
    /// the confirmation line. A new file is made readable by you only, an
    /// existing one gets those lines set and keeps the rest. A door's port
    /// may change when the infra restarts or is upgraded: run it again to
    /// write the new address.
    Env {
        /// The infra node, named as `weft infra status` lists it.
        #[arg(value_name = "node")]
        node: String,
        /// The env file to write (`.env`, `web/.env.local`).
        #[arg(long, value_name = "file")]
        into: std::path::PathBuf,
        /// A variable and the card item it takes, by the item's label
        /// as `weft infra show` prints it. Repeatable.
        #[arg(long = "set", value_name = "NAME=Label")]
        set: Vec<String>,
        /// A variable for the card's only secret, whatever its label.
        #[arg(long = "as", value_name = "NAME")]
        secret_as: Option<String>,
        /// A variable and the door whose whole address (host:port) it
        /// takes, by the door's endpoint as `weft infra list-doors` prints
        /// it. Repeatable.
        #[arg(long = "door", value_name = "NAME=endpoint")]
        doors: Vec<String>,
        /// That instance's copy of a `@per_instance` node (the shared
        /// one when absent).
        #[arg(long, value_name = "id")]
        instance: Option<weft_core::instance::InstanceId>,
    },
    /// Print an infra node's card: every item (label, kind, value) and
    /// every button (its label, the action to hand `weft infra press`,
    /// and what it asks before pressing). A `secret` item is never
    /// printed, `--json` included; `weft infra env` writes it into a
    /// file.
    Show {
        /// The infra node, named as `weft infra status` lists it.
        #[arg(value_name = "node")]
        node: String,
        /// That instance's copy of a `@per_instance` node (the shared
        /// one when absent).
        #[arg(long, value_name = "id")]
        instance: Option<weft_core::instance::InstanceId>,
    },
    /// Press a button on an infra node's card, the one whose action is
    /// `<action>` (`weft infra show` lists them). It never asks: naming
    /// the action is the choice, and the button's warning is printed with
    /// the result.
    Press {
        /// The infra node, named as `weft infra status` lists it.
        #[arg(value_name = "node")]
        node: String,
        /// The button's action, as `weft infra show` prints it.
        #[arg(value_name = "action")]
        action: String,
        /// That instance's copy of a `@per_instance` node (the shared
        /// one when absent).
        #[arg(long, value_name = "id")]
        instance: Option<weft_core::instance::InstanceId>,
    },
    /// Make an infra node's baked outputs again: run its infra setup,
    /// which restarts nothing that already matches its spec and runs its
    /// body, whose values every fire reads from then on. For values that
    /// changed where weft could not see (a password reset whose report to
    /// weft never arrived). weft bakes again on its own whenever the infra is
    /// applied, and writes what the infra itself says changed.
    Rebake {
        /// The infra node, named as `weft infra status` lists it.
        #[arg(value_name = "node")]
        node: String,
        /// That instance's copy of a `@per_instance` node (the shared
        /// one when absent).
        #[arg(long, value_name = "id")]
        instance: Option<weft_core::instance::InstanceId>,
    },
    /// Print the current lifecycle state of each infra node.
    Status,
    /// Cancel in-flight infra work: halt claimed lifecycle commands
    /// (the supervisor stops between platform calls), cancel unclaimed
    /// ones outright, interrupt the provisioning run. HALT, not
    /// rollback: per-node partial state stays visible; terminate or
    /// retry per-node from where it stopped. 412 if nothing is in
    /// flight.
    Cancel {
        /// Only that instance's infra work (the shared infra's when
        /// absent).
        #[arg(long, value_name = "id")]
        instance: Option<weft_core::instance::InstanceId>,
    },
    /// Ask the supervisor to look at this project's infra again: its units'
    /// health, its waiting commands and what gone copies left behind. A
    /// health rule that gave up tries again. `weft status` shows what
    /// it finds.
    Look,
    /// Print what the project's infra containers wrote: every unit of
    /// every infra node, or one node's. Each block is headed by the node
    /// (and whose copy), the unit and the container that wrote it.
    Logs {
        /// The infra node to read, named as `weft infra status` lists
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
}

/// Which infra nodes a verb acts on.
#[derive(Debug, clap::Args, Default, Clone)]
struct InfraNodesOpts {
    /// Only this infra node, named as `weft infra status` lists it
    /// (`db`, or `one.db` for the `db` inside the file the site `one`
    /// includes). Repeat for several; every one of the owners' kind when
    /// none is named.
    #[arg(long = "infra", value_name = "node")]
    nodes: Vec<String>,
}

#[derive(Debug, Subcommand)]
enum FilesAction {
    /// List stored files, grouped by space (run, project, instance,
    /// shared and asset). Optional prefix filter.
    Ls {
        #[arg(value_name = "prefix")]
        prefix: Option<String>,
    },
    /// Show one file's full metadata.
    Inspect { key: String },
    /// Download a file: the install hands a short-lived link straight to
    /// the file store, and the bytes stream from there, resuming where
    /// they stopped when the connection drops.
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
    /// Bring the local install up: its database, its object store, the
    /// images it runs, and weft's runtime under the machine's service
    /// manager. Idempotent, so it is both the first boot and the
    /// refresh; `restart` is an alias for the same reconcile.
    #[command(visible_alias = "restart")]
    Start {
        /// Rebuild the runtime image and recompile the standard worker
        /// program even when present. For when either is corrupt or
        /// hand-modified.
        #[arg(long)]
        rebuild: bool,
        /// Open a public address for the doors outside callers use
        /// (provider event pushes, fire links, shared file links, live
        /// routes, the OAuth callback) through an outbound tunnel. The
        /// management API is never on it. Kept until --no-public-url.
        #[arg(long, overrides_with = "no_public_url")]
        public_url: bool,
        /// Close the public address (stop the tunnel).
        #[arg(long)]
        no_public_url: bool,
    },
    /// Stop weft's runtime and the workers it started. The database and
    /// the infra keep running.
    Stop,
    /// Take a NAMED install off this machine, leaving nothing of it: its
    /// projects, its workers and infra, its images, its compile caches,
    /// its database, its bucket, its binaries. A named install lives beside the default one with ports
    /// and files of its own: a branch checkout's (run this from that
    /// checkout), or one `WEFT_INSTALL=<name>` names, like a test cell
    /// (`WEFT_INSTALL=<name> weft daemon start` brings one up; add
    /// `WEFT_TIME_SCALE` to run its own timers faster). Refused for the
    /// default install, which `./setup.sh --uninstall` removes.
    Remove,
    /// Report whether the runtime of the install this command acts on is
    /// reachable, and its public address. `--on` and `--dispatcher` are
    /// refused; WEFT_DISPATCHER_URL is ignored, it does not change which
    /// install this reports on.
    Status,
    /// Print which install a command run here acts on, and why: the one
    /// WEFT_INSTALL names, else the one of the branch of the weft checkout
    /// you stand in (`main` is the default install), else the default
    /// install. Answers even when that install is not set up, and never
    /// touches it. `--json` prints one object: name (null for the default
    /// install), env (its WEFT_INSTALL value), dir, bin (its binaries'
    /// folder), branch (when the checkout picked it), installed.
    Which,
    /// Tail the runtime's log.
    Logs {
        /// Number of lines to print.
        #[arg(long, default_value_t = 100)]
        tail: usize,
        /// Keep streaming new lines as the daemon writes them.
        #[arg(long, short = 'f', default_value_t = false)]
        follow: bool,
    },
}

#[derive(Debug, Subcommand)]
enum WorkersAction {
    /// Set levers for this project (the others keep what they have).
    Set {
        /// Copies kept running when idle: 0 scales to zero (a cold start
        /// after a quiet stretch), 1 or more never waits for one.
        #[arg(long)]
        min_instances: Option<u32>,
        /// The most copies at once.
        #[arg(long)]
        max_instances: Option<u32>,
        /// How many calls Cloud Run sends one copy at once; a local
        /// install does not use it.
        #[arg(long)]
        concurrency: Option<u32>,
        /// CPUs per copy on a cloud (`1`, `2`, `0.5`); a local install
        /// does not cap a copy.
        #[arg(long)]
        cpu: Option<String>,
        /// Memory per copy on a cloud (`512Mi`, `2Gi`); a local install
        /// does not cap a copy.
        #[arg(long)]
        memory: Option<String>,
        /// Extra CPU while a copy starts, where the platform offers it.
        #[arg(long)]
        startup_boost: Option<bool>,
        /// Keep CPU on a copy between calls (billed while it is up), for
        /// a program that keeps working after it answered a live caller.
        #[arg(long)]
        cpu_always_allocated: Option<bool>,
        /// Seconds a copy holds a live thing its runs share (a pool of
        /// database connections, ready Python processes) once nothing used
        /// it.
        #[arg(long)]
        shared_idle_seconds: Option<u64>,
        /// The most calls and events one copy takes at once; one past it
        /// waits for a run to end. Unset, one per MiB of the copy's
        /// memory (on your machine, the machine's memory), and never fewer
        /// than 64.
        #[arg(long)]
        max_runs_at_once: Option<u32>,
        /// How long, in seconds, a call waits for a run to end when its
        /// copy is full, before it gets a busy `503` (30 unless set).
        #[arg(long)]
        max_queue_wait_seconds: Option<u64>,
    },
    /// Put levers back on the install's: the named ones, or every one.
    Reset { levers: Vec<String> },
}

#[derive(Debug, Subcommand)]
enum MaintenanceAction {
    /// Whether the install is in maintenance, since when, what `off`
    /// brings back, and what is on right now.
    Status,
    /// Take down every trigger that is on and stop every infra copy that
    /// runs (every unit of it; disks are kept), then wait until what is
    /// still going down has, however long that takes. Nothing is turned on
    /// until `off`.
    On {
        /// What happens to the runs going now: they finish, and this waits
        /// for them (wait, the default), or they are cancelled (cancel).
        #[arg(long, value_name = "wait|cancel")]
        running: Option<weft_core::Running>,
    },
    /// Build every project `on` took something down in again on this
    /// weft, start the infra copies `on` stopped on that build, wait until
    /// they run, then bring back the triggers `on` took down on it. What
    /// does not come back stays recorded, the install stays in maintenance,
    /// and `off` again tries it again.
    Off,
    /// Fail while something is on (a trigger, an infra copy, a run going):
    /// what `./setup.sh` and the cloud install run before they replace
    /// anything. An install that is not set up or not running passes.
    Check {
        /// Read the operator key from stdin rather than from the keys
        /// `weft login` stored, for a script that has it from elsewhere.
        #[arg(long)]
        key_stdin: bool,
        /// How the person runs the update, named as its step in the
        /// refusal (`./setup.sh`, `run this workflow again`).
        #[arg(long, default_value = "update weft")]
        update: String,
    },
}

impl From<MaintenanceAction> for commands::maintenance::MaintenanceAction {
    fn from(value: MaintenanceAction) -> Self {
        match value {
            MaintenanceAction::Status => Self::Status,
            MaintenanceAction::On { running } => Self::On { running },
            MaintenanceAction::Off => Self::Off,
            MaintenanceAction::Check { key_stdin, update } => Self::Check { key_stdin, update },
        }
    }
}

#[derive(Debug, Subcommand)]
enum DomainAction {
    /// Answer at a domain (`app.example.com`) you own.
    Add {
        name: String,
        /// What the domain serves: the install itself, or this project's
        /// frontend or API.
        #[arg(long = "for", value_enum, default_value = "install")]
        serves: commands::domain::Serves,
        /// Where a frontend runs (its https address), for `--for frontend`.
        #[arg(long)]
        to: Option<String>,
        /// Print the DNS record and return, instead of waiting until it
        /// is in place.
        #[arg(long)]
        no_wait: bool,
        /// Go ahead with what the door in front of the domains costs while
        /// it stands (on GCP, a load balancer), which the first domain
        /// makes. Without it, the first domain is refused, saying the cost.
        #[arg(long)]
        accept_cost: bool,
    },
    /// Every domain, with the DNS record each needs.
    List,
    /// Stop answering at a domain.
    Rm { name: String },
}

#[derive(Debug, Subcommand)]
enum FrontendAction {
    /// Make a frontend. Without `--repo` it runs wherever you run it, and
    /// its token is made now (written to a file only you can read). With
    /// `--repo owner/name` the install hosts it: it makes the service and
    /// lets that repository deploy to it, and its token is the deploy
    /// workflow's, handed over by `weft target export`.
    Add {
        /// Lower-case letters, digits and hyphens, at most 20.
        name: String,
        /// The GitHub repository whose CI deploys it to the install.
        #[arg(long, value_name = "owner/name")]
        repo: Option<String>,
        /// Where the frontend's server passes weft's doors on to its
        /// visitors (its `weftPassThrough` mount, like
        /// https://shop.example/weft): links to the project's stored files
        /// then go through the site.
        #[arg(long, value_name = "url")]
        visitor_address: Option<String>,
    },
    /// The project's frontends, and where each runs.
    #[command(alias = "list")]
    Ls,
    /// Set where a frontend's server passes weft's doors on to its
    /// visitors (its `weftPassThrough` mount, like
    /// https://shop.example/weft): links to the project's stored files then
    /// go through the site. One frontend of a project has one at most.
    VisitorAddress {
        name: String,
        /// The frontend's `weftPassThrough` mount, like
        /// https://shop.example/weft, or `off` to clear it.
        #[arg(value_name = "url|off")]
        url: String,
    },
    /// Remove a frontend: its tokens stop working, and a service the
    /// install made for it is deleted.
    Rm {
        name: String,
        /// Forget it even when its service cannot be removed; what stays
        /// on the cloud is named.
        #[arg(long)]
        force: bool,
    },
    /// A new token for a frontend, beside the one it has, which keeps
    /// working until `--done <id>` says the new one is in place.
    Token {
        name: String,
        /// The new token with this id is in place: every other token of
        /// the frontend stops working.
        #[arg(long, value_name = "token-id")]
        done: Option<uuid::Uuid>,
    },
}

impl From<FrontendAction> for commands::frontend::FrontendAction {
    fn from(value: FrontendAction) -> Self {
        match value {
            FrontendAction::Add { name, repo, visitor_address } => Self::Add { name, repo, visitor_address },
            FrontendAction::Ls => Self::List,
            FrontendAction::VisitorAddress { name, url } => Self::VisitorAddress { name, address: (url != "off").then_some(url) },
            FrontendAction::Rm { name, force } => Self::Rm { name, force },
            FrontendAction::Token { name, done } => Self::Token { name, done },
        }
    }
}

impl From<DomainAction> for commands::domain::DomainAction {
    fn from(value: DomainAction) -> Self {
        match value {
            DomainAction::Add { name, serves, to, no_wait, accept_cost } => Self::Add { name, serves, to, no_wait, accept_cost },
            DomainAction::List => Self::List,
            DomainAction::Rm { name } => Self::Rm { name },
        }
    }
}

impl From<WorkersAction> for commands::workers::WorkersAction {
    fn from(value: WorkersAction) -> Self {
        match value {
            WorkersAction::Set {
                min_instances,
                max_instances,
                concurrency,
                cpu,
                memory,
                startup_boost,
                cpu_always_allocated,
                shared_idle_seconds,
                max_runs_at_once,
                max_queue_wait_seconds,
            } => {
                Self::Set(weft_platform_traits::WorkerOverrides {
                    min_instances,
                    max_instances,
                    concurrency,
                    cpu,
                    memory,
                    startup_boost,
                    cpu_always_allocated,
                    shared_idle_seconds,
                    max_runs_at_once,
                    max_queue_wait_seconds,
                })
            }
            WorkersAction::Reset { levers } => Self::Reset(levers),
        }
    }
}

/// What an `infra` verb asks for: a lifecycle request to the
/// dispatcher, or one of the verbs on a node's card (env, which writes
/// a file from it, and show and press).
enum InfraRequest {
    Lifecycle(commands::infra::InfraAction, commands::infra::InfraOpts),
    Env(commands::infra_env::EnvArgs),
    Show(commands::infra_card::ShowArgs),
    Press(commands::infra_card::PressArgs),
    Rebake { node: String, instance: Option<weft_core::instance::InstanceId> },
}

impl InfraAction {
    async fn run(self, ctx: commands::Ctx) -> anyhow::Result<()> {
        match self.into_request()? {
            InfraRequest::Lifecycle(verb, opts) => commands::infra::run(ctx, verb, opts).await,
            InfraRequest::Env(args) => commands::infra_env::run(ctx, args).await,
            InfraRequest::Show(args) => commands::infra_card::run_show(ctx, args).await,
            InfraRequest::Press(args) => commands::infra_card::run_press(ctx, args).await,
            InfraRequest::Rebake { node, instance } => commands::infra_card::run_rebake(ctx, &node, instance.as_ref()).await,
        }
    }

    fn into_request(self) -> anyhow::Result<InfraRequest> {
        let opts = |infra: InfraNodesOpts, take_down: TakeDownOpts, instances: InstanceOpts| -> anyhow::Result<commands::infra::InfraOpts> {
            Ok(commands::infra::InfraOpts { nodes: infra.nodes, instances: instances.try_into()?, take_down: take_down.into() })
        };
        let (verb, opts) = match self {
            InfraAction::Start { infra, instances } => {
                (commands::infra::InfraAction::Start, opts(infra, TakeDownOpts::default(), instances)?)
            }
            InfraAction::Stop { infra, take_down, instances, force } => {
                (commands::infra::InfraAction::Stop { force }, opts(infra, take_down, instances)?)
            }
            InfraAction::Terminate { infra, take_down, instances, yes } => {
                (commands::infra::InfraAction::Terminate { yes }, opts(infra, take_down, instances)?)
            }
            InfraAction::Upgrade { infra, take_down, instances } => {
                (commands::infra::InfraAction::Upgrade, opts(infra, take_down, instances)?)
            }
            InfraAction::Status => (commands::infra::InfraAction::Status, Default::default()),
            InfraAction::Look => (commands::infra::InfraAction::Look, Default::default()),
            InfraAction::ListDoors => (commands::infra::InfraAction::ListDoors, Default::default()),
            InfraAction::Cancel { instance } => (commands::infra::InfraAction::Cancel { instance }, Default::default()),
            InfraAction::Logs { node, tail, follow } => {
                (commands::infra::InfraAction::Logs { node, tail, follow }, Default::default())
            }
            InfraAction::Env { node, into, set, secret_as, doors, instance } => {
                return Ok(InfraRequest::Env(commands::infra_env::EnvArgs { node, into, set, secret_as, doors, instance }));
            }
            InfraAction::Show { node, instance } => {
                return Ok(InfraRequest::Show(commands::infra_card::ShowArgs { node, instance }));
            }
            InfraAction::Press { node, action, instance } => {
                return Ok(InfraRequest::Press(commands::infra_card::PressArgs { node, action, instance }));
            }
            InfraAction::Rebake { node, instance } => return Ok(InfraRequest::Rebake { node, instance }),
        };
        Ok(InfraRequest::Lifecycle(verb, opts))
    }
}

/// A need's stage as `--stage` spells it: its own wire name
/// (`weft_core::needs::NeedStage`), so the two cannot drift.
fn need_stage(raw: &str) -> Result<weft_core::needs::NeedStage, String> {
    serde_json::from_value(serde_json::Value::String(raw.to_string())).map_err(|e| e.to_string())
}

/// Full builds are the default; --referenced opts into a graph-specific worker.
fn node_set(referenced: bool) -> weft_core::builds::NodeSet {
    if referenced {
        weft_core::builds::NodeSet::Referenced
    } else {
        weft_core::builds::NodeSet::Full
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
            DaemonAction::Start { rebuild, public_url, no_public_url } => commands::daemon::DaemonAction::Start {
                rebuild,
                public_url: public_url_choice(public_url, no_public_url),
            },
            DaemonAction::Stop => commands::daemon::DaemonAction::Stop,
            DaemonAction::Remove => commands::daemon::DaemonAction::Remove,
            DaemonAction::Status => commands::daemon::DaemonAction::Status,
            DaemonAction::Which => commands::daemon::DaemonAction::Which,
            DaemonAction::Logs { tail, follow } => {
                commands::daemon::DaemonAction::Logs { tail, follow }
            }
        }
    }
}

/// `--status` against the names a run can actually end with, so a
/// typo is a refusal naming the set rather than an empty listing that
/// reads as "nothing matched".
fn parse_run_status(text: &str) -> Result<weft_core::program::RunStatus, String> {
    use weft_core::program::RunStatus;
    let text = text.trim().to_ascii_lowercase();
    RunStatus::parse(&text).ok_or_else(|| format!("'{text}' is not a run status; use one of: {}", RunStatus::accepted()))
}

/// `--since 10m` as the unix second that long ago. A duration rather
/// than a timestamp, because the question is always "the run I made a
/// moment ago" and nobody wants to work out what time that was.
fn parse_duration_ago(text: &str) -> Result<u64, String> {
    let ago = parse_duration_secs(text)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| format!("the machine clock is before 1970: {e}"))?
        .as_secs();
    Ok(now.saturating_sub(ago))
}

/// A duration (`30s`, `10m`, `2h`, `3d`) in seconds.
fn parse_duration_secs(text: &str) -> Result<u64, String> {
    let text = text.trim();
    let (count, unit_secs) = match text.chars().last() {
        Some('s') => (&text[..text.len() - 1], 1u64),
        Some('m') => (&text[..text.len() - 1], 60),
        Some('h') => (&text[..text.len() - 1], 3600),
        Some('d') => (&text[..text.len() - 1], 86_400),
        _ => {
            return Err(format!(
                "'{text}' has no unit; give it in seconds, minutes, hours or days \
                 (`30s`, `10m`, `2h`, `3d`)"
            ))
        }
    };
    let count: u64 = count
        .parse()
        .map_err(|_| format!("'{text}' is not a whole number of units (`10m`, `2h`, `3d`)"))?;
    Ok(count.saturating_mul(unit_secs))
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

fn main() -> std::process::ExitCode {
    // Tab completion. The shell's completion script runs `weft` again
    // with `COMPLETE=<shell>` set and the words typed so far; this
    // answers with the candidates and exits, before anything else
    // runs. Without the variable it returns and `weft` runs normally.
    // `COMPLETE=bash weft` (or zsh, fish, ...) prints that script.
    clap_complete::CompleteEnv::with_factory(<Cli as clap::CommandFactory>::command).complete();
    // The flags are parsed out here so a failure anywhere below can be
    // reported the way the caller asked for it: `--json` gets one
    // error event on stdout, a terminal gets one `error:` line.
    let wanted = match prelude() {
        Ok(wanted) => wanted,
        Err(e) => {
            progress::report_plain_error(false, &e);
            return std::process::ExitCode::FAILURE;
        }
    };
    let cli = Cli::parse();
    let json = cli.json;
    // Started only now: the prelude writes this process's environment,
    // which is sound only while no other thread can be reading it.
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(e) => {
            progress::report_plain_error(json, &anyhow::anyhow!("could not start the async runtime: {e}"));
            return std::process::ExitCode::FAILURE;
        }
    };
    match runtime.block_on(run(cli, wanted)) {
        Ok(code) => code,
        // A verb's progress channel already reported this one; a
        // second report would be the same message again.
        Err(e) if e.is::<progress::Reported>() => std::process::ExitCode::FAILURE,
        Err(e) => {
            progress::report_plain_error(json, &e);
            std::process::ExitCode::FAILURE
        }
    }
}

/// Everything that has to happen before the flags are read, on the one
/// thread this process has so far. Answers the install the command acts
/// on, and what picked it.
fn prelude() -> anyhow::Result<Result<weft_core::infra::Wanted, String>> {
    // The front door, before anything else: a binary of one install hands
    // a command for another install to that install's own `weft`
    // (`startup::front_door`). Before the `.env` too, so the file the
    // other binary reads is read once, by it; and a `.env` never picks the
    // install, because the variable is set below before it loads.
    let cwd = std::env::current_dir().map_err(|e| anyhow::anyhow!("cannot read the working directory: {e}"))?;
    let env_value = std::env::var(weft_core::infra::INSTALL_ENV).ok();
    // Where no install can be picked (a checkout on no branch, as CI's
    // is), there is nothing to hand the command to: this binary runs it,
    // and only a command that acts on an install fails, with the reason
    // (`weft_core::infra::mark_unresolved`). Validating a project there
    // works.
    let wanted = match startup::wanted_here(env_value.as_deref(), &cwd) {
        Err(why) => {
            weft_core::infra::mark_unresolved(format!("{why:#}"));
            Err(format!("{why:#}"))
        }
        Ok((wanted, bin)) => {
            let mine = weft_core::infra::Install::of_current_exe();
            match startup::front_door(mine.as_ref(), &wanted, &bin, invoked_as_daemon_which()) {
                startup::Door::Here => {}
                startup::Door::Exec(path) => return Err(startup::exec(&path)),
                startup::Door::Refuse(why) => anyhow::bail!("{why}"),
            }
            // From here every `Install::from_env` in this process, and
            // every process it starts, names the same install; nothing
            // derives it from a working directory again. No other thread
            // runs yet (`main` starts the async runtime after this), which
            // is what makes writing the environment sound.
            std::env::set_var(weft_core::infra::INSTALL_ENV, wanted.install.env_value());
            Ok(wanted)
        }
    };

    weft_core::net::install_crypto_provider();
    // Local overrides (the sealing key, ports, paths) come from the
    // nearest `.env.local` and `.env` up from the invoking directory
    // (`startup::env_files`). Real env vars win over the files; no file is
    // normal; a malformed file fails loud.
    startup::load_env_files(&startup::env_files(&cwd))?;

    // Logs go to stderr so stdout stays a clean channel for
    // machine-readable output (notably `--json`). The extension
    // reads stdout-only and parses JSON; without this, tracing
    // warnings prepend themselves and the parse explodes.
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_writer(std::io::stderr)
        .init();
    Ok(wanted)
}

/// Whether this process was asked `weft daemon which`, which the front
/// door lets any binary answer. Asked of clap itself, on the same
/// arguments the real parse reads after it, so every spelling it accepts
/// (the `d` alias, a global flag before the verb) is recognised the same
/// way; a hand-rolled look at `argv` would drift from the grammar. Any
/// parse failure is "no": the command is then whatever the real parse
/// makes of it, after the front door.
fn invoked_as_daemon_which() -> bool {
    matches!(
        Cli::try_parse_from(std::env::args_os()),
        Ok(Cli { command: Cmd::Daemon { action: DaemonAction::Which }, .. })
    )
}

/// Run the command and answer the exit code it ends with: 0 for every
/// command that completes, except the two that report how a run ended
/// (`weft run` following its run, `weft wait`), which exit with that
/// ending's code (`commands::wait::Outcome`).
async fn run(cli: Cli, wanted: Result<weft_core::infra::Wanted, String>) -> anyhow::Result<std::process::ExitCode> {
    let ctx = commands::Ctx::new(commands::Dispatcher::from_flag_or_env(cli.dispatcher), cli.on, cli.json)?;
    let done = match cli.command {
        Cmd::New { name, assistants, remember, ci } => commands::new::run(ctx, name, assistants, remember, ci).await,
        Cmd::Ci { action: CiCmd::Add { cloud } } => commands::ci::add(ctx, cloud).await,
        Cmd::Build { referenced } => commands::build::run(ctx, node_set(referenced)).await,
        Cmd::BuildStandardWorker { out } => host_programs::pack_standard_program(&out).await,
        Cmd::TrimCargoTarget { dir } => weft_platform_local::cargo_cache::drop_superseded(&dir).map(|swept| {
            println!("removed {} superseded compiled pieces ({:.1}GB)", swept.units, swept.bytes as f64 / 1e9);
        }),
        Cmd::BuildImages { push, push_suffix, print, runtime_binary } => {
            commands::build::run_build_images(push, push_suffix, print, runtime_binary.as_deref()).await
        }
        Cmd::Connect { opts } => commands::connect::run(ctx, opts).await,
        Cmd::Options { node, field, search } => {
            commands::options::run(ctx, &node, &field, search.as_deref()).await
        }
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
        Cmd::Run { spec, detach, referenced, seed, seed_until, seed_before, root, from, target, before, group, feed, fire, emit, instance, save, check, clear, durable, fast, keep_for, hold_secs } => {
            return commands::run::run(
                ctx,
                commands::run::RunArgs {
                    spec,
                    detach,
                    node_set: Some(node_set(referenced)),
                    seed,
                    seed_until,
                    seed_before,
                    root,
                    flags: weft_core::run_flags::RunFlags { from, target, before, group, feed, fire, emit, instance, clear, durable, fast, keep_for, hold_secs },
                    save,
                    check,
                },
            )
            .await;
        }
        Cmd::NodeState { run_id, node } => commands::node_state::run(ctx, run_id, node).await,
        Cmd::Wait { run_id, timeout, waiting_for_input } => {
            return commands::wait::run(ctx, run_id, timeout, waiting_for_input).await.map(commands::wait::Outcome::exit_code);
        }
        Cmd::Checkpoint { label, root } => commands::checkpoint::run(ctx, label, root).await,
        Cmd::Branch { reference, copy } => commands::branch::run(ctx, reference, copy).await,
        Cmd::ForgetCopy { copy } => commands::branch::forget(ctx, copy).await,
        Cmd::Resolve { files, keep } => commands::resolve::run(ctx, files, keep).await,
        Cmd::RunningSource { dir } => commands::running_source::run(ctx, dir).await,
        Cmd::Tree => commands::tree::run(ctx).await,
        Cmd::Diff { left, right, full } => commands::diff::run(ctx, left, right, full).await,
        Cmd::Freeze { name, run_id, expect } => commands::freeze::run(ctx, name, run_id, expect).await,
        Cmd::Examples => commands::examples::run(ctx).await,
        Cmd::Prune { version, yes } => commands::prune::run(ctx, version, yes).await,
        Cmd::Wake { run_id, node, iteration } => commands::wake::run(ctx, run_id, node, iteration).await,
        Cmd::Domain { action } => commands::domain::run(ctx, action.into()).await,
        Cmd::Maintenance { action } => {
            commands::maintenance::run(ctx, action.map(Into::into).unwrap_or(commands::maintenance::MaintenanceAction::Status)).await
        }
        Cmd::Frontend { action } => commands::frontend::run(ctx, action.into()).await,
        Cmd::Workers { action } => commands::workers::run(ctx, action.map(Into::into).unwrap_or(commands::workers::WorkersAction::Show)).await,
        Cmd::Follow { project } => commands::follow::run(ctx, project).await,
        Cmd::Stop { run_id } => commands::stop::run(ctx, run_id).await,
        Cmd::Bake { project, referenced, scope } => {
            commands::bake::run(ctx, project, node_set(referenced), scope.try_into()?).await
        }
        Cmd::Activate { project, held, port, scope } => {
            commands::activate::run(ctx, project, held, port, scope.try_into()?).await
        }
        Cmd::Deactivate { project, take_down, scope } => {
            commands::deactivate::run(ctx, project, take_down.into(), scope.try_into()?).await
        }
        Cmd::CancelActivate { project, scope } => {
            commands::cancel_activate::run(ctx, project, scope.try_into()?).await
        }
        Cmd::CancelBuild { project } => {
            commands::cancel_build::run(ctx, project).await
        }
        Cmd::Resync { take_down, scope, resume: false } => {
            commands::resync::run(ctx, scope.try_into()?, take_down.into()).await
        }
        Cmd::Resync { take_down, scope, resume: true } => {
            let scope: weft_core::activation::ActivationScope = scope.try_into()?;
            let choice: weft_core::TakeDownChoice = take_down.into();
            if scope != weft_core::activation::ActivationScope::default() || choice != weft_core::TakeDownChoice::default() {
                anyhow::bail!(
                    "--resume takes the resync up with the triggers, answers and build it began with; leave out every other flag"
                );
            }
            commands::resync::resume(ctx).await
        }
        Cmd::Instances { project } => commands::instances::run(ctx, project).await,
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
        Cmd::Daemon { action } => commands::daemon::run(ctx, action.into(), wanted.map_err(anyhow::Error::msg)?).await,
        Cmd::Infra { action } => action.run(ctx).await,
        Cmd::DepsInstall { node_types, all, stage, yes, print } => {
            commands::deps_install::run(ctx, commands::deps_install::DepsInstall { node_types, all, stage, yes, print }).await
        }
        Cmd::DescribeNodes { list, stdlib, node, compact } => {
            commands::describe_nodes::run(ctx, stdlib, node, compact, list).await
        }
        Cmd::Parse { file } => commands::parse::parse(file).await,
        Cmd::Validate { file } => commands::parse::validate(ctx, file).await,
        Cmd::ParseServer => commands::parse::serve(),
        Cmd::Catalog { action } => match action {
            CatalogAction::Update => commands::catalog::update(ctx).await,
            CatalogAction::Preload { key_stdin } => commands::catalog::preload(ctx, key_stdin).await,
        },
        Cmd::ConnectLib { into } => commands::connect_lib::install(ctx, into).await,
        Cmd::InstanceValues { instance, set, clear } => commands::instance_values::run(ctx, instance, set, clear).await,
        Cmd::Tangle { action } => match action {
            TangleAction::Update { assistants } => commands::tangle::update(ctx, assistants).await,
        },
        Cmd::Target { action } => {
            let action = match action {
                TargetCmd::Add { name, url } => commands::target::TargetAction::Add { name, url },
                TargetCmd::List => commands::target::TargetAction::List,
                TargetCmd::Show { name } => commands::target::TargetAction::Show { name },
                TargetCmd::Remove { name } => commands::target::TargetAction::Remove { name },
                TargetCmd::Key => {
                    return commands::target::key(&ctx).map(|()| std::process::ExitCode::SUCCESS);
                }
                TargetCmd::Export { name, github, front_env, frontend } => commands::target::TargetAction::Export { name, github, front_env, frontend },
            };
            commands::target::run(ctx, action).await
        }
        Cmd::Login { target, key_stdin } => commands::target::login(ctx, target, key_stdin).await,
        Cmd::Logout { target } => commands::target::logout(ctx, target).await,
        Cmd::Token { action } => commands::token::run(ctx, action.into()).await,
        Cmd::Runs { limit, project, phase, node, since, offset, status, instance, tag, through, search } => {
            commands::runs::list(
                ctx,
                commands::runs::ListFilter {
                    limit, offset, project, phase, node, since, status, instance, tag, through, search,
                },
            )
            .await
        }
        Cmd::Events { run_id, node, kind, iteration, full } => {
            commands::runs::events(ctx, run_id, commands::runs::EventsQuery { node, kind, iteration }, full).await
        }
        Cmd::Migrate { action } => commands::migrate::run(ctx, action).await,
        Cmd::Files { action } => match action {
            FilesAction::Ls { prefix } => commands::files::ls(ctx, prefix).await,
            FilesAction::Inspect { key } => commands::files::inspect(ctx, key).await,
            FilesAction::Download { key, output } => {
                commands::files::download(ctx, key, output).await
            }
            FilesAction::Rm { target, yes } => commands::files::rm(ctx, target, yes).await,
            FilesAction::Usage => commands::files::usage(ctx).await,
        },
        Cmd::Clean {
            run_id, keep_days, all, images, build_plane, build_cache, project, instance, status, node, tag, cancel_running, yes,
        } => {
            if build_plane {
                return commands::runs::clean_build_plane().await.map(|()| std::process::ExitCode::SUCCESS);
            }
            let narrow = commands::runs::CleanNarrowing { instance, status, node, tag, cancel_running };
            commands::runs::clean(
                ctx, run_id, keep_days, all, images, build_cache, project, narrow, yes,
            )
            .await
        }
    };
    done.map(|()| std::process::ExitCode::SUCCESS)
}

#[cfg(test)]
mod command_contract_tests {
    use super::*;

    /// Every command's flags hold together (no two named alike), so a verb
    /// that takes `--all` beside its own flags parses.
    #[test]
    fn the_command_line_holds_together() {
        <Cli as clap::CommandFactory>::command().debug_assert();
    }

    /// `deps-install --stage` takes a stage by its own name, and refuses
    /// any other naming the ones it takes.
    #[test]
    fn a_stage_is_spelled_by_its_own_name() {
        let stage = |args: &[&str]| match Cli::try_parse_from([&["weft", "deps-install"][..], args].concat()).map(|cli| cli.command) {
            Ok(Cmd::DepsInstall { stage, .. }) => Ok(stage),
            Ok(_) => panic!("deps-install command"),
            Err(e) => Err(e.to_string()),
        };
        assert_eq!(stage(&["--stage", "build"]).unwrap(), Some(weft_core::needs::NeedStage::Build));
        assert_eq!(stage(&["--stage", "run"]).unwrap(), Some(weft_core::needs::NeedStage::Run));
        assert_eq!(stage(&[]).unwrap(), None);
        let refused = stage(&["--stage", "compile"]).unwrap_err();
        assert!(refused.contains("run") && refused.contains("build"), "{refused}");
    }

    /// `--all` is the program's own and every instance's; beside
    /// `--all-instances` or `--instance` it is refused.
    #[test]
    fn all_names_the_shared_ones_and_every_instance() {
        let scope = |args: &[&str]| -> anyhow::Result<weft_core::instance::InstanceTarget> {
            let cli = Cli::try_parse_from([&["weft", "resync"][..], args].concat())?;
            let Cmd::Resync { scope, .. } = cli.command else { panic!("resync command") };
            Ok(weft_core::activation::ActivationScope::try_from(scope)?.instances)
        };
        assert_eq!(scope(&["--all"]).unwrap(), weft_core::instance::InstanceTarget::Everything);
        assert_eq!(scope(&["--all-instances"]).unwrap(), weft_core::instance::InstanceTarget::EveryInstance);
        assert!(scope(&["--all", "--all-instances"]).is_err());
        assert!(scope(&["--all", "--instance", "ada"]).is_err());
        let cli = Cli::try_parse_from(["weft", "infra", "upgrade", "--all", "--running", "cancel"]).unwrap();
        assert!(matches!(cli.command, Cmd::Infra { .. }));
    }

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
                assert_eq!(node_set(flag), if referenced { weft_core::builds::NodeSet::Referenced } else { weft_core::builds::NodeSet::Full });
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
    fn wait_defaults_to_ten_minutes_and_refuses_longer() {
        let cli = Cli::try_parse_from(["weft", "wait", "3f2a"]).unwrap();
        let Cmd::Wait { run_id, timeout, waiting_for_input } = cli.command else { panic!("wait command") };
        assert_eq!((run_id.as_str(), timeout, waiting_for_input), ("3f2a", commands::wait::MAX_TIMEOUT, false));
        let cli = Cli::try_parse_from(["weft", "wait", "3f2a", "--timeout", "30s", "--waiting-for-input"]).unwrap();
        let Cmd::Wait { timeout, waiting_for_input, .. } = cli.command else { panic!("wait command") };
        assert_eq!((timeout, waiting_for_input), (std::time::Duration::from_secs(30), true));
        assert!(Cli::try_parse_from(["weft", "wait", "3f2a", "--timeout", "11m"]).is_err());
    }

    #[test]
    fn frozen_review_focus_is_repeatable() {
        let cli = Cli::try_parse_from(["weft", "freeze", "accepted", "--expect", "answer", "--expect", "summary"]).unwrap();
        let Cmd::Freeze { name, expect, .. } = cli.command else { panic!("freeze command") };
        assert_eq!(name, "accepted");
        assert_eq!(expect, ["answer", "summary"]);
    }
}
