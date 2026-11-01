//! What the install's project registry (`/projects`, `/projects/{id}`)
//! reads and answers, and the CLI sends and reads: one Rust type per
//! message, so the two ends cannot drift.

use serde::{Deserialize, Serialize};

/// One project as a list shows it: its shared activations read as one
/// status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectSummary {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub status: String,
}

/// `POST /projects`: the project exists on this install, under the
/// caller's tenant, with nothing built yet.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeclareRequest {
    pub id: uuid::Uuid,
    pub name: String,
}

crate::wire_enum! {
    /// Lifecycle status of a project's activation, written into
    /// `project.status` (and each activation's row). The dispatcher
    /// writes it, the supervisor reads it through the broker, and the
    /// status answer carries it.
    /// SYNC: ProjectStatus <-> packages/weft-graph/src/protocol.ts LifecycleStatus, packages/weft-graph/src/status.ts VALID_STATUSES
    pub enum ProjectStatus {
        /// Fresh row, never activated.
        Registered = "registered",
        /// Transient: user clicked activate; trigger setup in flight.
        Activating = "activating",
        /// Live; worker pool spawns on demand and fires execute.
        Active = "active",
        /// Taken down; its door holds or refuses what arrives
        /// (`weft_core::take_down::Door`).
        Inactive = "inactive",
    }
}

crate::wire_enum! {
    /// The single-flight transition of a project, orthogonal to its
    /// [`ProjectStatus`]: `Building` while a verb's image build is in
    /// flight, `CancellingBuild` after the person asked to cancel and
    /// before the driving process lands it back at `None`, `Resyncing`
    /// while a resync takes triggers down and brings them back on the new
    /// build (no build may start under it). While a build is in flight the
    /// only verb offered is `cancel_build`; while a resync is, only the
    /// cancels of the trigger step it is at.
    /// SYNC: ProjectTransition <-> packages/weft-graph/src/protocol.ts ProjectTransition,
    ///       packages/weft-graph/src/status.ts VALID_TRANSITIONS
    pub enum ProjectTransition {
        None = "none",
        Building = "building",
        CancellingBuild = "cancelling_build",
        Resyncing = "resyncing",
    }
}

impl ProjectTransition {
    /// True while a build transition is in flight (either phase).
    // SYNC: is_building <-> packages/weft-graph/src/webview/lib/components/project/ActionBar.svelte (building)
    pub fn is_building(self) -> bool {
        matches!(self, Self::Building | Self::CancellingBuild)
    }

    /// What a verb refused because of this transition tells the person:
    /// `None` when there is nothing to wait on.
    pub fn refusal(self) -> Option<&'static str> {
        match self {
            Self::Building => Some("the project is building; wait for the build to finish, or cancel it with `weft cancel-build`"),
            Self::CancellingBuild => Some("the project's build is being cancelled; try again once it has stopped"),
            Self::Resyncing => Some(
                "the project is resyncing; wait for it to finish (`weft resync --resume` takes one that stopped up again), \
                 or give it up with `weft cancel-activate`, which leaves the triggers it has not brought back down",
            ),
            Self::None => None,
        }
    }
}

/// `POST /projects/{id}/status`'s body: the hashes the client computed
/// for its current source, each compared with what the project runs. What
/// it leaves out is compared with the registered build's instead. A body,
/// never a URL: a program with many triggers names a hash for each.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StatusRequest {
    /// Compared against the running binary hash: "the worker image
    /// needs rebuilding", and, for a trigger that is on, whether its fires
    /// run older code.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub desired_binary_hash: Option<String>,
    /// The same build's binary hash with the whole catalog compiled in
    /// (`weft run --full`). A running image built either way is
    /// current: drift means the running hash matches neither.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub desired_full_binary_hash: Option<String>,
    /// Compared against the running definition hash: "the project shape
    /// has changed" (a config or topology edit not registered yet).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub desired_definition_hash: Option<String>,
    /// Every trigger of the current source by address, with its cut's
    /// fingerprint (`crate::run_spec::trigger_fingerprints`), `null` for
    /// one whose cut cannot be walked: the whole of the source's triggers,
    /// so a trigger that is on is behind when its recorded cut differs or
    /// its cut cannot be walked, and gone when the source has it no more.
    /// Left out when the source did not compile: the triggers are then
    /// compared with the registered build's cuts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub desired_trigger_cuts: Option<std::collections::BTreeMap<String, Option<String>>>,
    /// Every infra place of the current source with its hash: the whole
    /// of the source's places, so a copy applied from another hash is
    /// behind, and a copy of a place the source has no more is orphaned.
    /// Left out when the
    /// source's hashes could not be worked out: the copies are then
    /// compared with the registered build's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub desired_infra_places: Option<std::collections::BTreeMap<String, String>>,
}

/// What a person is told when something of a project is refused because
/// `weft rm` marked it and has not finished removing it (`project.removing`):
/// a run born for it, a verb on it, and what its status says of it.
// SYNC: PROJECT_BEING_REMOVED <-> packages/weft-graph/src/status.ts PROJECT_BEING_REMOVED
pub const PROJECT_BEING_REMOVED: &str = "the project is being removed; run `weft rm` again to finish removing it";

/// `POST /projects/{id}/status`: everything a client renders about one
/// project in one answer (registration, listener, infra, runs,
/// drift, and the verbs the dispatcher accepts right now).
// SYNC: ProjectStatusResponse <-> packages/weft-graph/src/status.ts RawStatusPayload
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectStatusResponse {
    pub id: uuid::Uuid,
    pub name: String,
    /// `weft rm` marked the project and has not finished removing it
    /// ([`PROJECT_BEING_REMOVED`]): what is left of it reads below, and no
    /// verb is offered.
    #[serde(default)]
    pub removing: bool,
    /// The aggregate status over the shared activations.
    pub status: ProjectStatus,
    /// The build transition axis, orthogonal to `status`.
    pub transition: ProjectTransition,
    pub listener_running: bool,
    /// True when the program has a shared trigger (one not marked
    /// `@per_instance`): what the action bar arms. Clients show the
    /// listening indicator ONLY when this is true; `listener_running`
    /// then says whether it is listening now.
    pub has_triggers: bool,
    pub infra: Vec<ProjectInfraEntry>,
    pub runs: ProjectRunsSummary,
    /// True when the project has any infra-typed nodes in its source:
    /// whether a client shows the infra controls at all.
    pub has_infra: bool,
    /// Whether this install has built the program at least once. Until
    /// it has, it knows nothing of the program's nodes, so an empty
    /// `infra` says nothing about the source.
    #[serde(default)]
    pub built: bool,
    /// The image builds running for the project right now.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub builds: Vec<BuildInFlight>,
    /// The project's own address, serving its routes at its root, once
    /// it has one (a local install gives each project a port at its first
    /// activation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<ProjectAddress>,
    /// True when live `infra_node` rows exist whose node is NOT in the
    /// current source (the node was deleted while deployed, or it sits
    /// behind a part that does not compile). Never gates
    /// run/activate; clients OR it into their infra-controls check so the
    /// controls for live infra never vanish.
    pub orphaned_infra: bool,
    /// Aggregate infra state across the project's infra nodes, one of
    /// nine values: "none" (no infra nodes defined), "running" (all up),
    /// "provisioning", "stopping", "terminating" (transitional),
    /// "stopped" (all down), "partial" (mixed), "flaky", "failed".
    /// SYNC: infra_rollup values <-> packages/weft-graph/src/status.ts (infra_rollup consumer)
    pub infra_rollup: String,
    /// An infra operation is in flight that the rollup cannot see yet (a
    /// claimed stop still draining, a provisioning run before any node
    /// row flips): the window where every verb but cancel is refused.
    pub infra_busy: bool,
    /// Desired vs running hash drift. Each bit is only meaningful when
    /// the caller named the matching hash in the [`StatusRequest`].
    pub drift: ProjectDrift,
    /// Verbs the dispatcher accepts right now, from its state machine.
    /// Clients render the action bar from this list directly.
    pub available_actions: Vec<String>,
    /// Every instance's copy of a per-instance infra node, and its state.
    /// `infra` above lists the shared copies only.
    pub instance_infra: Vec<InstanceInfraEntry>,
    /// Every trigger of the program, one entry per place, with its state
    /// (and each instance's, for one that exists once per instance), plus
    /// the activations of triggers the program no longer has: what the
    /// editor stamps on each trigger node and `weft status` lists.
    /// `status` above is the aggregate over the shared ones.
    pub triggers: Vec<TriggerEntry>,
    /// Every instance weft holds anything for, with what it holds: the
    /// editor's instance picker, and what `weft instances` lists.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub instances: Vec<crate::program::InstanceHoldings>,
    /// The public entries whose limits refused calls in the last minute
    /// or two, so an author sees a limit acting. Empty when none did.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub limited: Vec<LimitedEntry>,
    /// How many runs each trigger started in the last minute or two, and
    /// how many of those failed: the one trace a run kept unrecorded
    /// leaves when it goes well. Empty when no trigger started one.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub trigger_runs: Vec<TriggerRuns>,
    /// weft's own work the project's work waits on that keeps failing.
    /// Empty when none does.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unanswered: Vec<Unanswered>,
    /// The project's resync, while its transition is `resyncing`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resync: Option<ResyncStanding>,
}

/// A resync under way, as `weft status` shows it: stopped, with why and
/// what comes next, or going, with where its task last said it is.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResyncStanding {
    /// Set while it stands still with nothing driving it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopped: Option<ResyncStop>,
    #[serde(default)]
    pub progress: crate::deactivation::ResyncProgress,
}

/// Why a resync stands still, and what comes next, in a person's words.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResyncStop {
    pub error: String,
    pub then: String,
}

/// Part of weft's own work that keeps failing, so the work behind it
/// waits: a role woken to pick up work (the supervisor, which starts and
/// stops infra), the project's workers being handed a run, one of weft's
/// loops that gave up, the supervisor acting on the project's infra. This
/// is what a person waiting is shown, until it is resolved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Unanswered {
    /// What fails, in a person's words, as a whole clause ("weft's
    /// supervisor, which starts and stops infra, cannot be reached").
    pub what: String,
    /// What happens next, in a person's words: when weft tries again, or
    /// what a person can do ("weft rings it again on the next change that
    /// needs it").
    pub then: String,
    /// The last error written down.
    pub error: String,
    pub since_unix: i64,
    pub last_unix: i64,
    /// The install's clock when this was read, which the times above are
    /// on, so how long ago reads right whatever clock the reader has.
    pub as_of_unix: i64,
}

impl Unanswered {
    /// One line for a person: what fails, since when, what happens next,
    /// and why.
    pub fn describe(&self) -> String {
        let now_unix = self.as_of_unix;
        let span = |secs: i64| match secs.max(0) {
            secs @ 0..120 => format!("{secs}s"),
            secs @ 120..7200 => format!("{} min", secs / 60),
            secs => format!("{} h", secs / 3600),
        };
        format!(
            "{} (failing for {}, last tried {} ago; {}): {}",
            self.what,
            span(now_unix - self.since_unix),
            span(now_unix - self.last_unix),
            self.then,
            self.error,
        )
    }
}

/// What `POST /projects/{id}/quiesce` asks: `weft rm`'s first step.
#[derive(Debug, Clone, PartialEq, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuiesceRequest {
    /// Wait for every run of the project to end, not only for the last
    /// calls of the ones it cancels: `weft rm --journal`, before it erases
    /// the project's history.
    #[serde(default)]
    pub every_run: bool,
}

/// What `POST /projects/{id}/quiesce` answers once the project is down:
/// the task waiting for what it cancelled, which the person follows
/// (`GET /projects/{id}/follow/{task}`).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Quiesced {
    pub waiting: uuid::Uuid,
}

/// What `DELETE /projects/{id}` answers: what a forced removal could not
/// take off the cloud, one sentence each, naming it (empty otherwise).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProjectRemoved {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub left: Vec<String>,
}

/// One image build the project waits on, running on the install's
/// builder: one it started, or one another project started of the same
/// content.
// SYNC: BuildInFlight <-> packages/weft-graph/src/status.ts RawStatusPayload.builds
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildInFlight {
    /// The image ref it pushes.
    pub image: String,
    /// The builder's own id for it.
    pub build: String,
    pub started_at_unix: i64,
    /// Where its log is read, when the builder keeps one at an address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_url: Option<String>,
}

/// Where one image build is: going on, or how it ended
/// ([`crate::builds::ImageBuildState`]).
// SYNC: BuildState <-> crates/weft-dispatcher/src/build/ledger.rs (image_build.status)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildState {
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

/// Everything a caller following one image build is told about it.
/// `state` is `None` when that build is no longer on record (a cleanup
/// forgot its image, or another build of the same image replaced it).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildStateResponse {
    pub state: Option<BuildState>,
    /// Why it failed, once it did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The builder's own id for it, once the builder made it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_unix: Option<i64>,
    /// Where its log is read, when the builder keeps one at an address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_url: Option<String>,
}

/// One trigger's runs in the last minute or two (`ProjectStatusResponse::runs`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerRuns {
    /// The trigger's node, as the program spells it.
    pub node: String,
    pub started: u64,
    pub failed: u64,
}

/// One public entry that refused calls recently, and by which limit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LimitedEntry {
    /// The entry's node, as the program spells it.
    pub node: String,
    /// What refused: `this caller's calls per minute`, `this entry's
    /// calls per minute`, `this entry's runs at once`.
    pub limit: String,
    pub refused: u64,
}

/// One instance's copy of an infra node in the status answer.
// SYNC: InstanceInfraEntry <-> packages/weft-graph/src/protocol.ts InstanceInfraEntry
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceInfraEntry {
    pub node: String,
    pub instance: crate::instance::InstanceId,
    pub status: String,
    /// How far a start, stop or terminate of the copy got, while one is
    /// under way.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<crate::infra::wire::ChangeProgress>,
    /// Applied from other infra than the source holds now
    /// ([`ProjectInfraEntry::behind`]).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub behind: bool,
    /// The source declares its place no more ([`ProjectInfraEntry::orphaned`]).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub orphaned: bool,
    /// The latest restart of its containers since weft last started it
    /// ([`ProjectInfraEntry::last_restart`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_restart: Option<crate::infra::LastRestart>,
}

crate::wire_enum! {
    /// Where one activation of a trigger stands, as its node's badge reads
    /// it.
    // SYNC: TriggerState <-> packages/weft-graph/src/protocol.ts TriggerState
    pub enum TriggerState {
        /// Never turned on, or taken down.
        Off = "off",
        /// Its setup is running.
        TurningOn = "turning_on",
        /// On, firing the program as it is.
        On = "on",
        /// On, firing an older program: its cut, or the code it runs,
        /// changed since it was set up. `weft resync` brings it up to date.
        Behind = "behind",
    }
}

impl TriggerState {
    /// Whether a trigger in this state is on: its door takes fires, the
    /// program it fires current (`On`) or older (`Behind`).
    pub fn is_on(self) -> bool {
        matches!(self, Self::On | Self::Behind)
    }
}

/// One trigger place of the program, as the editor stamps it on its node
/// and `weft status` lists it.
// SYNC: TriggerEntry <-> packages/weft-graph/src/protocol.ts TriggerEntry
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TriggerEntry {
    /// The trigger, spelled the way the program writes it (`door`,
    /// `one.door`).
    pub node: String,
    /// It exists once per instance: `copies` holds every instance's, and
    /// it has no shared one.
    pub per_instance: bool,
    /// The program has it no more (an activation left from before the
    /// source dropped it): nothing to stamp, and `weft deactivate
    /// --trigger` takes it down.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub gone: bool,
    /// The shared trigger's own activation; `None` for one that exists
    /// once per instance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shared: Option<TriggerCopy>,
    /// Each instance's activation, for one that exists once per instance.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub copies: Vec<TriggerCopy>,
    /// The infra places its cut contains (`weft_core::run_spec::trigger_cut`):
    /// what taking one of them down takes this trigger down with. From the
    /// cut it was set up with while on, from the program otherwise.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
}

crate::wire_enum! {
    /// What took a trigger down when it was not a person.
    // SYNC: DownWith <-> trigger_activation.went_down_with (crates/weft-dispatcher/src/activation_store.rs), packages/weft-graph/src/protocol.ts DownWith
    pub enum DownWith {
        /// The health check: a copy of infra in its cut broke. Its node's
        /// health rules bring it back once that copy is healthy (the
        /// built-in ones do; rules a node wrote may not, and then a person
        /// does, with `weft activate`).
        Health = "health",
        /// An infra verb took it down with the copies in its cut; it comes
        /// back once they run again.
        Infra = "infra",
        /// `weft maintenance on` took it down with everything else on the
        /// install; `weft maintenance off` brings it back.
        Maintenance = "maintenance",
    }
}

/// One owner's activation of a trigger.
// SYNC: TriggerCopy <-> packages/weft-graph/src/protocol.ts TriggerCopy
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TriggerCopy {
    /// Whose: absent for the shared one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<crate::instance::InstanceId>,
    /// Goes on the wire with `on` beside it ([`TriggerState::is_on`]), so
    /// a script reading `weft status --json` needs not know which states
    /// are on. `on` is written from the state and never read back, so the
    /// two cannot disagree.
    #[serde(flatten, with = "state_and_on")]
    pub state: TriggerState,
    /// Where it stands in detail: for one taken down, what its door does.
    pub mode: crate::activation::ActivationMode,
    /// For one taken down by something other than a person, what: what
    /// brings it back.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub down_with: Option<DownWith>,
    /// For one taken down by something other than a person whose
    /// bring-back is refused, why it stays down and the way through: it
    /// would come on on the registered build while other triggers that are
    /// on fire another, and one binary answers a project's routes, so
    /// `weft resync --all` moves those first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub held_back: Option<String>,
    /// What it holds while down: the arrivals its door held and the runs
    /// it fired that are paused.
    #[serde(default, skip_serializing_if = "HeldWork::is_empty")]
    pub held: HeldWork,
    /// An instance's fires held because the instance has not given (or
    /// gave an invalid) value they need: how many, and why.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting: Option<crate::program::WaitingFires>,
    /// The version of the source its fires run (`weft tree` shows where
    /// it sits), once its setup finished; `None` while it is set up and
    /// once it is taken down.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// While it turns on: since when, and what its activation waits on
    /// right now ("its setup run <id>").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<crate::infra::wire::ChangeProgress>,
    /// The fires and calls dropped in the last minute or two: refused
    /// before any run started, which nothing tries again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dropped: Option<Dropped>,
}

/// [`TriggerCopy::state`] on the wire: `state`, and `on` beside it.
mod state_and_on {
    use super::TriggerState;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize)]
    struct Written {
        state: TriggerState,
        on: bool,
    }

    /// `on` is left unread: it is the state's, said again.
    #[derive(Deserialize)]
    struct Read {
        state: TriggerState,
    }

    pub fn serialize<S: Serializer>(state: &TriggerState, serializer: S) -> Result<S::Ok, S::Error> {
        Written { state: *state, on: state.is_on() }.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<TriggerState, D::Error> {
        Ok(Read::deserialize(deserializer)?.state)
    }
}

/// The fires and calls of a trigger dropped in the last minute or two:
/// refused before any run started (its program cannot run what they
/// reach, it takes no work, it has no armed program), by its worker's
/// door or by the install, counted where the door counts its runs
/// (`weft_core::signal::limits::dropped_key`).
// SYNC: Dropped <-> packages/weft-graph/src/protocol.ts Dropped
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Dropped {
    pub count: u64,
    /// Why the last one was dropped, as the door said it.
    pub last_reason: String,
}

impl Dropped {
    /// "3 fires or calls dropped in the last minute or two (refused before
    /// any run started), the last because: ...".
    // SYNC: Dropped::describe <-> packages/weft-graph/src/webview/lib/lifecycle.ts describeDropped
    pub fn describe(&self) -> String {
        let what = if self.count == 1 { "1 fire or call".to_string() } else { format!("{} fires or calls", self.count) };
        format!("{what} dropped in the last minute or two (refused before any run started), the last because: {}", self.last_reason)
    }
}

/// The work held on triggers that are down: the arrivals their door held
/// (events, calls) and the runs they fired that are paused on a wait (an
/// answer queued for one is that run's). What a take-down keeps or drops,
/// and what `weft activate` releases.
// SYNC: HeldWork <-> packages/weft-graph/src/protocol.ts HeldWork
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeldWork {
    pub arrivals: u64,
    pub paused: u64,
}

impl HeldWork {
    pub fn is_empty(&self) -> bool {
        self.arrivals == 0 && self.paused == 0
    }

    /// "3 arrivals and 1 paused run", naming only what there is.
    // SYNC: HeldWork::describe <-> packages/weft-graph/src/webview/lib/lifecycle.ts describeHeld
    pub fn describe(&self) -> String {
        let counted = |n: u64, one: &str, many: &str| if n == 1 { format!("1 {one}") } else { format!("{n} {many}") };
        match (self.arrivals, self.paused) {
            (0, 0) => "nothing".into(),
            (a, 0) => counted(a, "arrival", "arrivals"),
            (0, p) => counted(p, "paused run", "paused runs"),
            (a, p) => format!("{} and {}", counted(a, "arrival", "arrivals"), counted(p, "paused run", "paused runs")),
        }
    }
}

impl std::ops::AddAssign for HeldWork {
    fn add_assign(&mut self, other: Self) {
        self.arrivals += other.arrivals;
        self.paused += other.paused;
    }
}

/// Desired vs running drift, one bit per thing that can lag.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectDrift {
    /// Some infra copy was applied from other infra than the source holds
    /// now (`ProjectInfraEntry::behind`): `weft infra upgrade` is offered.
    pub infra_drift: bool,
    /// "Worker BINARY needs rebuilding." Flips on engine, node code,
    /// type-set or `weft.toml` edits; the dialog before running asks
    /// about killing running runs when it is set.
    pub binary_drift: bool,
    /// "Project SHAPE has changed." A config or topology edit flips it
    /// without flipping `binary_drift`; the next run picks up the
    /// new definition.
    pub definition_drift: bool,
}

/// One infra place in the status answer.
// SYNC: ProjectInfraEntry <-> packages/weft-graph/src/protocol.ts InfraPlacementStatus
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectInfraEntry {
    /// The node's placement, spelled the way the PROGRAM writes the node
    /// (`db`, or `one.db` inside the file the site `one` includes): what
    /// a person is shown, what the editor matches, and what every
    /// per-node verb takes. A file included twice lists its infra twice.
    pub node: String,
    /// Infra node type (e.g. "whatsapp_bridge"), so the extension can
    /// decorate the node without re-parsing the source.
    pub node_type: String,
    /// The copy's state (`provisioning`, `running`, `flaky`, `stopping`,
    /// `stopped`, `terminating`, `failed`),
    /// [`INFRA_NOT_STARTED`](crate::infra::wire::INFRA_NOT_STARTED) for a
    /// place with no copy and nothing starting one, or
    /// [`INFRA_PER_INSTANCE`](crate::infra::wire::INFRA_PER_INSTANCE) for
    /// a `@per_instance` place, which has no shared copy.
    pub status: String,
    pub endpoint_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_stage: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_message: Option<String>,
    /// For a `per_instance` place: how many instances have a copy (each
    /// is listed in `instance_infra`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_copy_count: Option<usize>,
    /// How far a start, stop or terminate of the copy got, while one is
    /// under way.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<crate::infra::wire::ChangeProgress>,
    /// Its copy was applied from other infra than the source holds now:
    /// `weft infra upgrade --infra <node>` brings it up to date.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub behind: bool,
    /// Its copy is live and the source declares its place no more (the
    /// person deleted the node): an orphan, counted in
    /// [`ProjectStatusResponse::orphaned_infra`], that keeps running until
    /// `weft infra stop` or `weft infra terminate` takes it down. Never
    /// [`Self::behind`] too: nothing brings it up to date.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub orphaned: bool,
    /// The triggers on whose cut contains it (each instance's too for a
    /// shared one), by owner: what taking it down takes down with it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub takes_down: Vec<crate::activation::OwnerTriggers>,
    /// The latest time one of its copy's containers stopped on its own
    /// (ran out of memory, exited) and its host brought it back, since
    /// weft last started the copy: what `weft status` and the editor tell,
    /// with what to do about it ([`LastRestart::describe`](crate::infra::LastRestart::describe)).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_restart: Option<crate::infra::LastRestart>,
}

// SYNC: ProjectRunsSummary <-> packages/weft-graph/src/status.ts RawStatusPayload.runs
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectRunsSummary {
    /// The program's own runs (fires and manual runs): what `weft tree`
    /// lists.
    pub total: usize,
    /// The runs that set the program up (arming its triggers, bringing its
    /// infra up), which `total` leaves out.
    #[serde(default)]
    pub setup: usize,
    pub last_completed_at: Option<u64>,
    pub last_run_id: Option<String>,
    pub last_status: Option<crate::program::RunStatus>,
    /// Every run running right now (suspended ones excluded), the
    /// same set `running_count` counts. The editor REPLACES its own
    /// running set with this on every status refresh, so a terminal event
    /// lost to a dropped stream never leaves a Stop button on a run that
    /// ended.
    ///
    /// OLDEST FIRST, so the last one is the most recently started: the
    /// editor's action bar follows "the latest run" and nothing else here
    /// says which that is. A queued run with no journal row yet sorts
    /// last, which is right, it is the newest thing in the list.
    ///
    /// Each carries its phase: the setup an `infra start` or an
    /// activation runs is running too, and the editor shows it as that
    /// verb working instead of as a run with a Stop button.
    pub running: Vec<GoingRun>,
}

// SYNC: GoingRun <-> packages/weft-graph/src/status.ts GoingRun
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoingRun {
    pub run_id: String,
    pub phase: crate::primitive::Phase,
}

#[cfg(test)]
mod tests {
    use super::*;

    crate::wire_enum_roundtrip_tests!(ProjectStatus, ProjectTransition);

    /// A trigger turning on says since when and what it waits on, in the
    /// editor's spelling; one that is not turning on says nothing of it.
    #[test]
    fn a_trigger_turning_on_carries_its_progress_on_the_wire() {
        let copy = TriggerCopy {
            instance: None,
            state: TriggerState::TurningOn,
            mode: crate::activation::ActivationMode::Registered,
            down_with: None,
            held_back: None,
            held: HeldWork::default(),
            waiting: None,
            version: None,
            progress: Some(crate::infra::wire::ChangeProgress { since_unix: 10, as_of_unix: 250, waiting: Some("its setup run r1".into()) }),
            dropped: None,
        };
        let wire = serde_json::to_value(&copy).unwrap();
        assert_eq!(wire["progress"], serde_json::json!({ "sinceUnix": 10, "asOfUnix": 250, "waiting": "its setup run r1" }));
        let read: TriggerCopy = serde_json::from_value(wire).unwrap();
        assert_eq!(read.progress, copy.progress);
        let resting = serde_json::to_value(TriggerCopy { progress: None, ..copy }).unwrap();
        assert!(resting.get("progress").is_none());
    }

    /// A trigger is on when its door takes fires, whichever program they
    /// run.
    #[test]
    fn a_trigger_on_or_behind_is_on() {
        assert!(TriggerState::On.is_on());
        assert!(TriggerState::Behind.is_on());
        assert!(!TriggerState::Off.is_on());
        assert!(!TriggerState::TurningOn.is_on());
    }

    /// Every copy carries `on` beside its state, said from the state; it
    /// reads back as written, and `on` is never read, so a copy written
    /// without it reads the same.
    #[test]
    fn a_trigger_copy_says_whether_it_is_on() {
        for (state, on) in
            [(TriggerState::Off, false), (TriggerState::TurningOn, false), (TriggerState::On, true), (TriggerState::Behind, true)]
        {
            let copy = TriggerCopy {
                instance: Some(crate::instance::InstanceId::new("ada").unwrap()),
                state,
                mode: crate::activation::ActivationMode::Active,
                down_with: None,
                held_back: None,
                held: HeldWork::default(),
                waiting: None,
                version: Some("v4".into()),
                progress: None,
                dropped: None,
            };
            let wire = serde_json::to_value(&copy).unwrap();
            assert_eq!(wire["state"], serde_json::json!(state.as_str()));
            assert_eq!(wire["on"], serde_json::json!(on), "{state:?}");
            let read: TriggerCopy = serde_json::from_value(wire.clone()).unwrap();
            assert_eq!(serde_json::to_value(&read).unwrap(), wire, "{state:?} reads back as written");
            let mut without_on = wire;
            without_on.as_object_mut().unwrap().remove("on");
            assert_eq!(serde_json::from_value::<TriggerCopy>(without_on).unwrap().state, state);
        }
    }

    /// A trigger that dropped fires or calls says how many and why, in the
    /// editor's spelling; one that dropped none says nothing of it.
    #[test]
    fn what_a_trigger_dropped_rides_the_wire() {
        let copy = TriggerCopy {
            instance: None,
            state: TriggerState::On,
            mode: crate::activation::ActivationMode::Active,
            down_with: None,
            held_back: None,
            held: HeldWork::default(),
            waiting: None,
            version: None,
            progress: None,
            dropped: Some(Dropped { count: 3, last_reason: "package 'relay' didn't compile".into() }),
        };
        let wire = serde_json::to_value(&copy).unwrap();
        assert_eq!(wire["dropped"], serde_json::json!({ "count": 3, "lastReason": "package 'relay' didn't compile" }));
        let read: TriggerCopy = serde_json::from_value(wire).unwrap();
        assert_eq!(read.dropped, copy.dropped);
        assert!(serde_json::to_value(TriggerCopy { dropped: None, ..copy }).unwrap().get("dropped").is_none());
        let one = Dropped { count: 1, last_reason: "the trigger is gone".into() };
        assert_eq!(
            one.describe(),
            "1 fire or call dropped in the last minute or two (refused before any run started), the last because: the trigger is gone"
        );
    }

    /// The status body's wire shape: camelCase keys, the hashes by name
    /// as maps, a trigger whose cut cannot be walked as `null`, nothing a
    /// client leaves out written (a source with no trigger or no infra
    /// still sends its empty map), and an unknown key (a misspelt one, which would
    /// silently compare nothing) refused.
    #[test]
    fn a_status_request_names_only_the_hashes_given() {
        assert_eq!(serde_json::to_value(StatusRequest::default()).unwrap(), serde_json::json!({}));
        let request = StatusRequest {
            desired_binary_hash: Some("b".into()),
            desired_trigger_cuts: Some([("one.door".to_string(), Some("c1".to_string())), ("one.loop".to_string(), None)].into()),
            desired_infra_places: Some([("db".to_string(), "i".to_string())].into()),
            ..Default::default()
        };
        let wire = serde_json::to_value(&request).unwrap();
        assert_eq!(
            wire,
            serde_json::json!({
                "desiredBinaryHash": "b",
                "desiredTriggerCuts": { "one.door": "c1", "one.loop": null },
                "desiredInfraPlaces": { "db": "i" }
            })
        );
        assert_eq!(serde_json::from_value::<StatusRequest>(wire).unwrap(), request);
        let no_trigger = StatusRequest {
            desired_trigger_cuts: Some(Default::default()),
            desired_infra_places: Some(Default::default()),
            ..Default::default()
        };
        let wire = serde_json::to_value(&no_trigger).unwrap();
        assert_eq!(wire, serde_json::json!({ "desiredTriggerCuts": {}, "desiredInfraPlaces": {} }));
        assert_eq!(serde_json::from_value::<StatusRequest>(wire).unwrap(), no_trigger);
        assert!(serde_json::from_value::<StatusRequest>(serde_json::json!({ "desired_binary_hash": "b" })).is_err());
    }
}

/// A project's own address: where its callers reach its routes, at the
/// root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ProjectAddress {
    Serving { url: String },
    /// It has one and it cannot be served right now, and why (another
    /// program holds its port).
    Unavailable { why: String },
}
