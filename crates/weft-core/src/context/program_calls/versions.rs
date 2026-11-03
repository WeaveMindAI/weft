//! `ctx.project(id).code()`: a project's code, as a program writes,
//! builds and runs it. Everything a person does to a project's code from
//! the CLI, with the same parameters and the same refusals: read and write
//! a working copy's files, stamp them into a version (`checkpoint`), move a
//! working copy to another version (`switch_to`), read the version tree,
//! build, and start a run (`run()`, which builds first, as `weft run`
//! does).
//!
//! A working copy is named with `.working_copy(id)`; without one, a call
//! acts on the project's default: its most recently changed folder (so the
//! person's folder follows what the program writes for it, written to disk
//! by their runtime), else the copy programs write into.

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use crate::builds::{BuildAnswer, BuildRequest, BuildsUnderway, BuiltProgram, NodeSet, RunPreview, VersionBuildState, VersionBuildStatus};
use crate::error::{WeftError, WeftResult};
use crate::program::runs::RunCall;
use crate::program::versions::VersionCall;
use crate::program::ProgramCall;
use crate::run_flags::RunFlags;
use crate::context::kept::Step;
use crate::context::Kept;
use crate::signal::timer::{Timer, TimerSpec};
use crate::signal::to_spec;
use crate::storage::StoredFile;
use crate::tag::StopSelf;
use crate::versions::{CheckpointRequest, PruneResponse, RunShape, RunStarted, RunningSource, VersionRunRequest, VersionTree, VersionUpsert};
use crate::working_copy::{
    Applied, Expect, FileContent, FileWrite, Forgotten, SwitchRequest, SwitchTo, Switched, WorkingCopy, WorkingCopyId, WorkingCopyRef,
    WorkingState, WriteFiles,
};
use crate::RunId;

use super::ProjectCalls;

/// How long a build being followed waits between looks.
const BUILD_LOOK_AGAIN: Timer = Timer { spec: TimerSpec::After { duration_ms: 5_000 } };

impl<'a> ProjectCalls<'a> {
    /// The project's code: its default working copy, or the one named with
    /// [`CodeCalls::working_copy`].
    pub fn code(self) -> CodeCalls<'a> {
        CodeCalls { at: self, copy: WorkingCopyRef::default() }
    }

    /// Every working copy of the project, each with its head.
    pub async fn working_copies(self) -> WeftResult<Vec<WorkingCopy>> {
        self.call(ProgramCall::Versions(VersionCall::WorkingCopies), StopSelf::Keep).await
    }

    /// Record the working copy `copy`'s files as a version
    /// ([`Forgotten::saved`]) and remove the copy, as `weft forget-copy`
    /// does: a folder that is gone, or the program copy once nothing writes
    /// into it. A program copy comes back on its own the next time a call
    /// names no working copy of a project that has none.
    pub async fn forget_working_copy(self, copy: WorkingCopyId) -> WeftResult<Forgotten> {
        self.call(ProgramCall::Versions(VersionCall::Forget { copy }), StopSelf::Keep).await
    }

    /// The project's version tree, as `weft tree --json` reads it.
    pub async fn tree(self) -> WeftResult<VersionTree> {
        self.call(ProgramCall::Versions(VersionCall::Tree), StopSelf::Keep).await
    }

    /// The files of the build the project runs.
    pub async fn running_source(self) -> WeftResult<RunningSource> {
        self.call(ProgramCall::Versions(VersionCall::Running), StopSelf::Keep).await
    }

    /// Delete the version `version` and everything under it, with their
    /// runs, as `weft prune` does; `frozen` is every `frozen_from.version`
    /// of the project's examples, which a prune never deletes under.
    /// `plan_only` says what it would remove and removes nothing.
    pub async fn prune(self, version: impl Into<String>, frozen: Vec<String>, plan_only: bool) -> WeftResult<PruneResponse> {
        let call = VersionCall::Prune { version: version.into(), frozen, plan: plan_only };
        self.call(ProgramCall::Versions(call), StopSelf::Keep).await
    }

    /// Remember that the run `run` was frozen as the example `example`
    /// (`None`: forget it), as `weft freeze` does.
    pub async fn set_run_example(self, run: RunId, example: Option<String>) -> WeftResult<()> {
        let call = VersionCall::UpdateRun { run, update: crate::versions::RunUpdate { example: Some(example) } };
        self.call::<Value>(ProgramCall::Versions(call), StopSelf::Keep).await.map(drop)
    }
}

/// A project's code, through one of its working copies. See
/// [`ProjectCalls::code`].
#[derive(Clone, Copy)]
pub struct CodeCalls<'a> {
    at: ProjectCalls<'a>,
    copy: WorkingCopyRef,
}

impl<'a> CodeCalls<'a> {
    /// Act on this working copy rather than the project's default.
    pub fn working_copy(mut self, id: WorkingCopyId) -> Self {
        self.copy = WorkingCopyRef { working_copy: Some(id) };
        self
    }

    /// The working copy and every file it holds (`path -> sha256`).
    pub async fn state(self) -> WeftResult<WorkingState> {
        self.at.call(ProgramCall::Versions(VersionCall::WorkingState { copy: self.copy }), StopSelf::Keep).await
    }

    /// The file at `path`, as a stored file to read with
    /// `ctx.storage(..).get(..)`; `None` when the working copy holds none.
    pub async fn file(self, path: impl Into<String>) -> WeftResult<Option<StoredFile>> {
        let call = VersionCall::ReadFile { copy: self.copy, path: path.into() };
        self.at.call(ProgramCall::Versions(call), StopSelf::Keep).await
    }

    /// The text of the file at `path`; `None` when the working copy holds
    /// none. A file that is not text is refused.
    pub async fn read_text(self, path: impl Into<String>) -> WeftResult<Option<String>> {
        let path = path.into();
        let Some(file) = self.file(path.clone()).await? else { return Ok(None) };
        let (_, bytes) = self
            .at
            .ctx
            .storage(crate::storage::StorageScope::Asset)
            .get_bytes(&crate::storage::FileHandle::key(file.key))
            .await?;
        String::from_utf8(bytes.to_vec()).map(Some).map_err(|_| WeftError::Input(format!("{path} is not text")))
    }

    /// Put `text` at `path`, whatever stands there now.
    pub async fn write_text(self, path: impl Into<String>, text: impl Into<String>) -> WeftResult<Applied> {
        self.edit().text(path, text).apply().await
    }

    /// Remove the file at `path`.
    pub async fn remove(self, path: impl Into<String>) -> WeftResult<Applied> {
        self.edit().remove(path).apply().await
    }

    /// Several files changed together: chain [`FileEdit::text`],
    /// [`FileEdit::stored`], [`FileEdit::blob`] and [`FileEdit::remove`], then
    /// [`FileEdit::apply`]. Every file lands, or none does.
    pub fn edit(self) -> FileEdit<'a> {
        FileEdit { code: self, files: Vec::new() }
    }

    /// Record the working copy's files as a version under its head, with
    /// no build and no run, as `weft checkpoint` does. Identical code is the
    /// version it already is.
    pub async fn checkpoint(self, label: Option<String>) -> WeftResult<VersionUpsert> {
        let request = CheckpointRequest { working_copy: self.copy, label, root: false };
        self.at.call(ProgramCall::Versions(VersionCall::Checkpoint { request }), StopSelf::Keep).await
    }

    /// Move the working copy to the version `version`, as `weft branch`
    /// does: what it held is recorded as a version first
    /// ([`Switched::saved`]), so switching back brings it back.
    pub async fn switch_to(self, version: impl Into<String>) -> WeftResult<Switched> {
        self.switch(SwitchTo { version: Some(version.into()), run: None }).await
    }

    /// Move the working copy to the run `run`: its version, with the run as
    /// head's run, so the next seeded run inherits from it.
    pub async fn switch_to_run(self, run: RunId) -> WeftResult<Switched> {
        self.switch(SwitchTo { version: None, run: Some(run) }).await
    }

    async fn switch(self, to: SwitchTo) -> WeftResult<Switched> {
        let request = SwitchRequest { working_copy: self.copy, to };
        self.at.call(ProgramCall::Versions(VersionCall::Switch { request }), StopSelf::Keep).await
    }

    /// Build the working copy's files (stamped into a version first), as
    /// `weft build` does.
    pub fn build(self) -> BuildCalls<'a> {
        BuildCalls { code: self, request: BuildRequest { working_copy: self.copy, ..Default::default() } }
    }

    /// Start a run of the working copy's code on the project, as `weft
    /// run` does: chain what it starts with, then [`RunCalls::start`],
    /// which builds the code and starts the run on that build.
    pub fn run(self) -> RunCalls<'a> {
        RunCalls { code: self, flags: RunFlags::default(), example: None, seed: false, seed_run: None, seed_until: Vec::new(), seed_before: Vec::new(), node_set: NodeSet::Full, root: false }
    }

    /// The working copy this names, resolved now: a run and the build it
    /// runs on have to name the same one.
    async fn resolved(self) -> WeftResult<Self> {
        if self.copy.working_copy.is_some() {
            return Ok(self);
        }
        let state = self.state().await?;
        Ok(self.working_copy(state.copy.id))
    }
}

/// Files changed together. See [`CodeCalls::edit`].
pub struct FileEdit<'a> {
    code: CodeCalls<'a>,
    files: Vec<FileWrite>,
}

impl FileEdit<'_> {
    /// Put `text` at `path` (at most [`crate::working_copy::MAX_WRITTEN_TEXT_BYTES`];
    /// a larger file goes through [`Self::stored`]).
    pub fn text(mut self, path: impl Into<String>, text: impl Into<String>) -> Self {
        self.files.push(FileWrite { path: path.into(), content: FileContent::Text { text: text.into() }, expected: Expect::Any });
        self
    }

    /// Put the bytes of a file this program stored (`ctx.storage(..)`, by
    /// its key) at `path`: a large or binary file.
    pub fn stored(mut self, path: impl Into<String>, key: impl Into<String>) -> Self {
        self.files.push(FileWrite { path: path.into(), content: FileContent::Stored { key: key.into() }, expected: Expect::Any });
        self
    }

    /// Put the file whose sha256 is `hash` at `path`: one the owner already
    /// holds, as a working copy's or a version's manifest names it
    /// ([`CodeCalls::state`]). Nothing is read or stored again, so copying
    /// a whole project is one call. A `hash` that is no sha256 is refused.
    pub fn blob(self, path: impl Into<String>, hash: impl AsRef<str>) -> WeftResult<Self> {
        let key = crate::storage::key::scope_key(&crate::storage::key::KeyScope::Asset, hash.as_ref()).map_err(WeftError::Input)?;
        Ok(self.stored(path, key))
    }

    /// Remove the file at `path`.
    pub fn remove(mut self, path: impl Into<String>) -> Self {
        self.files.push(FileWrite { path: path.into(), content: FileContent::Removed, expected: Expect::Any });
        self
    }

    /// Land the last change only while its path still holds `hash` (the
    /// sha256 a read of it saw; `None`: no file): two writers of one file
    /// then never overwrite each other unseen, and the stale one is
    /// answered [`Applied::Stale`].
    pub fn expecting(mut self, hash: Option<String>) -> Self {
        if let Some(last) = self.files.last_mut() {
            last.expected = Expect::read(hash.as_ref());
        }
        self
    }

    pub async fn apply(self) -> WeftResult<Applied> {
        let request = WriteFiles { working_copy: self.code.copy, files: self.files };
        self.code.at.call(ProgramCall::Versions(VersionCall::WriteFiles { request }), StopSelf::Keep).await
    }
}

/// A build being asked for. See [`CodeCalls::build`].
pub struct BuildCalls<'a> {
    code: CodeCalls<'a>,
    request: BuildRequest,
}

impl BuildCalls<'_> {
    /// Build this version as it is, stamping nothing.
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.request.version = Some(version.into());
        self
    }

    /// Build only the node types the program uses (`--referenced`).
    pub fn referenced(mut self) -> Self {
        self.request.node_set = NodeSet::Referenced;
        self
    }

    /// Stamp the working copy as a new tree root (`--root`).
    pub fn root(mut self) -> Self {
        self.request.root = true;
        self
    }

    /// Build, and return once the build registered, following its image
    /// builds for as long as they take: between looks the run pauses on a
    /// timer, holding no worker. A build that failed or was cancelled fails
    /// this call with why.
    pub async fn start<S: Serialize + DeserializeOwned + Send>(self, state: &mut Kept<S>) -> WeftResult<BuiltProgram> {
        let request = &self.request;
        let (built, _) = build_and_follow(self.code.at, state, "versions.build", request, || async { Ok(request.clone()) }).await?;
        Ok(built)
    }
}

/// Ask for the build `request` makes, once (the build it starts, and the
/// working copy it is of, are kept with a pause), and follow that build
/// until it registers, pausing on a timer between looks. `helper` and
/// `call` say which helper asks and for what (`Kept::follow`). Answers the
/// program and the working copy it was built from.
async fn build_and_follow<S, F, Fut>(
    at: ProjectCalls<'_>,
    kept: &mut Kept<S>,
    helper: &'static str,
    call: &impl Serialize,
    request: F,
) -> WeftResult<(BuiltProgram, WorkingCopyRef)>
where
    S: Serialize + DeserializeOwned + Send,
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = WeftResult<BuildRequest>>,
{
    let request = &request;
    kept.follow(helper, &(at.project, call), to_spec(BUILD_LOOK_AGAIN), None::<(WorkingCopyRef, BuildsUnderway)>, move |underway| async move {
        let Some((copy, underway)) = underway else {
            let request = request().await?;
            let copy = request.working_copy;
            let build = VersionCall::Build { request: Box::new(request) };
            return Ok(match at.call::<BuildAnswer>(ProgramCall::Versions(build), StopSelf::Keep).await? {
                BuildAnswer::Registered { program } => Step::Done((*program, copy)),
                BuildAnswer::Underway { underway } => Step::LookAgain(Some((copy, underway))),
            });
        };
        let state: VersionBuildState = at.call(ProgramCall::Versions(VersionCall::BuildState { build: underway.build }), StopSelf::Keep).await?;
        let why = || state.reason.clone().unwrap_or_else(|| "the install gave no reason".into());
        match state.state {
            VersionBuildStatus::Waiting => Ok(Step::LookAgain(Some((copy, underway)))),
            VersionBuildStatus::Registered => state
                .program
                .clone()
                .map(|program| Step::Done((program, copy)))
                .ok_or_else(|| WeftError::NodeFailed("the build registered, and its answer names no program".into())),
            VersionBuildStatus::Failed => Err(WeftError::NodeFailed(format!("the build of version {} failed: {}", underway.version, why()))),
            VersionBuildStatus::Cancelled => Err(WeftError::NodeFailed(format!("the build of version {} was cancelled", underway.version))),
            VersionBuildStatus::Superseded => {
                Err(WeftError::NodeFailed(format!("the build of version {} will not be registered: {}", underway.version, why())))
            }
        }
    })
    .await
}

/// A run being asked for. See [`CodeCalls::run`]. Each setter is the
/// `weft run` flag of the same name.
pub struct RunCalls<'a> {
    code: CodeCalls<'a>,
    flags: RunFlags,
    example: Option<String>,
    seed: bool,
    seed_run: Option<RunId>,
    seed_until: Vec<String>,
    seed_before: Vec<String>,
    node_set: NodeSet,
    root: bool,
}

impl RunCalls<'_> {
    /// Start from the saved example `examples/<name>.json` of the code,
    /// edited by whatever else is chained.
    pub fn example(mut self, name: impl Into<String>) -> Self {
        self.example = Some(name.into());
        self
    }

    /// `--from node={..}`: run from `node`, handing it these port values.
    pub fn from(mut self, node: impl Into<String>, ports: Value) -> Self {
        self.flags.from.push(format!("{}={ports}", node.into()));
        self
    }

    /// `--target node`: run what `node` needs, and stop there.
    pub fn target(mut self, node: impl Into<String>) -> Self {
        self.flags.target.push(node.into());
        self
    }

    /// `--before node`: run up to, not including, `node`.
    pub fn before(mut self, node: impl Into<String>) -> Self {
        self.flags.before.push(node.into());
        self
    }

    /// `--group group={..}`: run one group, handing it these port values.
    pub fn group(mut self, group: impl Into<String>, ports: Value) -> Self {
        self.flags.group = Some(format!("{}={ports}", group.into()));
        self
    }

    /// `--feed node.port`.
    pub fn feed(mut self, place: impl Into<String>) -> Self {
        self.flags.feed.push(place.into());
        self
    }

    /// `--fire trigger={..}`: fire a trigger with this payload.
    pub fn fire(mut self, trigger: impl Into<String>, payload: Value) -> Self {
        self.flags.fire = vec![format!("{}={payload}", trigger.into())];
        self
    }

    /// `--emit node={..}`: stand in for `node`'s outputs.
    pub fn emit(mut self, node: impl Into<String>, outputs: Value) -> Self {
        self.flags.emit.push(format!("{}={outputs}", node.into()));
        self
    }

    /// `--instance id`: the run is that instance's.
    pub fn instance(mut self, id: impl Into<String>) -> WeftResult<Self> {
        let id = crate::instance::InstanceId::new(id.into()).map_err(|e| WeftError::Input(format!("instance id: {e}")))?;
        self.flags.instance = Some(id);
        Ok(self)
    }

    /// `--durable`.
    pub fn durable(mut self) -> Self {
        self.flags.durable = true;
        self
    }

    /// `--fast`.
    pub fn fast(mut self) -> Self {
        self.flags.fast = true;
        self
    }

    /// `--keep-for`.
    pub fn keep_for(mut self, keep_for: crate::run_settings::KeepFor) -> Self {
        self.flags.keep_for = Some(keep_for);
        self
    }

    /// `--seed`: reuse what head's run made of every node whose code did
    /// not change.
    pub fn seed(mut self) -> Self {
        self.seed = true;
        self
    }

    /// `--seed=<run>`: reuse from this run instead.
    pub fn seed_run(mut self, run: RunId) -> Self {
        self.seed_run = Some(run);
        self
    }

    /// `--seed-until node`.
    pub fn seed_until(mut self, node: impl Into<String>) -> Self {
        self.seed_until.push(node.into());
        self
    }

    /// `--seed-before node`.
    pub fn seed_before(mut self, node: impl Into<String>) -> Self {
        self.seed_before.push(node.into());
        self
    }

    /// `--referenced`.
    pub fn referenced(mut self) -> Self {
        self.node_set = NodeSet::Referenced;
        self
    }

    /// `--root`: the code becomes a new tree root.
    pub fn root(mut self) -> Self {
        self.root = true;
        self
    }

    /// Build the code (the run it is for held to the compiled program
    /// before any image builds, so a run that could not start is refused
    /// at once), follow the build, then start the run on it. Answers once
    /// the run started; read how it goes with `ctx.runs()` and the run's
    /// own calls. Following the build pauses the run on a timer between
    /// looks, where `state` comes in.
    pub async fn start<S: Serialize + DeserializeOwned + Send>(self, state: &mut Kept<S>) -> WeftResult<RunStarted> {
        let code = self.code;
        let shape = RunShape { example: self.example, flags: self.flags };
        let seeded = self.seed || self.seed_run.is_some();
        let preview = RunPreview { shape: shape.clone(), seeded, seed_until: self.seed_until.clone(), seed_before: self.seed_before.clone() };
        let (node_set, root) = (self.node_set, self.root);
        // The working copy is resolved in the build's first look, so a
        // body resumed while the build goes on runs on the copy it built,
        // never one read again.
        let preview = &preview;
        let asked = (code.copy, node_set, root, preview);
        let (built, copy) = build_and_follow(code.at, state, "versions.run", &asked, || async move {
            let copy = code.resolved().await?.copy;
            Ok(BuildRequest { working_copy: copy, version: None, node_set, root, run: Some(preview.clone()) })
        })
        .await?;
        let request = VersionRunRequest {
            working_copy: copy,
            version: built.version,
            definition_hash: built.definition_hash,
            binary_hash: built.binary_hash,
            seed: self.seed,
            seed_run: self.seed_run,
            seed_until: self.seed_until,
            seed_before: self.seed_before,
            shape,
        };
        code.at.call(ProgramCall::Runs(RunCall::Start { request: Box::new(request) }), StopSelf::Keep).await
    }
}
