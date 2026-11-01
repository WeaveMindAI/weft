//! What the install's build endpoint (`POST /projects/{id}/builds`)
//! reads and answers, and the CLI sends and reads: one Rust type per
//! message, so the two ends cannot drift.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// What a build produced: the compiled program, its three hashes and the
/// implementations its worker carries, and the infra image every infra
/// place resolves to. The install registers all of it in one transaction
/// and answers it (a 200) once every image the version needs is in the
/// registry; downstream verbs name the build by its hashes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuiltProgram {
    /// The version built: the files it was compiled from, as the version
    /// tree names them.
    pub version: String,
    pub definition: crate::ProjectDefinition,
    /// The worker image's identity.
    #[serde(rename = "binaryHash")]
    pub binary_hash: String,
    /// The runtime shape; a resync compares it.
    #[serde(rename = "definitionHash")]
    pub definition_hash: String,
    /// The infra closure; an upgrade compares it.
    #[serde(rename = "infraHash")]
    pub infra_hash: String,
    pub implementations: BTreeMap<String, String>,
    /// `place -> { image name -> image ref }`, keyed the way each infra
    /// place's row is (`one.db`), which is how the supervisor reads it.
    #[serde(rename = "infraImages")]
    pub infra_images: BTreeMap<String, BTreeMap<String, String>>,
    /// The infra places whose images differ from the ones the build
    /// before registered: a new copy of one starts on the new image, while
    /// a copy already running keeps its own until it is upgraded (`weft
    /// infra upgrade`). Filled at registration, which knows what came
    /// before.
    #[serde(rename = "replacedInfraImages", default)]
    pub replaced_infra_images: Vec<String>,
    /// `place -> hash` of each infra place's own infra (its node, what
    /// feeds it, their packages; the compiler's `compute_infra_place_hashes`):
    /// a copy applied from this build carries its place's, and one whose
    /// place hashes otherwise in the source now is behind it.
    #[serde(rename = "infraPlaces", default)]
    pub infra_places: BTreeMap<String, String>,
    /// The worker image's ref, whose build record says which node
    /// packages it left out ([`LeftOut`]). Empty in a version a release
    /// before records wrote down, whose worker could leave nothing out.
    #[serde(rename = "workerImage", default)]
    pub worker_image: String,
    /// The node packages the worker's build left out because they did not
    /// compile ([`LeftOut`]): no run whose cut uses one of their node types
    /// starts. Filled at registration, from the worker image's record.
    #[serde(rename = "leftOut", default, skip_serializing_if = "Vec::is_empty")]
    pub left_out: Vec<LeftOutPackage>,
    /// Where the worker build's log is read, for every error of a package
    /// it left out (the record keeps only the first line of each).
    #[serde(rename = "workerLog", default, skip_serializing_if = "Option::is_none")]
    pub worker_log: Option<String>,
    /// What each node type the worker holds needs from the machine it runs
    /// on (`crate::needs::NodeNeeds`): a program that uses one whose need
    /// the machine does not meet is refused before it starts.
    #[serde(rename = "nodeNeeds", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub node_needs: crate::needs::NodeNeeds,
}

impl BuiltProgram {
    /// This build, as a later verb names it.
    pub fn named(&self) -> BuildHashes {
        BuildHashes {
            binary_hash: Some(self.binary_hash.clone()),
            definition_hash: Some(self.definition_hash.clone()),
            infra_hash: Some(self.infra_hash.clone()),
        }
    }
}

/// The repository part of a worker's image ref (`weft-worker:<binary
/// hash>`). An image ref is the name a platform's image builder gives a
/// build (`weft_platform_traits::ImageBuilder::image_ref`): on the cloud
/// it names an image in the install's registry; on a local install a
/// worker's and a test server's ref names a program compiled on the
/// machine (`weft_platform_local::images::Program`), and an infra image's
/// an image in the Docker daemon.
pub const WORKER_REPO: &str = "weft-worker";

/// The repository part of a node package's test server's ref
/// (`weft-node-tests:<test hash>`), a program on the machine that runs
/// the live test.
pub const NODE_TEST_REPO: &str = "weft-node-tests";

/// How a platform makes a project's worker, answered by its image
/// builder (`weft_platform_traits::ImageBuilder::worker_packaging`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerPackaging {
    /// An image a container runtime runs (a cloud), compiled FROM
    /// `builder`, the prebuilt builder base the install was set up with.
    Image { builder: String },
    /// A program the machine runs as a process, compiled on it (a local
    /// install). Every such build on the machine shares `shared_crates`:
    /// the worker-linked slice of the weft workspace and every package
    /// crate, each written once.
    Host { shared_crates: std::path::PathBuf },
}

/// How one planned artifact is built.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildRecipe {
    /// `docker build` (or a cloud builder) of the Dockerfile in the build's
    /// context: every infra image, and a worker packaged as an image.
    /// `result_file` is a file inside the built image whose bytes are the
    /// build's result (`LEFT_OUT_RECORD_PATH` for a worker built from a
    /// built-in template), read once the build succeeded without running
    /// the image; `None` for an image with nothing to report (an infra
    /// image, a worker from a custom template).
    Dockerfile { result_file: Option<String> },
    /// Compiled on the machine from the [`HostRecipe`] in the build's
    /// context ([`HOST_RECIPE_FILE`]): a worker packaged as a host program,
    /// whose recipe names its record.
    Host,
}

/// The file in a host build's context that says how to build it.
pub const HOST_RECIPE_FILE: &str = "host-recipe.json";

/// The lock beside `shared`, the folder of crates every host build on a
/// machine shares: held shared by every build from before it stages into
/// the folder until it is compiled, and alone by whatever removes from it,
/// so nothing a build is about to compile is removed.
pub fn shared_crates_lock(shared: &std::path::Path) -> std::path::PathBuf {
    shared.with_extension("lock")
}

/// How a worker is compiled on the machine that runs it, written by the
/// compiler (`weft_compiler::build`) into the build's context and run by
/// the machine's image builder (`weft_platform_local::images`): the same
/// steps a worker image's builder stage takes (`weft-isolate`, then the
/// binary and the record of what it left out), with the machine's own
/// toolchain and compile cache.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HostRecipe {
    /// The worker crate: the folder holding its `Cargo.toml`.
    pub crate_dir: std::path::PathBuf,
    /// The `weft-isolate` helper crate's folder, built first and run to
    /// build the worker.
    pub isolate_dir: std::path::PathBuf,
    /// The candidates file `weft-isolate` reads (its `isolate.json`).
    pub isolate_candidates: std::path::PathBuf,
    /// Where the build writes the record of the node packages it left out
    /// ([`LeftOut`]).
    pub left_out_record: std::path::PathBuf,
    /// The lock file copied beside both crates before they build, so every
    /// worker resolves the dependencies weft was tested with.
    pub seed_lock: std::path::PathBuf,
    /// The worker binary's name under `<target>/release/`.
    pub binary_name: String,
    /// What the nodes' `[build.env]` set (`{{catalog_path}}` expanded), in
    /// the environment of every cargo the build runs, over the machine's
    /// own, as an image's `ENV` lines are: there cargo reads the variables
    /// it takes for itself (`RUSTFLAGS`) and hands every one on to build
    /// scripts and rustc.
    pub build_env: std::collections::BTreeMap<String, String>,
    /// The node types that set each variable of [`Self::build_env`]:
    /// what a build refused for its `[build.env]` names.
    pub build_env_set_by: std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
    /// What tells this build's compile cache apart from the one builds
    /// whose nodes set no `[build.env]` share, `None` for those
    /// (`weft_compiler::worker_image::NodesBuildEnv::cache_tag`).
    pub cache_tag: Option<String>,
}

/// Where a worker image (a cloud's) keeps the record of the node packages
/// its build left out ([`LeftOut`]): the install reads it from the built
/// image without running it, and never trusts a record from anywhere
/// else. A worker compiled on the machine keeps its record beside the
/// program instead.
// SYNC: LEFT_OUT_RECORD_PATH <-> crates/weft-compiler/src/worker_image.rs (RUNTIME_STAGE_FRAGMENT, the record's COPY line)
pub const LEFT_OUT_RECORD_PATH: &str = "/weft-left-out.json";

/// The most bytes a record may take: what a Cloud Build step may hand back
/// as its output. The build shortens each package's first error line to
/// fit, and fails outright when even that does not fit (the full errors
/// are in its log either way).
// SYNC: LEFT_OUT_RECORD_LIMIT <-> crates/weft-compiler/src/worker_isolate.rs RECORD_LIMIT
pub const LEFT_OUT_RECORD_LIMIT: usize = 4000;

/// The node packages a worker image's build left out because they did
/// not compile: the image is the worker without them, and their node types
/// answer "package X didn't compile" if a run ever reaches one. Written by
/// the build itself (`weft_compiler::worker_isolate`), stored on the
/// install's ledger row for the image, and read back whenever the image is
/// used, so a cached image refuses the same runs. Empty for an image that
/// left nothing out.
// SYNC: LeftOut <-> crates/weft-compiler/src/worker_isolate.rs Record
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LeftOut {
    #[serde(default)]
    pub packages: Vec<LeftOutPackage>,
}

/// One package a worker's build left out ([`LeftOut`]).
// SYNC: LeftOutPackage <-> crates/weft-compiler/src/worker_isolate.rs LeftOutPackage
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LeftOutPackage {
    /// The package's name, as its folder or `package.toml` says.
    pub package: String,
    /// Every node type the package holds that the worker would have
    /// compiled in.
    pub node_types: Vec<String>,
    /// The first line of its first compile error, shortened to fit the
    /// record; the build log has them all.
    pub error: String,
}

impl LeftOut {
    /// A record as a build wrote it. A record that does not read is the
    /// build's failure, never an empty record.
    pub fn parse(bytes: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(bytes).map_err(|e| format!("the worker build's record of the packages it left out does not read: {e}"))
    }

    pub fn is_empty(&self) -> bool {
        self.packages.is_empty()
    }

    /// The broken parts these packages make of `definition`: one per node
    /// of a left-out type (`crate::project::Broken`), so a cut that keeps
    /// one is refused naming the package and its first error, and every
    /// other cut runs. `log` is where the build's full errors are read.
    pub fn broken_parts(packages: &[LeftOutPackage], definition: &crate::ProjectDefinition, log: Option<&str>) -> Vec<crate::project::Broken> {
        let mut parts = Vec::new();
        for node in &definition.nodes {
            let Some(package) = packages.iter().find(|p| p.node_types.contains(&node.node_type)) else { continue };
            parts.push(crate::project::Broken {
                parts: vec![node.part().to_string()],
                places: Vec::new(),
                why: format!("{}: {}", crate::project::plain_id(&node.id), package.reason(log)),
                hides: false,
            });
        }
        parts
    }
}

impl LeftOutPackage {
    /// What a person is told about a run that reaches one of its nodes. A
    /// package of the project's copy of the standard nodes that no longer
    /// compiles is that copy being older than this weft, and the one
    /// command that brings it up to date is named.
    pub fn reason(&self, log: Option<&str>) -> String {
        let log = match log {
            Some(url) => format!("its full errors are in the build log: {url}"),
            None => "its full errors are in the build log".to_string(),
        };
        let stale = match self.error.contains("/base_catalog/") {
            true => " (the project's copy of the standard nodes is older than this weft: `weft catalog update` in its folder brings it up to date)",
            false => "",
        };
        format!("package '{}' didn't compile: {}; {log}{stale}", self.package, self.error)
    }
}

/// The build a verb names (an activate, an infra sync), flattened into its
/// body. The install runs what the last build registered; a hash named
/// here only checks the caller means that build, and the verb is refused
/// when it names another (a teammate's build landed in between). A hash
/// left out is not checked, and a body naming none applies the registered
/// build as it is.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildHashes {
    #[serde(default, rename = "binaryHash", skip_serializing_if = "Option::is_none")]
    pub binary_hash: Option<String>,
    #[serde(default, rename = "definitionHash", skip_serializing_if = "Option::is_none")]
    pub definition_hash: Option<String>,
    #[serde(default, rename = "infraHash", skip_serializing_if = "Option::is_none")]
    pub infra_hash: Option<String>,
}

/// Which catalog node types a worker binary compiles in.
///
/// `Referenced` is the ordinary build: the types the program names, so
/// the binary is as small as the program. `Full` compiles every node
/// in the catalog, so a program edit that starts using a node the
/// previous program did not never rebuilds the image: the build is
/// content-addressed either way (the compiler's `compute_binary_hash` folds the same
/// set plus this choice), so a full image rebuilds only when a node
/// source was added, removed, or edited. CLI builds default to `Full`;
/// `--referenced` selects only the graph's types.
/// On the wire (the build request) as `"referenced"` / `"full"`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeSet {
    Referenced,
    #[default]
    Full,
}

impl NodeSet {
    /// The line the binary hash folds so a full image and a referenced
    /// image of one program never share a tag.
    pub fn hash_marker(self) -> &'static str {
        match self {
            NodeSet::Referenced => "referenced",
            NodeSet::Full => "full",
        }
    }
}

/// What `POST /projects/{id}/builds` answers, with a 202, when images of
/// the version were missing: the version build now waiting on them, and
/// the image builds it waits on, each one started by this request or
/// already running for the same content. The request answers as soon as
/// they run rather than when they end, so nothing is held open for the
/// minutes a build takes. The install registers the version itself once
/// every image is built; the caller follows it
/// (`GET /projects/{id}/builds/{build}`, [`VersionBuildState`]) until it
/// is registered or failed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildsUnderway {
    /// The version being built.
    pub version: String,
    /// This version build, as `GET /projects/{id}/builds/{build}` names it.
    pub build: uuid::Uuid,
    pub images: Vec<ImageBuild>,
}

/// What a build asked for from a program answers: the version registered
/// at once, or the image builds it waits on, which `ctx` follows through
/// `VersionCall::BuildState` until it registers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum BuildAnswer {
    Registered { program: Box<BuiltProgram> },
    Underway { underway: BuildsUnderway },
}

/// On the 409 `POST /projects/{id}/builds` answers when another ask of
/// the project is starting its image builds right now: that ask lets go
/// within moments, so a caller sending its ask again (one whose first
/// ask's answer was lost on the way) may wait and send it again, and its
/// ask then joins the version build the first one started.
pub const BUILD_BUSY_HEADER: &str = "x-weft-build-busy";

/// One image build a version waits on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageBuild {
    /// The image ref it pushes.
    pub image: String,
    /// The name weft minted for it. Known from the moment the build is
    /// decided, before the builder gives it an id of its own.
    pub name: String,
}

/// Where a version build that waited on image builds stands.
// SYNC: VersionBuildStatus <-> crates/weft-dispatcher/src/build/waiting.rs (version_build.state)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VersionBuildStatus {
    /// Some image it needs is still building.
    Waiting,
    /// Every image was built and the version is the project's running one.
    Registered,
    /// An image's build failed, or the version could not be registered;
    /// `reason` says which.
    Failed,
    /// The project's build was cancelled.
    Cancelled,
    /// A newer build of the project was asked for before this one was
    /// registered, so this one never will be.
    Superseded,
}

/// The answer to `GET /projects/{id}/builds/{build}`: where a version
/// build stands, and each image build it waits on.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionBuildState {
    pub state: VersionBuildStatus,
    pub images: Vec<ImageBuildState>,
    /// What registered, once it did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub program: Option<BuiltProgram>,
    /// Why it failed, was cancelled or superseded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// One image build of a [`VersionBuildState`]: which, and how it stands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageBuildState {
    #[serde(flatten)]
    pub build: ImageBuild,
    #[serde(flatten)]
    pub state: crate::projects::BuildStateResponse,
}

/// `POST /projects/{id}/builds`: build the project. What is built is a
/// working copy's files, stamped into a version first (the working copy
/// named, or the project's default), or a version the project holds,
/// built as it is. The install reads the version back, compiles it and
/// resolves its `@asset`s itself: nothing the caller computed is believed.
// SYNC: BuildRequest <-> crates/weft-core/src/context/project_calls.rs (`BuildCalls`)
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildRequest {
    #[serde(flatten)]
    pub working_copy: crate::working_copy::WorkingCopyRef,
    /// Build this version as it is, stamping nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default)]
    pub node_set: NodeSet,
    /// Stamp the working copy as a new tree root (`weft run --root`).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub root: bool,
    /// The run this build is for: held to the compiled program before any
    /// image is built, so a run that could not start is refused in
    /// seconds rather than after minutes of building.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<RunPreview>,
}

/// A run a build is for, as much of it as can be judged before the build
/// registers: what it starts with, and whether a seed feeds it (a seed may
/// feed an input nothing else does, which only the run itself can tell).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunPreview {
    #[serde(flatten)]
    pub shape: crate::versions::RunShape,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub seeded: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub seed_until: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub seed_before: Vec<String>,
}

/// One version's files and what they are compiled with (its name is its
/// own `weft.toml`'s): what a build builds, and what a project's registered build keeps
/// (`project.running_source` and `running_build_inputs`) so the install can
/// build the same version again on a newer weft (the maintenance lift,
/// `crate::maintenance`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VersionSource {
    /// The version: every covered file's hash, every blob in the tenant's
    /// assets.
    pub manifest: crate::project::hash::Manifest,
    #[serde(flatten)]
    pub inputs: BuildInputs,
}

/// What a build compiles a version's files with, beside the files
/// themselves: kept with the build a project registers
/// (`project.running_build_inputs`), so the install can build the same
/// version again on a newer weft (the maintenance lift,
/// `crate::maintenance`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BuildInputs {
    /// Compile every catalog node into the worker (`full`) or only the
    /// ones the program references (`referenced`).
    #[serde(rename = "nodeSet")]
    pub node_set: NodeSet,
    /// The `@asset` resolutions the version was built with
    /// (`FileRef::resolution_key`): what the install resolved when it
    /// first built it, kept so a build of the same version again resolves
    /// nothing anew (a URL may answer otherwise, a file outside the project
    /// may be gone).
    #[serde(default, with = "crate::project::resolutions_as_list")]
    pub assets: crate::project::AssetResolutions,
}

impl BuildInputs {
    /// What a registered build keeps of its inputs (`project.running_build_inputs`).
    /// Version 2 keeps `assets` as a list of `{ path, type, value }`
    /// ([`assets_to_list`]).
    pub const STORED: crate::stored::Kind<BuildInputs> =
        crate::stored::Kind::new("build's inputs", &[&[crate::stored::Op::Code { at: "assets", run: assets_to_list }]]);
}

/// Version 1 kept `assets` as an object keyed by the path and the type
/// joined with a NUL character; version 2 keeps a list of
/// `{ path, type, value }`. A type's spelling holds no NUL and a path may,
/// so the key splits at its last one.
fn assets_to_list(assets: &mut serde_json::Value) -> Result<(), String> {
    let serde_json::Value::Object(map) = assets else {
        return Err(format!("not an object of resolutions: {assets}"));
    };
    let mut list = Vec::with_capacity(map.len());
    for (key, value) in std::mem::take(map) {
        let Some((path, ty)) = key.rsplit_once('\u{0}') else {
            return Err(format!("the key {key:?} has no NUL between its path and its type"));
        };
        list.push(serde_json::json!({ "path": path, "type": ty, "value": value }));
    }
    *assets = serde_json::Value::Array(list);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project::ResolutionKey;

    fn text_key(path: &str) -> ResolutionKey {
        ResolutionKey { path: path.into(), ty: "String".into() }
    }

    /// Version 1's object of resolutions, keyed by path and type joined
    /// with a NUL, becomes version 2's list, a NUL inside a path kept in
    /// the path.
    #[test]
    fn version_1_assets_become_a_list() {
        let mut doc = serde_json::json!({
            "nodeSet": "full",
            "assets": { "logo.png\u{0}Image": { "key": "t/asset/aa" }, "odd\u{0}name.txt\u{0}String": "hi" },
        });
        crate::stored::upgrade(BuildInputs::STORED.steps, 1, &mut doc).unwrap();
        let inputs: BuildInputs = serde_json::from_value(doc).unwrap();
        assert_eq!(inputs.assets.len(), 2);
        assert_eq!(inputs.assets[&ResolutionKey { path: "logo.png".into(), ty: "Image".into() }], serde_json::json!({ "key": "t/asset/aa" }));
        assert_eq!(inputs.assets[&ResolutionKey { path: "odd\u{0}name.txt".into(), ty: "String".into() }], "hi");

        let mut keyless = serde_json::json!({ "nodeSet": "full", "assets": { "no separator": 1 } });
        assert!(crate::stored::upgrade(BuildInputs::STORED.steps, 1, &mut keyless).is_err());
    }

    /// A list naming one reference twice does not read: which of the two
    /// a build would use is not said anywhere.
    #[test]
    fn a_reference_resolved_twice_does_not_read() {
        let doc = serde_json::json!({ "nodeSet": "full", "assets": [
            { "path": "a", "type": "String", "value": 1 },
            { "path": "a", "type": "String", "value": 2 },
        ] });
        assert!(serde_json::from_value::<BuildInputs>(doc).is_err());
    }

    /// What a build compiles its files with sits beside them, as the
    /// project's registered build keeps them.
    #[test]
    fn a_version_source_keeps_its_inputs_beside_its_files() {
        let source = VersionSource {
            manifest: [("main.weft".to_string(), "sha256:aa".to_string())].into(),
            inputs: BuildInputs { node_set: NodeSet::Referenced, assets: [(text_key("a"), serde_json::json!("x"))].into() },
        };
        let wire = serde_json::to_value(&source).unwrap();
        assert_eq!(wire["nodeSet"], "referenced");
        assert_eq!(wire["assets"], serde_json::json!([{ "path": "a", "type": "String", "value": "x" }]));
        assert_eq!(serde_json::from_value::<VersionSource>(wire).unwrap(), source);
    }

    /// A build of the default working copy says nothing but what it builds.
    #[test]
    fn a_build_request_names_only_what_it_asks() {
        assert_eq!(serde_json::to_value(BuildRequest::default()).unwrap(), serde_json::json!({ "nodeSet": "full" }));
        let request = BuildRequest {
            working_copy: crate::working_copy::WorkingCopyRef { working_copy: Some(uuid::Uuid::nil()) },
            root: true,
            run: Some(RunPreview { shape: crate::versions::RunShape { example: Some("e".into()), ..Default::default() }, ..Default::default() }),
            ..Default::default()
        };
        let wire = serde_json::to_value(&request).unwrap();
        assert_eq!(wire["workingCopy"], "00000000-0000-0000-0000-000000000000");
        assert_eq!(wire["run"], serde_json::json!({ "example": "e" }));
        assert_eq!(serde_json::from_value::<BuildRequest>(wire).unwrap(), request);
    }

    #[test]
    fn a_built_program_round_trips_under_its_wire_names() {
        let built = BuiltProgram {
            version: "v".into(),
            definition: serde_json::from_value(serde_json::json!({
                "id": "00000000-0000-0000-0000-000000000001", "nodes": [], "edges": []
            }))
            .unwrap(),
            binary_hash: "b".into(),
            definition_hash: "d".into(),
            infra_hash: "i".into(),
            implementations: [("Llm".to_string(), "h".to_string())].into(),
            infra_images: [("one.db".to_string(), [("db".to_string(), "r/db:1".to_string())].into())].into(),
            replaced_infra_images: vec!["one.db".into()],
            infra_places: [("one.db".to_string(), "p".to_string())].into(),
            worker_image: "r/weft-worker:b".into(),
            left_out: vec![LeftOutPackage { package: "media".into(), node_types: vec!["Resize".into()], error: "error[E0308]".into() }],
            worker_log: Some("https://log/1".into()),
            node_needs: Default::default(),
        };
        let wire = serde_json::to_value(&built).unwrap();
        for key in ["binaryHash", "definitionHash", "infraHash", "infraImages", "replacedInfraImages", "infraPlaces", "workerImage", "leftOut", "workerLog"] {
            assert!(wire.get(key).is_some(), "{key} missing from {wire}");
        }
        let back: BuiltProgram = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(&back).unwrap(), wire);
    }

    /// A record reads as the build wrote it, refuses anything else, and
    /// breaks exactly the nodes of a left-out type, each refusal naming
    /// the package, its first error and where the rest are.
    #[test]
    fn a_left_out_record_breaks_the_nodes_of_its_packages() {
        let record = br#"{"packages":[{"package":"media","nodeTypes":["Resize","Crop"],"error":"error[E0308]: mismatched types"}]}"#;
        let left_out = LeftOut::parse(record).unwrap();
        assert_eq!(left_out.packages[0].node_types, ["Resize", "Crop"]);
        assert!(LeftOut::parse(br#"{"packages":[],"extra":1}"#).is_err(), "a record weft did not write");
        assert!(LeftOut::parse(b"not json").unwrap_err().contains("does not read"));
        assert!(LeftOut::parse(b"{}").unwrap().is_empty());

        let definition: crate::ProjectDefinition = serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000001",
            "nodes": [
                { "id": "shrink", "nodeType": "Resize", "inputs": [], "outputs": [], "position": { "x": 0, "y": 0 } },
                { "id": "say", "nodeType": "Text", "inputs": [], "outputs": [], "position": { "x": 0, "y": 0 } }
            ],
            "edges": []
        }))
        .unwrap();
        let parts = LeftOut::broken_parts(&left_out.packages, &definition, Some("https://log/1"));
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].parts, ["shrink"]);
        assert_eq!(
            parts[0].why,
            "shrink: package 'media' didn't compile: error[E0308]: mismatched types; its full errors are in the build log: https://log/1"
        );
        assert!(left_out.packages[0].reason(None).ends_with("its full errors are in the build log"));
        // A package of the project's copy of the standard nodes names the
        // command that brings the copy up to date.
        let stale = LeftOutPackage {
            package: "logic".into(),
            node_types: vec!["GetNodeState".into()],
            error: "error[E0599]: no method named `node_state` at /weft/project-nodes/nodes/base_catalog/logic/get_node_state/mod.rs:33".into(),
        };
        assert!(stale.reason(None).ends_with("`weft catalog update` in its folder brings it up to date)"), "{}", stale.reason(None));
        assert!(!left_out.packages[0].reason(None).contains("catalog update"), "a project's own package is its own to fix");
    }
}
