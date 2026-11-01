//! What an `@file` or an `@asset` stands for, worked out by the install
//! from the version it is in: for a build, every `@asset` its program
//! names; for a run, the example it starts with and every marker in what
//! it is handed.
//!
//! A path the version holds is the version's: its blob is already in the
//! tenant's assets, so an `@asset` of it becomes the stored file at once,
//! with nothing read or uploaded, and an `@file` of it is read from the
//! blob. A URL is fetched here, and a stored key (a file picked in the
//! editor) is looked up in the tenant's files.
//!
//! A path the version does not hold (an absolute one, `~/...`, or a file
//! inside the project no version covers) is the machine's. A local
//! install runs as its person on their machine, so it reads it: an
//! absolute path where it is, a relative one from the folder of the
//! working copy the build or run came from, uploading what the tenant's
//! assets lack. A cloud install has no such machine, and refuses it naming
//! the fix (put the file under `assets/`).

use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::Context;
use axum::http::StatusCode;
use weft_compiler::file_ref::{TextRead, TextValues};
use weft_core::project::hash::Manifest;
use weft_core::project::FileRef;
use weft_core::run_spec::RunSpec;
use weft_core::versions::RunShape;

use crate::actions::Refused;
use crate::state::DispatcherState;
use crate::working_copies::WorkingCopyRow;

/// Where the paths of one version resolve.
pub struct Reach<'a> {
    state: &'a DispatcherState,
    tenant: String,
    manifest: &'a Manifest,
    /// Whether the install reads files of the machine it runs on (a local
    /// install).
    machine: bool,
    /// The folder a project path the version does not hold is read from:
    /// the folder of the working copy the build or run came from, on an
    /// install that reads its machine.
    folder: Option<PathBuf>,
}

/// Where the version's files sit for a reader of paths
/// ([`weft_compiler::FileReader`]): a root no disk has, so nothing on the
/// install's own disk is ever read as a project file by mistake.
const VERSION_ROOT: &str = "/weft-version";

impl<'a> Reach<'a> {
    /// The paths of the version `manifest` of `project`, from `copy`.
    pub async fn of(
        state: &'a DispatcherState,
        project: uuid::Uuid,
        manifest: &'a Manifest,
        copy: Option<&WorkingCopyRow>,
    ) -> Result<Reach<'a>, Refused> {
        let tenant = state
            .projects
            .tenant_for(project)
            .await
            .map_err(|e| crate::actions::internal("project owner", e))?
            .ok_or((StatusCode::NOT_FOUND, crate::authenticator::NO_SUCH_PROJECT.to_string()))?;
        let machine = state.install_info.cloud.is_none();
        let folder = copy.and_then(|c| c.path.as_ref()).filter(|_| machine).map(PathBuf::from);
        Ok(Reach { state, tenant, manifest, machine, folder })
    }

    /// The bytes of the version's file at `path`, checked against the hash
    /// the version names; `None` when the version holds no such file.
    pub async fn version_file(&self, path: &str) -> Result<Option<Vec<u8>>, Refused> {
        let Some(hash) = self.manifest.get(path) else { return Ok(None) };
        let key = format!("{}/{}", self.tenant, asset_key(hash)?);
        let bytes = crate::storage::read_file(self.state, &key)
            .await
            .map_err(|e| (StatusCode::BAD_GATEWAY, format!("read '{path}' of the version: {e:#}")))?;
        let got = weft_core::project::hash::sha256_hex(&bytes);
        if &got != hash {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("'{path}' came back as {got}, and the version names {hash}; the store holds other bytes under that name"),
            ));
        }
        Ok(Some(bytes))
    }

    fn machine_path(&self, path: &str) -> Result<PathBuf, String> {
        machine_path(self.machine, self.folder.as_deref(), path)
    }

    /// The bytes of the file a path names, wherever it is: the version's,
    /// or the machine's.
    async fn read_any(&self, path: &str) -> Result<Vec<u8>, String> {
        if let Some(bytes) = self.version_file(path).await.map_err(|(_, why)| why)? {
            return Ok(bytes);
        }
        let at = self.machine_path(path)?;
        tokio::fs::read(&at).await.map_err(|e| format!("'{path}' cannot be read at {}: {e}", at.display()))
    }
}

/// Where the machine file a path names is, or why the install cannot
/// read it. `path` is spelled the way the compiler spells a marker's
/// (`spell_from_root`): absolute, or relative to the project root, read
/// from `folder`. `machine`: the install reads its machine's files.
fn machine_path(machine: bool, folder: Option<&Path>, path: &str) -> Result<PathBuf, String> {
    if !machine {
        return Err(format!(
            "'{path}' is not in the version, and this install runs in the cloud, so it cannot read files \
             off a machine: put the file in the project, under `assets/`, and name it from there"
        ));
    }
    let written = Path::new(path);
    if written.is_absolute() {
        return Ok(written.to_path_buf());
    }
    match folder {
        Some(folder) => Ok(folder.join(written)),
        None => Err(format!(
            "'{path}' is not one of the files a version holds (those are the program's `.weft` files, `weft.toml`, \
             and what is under `src/`, `nodes/`, `prompts/`, `scripts/`, `sql/`, `assets/`, `examples/` and `layouts/`), \
             and this was not asked from a folder it could be read from: move it under `assets/`"
        )),
    }
}

fn asset_key(hash: &str) -> Result<String, Refused> {
    weft_core::storage::key::scope_key(&weft_core::storage::key::KeyScope::Asset, hash)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("'{hash}' is not a storable address ({e})")))
}

/// Resolve every `@asset` `target` names (see the module doc), answering
/// the resolution map by `FileRef::resolution_key`, which the caller
/// applies (`weft_compiler::file_ref::apply_asset_resolutions`), and every
/// text it read. A text value is spelled as `texts` says; a caller that
/// names it by its content stores the text it read. Every ref that cannot
/// be used is named at once.
pub async fn resolve(
    reach: &Reach<'_>,
    target: &impl weft_compiler::file_ref::MarkedValues,
    texts: TextValues,
) -> Result<(weft_core::project::AssetResolutions, Vec<TextRead>), Refused> {
    let mut map = BTreeMap::new();
    let mut refused: Vec<String> = Vec::new();
    // Files: the version's become stored files at once; the machine's are
    // synced into the tenant's assets.
    let mut from_machine: Vec<(FileRef, PathBuf)> = Vec::new();
    for r in weft_compiler::file_ref::collect_asset_refs(target) {
        match reach.manifest.get(&r.path) {
            Some(hash) => match stored_value(reach, &r, hash).await {
                Ok(value) => {
                    map.insert(r.resolution_key(), value);
                }
                Err(why) => refused.push(why),
            },
            None => match reach.machine_path(&r.path) {
                Ok(at) => from_machine.push((r, at)),
                Err(why) => refused.push(format!("@asset({:?}, {}): {why}", r.path, r.ty)),
            },
        }
    }
    if !from_machine.is_empty() && refused.is_empty() {
        let refs: Vec<FileRef> = from_machine.iter().map(|(r, _)| r.clone()).collect();
        let source = MachineFiles(from_machine.iter().map(|(r, at)| (r.path.clone(), at.clone())).collect());
        let store = crate::storage::asset_store(reach.state, &reach.tenant);
        match weft_assets::sync_assets(&refs, &source, &store).await {
            Ok(synced) => map.extend(synced),
            Err(e) => refused.push(format!("{e:#}")),
        }
    }
    // Stored keys: looked up in the tenant's files, the same match every
    // build makes (`resolve_runtime_key_refs`).
    let key_refs = weft_compiler::file_ref::collect_runtime_key_refs(target);
    if !key_refs.is_empty() {
        let listing = crate::storage::tenant_list(reach.state, &reach.tenant)
            .await
            .map_err(|e| (StatusCode::BAD_GATEWAY, format!("list the stored files: {e:#}")))?;
        if let Err(errs) = weft_compiler::file_ref::resolve_runtime_key_refs(&key_refs, &listing, &mut map) {
            refused.extend(errs);
        }
    }
    // Text: read once (a URL fetched, a stored file read, a path read
    // from wherever it is) and cast.
    let read = match weft_compiler::file_ref::read_texts(target, reach).await {
        Ok(read) => read,
        Err(why) => {
            refused.extend(why);
            Vec::new()
        }
    };
    if !refused.is_empty() {
        return Err((StatusCode::UNPROCESSABLE_ENTITY, format!("assets that cannot be used:\n  {}", refused.join("\n  "))));
    }
    for text in &read {
        map.insert(text.file_ref.resolution_key(), text.resolution(texts));
    }
    Ok((map, read))
}

/// Where a text `@asset` of the version is read ([`weft_compiler::file_ref::read_texts`]).
impl weft_compiler::file_ref::TextSource for Reach<'_> {
    async fn url(&self, url: &str) -> Result<Vec<u8>, String> {
        // No limit beyond the connect one: the address is the person's,
        // part of a build they wait on and can stop.
        weft_core::net::get_body(&self.state.http, url).await
    }

    async fn stored(&self, key: &str) -> Result<Vec<u8>, String> {
        crate::storage::read_file(self.state, &format!("{}/{key}", self.tenant)).await.map_err(|e| format!("{e:#}"))
    }

    async fn path(&self, path: &str) -> Result<Vec<u8>, String> {
        self.read_any(path).await
    }
}

/// The value an `@asset` of the version's blob `hash` stands for: the
/// stored file as the store records it (never what a client said about
/// it), held to the kind the ref declares.
async fn stored_value(reach: &Reach<'_>, r: &FileRef, hash: &str) -> Result<serde_json::Value, String> {
    let key = format!("{}/{}", reach.tenant, asset_key(hash).map_err(|(_, why)| why)?);
    let meta = crate::storage::file_meta(reach.state, &key)
        .await
        .map_err(|e| format!("@asset({:?}, {}): the stored copy of the version's file cannot be read: {e:#}", r.path, r.ty))?;
    let mut file = weft_core::storage::StoredFile::from(&meta);
    // The version names the file by its path; the store, by whichever
    // path first uploaded these bytes.
    file.filename = r.path.clone();
    if let Some(declared) = r.ty.concrete_file_kind() {
        if declared != weft_core::weft_type::FileKind::Blob && file.kind() != declared {
            return Err(format!(
                "@asset({:?}, {}): the file is {} ({}), not {}",
                r.path,
                r.ty,
                file.kind().primitive(),
                meta.mime_type,
                declared.primitive()
            ));
        }
    }
    Ok(weft_core::storage::typed_file_value(&file, &r.ty))
}

/// Files of the machine an `@asset` names, by the path it is written
/// with ([`weft_assets::AssetSource`]).
struct MachineFiles(BTreeMap<String, PathBuf>);

impl weft_assets::AssetSource for MachineFiles {
    fn open(&self, path: &str) -> anyhow::Result<Box<dyn Read + Send>> {
        let at = self.0.get(path).ok_or_else(|| anyhow::anyhow!("'{path}' is no file this build reads"))?;
        let file = std::fs::File::open(at).map_err(|e| anyhow::anyhow!("asset not found at {}: {e}", at.display()))?;
        Ok(Box::new(file))
    }
}

// ----- what a run starts with --------------------------------------------

/// What a run shaped by `shape` starts with: the example the version holds
/// (`examples/<name>.json`, or the `.json` path it names), edited by its
/// flags, or the flags alone; `None` for a run of the whole program. Every
/// `@file` and `@asset` in it is resolved from the version (see the
/// module doc), so the run starts on what the version holds wherever it
/// was asked from.
pub async fn run_spec(reach: &Reach<'_>, shape: &RunShape) -> Result<Option<RunSpec>, Refused> {
    let mut spec = match &shape.example {
        Some(name) => {
            let path = if name.ends_with(".json") { name.clone() } else { format!("examples/{name}.json") };
            let bytes = reach
                .version_file(&path)
                .await?
                .ok_or((StatusCode::NOT_FOUND, format!("this version holds no example '{name}' (no {path} in it)")))?;
            let text = String::from_utf8(bytes).map_err(|_| (StatusCode::BAD_REQUEST, format!("{path} is not text")))?;
            let mut spec = RunSpec::parse_file(&text).map_err(|e| (StatusCode::BAD_REQUEST, format!("parse {path}: {e}")))?;
            // The file is the runnable name; a name edited inside it must
            // not point the run at another file.
            spec.name = Path::new(&path).file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_else(|| name.clone());
            Some(weft_core::run_flags::apply_run_flags(&spec.without_expected(), &shape.flags).map_err(|e| (StatusCode::BAD_REQUEST, e))?)
        }
        None if shape.flags.is_empty() => None,
        None => Some(weft_core::run_flags::spec_from_flags("one-off", &shape.flags).map_err(|e| (StatusCode::BAD_REQUEST, e))?),
    };
    let Some(spec) = spec.as_mut() else { return Ok(None) };
    resolve_file_markers(reach, spec).await?;
    let (map, _) = resolve(reach, &*spec, TextValues::Inline).await?;
    weft_compiler::file_ref::apply_asset_resolutions(spec, &map)
        .map_err(|errs| (StatusCode::UNPROCESSABLE_ENTITY, format!("unresolved assets:\n  {}", errs.join("\n  "))))?;
    Ok(Some(spec.clone()))
}

/// Resolve every `@file` in a run's values exactly as a build resolves
/// one in source, reading each file through [`VersionReader`]. The reader
/// is a blocking one (the compiler's), so this runs on a blocking thread
/// that waits on each fetch.
async fn resolve_file_markers(reach: &Reach<'_>, spec: &mut RunSpec) -> Result<(), Refused> {
    let handle = tokio::runtime::Handle::current();
    let reader = VersionReader { reach, handle };
    tokio::task::block_in_place(|| {
        let root = Path::new(VERSION_ROOT);
        let fs = weft_compiler::CompileFs::with_reader(&reader, Some(root));
        weft_compiler::file_ref::resolve_file_markers(spec, &fs)
    })
    .map_err(|errs| (StatusCode::UNPROCESSABLE_ENTITY, format!("a run value cannot be read:\n  {}", errs.join("\n  "))))
}

/// The version's files as the compiler reads paths ([`VERSION_ROOT`]),
/// fetched as they are asked for, and a file the version does not hold
/// from the machine where [`Reach`] says so.
struct VersionReader<'r, 'a> {
    reach: &'r Reach<'a>,
    handle: tokio::runtime::Handle,
}

impl weft_compiler::FileReader for VersionReader<'_, '_> {
    fn locate(&self, path: &Path) -> Result<PathBuf, String> {
        weft_compiler::file_reader::normalize_lexical(path).ok_or_else(|| format!("path {path:?} escapes the project root"))
    }

    fn read(&self, identity: &Path) -> Result<String, String> {
        let rel = identity
            .strip_prefix(VERSION_ROOT)
            .map_err(|_| "is outside the project".to_string())?
            .to_str()
            .ok_or_else(|| "is not a path that is text".to_string())?
            .to_string();
        let bytes = self.handle.block_on(self.reach.read_any(&rel))?;
        String::from_utf8(bytes).map_err(|_| "is not text".to_string())
    }

    fn identity(&self, path: &Path) -> Result<PathBuf, String> {
        self.locate(path)
    }
}

// ----- a build's preparation ---------------------------------------------

/// What a build of a version asked for now does between its compile and
/// its plan (`super::Prepare`): its `@asset`s resolved from the version
/// (see the module doc), and the run it is for, if any, held to the
/// compiled program the way the run itself will be, before any image is
/// built.
pub struct Resolving<'a> {
    pub reach: Reach<'a>,
    pub run: Option<weft_core::builds::RunPreview>,
}

/// A refusal of this module as the build's error: its words, which the
/// build answers with its own status (`crate::transition::build_version_gated`).
fn as_error((_, why): Refused) -> anyhow::Error {
    anyhow::anyhow!(why)
}

#[async_trait::async_trait]
impl super::Prepare for Resolving<'_> {
    /// Each `@asset` from the version, a text one named by its content and
    /// stored among the tenant's assets (a version's file is there already,
    /// and storing it again asks once and sends nothing), so a worker that
    /// reads the program finds it.
    async fn assets(
        &self,
        storage: &dyn super::ProjectStorage,
        tenant: &str,
        definition: &weft_core::ProjectDefinition,
    ) -> anyhow::Result<super::Assets> {
        let (resolutions, read) = resolve(&self.reach, definition, TextValues::Named).await.map_err(as_error)?;
        let texts: BTreeMap<String, Vec<u8>> = read.iter().map(|text| (text.file_ref.path.clone(), text.bytes.clone())).collect();
        if !texts.is_empty() {
            storage.store_texts(tenant, &texts).await.context("store the text the program's `@asset`s name")?;
        }
        let contents = read.into_iter().map(|text| (text.content.id(), std::sync::Arc::new(text.value))).collect();
        Ok(super::Assets { resolutions, contents })
    }

    async fn check(&self, definition: &weft_core::ProjectDefinition) -> anyhow::Result<()> {
        let Some(run) = &self.run else { return Ok(()) };
        let spec = run_spec(&self.reach, &run.shape).await.map_err(as_error)?;
        let verdict = weft_core::run_spec::judge::judge(definition, spec.as_ref(), run.seeded, &run.seed_until, &run.seed_before);
        if verdict.refusal().is_none() {
            return Ok(());
        }
        let errors = verdict.unfed.iter().map(|unfed| unfed.message.clone()).chain(verdict.refused.iter().cloned()).collect();
        Err(super::RunRefused(weft_core::run_spec::Refusal { errors }).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A path the version does not hold is refused on a cloud install with
    /// the fix, and read from the working copy's folder on a local one.
    #[test]
    fn a_path_outside_the_version_is_the_machines_or_refused() {
        let cloud = MachinePaths { machine: false, folder: Some(PathBuf::from("/home/me/shop")) };
        assert!(cloud.at("/data/big.csv").unwrap_err().contains("under `assets/`"));
        let local = MachinePaths { machine: true, folder: Some(PathBuf::from("/home/me/shop")) };
        assert_eq!(local.at("/data/big.csv").unwrap(), PathBuf::from("/data/big.csv"));
        assert_eq!(local.at("data/big.csv").unwrap(), PathBuf::from("/home/me/shop/data/big.csv"));
        let no_folder = MachinePaths { machine: true, folder: None };
        assert!(no_folder.at("data/big.csv").unwrap_err().contains("move it under `assets/`"));
    }

    /// [`Reach::machine_path`]'s rule, without the state around it.
    struct MachinePaths {
        machine: bool,
        folder: Option<PathBuf>,
    }

    impl MachinePaths {
        fn at(&self, path: &str) -> Result<PathBuf, String> {
            machine_path(self.machine, self.folder.as_deref(), path)
        }
    }
}
