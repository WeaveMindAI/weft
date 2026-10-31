//! The pure halves of a version build: what an author's asset resolutions
//! become, and where each infra image lands.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::*;
use weft_core::project::hash::Manifest;

/// A store answering from maps: the dumb fake the build reads through.
/// Every key a read or a download asked for is in `fetched`.
#[derive(Default)]
pub(crate) struct MapStorage {
    pub files: BTreeMap<String, Vec<u8>>,
    pub metas: BTreeMap<String, weft_core::storage::StoredFileMeta>,
    pub fetched: std::sync::Mutex<Vec<String>>,
}

impl MapStorage {
    fn file(&self, key: &str) -> Result<Vec<u8>> {
        self.fetched.lock().unwrap().push(key.to_string());
        self.files.get(key).cloned().ok_or_else(|| anyhow!("no file {key}"))
    }
}

#[async_trait::async_trait]
impl ProjectStorage for MapStorage {
    async fn read(&self, key: &str) -> Result<Vec<u8>> {
        self.file(key)
    }
    async fn download(&self, key: &str, dest: &std::path::Path) -> Result<()> {
        let bytes = self.file(key)?;
        let mut out = std::fs::File::create_new(dest)?;
        std::io::Write::write_all(&mut out, &bytes)?;
        Ok(())
    }
    async fn meta(&self, key: &str) -> Result<weft_core::storage::StoredFileMeta> {
        self.metas.get(key).cloned().ok_or_else(|| anyhow!("no file {key}"))
    }
}

fn meta(key: &str, mime: &str) -> weft_core::storage::StoredFileMeta {
    weft_core::storage::StoredFileMeta {
        key: key.into(),
        mime_type: mime.into(),
        size_bytes: 42,
        filename: "cat.png".into(),
        keep: false,
        expires_at_unix: None,
        keep_ttl_secs: None,
        created_at_unix: 0,
        version: weft_core::storage::FIRST_FILE_VERSION,
    }
}

/// One node whose config holds an `@asset` marker.
fn definition_with(marker: &str) -> weft_core::ProjectDefinition {
    serde_json::from_value(serde_json::json!({
        "id": uuid::Uuid::nil(),
        "nodes": [{
            "id": "n", "nodeType": "Debug", "inputs": [], "outputs": [],
            "position": {"x": 0, "y": 0},
            "config": { "picture": marker }
        }],
        "edges": []
    }))
    .unwrap()
}

const CAT: &str = r#"@asset("assets/cat.png", Image)"#;

/// The key a resolution for `marker` is sent under.
fn key_of(marker: &str) -> weft_core::project::ResolutionKey {
    weft_compiler::file_ref::parse_marker(marker).unwrap().unwrap().resolution_key()
}

fn claimed(key: &str, mime: &str, size: u64) -> serde_json::Value {
    weft_core::storage::StoredFile { key: key.into(), mime_type: mime.into(), size_bytes: size, filename: "lie.png".into(), version: weft_core::storage::FIRST_FILE_VERSION, link_secs: None }
        .to_value()
}

#[tokio::test]
async fn a_stored_file_resolution_is_rebuilt_from_the_store() {
    let key = "local/asset/p/aa";
    let mut storage = MapStorage::default();
    storage.metas.insert(key.into(), meta(key, "image/png"));
    let def = definition_with(CAT);
    // The client claims another size and name; the store's answer wins.
    let sent = BTreeMap::from([(key_of(CAT), claimed(key, "image/png", 1))]);
    let map = kept_assets(&storage, "local", &def, &sent).await.unwrap();
    let file = weft_core::storage::StoredFile::from_value(&map[&key_of(CAT)]).unwrap();
    assert_eq!(file.size_bytes, 42);
    assert_eq!(file.filename, "cat.png");
}

#[tokio::test]
async fn a_resolution_to_another_tenant_or_kind_is_refused() {
    let mut storage = MapStorage::default();
    storage.metas.insert("local/asset/p/doc".into(), meta("local/asset/p/doc", "application/pdf"));
    let def = definition_with(CAT);
    let foreign = BTreeMap::from([(key_of(CAT), claimed("other/asset/p/aa", "image/png", 1))]);
    let e = kept_assets(&storage, "local", &def, &foreign).await.unwrap_err().to_string();
    assert!(e.contains("not this install's"), "{e}");
    let wrong_kind = BTreeMap::from([(key_of(CAT), claimed("local/asset/p/doc", "image/png", 1))]);
    let e = kept_assets(&storage, "local", &def, &wrong_kind).await.unwrap_err().to_string();
    assert!(e.contains("not image") || e.contains("not Image"), "{e}");
}

fn storage_with(tenant: &str, files: &[(&str, &[u8])]) -> (MapStorage, Manifest) {
    let mut storage = MapStorage::default();
    let mut manifest = Manifest::new();
    for (path, bytes) in files {
        let hash = weft_core::project::hash::sha256_hex(bytes);
        storage.files.insert(format!("{tenant}/asset/{hash}"), bytes.to_vec());
        manifest.insert(path.to_string(), hash);
    }
    (storage, manifest)
}

/// Every file of the stdlib's `basic/` package, as the project's own
/// `nodes/base_catalog/basic/...` entries, with `edit` applied to each.
fn base_catalog_basic(edit: impl Fn(&str, Vec<u8>) -> Vec<u8>) -> Vec<(String, Vec<u8>)> {
    let seeded = tempfile::tempdir().unwrap();
    weft_compiler::project::seed_base_catalog(seeded.path()).unwrap();
    let basic = seeded.path().join("nodes/base_catalog/basic");
    let mut out = Vec::new();
    for file in weft_compiler::hash::walk_dir(&basic).unwrap() {
        let rel = format!("nodes/base_catalog/basic/{}", file.strip_prefix(&basic).unwrap().to_string_lossy());
        let bytes = std::fs::read(&file).unwrap();
        out.push((rel.clone(), edit(&rel, bytes)));
    }
    out
}

/// Compile the version laid out at `root` the way a build does: on a
/// blocking thread, through [`source::VersionFiles`] over `storage`.
async fn compile_version(
    storage: Arc<MapStorage>,
    tenant: &str,
    manifest: &Manifest,
    root: &std::path::Path,
) -> Result<(weft_core::ProjectDefinition, weft_catalog::FsCatalog, weft_compiler::project::Project)> {
    let files = Arc::new(source::VersionFiles::new(root.to_path_buf(), manifest.clone(), storage, tenant.to_string()));
    let root = root.to_path_buf();
    tokio::task::spawn_blocking(move || source::compile(&root, files)).await.unwrap()
}

const PROGRAM: &[(&str, &[u8])] = &[
    ("weft.toml", b"[package]\nname = 'v'\nid = '00000000-0000-0000-0000-000000000009'\n"),
    ("src/main.weft", b"greeting = Text { value: \"hi\" }\n"),
];

/// A version is the whole project: its `nodes/base_catalog/` comes back
/// exactly as the version holds it, an edit included, and the build reads
/// that copy and nothing this install ships.
#[tokio::test]
async fn a_version_builds_from_its_own_edited_standard_library() {
    let catalog = base_catalog_basic(|rel, bytes| {
        if rel.ends_with("text/metadata.json") {
            String::from_utf8(bytes).unwrap().replacen("\"label\": \"", "\"label\": \"Edited ", 1).into_bytes()
        } else {
            bytes
        }
    });
    let mut files: Vec<(&str, &[u8])> = PROGRAM.to_vec();
    files.extend(catalog.iter().map(|(p, b)| (p.as_str(), b.as_slice())));
    let (storage, manifest) = storage_with("local", &files);
    let storage = Arc::new(storage);
    let (_blobs, cache) = fresh_cache();
    let root = tempfile::tempdir().unwrap();
    source::materialize(storage.as_ref(), &cache, "local", &manifest, root.path()).await.unwrap();
    assert!(!root.path().join("nodes/base_catalog/logic").exists(), "nothing beyond the version is laid out");
    let (_, catalog, _) = compile_version(storage, "local", &manifest, root.path()).await.unwrap();
    let label = &catalog.entry("Text").expect("the version's own Text").metadata.label;
    assert!(label.starts_with("Edited "), "{label}");
}

/// A `.weft` file a package ships, and the prompt it reads, travel in the
/// version like the package's code, so a program that includes it
/// through `__catalog/` builds on the install from the version alone.
#[tokio::test]
async fn a_version_builds_a_program_that_includes_a_file_from_nodes() {
    let catalog = base_catalog_basic(|_, bytes| bytes);
    let mut files: Vec<(&str, &[u8])> = vec![
        PROGRAM[0],
        ("src/main.weft", b"greet = @include(\"__catalog/base_catalog/flows/greet.weft\")\n"),
        ("nodes/base_catalog/flows/greet.weft", b"# Say hello.\nGroup {\n hello = Text { value: @file(\"__catalog/base_catalog/flows/greeting.md\") }\n}\n"),
        ("nodes/base_catalog/flows/greeting.md", b"hello from the stdlib"),
    ];
    files.extend(catalog.iter().map(|(p, b)| (p.as_str(), b.as_slice())));
    let (storage, manifest) = storage_with("local", &files);
    let storage = Arc::new(storage);
    let (_blobs, cache) = fresh_cache();
    let root = tempfile::tempdir().unwrap();
    source::materialize(storage.as_ref(), &cache, "local", &manifest, root.path()).await.unwrap();
    let (definition, _, _) = compile_version(storage, "local", &manifest, root.path()).await.unwrap();
    let hello = definition
        .nodes
        .iter()
        .find(|n| n.id == "@nodes:base_catalog:flows:greet.hello")
        .expect("the included file's node");
    assert_eq!(hello.written_value("value"), Some(&serde_json::json!("hello from the stdlib")));
}

/// A version without `nodes/base_catalog/` has no standard library: the
/// node it lacks is broken, instead of borrowing this install's.
#[tokio::test]
async fn a_version_without_a_standard_library_gets_none() {
    let (storage, manifest) = storage_with("local", PROGRAM);
    let storage = Arc::new(storage);
    let (_blobs, cache) = fresh_cache();
    let root = tempfile::tempdir().unwrap();
    source::materialize(storage.as_ref(), &cache, "local", &manifest, root.path()).await.unwrap();
    assert_eq!(std::fs::read(root.path().join("src/main.weft")).unwrap(), PROGRAM[1].1);
    assert!(!root.path().join("nodes/base_catalog").exists());
    let (definition, _, _) = compile_version(storage, "local", &manifest, root.path()).await.unwrap();
    assert!(definition.broken.iter().any(|part| part.why.contains("Text")), "{:?}", definition.broken);
}

/// The files come from the building tenant's assets, whichever project
/// stored them, and never from another tenant's: the same content stored by
/// another tenant does not satisfy this one's version.
#[tokio::test]
async fn a_version_reads_its_own_tenants_assets_only() {
    let (storage, manifest) = storage_with("other", PROGRAM);
    let (_blobs, cache) = fresh_cache();
    // The other tenant builds first, so this replica's cache holds the bytes.
    let theirs = tempfile::tempdir().unwrap();
    source::materialize(&storage, &cache, "other", &manifest, theirs.path()).await.unwrap();
    assert_eq!(std::fs::read(theirs.path().join("src/main.weft")).unwrap(), PROGRAM[1].1);
    let mine = tempfile::tempdir().unwrap();
    source::materialize(&storage, &cache, "local", &manifest, mine.path())
        .await
        .expect_err("another tenant's copy, stored or cached, is not this tenant's file");
}

/// A cache in a directory of its own, empty.
fn fresh_cache() -> (tempfile::TempDir, super::blob_cache::BlobCache) {
    let dir = tempfile::tempdir().unwrap();
    let cache = super::blob_cache::BlobCache::new(dir.path().join("blobs"), u64::MAX);
    (dir, cache)
}

/// A file this replica fetched once comes from its cache the next time:
/// the second build lays the version out with the store emptied.
#[tokio::test]
async fn a_second_build_reads_kept_blobs_without_the_store() {
    let (mut storage, manifest) = storage_with("local", PROGRAM);
    let (_dir, cache) = fresh_cache();
    let first = tempfile::tempdir().unwrap();
    source::materialize(&storage, &cache, "local", &manifest, first.path()).await.unwrap();
    storage.files.clear();
    let second = tempfile::tempdir().unwrap();
    source::materialize(&storage, &cache, "local", &manifest, second.path()).await.unwrap();
    assert_eq!(std::fs::read(second.path().join("src/main.weft")).unwrap(), PROGRAM[1].1);
}

/// A version build names its files by the version's fingerprints for real:
/// the project the compile loads sits where the manifest's paths resolve
/// (a different spelling of the root would quietly read the disk instead),
/// so a file changed on the disk after it was checked still carries the
/// version's fingerprint.
#[tokio::test]
async fn a_version_build_finds_its_files_in_the_version() {
    let (storage, manifest) = storage_with("local", PROGRAM);
    let storage = Arc::new(storage);
    let (_blobs, cache) = fresh_cache();
    let root = tempfile::tempdir().unwrap();
    source::materialize(storage.as_ref(), &cache, "local", &manifest, root.path()).await.unwrap();
    let (_, _, project) = compile_version(storage, "local", &manifest, root.path()).await.unwrap();
    let fingerprints = weft_compiler::hash::Fingerprints::of_version(project.root.clone(), manifest.clone());
    let main = project.root.join("src/main.weft");
    std::fs::write(&main, b"changed after it was checked").unwrap();
    assert_eq!(fingerprints.of(&main).unwrap(), manifest["src/main.weft"], "the version's fingerprint, not the disk's");
    assert_ne!(weft_compiler::hash::DISK.of(&main).unwrap(), manifest["src/main.weft"]);
}

/// A kept blob whose bytes went bad on this replica's disk is never laid
/// out: it fails its hash, goes from the cache, and the store's copy is
/// fetched and kept in its place, so the build reads (and names its
/// images after) the version's bytes.
#[tokio::test]
async fn a_kept_blob_gone_bad_is_fetched_again_and_replaced() {
    let (storage, manifest) = storage_with("local", PROGRAM);
    let (dir, cache) = fresh_cache();
    let first = tempfile::tempdir().unwrap();
    source::materialize(&storage, &cache, "local", &manifest, first.path()).await.unwrap();
    let hash = &manifest["src/main.weft"];
    let kept = dir.path().join("blobs").join("local").join(hash);
    std::fs::write(&kept, b"greeting = Text { value: \"ho\" }\n").unwrap();
    let second = tempfile::tempdir().unwrap();
    source::materialize(&storage, &cache, "local", &manifest, second.path()).await.unwrap();
    assert_eq!(std::fs::read(second.path().join("src/main.weft")).unwrap(), PROGRAM[1].1, "the store's bytes, not the cache's");
    assert_eq!(std::fs::read(&kept).unwrap(), PROGRAM[1].1, "the cache holds the right bytes again");
}

/// A version stored while a manifest named the installed weft by a
/// `weft:<version>:<catalog hash>` entry fails by that entry's name.
#[tokio::test]
async fn an_old_weft_entry_is_refused_by_name() {
    let (storage, mut manifest) = storage_with("local", PROGRAM);
    manifest.insert("weft:0.5.0:abc".into(), String::new());
    let (_blobs, cache) = fresh_cache();
    let root = tempfile::tempdir().unwrap();
    let err = source::materialize(&storage, &cache, "local", &manifest, root.path()).await.unwrap_err().to_string();
    assert!(err.contains("weft:0.5.0:abc"), "{err}");
}

/// Bytes that do not hash to what the manifest names are refused by the
/// file's path, and leave nothing behind; so is a path that would land
/// outside the build's directory.
#[tokio::test]
async fn a_tampered_blob_or_a_climbing_path_is_refused() {
    let (mut storage, manifest) = storage_with("local", &[("src/main.weft", b"greeting = 1")]);
    let key = storage.files.keys().next().unwrap().clone();
    storage.files.insert(key, b"other bytes".to_vec());
    let (_blobs, cache) = fresh_cache();
    let root = tempfile::tempdir().unwrap();
    let e = source::materialize(&storage, &cache, "local", &manifest, root.path()).await.unwrap_err().to_string();
    let named = &manifest["src/main.weft"];
    let got = weft_core::project::hash::sha256_hex(b"other bytes");
    assert!(e.contains("'src/main.weft'") && e.contains(named.as_str()) && e.contains(got.as_str()), "{e}");
    assert!(!root.path().join("src/main.weft").exists(), "the refused file is removed");

    let (storage, manifest) = storage_with("local", &[("../escape", b"x")]);
    let e = source::materialize(&storage, &cache, "local", &manifest, root.path()).await.unwrap_err().to_string();
    assert!(e.contains("not a project-relative path"), "{e}");
}

/// The project's `assets/` stay in the store: the code is laid out and no
/// asset is fetched or written.
#[tokio::test]
async fn materialize_lays_out_code_and_leaves_assets_in_the_store() {
    let mut files: Vec<(&str, &[u8])> = PROGRAM.to_vec();
    files.push(("assets/cat.png", b"not really a png"));
    let (storage, manifest) = storage_with("local", &files);
    let (_blobs, cache) = fresh_cache();
    let root = tempfile::tempdir().unwrap();
    source::materialize(&storage, &cache, "local", &manifest, root.path()).await.unwrap();
    assert_eq!(std::fs::read(root.path().join("src/main.weft")).unwrap(), PROGRAM[1].1);
    assert!(root.path().join("weft.toml").is_file());
    assert!(!root.path().join("assets").exists());
    let asset_key = format!("local/asset/{}", manifest["assets/cat.png"]);
    assert!(!storage.fetched.lock().unwrap().contains(&asset_key));
}

/// A `@file` naming an `assets/` file reads it through the version's
/// files, fetched from the store when the compile asks; an asset nothing
/// names, however big, is never fetched.
#[tokio::test]
async fn version_files_fetch_an_asset_only_when_the_compile_reads_it() {
    let catalog = base_catalog_basic(|_, bytes| bytes);
    let clip = vec![7u8; 2 * 1024 * 1024];
    let mut files: Vec<(&str, &[u8])> = vec![
        PROGRAM[0],
        ("src/main.weft", b"notes = Text { value: @file(\"assets/notes.txt\") }\n"),
        ("assets/notes.txt", b"read me lazily"),
        ("assets/clip.bin", &clip),
    ];
    files.extend(catalog.iter().map(|(p, b)| (p.as_str(), b.as_slice())));
    let (storage, manifest) = storage_with("local", &files);
    let storage = Arc::new(storage);
    let (_blobs, cache) = fresh_cache();
    let root = tempfile::tempdir().unwrap();
    source::materialize(storage.as_ref(), &cache, "local", &manifest, root.path()).await.unwrap();
    let (definition, _, _) = compile_version(storage.clone(), "local", &manifest, root.path()).await.unwrap();
    let notes = definition.nodes.iter().find(|n| n.id == "notes").expect("the notes node");
    assert_eq!(notes.written_value("value"), Some(&serde_json::json!("read me lazily")));
    let fetched = storage.fetched.lock().unwrap();
    let notes_key = format!("local/asset/{}", manifest["assets/notes.txt"]);
    let clip_key = format!("local/asset/{}", manifest["assets/clip.bin"]);
    assert_eq!(fetched.iter().filter(|k| **k == notes_key).count(), 1, "{fetched:?}");
    assert!(!fetched.contains(&clip_key), "{fetched:?}");
}

#[test]
fn a_build_names_the_infra_places_whose_image_it_replaced() {
    let before: crate::project_store::InfraImageTags = [
        ("db".to_string(), [("main".to_string(), "db:1".to_string())].into_iter().collect()),
        ("cache".to_string(), [("main".to_string(), "cache:1".to_string())].into_iter().collect()),
    ]
    .into_iter()
    .collect();
    let after: BTreeMap<String, BTreeMap<String, String>> = [
        ("db".to_string(), BTreeMap::from([("main".to_string(), "db:2".to_string())])),
        ("cache".to_string(), BTreeMap::from([("main".to_string(), "cache:1".to_string())])),
        ("fresh".to_string(), BTreeMap::from([("main".to_string(), "fresh:1".to_string())])),
    ]
    .into_iter()
    .collect();
    assert_eq!(replaced_infra_images(&before, &after), vec!["db".to_string()], "unchanged and new places replace nothing");
}
