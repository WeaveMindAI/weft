//! The pure halves of a version build: what an author's asset resolutions
//! become, and where each infra image lands.

use std::collections::BTreeMap;

use super::*;
use weft_core::project::hash::Manifest;

/// A store answering from maps: the dumb fake the build reads through.
#[derive(Default)]
pub(crate) struct MapStorage {
    pub files: BTreeMap<String, Vec<u8>>,
    pub metas: BTreeMap<String, weft_core::storage::StoredFileMeta>,
}

#[async_trait::async_trait]
impl ProjectStorage for MapStorage {
    async fn read(&self, key: &str) -> Result<Vec<u8>> {
        self.files.get(key).cloned().ok_or_else(|| anyhow!("no file {key}"))
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
fn key_of(marker: &str) -> String {
    weft_compiler::file_ref::parse_marker(marker).unwrap().unwrap().resolution_key()
}

fn claimed(key: &str, mime: &str, size: u64) -> serde_json::Value {
    weft_core::storage::StoredFile { key: key.into(), mime_type: mime.into(), size_bytes: size, filename: "lie.png".into(), version: weft_core::storage::FIRST_FILE_VERSION }
        .to_value()
}

#[tokio::test]
async fn a_stored_file_resolution_is_rebuilt_from_the_store() {
    let key = "local/asset/p/aa";
    let mut storage = MapStorage::default();
    storage.metas.insert(key.into(), meta(key, "image/png"));
    let mut def = definition_with(CAT);
    // The client claims another size and name; the store's answer wins.
    let sent = BTreeMap::from([(key_of(CAT), claimed(key, "image/png", 1))]);
    resolve_assets(&storage, "local", &mut def, &sent).await.unwrap();
    let file = weft_core::storage::StoredFile::from_value(&def.nodes[0].config["picture"]).unwrap();
    assert_eq!(file.size_bytes, 42);
    assert_eq!(file.filename, "cat.png");
}

#[tokio::test]
async fn a_resolution_to_another_tenant_or_kind_is_refused() {
    let mut storage = MapStorage::default();
    storage.metas.insert("local/asset/p/doc".into(), meta("local/asset/p/doc", "application/pdf"));
    let mut def = definition_with(CAT);
    let foreign = BTreeMap::from([(key_of(CAT), claimed("other/asset/p/aa", "image/png", 1))]);
    let e = resolve_assets(&storage, "local", &mut def, &foreign).await.unwrap_err().to_string();
    assert!(e.contains("not this install's"), "{e}");
    let wrong_kind = BTreeMap::from([(key_of(CAT), claimed("local/asset/p/doc", "image/png", 1))]);
    let e = resolve_assets(&storage, "local", &mut def, &wrong_kind).await.unwrap_err().to_string();
    assert!(e.contains("not image") || e.contains("not Image"), "{e}");
}

#[tokio::test]
async fn an_unresolved_asset_fails_by_name() {
    let storage = MapStorage::default();
    let mut def = definition_with(CAT);
    let e = resolve_assets(&storage, "local", &mut def, &BTreeMap::new()).await.unwrap_err().to_string();
    assert!(e.contains("assets/cat.png"), "{e}");
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
    let (_blobs, cache) = fresh_cache();
    let root = tempfile::tempdir().unwrap();
    source::materialize(&storage, &cache, "local", &manifest, root.path()).await.unwrap();
    assert!(!root.path().join("nodes/base_catalog/logic").exists(), "nothing beyond the version is laid out");
    let (_, catalog, _) = source::compile(root.path()).unwrap();
    let label = &catalog.entry("Text").expect("the version's own Text").metadata.label;
    assert!(label.starts_with("Edited "), "{label}");
}

/// A version without `nodes/base_catalog/` has no standard library: the
/// build fails on the node it lacks instead of borrowing this install's.
#[tokio::test]
async fn a_version_without_a_standard_library_gets_none() {
    let (storage, manifest) = storage_with("local", PROGRAM);
    let (_blobs, cache) = fresh_cache();
    let root = tempfile::tempdir().unwrap();
    source::materialize(&storage, &cache, "local", &manifest, root.path()).await.unwrap();
    assert_eq!(std::fs::read(root.path().join("src/main.weft")).unwrap(), PROGRAM[1].1);
    assert!(!root.path().join("nodes/base_catalog").exists());
    let err = source::compile(root.path()).expect_err("Text is not in the version").to_string();
    assert!(err.contains("Text"), "{err}");
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

/// Bytes that do not hash to what the manifest names are refused, and so
/// is a path that would land outside the build's directory.
#[tokio::test]
async fn a_tampered_blob_or_a_climbing_path_is_refused() {
    let (mut storage, manifest) = storage_with("local", &[("weft.toml", b"[package]")]);
    let key = storage.files.keys().next().unwrap().clone();
    storage.files.insert(key, b"other bytes".to_vec());
    let (_blobs, cache) = fresh_cache();
    let root = tempfile::tempdir().unwrap();
    let e = source::materialize(&storage, &cache, "local", &manifest, root.path()).await.unwrap_err().to_string();
    assert!(e.contains("other bytes") || e.contains("came back as"), "{e}");

    let (storage, manifest) = storage_with("local", &[("../escape", b"x")]);
    let e = source::materialize(&storage, &cache, "local", &manifest, root.path()).await.unwrap_err().to_string();
    assert!(e.contains("not a project-relative path"), "{e}");
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
