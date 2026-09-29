//! The pure halves of a version build: what an author's asset resolutions
//! become, and where each infra image lands.

use std::collections::BTreeMap;

use super::*;

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
    weft_core::storage::StoredFile { key: key.into(), mime_type: mime.into(), size_bytes: size, filename: "lie.png".into() }
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

fn storage_with(tenant: &str, project: uuid::Uuid, files: &[(&str, &[u8])]) -> (MapStorage, Manifest) {
    let mut storage = MapStorage::default();
    let mut manifest = Manifest::new();
    for (path, bytes) in files {
        let hash = weft_core::project::hash::sha256_hex(bytes);
        storage.files.insert(format!("{tenant}/asset/{project}/{hash}"), bytes.to_vec());
        manifest.insert(path.to_string(), hash);
    }
    (storage, manifest)
}

/// A version's files come back where the manifest puts them, each checked
/// against its hash, with this install's catalog seeded beside them.
#[tokio::test]
async fn a_version_is_laid_out_and_checked() {
    let project = uuid::Uuid::from_u128(9);
    let (storage, mut manifest) = storage_with("local", project, &[("weft.toml", b"[package]"), ("src/main.weft", b"x")]);
    let root = tempfile::tempdir().unwrap();
    // The entry this install writes for its own catalog.
    let seeded = tempfile::tempdir().unwrap();
    weft_compiler::project::seed_base_catalog(seeded.path()).unwrap();
    manifest.insert(weft_compiler::project::weft_entry(seeded.path()).unwrap(), String::new());
    source::materialize(&storage, "local", project, &manifest, root.path()).await.unwrap();
    assert_eq!(std::fs::read(root.path().join("src/main.weft")).unwrap(), b"x");
    assert!(root.path().join("nodes/base_catalog").is_dir());
}

/// Bytes that do not hash to what the manifest names are refused, and so
/// is a path that would land outside the build's directory.
#[tokio::test]
async fn a_tampered_blob_or_a_climbing_path_is_refused() {
    let project = uuid::Uuid::from_u128(9);
    let (mut storage, manifest) = storage_with("local", project, &[("weft.toml", b"[package]")]);
    let key = storage.files.keys().next().unwrap().clone();
    storage.files.insert(key, b"other bytes".to_vec());
    let root = tempfile::tempdir().unwrap();
    let e = source::materialize(&storage, "local", project, &manifest, root.path()).await.unwrap_err().to_string();
    assert!(e.contains("other bytes") || e.contains("came back as"), "{e}");

    let (storage, manifest) = storage_with("local", project, &[("../escape", b"x")]);
    let e = source::materialize(&storage, "local", project, &manifest, root.path()).await.unwrap_err().to_string();
    assert!(e.contains("not a project-relative path"), "{e}");
}
