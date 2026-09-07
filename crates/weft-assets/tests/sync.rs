//! Layer-3 contract tests: the real sync orchestration against hand-rolled
//! in-memory fakes of its two I/O seams (project files + the asset store).

use std::collections::BTreeMap;
use std::io::Read;
use std::sync::Mutex;

use serde_json::json;
use weft_assets::{hash_reader, referenced_asset_keys, sync_assets, AssetSource, AssetStore};
use weft_core::project::{FileRef, ProjectDefinition};
use weft_core::storage::StoredFile;
use weft_core::weft_type::{WeftPrimitive, WeftType};

/// Dumb in-memory project files: path -> bytes.
struct FakeSource(BTreeMap<String, Vec<u8>>);

/// A file that changes under the sync: each open hands out the next
/// version, so the hashing read and the upload read disagree.
struct ChangingSource {
    path: String,
    versions: Mutex<Vec<Vec<u8>>>,
}

impl AssetSource for ChangingSource {
    fn open(&self, path: &str) -> anyhow::Result<Box<dyn Read + Send>> {
        anyhow::ensure!(path == self.path, "asset not found: {path}");
        let mut versions = self.versions.lock().unwrap();
        anyhow::ensure!(!versions.is_empty(), "opened more times than versions");
        Ok(Box::new(std::io::Cursor::new(versions.remove(0))))
    }
}

impl AssetSource for FakeSource {
    fn open(&self, path: &str) -> anyhow::Result<Box<dyn Read + Send>> {
        match self.0.get(path) {
            Some(bytes) => Ok(Box::new(std::io::Cursor::new(bytes.clone()))),
            None => anyhow::bail!("asset not found: {path}"),
        }
    }
}

/// Dumb in-memory asset store: hash -> key, plus append-only call logs.
/// An upload fault makes the sync's abort-loudly promise testable.
#[derive(Default)]
struct FakeStore {
    existing: Mutex<BTreeMap<String, String>>,
    uploads: Mutex<Vec<String>>,
    deletes: Mutex<Vec<String>>,
    /// (hash, mime) of every upload, for the media-type contract.
    mimes: Mutex<Vec<(String, String)>>,
    fail_upload_of: Option<String>,
    /// A hash the store already holds when the upload starts: it reads
    /// nothing and answers the key it has (the begin verb's
    /// `already_stored`, which a concurrent build's completed upload
    /// produces between our list and our begin).
    already_stored_of: Option<String>,
}

fn key_for(hash: &str) -> String {
    format!("t/asset/p/{hash}")
}

#[async_trait::async_trait]
impl AssetStore for FakeStore {
    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.existing.lock().unwrap().retain(|_, k| k != key);
        self.deletes.lock().unwrap().push(key.to_string());
        Ok(())
    }
    async fn list(&self) -> anyhow::Result<BTreeMap<String, String>> {
        Ok(self.existing.lock().unwrap().clone())
    }
    async fn upload(
        &self,
        hash: &str,
        mime: &str,
        _filename: &str,
        size_bytes: u64,
        bytes: &mut (dyn Read + Send),
    ) -> anyhow::Result<weft_assets::Uploaded> {
        if self.already_stored_of.as_deref() == Some(hash) {
            // The store already held this content: it reads nothing and
            // answers the key it has, exactly as the begin verb does.
            return Ok(weft_assets::Uploaded::AlreadyStored(key_for(hash)));
        }
        if self.fail_upload_of.as_deref() == Some(hash) {
            anyhow::bail!("store rejected upload of {hash}");
        }
        // The fake verifies what the broker enforces: the streamed bytes
        // really are `size_bytes` long. (The broker checks only the size;
        // the hash is the sync's own promise, tested through `delete`.)
        let (_, streamed_size) = hash_reader(bytes)?;
        assert_eq!(streamed_size, size_bytes, "uploaded bytes match the declared size");
        self.mimes.lock().unwrap().push((hash.to_string(), mime.to_string()));
        let key = key_for(hash);
        self.existing.lock().unwrap().insert(hash.to_string(), key.clone());
        self.uploads.lock().unwrap().push(hash.to_string());
        Ok(weft_assets::Uploaded::Stored(key))
    }
}

fn image_ref(path: &str) -> FileRef {
    FileRef {
        path: path.into(),
        ty: WeftType::Primitive(WeftPrimitive::Image),
        marker: weft_core::project::FileMarker::Asset,
    }
}

/// The map key a ref of `path` declared `ty` resolves under.
fn key_of(path: &str, ty: WeftPrimitive) -> String {
    FileRef { path: path.into(), ty: WeftType::Primitive(ty), marker: weft_core::project::FileMarker::Asset }
        .resolution_key()
}
fn image_key(path: &str) -> String {
    key_of(path, WeftPrimitive::Image)
}
fn audio_key(path: &str) -> String {
    key_of(path, WeftPrimitive::Audio)
}
fn blob_key(path: &str) -> String {
    key_of(path, WeftPrimitive::Blob)
}

fn sha(bytes: &[u8]) -> String {
    hash_reader(std::io::Cursor::new(bytes.to_vec())).unwrap().0
}

/// Bytes that announce themselves as a PNG: the signature, then `tag`
/// (the sync holds every file to the kind its ref declares).
fn png(tag: &[u8]) -> Vec<u8> {
    [b"\x89PNG\r\n\x1a\n".as_slice(), tag].concat()
}

/// Bytes that announce themselves as a WAV.
fn wav(tag: &[u8]) -> Vec<u8> {
    [b"RIFF\0\0\0\0WAVEfmt ".as_slice(), tag].concat()
}

#[test]
fn source_asset_references_include_nested_and_key_selected_files_but_not_generated_files() {
    let project_id = "00000000-0000-0000-0000-000000000001";
    let own_key = format!("t/asset/{project_id}/{}", "a".repeat(64));
    let file = |key: String| StoredFile {
        key, mime_type: "image/png".into(), filename: "cat.png".into(), size_bytes: 3,
    }.to_value();
    let mut project: ProjectDefinition = serde_json::from_value(json!({
        "id": project_id,
        "edges": [],
        "nodes": [{
            "id": "send", "nodeType": "Example", "position": {"x": 0, "y": 0},
            "inputs": [{"name": "photos", "portType": "List[Image]", "required": true}],
            "portLiterals": {"photos": [
                file(own_key.clone()), file(own_key.clone()),
                file("t/exec/c/generated".into()),
                file(format!("t/asset/another-project/{}", "b".repeat(64)))
            ]}
        }]
    })).unwrap();
    assert_eq!(referenced_asset_keys(&project).unwrap(), vec![own_key.clone()]);
    let value = project.nodes[0].port_literals.remove("photos").unwrap();
    project.nodes[0].config = json!({"photos": value});
    assert_eq!(referenced_asset_keys(&project).unwrap(), vec![own_key]);
    project.nodes[0].config = json!({});
    assert!(referenced_asset_keys(&project).unwrap().is_empty());
}

#[tokio::test]
async fn a_fresh_sync_uploads_and_resolves_markers() {
    let source = FakeSource(BTreeMap::from([
        ("assets/pic.png".to_string(), png(b"PNGBYTES")),
        ("assets/clip.wav".to_string(), wav(b"WAVBYTES")),
    ]));
    let store = FakeStore::default();
    let refs = vec![
        image_ref("assets/pic.png"),
        FileRef {
            path: "assets/clip.wav".into(),
            ty: WeftType::Primitive(WeftPrimitive::Audio),
            marker: weft_core::project::FileMarker::Asset,
        },
    ];

    let map = sync_assets(&refs, &source, &store).await.unwrap();
    assert_eq!(store.uploads.lock().unwrap().len(), 2);
    assert_eq!(store.existing.lock().unwrap().len(), 2);

    // The marker kind comes from the DECLARED type; the key is the hash key.
    let pic = &map[&image_key("assets/pic.png")]["__weft_image__"];
    assert_eq!(pic["key"], key_for(&sha(&png(b"PNGBYTES"))));
    assert_eq!(pic["sizeBytes"], png(b"PNGBYTES").len());
    assert_eq!(pic["filename"], "assets/pic.png");
    assert!(map[&audio_key("assets/clip.wav")].get("__weft_audio__").is_some());
}

#[tokio::test]
async fn an_unchanged_sync_moves_nothing() {
    let source = FakeSource(BTreeMap::from([("a.png".to_string(), png(b"X"))]));
    let store = FakeStore::default();
    let refs = vec![image_ref("a.png")];
    sync_assets(&refs, &source, &store).await.unwrap();
    let map = sync_assets(&refs, &source, &store).await.unwrap();
    assert_eq!(store.uploads.lock().unwrap().len(), 1, "second sync uploads nothing");
    assert_eq!(store.existing.lock().unwrap().len(), 1);
    assert_eq!(map[&image_key("a.png")]["__weft_image__"]["key"], key_for(&sha(&png(b"X"))));
}

#[tokio::test]
async fn a_changed_file_preserves_the_copy_an_old_run_needs() {
    let store = FakeStore::default();
    let refs = vec![image_ref("a.png")];
    let v1 = FakeSource(BTreeMap::from([("a.png".to_string(), png(b"OLD"))]));
    sync_assets(&refs, &v1, &store).await.unwrap();
    let v2 = FakeSource(BTreeMap::from([("a.png".to_string(), png(b"NEW"))]));
    let map = sync_assets(&refs, &v2, &store).await.unwrap();

    assert_eq!(store.uploads.lock().unwrap().as_slice(), &[sha(&png(b"OLD")), sha(&png(b"NEW"))]);
    assert_eq!(store.existing.lock().unwrap().get(&sha(&png(b"OLD"))), Some(&key_for(&sha(&png(b"OLD")))));
    assert_eq!(store.existing.lock().unwrap().len(), 2);
    assert_eq!(map[&image_key("a.png")]["__weft_image__"]["key"], key_for(&sha(&png(b"NEW"))));
}

#[tokio::test]
async fn a_dropped_ref_preserves_its_stored_content_for_expiry() {
    let source = FakeSource(BTreeMap::from([
        ("a.png".to_string(), png(b"A")),
        ("b.png".to_string(), png(b"B")),
    ]));
    let store = FakeStore::default();
    sync_assets(&[image_ref("a.png"), image_ref("b.png")], &source, &store).await.unwrap();
    let map = sync_assets(&[image_ref("a.png")], &source, &store).await.unwrap();
    assert_eq!(store.existing.lock().unwrap().get(&sha(&png(b"B"))), Some(&key_for(&sha(&png(b"B")))));
    assert!(map.contains_key(&image_key("a.png")) && !map.contains_key(&image_key("b.png")));
}

#[tokio::test]
async fn identical_bytes_under_two_paths_share_one_stored_asset() {
    let source = FakeSource(BTreeMap::from([
        ("a.png".to_string(), png(b"SAME")),
        ("b.png".to_string(), png(b"SAME")),
    ]));
    let store = FakeStore::default();
    let map =
        sync_assets(&[image_ref("a.png"), image_ref("b.png")], &source, &store).await.unwrap();
    assert_eq!(store.uploads.lock().unwrap().len(), 1, "one upload for identical bytes");
    assert_eq!(
        map[&image_key("a.png")]["__weft_image__"]["key"],
        map[&image_key("b.png")]["__weft_image__"]["key"]
    );
}

#[tokio::test]
async fn every_broken_ref_is_named_in_one_error() {
    let source = FakeSource(BTreeMap::new());
    let store = FakeStore::default();
    let err = sync_assets(&[image_ref("gone1.png"), image_ref("gone2.png")], &source, &store)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("gone1.png") && err.contains("gone2.png"), "both named: {err}");
    assert!(store.uploads.lock().unwrap().is_empty(), "nothing uploaded on a broken set");
}

#[tokio::test]
async fn an_upload_failure_aborts_loudly_naming_the_path() {
    let source = FakeSource(BTreeMap::from([
        ("a.png".to_string(), png(b"A")),
        ("b.png".to_string(), png(b"B")),
    ]));
    // Refs hash in order, so failing `a.png`'s hash stops the pass before
    // `b.png` uploads: an aborted sync moves nothing further.
    let store = FakeStore { fail_upload_of: Some(sha(&png(b"A"))), ..FakeStore::default() };
    let err = sync_assets(&[image_ref("a.png"), image_ref("b.png")], &source, &store)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("a.png"), "the failed path is named: {err}");
    assert!(store.uploads.lock().unwrap().is_empty(), "no upload landed after the abort");
    assert!(store.existing.lock().unwrap().is_empty());
}

#[tokio::test]
async fn removing_all_refs_keeps_old_files_for_the_storage_expiry_rule() {
    let source = FakeSource(BTreeMap::from([("a.png".to_string(), png(b"A"))]));
    let store = FakeStore::default();
    sync_assets(&[image_ref("a.png"), image_ref("b.png")], &FakeSource(BTreeMap::from([
        ("a.png".to_string(), png(b"A")),
        ("b.png".to_string(), png(b"B")),
    ])), &store).await.unwrap();

    let map = sync_assets(&[], &source, &store).await.unwrap();
    assert!(map.is_empty());
    assert_eq!(store.existing.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn an_already_listed_hash_resolves_without_uploading() {
    // The idempotent path: the store already holds this content (a previous
    // sync, or a concurrent build finished first); the sync resolves to the
    // existing key and transfers nothing.
    let source = FakeSource(BTreeMap::from([("a.png".to_string(), png(b"A"))]));
    let store = FakeStore::default();
    store.existing.lock().unwrap().insert(sha(&png(b"A")), key_for(&sha(&png(b"A"))));
    let map = sync_assets(&[image_ref("a.png")], &source, &store).await.unwrap();
    assert!(store.uploads.lock().unwrap().is_empty(), "already-stored content re-uploads nothing");
    assert_eq!(map[&image_key("a.png")]["__weft_image__"]["key"], key_for(&sha(&png(b"A"))));
}

#[tokio::test]
async fn bytes_are_held_to_the_declared_kind() {
    // An `Image` declaration over WAV bytes, and over bytes with no known
    // signature, both refuse naming the file and the kinds; `Blob` takes
    // anything, and nothing uploads while a ref is refused.
    let source = FakeSource(BTreeMap::from([
        ("a.png".to_string(), wav(b"NOTANIMAGE")),
        ("notes.txt".to_string(), b"just text".to_vec()),
        ("ok.png".to_string(), png(b"OK")),
    ]));
    let store = FakeStore::default();
    let err = sync_assets(&[image_ref("a.png"), image_ref("notes.txt"), image_ref("ok.png")], &source, &store)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("@asset(\"a.png\", Image): the file's bytes are Audio, not Image"), "{err}");
    assert!(err.contains("@asset(\"notes.txt\", Image)") && err.contains("no Image signature"), "{err}");
    assert!(store.uploads.lock().unwrap().is_empty(), "nothing uploaded while a ref is refused");

    let blob = FileRef {
        path: "notes.txt".into(),
        ty: WeftType::Primitive(WeftPrimitive::Blob),
        marker: weft_core::project::FileMarker::Asset,
    };
    let map = sync_assets(&[blob, image_ref("ok.png")], &source, &store).await.unwrap();
    assert!(map[&blob_key("notes.txt")].get("__weft_blob__").is_some());
    assert!(map[&image_key("ok.png")].get("__weft_image__").is_some());
}

#[tokio::test]
async fn one_path_under_two_types_is_checked_under_each() {
    // The same wav declared `Audio` in one place and `Image` in another:
    // the Audio ref is fine, the Image ref is refused naming itself, and
    // the refusal does not depend on which ref comes first (keyed by path
    // alone, the first declaration's check once covered the second).
    let source = FakeSource(BTreeMap::from([("clip.wav".to_string(), wav(b"X"))]));
    let store = FakeStore::default();
    let audio = FileRef {
        path: "clip.wav".into(),
        ty: WeftType::Primitive(WeftPrimitive::Audio),
        marker: weft_core::project::FileMarker::Asset,
    };
    let err = sync_assets(&[audio.clone(), image_ref("clip.wav")], &source, &store)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("@asset(\"clip.wav\", Image)") && err.contains("are Audio, not Image"), "{err}");
    assert!(store.uploads.lock().unwrap().is_empty(), "nothing uploads while a ref is broken");

    // Two agreeing declarations of one path: one upload, one value each.
    let map = sync_assets(&[audio.clone(), audio], &source, &store).await.unwrap();
    assert_eq!(store.uploads.lock().unwrap().len(), 1);
    assert!(map[&audio_key("clip.wav")].get("__weft_audio__").is_some());
}

#[tokio::test]
async fn a_file_edited_between_the_two_reads_is_refused_and_its_upload_discarded() {
    let source = ChangingSource {
        path: "a.png".into(),
        versions: Mutex::new(vec![png(b"one"), png(b"two")]),
    };
    let store = FakeStore::default();
    let err = sync_assets(&[image_ref("a.png")], &source, &store).await.unwrap_err();
    assert!(err.to_string().contains("a.png changed while it was being synced"), "{err:#}");
    let first = sha(&png(b"one"));
    assert_eq!(*store.deletes.lock().unwrap(), vec![key_for(&first)], "the mis-hashed upload is removed");
    assert!(store.existing.lock().unwrap().is_empty(), "nothing is left under the first hash");
}

#[tokio::test]
async fn the_stored_media_type_comes_from_the_bytes_before_the_filename() {
    let source = FakeSource(BTreeMap::from([
        ("pic.txt".to_string(), png(b"x")),
        ("notes.txt".to_string(), b"plain words".to_vec()),
    ]));
    let store = FakeStore::default();
    let blob = |path: &str| FileRef {
        path: path.into(),
        ty: WeftType::primitive(WeftPrimitive::Blob),
        marker: weft_core::project::FileMarker::Asset,
    };
    let map = sync_assets(&[blob("pic.txt"), blob("notes.txt")], &source, &store).await.unwrap();
    let mimes: BTreeMap<String, String> = store.mimes.lock().unwrap().iter().cloned().collect();
    assert_eq!(mimes[&sha(&png(b"x"))], "image/png", "a PNG named .txt is stored as a PNG");
    assert_eq!(mimes[&sha(b"plain words")], "text/plain", "no signature: the filename decides");
    assert_eq!(map[&blob_key("pic.txt")]["__weft_blob__"]["mimeType"], json!("image/png"));
}

/// A concurrent build finished uploading this exact content between our
/// list and our begin. The store reads nothing and answers its key, so
/// there is no streamed hash to hold the upload to: checking one anyway
/// compared the file against the empty digest and deleted a healthy
/// asset out from under the build that made it.
#[tokio::test]
async fn content_the_store_already_holds_is_kept_not_deleted() {
    let bytes = png(b"shared");
    let hash = sha(&bytes);
    let source = FakeSource(BTreeMap::from([("logo.png".to_string(), bytes)]));
    let store = FakeStore { already_stored_of: Some(hash.clone()), ..FakeStore::default() };
    let refs = vec![image_ref("logo.png")];
    let map = sync_assets(&refs, &source, &store).await.expect("an already-stored asset syncs");
    assert!(store.deletes.lock().unwrap().is_empty(), "nothing is deleted");
    assert!(store.uploads.lock().unwrap().is_empty(), "nothing is transferred");
    assert_eq!(
        map[&image_key("logo.png")]["__weft_image__"]["key"],
        serde_json::json!(key_for(&hash)),
        "the ref resolves to the key the store already had"
    );
}
