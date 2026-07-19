//! Layer-3 contract tests: the real sync orchestration against hand-rolled
//! in-memory fakes of its two I/O seams (project files + the asset store).

use std::collections::BTreeMap;
use std::io::Read;
use std::sync::Mutex;

use weft_assets::{hash_reader, sync_assets, AssetSource, AssetStore};
use weft_core::project::FileRef;
use weft_core::weft_type::{WeftPrimitive, WeftType};

/// Dumb in-memory project files: path -> bytes.
struct FakeSource(BTreeMap<String, Vec<u8>>);

impl AssetSource for FakeSource {
    fn open(&self, path: &str) -> anyhow::Result<Box<dyn Read + Send>> {
        match self.0.get(path) {
            Some(bytes) => Ok(Box::new(std::io::Cursor::new(bytes.clone()))),
            None => anyhow::bail!("asset not found: {path}"),
        }
    }
}

/// Dumb in-memory asset store: hash -> key, plus append-only call logs.
/// The two fault knobs make one upload (by hash) or every delete fail, so
/// the sync's abort-loudly promise is testable.
#[derive(Default)]
struct FakeStore {
    existing: Mutex<BTreeMap<String, String>>,
    uploads: Mutex<Vec<String>>,
    deletes: Mutex<Vec<String>>,
    fail_upload_of: Option<String>,
    fail_deletes: bool,
}

fn key_for(hash: &str) -> String {
    format!("t/asset/p/{hash}")
}

#[async_trait::async_trait]
impl AssetStore for FakeStore {
    async fn list(&self) -> anyhow::Result<BTreeMap<String, String>> {
        Ok(self.existing.lock().unwrap().clone())
    }
    async fn upload(
        &self,
        hash: &str,
        _mime: &str,
        _filename: &str,
        size_bytes: u64,
        bytes: &mut (dyn Read + Send),
    ) -> anyhow::Result<String> {
        if self.fail_upload_of.as_deref() == Some(hash) {
            anyhow::bail!("store rejected upload of {hash}");
        }
        // The fake verifies the CONTRACT the broker enforces: the streamed
        // bytes really are `size_bytes` long and hash to `hash`.
        let (streamed_hash, streamed_size) = hash_reader(bytes)?;
        assert_eq!(streamed_hash, hash, "uploaded bytes hash to the declared id");
        assert_eq!(streamed_size, size_bytes, "uploaded bytes match the declared size");
        let key = key_for(hash);
        self.existing.lock().unwrap().insert(hash.to_string(), key.clone());
        self.uploads.lock().unwrap().push(hash.to_string());
        Ok(key)
    }
    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        if self.fail_deletes {
            anyhow::bail!("store rejected delete of {key}");
        }
        self.existing.lock().unwrap().retain(|_, k| k != key);
        self.deletes.lock().unwrap().push(key.to_string());
        Ok(())
    }
}

fn image_ref(path: &str) -> FileRef {
    FileRef {
        path: path.into(),
        ty: WeftType::Primitive(WeftPrimitive::Image),
        marker: weft_core::project::FileMarker::Asset,
    }
}

fn sha(bytes: &[u8]) -> String {
    hash_reader(std::io::Cursor::new(bytes.to_vec())).unwrap().0
}

#[tokio::test]
async fn a_fresh_sync_uploads_and_resolves_markers() {
    let source = FakeSource(BTreeMap::from([
        ("assets/pic.png".to_string(), b"PNGBYTES".to_vec()),
        ("assets/clip.wav".to_string(), b"WAVBYTES".to_vec()),
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
    assert!(store.deletes.lock().unwrap().is_empty());

    // The marker kind comes from the DECLARED type; the key is the hash key.
    let pic = &map["assets/pic.png"]["__weft_image__"];
    assert_eq!(pic["key"], key_for(&sha(b"PNGBYTES")));
    assert_eq!(pic["sizeBytes"], 8);
    assert_eq!(pic["filename"], "assets/pic.png");
    assert!(map["assets/clip.wav"].get("__weft_audio__").is_some());
}

#[tokio::test]
async fn an_unchanged_sync_moves_nothing() {
    let source = FakeSource(BTreeMap::from([("a.png".to_string(), b"X".to_vec())]));
    let store = FakeStore::default();
    let refs = vec![image_ref("a.png")];
    sync_assets(&refs, &source, &store).await.unwrap();
    let map = sync_assets(&refs, &source, &store).await.unwrap();
    assert_eq!(store.uploads.lock().unwrap().len(), 1, "second sync uploads nothing");
    assert!(store.deletes.lock().unwrap().is_empty());
    assert_eq!(map["a.png"]["__weft_image__"]["key"], key_for(&sha(b"X")));
}

#[tokio::test]
async fn a_changed_file_replaces_its_stored_content() {
    let store = FakeStore::default();
    let refs = vec![image_ref("a.png")];
    let v1 = FakeSource(BTreeMap::from([("a.png".to_string(), b"OLD".to_vec())]));
    sync_assets(&refs, &v1, &store).await.unwrap();
    let v2 = FakeSource(BTreeMap::from([("a.png".to_string(), b"NEW".to_vec())]));
    let map = sync_assets(&refs, &v2, &store).await.unwrap();

    assert_eq!(store.uploads.lock().unwrap().as_slice(), &[sha(b"OLD"), sha(b"NEW")]);
    assert_eq!(store.deletes.lock().unwrap().as_slice(), &[key_for(&sha(b"OLD"))]);
    assert_eq!(map["a.png"]["__weft_image__"]["key"], key_for(&sha(b"NEW")));
}

#[tokio::test]
async fn a_dropped_ref_deletes_its_stored_content() {
    let source = FakeSource(BTreeMap::from([
        ("a.png".to_string(), b"A".to_vec()),
        ("b.png".to_string(), b"B".to_vec()),
    ]));
    let store = FakeStore::default();
    sync_assets(&[image_ref("a.png"), image_ref("b.png")], &source, &store).await.unwrap();
    let map = sync_assets(&[image_ref("a.png")], &source, &store).await.unwrap();
    assert_eq!(store.deletes.lock().unwrap().as_slice(), &[key_for(&sha(b"B"))]);
    assert!(map.contains_key("a.png") && !map.contains_key("b.png"));
}

#[tokio::test]
async fn identical_bytes_under_two_paths_share_one_stored_asset() {
    let source = FakeSource(BTreeMap::from([
        ("a.png".to_string(), b"SAME".to_vec()),
        ("b.png".to_string(), b"SAME".to_vec()),
    ]));
    let store = FakeStore::default();
    let map =
        sync_assets(&[image_ref("a.png"), image_ref("b.png")], &source, &store).await.unwrap();
    assert_eq!(store.uploads.lock().unwrap().len(), 1, "one upload for identical bytes");
    assert_eq!(
        map["a.png"]["__weft_image__"]["key"],
        map["b.png"]["__weft_image__"]["key"]
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
        ("a.png".to_string(), b"A".to_vec()),
        ("b.png".to_string(), b"B".to_vec()),
    ]));
    // Refs hash in order, so failing `a.png`'s hash stops the pass before
    // `b.png` uploads: an aborted sync moves nothing further.
    let store = FakeStore { fail_upload_of: Some(sha(b"A")), ..FakeStore::default() };
    let err = sync_assets(&[image_ref("a.png"), image_ref("b.png")], &source, &store)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("a.png"), "the failed path is named: {err}");
    assert!(store.uploads.lock().unwrap().is_empty(), "no upload landed after the abort");
    assert!(store.deletes.lock().unwrap().is_empty(), "an aborted sync deletes nothing");
}

#[tokio::test]
async fn a_delete_failure_aborts_loudly_naming_the_key() {
    let source = FakeSource(BTreeMap::from([("a.png".to_string(), b"A".to_vec())]));
    let store = FakeStore::default();
    sync_assets(&[image_ref("a.png"), image_ref("b.png")], &FakeSource(BTreeMap::from([
        ("a.png".to_string(), b"A".to_vec()),
        ("b.png".to_string(), b"B".to_vec()),
    ])), &store).await.unwrap();

    let store = FakeStore {
        existing: Mutex::new(store.existing.lock().unwrap().clone()),
        fail_deletes: true,
        ..FakeStore::default()
    };
    let err =
        sync_assets(&[image_ref("a.png")], &source, &store).await.unwrap_err().to_string();
    assert!(err.contains(&key_for(&sha(b"B"))), "the failed key is named: {err}");
}

#[tokio::test]
async fn an_already_listed_hash_resolves_without_uploading() {
    // The idempotent path: the store already holds this content (a previous
    // sync, or a concurrent build finished first); the sync resolves to the
    // existing key and transfers nothing.
    let source = FakeSource(BTreeMap::from([("a.png".to_string(), b"A".to_vec())]));
    let store = FakeStore::default();
    store.existing.lock().unwrap().insert(sha(b"A"), key_for(&sha(b"A")));
    let map = sync_assets(&[image_ref("a.png")], &source, &store).await.unwrap();
    assert!(store.uploads.lock().unwrap().is_empty(), "already-stored content re-uploads nothing");
    assert_eq!(map["a.png"]["__weft_image__"]["key"], key_for(&sha(b"A")));
}
