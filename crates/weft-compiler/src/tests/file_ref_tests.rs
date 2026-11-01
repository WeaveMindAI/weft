use super::*;
use weft_core::project::FileMarker;
use weft_core::WeftPrimitive;

/// A text-typed `@file` ref (the bidirectional form).
fn text_ref(path: &str, ty: WeftType) -> FileRef {
    FileRef { path: path.into(), ty, marker: FileMarker::File }
}

/// What resolving a `@file` of `text` cast to `ty` gives: the value, and
/// the content it was read from.
fn read(path: &str, text: &str, ty: WeftType, value: serde_json::Value) -> Resolved {
    let (content, cast) = weft_core::content::ContentRef::of_text(text, &ty, path).unwrap();
    assert_eq!(cast, value);
    Resolved::Read(content, value)
}

/// A file-typed `@asset` ref (the deferred pull-only form).
fn asset_ref(path: &str, ty: WeftType) -> FileRef {
    FileRef { path: path.into(), ty, marker: FileMarker::Asset }
}

// ── Marker parsing (Layer 1, pure) ──────────────────────────────────────

#[test]
fn not_a_marker_returns_none() {
    assert!(parse_marker("hello world").is_none());
    assert!(parse_marker("\"a quoted string\"").is_none());
    assert!(parse_marker("42").is_none());
    // A bare @ that isn't @file is not ours.
    assert!(parse_marker("@require_one_of(a, b)").is_none());
}

#[test]
fn marker_default_type_is_string() {
    let fr = parse_marker("@file(\"prompts/system.txt\")").unwrap().unwrap();
    assert_eq!(fr.path, "prompts/system.txt");
    assert_eq!(fr.ty, WeftType::Primitive(WeftPrimitive::String));
    assert_eq!(fr.marker, FileMarker::File);
}

#[test]
fn asset_marker_parses_with_the_same_grammar() {
    let fr = parse_marker("@asset(\"assets/pic.png\", Image)").unwrap().unwrap();
    assert_eq!(fr.path, "assets/pic.png");
    assert_eq!(fr.ty, WeftType::Primitive(WeftPrimitive::Image));
    assert_eq!(fr.marker, FileMarker::Asset);
    // Errors name the directive that was written.
    let err = parse_marker("@asset(\"x.png\", Banana)").unwrap().unwrap_err();
    assert!(err.contains("@asset"), "got: {err}");
}

#[test]
fn marker_refuses_a_spaced_paren_loudly() {
    // `@file ("x")` (space before paren) is NOT a marker argument list:
    // tolerating it once made prose like `@file ("notes.txt") is what I
    // want` read a file off disk and swallow the sentence. At the top
    // of a field it is a malformed marker and fails loud, so the fix
    // (delete the space) is one keystroke away.
    assert!(matches!(parse_marker("@file (\"p.txt\")"), Some(Err(_))));
    // Trailing text after the closing paren is prose, never a marker
    // whose surroundings get silently replaced by the file's contents.
    assert!(matches!(parse_marker("@file(\"p.txt\") please"), Some(Err(_))));
}

#[test]
fn file_without_paren_fails_loudly() {
    // `@file` not followed by `(` is a malformed marker, not a silent
    // pass-through as an ordinary string value.
    assert!(matches!(parse_marker("@file"), Some(Err(_))));
    assert!(matches!(parse_marker("@file \"x\""), Some(Err(_))));
}

#[test]
fn marker_with_explicit_type() {
    let fr = parse_marker("@file(\"schema.json\", JsonDict)").unwrap().unwrap();
    assert_eq!(fr.path, "schema.json");
    assert_eq!(fr.ty, WeftType::JsonDict);

    let fr = parse_marker("@file(\"n.txt\", Number)").unwrap().unwrap();
    assert_eq!(fr.ty, WeftType::Primitive(WeftPrimitive::Number));
}

#[test]
fn scoped_aliases_with_the_same_name_are_distinct_asset_contracts() {
    let image = parse_marker("@asset(\"same.bin\", Local=Image)").unwrap().unwrap();
    let audio = parse_marker("@asset(\"same.bin\", Local=Audio)").unwrap().unwrap();
    assert_eq!(image.ty.to_string(), audio.ty.to_string());
    assert_ne!(image.resolution_key(), audio.resolution_key());
}

/// A path may hold any character, a separator's included, so two
/// references whose paths differ only by one (a NUL, a quote, a comma, a
/// suffix spelled like a type) never share a key, and the key keeps the
/// path exactly as written.
#[test]
fn paths_differing_by_any_character_are_distinct_keys() {
    let string = || WeftType::Primitive(WeftPrimitive::String);
    let refs = [
        asset_ref("a", string()),
        asset_ref("a\u{0}", string()),
        asset_ref("a\u{0}String", string()),
        asset_ref("a\"", string()),
        asset_ref("a,", string()),
        asset_ref("a, String", string()),
        asset_ref("a", WeftType::Primitive(WeftPrimitive::Image)),
    ];
    let keys: std::collections::BTreeSet<_> = refs.iter().map(FileRef::resolution_key).collect();
    assert_eq!(keys.len(), refs.len(), "{keys:?}");
    for r in &refs {
        assert_eq!(r.resolution_key().path, r.path);
    }
}

#[test]
fn marker_with_bracketed_type_containing_comma() {
    // The comma inside Dict[String, Number] must not be read as the
    // path/type separator.
    let fr = parse_marker("@file(\"d.json\", Dict[String, Number])").unwrap().unwrap();
    assert_eq!(fr.path, "d.json");
    assert_eq!(
        fr.ty,
        WeftType::dict(WeftType::Primitive(WeftPrimitive::Number))
    );
}

#[test]
fn marker_tolerates_whitespace() {
    let fr = parse_marker("  @file(  \"p.txt\" ,  Number )  ").unwrap().unwrap();
    assert_eq!(fr.path, "p.txt");
    assert_eq!(fr.ty, WeftType::Primitive(WeftPrimitive::Number));
}

#[test]
fn malformed_markers_error() {
    // Unquoted path.
    assert!(parse_marker("@file(prompts/system.txt)").unwrap().is_err());
    // Missing closing paren.
    assert!(parse_marker("@file(\"x.txt\"").unwrap().is_err());
    // Empty path.
    assert!(parse_marker("@file(\"\")").unwrap().is_err());
    // Trailing comma, no type.
    assert!(parse_marker("@file(\"x.txt\",)").unwrap().is_err());
    // Unknown type.
    assert!(parse_marker("@file(\"x.txt\", Banana)").unwrap().is_err());
}

// ── Resolution (Layer 3, tempdir) ───────────────────────────────────────

#[test]
fn resolve_string_verbatim() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("p.txt"), "you are a helpful poet").unwrap();
    let fr = text_ref("p.txt", WeftType::Primitive(WeftPrimitive::String));
    assert_eq!(
        resolve(&fr, &crate::file_reader::CompileFs::disk(dir.path())).unwrap(),
        read("p.txt", "you are a helpful poet", WeftType::Primitive(WeftPrimitive::String), serde_json::json!("you are a helpful poet"))
    );
    // A text-typed `@asset` is deferred like every asset: the build reads
    // it, from anywhere on the machine, and the parse keeps the marker.
    let fr = asset_ref("p.txt", WeftType::Primitive(WeftPrimitive::String));
    assert_eq!(resolve(&fr, &crate::file_reader::CompileFs::disk(dir.path())).unwrap(), Resolved::Deferred);
}

#[test]
fn resolve_json_dict() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("c.json"), r#"{"model": "gpt", "temp": 0.7}"#).unwrap();
    let fr = text_ref("c.json", WeftType::JsonDict);
    assert_eq!(
        resolve(&fr, &crate::file_reader::CompileFs::disk(dir.path())).unwrap(),
        read("c.json", r#"{"model": "gpt", "temp": 0.7}"#, WeftType::JsonDict, serde_json::json!({"model": "gpt", "temp": 0.7}))
    );
}

#[test]
fn file_typed_asset_refs_always_defer_at_parse() {
    // A file-typed `@asset` never reads bytes at parse: keep the marker
    // string, record the ref, no error, even though no such file exists on
    // disk. This holds for path AND url refs; resolution is the explicit
    // build post-pass.
    let dir = tempfile::tempdir().unwrap();
    let fs = crate::file_reader::CompileFs::disk(dir.path());
    for path in ["assets/pic.png", "https://ex.com/a.png"] {
        let fr = asset_ref(path, WeftType::Primitive(WeftPrimitive::Image));
        assert_eq!(resolve(&fr, &fs).unwrap(), Resolved::Deferred, "{path}");
    }
}

#[test]
fn file_marker_rejects_types_without_bidirectional_edit() {
    // `@file`'s contract is write-back; a file-typed value can't be written
    // back as text, so it's an error that points at `@asset`.
    let dir = tempfile::tempdir().unwrap();
    let fs = crate::file_reader::CompileFs::disk(dir.path());
    let fr = text_ref("assets/pic.png", WeftType::Primitive(WeftPrimitive::Image));
    let err = resolve(&fr, &fs).unwrap_err();
    assert!(err.contains("use @asset"), "got: {err}");
}

#[test]
fn text_typed_asset_from_a_url_or_a_stored_file_defers_to_the_build() {
    // Nothing on disk to read at parse: the build reads the bytes and
    // casts them ([`read_texts`]); the parse keeps the marker.
    let dir = tempfile::tempdir().unwrap();
    let fs = crate::file_reader::CompileFs::disk(dir.path());
    let url = asset_ref("https://ex.com/a.txt", WeftType::Primitive(WeftPrimitive::String));
    assert_eq!(resolve(&url, &fs).unwrap(), Resolved::Deferred);
    let key = asset_ref(
        "project/11111111-2222-3333-4444-555555555555/f1",
        WeftType::Primitive(WeftPrimitive::Number),
    );
    assert_eq!(resolve(&key, &fs).unwrap(), Resolved::Deferred);
}

/// A driver's text source answering from a map, by the kind of place
/// asked: `url:`, `stored:` or `path:` before what was asked.
struct MapTexts(std::collections::BTreeMap<String, Vec<u8>>, std::sync::Mutex<usize>);

impl TextSource for MapTexts {
    async fn url(&self, url: &str) -> Result<Vec<u8>, String> {
        *self.1.lock().unwrap() += 1;
        self.0.get(&format!("url:{url}")).cloned().ok_or_else(|| format!("GET {url} answered 404"))
    }
    async fn stored(&self, key: &str) -> Result<Vec<u8>, String> {
        self.0.get(&format!("stored:{key}")).cloned().ok_or_else(|| format!("no stored file {key}"))
    }
    async fn path(&self, path: &str) -> Result<Vec<u8>, String> {
        self.0.get(&format!("path:{path}")).cloned().ok_or_else(|| format!("no file {path}"))
    }
}

/// Polls a future whose every await is ready at once (a [`MapTexts`]
/// read) to its end.
fn ready<T>(future: impl std::future::Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    match future.as_mut().poll(&mut std::task::Context::from_waker(std::task::Waker::noop())) {
        std::task::Poll::Ready(value) => value,
        std::task::Poll::Pending => panic!("a map read is never pending"),
    }
}

/// Each text `@asset` is read from the place its ref names, once, and
/// cast to its type: named by its content in a program, the value itself
/// in a run. Every one that cannot be read or cast is named once, with
/// its source.
#[test]
fn text_assets_are_read_from_where_they_are_and_cast() {
    let key = "project/11111111-2222-3333-4444-555555555555/f1";
    let values = serde_json::json!({
        "n": format!("@asset(\"{key}\", Number)"),
        "u": "@asset(\"https://ex.com/a.txt\", String)",
        "u2": "@asset(\"https://ex.com/a.txt\", JsonDict)",
        "p": "@asset(\"/tmp/b.txt\", String)",
    });
    let source = MapTexts(
        [
            (format!("stored:{key}"), b"42".to_vec()),
            ("url:https://ex.com/a.txt".to_string(), br#"{"a": 1}"#.to_vec()),
            ("path:/tmp/b.txt".to_string(), b"from a path".to_vec()),
        ]
        .into(),
        Default::default(),
    );
    let program = definition_with_config(values);
    let read = ready(read_texts(&program, &source)).unwrap();
    assert_eq!(*source.1.lock().unwrap(), 1, "one URL cast two ways is fetched once");
    let dict = read.iter().find(|r| r.file_ref.ty == WeftType::parse("JsonDict").unwrap()).unwrap();
    assert_eq!(dict.value, serde_json::json!({ "a": 1 }));
    let number = read.iter().find(|r| r.file_ref.path == key).unwrap();
    assert_eq!(number.resolution(TextValues::Inline).as_f64(), Some(42.0));
    let content = weft_core::content::ContentRef::from_value(&number.resolution(TextValues::Named)).expect("named by its content");
    assert_eq!(content.sha256(), weft_core::project::hash::sha256_hex(b"42"));
    assert_eq!(number.bytes, b"42");
    assert_eq!(read.iter().find(|r| r.file_ref.path == "/tmp/b.txt").unwrap().value, serde_json::json!("from a path"));

    let broken = MapTexts([(format!("stored:{key}"), b"forty-two".to_vec())].into(), Default::default());
    let errors = ready(read_texts(&program, &broken)).unwrap_err();
    assert_eq!(errors.len(), 4, "{errors:?}");
    assert!(errors.iter().any(|e| e.starts_with(&format!("@asset(\"{key}\", Number): "))), "{errors:?}");
    assert!(errors.iter().all(|e| e.matches("@asset(").count() == 1), "each source is named once: {errors:?}");
}

#[test]
fn an_asset_names_its_type_and_one_kind_of_file() {
    let err = parse_marker("@asset(\"a.png\")").unwrap().unwrap_err();
    assert!(err.contains("@asset(\"a.png\", Image)"), "the fix is spelled out: {err}");
    let err = parse_marker("@asset(\"a.png\", File)").unwrap().unwrap_err();
    assert!(err.contains("one kind of file"), "{err}");
    let err = parse_marker("@asset(\"a.png\", Media)").unwrap().unwrap_err();
    assert!(err.contains("one kind of file"), "{err}");
    let ok = parse_marker("@asset(\"a.bin\", Blob)").unwrap().unwrap();
    assert_eq!(ok.ty, WeftType::Primitive(WeftPrimitive::Blob));
}

#[test]
fn literal_type_reads_a_marker_as_the_type_it_declares() {
    let img = serde_json::json!("@asset(\"a.png\", Image)");
    assert_eq!(literal_type(&img), WeftType::Primitive(WeftPrimitive::Image));
    let list = serde_json::json!(["@asset(\"a.png\", Image)", "@asset(\"b.png\", Image)"]);
    assert_eq!(literal_type(&list), WeftType::parse("List[Image]").unwrap());
    let text = serde_json::json!("@file(\"n.txt\", Number)");
    assert_eq!(literal_type(&text), WeftType::Primitive(WeftPrimitive::Number));
    let plain = serde_json::json!("hello");
    assert_eq!(literal_type(&plain), WeftType::Primitive(WeftPrimitive::String));
}

#[test]
fn resolve_runtime_key_refs_matches_listing_by_tenant_less_key() {
    let meta = |key: &str, filename: &str| weft_core::storage::StoredFileMeta {
        key: key.into(),
        mime_type: "image/png".into(),
        size_bytes: 9,
        filename: filename.into(),
        expires_at_unix: None,
        keep_ttl_secs: None,
        created_at_unix: 0,
        version: weft_core::storage::FIRST_FILE_VERSION,
    };
    let listing = vec![
        meta("t1/project/p1/f1", "pic.png"),
        meta("t1/project/OTHER/f1", "other.png"),
    ];
    let refs = vec![
        asset_ref("project/p1/f1", WeftType::Primitive(WeftPrimitive::Image)),
        asset_ref("project/p1/missing", WeftType::Primitive(WeftPrimitive::Image)),
    ];
    let mut map = std::collections::BTreeMap::new();
    resolve_runtime_key_refs(&refs, &listing, &mut map).unwrap();
    // The matched ref resolves to the tenant-anchored key; the unmatched one
    // stays unmapped (apply_asset_resolutions reports it loudly).
    assert_eq!(map.len(), 1);
    let image_key = refs[0].resolution_key();
    assert_eq!(map[&image_key]["__weft_image__"]["key"], "t1/project/p1/f1");
    assert_eq!(map[&image_key]["__weft_image__"]["filename"], "pic.png");

    // A stored file of another kind than the ref declared is refused
    // naming both; `Blob` takes anything.
    let wrong = vec![asset_ref("project/p1/f1", WeftType::Primitive(WeftPrimitive::Audio))];
    let mut map = std::collections::BTreeMap::new();
    let errs = resolve_runtime_key_refs(&wrong, &listing, &mut map).unwrap_err();
    assert!(errs[0].contains("is Image (image/png), not Audio"), "{errs:?}");
    let any = vec![asset_ref("project/p1/f1", WeftType::Primitive(WeftPrimitive::Blob))];
    let mut map = std::collections::BTreeMap::new();
    resolve_runtime_key_refs(&any, &listing, &mut map).unwrap();
    assert!(map[&any[0].resolution_key()].get("__weft_blob__").is_some(), "declared Blob, marked Blob");
}

#[test]
fn file_marker_rejects_a_url_source_naming_the_asset_directive() {
    // `@file("https://…")` must not fall through to a confusing disk-read
    // error: the message names the directive that CAN take a URL.
    let dir = tempfile::tempdir().unwrap();
    let fs = crate::file_reader::CompileFs::disk(dir.path());
    let fr = text_ref("https://ex.com/a.txt", WeftType::Primitive(WeftPrimitive::String));
    let err = resolve(&fr, &fs).unwrap_err();
    assert!(err.contains("@file cannot read from a URL"), "{err}");
    assert!(err.contains("use @asset(\"https://ex.com/a.txt\", String)"), "{err}");
}

#[test]
fn file_marker_keeps_disk_semantics_for_scope_tag_shaped_paths() {
    // A project may legitimately contain a directory named `project/`;
    // `@file` must read such paths from disk, never treat them as stored
    // keys (only `@asset` claims the key form).
    let dir = tempfile::tempdir().unwrap();
    let sub = dir.path().join("project/p1");
    std::fs::create_dir_all(&sub).unwrap();
    std::fs::write(sub.join("note.txt"), "hello").unwrap();
    let fs = crate::file_reader::CompileFs::disk(dir.path());
    let fr = text_ref("project/p1/note.txt", WeftType::Primitive(WeftPrimitive::String));
    assert_eq!(
        resolve(&fr, &fs).unwrap(),
        read("project/p1/note.txt", "hello", WeftType::Primitive(WeftPrimitive::String), serde_json::Value::String("hello".into()))
    );
}

/// A one-node definition whose config is `cfg`.
fn definition_with_config(cfg: serde_json::Value) -> weft_core::project::ProjectDefinition {
    let node = weft_core::project::NodeDefinition {
        config: cfg,
        ..serde_json::from_value(serde_json::json!({
            "id": "n", "nodeType": "T", "label": null,
            "position": {"x": 0, "y": 0}, "inputs": [], "outputs": []
        }))
        .unwrap()
    };
    let base: weft_core::project::ProjectDefinition = serde_json::from_value(serde_json::json!({
        "id": "00000000-0000-0000-0000-000000000001",
        "nodes": [], "edges": []
    }))
    .unwrap();
    weft_core::project::ProjectDefinition { nodes: vec![node], ..base }
}

#[test]
fn apply_asset_resolutions_substitutes_paths_and_urls() {
    // The build post-pass: path refs substitute the sync's value; URL refs
    // resolve inline (marker kind from the DECLARED type, so Image wins over
    // the .bin extension's octet-stream guess); text values are untouched.
    let mut project = definition_with_config(serde_json::json!({
        "pic": "@asset(\"assets/pic.png\", Image)",
        "ext": "@asset(\"https://ex.com/a.bin\", Image)",
        "prompt": "plain value"
    }));
    let marker = serde_json::json!({"__weft_image__": {
        "key": "t/asset/abc", "mimeType": "image/png", "sizeBytes": 4, "filename": "assets/pic.png"
    }});
    let map = std::collections::BTreeMap::from([(
        asset_ref("assets/pic.png", WeftType::Primitive(WeftPrimitive::Image)).resolution_key(),
        marker.clone(),
    )]);
    apply_asset_resolutions(&mut project, &map).unwrap();

    let cfg = project.nodes[0].config.as_object().unwrap();
    assert_eq!(cfg["pic"], marker);
    assert_eq!(cfg["ext"]["__weft_image__"]["url"], "https://ex.com/a.bin");
    assert_eq!(cfg["prompt"], "plain value");
}

#[test]
fn apply_asset_resolutions_names_every_unresolved_ref() {
    let mut project = definition_with_config(serde_json::json!({
        "a": "@asset(\"assets/gone1.png\", Image)",
        "b": "@asset(\"assets/gone2.png\", Image)"
    }));
    let errs = apply_asset_resolutions(&mut project, &std::collections::BTreeMap::new())
        .unwrap_err();
    assert_eq!(errs.len(), 2, "both unresolved refs named: {errs:?}");
    assert!(errs.iter().all(|e| e.contains("was not read by the build")));
}

#[test]
fn collect_asset_refs_finds_path_media_refs_only() {
    // Path-sourced file-typed `@asset` refs are collected (deduped); URL
    // refs and text refs are not (URLs resolve inline; text reads inline).
    let src = serde_json::json!({
        "a": "@asset(\"assets/pic.png\", Image)",
        "b": "@asset(\"assets/pic.png\", Image)",
        "c": "@asset(\"https://ex.com/x.png\", Image)",
        "d": "@file(\"prompt.txt\")",
        "e": "plain value"
    });
    let project = definition_with_config(src);
    let refs = collect_asset_refs(&project);
    assert_eq!(refs.len(), 1, "one deduped path media ref");
    assert_eq!(refs[0].path, "assets/pic.png");
}

#[test]
fn one_path_under_two_declared_types_is_two_refs() {
    // Dedupe is by path AND type: each declaration is held to its own
    // bytes check and gets its own marker kind, so flipping one of two
    // refs to the same file cannot ride the other's check.
    let src = serde_json::json!({
        "a": "@asset(\"assets/clip.mp3\", Audio)",
        "b": "@asset(\"assets/clip.mp3\", Image)",
        "c": "@asset(\"assets/clip.mp3\", Audio)"
    });
    let project = definition_with_config(src);
    let refs = collect_asset_refs(&project);
    assert_eq!(refs.len(), 2, "{refs:?}");
    let mut types: Vec<String> = refs.iter().map(|r| r.ty.to_string()).collect();
    types.sort();
    assert_eq!(types, ["Audio", "Image"]);
    assert_ne!(refs[0].resolution_key(), refs[1].resolution_key());
}

#[test]
fn collect_runtime_key_refs_finds_storage_key_media_refs_only() {
    // Storage-key media refs (a tenant-less `scope/owner/id` path, from the
    // stored-file picker) are collected separately from path asset refs, and
    // never show up in collect_asset_refs (nothing to sync from disk).
    let src = serde_json::json!({
        "a": "@asset(\"project/11111111-2222-3333-4444-555555555555/f1\", Image)",
        "b": "@asset(\"project/11111111-2222-3333-4444-555555555555/f1\", Image)",
        "c": "@asset(\"assets/pic.png\", Image)",
        "d": "@asset(\"https://ex.com/x.png\", Image)"
    });
    let project = definition_with_config(src);
    let keys = collect_runtime_key_refs(&project);
    assert_eq!(keys.len(), 1, "one deduped key ref");
    assert_eq!(keys[0].path, "project/11111111-2222-3333-4444-555555555555/f1");
    let assets = collect_asset_refs(&project);
    assert_eq!(assets.len(), 1);
    assert_eq!(assets[0].path, "assets/pic.png");
}

#[test]
fn collect_text_refs_finds_text_typed_url_and_key_refs() {
    // A text-typed `@asset` from a URL or a stored key is the build
    // driver's to fetch; a file-typed one and a disk-path one are not.
    let src = serde_json::json!({
        "a": "@asset(\"https://ex.com/sys.txt\", String)",
        "b": "@asset(\"project/11111111-2222-3333-4444-555555555555/f1\", Number)",
        "c": "@asset(\"https://ex.com/x.png\", Image)",
        "d": "@asset(\"assets/pic.png\", Image)"
    });
    let project = definition_with_config(src);
    let refs = collect_text_refs(&project);
    let paths: Vec<&str> = refs.iter().map(|r| r.path.as_str()).collect();
    assert_eq!(paths, ["https://ex.com/sys.txt", "project/11111111-2222-3333-4444-555555555555/f1"]);
    // Unfetched, it is a loud miss naming the URL.
    let mut project = definition_with_config(serde_json::json!({
        "a": "@asset(\"https://ex.com/sys.txt\", String)"
    }));
    let errs = apply_asset_resolutions(&mut project, &std::collections::BTreeMap::new()).unwrap_err();
    assert!(errs[0].contains("did not fetch"), "{errs:?}");
    // Fetched, the map's value lands in place.
    let map = std::collections::BTreeMap::from([(
        asset_ref("https://ex.com/sys.txt", WeftType::Primitive(WeftPrimitive::String)).resolution_key(),
        serde_json::json!("hello"),
    )]);
    apply_asset_resolutions(&mut project, &map).unwrap();
    assert_eq!(project.nodes[0].config["a"], "hello");
}

#[test]
fn apply_asset_resolutions_reports_a_missing_stored_file_distinctly() {
    // An unresolved key ref means the stored file is gone (deleted/expired),
    // not a sync failure: the error says so instead of "not a synced asset".
    let mut project = definition_with_config(serde_json::json!({
        "a": "@asset(\"project/11111111-2222-3333-4444-555555555555/gone\", Image)"
    }));
    let errs = apply_asset_resolutions(&mut project, &std::collections::BTreeMap::new())
        .unwrap_err();
    assert_eq!(errs.len(), 1);
    assert!(errs[0].contains("stored file"), "got: {}", errs[0]);
}

#[test]
fn resolve_missing_file_errors() {
    let dir = tempfile::tempdir().unwrap();
    let fr = text_ref("nope.txt", WeftType::Primitive(WeftPrimitive::String));
    assert!(resolve(&fr, &crate::file_reader::CompileFs::disk(dir.path())).is_err());
}

#[test]
fn resolve_rejects_path_escape() {
    let dir = tempfile::tempdir().unwrap();
    // A real file one level above the project root, reachable via `../`.
    std::fs::write(dir.path().join("secret.txt"), "leak").unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir(&root).unwrap();
    let fr = text_ref("../secret.txt", WeftType::Primitive(WeftPrimitive::String));
    let err = resolve(&fr, &crate::file_reader::CompileFs::disk(&root)).unwrap_err();
    assert!(err.contains("escapes"), "got: {err}");
}

#[test]
fn resolve_failed_cast_errors() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("n.txt"), "not a number").unwrap();
    let fr = text_ref("n.txt", WeftType::Primitive(WeftPrimitive::Number));
    assert!(resolve(&fr, &crate::file_reader::CompileFs::disk(dir.path())).is_err());
}

/// A node whose written value is a LIST of markers: an attachments port
/// taking several files. Every pass that reads or rewrites markers has to
/// see the ones inside the list, not just a marker standing alone.
mod markers_in_a_list {
    use serde_json::json;
    use weft_core::project::{NodeDefinition, ProjectDefinition};

    fn image_key(path: &str) -> weft_core::project::ResolutionKey {
        super::asset_ref(path, super::WeftType::Primitive(super::WeftPrimitive::Image)).resolution_key()
    }

    fn project_with(literal: serde_json::Value) -> ProjectDefinition {
        let mut node: NodeDefinition = serde_json::from_value(json!({
            "id": "send", "nodeType": "GmailSend", "label": null,
            "position": { "x": 0, "y": 0 }, "inputs": [], "outputs": []
        }))
        .unwrap();
        node.port_literals.insert("attachments".to_string(), literal);
        let base: ProjectDefinition = serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000001", "nodes": [], "edges": []
        }))
        .unwrap();
        ProjectDefinition { nodes: vec![node], ..base }
    }

    #[test]
    fn every_ref_in_the_list_is_collected() {
        let project = project_with(json!([
            "@asset(\"assets/a.png\", Image)",
            "@asset(\"assets/b.png\", Image)"
        ]));
        let paths: Vec<String> =
            crate::file_ref::collect_asset_refs(&project).into_iter().map(|r| r.path).collect();
        assert_eq!(paths, ["assets/a.png", "assets/b.png"]);
    }

    #[test]
    fn every_ref_in_the_list_is_resolved() {
        let mut project = project_with(json!([
            "@asset(\"assets/a.png\", Image)",
            "@asset(\"assets/b.png\", Image)"
        ]));
        let mut map = std::collections::BTreeMap::new();
        map.insert(image_key("assets/a.png"), json!({ "resolved": "a" }));
        map.insert(image_key("assets/b.png"), json!({ "resolved": "b" }));
        crate::file_ref::apply_asset_resolutions(&mut project, &map).expect("both resolve");
        assert_eq!(
            project.nodes[0].port_literals["attachments"],
            json!([{ "resolved": "a" }, { "resolved": "b" }])
        );
    }

    /// One missing file in a list is named, exactly as a lone marker is,
    /// rather than the list silently keeping its raw text.
    #[test]
    fn a_missing_file_in_the_list_is_named() {
        let mut project = project_with(json!([
            "@asset(\"assets/a.png\", Image)",
            "@asset(\"assets/gone.png\", Image)"
        ]));
        let mut map = std::collections::BTreeMap::new();
        map.insert(image_key("assets/a.png"), json!({ "resolved": "a" }));
        let errs = crate::file_ref::apply_asset_resolutions(&mut project, &map).unwrap_err();
        assert_eq!(errs.len(), 1);
        assert!(errs[0].contains("gone.png"), "got: {}", errs[0]);
    }
}

/// The entry file `source` lowered with every `@include` inlined
/// (`compile_lenient`), or every error it lowered with.
fn compile(
    source: &str,
    project_id: uuid::Uuid,
    fs: crate::file_reader::CompileFs,
) -> Result<weft_core::project::ProjectDefinition, Vec<crate::weft_compiler::SourceError>> {
    compile_with_mode(source, project_id, fs, crate::weft_compiler::IncludeMode::Full, None)
}

/// [`compile`] under the include mode the test names.
fn compile_with_mode(
    source: &str,
    project_id: uuid::Uuid,
    fs: crate::file_reader::CompileFs,
    mode: crate::weft_compiler::IncludeMode,
    source_name: Option<&str>,
) -> Result<weft_core::project::ProjectDefinition, Vec<crate::weft_compiler::SourceError>> {
    let (project, errors) = crate::weft_compiler::compile_lenient(source, project_id, fs, mode, source_name);
    if errors.is_empty() { Ok(project) } else { Err(errors) }
}

/// A `@file` inside a list resolves like one standing alone: its content
/// replaces it. Nothing may leave a raw marker in a value the runtime
/// would then receive as text.
#[test]
fn a_file_marker_inside_a_list_resolves() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.txt"), "first").unwrap();
    std::fs::write(dir.path().join("b.txt"), "second").unwrap();
    let project = compile(
        "n = LlmParams { stop: [@file(\"a.txt\"), @file(\"b.txt\")] }",
        uuid::Uuid::nil(), crate::file_reader::CompileFs::disk(dir.path()),
    ).unwrap();
    assert_eq!(project.nodes[0].config["stop"], serde_json::json!(["first", "second"]));
    assert!(
        project.nodes[0].file_refs.is_empty(),
        "a list is not one file-backed field, so nothing is editable through it"
    );
}

#[test]
fn file_markers_keep_scoped_types_after_the_source_scope_ends() {
    let reader = crate::file_reader::MapFileReader::new(std::collections::BTreeMap::from([
        (std::path::PathBuf::from("/project/a.txt"), "local text".into()),
    ]));
    let project = compile(
        r#"type TextAlias = String
n = Text { value: @file("a.txt", TextAlias) }
g = Group(input: String) {
  type LocalText = String
  remote = Text { value: @asset("https://example.test/a.txt", LocalText) }
}
g.input = @file("a.txt", TextAlias)
"#,
        uuid::Uuid::nil(),
        crate::file_reader::CompileFs::with_reader(&reader, Some(std::path::Path::new("/project"))),
    ).unwrap();
    let local = project.nodes.iter().find(|n| n.id == "n").unwrap();
    assert_eq!(local.config["value"], serde_json::json!("local text"));
    assert!(matches!(&local.file_refs["value"].ty, WeftType::Named { name, .. } if name == "TextAlias"));
    let group = project.groups.iter().find(|g| g.id == "g").unwrap();
    assert_eq!(group.port_literals["input"], serde_json::json!("local text"));
    assert!(group.file_refs.contains_key("input"));
    let remote = crate::file_ref::collect_text_refs(&project);
    assert_eq!(remote.len(), 1);
    assert!(matches!(&remote[0].ty, WeftType::Named { name, .. } if name == "LocalText"));
}

#[test]
fn include_arguments_and_body_files_resolve_from_the_project_root() {
    // A `@file` path is relative to the project root wherever it is
    // typed: the same text in the caller and in the included file names
    // the same file.
    let reader = crate::file_reader::MapFileReader::new(std::collections::BTreeMap::from([
        (std::path::PathBuf::from("/project/input.txt"), "caller".into()),
        (std::path::PathBuf::from("/project/components/input.txt"), "not this one".into()),
        (std::path::PathBuf::from("/project/components/box.weft"), "Group(input: String) {\n n = Text { value: @file(\"input.txt\") }\n}".into()),
    ]));
    for mode in [crate::weft_compiler::IncludeMode::Full, crate::weft_compiler::IncludeMode::Interface] {
        let project = compile_with_mode(
            "type CallerText = String\nbox = @include(\"components/box.weft\")\nbox.input = @file(\"input.txt\", CallerText)\n",
            uuid::Uuid::nil(), crate::file_reader::CompileFs::with_reader(&reader, Some(std::path::Path::new("/project"))),
            mode, None,
        ).unwrap();
        match mode {
            crate::weft_compiler::IncludeMode::Full => {
                let site = project.groups.iter().find(|g| g.id == "box").unwrap();
                assert_eq!(site.port_literals["input"], serde_json::json!("caller"));
                assert_eq!(site.file_refs["input"].path, "input.txt");
                let body = project.nodes.iter().find(|n| n.id == "@components:box.n").unwrap();
                assert_eq!(body.config["value"], serde_json::json!("caller"));
                assert_eq!(body.file_refs["value"].path, "input.txt");
            }
            crate::weft_compiler::IncludeMode::Interface => {
                let node = project.nodes.iter().find(|n| n.id == "box").unwrap();
                assert_eq!(node.config["input"], serde_json::json!("caller"));
                assert!(node.file_refs.contains_key("input"));
            }
        }
    }
}

#[test]
fn a_deferred_asset_in_an_included_file_keeps_its_root_spelling() {
    let reader = crate::file_reader::MapFileReader::new(std::collections::BTreeMap::from([
        (std::path::PathBuf::from("/project/components/box.weft"), "Group {\n n = Text { value: @asset(\"pics/a.png\", Image) }\n}".into()),
    ]));
    let project = compile_with_mode(
        "box = @include(\"components/box.weft\")\n",
        uuid::Uuid::nil(), crate::file_reader::CompileFs::with_reader(&reader, Some(std::path::Path::new("/project"))),
        crate::weft_compiler::IncludeMode::Full, None,
    ).unwrap();
    let body = project.nodes.iter().find(|n| n.id == "@components:box.n").unwrap();
    assert_eq!(body.config["value"], serde_json::json!("@asset(\"pics/a.png\", Image)"));
    assert_eq!(body.file_refs["value"].path, "pics/a.png");
    let refs = crate::file_ref::collect_asset_refs(&project);
    assert_eq!(refs.iter().map(|r| r.path.as_str()).collect::<Vec<_>>(), vec!["pics/a.png"]);
}

#[test]
fn an_included_ref_reads_from_the_root_and_a_climb_above_it_is_refused() {
    let reader = crate::file_reader::MapFileReader::new(std::collections::BTreeMap::from([
        (std::path::PathBuf::from("/project/shared.txt"), "shared".into()),
        (std::path::PathBuf::from("/project/components/box.weft"), "Group {\n n = Text { value: @file(\"sub/../shared.txt\") }\n}".into()),
        (std::path::PathBuf::from("/project/components/bad.weft"), "Group {\n n = Text { value: @file(\"../etc/passwd\") }\n}".into()),
    ]));
    let fs = crate::file_reader::CompileFs::with_reader(&reader, Some(std::path::Path::new("/project")));
    let project = compile_with_mode(
        "box = @include(\"components/box.weft\")\n", uuid::Uuid::nil(), fs,
        crate::weft_compiler::IncludeMode::Full, None,
    ).unwrap();
    let body = project.nodes.iter().find(|n| n.id == "@components:box.n").unwrap();
    assert_eq!(body.config["value"], serde_json::json!("shared"));
    assert_eq!(body.file_refs["value"].path, "shared.txt");
    let errs = compile_with_mode(
        "box = @include(\"components/bad.weft\")\n", uuid::Uuid::nil(), fs,
        crate::weft_compiler::IncludeMode::Full, None,
    ).unwrap_err();
    // There is nothing above the project root a relative path could
    // mean, so a climb is refused before any read.
    assert!(errs.iter().any(|e| e.message.contains("escapes the project root")), "{errs:?}");
}

/// A DEFERRED asset is never read by the compiler, so nothing refuses a
/// file outside the project: naming a file where it already sits, by an
/// absolute path, is allowed from any file, and the spelling is kept.
#[test]
fn a_deferred_asset_outside_the_root_keeps_its_absolute_spelling() {
    let reader = crate::file_reader::MapFileReader::new(std::collections::BTreeMap::from([
        (
            std::path::PathBuf::from("/project/components/box.weft"),
            "Group {\n n = Text { value: @asset(\"/shared/logo.png\", Image) }\n}".into(),
        ),
    ]));
    let project = compile_with_mode(
        "box = @include(\"components/box.weft\")\n",
        uuid::Uuid::nil(),
        crate::file_reader::CompileFs::with_reader(&reader, Some(std::path::Path::new("/project"))),
        crate::weft_compiler::IncludeMode::Full,
        None,
    )
    .unwrap();
    let body = project.nodes.iter().find(|n| n.id == "@components:box.n").unwrap();
    assert_eq!(body.file_refs["value"].path, "/shared/logo.png");
    assert_eq!(
        body.config["value"],
        serde_json::json!("@asset(\"/shared/logo.png\", Image)")
    );
}

/// `~` names the home directory, so a file kept there is written the way a
/// person writes it, and the wire carries the real path.
#[test]
fn a_tilde_path_expands_to_the_home_directory() {
    let home = std::env::var_os("HOME").map(std::path::PathBuf::from).expect("HOME is set in the test environment");
    let reader = crate::file_reader::MapFileReader::new(std::collections::BTreeMap::new());
    let project = compile_with_mode(
        "n = Text { value: @asset(\"~/pics/logo.png\", Image) }\n",
        uuid::Uuid::nil(),
        crate::file_reader::CompileFs::with_reader(&reader, Some(std::path::Path::new("/project"))),
        crate::weft_compiler::IncludeMode::Full,
        None,
    )
    .unwrap();
    let node = project.nodes.iter().find(|n| n.id == "n").unwrap();
    assert_eq!(node.file_refs["value"].path, home.join("pics/logo.png").to_str().unwrap());
}

/// A program includes a file the stdlib ships under `nodes/`: the file
/// reads its prompt through `__catalog/` too, and its own relative
/// include resolves against its own folder. Its nodes take their id
/// from their path under the root, and every ref leaves the compiler
/// spelled as the file it is (`nodes/...`), so no consumer needs the
/// prefix.
#[test]
fn a_program_includes_a_stdlib_file_through_the_catalog_prefix() {
    let reader = crate::file_reader::MapFileReader::new(std::collections::BTreeMap::from([
        (
            std::path::PathBuf::from("/project/nodes/base_catalog/computer/agent.weft"),
            "Group {\n n = Text { value: @file(\"__catalog/base_catalog/computer/agent_prompt.md\") }\n p = @include(\"perform.weft\")\n}".into(),
        ),
        (std::path::PathBuf::from("/project/nodes/base_catalog/computer/agent_prompt.md"), "Act carefully.".into()),
        (std::path::PathBuf::from("/project/nodes/base_catalog/computer/perform.weft"), "Group {\n m = Text { value: \"go\" }\n}".into()),
        (std::path::PathBuf::from("/project/nodes/my_pkg/x.weft"), "Group {\n k = Text { value: @asset(\"__catalog/my_pkg/pic.png\", Image) }\n}".into()),
    ]));
    let fs = crate::file_reader::CompileFs::with_reader(&reader, Some(std::path::Path::new("/project")))
        .anchored_at(Some(std::path::Path::new("/project/src")));
    let project = compile(
        "agent = @include(\"__catalog/base_catalog/computer/agent.weft\")\nown = @include(\"__catalog/my_pkg/x.weft\")\n",
        uuid::Uuid::nil(),
        fs,
    )
    .unwrap();
    let prompt = project.nodes.iter().find(|n| n.id == "@nodes:base_catalog:computer:agent.n").unwrap();
    assert_eq!(prompt.config["value"], serde_json::json!("Act carefully."));
    assert_eq!(prompt.file_refs["value"].path, "nodes/base_catalog/computer/agent_prompt.md");
    assert!(project.nodes.iter().any(|n| n.id == "@nodes:base_catalog:computer:perform.m"), "the nested include resolved beside its file");
    let pic = project.nodes.iter().find(|n| n.id == "@nodes:my_pkg:x.k").unwrap();
    assert_eq!(pic.file_refs["value"].path, "nodes/my_pkg/pic.png");
    assert_eq!(pic.config["value"], serde_json::json!("@asset(\"nodes/my_pkg/pic.png\", Image)"));
}

/// A `@file` through `__catalog/` that climbs out of `nodes/` is refused
/// on its spelling, before the normalization that would hide the climb.
#[test]
fn a_file_marker_through_the_catalog_prefix_may_not_leave_nodes() {
    let reader = crate::file_reader::MapFileReader::new(std::collections::BTreeMap::from([
        (std::path::PathBuf::from("/project/src/secret.txt"), "not for you".into()),
    ]));
    let errs = compile(
        "n = Text { value: @file(\"__catalog/../src/secret.txt\") }\n",
        uuid::Uuid::nil(),
        crate::file_reader::CompileFs::with_reader(&reader, Some(std::path::Path::new("/project"))),
    )
    .unwrap_err();
    assert!(
        errs.iter().any(|e| e.message
            == "@file(\"__catalog/../src/secret.txt\") escapes the project's nodes/ folder: a `__catalog/` path is read from nodes/ and has to stay inside it"),
        "{errs:?}"
    );
}

/// The same, on disk, through the reader every build uses: a program in
/// `src/` includes a package's file, which includes its sibling. A
/// broken `.weft` beside them that nobody includes is never read.
#[test]
fn a_catalog_include_compiles_from_disk() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    std::fs::create_dir_all(root.join("src")).unwrap();
    std::fs::create_dir_all(root.join("nodes/my_pkg/flows")).unwrap();
    std::fs::write(root.join("nodes/my_pkg/flows/broken.weft"), "this is { not weft").unwrap();
    std::fs::write(root.join("nodes/my_pkg/flows/outer.weft"), "Group {\n inner = @include(\"inner.weft\")\n}").unwrap();
    std::fs::write(root.join("nodes/my_pkg/flows/inner.weft"), "Group {\n m = Text { value: @file(\"__catalog/my_pkg/note.md\") }\n}").unwrap();
    std::fs::write(root.join("nodes/my_pkg/note.md"), "hello").unwrap();
    let project = compile(
        "outer = @include(\"__catalog/my_pkg/flows/outer.weft\")\n",
        uuid::Uuid::nil(),
        crate::file_reader::CompileFs::disk(&root).anchored_at(Some(&root.join("src"))),
    )
    .unwrap();
    let inner = project.nodes.iter().find(|n| n.id == "@nodes:my_pkg:flows:inner.m").unwrap();
    assert_eq!(inner.config["value"], serde_json::json!("hello"));
}

/// The values a run hands in (`--from`, `--emit`, `--group`, `--fire`)
/// resolve the way the same markers written in source do.
mod run_values {
    use super::*;
    use serde_json::json;
    use weft_core::run_spec::RunSpec;

    fn spec() -> RunSpec {
        serde_json::from_value(json!({
            "name": "voice",
            "emit": { "receive": {
                "messageType": "audio",
                "file": "@asset(\"samples/hello.ogg\", Audio)",
            } },
            "from": { "summarize": { "prompt": "@file(\"prompts/short.md\")", "notes": ["@file mentioned in prose", "x"] } },
            "group": ["triage", { "limit": "@file(\"limit.txt\", Number)" }],
            "fire": ["inbound", { "attachments": ["@asset(\"samples/a.png\", Image)"] }],
        }))
        .unwrap()
    }

    fn reader() -> crate::file_reader::MapFileReader {
        crate::file_reader::MapFileReader::new(
            [("/p/prompts/short.md", "Be brief."), ("/p/limit.txt", "3")]
                .into_iter()
                .map(|(path, text)| (std::path::PathBuf::from(path), text.to_string()))
                .collect(),
        )
    }

    #[test]
    fn every_handed_value_is_walked() {
        let spec = spec();
        let paths: Vec<String> = collect_asset_refs(&spec).into_iter().map(|r| r.path).collect();
        assert_eq!(paths, ["samples/hello.ogg", "samples/a.png"]);
        assert_eq!(spec.marked_values().len(), 6);
    }

    #[test]
    fn a_file_is_read_and_cast_and_an_asset_waits_for_the_asset_step() {
        let mut spec = spec();
        let reader = reader();
        let fs = CompileFs::with_reader(&reader, Some(std::path::Path::new("/p")));
        resolve_file_markers(&mut spec, &fs).unwrap();
        assert_eq!(spec.from["summarize"]["prompt"], json!("Be brief."));
        // Prose inside a list that never opens a marker stays prose.
        assert_eq!(spec.from["summarize"]["notes"], json!(["@file mentioned in prose", "x"]));
        assert_eq!(spec.group.as_ref().unwrap().1["limit"], json!(3.0));
        assert_eq!(spec.emit["receive"]["file"], json!("@asset(\"samples/hello.ogg\", Audio)"));

        let audio = asset_ref("samples/hello.ogg", WeftType::Primitive(WeftPrimitive::Audio));
        let image = asset_ref("samples/a.png", WeftType::Primitive(WeftPrimitive::Image));
        let map = [(audio.resolution_key(), json!({ "stored": "hello" })), (image.resolution_key(), json!({ "stored": "a" }))]
            .into_iter()
            .collect();
        apply_asset_resolutions(&mut spec, &map).unwrap();
        assert_eq!(spec.emit["receive"]["file"], json!({ "stored": "hello" }));
        assert_eq!(spec.emit["receive"]["messageType"], json!("audio"));
        assert_eq!(spec.fire.as_ref().unwrap().1, json!({ "attachments": [{ "stored": "a" }] }));
    }

    #[test]
    fn a_missing_file_or_a_bad_marker_is_named() {
        let mut spec: RunSpec = serde_json::from_value(json!({
            "name": "x",
            "emit": { "n": { "a": "@file(\"gone.md\")", "b": "@asset(\"x.ogg\")" } },
        }))
        .unwrap();
        let reader = reader();
        let fs = CompileFs::with_reader(&reader, Some(std::path::Path::new("/p")));
        let errors = resolve_file_markers(&mut spec, &fs).unwrap_err();
        assert_eq!(errors.len(), 2, "{errors:?}");
        assert!(errors.iter().any(|e| e.contains("gone.md") && e.contains("not found")), "{errors:?}");
        assert!(errors.iter().any(|e| e.contains("needs its type")), "{errors:?}");
    }

    /// Once resolved, a file value on a port that takes no file is
    /// refused by the run's own port check, naming the port's type.
    #[test]
    fn a_file_on_a_port_that_takes_none_is_refused() {
        let file = weft_core::storage::StoredFile {
            key: "t/asset/abc".into(),
            mime_type: "audio/ogg".into(),
            size_bytes: 3,
            filename: "hello.ogg".into(),
            version: weft_core::storage::FIRST_FILE_VERSION,
            link_secs: None,
        };
        let audio = weft_core::storage::typed_file_value(&file, &WeftType::Primitive(WeftPrimitive::Audio));
        let takes_file = WeftType::parse("File").unwrap();
        assert!(weft_core::run_spec::validate_supplied_value(&takes_file, &audio).is_ok());
        let number = WeftType::Primitive(WeftPrimitive::Number);
        assert!(weft_core::run_spec::validate_supplied_value(&number, &audio).is_err());
    }
}

/// Each `@file` is named exactly where it was read, however deep: in a
/// list, under a key holding `/` or `~`. One inside an `@instance_filled`
/// stays the text, and so does a value a later pass rewrote. Every value
/// named is handed back for the build's gate.
#[test]
fn a_file_is_named_at_the_place_it_was_read() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.txt"), "alpha").unwrap();
    std::fs::write(dir.path().join("b.txt"), "beta").unwrap();
    let fs = crate::file_reader::CompileFs::disk(dir.path());
    let mut program = definition_with_config(serde_json::json!({
        "list": ["@file(\"a.txt\")", "@file(\"b.txt\")"],
        "deep": { "k/~x": ["@file(\"a.txt\")"] },
        "filled": weft_core::instance::instance_filled_literal(Some(serde_json::json!("@file(\"b.txt\")"))),
        "rewritten": "@file(\"b.txt\")",
    }));
    let mut errors = Vec::new();
    let mut reads = Vec::new();
    resolve_project_file_refs(&mut program, &fs, &mut errors, Some(&mut reads));
    assert!(errors.is_empty(), "{errors:?}");
    *program.nodes[0].config.get_mut("rewritten").unwrap() = serde_json::json!("cast by a later pass");
    let named = name_contents(&mut program, reads);

    let config = &program.nodes[0].config;
    let path_at = |value: &serde_json::Value| weft_core::content::ContentRef::from_value(value).map(|c| c.path().to_string());
    assert_eq!(path_at(&config["list"][0]).as_deref(), Some("a.txt"));
    assert_eq!(path_at(&config["list"][1]).as_deref(), Some("b.txt"));
    assert_eq!(path_at(&config["deep"]["k/~x"][0]).as_deref(), Some("a.txt"));
    let filled = weft_core::instance::as_instance_filled(&config["filled"]).expect("still the instance's field");
    assert_eq!(filled.fallback, Some(&serde_json::json!("beta")), "an instance's fallback stays the text");
    assert_eq!(config["rewritten"], serde_json::json!("cast by a later pass"));
    let values: std::collections::BTreeSet<String> = named.values().map(|v| v.as_str().unwrap().to_string()).collect();
    assert_eq!(values, ["alpha", "beta"].map(String::from).into());
}
