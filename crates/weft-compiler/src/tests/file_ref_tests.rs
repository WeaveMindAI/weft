use super::*;
use weft_core::project::FileMarker;
use weft_core::WeftPrimitive;

/// A text-typed `@file` ref (the bidirectional form).
fn text_ref(path: &str, ty: WeftType) -> FileRef {
    FileRef { path: path.into(), ty, marker: FileMarker::File }
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

#[test]
fn marker_with_bracketed_type_containing_comma() {
    // The comma inside Dict[String, Number] must not be read as the
    // path/type separator.
    let fr = parse_marker("@file(\"d.json\", Dict[String, Number])").unwrap().unwrap();
    assert_eq!(fr.path, "d.json");
    assert_eq!(
        fr.ty,
        WeftType::dict(
            WeftType::Primitive(WeftPrimitive::String),
            WeftType::Primitive(WeftPrimitive::Number)
        )
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
        Resolved::Value(serde_json::json!("you are a helpful poet"))
    );
    // A text-typed `@asset` reads the same way (pull-only is an EDITOR
    // contract; the compile reads identically).
    let fr = asset_ref("p.txt", WeftType::Primitive(WeftPrimitive::String));
    assert_eq!(
        resolve(&fr, &crate::file_reader::CompileFs::disk(dir.path())).unwrap(),
        Resolved::Value(serde_json::json!("you are a helpful poet"))
    );
}

#[test]
fn resolve_json_dict() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("c.json"), r#"{"model": "gpt", "temp": 0.7}"#).unwrap();
    let fr = text_ref("c.json", WeftType::JsonDict);
    assert_eq!(
        resolve(&fr, &crate::file_reader::CompileFs::disk(dir.path())).unwrap(),
        Resolved::Value(serde_json::json!({"model": "gpt", "temp": 0.7}))
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
    // Nothing on disk to read at parse: the build fetches the bytes and
    // casts them (`resolve_text_bytes`); the parse keeps the marker.
    let dir = tempfile::tempdir().unwrap();
    let fs = crate::file_reader::CompileFs::disk(dir.path());
    let url = asset_ref("https://ex.com/a.txt", WeftType::Primitive(WeftPrimitive::String));
    assert_eq!(resolve(&url, &fs).unwrap(), Resolved::Deferred);
    let key = asset_ref(
        "project/11111111-2222-3333-4444-555555555555/f1",
        WeftType::Primitive(WeftPrimitive::Number),
    );
    assert_eq!(resolve(&key, &fs).unwrap(), Resolved::Deferred);
    assert_eq!(resolve_text_bytes(&key, b"42").unwrap().as_f64(), Some(42.0));
    let err = resolve_text_bytes(&key, b"forty-two").unwrap_err();
    assert!(err.contains("@asset(\"project/"), "the error names the source: {err}");
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
        keep: false,
        expires_at_unix: None,
        keep_ttl_secs: None,
        created_at_unix: 0,
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
        Resolved::Value(serde_json::Value::String("hello".into()))
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
        "key": "t/asset/p/abc", "mimeType": "image/png", "sizeBytes": 4, "filename": "assets/pic.png"
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
    assert!(errs.iter().all(|e| e.contains("not a synced asset")));
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
fn collect_remote_text_refs_finds_text_typed_url_and_key_refs() {
    // A text-typed `@asset` from a URL or a stored key is the build
    // driver's to fetch; a file-typed one and a disk-path one are not.
    let src = serde_json::json!({
        "a": "@asset(\"https://ex.com/sys.txt\", String)",
        "b": "@asset(\"project/11111111-2222-3333-4444-555555555555/f1\", Number)",
        "c": "@asset(\"https://ex.com/x.png\", Image)",
        "d": "@asset(\"assets/pic.png\", Image)"
    });
    let project = definition_with_config(src);
    let refs = collect_remote_text_refs(&project);
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

    fn image_key(path: &str) -> String {
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

/// A `@file` inside a list resolves like one standing alone: its content
/// replaces it. Nothing may leave a raw marker in a value the runtime
/// would then receive as text.
#[test]
fn a_file_marker_inside_a_list_resolves() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.txt"), "first").unwrap();
    std::fs::write(dir.path().join("b.txt"), "second").unwrap();
    let project = crate::weft_compiler::compile(
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
    let project = crate::weft_compiler::compile(
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
    let boundary = project.nodes.iter().find(|n| n.id == "g__in").unwrap();
    assert_eq!(boundary.port_literals["input"], serde_json::json!("local text"));
    assert!(boundary.file_refs.contains_key("input"));
    let remote = crate::file_ref::collect_remote_text_refs(&project);
    assert_eq!(remote.len(), 1);
    assert!(matches!(&remote[0].ty, WeftType::Named { name, .. } if name == "LocalText"));
}

#[test]
fn include_arguments_and_body_files_resolve_from_their_own_source() {
    let reader = crate::file_reader::MapFileReader::new(std::collections::BTreeMap::from([
        (std::path::PathBuf::from("/project/input.txt"), "caller".into()),
        (std::path::PathBuf::from("/project/components/input.txt"), "callee".into()),
        (std::path::PathBuf::from("/project/components/box.weft"), "Group(input: String) {\n n = Text { value: @file(\"input.txt\") }\n}".into()),
    ]));
    for mode in [crate::weft_compiler::IncludeMode::Full, crate::weft_compiler::IncludeMode::Interface] {
        let project = crate::weft_compiler::compile_with_mode(
            "type CallerText = String\nbox = @include(\"components/box.weft\")\nbox.input = @file(\"input.txt\", CallerText)\n",
            uuid::Uuid::nil(), crate::file_reader::CompileFs::with_reader(&reader, Some(std::path::Path::new("/project"))),
            mode, None,
        ).unwrap();
        match mode {
            crate::weft_compiler::IncludeMode::Full => {
                let boundary = project.nodes.iter().find(|n| n.id == "box__in").unwrap();
                assert_eq!(boundary.port_literals["input"], serde_json::json!("caller"));
                assert_eq!(boundary.file_refs["input"].path, "input.txt");
                let body = project.nodes.iter().find(|n| n.id == "box.n").unwrap();
                assert_eq!(body.config["value"], serde_json::json!("callee"));
                // The body's ref leaves the compiler spelled under the project
                // root, the anchor every consumer resolves against.
                assert_eq!(body.file_refs["value"].path, "components/input.txt");
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
fn a_deferred_asset_in_an_included_file_is_respelled_under_the_project_root() {
    let reader = crate::file_reader::MapFileReader::new(std::collections::BTreeMap::from([
        (std::path::PathBuf::from("/project/components/box.weft"), "Group {\n n = Text { value: @asset(\"pics/a.png\", Image) }\n}".into()),
    ]));
    let project = crate::weft_compiler::compile_with_mode(
        "box = @include(\"components/box.weft\")\n",
        uuid::Uuid::nil(), crate::file_reader::CompileFs::with_reader(&reader, Some(std::path::Path::new("/project"))),
        crate::weft_compiler::IncludeMode::Full, None,
    ).unwrap();
    let body = project.nodes.iter().find(|n| n.id == "box.n").unwrap();
    assert_eq!(body.config["value"], serde_json::json!("@asset(\"components/pics/a.png\", Image)"));
    assert_eq!(body.file_refs["value"].path, "components/pics/a.png");
    let refs = crate::file_ref::collect_asset_refs(&project);
    assert_eq!(refs.iter().map(|r| r.path.as_str()).collect::<Vec<_>>(), vec!["components/pics/a.png"]);
}

#[test]
fn an_included_ref_climbs_to_the_root_and_a_read_outside_it_is_still_refused() {
    let reader = crate::file_reader::MapFileReader::new(std::collections::BTreeMap::from([
        (std::path::PathBuf::from("/project/shared.txt"), "shared".into()),
        (std::path::PathBuf::from("/project/components/box.weft"), "Group {\n n = Text { value: @file(\"../shared.txt\") }\n}".into()),
        (std::path::PathBuf::from("/project/components/bad.weft"), "Group {\n n = Text { value: @file(\"../../etc/passwd\") }\n}".into()),
    ]));
    let fs = crate::file_reader::CompileFs::with_reader(&reader, Some(std::path::Path::new("/project")));
    let project = crate::weft_compiler::compile_with_mode(
        "box = @include(\"components/box.weft\")\n", uuid::Uuid::nil(), fs,
        crate::weft_compiler::IncludeMode::Full, None,
    ).unwrap();
    let body = project.nodes.iter().find(|n| n.id == "box.n").unwrap();
    assert_eq!(body.config["value"], serde_json::json!("shared"));
    assert_eq!(body.file_refs["value"].path, "shared.txt");
    let errs = crate::weft_compiler::compile_with_mode(
        "box = @include(\"components/bad.weft\")\n", uuid::Uuid::nil(), fs,
        crate::weft_compiler::IncludeMode::Full, None,
    ).unwrap_err();
    // The wall on COMPILER-READ files is the reader's, and it still
    // holds for a ref written in an included file: the anchored path
    // lands outside the root and the read is refused there.
    assert!(errs.iter().any(|e| e.message.contains("escapes the project root")), "{errs:?}");
}

/// A DEFERRED asset is never read by the compiler, so nothing refuses a
/// path outside the project: naming a file where it already sits is
/// allowed, exactly as it is from the compiled file itself. What the
/// anchoring must guarantee is that the path means the same thing
/// wherever it was typed, so it comes out absolute rather than as a
/// climb out of somebody's directory.
#[test]
fn a_deferred_asset_outside_the_root_keeps_one_absolute_spelling() {
    let reader = crate::file_reader::MapFileReader::new(std::collections::BTreeMap::from([
        (
            std::path::PathBuf::from("/project/components/box.weft"),
            "Group {\n n = Text { value: @asset(\"../../shared/logo.png\", Image) }\n}".into(),
        ),
    ]));
    let project = crate::weft_compiler::compile_with_mode(
        "box = @include(\"components/box.weft\")\n",
        uuid::Uuid::nil(),
        crate::file_reader::CompileFs::with_reader(&reader, Some(std::path::Path::new("/project"))),
        crate::weft_compiler::IncludeMode::Full,
        None,
    )
    .unwrap();
    let body = project.nodes.iter().find(|n| n.id == "box.n").unwrap();
    assert_eq!(body.file_refs["value"].path, "/shared/logo.png");
    assert_eq!(
        body.config["value"],
        serde_json::json!("@asset(\"/shared/logo.png\", Image)")
    );
}
