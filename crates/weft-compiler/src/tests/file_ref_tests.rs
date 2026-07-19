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
fn marker_tolerates_space_before_paren() {
    // `@file ("x")` (space before paren) is recognized, same as @include.
    let fr = parse_marker("@file (\"p.txt\")").unwrap().unwrap();
    assert_eq!(fr.path, "p.txt");
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
fn text_typed_asset_rejects_url_and_stored_file_sources() {
    // No text to read at parse from a URL or a stored file: only the
    // deferred file-typed form supports those sources.
    let dir = tempfile::tempdir().unwrap();
    let fs = crate::file_reader::CompileFs::disk(dir.path());
    let fr = asset_ref("https://ex.com/a.txt", WeftType::Primitive(WeftPrimitive::String));
    let err = resolve(&fr, &fs).unwrap_err();
    assert!(err.contains("needs a file-typed @asset"), "{err}");
    let fr = asset_ref(
        "project/11111111-2222-3333-4444-555555555555/f1",
        WeftType::Primitive(WeftPrimitive::String),
    );
    let err = resolve(&fr, &fs).unwrap_err();
    assert!(err.contains("must reference a project file"), "{err}");
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
    resolve_runtime_key_refs(&refs, &listing, &mut map);
    // The matched ref resolves to the tenant-anchored key; the unmatched one
    // stays unmapped (apply_asset_resolutions reports it loudly).
    assert_eq!(map.len(), 1);
    assert_eq!(map["project/p1/f1"]["__weft_image__"]["key"], "t1/project/p1/f1");
    assert_eq!(map["project/p1/f1"]["__weft_image__"]["filename"], "pic.png");
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
    assert!(err.contains("file-typed @asset"), "{err}");
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
    let map = std::collections::BTreeMap::from([("assets/pic.png".to_string(), marker.clone())]);
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
