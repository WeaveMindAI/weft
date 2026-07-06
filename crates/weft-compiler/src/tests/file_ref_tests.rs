use super::*;
use weft_core::WeftPrimitive;

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
    let fr = FileRef {
        path: "p.txt".into(),
        ty: WeftType::Primitive(WeftPrimitive::String),
    };
    assert_eq!(
        resolve(&fr, &crate::file_reader::CompileFs::disk(dir.path())).unwrap(),
        serde_json::json!("you are a helpful poet")
    );
}

#[test]
fn resolve_json_dict() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("c.json"), r#"{"model": "gpt", "temp": 0.7}"#).unwrap();
    let fr = FileRef { path: "c.json".into(), ty: WeftType::JsonDict };
    assert_eq!(
        resolve(&fr, &crate::file_reader::CompileFs::disk(dir.path())).unwrap(),
        serde_json::json!({"model": "gpt", "temp": 0.7})
    );
}

#[test]
fn resolve_missing_file_errors() {
    let dir = tempfile::tempdir().unwrap();
    let fr = FileRef {
        path: "nope.txt".into(),
        ty: WeftType::Primitive(WeftPrimitive::String),
    };
    assert!(resolve(&fr, &crate::file_reader::CompileFs::disk(dir.path())).is_err());
}

#[test]
fn resolve_rejects_path_escape() {
    let dir = tempfile::tempdir().unwrap();
    // A real file one level above the project root, reachable via `../`.
    std::fs::write(dir.path().join("secret.txt"), "leak").unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir(&root).unwrap();
    let fr = FileRef {
        path: "../secret.txt".into(),
        ty: WeftType::Primitive(WeftPrimitive::String),
    };
    let err = resolve(&fr, &crate::file_reader::CompileFs::disk(&root)).unwrap_err();
    assert!(err.contains("escapes"), "got: {err}");
}

#[test]
fn resolve_failed_cast_errors() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("n.txt"), "not a number").unwrap();
    let fr = FileRef {
        path: "n.txt".into(),
        ty: WeftType::Primitive(WeftPrimitive::Number),
    };
    assert!(resolve(&fr, &crate::file_reader::CompileFs::disk(dir.path())).is_err());
}
