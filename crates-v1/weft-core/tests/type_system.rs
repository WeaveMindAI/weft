/// Comprehensive auto-generated tests for the WeftType system.
/// Covers: parsing, inference, compatibility, runtime checks, expand/gather transforms.
use serde_json::json;
use weft_core::weft_type::{WeftType, WeftPrimitive};

// =========================================================================
// Helpers
// =========================================================================

fn p(s: &str) -> WeftType { WeftType::parse(s).unwrap_or_else(|| panic!("Failed to parse type: {}", s)) }
fn infer(v: &serde_json::Value) -> WeftType { WeftType::infer(v) }
fn compat(src: &WeftType, dst: &WeftType) -> bool { WeftType::is_compatible(src, dst) }

// =========================================================================
// 1. Parse → Display round-trip
// Every type string should parse and display back to itself (canonical form).
// =========================================================================

#[test]
fn test_parse_display_roundtrip() {
    let cases = vec![
        "String", "Number", "Boolean", "Null",
        "Image", "Video", "Audio", "Document",
        "List[String]", "List[Number]", "List[Boolean]",
        "List[List[String]]", "List[List[List[Number]]]",
        "Dict[String, Number]", "Dict[String, String]",
        "Dict[String, List[String]]",
        "Dict[String, Dict[String, Number]]",
        "String | Number", "String | Number | Boolean",
        "String | Null",
        "List[String] | String",
        "List[String | Number]",
        "Dict[String, String | Number | Boolean]",
        "Dict[String, String | Number | Boolean | List[String]]",
        "List[Dict[String, String | Number | Boolean | List[String]]]",
    ];
    for case in cases {
        let parsed = p(case);
        let displayed = parsed.to_string();
        let reparsed = p(&displayed);
        assert_eq!(
            parsed, reparsed,
            "Round-trip failed for '{}': displayed as '{}', reparsed differently", case, displayed
        );
    }
}

// =========================================================================
// 2. Inference: value → WeftType
// =========================================================================

#[test]
fn test_infer_primitives() {
    assert_eq!(infer(&json!("hello")), p("String"));
    assert_eq!(infer(&json!(42)), p("Number"));
    assert_eq!(infer(&json!(3.14)), p("Number"));
    assert_eq!(infer(&json!(true)), p("Boolean"));
    assert_eq!(infer(&json!(false)), p("Boolean"));
    assert_eq!(infer(&json!(null)), p("Null"));
}

#[test]
fn test_infer_lists() {
    assert_eq!(infer(&json!(["a", "b"])), p("List[String]"));
    assert_eq!(infer(&json!([1, 2, 3])), p("List[Number]"));
    assert_eq!(infer(&json!([true, false])), p("List[Boolean]"));
    // Empty list infers as List[Empty]
    assert_eq!(infer(&json!([])), p("List[Empty]"));
}

#[test]
fn test_infer_mixed_lists() {
    // Mixed list → union element type
    let t = infer(&json!(["a", 1, "b"]));
    // Should be List[String | Number] (order may vary)
    if let WeftType::List(inner) = &t {
        if let WeftType::Union(types) = inner.as_ref() {
            assert!(types.contains(&p("String")));
            assert!(types.contains(&p("Number")));
        } else {
            panic!("Expected List[Union], got List[{}]", inner);
        }
    } else {
        panic!("Expected List[...], got {}", t);
    }
}

#[test]
fn test_infer_list_with_nulls() {
    let t = infer(&json!(["a", null, "b"]));
    // Should be List[String | Null] or List[String] depending on how we handle null in lists
    if let WeftType::List(inner) = &t {
        match inner.as_ref() {
            WeftType::Union(types) => {
                assert!(types.contains(&p("String")));
                assert!(types.contains(&p("Null")));
            }
            WeftType::Primitive(WeftPrimitive::String) => {
                // Also acceptable if we skip nulls in inference
            }
            other => panic!("Expected String or String|Null, got {}", other),
        }
    } else {
        panic!("Expected List[...], got {}", t);
    }
}

#[test]
fn test_infer_dicts() {
    let t = infer(&json!({"name": "Alice", "age": 30}));
    // Dict[String, String | Number]
    if let WeftType::Dict(k, v) = &t {
        assert_eq!(**k, p("String"));
        // value type should contain both String and Number
        match v.as_ref() {
            WeftType::Union(types) => {
                assert!(types.contains(&p("String")));
                assert!(types.contains(&p("Number")));
            }
            _ => panic!("Expected union value type, got {}", v),
        }
    } else {
        panic!("Expected Dict, got {}", t);
    }
}

#[test]
fn test_infer_nested_dicts() {
    let v = json!({
        "name": "Alice",
        "skills": ["rust", "python"],
        "years": 5,
        "active": true
    });
    let t = infer(&v);
    // Dict[String, String | List[String] | Number | Boolean]
    if let WeftType::Dict(_, val_type) = &t {
        if let WeftType::Union(types) = val_type.as_ref() {
            assert!(types.contains(&p("String")), "should contain String");
            assert!(types.contains(&p("Number")), "should contain Number");
            assert!(types.contains(&p("Boolean")), "should contain Boolean");
            // Should contain List[String]
            let has_list_string = types.iter().any(|t| matches!(t, WeftType::List(inner) if **inner == p("String")));
            assert!(has_list_string, "should contain List[String], got types: {:?}", types);
        } else {
            panic!("Expected union value type, got {}", val_type);
        }
    } else {
        panic!("Expected Dict, got {}", t);
    }
}

#[test]
fn test_infer_deeply_nested() {
    let v = json!([{"items": [1, 2]}, {"items": [3, 4]}]);
    let t = infer(&v);
    // Should be List[Dict[String, List[Number]]]
    assert_eq!(t.to_string().contains("List"), true);
    assert_eq!(t.to_string().contains("Dict"), true);
    assert_eq!(t.to_string().contains("Number"), true);
}

#[test]
fn test_infer_media_objects() {
    let img = json!({"url": "http://example.com/img.png", "mimeType": "image/png", "filename": "img.png"});
    let t = infer(&img);
    assert_eq!(t, p("Image"), "Should detect Image media object");

    let vid = json!({"url": "http://example.com/v.mp4", "mimeType": "video/mp4", "filename": "v.mp4"});
    assert_eq!(infer(&vid), p("Video"));

    let aud = json!({"url": "http://example.com/a.ogg", "mimeType": "audio/ogg", "filename": "a.ogg"});
    assert_eq!(infer(&aud), p("Audio"));

    let doc = json!({"url": "http://example.com/d.pdf", "mimeType": "application/pdf", "filename": "d.pdf"});
    assert_eq!(infer(&doc), p("Document"));
}

// =========================================================================
// 3. Compatibility: source type → target type
// Exhaustive matrix of pass/fail combinations.
// =========================================================================

#[test]
fn test_compatibility_matrix() {
    // (source_type_str, target_type_str, should_pass)
    let cases: Vec<(&str, &str, bool)> = vec![
        // Exact matches
        ("String", "String", true),
        ("Number", "Number", true),
        ("Boolean", "Boolean", true),
        ("Null", "Null", true),
        ("Image", "Image", true),

        // WeftPrimitive mismatches
        ("String", "Number", false),
        ("Number", "String", false),
        ("Boolean", "String", false),
        ("String", "Boolean", false),
        ("Image", "String", false),
        ("String", "Image", false),

        // Source into union target (narrowing)
        ("String", "String | Number", true),
        ("Number", "String | Number", true),
        ("Boolean", "String | Number", false),
        ("String", "String | Number | Boolean", true),
        ("Null", "String | Null", true),
        ("String", "String | Null", true),

        // Union source into narrower target (widening = fail)
        ("String | Number", "String", false),
        ("String | Number | Boolean", "String | Number", false),

        // Union into same union (order shouldn't matter)
        ("String | Number", "Number | String", true),
        ("Boolean | String | Number", "Number | Boolean | String", true),

        // List compatibility
        ("List[String]", "List[String]", true),
        ("List[Number]", "List[String]", false),
        ("List[String]", "List[String | Number]", true),
        ("List[String | Number]", "List[String]", false),

        // Nested lists
        ("List[List[String]]", "List[List[String]]", true),
        ("List[List[String]]", "List[List[Number]]", false),
        ("List[List[String]]", "List[List[String | Number]]", true),

        // Dict compatibility
        ("Dict[String, String]", "Dict[String, String]", true),
        ("Dict[String, String]", "Dict[String, Number]", false),
        ("Dict[String, String]", "Dict[String, String | Number]", true),
        ("Dict[String, String | Number]", "Dict[String, String]", false),

        // List vs non-list
        ("List[String]", "String", false),
        ("String", "List[String]", false),

        // Media types
        ("Image", "Image | Video", true),
        ("Image", "Image | Video | Audio | Document", true),
        ("String", "Image", false),

        // Complex nested
        ("List[Dict[String, String]]", "List[Dict[String, String | Number]]", true),
        ("List[Dict[String, String | Number]]", "List[Dict[String, String]]", false),
        ("Dict[String, List[String]]", "Dict[String, List[String | Number]]", true),

        // TypeVar (unresolved) targets always accept
        ("String", "T", true),
        ("List[Number]", "T", true),
        ("Dict[String, Boolean]", "T", true),

        // Null in unions
        ("Null", "String | Null", true),
        ("Null", "String", false),
        ("String | Null", "String | Null", true),
        ("String | Null", "String", false),
        ("String", "String | Null", true),
    ];

    for (i, (src, dst, expected)) in cases.iter().enumerate() {
        let src_type = p(src);
        let dst_type = p(dst);
        let result = compat(&src_type, &dst_type);
        assert_eq!(
            result, *expected,
            "Case {}: is_compatible({}, {}) = {}, expected {}",
            i, src, dst, result, expected
        );
    }
}

// =========================================================================
// 4. Runtime type check: infer(value) compatible with declared type
// Auto-generated matrix: (declared_type, json_value, should_pass)
// =========================================================================

#[test]
fn test_runtime_type_check_matrix() {
    let cases: Vec<(&str, serde_json::Value, bool)> = vec![
        // Primitives
        ("String", json!("hello"), true),
        ("String", json!(42), false),
        ("String", json!(true), false),
        ("String", json!([1]), false),
        ("Number", json!(42), true),
        ("Number", json!(3.14), true),
        ("Number", json!("42"), false),
        ("Boolean", json!(true), true),
        ("Boolean", json!(false), true),
        ("Boolean", json!(1), false),
        ("Boolean", json!("true"), false),

        // Lists
        ("List[String]", json!(["a", "b"]), true),
        ("List[String]", json!([1, 2]), false),
        ("List[String]", json!([]), true), // empty list infers as List[Empty], compatible with any
        ("List[Number]", json!([1, 2, 3]), true),
        ("List[Number]", json!(["a"]), false),
        ("List[String]", json!("not a list"), false),

        // Mixed list vs union element
        ("List[String | Number]", json!(["a", 1, "b"]), true),
        ("List[String]", json!(["a", 1, "b"]), false), // 1 is not String
        ("List[String | Null]", json!(["a", null, "b"]), true),
        ("List[String]", json!(["a", null, "b"]), false), // null is not String

        // Nested lists
        ("List[List[String]]", json!([["a", "b"], ["c"]]), true),
        ("List[List[String]]", json!([["a", 1]]), false),
        ("List[List[Number]]", json!([[1, 2], [3, 4]]), true),

        // Dicts
        ("Dict[String, String]", json!({"a": "b"}), true),
        ("Dict[String, String]", json!({"a": 1}), false),
        ("Dict[String, Number]", json!({"x": 42, "y": 3.14}), true),
        ("Dict[String, String | Number]", json!({"a": "b", "c": 1}), true),
        ("Dict[String, String]", json!({"a": "b", "c": 1}), false), // 1 not String
        ("Dict[String, String]", json!({}), true), // empty dict infers as Dict[String, Empty], compatible

        // Complex nested dicts
        ("Dict[String, String | Number | Boolean | List[String]]",
         json!({"name": "Alice", "age": 30, "active": true, "skills": ["rust"]}), true),
        ("Dict[String, String | Number | Boolean | List[String]]",
         json!({"name": "Alice", "nested": {"bad": true}}), false), // nested dict not in union

        // Unions
        ("String | Number", json!("hello"), true),
        ("String | Number", json!(42), true),
        ("String | Number", json!(true), false),
        ("String | Null", json!("hello"), true),
        ("String | Null", json!(null), true),
        ("String | Null", json!(42), false),

        // Media objects
        ("Image", json!({"url": "http://x.com/i.png", "mimeType": "image/png", "filename": "i.png"}), true),
        ("Image", json!({"url": "http://x.com/v.mp4", "mimeType": "video/mp4", "filename": "v.mp4"}), false),
        ("Image | Video", json!({"url": "http://x.com/v.mp4", "mimeType": "video/mp4", "filename": "v.mp4"}), true),
        ("Image", json!("not a media object"), false),
        ("Image", json!({"url": "http://x.com/i.png"}), false), // missing mimeType

        // List of dicts (the pattern that was failing in the project)
        ("List[Dict[String, String | Number | Boolean | List[String]]]",
         json!([
             {"name": "Alice", "age": 30, "skills": ["rust", "python"], "active": true},
             {"name": "Bob", "age": 25, "skills": ["go"], "active": false}
         ]), true),

        // List of dicts with nested list of dicts (shouldn't match if inner type doesn't include it)
        ("List[Dict[String, String | Number | Boolean | List[String]]]",
         json!([
             {"name": "Alice", "employees": [{"name": "Bob"}]}
         ]), false), // employees is List[Dict] which is not in the union

        // TypeVar always passes
        ("T", json!("anything"), true),
        ("T", json!(42), true),
        ("T", json!([1, 2, 3]), true),
        ("T", json!({"a": "b"}), true),
    ];

    for (i, (type_str, value, expected)) in cases.iter().enumerate() {
        let declared = p(type_str);
        let inferred = WeftType::infer(value);
        let result = compat(&inferred, &declared);
        assert_eq!(
            result, *expected,
            "Case {}: runtime check type={}, value={}, inferred={}, expected={}",
            i, type_str, value, inferred, expected
        );
    }
}

// =========================================================================
// 5. Edge cases and regression tests
// =========================================================================

#[test]
fn test_empty_containers_compatible_with_any() {
    // Empty list infers as List[Empty], compatible with any List[X]
    let inferred = infer(&json!([]));
    assert_eq!(inferred, p("List[Empty]"));
    assert!(compat(&inferred, &p("List[String]")));
    assert!(compat(&inferred, &p("List[Number]")));
    assert!(compat(&inferred, &p("List[Dict[String, String]]")));

    // Empty dict infers as Dict[String, Empty], compatible with any Dict[String, X]
    let inferred = infer(&json!({}));
    assert_eq!(inferred, p("Dict[String, Empty]"));
    assert!(compat(&inferred, &p("Dict[String, String]")));
    assert!(compat(&inferred, &p("Dict[String, Number]")));
}

#[test]
fn test_deeply_nested_type_mismatch() {
    // List[List[List[String]]] vs List[List[List[Number]]]
    let t1 = p("List[List[List[String]]]");
    let t2 = p("List[List[List[Number]]]");
    assert!(!compat(&t1, &t2));
    assert!(compat(&t1, &t1));
}

#[test]
fn test_union_order_irrelevant() {
    assert!(compat(&p("String | Number"), &p("Number | String")));
    assert!(compat(&p("Boolean | String | Number"), &p("Number | Boolean | String")));
    assert!(compat(&p("Image | Video | Audio"), &p("Audio | Image | Video")));
}

#[test]
fn test_single_element_union_equals_primitive() {
    // String | (nothing) should behave like String
    let single_union = WeftType::Union(vec![WeftType::primitive(WeftPrimitive::String)]);
    assert!(compat(&single_union, &p("String")));
    assert!(compat(&p("String"), &single_union));
}

#[test]
fn test_list_of_nulls_vs_list_of_string() {
    // [null, null, null] → List[Null]. Not compatible with List[String].
    let inferred = infer(&json!([null, null, null]));
    assert!(!compat(&inferred, &p("List[String]")));
    // But compatible with List[Null]
    assert!(compat(&inferred, &p("List[Null]")));
    // And compatible with List[String | Null]
    assert!(compat(&inferred, &p("List[String | Null]")));
}

#[test]
fn test_dict_with_null_values() {
    // {"a": "hello", "b": null} → Dict[String, String | Null]
    let inferred = infer(&json!({"a": "hello", "b": null}));
    assert!(compat(&inferred, &p("Dict[String, String | Null]")));
    assert!(!compat(&inferred, &p("Dict[String, String]"))); // null not compatible with String
}

#[test]
fn test_must_override_never_passes_runtime() {
    let mo = WeftType::MustOverride;
    // MustOverride is unresolved, so runtime check should always pass
    assert!(mo.is_unresolved());
}
