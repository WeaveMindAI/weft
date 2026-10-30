use super::*;
use crate::weft_type::FileKind;

/// A stored-file value carrying the CONCRETE marker its mime implies,
/// built through the real production path (from_mime + file_marker), so
/// fixtures match exactly what a producer emits and the test exercises
/// the marker logic, not a hand-written shape.
fn file_val(mime: &str, handle_key: &str, handle: &str) -> serde_json::Value {
    let kind = FileKind::from_mime(mime);
    WeftType::file_marker(
        kind,
        serde_json::json!({ handle_key: handle, "mimeType": mime }),
    )
}

/// Convenience: a url-backed stored-file value of the given mime.
fn file_url(mime: &str) -> serde_json::Value {
    file_val(mime, "url", "https://x.com/f")
}

  #[test]
  fn parse_primitives() {
      assert_eq!(WeftType::parse("String"), Some(WeftType::Primitive(WeftPrimitive::String)));
      assert_eq!(WeftType::parse("Number"), Some(WeftType::Primitive(WeftPrimitive::Number)));
      assert_eq!(WeftType::parse("Boolean"), Some(WeftType::Primitive(WeftPrimitive::Boolean)));
  }

  #[test]
  fn parse_list() {
      assert_eq!(
          WeftType::parse("List[String]"),
          Some(WeftType::list(WeftType::primitive(WeftPrimitive::String)))
      );
  }

  #[test]
  fn parse_nested_list() {
      assert_eq!(
          WeftType::parse("List[List[Number]]"),
          Some(WeftType::list(WeftType::list(WeftType::primitive(WeftPrimitive::Number))))
      );
  }

  #[test]
  fn parse_dict() {
      assert_eq!(
          WeftType::parse("Dict[String, Number]"),
          Some(WeftType::dict(WeftType::primitive(WeftPrimitive::Number)))
      );
  }

  #[test]
  fn parse_stack_rejected() {
      // Stack is not a user-facing type : tracked by compiler internally
      assert_eq!(WeftType::parse("Stack[String]"), None);
  }

  #[test]
  fn parse_union() {
      let parsed = WeftType::parse("String | Number").unwrap();
      assert_eq!(parsed, WeftType::Union(vec![
          WeftType::Primitive(WeftPrimitive::String),
          WeftType::Primitive(WeftPrimitive::Number),
      ]));
  }

  #[test]
  fn parse_type_var() {
      assert_eq!(WeftType::parse("T"), Some(WeftType::TypeVar("T".to_string())));
      assert_eq!(WeftType::parse("T1"), Some(WeftType::TypeVar("T1".to_string())));
      assert_eq!(WeftType::parse("T42"), Some(WeftType::TypeVar("T42".to_string())));
  }

  // ── Media alias tests ────────────────────────────────────────────────

  #[test]
  fn parse_media_alias() {
      // Media = Image | Video | Audio (NOT Blob: a spreadsheet/zip is
      // not media). File is the wider "any stored file" union.
      let m = WeftType::parse("Media").unwrap();
      assert!(matches!(m, WeftType::Union(_)));
      if let WeftType::Union(types) = &m {
          assert_eq!(types.len(), 3);
      }
      let f = WeftType::parse("File").unwrap();
      if let WeftType::Union(types) = &f {
          assert_eq!(types.len(), 4); // Image | Video | Audio | Blob
      } else {
          panic!("expected File union");
      }
  }

  #[test]
  fn parse_list_of_media() {
      let t = WeftType::parse("List[Media]").unwrap();
      assert!(matches!(t, WeftType::List(_)));
      if let WeftType::List(inner) = &t {
          assert!(matches!(inner.as_ref(), WeftType::Union(_)));
      }
  }

  #[test]
  fn parse_media_union_with_string() {
      // Media | String = Image | Video | Audio | String
      let t = WeftType::parse("Media | String").unwrap();
      if let WeftType::Union(types) = &t {
          assert_eq!(types.len(), 4); // 3 media + String
      } else {
          panic!("expected union");
      }
  }

  #[test]
  fn parse_list_media_or_string() {
      let t = WeftType::parse("List[Media | String]").unwrap();
      if let WeftType::List(inner) = &t {
          if let WeftType::Union(types) = inner.as_ref() {
              assert_eq!(types.len(), 4); // Image | Video | Audio | String
          } else {
              panic!("expected union inside list");
          }
      } else {
          panic!("expected list");
      }
  }

  #[test]
  fn parse_dict_string_media() {
      let t = WeftType::parse("Dict[String, Media]").unwrap();
      if let WeftType::Dict(v) = &t {
          assert!(matches!(v.as_ref(), WeftType::Union(_)));
      } else {
          panic!("expected dict");
      }
  }

  #[test]
  fn compatibility_media_accepts_image() {
      let media = WeftType::parse("Media").unwrap();
      let image = WeftType::primitive(WeftPrimitive::Image);
      // Image fits into Media (Image | Video | Audio)
      assert!(WeftType::is_compatible(&image, &media));
      // Media does not fit into just Image (Video/Audio might arrive)
      assert!(!WeftType::is_compatible(&media, &image));
  }

  #[test]
  fn compatibility_media_accepts_all_media_types() {
      let media = WeftType::parse("Media").unwrap();
      assert!(WeftType::is_compatible(&WeftType::primitive(WeftPrimitive::Image), &media));
      assert!(WeftType::is_compatible(&WeftType::primitive(WeftPrimitive::Video), &media));
      assert!(WeftType::is_compatible(&WeftType::primitive(WeftPrimitive::Audio), &media));
      // Blob is NOT Media (it IS File though).
      assert!(!WeftType::is_compatible(&WeftType::primitive(WeftPrimitive::Blob), &media));
      assert!(WeftType::is_compatible(&WeftType::primitive(WeftPrimitive::Blob), &WeftType::file()));
      // String is NOT Media
      assert!(!WeftType::is_compatible(&WeftType::primitive(WeftPrimitive::String), &media));
  }

  #[test]
  fn compatibility_media_or_string() {
      let ms = WeftType::parse("Media | String").unwrap();
      // Image fits (via Media)
      assert!(WeftType::is_compatible(&WeftType::primitive(WeftPrimitive::Image), &ms));
      // String fits
      assert!(WeftType::is_compatible(&WeftType::primitive(WeftPrimitive::String), &ms));
      // Number does NOT fit
      assert!(!WeftType::is_compatible(&WeftType::primitive(WeftPrimitive::Number), &ms));
  }

  #[test]
  fn compatibility_list_media_vs_list_image() {
      let list_media = WeftType::parse("List[Media]").unwrap();
      let list_image = WeftType::parse("List[Image]").unwrap();
      // List[Image] → List[Media]: OK (Image fits into Media)
      assert!(WeftType::is_compatible(&list_image, &list_media));
      // List[Media] → List[Image]: fails (Video/Audio don't fit into Image)
      assert!(!WeftType::is_compatible(&list_media, &list_image));
  }

  #[test]
  fn compatibility_dict_string_media_vs_dict_string_image() {
      let dict_media = WeftType::parse("Dict[String, Media]").unwrap();
      let dict_image = WeftType::parse("Dict[String, Image]").unwrap();
      // Dict[String, Image] → Dict[String, Media]: OK
      assert!(WeftType::is_compatible(&dict_image, &dict_media));
      // Dict[String, Media] → Dict[String, Image]: fails
      assert!(!WeftType::is_compatible(&dict_media, &dict_image));
  }

  fn check(expected: &WeftType, value: &serde_json::Value) -> bool {
      WeftType::is_compatible(&WeftType::infer(value), expected)
  }

  #[test]
  fn runtime_check_media_image_matches() {
      let media = WeftType::parse("Media").unwrap();
      assert!(check(&media, &file_url("image/png")));
      assert!(check(&media, &file_url("video/mp4")));
      // A Blob (pdf) is NOT Media; it IS File.
      assert!(!check(&media, &file_url("application/pdf")));
      assert!(check(&WeftType::file(), &file_url("application/pdf")));
      assert!(!check(&media, &serde_json::json!("just a string")));
      assert!(!check(&media, &serde_json::json!(42)));
  }

  #[test]
  fn runtime_check_list_media_or_string() {
      let t = WeftType::parse("List[Media | String]").unwrap();
      assert!(check(&t, &serde_json::json!([file_url("image/png"), "hello"])));
      assert!(!check(&t, &serde_json::json!([file_url("image/png"), 42])));
  }

  #[test]
  fn runtime_check_dict_string_media() {
      let t = WeftType::parse("Dict[String, Media]").unwrap();
      assert!(check(&t, &serde_json::json!({
          "photo": file_url("image/png"),
          "clip": file_url("video/mp4")
      })));
      assert!(!check(&t, &serde_json::json!({
          "photo": "not a media object"
      })));
  }

  #[test]
  fn media_roundtrip_display() {
      let t = WeftType::parse("Media").unwrap();
      let s = t.to_string();
      // The alias renders under its NAME (the expansion leaking into
      // user-facing surfaces was a real bug); re-parsing gives the same
      // structural type back.
      assert_eq!(s, "Media");
      let reparsed = WeftType::parse(&s).unwrap();
      assert_eq!(t, reparsed);
      // The structural spelling still parses to the identical type.
      assert_eq!(WeftType::parse("Image | Video | Audio").unwrap(), t);
  }

  #[test]
  fn parse_type_var_in_list() {
      assert_eq!(
          WeftType::parse("List[T]"),
          Some(WeftType::list(WeftType::type_var("T")))
      );
  }


  #[test]
  fn parse_type_var_union() {
      let parsed = WeftType::parse("T1 | T2").unwrap();
      assert_eq!(parsed, WeftType::Union(vec![
          WeftType::TypeVar("T1".to_string()),
          WeftType::TypeVar("T2".to_string()),
      ]));
  }

  #[test]
  fn union_dedup_same_types() {
      // String | String should collapse to String
      let u = WeftType::union(vec![
          WeftType::primitive(WeftPrimitive::String),
          WeftType::primitive(WeftPrimitive::String),
      ]);
      assert_eq!(u, WeftType::primitive(WeftPrimitive::String));
  }

  #[test]
  fn bare_list_rejected() {
      assert_eq!(WeftType::parse("List"), None);
  }

  #[test]
  fn bare_dict_rejected() {
      assert_eq!(WeftType::parse("Dict"), None);
  }

  #[test]
  fn bare_any_rejected() {
      assert_eq!(WeftType::parse("Any"), None);
  }

  #[test]
  fn display_roundtrip() {
      let cases = vec![
          "String",
          "List[String]",
          "List[List[Number]]",
          "Dict[String, Number]",
          "String | Number",
          "List[String] | Dict[String, Number]",
          "T",
          "T1",
          "List[T]",
      ];
      for case in cases {
          let parsed = WeftType::parse(case).unwrap();
          let displayed = parsed.to_string();
          let reparsed = WeftType::parse(&displayed).unwrap();
          assert_eq!(parsed, reparsed, "roundtrip failed for: {}", case);
      }
  }

  #[test]
  fn compatibility_basic() {
      let s = WeftType::primitive(WeftPrimitive::String);
      let n = WeftType::primitive(WeftPrimitive::Number);
      assert!(WeftType::is_compatible(&s, &s));
      assert!(!WeftType::is_compatible(&s, &n));
  }

  #[test]
  fn compatibility_list() {
      let ls = WeftType::list(WeftType::primitive(WeftPrimitive::String));
      let ln = WeftType::list(WeftType::primitive(WeftPrimitive::Number));
      assert!(WeftType::is_compatible(&ls, &ls));
      assert!(!WeftType::is_compatible(&ls, &ln));
  }


  #[test]
  fn compatibility_union() {
      let s = WeftType::primitive(WeftPrimitive::String);
      let n = WeftType::primitive(WeftPrimitive::Number);
      let b = WeftType::primitive(WeftPrimitive::Boolean);
      let sn = WeftType::union(vec![s.clone(), n.clone()]);
      let sb = WeftType::union(vec![s.clone(), b.clone()]);
      let snb = WeftType::union(vec![s.clone(), n.clone(), b.clone()]);

      // Single into union: ok if single is in union
      assert!(WeftType::is_compatible(&s, &sn));
      // Union into single: fails (Number could arrive)
      assert!(!WeftType::is_compatible(&sn, &s));
      // Same union: ok
      assert!(WeftType::is_compatible(&sn, &sn));
      // Subset union: String|Number → String|Number|Boolean ok
      assert!(WeftType::is_compatible(&sn, &snb));
      // Superset union: String|Number|Boolean → String|Number fails (Boolean not handled)
      assert!(!WeftType::is_compatible(&snb, &sn));
      // Overlapping but not subset: String|Number → String|Boolean fails (Number not handled)
      assert!(!WeftType::is_compatible(&sn, &sb));
  }

  #[test]
  fn compatibility_type_var() {
      let s = WeftType::primitive(WeftPrimitive::String);
      let t = WeftType::type_var("T");
      // TypeVar is compatible with anything (resolved per-node at compile time)
      assert!(WeftType::is_compatible(&s, &t));
      assert!(WeftType::is_compatible(&t, &s));
  }

  #[test]
  fn compatibility_port_var() {
      // An untyped port's own variable takes its type from what is wired.
      let s = WeftType::primitive(WeftPrimitive::String);
      let v = WeftType::port_var(crate::weft_type::PortSide::Input, "x");
      assert!(WeftType::is_compatible(&s, &v));
      assert!(WeftType::is_compatible(&v, &s));
  }

  // ── List covariance and nested type tests ───────────────────────────

  #[test]
  fn compatibility_list_covariant() {
      let s = WeftType::primitive(WeftPrimitive::String);
      let n = WeftType::primitive(WeftPrimitive::Number);
      let sn = WeftType::union(vec![s.clone(), n.clone()]);

      let ls = WeftType::list(s.clone());
      let lsn = WeftType::list(sn.clone());
      let ln = WeftType::list(n.clone());

      // List[String] → List[String|Number]: String fits into String|Number → covariant OK
      assert!(WeftType::is_compatible(&ls, &lsn));
      // List[String|Number] → List[String]: Number doesn't fit into String → fails
      assert!(!WeftType::is_compatible(&lsn, &ls));
      // List[Number] → List[String|Number]: Number fits into String|Number → OK
      assert!(WeftType::is_compatible(&ln, &lsn));
      // List[String|Number] → List[Number]: String doesn't fit into Number → fails
      assert!(!WeftType::is_compatible(&lsn, &ln));
  }

  #[test]
  fn compatibility_dict_covariant_value() {
      let s = WeftType::primitive(WeftPrimitive::String);
      let n = WeftType::primitive(WeftPrimitive::Number);
      let sn = WeftType::union(vec![s.clone(), n.clone()]);

      let ds = WeftType::dict(s.clone());
      let dsn = WeftType::dict(sn.clone());

      // Dict[String, String] → Dict[String, String|Number]: OK (value narrows)
      assert!(WeftType::is_compatible(&ds, &dsn));
      // Dict[String, String|Number] → Dict[String, String]: fails (Number not handled)
      assert!(!WeftType::is_compatible(&dsn, &ds));
  }

  #[test]
  fn compatibility_nested_list_of_list() {
      let s = WeftType::primitive(WeftPrimitive::String);
      let n = WeftType::primitive(WeftPrimitive::Number);
      let sn = WeftType::union(vec![s.clone(), n.clone()]);

      let lls = WeftType::list(WeftType::list(s.clone()));
      let llsn = WeftType::list(WeftType::list(sn.clone()));

      // List[List[String]] → List[List[String|Number]]: OK (covariant all the way down)
      assert!(WeftType::is_compatible(&lls, &llsn));
      // List[List[String|Number]] → List[List[String]]: fails
      assert!(!WeftType::is_compatible(&llsn, &lls));
  }

  #[test]
  fn compatibility_list_of_dict_with_union_values() {
      let s = WeftType::primitive(WeftPrimitive::String);
      let n = WeftType::primitive(WeftPrimitive::Number);
      let b = WeftType::primitive(WeftPrimitive::Boolean);
      let sn = WeftType::union(vec![s.clone(), n.clone()]);
      let snb = WeftType::union(vec![s.clone(), n.clone(), b.clone()]);

      // List[Dict[String, String|Number]] → List[Dict[String, String|Number|Boolean]]
      let ld_sn = WeftType::list(WeftType::dict(sn.clone()));
      let ld_snb = WeftType::list(WeftType::dict(snb.clone()));

      // Narrower dict values fit into wider → OK
      assert!(WeftType::is_compatible(&ld_sn, &ld_snb));
      // Wider dict values don't fit into narrower → fails
      assert!(!WeftType::is_compatible(&ld_snb, &ld_sn));
  }

  #[test]
  fn compatibility_deeply_nested_dict_union_overlap() {
      let s = WeftType::primitive(WeftPrimitive::String);
      let n = WeftType::primitive(WeftPrimitive::Number);
      let b = WeftType::primitive(WeftPrimitive::Boolean);

      // Dict[String, Dict[String, String|Number]]
      let inner_sn = WeftType::dict(WeftType::union(vec![s.clone(), n.clone()]));
      let outer_sn = WeftType::dict(inner_sn);

      // Dict[String, Dict[String, String|Boolean]]
      let inner_sb = WeftType::dict(WeftType::union(vec![s.clone(), b.clone()]));
      let outer_sb = WeftType::dict(inner_sb);

      // Dict[String, Dict[String, String|Number]] → Dict[String, Dict[String, String|Boolean]]
      // Inner dict values: String|Number → String|Boolean. Number not in String|Boolean → fails
      assert!(!WeftType::is_compatible(&outer_sn, &outer_sb));
      // Reverse also fails (Boolean not in String|Number)
      assert!(!WeftType::is_compatible(&outer_sb, &outer_sn));
  }

  #[test]
  fn compatibility_deeply_nested_dict_union_subset() {
      let s = WeftType::primitive(WeftPrimitive::String);
      let n = WeftType::primitive(WeftPrimitive::Number);
      let b = WeftType::primitive(WeftPrimitive::Boolean);

      // Dict[String, Dict[String, String]]
      let inner_s = WeftType::dict(s.clone());
      let outer_s = WeftType::dict(inner_s);

      // Dict[String, Dict[String, String|Number|Boolean]]
      let inner_snb = WeftType::dict(WeftType::union(vec![s.clone(), n.clone(), b.clone()]));
      let outer_snb = WeftType::dict(inner_snb);

      // Dict[String, Dict[String, String]] → Dict[String, Dict[String, String|Number|Boolean]]
      // String fits into String|Number|Boolean → OK
      assert!(WeftType::is_compatible(&outer_s, &outer_snb));
      // Reverse: String|Number|Boolean doesn't fit into String → fails
      assert!(!WeftType::is_compatible(&outer_snb, &outer_s));
  }

  #[test]
  fn compatibility_list_of_list_of_dict_nested() {
      let s = WeftType::primitive(WeftPrimitive::String);
      let n = WeftType::primitive(WeftPrimitive::Number);

      // List[List[Dict[String, String]]]
      let t1 = WeftType::list(WeftType::list(WeftType::dict(s.clone())));
      // List[List[Dict[String, String|Number]]]
      let t2 = WeftType::list(WeftType::list(WeftType::dict(WeftType::union(vec![s.clone(), n.clone()]))));

      // Narrow → Wide: OK
      assert!(WeftType::is_compatible(&t1, &t2));
      // Wide → Narrow: fails
      assert!(!WeftType::is_compatible(&t2, &t1));
      // Same → Same: OK
      assert!(WeftType::is_compatible(&t1, &t1));
      assert!(WeftType::is_compatible(&t2, &t2));
  }

  #[test]
  fn runtime_check_list() {
      let ls = WeftType::list(WeftType::primitive(WeftPrimitive::String));
      assert!(check(&ls, &serde_json::json!(["a", "b"])));
      assert!(!check(&ls, &serde_json::json!([1, 2])));
      assert!(!check(&ls, &serde_json::json!("not a list")));
  }

  #[test]
  fn runtime_check_dict() {
      let d = WeftType::dict(WeftType::primitive(WeftPrimitive::Number));
      assert!(check(&d, &serde_json::json!({"a": 1, "b": 2})));
      assert!(!check(&d, &serde_json::json!({"a": "not a number"})));
  }

  // ── Runtime check: primitives ───────────────────────────────────────

  #[test]
  fn runtime_check_primitives() {
      assert!(check(&WeftType::primitive(WeftPrimitive::String), &serde_json::json!("hello")));
      assert!(!check(&WeftType::primitive(WeftPrimitive::String), &serde_json::json!(42)));
      assert!(check(&WeftType::primitive(WeftPrimitive::Number), &serde_json::json!(2.75)));
      assert!(!check(&WeftType::primitive(WeftPrimitive::Number), &serde_json::json!("nope")));
      assert!(check(&WeftType::primitive(WeftPrimitive::Boolean), &serde_json::json!(true)));
      assert!(!check(&WeftType::primitive(WeftPrimitive::Boolean), &serde_json::json!("true")));
  }

  // ── Type inference ───────────────────────────────────────────────────

  #[test]
  fn infer_primitives() {
      assert_eq!(WeftType::infer(&serde_json::json!("hello")), WeftType::primitive(WeftPrimitive::String));
      assert_eq!(WeftType::infer(&serde_json::json!(42)), WeftType::primitive(WeftPrimitive::Number));
      assert_eq!(WeftType::infer(&serde_json::json!(true)), WeftType::primitive(WeftPrimitive::Boolean));
      assert_eq!(WeftType::infer(&serde_json::json!(null)), WeftType::primitive(WeftPrimitive::Null));
  }

  #[test]
  fn infer_media() {
      // The CONCRETE marker IS the inferred type, no mime re-guessing.
      assert_eq!(WeftType::infer(&file_url("image/png")), WeftType::primitive(WeftPrimitive::Image));
      assert_eq!(WeftType::infer(&file_url("video/mp4")), WeftType::primitive(WeftPrimitive::Video));
      assert_eq!(WeftType::infer(&file_url("audio/mpeg")), WeftType::primitive(WeftPrimitive::Audio));
      // Any non image/video/audio mime is the Blob catch-all.
      assert_eq!(WeftType::infer(&file_url("application/pdf")), WeftType::primitive(WeftPrimitive::Blob));
      assert_eq!(WeftType::infer(&file_url("application/zip")), WeftType::primitive(WeftPrimitive::Blob));
  }

  #[test]
  fn infer_nested_list_dict() {
      // List[Dict[String, String | Number | Boolean | List[String]]]
      let val = serde_json::json!([{
          "department": "Engineering",
          "budget": 316739,
          "active": true,
          "employees": [{"name": "Alice", "role": "Engineer"}]
      }]);
      let inferred = WeftType::infer(&val);
      let s = inferred.to_string();
      assert!(s.contains("List["), "should be a List, got: {}", s);
      assert!(s.contains("Dict["), "should contain Dict, got: {}", s);
  }

  #[test]
  fn infer_type_error_message_is_readable() {
      // The exact case from the bug report
      let expected = WeftType::parse("List[Dict[String, String | Number | Boolean]]").unwrap();
      let val = serde_json::json!([{
          "department": "Engineering",
          "budget": 316739,
          "employees": ["Alice", "Bob"]
      }]);
      let inferred = WeftType::infer(&val);
      assert!(!WeftType::is_compatible(&inferred, &expected));
      let msg = format!("expected {}, got {}", expected, inferred);
      assert!(!msg.contains("Array"), "should not say 'Array', got: {}", msg);
      assert!(!msg.contains("Object"), "should not say 'Object', got: {}", msg);
  }

  // ── Runtime check: media types ──────────────────────────────────────

  #[test]
  fn runtime_check_image() {
      let img = WeftType::primitive(WeftPrimitive::Image);
      assert!(check(&img, &file_url("image/png")));
      // A video value carries the __weft_video__ marker, NOT __weft_image__.
      assert!(!check(&img, &file_url("video/mp4")));
      assert!(!check(&img, &serde_json::json!("just a string")));
      // A marker object with no handle (url/data/key) is not a stored file.
      assert!(!check(&img, &WeftType::file_marker(FileKind::Image, serde_json::json!({"mimeType": "image/png"}))));
  }

  #[test]
  fn runtime_check_video() {
      let vid = WeftType::primitive(WeftPrimitive::Video);
      assert!(check(&vid, &file_url("video/mp4")));
      assert!(!check(&vid, &file_url("audio/mpeg")));
  }

  #[test]
  fn runtime_check_audio() {
      let aud = WeftType::primitive(WeftPrimitive::Audio);
      assert!(check(&aud, &file_url("audio/mpeg")));
      assert!(!check(&aud, &file_url("image/png")));
  }

  #[test]
  fn runtime_check_blob() {
      // Blob is the catch-all: any mime that is not image/video/audio.
      let blob = WeftType::primitive(WeftPrimitive::Blob);
      assert!(check(&blob, &file_url("application/pdf")));
      assert!(check(&blob, &file_url("application/zip")));
      assert!(check(&blob, &file_url("text/csv")));
      // image/video/audio are NOT blobs (they carry their own markers).
      assert!(!check(&blob, &file_url("image/png")));
      // A marker with no handle is not a stored file.
      assert!(!check(&blob, &WeftType::file_marker(FileKind::Blob, serde_json::json!({"mimeType": "application/pdf"}))));
  }

  #[test]
  fn runtime_check_media_alias() {
      let media = WeftType::media(); // Image | Video | Audio
      assert!(check(&media, &file_url("image/png")));
      assert!(check(&media, &file_url("video/mp4")));
      assert!(check(&media, &file_url("audio/mpeg")));
      // A Blob (pdf) is NOT Media (it is File).
      assert!(!check(&media, &file_url("application/pdf")));
      assert!(check(&WeftType::file(), &file_url("application/pdf")));
      assert!(!check(&media, &serde_json::json!("just a string")));
      assert!(!check(&media, &serde_json::json!(42)));
  }

  #[test]
  fn runtime_check_refuses_a_marker_with_only_a_data_field() {
      // Nothing in weft reads a file through inline `data`, so a marker
      // whose only handle it is is no file value.
      let img = WeftType::primitive(WeftPrimitive::Image);
      assert!(!check(&img, &file_val("image/png", "data", "base64...")));
  }

  // ── Runtime check: unions ───────────────────────────────────────────

  #[test]
  fn runtime_check_union() {
      let sn = WeftType::union(vec![
          WeftType::primitive(WeftPrimitive::String),
          WeftType::primitive(WeftPrimitive::Number),
      ]);
      assert!(check(&sn, &serde_json::json!("hello")));
      assert!(check(&sn, &serde_json::json!(42)));
      assert!(!check(&sn, &serde_json::json!(true)));
      assert!(!check(&sn, &serde_json::json!([1, 2])));
  }

  #[test]
  fn runtime_check_union_with_media() {
      let sm = WeftType::union(vec![
          WeftType::primitive(WeftPrimitive::String),
          WeftType::primitive(WeftPrimitive::Image),
      ]);
      assert!(check(&sm, &serde_json::json!("hello")));
      assert!(check(&sm, &file_url("image/png")));
      assert!(!check(&sm, &serde_json::json!(42)));
  }

  // ── Runtime check: nested types ─────────────────────────────────────

  #[test]
  fn runtime_check_list_of_dicts() {
      let t = WeftType::list(WeftType::dict(
          WeftType::primitive(WeftPrimitive::Number),
      ));
      assert!(check(&t, &serde_json::json!([{"a": 1}, {"b": 2}])));
      assert!(!check(&t, &serde_json::json!([{"a": "string"}])));
      // Empty list infers as List[Empty], compatible with any List[X]
      assert!(check(&t, &serde_json::json!([])));
  }

  #[test]
  fn runtime_check_dict_with_union_values() {
      let t = WeftType::dict(
          WeftType::union(vec![
              WeftType::primitive(WeftPrimitive::String),
              WeftType::primitive(WeftPrimitive::Number),
          ]),
      );
      assert!(check(&t, &serde_json::json!({"name": "alice", "age": 30})));
      assert!(!check(&t, &serde_json::json!({"flag": true}))); // Boolean not in union
  }

  #[test]
  fn runtime_check_list_of_list_of_string() {
      let t = WeftType::list(WeftType::list(WeftType::primitive(WeftPrimitive::String)));
      assert!(check(&t, &serde_json::json!([["a", "b"], ["c"]])));
      assert!(!check(&t, &serde_json::json!([[1, 2]])));
      assert!(!check(&t, &serde_json::json!(["a", "b"]))); // not nested
  }

  #[test]
  fn runtime_check_typevar_always_passes() {
      let t = WeftType::type_var("T");
      assert!(check(&t, &serde_json::json!("anything")));
      assert!(check(&t, &serde_json::json!(42)));
      assert!(check(&t, &serde_json::json!(null)));
      assert!(check(&t, &serde_json::json!([1, 2, 3])));
  }

  #[test]
  fn runtime_check_port_var_always_passes() {
      let t = WeftType::port_var(crate::weft_type::PortSide::Input, "x");
      assert!(check(&t, &serde_json::json!("anything")));
      assert!(check(&t, &serde_json::json!(42)));
  }

  #[test]
  fn runtime_check_empty_list_compatible_with_any() {
      // Empty list infers as List[Empty], compatible with any List[X]
      let t = WeftType::list(WeftType::primitive(WeftPrimitive::String));
      assert!(check(&t, &serde_json::json!([])));
      let t2 = WeftType::list(WeftType::primitive(WeftPrimitive::Number));
      assert!(check(&t2, &serde_json::json!([])));
      let t3 = WeftType::list(WeftType::primitive(WeftPrimitive::Boolean));
      assert!(check(&t3, &serde_json::json!([])));
  }

  // ── Null type tests ───────────────────────────────────────────────

  #[test]
  fn runtime_check_null_type() {
      let n = WeftType::primitive(WeftPrimitive::Null);
      assert!(check(&n, &serde_json::json!(null)));
      assert!(!check(&n, &serde_json::json!("string")));
      assert!(!check(&n, &serde_json::json!(42)));
  }

  #[test]
  fn runtime_check_null_rejected_without_null_type() {
      // String does NOT accept null
      assert!(!check(&WeftType::primitive(WeftPrimitive::String), &serde_json::json!(null)));
      assert!(!check(&WeftType::primitive(WeftPrimitive::Number), &serde_json::json!(null)));
      assert!(!check(&WeftType::list(WeftType::primitive(WeftPrimitive::String)), &serde_json::json!(null)));
  }

  #[test]
  fn runtime_check_string_or_null() {
      let sn = WeftType::union(vec![
          WeftType::primitive(WeftPrimitive::String),
          WeftType::primitive(WeftPrimitive::Null),
      ]);
      assert!(check(&sn, &serde_json::json!("hello")));
      assert!(check(&sn, &serde_json::json!(null)));
      assert!(!check(&sn, &serde_json::json!(42)));
  }

  #[test]
  fn runtime_check_dict_with_nullable_values() {
      // Dict[String, String | Null] : values can be strings or null
      let t = WeftType::dict(
          WeftType::union(vec![
              WeftType::primitive(WeftPrimitive::String),
              WeftType::primitive(WeftPrimitive::Null),
          ]),
      );
      assert!(check(&t, &serde_json::json!({"a": "hello", "b": null})));
      assert!(!check(&t, &serde_json::json!({"a": "hello", "b": 42})));
  }

  #[test]
  fn runtime_check_dict_without_null_rejects_null_value() {
      // Dict[String, String] : null values NOT allowed
      let t = WeftType::dict(WeftType::primitive(WeftPrimitive::String));
      assert!(!check(&t, &serde_json::json!({"a": "hello", "b": null})));
  }

  #[test]
  fn runtime_check_list_with_nullable_elements() {
      // List[Number | Null]
      let t = WeftType::list(WeftType::union(vec![
          WeftType::primitive(WeftPrimitive::Number),
          WeftType::primitive(WeftPrimitive::Null),
      ]));
      assert!(check(&t, &serde_json::json!([1, 2, null, 3])));
      assert!(!check(&t, &serde_json::json!([1, "string", 3])));
  }

  #[test]
  fn runtime_check_list_without_null_rejects_null_element() {
      let t = WeftType::list(WeftType::primitive(WeftPrimitive::Number));
      assert!(!check(&t, &serde_json::json!([1, 2, null, 3])));
  }

  #[test]
  fn runtime_check_nested_nullable() {
      // Dict[String, Dict[String, Number | Null]]
      let t = WeftType::dict(
          WeftType::dict(
              WeftType::union(vec![
                  WeftType::primitive(WeftPrimitive::Number),
                  WeftType::primitive(WeftPrimitive::Null),
              ]),
          ),
      );
      assert!(check(&t, &serde_json::json!({"outer": {"a": 1, "b": null}})));
      assert!(!check(&t, &serde_json::json!({"outer": {"a": 1, "b": "wrong"}})));
  }

  #[test]
  fn parse_null_type() {
      assert_eq!(WeftType::parse("Null"), Some(WeftType::primitive(WeftPrimitive::Null)));
  }

  #[test]
  fn parse_string_or_null() {
      let t = WeftType::parse("String | Null").unwrap();
      assert!(matches!(t, WeftType::Union(_)));
  }

  #[test]
  fn parse_list_number_or_null() {
      let t = WeftType::parse("List[Number | Null]").unwrap();
      assert!(matches!(t, WeftType::List(_)));
  }

  #[test]
  fn compatibility_null_in_union() {
      // String | Null → String | Null: OK
      assert!(WeftType::is_compatible(
          &WeftType::parse("String | Null").unwrap(),
          &WeftType::parse("String | Null").unwrap(),
      ));
      // String → String | Null: OK (String fits)
      assert!(WeftType::is_compatible(
          &WeftType::parse("String").unwrap(),
          &WeftType::parse("String | Null").unwrap(),
      ));
      // String | Null → String: fails (Null doesn't fit into String)
      assert!(!WeftType::is_compatible(
          &WeftType::parse("String | Null").unwrap(),
          &WeftType::parse("String").unwrap(),
      ));
      // Null → String | Null: OK
      assert!(WeftType::is_compatible(
          &WeftType::parse("Null").unwrap(),
          &WeftType::parse("String | Null").unwrap(),
      ));
  }

  #[test]
  fn runtime_check_empty_dict_compatible_with_any() {
      // Empty dict infers as Dict[String, Empty], compatible with any Dict[String, X]
      let t = WeftType::dict(WeftType::primitive(WeftPrimitive::Number));
      assert!(check(&t, &serde_json::json!({})));
      let t2 = WeftType::dict(WeftType::primitive(WeftPrimitive::Boolean));
      assert!(check(&t2, &serde_json::json!({})));
  }

  #[test]
  fn runtime_check_llm_parsed_json_with_nested_arrays() {
      // Real LLM output with parseJson: Dict[String, List[String]]
      // This is the actual shape that caused a runtime type error
      let expected_type = WeftType::dict(
          WeftType::list(WeftType::primitive(WeftPrimitive::String)),
      );
      let actual_value = serde_json::json!({
          "keywords": [
              "artificial intelligence",
              "machine learning",
              "AI automation",
              "ML engineering",
              "series A",
              "series B",
              "startup"
          ],
          "employeeRanges": [
              "21,50",
              "51,100",
              "101,200"
          ]
      });
      assert!(check(&expected_type, &actual_value),
          "Dict[String, List[String]] should match LLM parsed JSON with array values");
  }

  #[test]
  fn runtime_check_llm_parsed_json_wrong_type_fails() {
      // Same shape but declared as Dict[String, String | Number | Boolean]
      // Arrays inside the dict would NOT match because List[String] is not String|Number|Boolean
      let wrong_type = WeftType::dict(
          WeftType::union(vec![
              WeftType::primitive(WeftPrimitive::String),
              WeftType::primitive(WeftPrimitive::Number),
              WeftType::primitive(WeftPrimitive::Boolean),
          ]),
      );
      let actual_value = serde_json::json!({
          "keywords": ["AI", "ML"],
          "employeeRanges": ["21,50"]
      });
      assert!(!check(&wrong_type, &actual_value),
          "Dict[String, String|Number|Boolean] should NOT match values that are arrays");
  }

  // ── Runtime check: deeply nested wrong types ────────────────────────

  #[test]
  fn runtime_check_list_of_dicts_wrong_inner_value() {
      // List[Dict[String, Number]] but one dict has a String value
      let t = WeftType::list(WeftType::dict(
          WeftType::primitive(WeftPrimitive::Number),
      ));
      assert!(!check(&t, &serde_json::json!([{"a": 1}, {"b": "wrong"}])));
  }

  #[test]
  fn runtime_check_list_of_dicts_one_element_not_dict() {
      let t = WeftType::list(WeftType::dict(
          WeftType::primitive(WeftPrimitive::Number),
      ));
      assert!(!check(&t, &serde_json::json!([{"a": 1}, "not a dict"])));
  }

  #[test]
  fn runtime_check_list_of_list_wrong_deep_element() {
      // List[List[String]] but inner list has a number
      let t = WeftType::list(WeftType::list(WeftType::primitive(WeftPrimitive::String)));
      assert!(!check(&t, &serde_json::json!([["a", "b"], ["c", 42]])));
  }

  #[test]
  fn runtime_check_list_of_list_inner_not_array() {
      let t = WeftType::list(WeftType::list(WeftType::primitive(WeftPrimitive::String)));
      assert!(!check(&t, &serde_json::json!([["a"], "not a list"])));
  }

  #[test]
  fn runtime_check_dict_of_dicts_wrong_inner_value() {
      // Dict[String, Dict[String, Number]] but inner dict has a wrong value
      let t = WeftType::dict(
          WeftType::dict(
              WeftType::primitive(WeftPrimitive::Number),
          ),
      );
      assert!(check(&t, &serde_json::json!({"outer": {"inner": 42}})));
      assert!(!check(&t, &serde_json::json!({"outer": {"inner": "wrong"}})));
      assert!(!check(&t, &serde_json::json!({"outer": "not a dict"})));
  }

  #[test]
  fn runtime_check_list_of_list_of_dict_wrong_at_deepest() {
      // List[List[Dict[String, Number]]] : wrong at the deepest level
      let t = WeftType::list(WeftType::list(WeftType::dict(
          WeftType::primitive(WeftPrimitive::Number),
      )));
      assert!(check(&t, &serde_json::json!([[{"a": 1}], [{"b": 2}]])));
      assert!(!check(&t, &serde_json::json!([[{"a": 1}], [{"b": "wrong"}]])));
      assert!(!check(&t, &serde_json::json!([[{"a": 1}], ["not a dict"]])));
      assert!(!check(&t, &serde_json::json!([[{"a": 1}], "not a list"])));
  }

  #[test]
  fn runtime_check_dict_with_union_wrong_variant() {
      // Dict[String, String | Number] : Boolean is not in the union
      let t = WeftType::dict(
          WeftType::union(vec![
              WeftType::primitive(WeftPrimitive::String),
              WeftType::primitive(WeftPrimitive::Number),
          ]),
      );
      assert!(check(&t, &serde_json::json!({"a": "ok", "b": 42})));
      assert!(!check(&t, &serde_json::json!({"a": "ok", "b": true})));
  }

  #[test]
  fn runtime_check_list_of_union_wrong_element() {
      // List[String | Number] : Boolean not in union
      let t = WeftType::list(WeftType::union(vec![
          WeftType::primitive(WeftPrimitive::String),
          WeftType::primitive(WeftPrimitive::Number),
      ]));
      assert!(check(&t, &serde_json::json!(["hello", 42, "world"])));
      assert!(!check(&t, &serde_json::json!(["hello", true])));
  }

  #[test]
  fn runtime_check_list_of_media_mixed() {
      // List[Image] : all elements must be images
      let t = WeftType::list(WeftType::primitive(WeftPrimitive::Image));
      assert!(check(&t, &serde_json::json!([file_url("image/png"), file_url("image/jpeg")])));
      // One element is a video, not an image
      assert!(!check(&t, &serde_json::json!([file_url("image/png"), file_url("video/mp4")])));
  }

  #[test]
  fn runtime_check_deeply_nested_correct() {
      // Dict[String, List[Dict[String, String | Number]]]
      let t = WeftType::dict(
          WeftType::list(WeftType::dict(
              WeftType::union(vec![
                  WeftType::primitive(WeftPrimitive::String),
                  WeftType::primitive(WeftPrimitive::Number),
              ]),
          )),
      );
      assert!(check(&t, &serde_json::json!({
          "users": [
              {"name": "Alice", "age": 30},
              {"name": "Bob", "score": 95.5}
          ]
      })));
  }

  #[test]
  fn runtime_check_deeply_nested_wrong_at_leaf() {
      // Same type but Boolean sneaks in at the deepest level
      let t = WeftType::dict(
          WeftType::list(WeftType::dict(
              WeftType::union(vec![
                  WeftType::primitive(WeftPrimitive::String),
                  WeftType::primitive(WeftPrimitive::Number),
              ]),
          )),
      );
      assert!(!check(&t, &serde_json::json!({
          "users": [
              {"name": "Alice", "active": true}
          ]
      })));
  }

  #[test]
  fn runtime_check_deeply_nested_wrong_at_middle() {
      // Dict[String, List[Dict[String, Number]]] : middle layer has a string instead of dict
      let t = WeftType::dict(
          WeftType::list(WeftType::dict(
              WeftType::primitive(WeftPrimitive::Number),
          )),
      );
      assert!(!check(&t, &serde_json::json!({
          "data": ["not a dict", {"a": 1}]
      })));
  }

  #[test]
  fn runtime_check_deeply_nested_wrong_at_outer() {
      // Dict[String, List[String]] : outer value is not a list
      let t = WeftType::dict(
          WeftType::list(WeftType::primitive(WeftPrimitive::String)),
      );
      assert!(!check(&t, &serde_json::json!({
          "items": "not a list"
      })));
  }

  // ── JsonDict tests ──────────────────────────────────────────────────

  #[test]
  fn parse_json_dict() {
      assert_eq!(WeftType::parse("JsonDict"), Some(WeftType::JsonDict));
  }

  #[test]
  fn parse_list_json_dict() {
      assert_eq!(
          WeftType::parse("List[JsonDict]"),
          Some(WeftType::list(WeftType::JsonDict))
      );
  }

  #[test]
  fn json_dict_display() {
      assert_eq!(WeftType::JsonDict.to_string(), "JsonDict");
      assert_eq!(WeftType::list(WeftType::JsonDict).to_string(), "List[JsonDict]");
  }

  #[test]
  fn json_dict_roundtrip() {
      let parsed = WeftType::parse("JsonDict").unwrap();
      assert_eq!(parsed.to_string(), "JsonDict");
  }

  #[test]
  fn json_dict_compatible_with_dict_string_v() {
      let jd = WeftType::JsonDict;
      let dict_str_num = WeftType::dict(
          WeftType::primitive(WeftPrimitive::Number),
      );
      let dict_str_bool = WeftType::dict(
          WeftType::primitive(WeftPrimitive::Boolean),
      );
      let dict_str_nested = WeftType::dict(
          WeftType::dict(
              WeftType::primitive(WeftPrimitive::Number),
          ),
      );
      // JsonDict → Dict[String, V]: compatible for any V
      assert!(WeftType::is_compatible(&jd, &dict_str_num));
      assert!(WeftType::is_compatible(&jd, &dict_str_bool));
      assert!(WeftType::is_compatible(&jd, &dict_str_nested));
      // Dict[String, V] → JsonDict: compatible for any V
      assert!(WeftType::is_compatible(&dict_str_num, &jd));
      assert!(WeftType::is_compatible(&dict_str_bool, &jd));
      assert!(WeftType::is_compatible(&dict_str_nested, &jd));
      // JsonDict → JsonDict
      assert!(WeftType::is_compatible(&jd, &jd));
  }

  #[test]
  fn json_dict_not_compatible_with_non_dict() {
      let jd = WeftType::JsonDict;
      assert!(!WeftType::is_compatible(&jd, &WeftType::primitive(WeftPrimitive::String)));
      assert!(!WeftType::is_compatible(&jd, &WeftType::primitive(WeftPrimitive::Number)));
      assert!(!WeftType::is_compatible(&jd, &WeftType::list(WeftType::primitive(WeftPrimitive::String))));
      assert!(!WeftType::is_compatible(&WeftType::primitive(WeftPrimitive::String), &jd));
  }

  /// A dict's keys are always text, so `Dict[String, V]` is the one
  /// spelling: it reads and prints as written, and any other key type
  /// fails to parse pointing at a list of key/value records.
  #[test]
  fn a_dict_keyed_by_anything_but_text_does_not_parse() {
      let t = WeftType::try_parse("Dict[String, Number]").expect("a text key parses");
      assert_eq!(t, WeftType::dict(WeftType::primitive(WeftPrimitive::Number)));
      assert_eq!(t.to_string(), "Dict[String, Number]");
      for text in ["Dict[Number, String]", "Dict[Video, String]", "Dict[T, String]"] {
          let e = WeftType::try_parse(text).expect_err(text);
          assert!(e.contains("keys are always text"), "{text}: {e}");
          assert!(e.contains("List[{key:"), "{text}: {e}");
          assert_eq!(WeftType::parse(text), None, "{text}");
      }
  }

  #[test]
  fn json_dict_in_union() {
      let union_type = WeftType::union(vec![
          WeftType::JsonDict,
          WeftType::primitive(WeftPrimitive::String),
      ]);
      let dict = WeftType::dict(
          WeftType::primitive(WeftPrimitive::Number),
      );
      // Dict[String, Number] → JsonDict | String: compatible (matches JsonDict variant)
      assert!(WeftType::is_compatible(&dict, &union_type));
      // String → JsonDict | String: compatible (matches String variant)
      assert!(WeftType::is_compatible(&WeftType::primitive(WeftPrimitive::String), &union_type));
      // Number → JsonDict | String: incompatible
      assert!(!WeftType::is_compatible(&WeftType::primitive(WeftPrimitive::Number), &union_type));
  }

  #[test]
  fn list_json_dict_compatible_with_list_dict() {
      let list_jd = WeftType::list(WeftType::JsonDict);
      let list_dict = WeftType::list(WeftType::dict(
          WeftType::primitive(WeftPrimitive::Number),
      ));
      assert!(WeftType::is_compatible(&list_jd, &list_dict));
      assert!(WeftType::is_compatible(&list_dict, &list_jd));
  }

  #[test]
  fn runtime_check_json_dict_accepts_any_dict() {
      let jd = WeftType::JsonDict;
      // Simple flat dict
      assert!(check(&jd, &serde_json::json!({"name": "test", "count": 42})));
      // Deeply nested dict
      assert!(check(&jd, &serde_json::json!({
          "user": {"name": "Alice", "scores": [1, 2, 3]},
          "active": true
      })));
      // Empty dict
      assert!(check(&jd, &serde_json::json!({})));
      // Not a dict
      assert!(!check(&jd, &serde_json::json!("string")));
      assert!(!check(&jd, &serde_json::json!(42)));
      assert!(!check(&jd, &serde_json::json!([1, 2, 3])));
  }

  // ── Inference: no typevars in inferred types ────────────────────────

  #[test]
  fn infer_empty_array_no_typevar() {
      let t = WeftType::infer(&serde_json::json!([]));
      assert_eq!(t, WeftType::list(WeftType::primitive(WeftPrimitive::Empty)));
      assert_eq!(t.to_string(), "List[Empty]");
  }

  #[test]
  fn infer_empty_object_no_typevar() {
      let t = WeftType::infer(&serde_json::json!({}));
      assert_eq!(t.to_string(), "Dict[String, Empty]");
  }

  #[test]
  fn infer_nested_empty_array_no_typevar() {
      let t = WeftType::infer(&serde_json::json!({"items": [], "name": "test"}));
      // items is List[Null], name is String → value union is List[Null] | String
      let s = t.to_string();
      assert!(!s.contains("T"), "inferred type should not contain type variables: {}", s);
  }

  #[test]
  fn infer_never_produces_typevar() {
      // Complex nested structure with empty arrays and objects
      let value = serde_json::json!({
          "users": [
              {"name": "Alice", "tags": [], "meta": {}},
              {"name": "Bob", "tags": ["admin"], "meta": {"role": "dev"}}
          ],
          "empty": []
      });
      let t = WeftType::infer(&value);
      let s = t.to_string();
      assert!(!s.contains(" T"), "inferred type should not contain type variables: {}", s);
      assert!(!s.ends_with("T"), "inferred type should not contain type variables: {}", s);
      assert!(!s.contains("T]"), "inferred type should not contain type variables: {}", s);
  }

  // ── Empty type stress tests ─────────────────────────────────────────

  #[test]
  fn empty_list_compatible_with_list_of_anything() {
      let empty = WeftType::infer(&serde_json::json!([]));
      assert!(check(&WeftType::list(WeftType::primitive(WeftPrimitive::String)), &serde_json::json!([])));
      assert!(check(&WeftType::list(WeftType::primitive(WeftPrimitive::Number)), &serde_json::json!([])));
      assert!(check(&WeftType::list(WeftType::primitive(WeftPrimitive::Boolean)), &serde_json::json!([])));
      assert!(check(&WeftType::list(WeftType::media()), &serde_json::json!([])));
      // List[List[String]] accepts []
      assert!(check(&WeftType::list(WeftType::list(WeftType::primitive(WeftPrimitive::String))), &serde_json::json!([])));
      // List[Dict[String, Number]] accepts []
      assert!(check(&WeftType::list(WeftType::dict(
          WeftType::primitive(WeftPrimitive::Number),
      )), &serde_json::json!([])));
      // List[JsonDict] accepts []
      assert!(check(&WeftType::list(WeftType::JsonDict), &serde_json::json!([])));
      // Empty is only compatible as source, not target
      assert!(!WeftType::is_compatible(&WeftType::primitive(WeftPrimitive::String), &empty));
  }

  #[test]
  fn empty_dict_compatible_with_dict_of_anything() {
      assert!(check(&WeftType::dict(WeftType::primitive(WeftPrimitive::String)), &serde_json::json!({})));
      assert!(check(&WeftType::dict(WeftType::primitive(WeftPrimitive::Number)), &serde_json::json!({})));
      assert!(check(&WeftType::dict(
          WeftType::list(WeftType::primitive(WeftPrimitive::String)),
      ), &serde_json::json!({})));
      // JsonDict accepts {}
      assert!(check(&WeftType::JsonDict, &serde_json::json!({})));
  }

  #[test]
  fn null_list_elements_not_compatible_with_typed_list() {
      // [null, null] is List[Null], NOT compatible with List[String]
      assert!(!check(&WeftType::list(WeftType::primitive(WeftPrimitive::String)), &serde_json::json!([null, null])));
      // But compatible with List[Null]
      assert!(check(&WeftType::list(WeftType::primitive(WeftPrimitive::Null)), &serde_json::json!([null, null])));
      // And compatible with List[String | Null]
      assert!(check(&WeftType::parse("List[String | Null]").unwrap(), &serde_json::json!([null, null])));
  }

  #[test]
  fn deeply_nested_empty_containers_in_real_api_response() {
      // Simulating Apollo-like response with empty arrays and nested objects
      let value = serde_json::json!({
          "people": [
              {
                  "name": "Alice",
                  "emails": [],
                  "organization": {
                      "name": "Acme",
                      "departments": [],
                      "metadata": {}
                  },
                  "phone_numbers": [
                      {"number": "123", "type": "work"}
                  ]
              }
          ],
          "pagination": {"total": 1, "page": 1}
      });
      let inferred = WeftType::infer(&value);
      let s = inferred.to_string();
      // Must not contain any type variables
      assert!(!s.contains(" T"), "no typevars in: {}", s);
      assert!(!s.ends_with("T"), "no typevars in: {}", s);
      assert!(!s.contains("T]"), "no typevars in: {}", s);
      assert!(!s.contains("T,"), "no typevars in: {}", s);
      // The inferred type should be compatible with JsonDict
      assert!(WeftType::is_compatible(&inferred, &WeftType::JsonDict));
      // And compatible with List[JsonDict] for the people array
      let list_jd = WeftType::list(WeftType::JsonDict);
      let people = WeftType::infer(&value["people"]);
      assert!(WeftType::is_compatible(&people, &list_jd));
  }

  #[test]
  fn mixed_empty_and_nonempty_arrays_in_dict() {
      // Dict where some values are empty arrays and some have elements
      let value = serde_json::json!({
          "tags": ["admin", "user"],
          "roles": [],
          "name": "test"
      });
      let inferred = WeftType::infer(&value);
      let s = inferred.to_string();
      assert!(!s.contains("T]"), "no typevars in: {}", s);
      // Should be Dict[String, List[String] | List[Empty] | String]
      // or simplified depending on unification
      assert!(WeftType::is_compatible(&inferred, &WeftType::JsonDict));
  }

  #[test]
  fn empty_does_not_leak_as_target() {
      // Empty should only work as source, never as target
      let empty = WeftType::primitive(WeftPrimitive::Empty);
      // String → Empty: no
      assert!(!WeftType::is_compatible(&WeftType::primitive(WeftPrimitive::String), &empty));
      // Number → Empty: no
      assert!(!WeftType::is_compatible(&WeftType::primitive(WeftPrimitive::Number), &empty));
      // List[String] → List[Empty]: no
      assert!(!WeftType::is_compatible(
          &WeftType::list(WeftType::primitive(WeftPrimitive::String)),
          &WeftType::list(empty.clone()),
      ));
      // But Empty → String: yes
      assert!(WeftType::is_compatible(&empty, &WeftType::primitive(WeftPrimitive::String)));
      // Empty → Empty: yes (source is Empty)
      assert!(WeftType::is_compatible(&empty, &empty));
  }

  #[test]
  fn json_dict_with_empty_containers_in_runtime_values() {
      let jd = WeftType::JsonDict;
      // Dict with empty array value
      assert!(check(&jd, &serde_json::json!({"items": [], "count": 0})));
      // Dict with nested empty dict
      assert!(check(&jd, &serde_json::json!({"meta": {}, "name": "x"})));
      // Dict with deeply nested empties
      assert!(check(&jd, &serde_json::json!({
          "level1": {
              "level2": {
                  "level3": [],
                  "empty_obj": {}
              }
          }
      })));
  }

  // ── cast_text (@file value injection) ────────────────────────────────

  fn ty(s: &str) -> WeftType {
      WeftType::parse(s).unwrap()
  }

  #[test]
  fn cast_string_is_verbatim() {
      // String never parses: the bytes become a JSON string as-is, including
      // content that looks like JSON or numbers.
      assert_eq!(ty("String").cast_text("hello world"), Ok(serde_json::json!("hello world")));
      assert_eq!(ty("String").cast_text("  spaces kept  "), Ok(serde_json::json!("  spaces kept  ")));
      assert_eq!(ty("String").cast_text("42"), Ok(serde_json::json!("42")));
      assert_eq!(ty("String").cast_text("{\"a\":1}"), Ok(serde_json::json!("{\"a\":1}")));
      assert_eq!(ty("String").cast_text("line1\nline2"), Ok(serde_json::json!("line1\nline2")));
  }

  #[test]
  fn cast_number() {
      assert_eq!(ty("Number").cast_text("42"), Ok(serde_json::json!(42.0)));
      assert_eq!(ty("Number").cast_text("  2.75  "), Ok(serde_json::json!(2.75)));
      assert_eq!(ty("Number").cast_text("-7"), Ok(serde_json::json!(-7.0)));
      assert!(ty("Number").cast_text("hello").is_err());
      assert!(ty("Number").cast_text("").is_err());
  }

  #[test]
  fn cast_boolean() {
      assert_eq!(ty("Boolean").cast_text("true"), Ok(serde_json::json!(true)));
      assert_eq!(ty("Boolean").cast_text("  false  "), Ok(serde_json::json!(false)));
      assert!(ty("Boolean").cast_text("yes").is_err());
      assert!(ty("Boolean").cast_text("True").is_err());
  }

  #[test]
  fn cast_json_dict() {
      let jd = WeftType::JsonDict;
      assert_eq!(
          jd.cast_text("{\"model\": \"gpt\", \"temp\": 0.7}"),
          Ok(serde_json::json!({"model": "gpt", "temp": 0.7}))
      );
      // A JSON array is not a dict.
      assert!(jd.cast_text("[1, 2, 3]").is_err());
      // Not valid JSON.
      assert!(jd.cast_text("not json").is_err());
  }

  #[test]
  fn cast_list() {
      assert_eq!(
          ty("List[Number]").cast_text("[1, 2, 3]"),
          Ok(serde_json::json!([1, 2, 3]))
      );
      assert_eq!(
          ty("List[String]").cast_text("[\"a\", \"b\"]"),
          Ok(serde_json::json!(["a", "b"]))
      );
      // Wrong element type surfaces as an incompatibility error.
      assert!(ty("List[Number]").cast_text("[\"a\", \"b\"]").is_err());
      // An empty list is compatible with any List[T] (Empty is bottom).
      assert!(ty("List[Number]").cast_text("[]").is_ok());
  }

  #[test]
  fn cast_dict() {
      assert_eq!(
          ty("Dict[String, Number]").cast_text("{\"a\": 1, \"b\": 2}"),
          Ok(serde_json::json!({"a": 1, "b": 2}))
      );
      assert!(ty("Dict[String, Number]").cast_text("{\"a\": \"x\"}").is_err());
  }

  #[test]
  fn cast_rejects_unresolved_types() {
      assert!(ty("T").cast_text("anything").is_err());
      assert!(WeftType::port_var(crate::weft_type::PortSide::Input, "x").cast_text("anything").is_err());
  }

  #[test]
  fn cast_rejects_file_types_loudly() {
      // A stored file is a {key|url, mimeType} reference, not inline bytes:
      // @file can't load it. Fails with a clear reason, not a confusing
      // JSON-parse error. Covers the concrete types and the unions.
      for t in ["Image", "Video", "Audio", "Blob", "Media", "File"] {
          let err = ty(t).cast_text("\u{0089}PNG binary").unwrap_err();
          assert!(err.contains("a stored file is referenced by key/URL"), "{t}: {err}");
      }
  }

  /// A stored file that went through a database comes back as a plain
  /// object holding its marker: the table lets an object shape claim a
  /// file kind, and the runtime cast passes a marker of that kind, names
  /// a marker of another kind, and names a plain object for what it is.
  #[test]
  fn cast_lets_an_object_shape_claim_a_stored_file_and_checks_the_marker() {
      use serde_json::json;
      for source in ["JsonDict", "Dict[String, String]", "{ key: String }"] {
          assert!(WeftType::cast_allowed(&ty(source), &ty("Image")).is_ok(), "{source} -> Image");
          assert!(WeftType::cast_allowed(&ty(source), &ty("File")).is_ok(), "{source} -> File");
      }
      assert!(WeftType::cast_allowed(&ty("String"), &ty("Image")).is_err(), "text never becomes a file");
      assert!(WeftType::cast_allowed(&ty("Number"), &ty("Blob")).is_err());

      let image = json!({ "__weft_image__": { "key": "project/p/1", "mimeType": "image/png", "sizeBytes": 3, "filename": "cat.png" } });
      assert_eq!(ty("Image").cast_value(&image).unwrap(), image, "a marker of the kind flows unchanged");
      assert_eq!(ty("File").cast_value(&image).unwrap(), image, "File takes any kind");
      let err = ty("Video").cast_value(&image).unwrap_err();
      assert!(err.contains("a stored Image, not Video"), "{err}");
      let err = ty("Image").cast_value(&json!({ "photo": "cat.png" })).unwrap_err();
      assert!(err.contains("__weft_image__") && err.contains("is not one"), "{err}");
  }

  #[test]
  fn zero_value_per_type() {
      use serde_json::json;
      assert_eq!(ty("Number").zero_value(), json!(0));
      assert_eq!(ty("String").zero_value(), json!(""));
      assert_eq!(ty("Boolean").zero_value(), json!(false));
      assert_eq!(ty("List[Number]").zero_value(), json!([]));
      assert_eq!(ty("Dict[String, Number]").zero_value(), json!({}));
      // Media / bus / null have no literal default: JSON null.
      assert_eq!(ty("Image").zero_value(), json!(null));
  }

  #[test]
  fn zero_value_of_nullable_union_is_the_non_null_variant() {
      use serde_json::json;
      // The `?` optional marker lives on the PORT (the `required` flag),
      // not inside the type, so a carry port of `Number?` has the plain
      // `Number` type and already zeroes to 0. A type can still be an
      // explicit nullable union; its zero value is the first non-null
      // variant, order-independent.
      let num_or_null = WeftType::union(vec![
          WeftType::Primitive(WeftPrimitive::Number),
          WeftType::Primitive(WeftPrimitive::Null),
      ]);
      assert_eq!(num_or_null.zero_value(), json!(0));
      let null_or_str = WeftType::union(vec![
          WeftType::Primitive(WeftPrimitive::Null),
          WeftType::Primitive(WeftPrimitive::String),
      ]);
      assert_eq!(null_or_str.zero_value(), json!(""));
      // A purely-nullish type zeroes to null.
      assert_eq!(WeftType::Primitive(WeftPrimitive::Null).zero_value(), json!(null));
  }

  #[test]
  fn union_aliases_round_trip_through_display_and_serde() {
      // `File`/`Media` are naming sugar over structural unions; parse ->
      // to_string must yield the NAME back (the expansion leaking into
      // source lines / re-serialized metadata was a real regression), and
      // serde rides Display, so the wire shape is the alias string too.
      for name in ["File", "Media"] {
          let ty = WeftType::parse(name).unwrap();
          assert_eq!(ty.to_string(), name);
          assert_eq!(serde_json::to_string(&ty).unwrap(), format!("\"{name}\""));
          let back: WeftType = serde_json::from_str(&format!("\"{name}\"")).unwrap();
          assert_eq!(back, ty);
      }
      // A hand-written structural spelling of an alias renders under its
      // canonical name (same type, one rendering); order is insensitive.
      let media_reordered = WeftType::union(vec![
          WeftType::Primitive(WeftPrimitive::Audio),
          WeftType::Primitive(WeftPrimitive::Image),
          WeftType::Primitive(WeftPrimitive::Video),
      ]);
      assert_eq!(media_reordered.to_string(), "Media");
      // A NON-alias union still renders structurally.
      let other = WeftType::union(vec![
          WeftType::Primitive(WeftPrimitive::Image),
          WeftType::Primitive(WeftPrimitive::Null),
      ]);
      assert_eq!(other.to_string(), "Image | Null");
  }

/// Literal lenience: `cast_value` converts a written value into the
/// declared type when the conversion is unambiguous, recursively, and
/// refuses everything else. The compile-time twin of the `@file` cast.
#[test]
fn cast_value_converts_unambiguous_shapes() {
    use serde_json::json;
    let t = |s: &str| WeftType::parse(s).unwrap();

    // Scalars both ways.
    assert_eq!(t("Number").cast_value(&json!("18")).unwrap(), json!(18.0));
    assert_eq!(t("Number").cast_value(&json!(" 2.5 ")).unwrap(), json!(2.5));
    assert_eq!(t("Boolean").cast_value(&json!("true")).unwrap(), json!(true));
    assert_eq!(t("Boolean").cast_value(&json!("True")).unwrap(), json!(true), "case-insensitive");
    assert_eq!(t("Boolean").cast_value(&json!("FALSE")).unwrap(), json!(false));
    assert_eq!(t("Boolean").cast_value(&json!("1")).unwrap(), json!(true));
    assert_eq!(t("Boolean").cast_value(&json!(1)).unwrap(), json!(true));
    assert_eq!(t("Boolean").cast_value(&json!(0)).unwrap(), json!(false));
    assert_eq!(t("Number").cast_value(&json!(true)).unwrap(), json!(1));
    assert_eq!(t("Number").cast_value(&json!(false)).unwrap(), json!(0));
    assert_eq!(t("String").cast_value(&json!(18)).unwrap(), json!("18"));
    assert_eq!(t("String").cast_value(&json!(true)).unwrap(), json!("true"));

    // An exact fit passes through untouched (an integer stays an integer).
    assert_eq!(t("Number").cast_value(&json!(18)).unwrap(), json!(18));
    assert_eq!(t("String").cast_value(&json!("x")).unwrap(), json!("x"));

    // JSON-in-a-string parses into structural types, recursively.
    assert_eq!(t("List[Number]").cast_value(&json!("[1, 2]")).unwrap(), json!([1, 2]));
    assert_eq!(t("JsonDict").cast_value(&json!("{\"a\": 1}")).unwrap(), json!({"a": 1}));
    assert_eq!(
        t("Dict[String, Number]").cast_value(&json!({"a": "3"})).unwrap(),
        json!({"a": 3.0}),
        "dict values cast recursively"
    );
    assert_eq!(
        t("List[Number]").cast_value(&json!(["1", 2])).unwrap(),
        json!([1.0, 2]),
        "list elements cast recursively"
    );

    // A union prefers the exact member, then casts only when exactly
    // one member can (order-independent; disagreement fails loudly).
    assert_eq!(t("String | Number").cast_value(&json!("18")).unwrap(), json!("18"));
    assert_eq!(t("Number | Null").cast_value(&json!("18")).unwrap(), json!(18.0));

    // Genuinely uncastable shapes refuse loudly.
    assert!(t("Number").cast_value(&json!("banana")).is_err());
    assert!(t("Boolean").cast_value(&json!("yes")).is_err());
    assert!(t("Boolean").cast_value(&json!(2)).is_err(), "only 1/0 cast to a boolean");
    assert!(t("Number").cast_value(&json!(null)).is_err());
    assert!(t("List[Number]").cast_value(&json!("not json")).is_err());
    assert!(t("JsonDict").cast_value(&json!("[1, 2]")).is_err(), "an array is not a JsonDict");
    assert!(t("Image").cast_value(&json!("cat.png")).is_err(), "file types never cast");
    assert!(t("Bus").cast_value(&json!("b")).is_err());
    assert!(t("String").cast_value(&json!({"a": 1})).is_err(), "no lossy stringify of structures");
}

/// The tricky union/nesting corners of the literal cast: mixed lists
/// over union element types, unions of containers, a JSON string
/// feeding a nested structure, and the identity-preserving fast path
/// (an element that already fits is never rewritten by a cast of its
/// siblings).
#[test]
fn cast_value_handles_union_and_nested_shapes() {
    use serde_json::json;
    let t = |s: &str| WeftType::parse(s).unwrap();

    // A list whose elements each pick their own union member: fitting
    // elements pass through IDENTICALLY, misfits cast.
    assert_eq!(
        t("List[Number | Boolean]").cast_value(&json!([1, 0, "true"])).unwrap(),
        json!([1, 0, true]),
        "1 and 0 stay numbers (they already fit); only the string casts"
    );

    // A fully-compatible mixed list is untouched (no spurious cast).
    assert_eq!(
        t("List[String | Number]").cast_value(&json!(["1", 2])).unwrap(),
        json!(["1", 2])
    );

    // A union of containers: the member that fits the parsed shape wins.
    assert_eq!(
        t("List[Number] | Dict[String, Number]")
            .cast_value(&json!("{\"a\": \"1\"}"))
            .unwrap(),
        json!({"a": 1.0})
    );

    // Deep nesting through a JSON string.
    assert_eq!(
        t("List[List[Number]]").cast_value(&json!("[[\"1\"], [2]]")).unwrap(),
        json!([[1.0], [2]])
    );

    // A value only ONE member can cast is unambiguous regardless of
    // declaration order (Boolean refuses 2, so String takes it).
    assert_eq!(t("String | Boolean").cast_value(&json!(2)).unwrap(), json!("2"));
    assert_eq!(t("Boolean | String").cast_value(&json!(2)).unwrap(), json!("2"));

    // A value SEVERAL members would cast DIFFERENTLY is ambiguous:
    // refused loudly naming both readings, never picked by member order.
    let err = t("Boolean | String").cast_value(&json!(1)).unwrap_err();
    assert!(err.contains("ambiguous"), "{err}");
    assert!(err.contains("true") && err.contains("\"1\""), "both readings named: {err}");
    assert!(t("Number | Boolean").cast_value(&json!("1")).is_err(), "1.0 vs true is ambiguous");

    // One uncastable element fails the WHOLE cast (no partial rewrite).
    assert!(t("List[Number]").cast_value(&json!([1, "banana"])).is_err());
    assert!(t("List[Number | Boolean]").cast_value(&json!([1, "maybe"])).is_err());
}

// ═══════════════════════════════════════════════════════════════════════
// Records, named types, and the type registry
// ═══════════════════════════════════════════════════════════════════════

/// A registry with the chat-ish shapes used across these tests. Built
/// through the real `TypeRegistry::build`, declared out of dependency
/// order on purpose (the fixed point must resolve forward references).
fn test_registry() -> std::sync::Arc<TypeRegistry> {
    let d = |n: &str, b: &str| (n.to_string(), b.to_string(), "test".to_string());
    std::sync::Arc::new(
        TypeRegistry::build(&[
            d("ChatHistory", "List[ChatMessage]"),
            d("ChatMessage", "{ role: String, content: String | List[Part], name?: String }"),
            d("Part", "{ type: String, text?: String, image?: Image }"),
        ])
        .unwrap(),
    )
}

fn reg_parse(s: &str) -> WeftType {
    test_registry().scoped(|| WeftType::parse(s)).unwrap_or_else(|| panic!("parse failed: {s}"))
}

#[test]
fn record_syntax_parses_and_round_trips_display() {
    let t = WeftType::parse("{ role: String, name?: String, parts: List[Number] }").unwrap();
    let WeftType::Record { fields, open: false } = &t else { panic!("not a closed record: {t}") };
    assert_eq!(fields.len(), 3);
    assert!(!fields[0].optional && fields[1].optional);
    // Display round-trips through parse.
    let rendered = t.to_string();
    assert_eq!(rendered, "{role: String, name?: String, parts: List[Number]}");
    assert_eq!(WeftType::parse(&rendered).unwrap(), t);
}

#[test]
fn record_rejects_bad_syntax() {
    for bad in ["{}", "{ role }", "{ role: Foo }", "{ role: String, role: Number }", "{ : String }"] {
        assert!(WeftType::parse(bad).is_none(), "{bad:?} must not parse");
    }
}

#[test]
fn record_equality_is_field_order_insensitive() {
    let a = WeftType::parse("{ x: String, y?: Number }").unwrap();
    let b = WeftType::parse("{ y?: Number, x: String }").unwrap();
    assert_eq!(a, b);
    assert_ne!(a, WeftType::parse("{ x: String, y: Number }").unwrap(), "optionality differs");
}

#[test]
fn bare_names_resolve_only_inside_a_registry_scope() {
    assert!(WeftType::parse("ChatHistory").is_none(), "no scope, no name");
    let t = reg_parse("ChatHistory");
    let WeftType::Named { name, body } = &t else { panic!("not named: {t}") };
    assert_eq!(name, "ChatHistory");
    assert!(matches!(body.as_ref(), WeftType::List(_)));
    // Display is the bare authored name; the wire form is self-contained
    // and parses back to the same type WITHOUT any scope.
    assert_eq!(t.to_string(), "ChatHistory");
    let wire = t.wire_string();
    assert!(wire.starts_with("ChatHistory=List["), "{wire}");
    // `==` on Named compares the NAME alone, so a round-trip must be
    // asserted through `wire_string` (the total, order-preserving
    // projection) or the body's survival goes unchecked.
    assert_eq!(WeftType::parse(&wire).unwrap().wire_string(), wire);
    // Serde uses the wire form.
    let json = serde_json::to_string(&t).unwrap();
    let back: WeftType = serde_json::from_str(&json).unwrap();
    assert_eq!(back.wire_string(), wire);
}

#[test]
fn named_union_body_wire_form_is_unambiguous() {
    // A named type whose BODY is a union parenthesizes on the wire
    // (`Kind=(A | B)`), so it round-trips as one named type instead of
    // re-parsing as the union `Kind=A | B`.
    let named_union = WeftType::Named {
        name: "Kind".into(),
        body: Box::new(WeftType::Union(vec![
            WeftType::Primitive(WeftPrimitive::String),
            WeftType::Primitive(WeftPrimitive::Number),
        ])),
    };
    let wire = named_union.wire_string();
    assert_eq!(wire, "Kind=(String | Number)");
    // Compare through wire_string: `==` on Named ignores the body.
    assert_eq!(WeftType::parse(&wire).unwrap().wire_string(), wire);

    // The un-parenthesized spelling is the OTHER type: a union whose
    // first member is named. Both directions round-trip.
    let union_with_named = WeftType::Union(vec![
        WeftType::Named {
            name: "Kind".into(),
            body: Box::new(WeftType::Primitive(WeftPrimitive::String)),
        },
        WeftType::Primitive(WeftPrimitive::Number),
    ]);
    let wire = union_with_named.wire_string();
    assert_eq!(wire, "Kind=String | Number");
    assert_eq!(WeftType::parse(&wire).unwrap().wire_string(), wire);

    // The common optional-port shape: a record-bodied named type beside
    // Null (braces protect the body, no parens needed).
    let optional = WeftType::Union(vec![
        WeftType::Named {
            name: "Profile".into(),
            body: Box::new(WeftType::record(vec![RecordField {
                name: "name".into(),
                ty: WeftType::Primitive(WeftPrimitive::String),
                optional: false,
            }])),
        },
        WeftType::Primitive(WeftPrimitive::Null),
    ]);
    assert_eq!(
        WeftType::parse(&optional.wire_string()).unwrap().wire_string(),
        optional.wire_string()
    );

    // Plain paren grouping parses (and nests).
    assert_eq!(
        WeftType::parse("(String | Number)").unwrap(),
        WeftType::parse("String | Number").unwrap()
    );
    assert_eq!(WeftType::parse("((String))").unwrap(), WeftType::Primitive(WeftPrimitive::String));
}

#[test]
fn registry_build_absorbs_identical_and_refuses_conflicts() {
    let d = |n: &str, b: &str, o: &str| (n.to_string(), b.to_string(), o.to_string());
    // Identical redeclaration (even with different field order): absorbed.
    TypeRegistry::build(&[
        d("M", "{ a: String, b?: Number }", "pkg1"),
        d("M", "{ b?: Number, a: String }", "pkg2"),
    ])
    .unwrap();
    // Different bodies: loud error naming the second origin.
    let err = TypeRegistry::build(&[
        d("M", "{ a: String }", "pkg1"),
        d("M", "{ a: Number }", "pkg2"),
    ])
    .unwrap_err();
    assert!(err.contains("declared twice") && err.contains("pkg2"), "{err}");
    // Reserved and malformed names refused.
    for bad in ["String", "List", "Media", "T", "T3", "lower", "Has Space"] {
        assert!(
            TypeRegistry::build(&[d(bad, "String", "pkg")]).is_err(),
            "{bad:?} must be refused"
        );
    }
    // `MustOverride` is no longer reserved: an ordinary declarable name.
    TypeRegistry::build(&[d("MustOverride", "String", "pkg")]).unwrap();
    // A cycle fails loud instead of spinning.
    let err = TypeRegistry::build(&[d("A", "List[B]", "p"), d("B", "List[A]", "p")]).unwrap_err();
    assert!(err.contains("could not be resolved"), "{err}");
    assert!(err.contains("`A` = `List[B]` (p): it names `B`, which cannot resolve either"), "{err}");
    // A chain ending on a name nobody declares names that name on its
    // own line.
    let err = TypeRegistry::build(&[d("A", "List[B]", "p"), d("B", "List[Nope]", "p")]).unwrap_err();
    assert!(err.contains("`B` = `List[Nope]` (p): `Nope` is not a type"), "{err}");
}

#[test]
fn a_group_paren_is_stripped_only_when_it_wraps_the_whole_type() {
    assert_eq!(WeftType::try_parse("(String | Number)").unwrap(), WeftType::parse("String | Number").unwrap());
    let e = WeftType::try_parse("(String)(Number)").unwrap_err();
    assert!(e.contains("`(String)(Number)`"), "the outer pair must not be stripped: {e}");
    assert!(WeftType::try_parse("(String]").is_err(), "a closer that does not match is no group");
}

#[test]
fn wire_form_refuses_reserved_names() {
    // A `Name=Body` wire string minting a nominal type that shadows a
    // reserved name (a union alias, a primitive) would round-trip into
    // a DIFFERENT type (`Media=Number` displays as `Media`, which
    // reparses as the builtin alias). The parse refuses it outright.
    for bad in ["Media=Number", "File=String", "String=Number", "List=String"] {
        assert!(WeftType::parse(bad).is_none(), "{bad:?} must not parse");
    }
}

#[test]
fn union_with_unresolved_leaf_keeps_the_permissive_gate() {
    use serde_json::json;
    // `A={x: Number} | T` carries a declared shape AND an unresolved
    // leaf. The declared-shape validator refuses unresolved leaves, so
    // routing there would flip the union from accept-anything to
    // accept-only-A; the gate must stay structural (like bare `T`).
    let mixed = WeftType::Union(vec![
        WeftType::Named {
            name: "A".into(),
            body: Box::new(WeftType::parse("{ x: Number }").unwrap()),
        },
        WeftType::TypeVar("T".into()),
    ]);
    assert!(mixed.accepts_runtime_value(&json!({ "x": 1 })));
    assert!(mixed.accepts_runtime_value(&json!(5)));
    // Fully resolved, the declared gate takes over and refuses misfits.
    let resolved = WeftType::Union(vec![
        WeftType::Named {
            name: "A".into(),
            body: Box::new(WeftType::parse("{ x: Number }").unwrap()),
        },
        WeftType::Primitive(WeftPrimitive::String),
    ]);
    assert!(resolved.accepts_runtime_value(&json!({ "x": 1 })));
    assert!(resolved.accepts_runtime_value(&json!("s")));
    assert!(!resolved.accepts_runtime_value(&json!(5)));
}

/// The declared type is the question: a dict port takes an object
/// whatever its keys, a file marker included (a Python node handing a
/// stored file back as plain data on a `JsonDict` port used to be
/// refused because the value's shape was guessed first). Only a port
/// declared as a file reads the marker, and it still refuses a plain
/// object.
#[test]
fn a_declared_dict_accepts_an_object_that_happens_to_carry_a_file_marker() {
    use serde_json::json;
    let marker = json!({ "__weft_image__": { "key": "project/p/1", "mimeType": "image/png", "sizeBytes": 3, "filename": "cat.png" } });
    assert!(WeftType::JsonDict.accepts_runtime_value(&marker));
    assert!(WeftType::parse("Dict[String, JsonDict]").unwrap().accepts_runtime_value(&marker));
    assert!(WeftType::parse("Image").unwrap().accepts_runtime_value(&marker));
    assert!(!WeftType::parse("Image").unwrap().accepts_runtime_value(&json!({ "url": "x" })));
    assert!(!WeftType::parse("Dict[String, Number]").unwrap().accepts_runtime_value(&marker));
}

#[test]
fn named_compatibility_is_by_name_and_decays_structurally() {
    let reg = test_registry();
    reg.scoped(|| {
        let history = WeftType::parse("ChatHistory").unwrap();
        let message = WeftType::parse("ChatMessage").unwrap();
        let compat = WeftType::is_compatible;

        // Same name flows; different names never do, even shape-alike.
        assert!(compat(&history, &history));
        assert!(!compat(&message, &history));
        // Forgetting the name is safe: Named -> JsonDict / structural body.
        assert!(compat(&message, &WeftType::JsonDict));
        assert!(compat(&history, &WeftType::parse("List[ChatMessage]").unwrap()));
        // The reverse is the refused soup door.
        assert!(!compat(&WeftType::JsonDict, &message));
        assert!(!compat(&WeftType::parse("List[ChatMessage]").unwrap(), &history));
        // Through a union target.
        assert!(compat(&history, &WeftType::parse("ChatHistory | Null").unwrap()));
    });
}

#[test]
fn record_compatibility_follows_the_target_contract() {
    let t = |s: &str| WeftType::parse(s).unwrap();
    let compat = WeftType::is_compatible;
    // Optional target field may be missing from the source.
    assert!(compat(&t("{ a: String }"), &t("{ a: String, b?: Number }")));
    // Extra source field = undeclared key at runtime: refused.
    assert!(!compat(&t("{ a: String, x: Number }"), &t("{ a: String }")));
    // Optional source into required target: the key may be absent, refused.
    assert!(!compat(&t("{ a?: String }"), &t("{ a: String }")));
    // Record forgets into JsonDict / Dict[String, V]; never the reverse.
    assert!(compat(&t("{ a: String }"), &WeftType::JsonDict));
    assert!(compat(&t("{ a: String, b: String }"), &t("Dict[String, String]")));
    assert!(!compat(&WeftType::JsonDict, &t("{ a: String }")));
    assert!(!compat(&t("Dict[String, String]"), &t("{ a: String }")));
}

#[test]
fn validate_value_names_the_offending_path() {
    use serde_json::json;
    let history = reg_parse("ChatHistory");

    assert!(history.validate_value(&json!([])).is_ok());
    assert!(history
        .validate_value(&json!([{ "role": "user", "content": "hi" }]))
        .is_ok());
    // Optional field present, and content as parts with a stored image.
    assert!(history
        .validate_value(&json!([{
            "role": "assistant", "name": "bot",
            "content": [
                { "type": "text", "text": "look" },
                { "type": "image", "image": file_url("image/png") },
            ],
        }]))
        .is_ok());

    let err = history
        .validate_value(&json!([{ "role": "user", "content": "a" }, { "role": 3, "content": "b" }]))
        .unwrap_err();
    assert!(err.contains("[1].role"), "path names the bad message: {err}");

    let err = history
        .validate_value(&json!([{ "role": "user", "content": "a", "extra": 1 }]))
        .unwrap_err();
    assert!(err.contains("unknown field \"extra\""), "{err}");

    let err = history.validate_value(&json!([{ "role": "user" }])).unwrap_err();
    assert!(err.contains("missing required field \"content\""), "{err}");

    // A raw URL string is NOT a stored image: the marker is the contract.
    let err = history
        .validate_value(&json!([{
            "role": "user",
            "content": [{ "type": "image", "image": "https://x.com/f.png" }],
        }]))
        .unwrap_err();
    assert!(err.contains(".image"), "{err}");
}

#[test]
fn cast_value_validates_into_named_and_record_types() {
    use serde_json::json;
    let message = reg_parse("ChatMessage");

    // A hand-built matching dict casts (the Cast node's runtime door).
    let v = json!({ "role": "user", "content": "hi" });
    assert_eq!(message.cast_value(&v).unwrap(), v);
    // A JSON string parses then validates.
    assert_eq!(
        message.cast_value(&json!("{\"role\": \"user\", \"content\": \"hi\"}")).unwrap(),
        v
    );
    // Wrong shapes are refused naming the field.
    let err = message.cast_value(&json!({ "role": "user" })).unwrap_err();
    assert!(err.contains("missing required field"), "{err}");
    let err = message.cast_value(&json!({ "role": "user", "content": "x", "zz": 1 })).unwrap_err();
    assert!(err.contains("unknown field"), "{err}");
    // A record with a file-typed field accepts a marker, refuses raw bytes.
    let part = reg_parse("Part");
    assert!(part
        .cast_value(&json!({ "type": "image", "image": file_url("image/png") }))
        .is_ok());
    assert!(part.cast_value(&json!({ "type": "image", "image": "base64stuff" })).is_err());
}

#[test]
fn required_nullable_fields_allow_null_but_must_be_present() {
    let required = WeftType::parse("{ value: Number | Null }").unwrap();
    for value in [serde_json::json!({ "value": null }), serde_json::json!({ "value": 3 })] {
        assert!(required.validate_value(&value).is_ok());
        assert_eq!(required.cast_value(&value).unwrap(), value);
    }
    let absent = serde_json::json!({});
    assert!(required.validate_value(&absent).unwrap_err().contains("missing required field"));
    assert!(required.cast_value(&absent).unwrap_err().contains("missing required field"));

    let optional = WeftType::parse("{ value?: Number }").unwrap();
    for value in [absent, serde_json::json!({ "value": null })] {
        assert!(optional.validate_value(&value).is_ok());
        assert!(optional.cast_value(&value).is_ok());
    }
}

#[test]
fn installed_registry_is_idempotent_for_identical_content_only() {
    // `install` is process-global; this test only checks the content
    // rule through fresh threads' current() view being scope-driven.
    let reg = test_registry();
    let seen = reg.clone().scoped(TypeRegistry::current);
    assert_eq!(*seen, *reg);
    // Outside the scope, bare names are gone again (builtin only).
    assert!(WeftType::parse("ChatMessage").is_none());
}

// ----- Generator[T] ---------------------------------------------------

#[test]
fn generator_parses_prints_and_round_trips() {
    let g = WeftType::parse("Generator[Number]").expect("parses");
    assert_eq!(g, WeftType::Generator(Box::new(WeftType::Primitive(WeftPrimitive::Number))));
    assert_eq!(g.to_string(), "Generator[Number]");
    assert_eq!(WeftType::parse(&g.to_string()), Some(g));
    // Nested element types round-trip too.
    let nested = WeftType::parse("Generator[List[String | Number]]").expect("parses");
    assert_eq!(WeftType::parse(&nested.to_string()), Some(nested.clone()));
    assert_eq!(WeftType::parse(&nested.wire_string()), Some(nested));
    // `Generator` bare (no element) is not a type.
    assert!(WeftType::parse("Generator").is_none());
    // The name is reserved for user type declarations.
    assert!(TypeRegistry::build(&[(
        "Generator".into(),
        "String".into(),
        "test".into()
    )])
    .is_err());
}

#[test]
fn generator_compatibility_is_invariant_and_never_decays() {
    let g_num = WeftType::parse("Generator[Number]").unwrap();
    let g_str = WeftType::parse("Generator[String]").unwrap();
    let g_union = WeftType::parse("Generator[Number | String]").unwrap();
    let num = WeftType::parse("Number").unwrap();
    assert!(WeftType::is_compatible(&g_num, &g_num), "same element connects");
    assert!(!WeftType::is_compatible(&g_num, &g_str), "different element refuses");
    // Invariant: a narrower element does NOT flow into a wider one.
    assert!(!WeftType::is_compatible(&g_num, &g_union));
    assert!(!WeftType::is_compatible(&g_union, &g_num));
    // A stream never decays into (or from) its element or a list.
    assert!(!WeftType::is_compatible(&g_num, &num));
    assert!(!WeftType::is_compatible(&num, &g_num));
    assert!(!WeftType::is_compatible(&g_num, &WeftType::parse("List[Number]").unwrap()));
    assert!(!WeftType::is_compatible(&WeftType::parse("List[Number]").unwrap(), &g_num));
}

#[test]
fn generator_is_wire_only_with_no_literal_no_default_no_cast() {
    let g = WeftType::parse("Generator[Number]").unwrap();
    assert_eq!(crate::node::Accepts::for_type(&g), crate::node::Accepts::wire_only(), "a live edge takes no literal");
    assert_eq!(g.zero_value(), serde_json::Value::Null, "no honest zero for a stream");
    assert!(g.cast_value(&serde_json::json!([1, 2, 3])).is_err(), "nothing casts into a stream");
    assert!(WeftType::cast_allowed(&WeftType::parse("List[Number]").unwrap(), &g).is_err());
    assert!(
        WeftType::cast_allowed(&g, &WeftType::parse("String").unwrap()).is_err(),
        "a stream does not stringify"
    );
}

#[test]
fn generator_with_unresolved_element_reads_as_unresolved_leaf() {
    let g = WeftType::parse("Generator[T]").unwrap();
    assert!(g.contains_unresolved_leaf(), "an unresolved element must resolve before use");
    assert!(!WeftType::parse("Generator[Number]").unwrap().contains_unresolved_leaf());
}

#[test]
fn generator_marker_validates_as_the_port_value() {
    let g = WeftType::parse("Generator[Number]").unwrap();
    let marker = serde_json::json!({ GENERATOR_MARKER_KEY: { "id": "00000000-0000-0000-0000-000000000000" } });
    assert!(g.validate_value(&marker).is_ok(), "the live-handle marker is the port value");
    assert!(g.validate_value(&serde_json::json!(1)).is_err(), "an item is not the port value");
    assert!(g.validate_value(&serde_json::json!([1, 2])).is_err(), "a list is not a stream");
}

#[test]
fn as_generator_peels_nominal_aliases_and_port_value_type_answers_the_element() {
    // A nominal alias over a stream IS a stream: every generator rule
    // (compiler and runtime) reads the port through `as_generator`, so
    // the alias must peel exactly like the bare `Generator[T]`.
    let bare = WeftType::parse("Generator[Number]").unwrap();
    let number = WeftType::parse("Number").unwrap();
    let alias = WeftType::Named { name: "Rows".into(), body: Box::new(bare.clone()) };
    assert_eq!(bare.as_generator(), Some(&number));
    assert_eq!(alias.as_generator(), Some(&number), "a nominal alias over a stream is a stream");
    let chained = WeftType::Named { name: "F2".into(), body: Box::new(alias.clone()) };
    assert_eq!(chained.as_generator(), Some(&number), "aliases chain; every layer peels");
    assert_eq!(number.as_generator(), None);
    // The port VALUE of a stream port is one item; anything else is itself.
    assert_eq!(bare.port_value_type(), &number);
    assert_eq!(alias.port_value_type(), &number);
    assert_eq!(number.port_value_type(), &number);
}

#[test]
fn a_declared_type_body_with_a_typevar_is_refused() {
    // `T` is a node-scoped port variable; a declaration carrying one
    // could only ever produce two same-named types with different
    // bodies, which name-only nominal compatibility must never see.
    let err = TypeRegistry::build(&[(
        "MyBox".to_string(),
        "List[T]".to_string(),
        "test".to_string(),
    )])
    .expect_err("a typevar body must be refused");
    assert!(err.contains("must be concrete"), "{err}");
    assert!(err.contains("MyBox"), "{err}");
}

#[test]
fn the_wire_named_form_refuses_a_typevar_body_but_stays_registry_independent() {
    // The self-contained `Name=Body` wire form is the SECOND door a
    // Named type can be built through; it enforces the one
    // registry-independent half of the contract (no type variable in
    // the body). Restatement agreement is the compiler's job.
    assert!(WeftType::parse("MyBox=List[T]").is_none(), "a typevar body is refused");
    // Parsing stays PURE: a wire string deserializes identically with
    // or without a registry, even one whose declaration has since been
    // EDITED (a resumed run must get the exact shape it started
    // on). Restatement agreement is the compiler's job
    // (`named-type-conflict`), never the parser's.
    let reg = std::sync::Arc::new(
        TypeRegistry::build(&[(
            "Feed".to_string(),
            "Generator[String]".to_string(),
            "test".to_string(),
        )])
        .expect("registry builds"),
    );
    reg.scoped(|| {
        assert!(
            WeftType::parse("Feed=Generator[Number]").is_some(),
            "an old stored wire string keeps parsing after the declaration changed"
        );
    });
}

/// A nullable file union keeps its picker: `Image | Null` is a file
/// port the user may leave empty, not a JSON textarea. A union mixing
/// files with real non-file data still gets none (a picker would take
/// the other half away).
#[test]
fn a_nullable_file_union_still_offers_the_file_picker() {
    let nullable = WeftType::parse("Image | Null").expect("parses");
    let control = nullable.file_control().expect("nullable file keeps its picker");
    assert!(!control.multiple);
    assert!(WeftType::parse("Image | String").expect("parses").file_control().is_none());
}

/// A declared name over a list (or union) of files offers what its
/// body offers: `type Attachments = List[Media]` is a multi-file
/// picker, never the JSON textarea the bare name would fall to.
#[test]
fn a_named_alias_over_files_still_offers_the_file_picker() {
    let attachments = WeftType::parse("Attachments=List[Image | Audio]").expect("parses");
    let control = attachments.file_control().expect("the alias peels to its body");
    assert!(control.multiple);
    assert_eq!(control.file_type, WeftType::parse("Image | Audio").expect("parses"));
    let single = WeftType::parse("Pic=Image").expect("parses");
    let control = single.file_control().expect("a named file primitive keeps its name");
    assert_eq!(control.file_type, single, "the written type keeps the alias");
    assert!(WeftType::parse("Row=List[String]").expect("parses").file_control().is_none());
}

// ═══════════════════════════════════════════════════════════════════════
// Open records, fits_within, substitution, type parameters
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn an_open_record_parses_prints_and_round_trips_on_the_wire() {
    let t = ty("{ email: String, age?: Number, .. }");
    let WeftType::Record { fields, open: true } = &t else { panic!("not an open record: {t}") };
    assert_eq!(fields.len(), 2);
    assert!(!fields[0].optional && fields[1].optional);
    assert_eq!(t.to_string(), "{email: String, age?: Number, ..}");
    assert_eq!(ty(&t.to_string()), t);
    let json = serde_json::to_string(&t).unwrap();
    let back: WeftType = serde_json::from_str(&json).unwrap();
    assert_eq!(back, t);
    assert_eq!(back.wire_string(), t.wire_string());
}

#[test]
fn an_open_record_takes_dot_dot_last_and_at_least_one_field() {
    let e = WeftType::try_parse("{ .., email: String }").unwrap_err();
    assert!(e.contains("comes last"), "{e}");
    for empty in ["{ .. }", "{}"] {
        let e = WeftType::try_parse(empty).unwrap_err();
        assert!(e.contains("declares no field"), "{empty}: {e}");
    }
}

#[test]
fn record_equality_tells_open_from_closed() {
    assert_ne!(ty("{ a: String, .. }"), ty("{ a: String }"));
    assert_eq!(ty("{ a: String, .. }"), ty("{ a: String, .. }"));
    assert_eq!(WeftType::record(vec![RecordField {
        name: "a".into(),
        ty: WeftType::primitive(WeftPrimitive::String),
        optional: false,
    }]), ty("{ a: String }"), "record() builds a closed one");
}

#[test]
fn validate_value_refuses_an_unknown_key_only_on_a_closed_record() {
    use serde_json::json;
    let value = json!({ "email": "a@b", "utm": "x" });
    let e = ty("{ email: String }").validate_value(&value).unwrap_err();
    assert!(e.contains("unknown field \"utm\""), "{e}");
    ty("{ email: String, .. }").validate_value(&value).expect("an open record takes it");
    for t in ["{ email: String }", "{ email: String, .. }"] {
        let e = ty(t).validate_value(&json!({})).unwrap_err();
        assert!(e.contains("missing required field \"email\""), "{t}: {e}");
    }
}

#[test]
fn conform_value_drops_what_an_open_record_does_not_declare_at_any_depth() {
    use serde_json::json;
    let list = ty("List[{ email: String, .. }]");
    assert_eq!(
        list.conform_value(json!([{ "email": "a", "utm": 1 }, { "email": "b" }])).unwrap(),
        json!([{ "email": "a" }, { "email": "b" }]),
    );
    let nested = ty("{ id: Number, user: { email: String, .. } }");
    assert_eq!(
        nested.conform_value(json!({ "id": 1, "user": { "email": "a", "utm": "x" } })).unwrap(),
        json!({ "id": 1, "user": { "email": "a" } }),
    );
    let e = nested
        .conform_value(json!({ "id": 1, "extra": true, "user": { "email": "a" } }))
        .unwrap_err();
    assert!(e.contains("unknown field \"extra\""), "the closed parent still refuses: {e}");

    let closed = ty("{ a: String, b?: Number }");
    let value = json!({ "a": "x", "b": null });
    assert_eq!(closed.conform_value(value.clone()).unwrap(), value, "a closed record's value is untouched");
}

#[test]
fn cast_value_into_an_open_record_drops_unknown_keys() {
    use serde_json::json;
    let open = ty("{ email: String, .. }");
    assert_eq!(open.cast_value(&json!({ "email": "a", "utm": "x" })).unwrap(), json!({ "email": "a" }));
    let e = ty("{ email: String }").cast_value(&json!({ "email": "a", "utm": "x" })).unwrap_err();
    assert!(e.contains("utm"), "{e}");
}

#[test]
fn record_compatibility_lets_an_open_target_take_undeclared_fields() {
    let compat = WeftType::is_compatible;
    let source = ty("{ email: String, utm: String }");
    assert!(!compat(&source, &ty("{ email: String }")), "unknown field into a closed target");
    assert!(compat(&source, &ty("{ email: String, .. }")), "unknown field into an open target");
    for target in ["{ email: String, age: Number }", "{ email: String, age: Number, .. }"] {
        assert!(!compat(&source, &ty(target)), "missing required `age` into {target}");
    }
}

#[test]
fn fits_within_narrows_files_records_and_stream_items() {
    assert!(WeftType::fits_within(&ty("Image"), &ty("Media")));
    assert!(!WeftType::fits_within(&ty("File"), &ty("Media")));
    assert!(WeftType::fits_within(&ty("{ a: String }"), &WeftType::JsonDict));
    let stream = ty("Generator[{ a: String }]");
    let bound = ty("Generator[JsonDict]");
    assert!(WeftType::fits_within(&stream, &bound), "a stream narrows item-wise");
    assert!(!WeftType::is_compatible(&stream, &bound), "wiring one stream into another stays invariant");
}

/// A named bound stays nominal: only its own name fits it, never the
/// bare structure nor another name over the same body.
#[test]
fn fits_within_keeps_a_named_bound_nominal() {
    let rows = ty("Rows=Generator[Number]");
    assert!(!WeftType::fits_within(&ty("Generator[Number]"), &rows));
    assert!(!WeftType::fits_within(&ty("Other=Generator[Number]"), &rows));
    assert!(WeftType::fits_within(&rows, &rows));
    assert!(WeftType::fits_within(&rows, &ty("Generator[Number]")), "a name decays into its structure");
}

/// An optional record field may hold null, so a dict it flows into must
/// take null too.
#[test]
fn a_record_with_an_optional_field_needs_a_dict_taking_null() {
    assert!(!WeftType::is_compatible(&ty("{ a?: Number }"), &ty("Dict[String, Number]")));
    assert!(WeftType::is_compatible(&ty("{ a?: Number }"), &ty("Dict[String, Number | Null]")));
    assert!(WeftType::is_compatible(&ty("{ a: Number }"), &ty("Dict[String, Number]")));
}

/// A union whose members would hand the same value on differently is
/// refused rather than read through whichever member was written first.
#[test]
fn conform_refuses_a_union_reading_a_value_two_ways() {
    use serde_json::json;
    let value = json!({ "a": "x", "b": "y" });
    for written in ["{ a: String, .. } | JsonDict", "JsonDict | { a: String, .. }"] {
        let e = ty(written).conform_value(value.clone()).unwrap_err();
        assert!(e.contains("ambiguous value"), "{written}: {e}");
    }
    // Members that agree on the value are no ambiguity.
    assert_eq!(ty("{ a: String, .. } | { a: String, b?: String, .. }").conform_value(json!({ "a": "x" })).unwrap(), json!({ "a": "x" }));
    assert_eq!(ty("{ a: String, .. } | Number").conform_value(value).unwrap(), json!({ "a": "x" }));
}

/// Substitution rewrites every shape a type variable can occur in, so
/// a binding never leaves a port half-resolved. (No Named case: the
/// registry refuses a declared body carrying a type variable.)
#[test]
fn substitute_var_rewrites_every_shape_a_typevar_can_occur_in() {
    let number = ty("Number");
    for (before, after) in [
        ("{a: T, b: String}", "{a: Number, b: String}"),
        ("Generator[T]", "Generator[Number]"),
        ("T | Null", "Number | Null"),
        ("Dict[String, T]", "Dict[String, Number]"),
    ] {
        let mut t = ty(before);
        assert!(t.substitute_var("T", &number), "{before}");
        assert_eq!(t, ty(after), "{before}");
    }
    let mut untouched = ty("List[T2]");
    assert!(!untouched.substitute_var("T", &number));
    assert_eq!(untouched, ty("List[T2]"));
}

#[test]
fn substituting_a_union_into_a_union_flattens_it() {
    let mut t = ty("T | Null");
    assert!(t.substitute_var("T", &ty("String | Number")));
    assert_eq!(t, ty("String | Number | Null"));
    let WeftType::Union(members) = &t else { panic!("not a union: {t}") };
    assert_eq!(members.len(), 3, "one flat union: {t}");
}

#[test]
fn type_param_lists_parse_in_written_order() {
    let params = TypeParam::parse_list("[T: Media, T2: List[T]]").unwrap();
    assert_eq!(params.len(), 2);
    assert_eq!(params[0], TypeParam::new("T", WeftType::media()));
    assert_eq!(params[1], TypeParam::new("T2", ty("List[T]")));
    TypeParam::check_declarations(&params).expect("a bound may name an earlier variable");

    let e = TypeParam::parse_list("[T]").unwrap_err();
    assert!(e.contains("has no bound"), "{e}");
    let e = TypeParam::parse_list("[]").unwrap_err();
    assert!(e.contains("empty type parameter list"), "{e}");
    let e = TypeParam::parse_list("[T: Foo]").unwrap_err();
    assert!(e.contains("the bound of type parameter `T`"), "{e}");
}

#[test]
fn type_param_declarations_refuse_a_non_variable_a_duplicate_and_a_cycle() {
    let e = TypeParam::check_declarations(&[TypeParam::new("Foo", ty("String"))]).unwrap_err();
    assert!(e.contains("`Foo` is not a type variable"), "{e}");

    let twice = [TypeParam::new("T", ty("String")), TypeParam::new("T", ty("Number"))];
    let e = TypeParam::check_declarations(&twice).unwrap_err();
    assert!(e.contains("declared twice"), "{e}");

    let cycle = TypeParam::parse_list("[T1: List[T2], T2: List[T1]]").unwrap();
    let e = TypeParam::check_declarations(&cycle).unwrap_err();
    assert!(e.contains("`T1`") && e.contains("`T2`"), "{e}");
    assert!(e.contains("T1 -> T2 -> T1"), "{e}");

    // A node type's bounds and the ones a program writes over them are
    // checked together: each alone is acyclic, the two make a cycle.
    let mut combined = TypeParam::parse_list("[T: List[T2], T2: String]").unwrap();
    combined.extend(TypeParam::parse_list("[T2: List[T]]").unwrap());
    let e = TypeParam::check_cycles(&combined).unwrap_err();
    assert!(e.contains("cycle"), "{e}");
    assert_eq!(TypeParam::written_entries("[T: Image | Video, T2: { a: String, .. }]").unwrap(), vec!["T: Image | Video", "T2: { a: String, .. }"]);
}

#[test]
fn an_unused_type_param_is_refused() {
    let params = TypeParam::parse_list("[T: Media, T2: String]").unwrap();
    let used: std::collections::BTreeSet<std::string::String> = ["T".to_string()].into();
    let e = TypeParam::check_used(&params, &used).unwrap_err();
    assert!(e.contains("`T2`") && e.contains("no port uses it"), "{e}");
    let both: std::collections::BTreeSet<std::string::String> = ["T".to_string(), "T2".to_string()].into();
    TypeParam::check_used(&params, &both).expect("every parameter is used");
}

#[test]
fn a_type_param_violation_is_reported_only_once_there_is_something_to_check() {
    let mut media = TypeParam::new("T", WeftType::media());
    assert_eq!(media.violation(), None, "unresolved");

    let mut chained = TypeParam::new("T2", ty("List[T]"));
    chained.resolved = Some(ty("Number"));
    assert_eq!(chained.violation(), None, "the bound still holds a variable");

    media.resolved = Some(ty("String"));
    let e = media.violation().expect("String does not fit Media");
    assert!(e.contains(&WeftType::media().to_string()) && e.contains("String"), "{e}");

    media.resolved = Some(ty("Image"));
    assert_eq!(media.violation(), None, "Image fits Media");
}

#[test]
fn an_unresolved_leaf_takes_any_value_inside_a_checked_shape() {
    use serde_json::json;
    let open_ended = ty("List[T]");
    assert_eq!(open_ended.conform_value(json!([1, "a"])).unwrap(), json!([1, "a"]));
    assert!(open_ended.conform_value(json!("not a list")).is_err());
    // A record holding a variable is still a record: its undeclared keys
    // drop when it is open, and a closed one refuses them.
    assert_eq!(ty("{ x: T, .. }").conform_value(json!({ "x": 1, "y": 2 })).unwrap(), json!({ "x": 1 }));
    assert!(ty("{ x: T }").conform_value(json!({ "x": 1, "y": 2 })).is_err());

    let e = ty("{ score: Number }").conform_value(json!({})).unwrap_err();
    assert!(e.contains("missing required field \"score\""), "{e}");
}

// ── Json ─────────────────────────────────────────────────────────────

#[test]
fn json_parses_displays_and_is_a_reserved_name() {
    for written in ["Json", "List[Json]", "Generator[Json]", "Json | Null", "Dict[String, Json]"] {
        let parsed = ty(written);
        assert_eq!(parsed.to_string(), written);
        assert_eq!(WeftType::parse(&parsed.wire_string()), Some(parsed));
    }
    assert_eq!(ty("Json"), WeftType::Json);
    assert_ne!(ty("Json"), WeftType::JsonDict);
    assert!(TypeRegistry::build(&[("Json".into(), "String".into(), "test".into())]).is_err(), "Json is the language's");
}

/// Every JSON-made type flows into `Json`, at any depth; nothing that is
/// a file, a live handle or an access does, wherever it sits.
#[test]
fn every_json_made_type_fits_json_and_nothing_else_does() {
    let json = WeftType::Json;
    let fits = |s: &str| WeftType::is_compatible(&ty(s), &json);
    for source in [
        "String",
        "Number",
        "Boolean",
        "Null",
        "Empty",
        "JsonDict",
        "Json",
        "Dict[String, Number]",
        "Dict[String, List[JsonDict]]",
        "List[String]",
        "List[List[List[Number | String]]]",
        "{ a: String, b?: List[Number] }",
        "{ a: String, .. }",
        "String | Number | Null",
        "List[T]",
        "T",
    ] {
        assert!(fits(source), "{source} fits Json");
    }
    let named = WeftType::Named { name: "Rows".into(), body: Box::new(ty("List[List[Number]]")) };
    assert!(WeftType::is_compatible(&named, &json), "a name falls away into its shape");
    for source in [
        "Image",
        "Blob",
        "Media",
        "File",
        "Bus",
        "Access",
        "Infra",
        "Generator[Json]",
        "Generator[String]",
        "List[Image]",
        "Dict[String, Audio]",
        "{ pic: Image }",
        "String | Image",
        "List[List[Bus]]",
    ] {
        assert!(!fits(source), "{source} is not JSON");
    }
    let named_file = WeftType::Named { name: "Photo".into(), body: Box::new(ty("Image")) };
    assert!(!WeftType::is_compatible(&named_file, &json));
}

/// Json flows out only into Json: anything narrower is a claim about the
/// shape, which is the Cast node's to check.
#[test]
fn json_flows_only_into_json() {
    let json = WeftType::Json;
    for target in ["Json", "T", "String | Json", "Json | Null"] {
        assert!(WeftType::is_compatible(&json, &ty(target)), "Json into {target}");
    }
    for target in ["JsonDict", "String", "Number", "Null", "List[Json]", "Dict[String, Json]", "{ a: String, .. }"] {
        assert!(!WeftType::is_compatible(&json, &ty(target)), "Json into {target} needs a Cast");
    }
    let named = WeftType::Named { name: "Rows".into(), body: Box::new(ty("Json")) };
    assert!(!WeftType::is_compatible(&json, &named), "nothing unnamed flows into a name");
    // A narrowable output whose ceiling holds Json takes any JSON-made
    // stream item; a stream wired to a stream stays invariant.
    let ceiling = ty("Generator[JsonDict | Json | File]");
    for declared in ["Generator[JsonDict]", "Generator[String]", "Generator[List[List[Number]]]", "Generator[{ pic: Image }]"] {
        assert!(WeftType::fits_within(&ty(declared), &ceiling), "{declared}");
    }
    assert!(!WeftType::is_compatible(&ty("Generator[String]"), &ty("Generator[Json]")));
}

#[test]
fn json_takes_any_json_value_at_run_time() {
    use serde_json::json;
    let json_type = WeftType::Json;
    for value in [json!([[1, "a"], [2]]), json!("text"), json!(3.5), json!(null), json!(true), json!({ "a": [1] })] {
        assert!(json_type.validate_value(&value).is_ok(), "{value}");
        assert_eq!(json_type.conform_value(value.clone()).unwrap(), value, "held as it is");
    }
    assert!(json_type.contains_null(), "null is a JSON value, so it is data on a Json port");
    assert_eq!(json_type.zero_value(), json!(null));
    assert!(!json_type.references_file());
    // Inside a shape, the shape is still held.
    assert!(ty("List[Json]").validate_value(&json!("not a list")).is_err());
    assert!(ty("{ raw: Json }").validate_value(&json!({})).is_err(), "a required field is still required");
}

/// The Cast node narrows Json into any JSON-made type, checking the
/// value at run time; a file target or a stream never comes from Json.
#[test]
fn a_cast_narrows_json_and_checks_the_value() {
    use serde_json::json;
    let json_type = WeftType::Json;
    for target in ["String", "Number", "Boolean", "JsonDict", "List[List[Number]]", "Dict[String, Json]", "{ id: String }"] {
        assert!(WeftType::cast_allowed(&json_type, &ty(target)).is_ok(), "Json -> {target}");
    }
    let named = WeftType::Named { name: "Ticket".into(), body: Box::new(ty("{ id: String }")) };
    assert!(WeftType::cast_allowed(&json_type, &named).is_ok());
    assert!(WeftType::cast_allowed(&json_type, &ty("Generator[Json]")).is_err());
    assert!(WeftType::cast_allowed(&json_type, &ty("Bus")).is_err());
    assert!(WeftType::cast_allowed(&ty("Image"), &json_type).is_err(), "a file never becomes Json");

    let record = ty("{ id: String, n?: Number }");
    assert_eq!(record.cast_requested(&json!({ "id": "a" })).unwrap(), json!({ "id": "a" }));
    let e = record.cast_requested(&json!([1, 2])).unwrap_err();
    assert!(e.contains("no cast from List"), "{e}");
    let e = record.cast_requested(&json!({ "n": 1 })).unwrap_err();
    assert!(e.contains("missing required field \"id\""), "{e}");
    assert_eq!(ty("List[List[Number]]").cast_requested(&json!([[1], [2, 3]])).unwrap(), json!([[1], [2, 3]]));
    assert!(ty("JsonDict").cast_requested(&json!([1])).is_err(), "a list is no object");
    // Into Json itself, any JSON value passes; a stored file does not.
    assert_eq!(json_type.cast_value(&json!([1, "a"])).unwrap(), json!([1, "a"]));
    assert!(json_type.cast_value(&file_url("image/png")).is_err());
}

/// The Cast node turns a structure into its JSON text when asked; a
/// value written into a String port never is.
#[test]
fn only_a_requested_cast_turns_a_structure_into_text() {
    use serde_json::json;
    let text = ty("String");
    assert_eq!(text.cast_requested(&json!({ "a": [1, 2] })).unwrap(), json!(r#"{"a":[1,2]}"#));
    assert_eq!(ty("List[String]").cast_requested(&json!([[1], "x"])).unwrap(), json!(["[1]", "x"]));
    assert!(text.cast_value(&json!({ "a": 1 })).is_err());
    // In a union, the text is the last resort: a member taking the
    // structure as it is wins, and the text comes out only when none can.
    assert_eq!(ty("String | List[String]").cast_requested(&json!([1, 2])).unwrap(), json!(["1", "2"]));
    assert_eq!(ty("String | List[Number]").cast_requested(&json!(["1", "2"])).unwrap(), json!([1.0, 2.0]));
    assert_eq!(ty("String | Number").cast_requested(&json!({ "a": 1 })).unwrap(), json!(r#"{"a":1}"#));
    let label = WeftType::Named { name: "Label".into(), body: Box::new(ty("String | Number")) };
    let target = WeftType::union(vec![label, ty("List[String]")]);
    assert_eq!(target.cast_requested(&json!([1, 2])).unwrap(), json!(["1", "2"]), "a name over a union with text in it waits too");
    assert!(WeftType::cast_allowed(&ty("JsonDict"), &text).is_ok(), "the compile-time table grants it");
}
