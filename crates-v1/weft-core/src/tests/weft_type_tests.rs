use super::*;

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
          Some(WeftType::dict(WeftType::primitive(WeftPrimitive::String), WeftType::primitive(WeftPrimitive::Number)))
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

  #[test]
  fn parse_must_override() {
      assert_eq!(WeftType::parse("MustOverride"), Some(WeftType::MustOverride));
  }

  // ── Media alias tests ────────────────────────────────────────────────

  #[test]
  fn parse_media_alias() {
      let m = WeftType::parse("Media").unwrap();
      assert!(matches!(m, WeftType::Union(_)));
      if let WeftType::Union(types) = &m {
          assert_eq!(types.len(), 4);
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
      // Media | String = Image | Video | Audio | Document | String
      let t = WeftType::parse("Media | String").unwrap();
      if let WeftType::Union(types) = &t {
          assert_eq!(types.len(), 5); // 4 media + String
      } else {
          panic!("expected union");
      }
  }

  #[test]
  fn parse_list_media_or_string() {
      let t = WeftType::parse("List[Media | String]").unwrap();
      if let WeftType::List(inner) = &t {
          if let WeftType::Union(types) = inner.as_ref() {
              assert_eq!(types.len(), 5);
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
      if let WeftType::Dict(_, v) = &t {
          assert!(matches!(v.as_ref(), WeftType::Union(_)));
      } else {
          panic!("expected dict");
      }
  }

  #[test]
  fn compatibility_media_accepts_image() {
      let media = WeftType::parse("Media").unwrap();
      let image = WeftType::primitive(WeftPrimitive::Image);
      // Image fits into Media (Image | Video | Audio | Document)
      assert!(WeftType::is_compatible(&image, &media));
      // Media does not fit into just Image (Video/Audio/Document might arrive)
      assert!(!WeftType::is_compatible(&media, &image));
  }

  #[test]
  fn compatibility_media_accepts_all_media_types() {
      let media = WeftType::parse("Media").unwrap();
      assert!(WeftType::is_compatible(&WeftType::primitive(WeftPrimitive::Image), &media));
      assert!(WeftType::is_compatible(&WeftType::primitive(WeftPrimitive::Video), &media));
      assert!(WeftType::is_compatible(&WeftType::primitive(WeftPrimitive::Audio), &media));
      assert!(WeftType::is_compatible(&WeftType::primitive(WeftPrimitive::Document), &media));
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
      // List[Media] → List[Image]: fails (Video/Audio/Document don't fit into Image)
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
      assert!(check(&media, &serde_json::json!({"url": "https://x.com/i.png", "mimeType": "image/png"})));
      assert!(check(&media, &serde_json::json!({"url": "https://x.com/v.mp4", "mimeType": "video/mp4"})));
      assert!(!check(&media, &serde_json::json!("just a string")));
      assert!(!check(&media, &serde_json::json!(42)));
  }

  #[test]
  fn runtime_check_list_media_or_string() {
      let t = WeftType::parse("List[Media | String]").unwrap();
      assert!(check(&t, &serde_json::json!([
          {"url": "https://x.com/i.png", "mimeType": "image/png"},
          "hello"
      ])));
      assert!(!check(&t, &serde_json::json!([
          {"url": "https://x.com/i.png", "mimeType": "image/png"},
          42
      ])));
  }

  #[test]
  fn runtime_check_dict_string_media() {
      let t = WeftType::parse("Dict[String, Media]").unwrap();
      assert!(check(&t, &serde_json::json!({
          "photo": {"url": "https://x.com/i.png", "mimeType": "image/png"},
          "clip": {"url": "https://x.com/v.mp4", "mimeType": "video/mp4"}
      })));
      assert!(!check(&t, &serde_json::json!({
          "photo": "not a media object"
      })));
  }

  #[test]
  fn media_roundtrip_display() {
      let t = WeftType::parse("Media").unwrap();
      let s = t.to_string();
      // Media expands to union, so display shows the expanded form
      assert_eq!(s, "Image | Video | Audio | Document");
      // Re-parsing the expanded form should give the same type
      let reparsed = WeftType::parse(&s).unwrap();
      assert_eq!(t, reparsed);
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
          "MustOverride",
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
  fn compatibility_must_override() {
      let s = WeftType::primitive(WeftPrimitive::String);
      let mo = WeftType::must_override();
      assert!(WeftType::is_compatible(&s, &mo));
      assert!(WeftType::is_compatible(&mo, &s));
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

      let ds = WeftType::dict(s.clone(), s.clone());
      let dsn = WeftType::dict(s.clone(), sn.clone());

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
      let ld_sn = WeftType::list(WeftType::dict(s.clone(), sn.clone()));
      let ld_snb = WeftType::list(WeftType::dict(s.clone(), snb.clone()));

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
      let inner_sn = WeftType::dict(s.clone(), WeftType::union(vec![s.clone(), n.clone()]));
      let outer_sn = WeftType::dict(s.clone(), inner_sn);

      // Dict[String, Dict[String, String|Boolean]]
      let inner_sb = WeftType::dict(s.clone(), WeftType::union(vec![s.clone(), b.clone()]));
      let outer_sb = WeftType::dict(s.clone(), inner_sb);

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
      let inner_s = WeftType::dict(s.clone(), s.clone());
      let outer_s = WeftType::dict(s.clone(), inner_s);

      // Dict[String, Dict[String, String|Number|Boolean]]
      let inner_snb = WeftType::dict(s.clone(), WeftType::union(vec![s.clone(), n.clone(), b.clone()]));
      let outer_snb = WeftType::dict(s.clone(), inner_snb);

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
      let t1 = WeftType::list(WeftType::list(WeftType::dict(s.clone(), s.clone())));
      // List[List[Dict[String, String|Number]]]
      let t2 = WeftType::list(WeftType::list(WeftType::dict(s.clone(), WeftType::union(vec![s.clone(), n.clone()]))));

      // Narrow → Wide: OK
      assert!(WeftType::is_compatible(&t1, &t2));
      // Wide → Narrow: fails
      assert!(!WeftType::is_compatible(&t2, &t1));
      // Same → Same: OK
      assert!(WeftType::is_compatible(&t1, &t1));
      assert!(WeftType::is_compatible(&t2, &t2));
  }

  #[test]
  fn expand_element_type() {
      let ls = WeftType::list(WeftType::primitive(WeftPrimitive::String));
      assert_eq!(ls.expand_element_type(), Some(WeftType::primitive(WeftPrimitive::String)));
      // Non-list can't be expanded
      let s = WeftType::primitive(WeftPrimitive::String);
      assert_eq!(s.expand_element_type(), None);
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
      let d = WeftType::dict(WeftType::primitive(WeftPrimitive::String), WeftType::primitive(WeftPrimitive::Number));
      assert!(check(&d, &serde_json::json!({"a": 1, "b": 2})));
      assert!(!check(&d, &serde_json::json!({"a": "not a number"})));
  }

  // ── Runtime check: primitives ───────────────────────────────────────

  #[test]
  fn runtime_check_primitives() {
      assert!(check(&WeftType::primitive(WeftPrimitive::String), &serde_json::json!("hello")));
      assert!(!check(&WeftType::primitive(WeftPrimitive::String), &serde_json::json!(42)));
      assert!(check(&WeftType::primitive(WeftPrimitive::Number), &serde_json::json!(3.14)));
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
      let img = serde_json::json!({"url": "https://x.com/i.png", "mimeType": "image/png"});
      assert_eq!(WeftType::infer(&img), WeftType::primitive(WeftPrimitive::Image));
      let vid = serde_json::json!({"url": "https://x.com/v.mp4", "mimeType": "video/mp4"});
      assert_eq!(WeftType::infer(&vid), WeftType::primitive(WeftPrimitive::Video));
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
      assert!(check(&img, &serde_json::json!({"url": "https://example.com/img.png", "mimeType": "image/png"})));
      assert!(!check(&img, &serde_json::json!({"url": "https://example.com/vid.mp4", "mimeType": "video/mp4"})));
      assert!(!check(&img, &serde_json::json!("just a string")));
      assert!(!check(&img, &serde_json::json!({"url": "https://example.com/img.png"}))); // no mimeType
  }

  #[test]
  fn runtime_check_video() {
      let vid = WeftType::primitive(WeftPrimitive::Video);
      assert!(check(&vid, &serde_json::json!({"url": "https://x.com/v.mp4", "mimeType": "video/mp4"})));
      assert!(!check(&vid, &serde_json::json!({"url": "https://x.com/a.mp3", "mimeType": "audio/mpeg"})));
  }

  #[test]
  fn runtime_check_audio() {
      let aud = WeftType::primitive(WeftPrimitive::Audio);
      assert!(check(&aud, &serde_json::json!({"url": "https://x.com/a.mp3", "mimeType": "audio/mpeg"})));
      assert!(!check(&aud, &serde_json::json!({"url": "https://x.com/i.png", "mimeType": "image/png"})));
  }

  #[test]
  fn runtime_check_document() {
      let doc = WeftType::primitive(WeftPrimitive::Document);
      assert!(check(&doc, &serde_json::json!({"url": "https://x.com/f.pdf", "mimeType": "application/pdf"})));
      // image/video/audio are NOT documents
      assert!(!check(&doc, &serde_json::json!({"url": "https://x.com/i.png", "mimeType": "image/png"})));
      // missing url
      assert!(!check(&doc, &serde_json::json!({"mimeType": "application/pdf"})));
  }

  #[test]
  fn runtime_check_media_alias() {
      let media = WeftType::media(); // Image | Video | Audio | Document
      assert!(check(&media, &serde_json::json!({"url": "https://x.com/i.png", "mimeType": "image/png"})));
      assert!(check(&media, &serde_json::json!({"url": "https://x.com/v.mp4", "mimeType": "video/mp4"})));
      assert!(check(&media, &serde_json::json!({"url": "https://x.com/a.mp3", "mimeType": "audio/mpeg"})));
      assert!(check(&media, &serde_json::json!({"url": "https://x.com/f.pdf", "mimeType": "application/pdf"})));
      assert!(!check(&media, &serde_json::json!("just a string")));
      assert!(!check(&media, &serde_json::json!(42)));
  }

  #[test]
  fn runtime_check_media_with_mimetype_lowercase() {
      // Some nodes use lowercase "mimetype" instead of "mimeType"
      let img = WeftType::primitive(WeftPrimitive::Image);
      assert!(check(&img, &serde_json::json!({"url": "https://x.com/i.png", "mimetype": "image/png"})));
  }

  #[test]
  fn runtime_check_media_with_data_field() {
      // Some media uses "data" instead of "url"
      let img = WeftType::primitive(WeftPrimitive::Image);
      assert!(check(&img, &serde_json::json!({"data": "base64...", "mimeType": "image/png"})));
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
      assert!(check(&sm, &serde_json::json!({"url": "https://x.com/i.png", "mimeType": "image/png"})));
      assert!(!check(&sm, &serde_json::json!(42)));
  }

  // ── Runtime check: nested types ─────────────────────────────────────

  #[test]
  fn runtime_check_list_of_dicts() {
      let t = WeftType::list(WeftType::dict(
          WeftType::primitive(WeftPrimitive::String),
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
          WeftType::primitive(WeftPrimitive::String),
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
  fn runtime_check_must_override_always_passes() {
      let t = WeftType::MustOverride;
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
          WeftType::primitive(WeftPrimitive::String),
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
      let t = WeftType::dict(WeftType::primitive(WeftPrimitive::String), WeftType::primitive(WeftPrimitive::String));
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
          WeftType::primitive(WeftPrimitive::String),
          WeftType::dict(
              WeftType::primitive(WeftPrimitive::String),
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
      let t = WeftType::dict(WeftType::primitive(WeftPrimitive::String), WeftType::primitive(WeftPrimitive::Number));
      assert!(check(&t, &serde_json::json!({})));
      let t2 = WeftType::dict(WeftType::primitive(WeftPrimitive::String), WeftType::primitive(WeftPrimitive::Boolean));
      assert!(check(&t2, &serde_json::json!({})));
  }

  #[test]
  fn runtime_check_llm_parsed_json_with_nested_arrays() {
      // Real LLM output with parseJson: Dict[String, List[String]]
      // This is the actual shape that caused a runtime type error
      let expected_type = WeftType::dict(
          WeftType::primitive(WeftPrimitive::String),
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
          WeftType::primitive(WeftPrimitive::String),
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
          WeftType::primitive(WeftPrimitive::String),
          WeftType::primitive(WeftPrimitive::Number),
      ));
      assert!(!check(&t, &serde_json::json!([{"a": 1}, {"b": "wrong"}])));
  }

  #[test]
  fn runtime_check_list_of_dicts_one_element_not_dict() {
      let t = WeftType::list(WeftType::dict(
          WeftType::primitive(WeftPrimitive::String),
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
          WeftType::primitive(WeftPrimitive::String),
          WeftType::dict(
              WeftType::primitive(WeftPrimitive::String),
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
          WeftType::primitive(WeftPrimitive::String),
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
          WeftType::primitive(WeftPrimitive::String),
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
      assert!(check(&t, &serde_json::json!([
          {"url": "https://x.com/a.png", "mimeType": "image/png"},
          {"url": "https://x.com/b.jpg", "mimeType": "image/jpeg"}
      ])));
      // One element is a video, not an image
      assert!(!check(&t, &serde_json::json!([
          {"url": "https://x.com/a.png", "mimeType": "image/png"},
          {"url": "https://x.com/v.mp4", "mimeType": "video/mp4"}
      ])));
  }

  #[test]
  fn runtime_check_deeply_nested_correct() {
      // Dict[String, List[Dict[String, String | Number]]]
      let t = WeftType::dict(
          WeftType::primitive(WeftPrimitive::String),
          WeftType::list(WeftType::dict(
              WeftType::primitive(WeftPrimitive::String),
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
          WeftType::primitive(WeftPrimitive::String),
          WeftType::list(WeftType::dict(
              WeftType::primitive(WeftPrimitive::String),
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
          WeftType::primitive(WeftPrimitive::String),
          WeftType::list(WeftType::dict(
              WeftType::primitive(WeftPrimitive::String),
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
          WeftType::primitive(WeftPrimitive::String),
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
          WeftType::primitive(WeftPrimitive::String),
          WeftType::primitive(WeftPrimitive::Number),
      );
      let dict_str_bool = WeftType::dict(
          WeftType::primitive(WeftPrimitive::String),
          WeftType::primitive(WeftPrimitive::Boolean),
      );
      let dict_str_nested = WeftType::dict(
          WeftType::primitive(WeftPrimitive::String),
          WeftType::dict(
              WeftType::primitive(WeftPrimitive::String),
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

  #[test]
  fn json_dict_not_compatible_with_dict_number_key() {
      let jd = WeftType::JsonDict;
      let dict_num_str = WeftType::dict(
          WeftType::primitive(WeftPrimitive::Number),
          WeftType::primitive(WeftPrimitive::String),
      );
      // Dict[Number, String] → JsonDict: incompatible (key must be String)
      assert!(!WeftType::is_compatible(&dict_num_str, &jd));
      assert!(!WeftType::is_compatible(&jd, &dict_num_str));
  }

  #[test]
  fn json_dict_in_union() {
      let union_type = WeftType::union(vec![
          WeftType::JsonDict,
          WeftType::primitive(WeftPrimitive::String),
      ]);
      let dict = WeftType::dict(
          WeftType::primitive(WeftPrimitive::String),
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
          WeftType::primitive(WeftPrimitive::String),
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
          WeftType::primitive(WeftPrimitive::String),
          WeftType::primitive(WeftPrimitive::Number),
      )), &serde_json::json!([])));
      // List[JsonDict] accepts []
      assert!(check(&WeftType::list(WeftType::JsonDict), &serde_json::json!([])));
      // Empty is only compatible as source, not target
      assert!(!WeftType::is_compatible(&WeftType::primitive(WeftPrimitive::String), &empty));
  }

  #[test]
  fn empty_dict_compatible_with_dict_of_anything() {
      assert!(check(&WeftType::dict(WeftType::primitive(WeftPrimitive::String), WeftType::primitive(WeftPrimitive::String)), &serde_json::json!({})));
      assert!(check(&WeftType::dict(WeftType::primitive(WeftPrimitive::String), WeftType::primitive(WeftPrimitive::Number)), &serde_json::json!({})));
      assert!(check(&WeftType::dict(
          WeftType::primitive(WeftPrimitive::String),
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
