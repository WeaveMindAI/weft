//! Layer 1: where a metadata wire stops and what runs on past it.

use weft_core::project::{Crossing, CrossingSide, Edge, Span};

use super::{cut_at, keys_through, line_at, sent_from};

fn crossing(group: &str, port: &str, side: CrossingSide, path: &[&str], line: Option<usize>) -> Crossing {
    Crossing {
        group: group.into(),
        port: port.into(),
        side,
        path: path.iter().map(|k| k.to_string()).collect(),
        span: line.map(|l| Span { start_line: l, start_column: 0, end_line: l, end_column: 1 }),
        source_file: None,
    }
}

/// `tools.x`'s wire from node `s`'s metadata port `tool`, into group `g`'s
/// input `x` (read off with `.spec`), then on to the member `g.m` reading
/// `self.x.name`.
fn into_group() -> Edge {
    let via = vec![crossing("g", "x", CrossingSide::In, &["spec"], Some(3))];
    Edge {
        id: crate::wires::wire_id("s", "tool", &via, "g.m", "in", &["spec".into(), "name".into()]),
        source: "s".into(),
        target: "g.m".into(),
        source_handle: Some("tool".into()),
        target_handle: Some("in".into()),
        path: vec!["spec".into(), "name".into()],
        via,
        span: Some(Span { start_line: 7, start_column: 0, end_line: 7, end_column: 1 }),
        source_file: None,
    }
}

#[test]
fn the_keys_read_up_to_a_crossing_are_its_legs_and_the_ones_before() {
    let edge = into_group();
    assert_eq!(keys_through(&edge, 0), vec!["spec".to_string()]);
    assert_eq!(keys_through(&edge, edge.via.len()), vec!["spec".to_string(), "name".to_string()]);
}

#[test]
fn the_line_of_a_crossing_is_the_leg_that_ends_there() {
    let edge = into_group();
    assert_eq!(line_at(&edge, 0).0.start_line, 3);
    assert_eq!(line_at(&edge, edge.via.len()).0.start_line, 7);
}

#[test]
fn a_wire_cut_at_a_group_input_ends_on_that_port() {
    let edge = into_group();
    let cut = cut_at(&edge, 0);
    assert_eq!((cut.target.as_str(), cut.target_port()), ("g", "x"));
    assert!(cut.to_group_port(), "the editor draws it up to the group's face");
    assert_eq!(cut.path, vec!["spec".to_string()]);
    assert_eq!(cut.span.map(|s| s.start_line), Some(3));
    assert_ne!(cut.id, edge.id);
}

#[test]
fn past_a_group_input_the_wire_is_sent_from_that_port() {
    let edge = into_group();
    let rest = sent_from(&edge, 0).expect("a member reads the port");
    assert_eq!(rest.source, "g");
    assert_eq!(rest.source_port(), "x");
    assert!(rest.from_group_port());
    assert_eq!(rest.via.len(), 1);
    assert!(rest.via[0].span.is_none() && rest.via[0].path.is_empty());
    // Only the keys read past the port: the value written there is
    // already `spec`.
    assert_eq!(rest.path, vec!["name".to_string()]);
    assert_eq!((rest.target.as_str(), rest.target_port()), ("g.m", "in"));
}

#[test]
fn a_group_input_nothing_reads_sends_nothing() {
    let via = vec![crossing("g", "x", CrossingSide::In, &[], Some(3))];
    let edge = Edge {
        id: crate::wires::wire_id("s", "tool", &via, "g", "x", &[]),
        source: "s".into(),
        target: "g".into(),
        source_handle: Some("tool".into()),
        target_handle: Some("x".into()),
        path: Vec::new(),
        via,
        span: None,
        source_file: None,
    };
    assert!(sent_from(&edge, 0).is_none());
}

mod written {
    use serde_json::json;
    use weft_core::project::{MetadataExpr, MetadataField, MetadataItem};

    use crate::metadata::parse_written;

    fn port(source: &str, port: &str, path: &[&str]) -> MetadataExpr {
        MetadataExpr::Port { source: source.into(), port: port.into(), path: path.iter().map(|k| k.to_string()).collect() }
    }

    #[test]
    fn a_value_that_names_no_port_is_one_literal() {
        assert_eq!(
            parse_written("{ \"name\": \"x\", \"tags\": [1, true, null, -2.5e3] }").unwrap(),
            MetadataExpr::Literal { value: json!({ "name": "x", "tags": [1, true, null, -2500.0] }) }
        );
    }

    #[test]
    fn a_dotted_name_is_a_port_and_three_dots_splice_a_list() {
        let written = parse_written(
            "{\n  \"anthropic\": [...screen.tools.anthropic, pause.tool, { \"name\": \"x\" }],\n  \"one\": inner.tool\n}",
        )
        .unwrap();
        assert_eq!(
            written,
            MetadataExpr::Object {
                fields: vec![
                    MetadataField {
                        key: "anthropic".into(),
                        value: MetadataExpr::List {
                            items: vec![
                                MetadataItem { spread: true, value: port("screen", "tools", &["anthropic"]) },
                                MetadataItem { spread: false, value: port("pause", "tool", &[]) },
                                MetadataItem { spread: false, value: MetadataExpr::Literal { value: json!({ "name": "x" }) } },
                            ],
                        },
                    },
                    MetadataField { key: "one".into(), value: port("inner", "tool", &[]) },
                ],
            }
        );
    }

    #[test]
    fn what_does_not_read_says_where() {
        for (text, says) in [
            ("[a.b c.d]", "`,` or `]`"),
            ("{ name: \"x\" }", "a quoted key"),
            ("{ \"a\": 1, \"a\": 2 }", "written twice"),
            ("[tool]", "`tool` is no value"),
            ("[\"open", "not closed"),
            ("[...a.b", "the value ended"),
            ("[a.b,]", "a value"),
            ("{ \"a\": a.b, }", "a quoted key"),
            ("[a.b # a comment\n]", "`,` or `]`"),
        ] {
            let why = parse_written(text).expect_err(text);
            assert!(why.contains(says), "{text}: {why}");
        }
    }

    #[test]
    fn a_written_value_evaluates_its_ports_and_splices() {
        let written = parse_written("{ \"all\": [...a.list, b.one], \"b\": b.one }").unwrap();
        let value = written.evaluate(
            &mut |source, _, _| Ok::<_, String>(if source == "a" { json!([1, 2]) } else { json!(3) }),
            |_, _| "not a list".to_string(),
        );
        assert_eq!(value, Ok(json!({ "all": [1, 2, 3], "b": 3 })));
        let not_a_list = parse_written("[...b.one]").unwrap().evaluate(&mut |_, _, _| Ok::<_, String>(json!(3)), |_, _| "not a list".to_string());
        assert_eq!(not_a_list, Err("not a list".to_string()));
    }
}
