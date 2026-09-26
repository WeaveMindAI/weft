//! `@per_member` and `@member_filled`: the two starting points, where each
//! may stand, and the slice the compiler grows from them along the wires.

use weft_catalog::{stdlib_root, FsCatalog};
use weft_compiler::enrich::enrich;
use weft_compiler::validate::validate;
use weft_compiler::weft_compiler::compile;
use weft_compiler::CompileFs;
use weft_core::member::PerMember;

fn catalog() -> FsCatalog {
    FsCatalog::discover(&stdlib_root().expect("stdlib root")).expect("stdlib catalog")
}

fn build(source: &str) -> weft_core::ProjectDefinition {
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile ok");
    enrich(&mut project, &catalog()).expect("enrich ok");
    project
}

fn per_member(project: &weft_core::ProjectDefinition, id: &str) -> Option<PerMember> {
    project.nodes.iter().find(|n| n.id == id).unwrap_or_else(|| panic!("no node {id}")).per_member
}

fn codes(project: &weft_core::ProjectDefinition) -> Vec<String> {
    validate(project, &catalog()).into_iter().filter_map(|d| d.code).collect()
}

const BRIDGE: &str = r#"
bridge = BaileyBridge {
  @per_member
}
receive = BaileyReceive
receive.endpointUrl = bridge.endpointUrl
reply = BaileySend { message: "hi" }
reply.endpointUrl = bridge.endpointUrl
reply.to = receive.from
log = Debug
log.data = receive.content
"#;

/// A trigger reading a member's bridge is armed per member, and
/// everything its runs touch runs in that member's runs.
#[test]
fn the_slice_follows_the_wires() {
    let project = build(BRIDGE);
    assert_eq!(per_member(&project, "bridge"), Some(PerMember::Marked));
    for derived in ["receive", "reply", "log"] {
        assert_eq!(per_member(&project, derived), Some(PerMember::Derived), "{derived}");
    }
    assert!(!codes(&project).iter().any(|c| c.starts_with("per-member")), "{:?}", codes(&project));
}

/// A shared part beside it stays shared: a cron on the shared database
/// is nobody's.
#[test]
fn a_shared_part_beside_it_stays_shared() {
    let project = build(&format!(
        "{BRIDGE}\npg = PostgresDatabase {{ database: \"app\" }}\nq = PostgresExecuteQuery {{ query: \"select 1\" }}\nq.account = pg.access\n"
    ));
    assert_eq!(per_member(&project, "pg"), None);
    assert_eq!(per_member(&project, "q"), None);
}

/// The slice crosses group boundaries: the boundary is an ordinary node
/// after flattening. A member's connection starts one like a member's
/// container does.
#[test]
fn the_slice_crosses_a_group() {
    let project = build(
        r#"
slack = SlackAccess { account: @member_filled }
g = Group(access: Access) -> () {
  d = Debug
  d.data = self.access
}
g.access = slack.access
"#,
    );
    assert_eq!(per_member(&project, "slack"), Some(PerMember::Filled));
    assert_eq!(per_member(&project, "g.d"), Some(PerMember::Derived));
}

/// Only an infra node may carry the mark; an access step is pointed at
/// `@member_filled` on its connection.
#[test]
fn only_an_infra_node_carries_the_mark() {
    let project = build("t = Text {\n  value: \"x\"\n  @per_member\n}\n");
    assert!(codes(&project).contains(&"per-member-ineligible".to_string()), "{:?}", codes(&project));
    let access = build("ws = SlackAccess {\n  @per_member\n}\n");
    let found = validate(&access, &catalog());
    let refusal = found.iter().find(|d| d.code.as_deref() == Some("per-member-ineligible")).expect("refused");
    assert!(refusal.message.contains("`account: @member_filled`"), "{}", refusal.message);
}

/// A connection each member provides needs no pick in the source, and the
/// "no connection picked" rule holds off: the member's is checked when
/// the run is for them.
#[test]
fn a_member_filled_connection_needs_no_pick() {
    use weft_compiler::validate::{validate_with_mode, ValidationMode};
    let project = build("ws = SlackAccess { account: @member_filled }\n");
    let runtime = validate_with_mode(&project, &catalog(), ValidationMode::Runtime);
    assert!(runtime.is_empty(), "{runtime:?}");
}

/// The mark goes on a node, never a group, and takes no arguments.
#[test]
fn the_mark_is_refused_where_it_means_nothing() {
    let group = compile("g = Group() -> () {\n  @per_member\n}\n", uuid::Uuid::new_v4(), CompileFs::none());
    assert!(format!("{:?}", group.unwrap_err()).contains("goes inside the braces of the node"));
    let args = compile("b = BaileyBridge {\n  @per_member(x)\n}\n", uuid::Uuid::new_v4(), CompileFs::none());
    assert!(format!("{:?}", args.unwrap_err()).contains("takes no arguments"));
    let unknown = compile("b = BaileyBridge {\n  @per_membr\n}\n", uuid::Uuid::new_v4(), CompileFs::none());
    assert!(format!("{:?}", unknown.unwrap_err()).contains("unknown directive '@per_membr'"));
}

/// A member-filled connection carries its service's recipe on the
/// definition, and every member-filled node its rules: a member connects
/// and fills from their own page, and the program is all that door has to
/// read them from. A shared access step, and a step the slice only
/// reached, carry neither.
#[test]
fn a_member_filled_node_carries_what_the_member_door_reads() {
    let project = build(
        r#"
slack = SlackAccess { account: @member_filled }
shared = SlackAccess
d = Debug
d.data = slack.access
"#,
    );
    let node = |id: &str| project.nodes.iter().find(|n| n.id == id).unwrap();
    assert_eq!(node("slack").member_service.as_ref().map(|s| s.service.as_str()), Some("slack"));
    assert!(node("slack").member_rules.as_ref().is_some_and(|r| !r.rules.is_empty()), "the connection rule rides along");
    for id in ["shared", "d"] {
        assert!(node(id).member_service.is_none() && node(id).member_rules.is_none(), "{id}");
    }
    let places: Vec<String> = weft_core::project::member_filled_places(&project).into_iter().map(|(place, _)| place).collect();
    assert_eq!(places, vec!["slack".to_string()]);
}

/// Where the marker cannot stand, each refused on its own line: a wired
/// field, a group's own port, and a key that is not an input.
#[test]
fn the_marker_is_refused_where_no_member_value_can_land() {
    let wired = build("t = Text { value: \"x\" }\nd = Debug { data: @member_filled }\nd.data = t.value\n");
    assert!(codes(&wired).contains(&"member-filled-wired".to_string()), "{:?}", codes(&wired));
    let boundary = build("g = Group(x: String) -> () {\n  d = Debug\n  d.data = self.x\n}\ng.x = @member_filled\n");
    assert!(codes(&boundary).contains(&"member-filled-boundary".to_string()), "{:?}", codes(&boundary));
    // A config key the node does not declare as an input: validate
    // refuses it, since no member's value can reach it.
    let not_an_input = build("t = Text {\n  value: \"x\"\n  mystery: @member_filled\n}\n");
    assert!(codes(&not_an_input).contains(&"member-filled-not-an-input".to_string()), "{:?}", codes(&not_an_input));
    // A reserved key (`_tags`): the parse refuses it before validate runs.
    let reserved = compile("t = Text {\n  value: \"x\"\n  _tags: @member_filled\n}\n", uuid::Uuid::new_v4(), CompileFs::none());
    assert!(format!("{:?}", reserved.unwrap_err()).contains("cannot be `@member_filled`"));
}

/// The marker's shapes: bare, with a fallback, and the malformed ones.
#[test]
fn the_marker_takes_an_optional_fallback() {
    let project = build("c = Cron { cron: @member_filled(\"0 0 3 * * *\") }\nm = Text { value: @member_filled }\n");
    let literal = |id: &str, field: &str| project.nodes.iter().find(|n| n.id == id).unwrap().port_literals[field].clone();
    let cron = literal("c", "cron");
    let filled = weft_core::member::as_member_filled(&cron).expect("a marker");
    assert_eq!(filled.fallback, Some(&serde_json::json!("0 0 3 * * *")));
    assert_eq!(weft_core::member::as_member_filled(&literal("m", "value")).unwrap().fallback, None);
    for bad in ["@member_filled()", "@member_filled(@member_filled)", "@member_filled(@require_one_of(a))", "@member_filled (\"x\")"] {
        let src = format!("m = Text {{ value: {bad} }}\n");
        assert!(compile(&src, uuid::Uuid::new_v4(), CompileFs::none()).is_err(), "{bad}");
    }
    let key = compile("m = Debug { data: {\"__weft_member_filled__\": {}} }\n", uuid::Uuid::new_v4(), CompileFs::none());
    assert!(format!("{:?}", key.unwrap_err()).contains("key the language uses"));
}

/// A fallback may be a file, read the way a written `@file` is: the
/// compiled fallback holds the file's content, and the field keeps its
/// file record so the editor writes `@member_filled(@file(...))` back.
/// An `@asset` fallback waits for the build like any written one.
#[test]
fn a_fallback_may_be_a_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(dir.path().join("prompts")).unwrap();
    std::fs::write(dir.path().join("prompts/default.md"), "Be kind.").unwrap();
    let source = "t = Text { value: @member_filled(@file(\"prompts/default.md\")) }\n\
                  a = Text { value: @member_filled(@asset(\"https://example.com/p.md\", String)) }\n";
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).expect("compile ok");
    enrich(&mut project, &catalog()).expect("enrich ok");
    let node = |id: &str| project.nodes.iter().find(|n| n.id == id).unwrap();
    let text = weft_core::member::as_member_filled(&node("t").port_literals["value"]).expect("a marker").fallback.cloned();
    assert_eq!(text, Some(serde_json::json!("Be kind.")));
    assert_eq!(node("t").file_refs["value"].path, "prompts/default.md");
    let asset = weft_core::member::as_member_filled(&node("a").port_literals["value"]).expect("a marker").fallback.cloned();
    assert_eq!(asset, Some(serde_json::json!("@asset(\"https://example.com/p.md\", String)")));
    let refs: Vec<String> = weft_compiler::file_ref::collect_text_refs(&project).into_iter().map(|r| r.path).collect();
    assert_eq!(refs, vec!["https://example.com/p.md".to_string()]);
    assert!(!codes(&project).iter().any(|c| c == "config-type-mismatch"), "{:?}", codes(&project));
}

/// A fallback is a value like any written one: its type and the node's
/// rules check it now. A bare marker's content waits for the member.
#[test]
fn a_fallback_is_checked_now_and_a_bare_marker_later() {
    let bad = build("c = Cron { cron: @member_filled(\"0 3 * * *\") }\n");
    let found = validate(&bad, &catalog());
    assert!(found.iter().any(|d| d.message.contains("needs six fields")), "{found:?}");
    let wrong_type = build("t = Text { value: @member_filled(5) }\nr = Range { from: @member_filled(\"x\") }\n");
    assert!(codes(&wrong_type).contains(&"config-type-mismatch".to_string()), "{:?}", codes(&wrong_type));
    let bare = build("c = Cron { cron: @member_filled }\n");
    assert!(validate(&bare, &catalog()).iter().all(|d| !d.message.contains("six fields")));
    assert_eq!(per_member(&bare, "c"), Some(PerMember::Filled));
}

/// A member's list field reads through the connection its run would use:
/// the wire into the widget's access input leads to the access node, whose
/// connection is the member's value when it is `@member_filled` and the
/// written one otherwise. Across a group's port, the same.
#[test]
fn a_list_field_reads_through_the_members_connection() {
    use weft_core::frames::Located;
    use weft_core::member::{lookup_connection, LookupConnection, MemberValues};
    use weft_core::member_door::FieldConnection;
    let project = build(
        r#"
google = GoogleAccess { account: @member_filled }
read = GoogleSheetsRead { spreadsheet: @member_filled }
read.account = google.access
"#,
    );
    let at = Located::top("read");
    // Not connected yet: the field listing shows no connection (so a new
    // member's settings page still opens), the lookup refuses with why.
    let pending = lookup_connection(&project, &at, "spreadsheet", &MemberValues::new()).unwrap();
    assert!(matches!(&pending, LookupConnection::NotConnected(why) if why.contains("connect your account at 'google' first")), "{pending:?}");
    assert_eq!(pending.whose(), FieldConnection::None);
    let err = pending.signing().unwrap_err();
    assert!(err.contains("connect your account at 'google' first"), "{err}");
    let id = uuid::Uuid::new_v4();
    let values = MemberValues::from([("google".to_string(), [("account".to_string(), serde_json::json!({ "id": id }))].into())]);
    let found = lookup_connection(&project, &at, "spreadsheet", &values).unwrap().signing().unwrap().expect("a connection");
    assert_eq!((found.id, found.service.as_str()), (id, "google"));
    assert_eq!(found.whose, FieldConnection::Own);
    let shared = uuid::Uuid::new_v4();
    let project = build(&format!(
        "google = GoogleAccess {{ account: {{\"id\": \"{shared}\"}} }}\ng = Group(a: Access) -> () {{\n  read = GoogleSheetsRead {{ spreadsheet: @member_filled }}\n  read.account = self.a\n}}\ng.a = google.access\n"
    ));
    let found = lookup_connection(&project, &Located::top("g.read"), "spreadsheet", &MemberValues::new())
        .unwrap()
        .signing()
        .unwrap()
        .expect("the shared one");
    assert_eq!(found.id, shared);
    assert_eq!(found.whose, FieldConnection::Shared);
    assert!(lookup_connection(&project, &Located::top("g.read"), "hasHeader", &MemberValues::new()).is_err());
}
