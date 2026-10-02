//! `@per_instance` and `@instance_filled`: the two starting points, where each
//! may stand, and the slice the compiler grows from them along the wires.

use weft_catalog::{stdlib_root, FsCatalog};
use weft_compiler::enrich::enrich;
use weft_compiler::validate::validate;
use weft_compiler::weft_compiler::compile;
use weft_compiler::CompileFs;
use weft_core::instance::PerInstance;

fn catalog() -> FsCatalog {
    FsCatalog::discover(&stdlib_root().expect("stdlib root")).expect("stdlib catalog")
}

fn build(source: &str) -> weft_core::ProjectDefinition {
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile ok");
    enrich(&mut project, &catalog()).expect("enrich ok");
    project
}

fn per_instance(project: &weft_core::ProjectDefinition, id: &str) -> Option<PerInstance> {
    project.nodes.iter().find(|n| n.id == id).unwrap_or_else(|| panic!("no node {id}")).per_instance
}

fn codes(project: &weft_core::ProjectDefinition) -> Vec<String> {
    validate(project, &catalog()).into_iter().filter_map(|d| d.code).collect()
}

const BRIDGE: &str = r#"
bridge = BaileyBridge {
  @per_instance
}
receive = BaileyReceive
receive.bridge = bridge.bridge
reply = BaileySend { message: "hi" }
reply.bridge = bridge.bridge
reply.to = receive.from
log = Debug
log.data = receive.content
"#;

/// A trigger reading an instance's bridge is armed per instance, and
/// everything its runs touch runs in that instance's runs.
#[test]
fn the_slice_follows_the_wires() {
    let project = build(BRIDGE);
    assert_eq!(per_instance(&project, "bridge"), Some(PerInstance::Marked));
    for derived in ["receive", "reply", "log"] {
        assert_eq!(per_instance(&project, derived), Some(PerInstance::Derived), "{derived}");
    }
    assert!(!codes(&project).iter().any(|c| c.starts_with("per-instance")), "{:?}", codes(&project));
}

/// A shared part beside it stays shared: a cron on the shared database
/// is nobody's.
#[test]
fn a_shared_part_beside_it_stays_shared() {
    let project = build(&format!(
        "{BRIDGE}\npg = PostgresDatabase {{ database: \"app\" }}\nq = PostgresExecuteQuery {{ query: \"select 1\" }}\nq.account = pg.access\n"
    ));
    assert_eq!(per_instance(&project, "pg"), None);
    assert_eq!(per_instance(&project, "q"), None);
}

/// The slice crosses group boundaries: the boundary is an ordinary node
/// after flattening. An instance's connection starts one like an instance's
/// container does.
#[test]
fn the_slice_crosses_a_group() {
    let project = build(
        r#"
slack = SlackAccess { account: @instance_filled }
g = Group(access: Access) -> () {
  d = Debug
  d.data = self.access
}
g.access = slack.access
"#,
    );
    assert_eq!(per_instance(&project, "slack"), Some(PerInstance::Filled));
    assert_eq!(per_instance(&project, "g.d"), Some(PerInstance::Derived));
}

/// Only an infra node may carry the mark; an access step is pointed at
/// `@instance_filled` on its connection.
#[test]
fn only_an_infra_node_carries_the_mark() {
    let project = build("t = Text {\n  value: \"x\"\n  @per_instance\n}\n");
    assert!(codes(&project).contains(&"per-instance-ineligible".to_string()), "{:?}", codes(&project));
    let access = build("ws = SlackAccess {\n  @per_instance\n}\n");
    let found = validate(&access, &catalog());
    let refusal = found.iter().find(|d| d.code.as_deref() == Some("per-instance-ineligible")).expect("refused");
    assert!(refusal.message.contains("`account: @instance_filled`"), "{}", refusal.message);
}

/// A connection each instance provides needs no pick in the source, and the
/// "no connection picked" rule holds off: the instance's is checked when
/// the run belongs to it.
#[test]
fn an_instance_filled_connection_needs_no_pick() {
    use weft_compiler::validate::{validate_with_mode, ValidationMode};
    let project = build("ws = SlackAccess { account: @instance_filled }\n");
    let runtime = validate_with_mode(&project, &catalog(), ValidationMode::Runtime);
    assert!(runtime.is_empty(), "{runtime:?}");
}

/// The mark goes on a node, never a group, and takes no arguments.
#[test]
fn the_mark_is_refused_where_it_means_nothing() {
    let group = compile("g = Group() -> () {\n  @per_instance\n}\n", uuid::Uuid::new_v4(), CompileFs::none());
    assert!(format!("{:?}", group.unwrap_err()).contains("goes inside the braces of the node"));
    let args = compile("b = BaileyBridge {\n  @per_instance(x)\n}\n", uuid::Uuid::new_v4(), CompileFs::none());
    assert!(format!("{:?}", args.unwrap_err()).contains("takes no arguments"));
    let unknown = compile("b = BaileyBridge {\n  @per_instanc\n}\n", uuid::Uuid::new_v4(), CompileFs::none());
    assert!(format!("{:?}", unknown.unwrap_err()).contains("unknown directive '@per_instanc'"));
}

/// An instance-filled connection carries its service's recipe on the
/// definition, and every instance-filled node its rules: an instance is
/// connected and filled from its own page, and the program is all that
/// door has to read them from. A shared access step, and a step the slice
/// only reached, carry neither.
#[test]
fn an_instance_filled_node_carries_what_the_instance_door_reads() {
    let project = build(
        r#"
slack = SlackAccess { account: @instance_filled }
shared = SlackAccess
d = Debug
d.data = slack.access
"#,
    );
    let node = |id: &str| project.nodes.iter().find(|n| n.id == id).unwrap();
    assert_eq!(node("slack").instance_service.as_ref().map(|s| s.service.as_str()), Some("slack"));
    assert!(node("slack").instance_rules.as_ref().is_some_and(|r| !r.rules.is_empty()), "the connection rule rides along");
    for id in ["shared", "d"] {
        assert!(node(id).instance_service.is_none() && node(id).instance_rules.is_none(), "{id}");
    }
    let places: Vec<String> = weft_core::project::instance_filled_places(&project).into_iter().map(|(place, _)| place).collect();
    assert_eq!(places, vec!["slack".to_string()]);
}

/// Where the marker cannot stand, each refused on its own line: a wired
/// field, a group's own port, and a key that is not an input.
#[test]
fn the_marker_is_refused_where_no_instance_value_can_land() {
    let wired = build("t = Text { value: \"x\" }\nd = Debug { data: @instance_filled }\nd.data = t.value\n");
    assert!(codes(&wired).contains(&"instance-filled-wired".to_string()), "{:?}", codes(&wired));
    let boundary = build("g = Group(x: String) -> () {\n  d = Debug\n  d.data = self.x\n}\ng.x = @instance_filled\n");
    assert!(codes(&boundary).contains(&"instance-filled-boundary".to_string()), "{:?}", codes(&boundary));
    // A config key the node does not declare as an input: validate
    // refuses it, since no instance's value can reach it.
    let not_an_input = build("t = Text {\n  value: \"x\"\n  mystery: @instance_filled\n}\n");
    assert!(codes(&not_an_input).contains(&"instance-filled-not-an-input".to_string()), "{:?}", codes(&not_an_input));
    // A reserved key (`_tags`): the parse refuses it before validate runs.
    let reserved = compile("t = Text {\n  value: \"x\"\n  _tags: @instance_filled\n}\n", uuid::Uuid::new_v4(), CompileFs::none());
    assert!(format!("{:?}", reserved.unwrap_err()).contains("cannot be `@instance_filled`"));
}

/// The marker's shapes: bare, with a fallback, and the malformed ones.
#[test]
fn the_marker_takes_an_optional_fallback() {
    let project = build("c = Cron { cron: @instance_filled(\"0 0 3 * * *\") }\nm = Text { value: @instance_filled }\n");
    let literal = |id: &str, field: &str| project.nodes.iter().find(|n| n.id == id).unwrap().port_literals[field].clone();
    let cron = literal("c", "cron");
    let filled = weft_core::instance::as_instance_filled(&cron).expect("a marker");
    assert_eq!(filled.fallback, Some(&serde_json::json!("0 0 3 * * *")));
    assert_eq!(weft_core::instance::as_instance_filled(&literal("m", "value")).unwrap().fallback, None);
    for bad in ["@instance_filled()", "@instance_filled(@instance_filled)", "@instance_filled(@require_one_of(a))", "@instance_filled (\"x\")"] {
        let src = format!("m = Text {{ value: {bad} }}\n");
        assert!(compile(&src, uuid::Uuid::new_v4(), CompileFs::none()).is_err(), "{bad}");
    }
    let key = compile("m = Debug { data: {\"__weft_instance_filled__\": {}} }\n", uuid::Uuid::new_v4(), CompileFs::none());
    assert!(format!("{:?}", key.unwrap_err()).contains("key the language uses"));
}

/// A fallback may be a file, read the way a written `@file` is: the
/// compiled fallback holds the file's content, and the field keeps its
/// file record so the editor writes `@instance_filled(@file(...))` back.
/// An `@asset` fallback waits for the build like any written one.
#[test]
fn a_fallback_may_be_a_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(dir.path().join("prompts")).unwrap();
    std::fs::write(dir.path().join("prompts/default.md"), "Be kind.").unwrap();
    let source = "t = Text { value: @instance_filled(@file(\"prompts/default.md\")) }\n\
                  a = Text { value: @instance_filled(@asset(\"https://example.com/p.md\", String)) }\n";
    let mut project = compile(source, uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).expect("compile ok");
    enrich(&mut project, &catalog()).expect("enrich ok");
    let node = |id: &str| project.nodes.iter().find(|n| n.id == id).unwrap();
    let text = weft_core::instance::as_instance_filled(&node("t").port_literals["value"]).expect("a marker").fallback.cloned();
    assert_eq!(text, Some(serde_json::json!("Be kind.")));
    assert_eq!(node("t").file_refs["value"].path, "prompts/default.md");
    let asset = weft_core::instance::as_instance_filled(&node("a").port_literals["value"]).expect("a marker").fallback.cloned();
    assert_eq!(asset, Some(serde_json::json!("@asset(\"https://example.com/p.md\", String)")));
    let refs: Vec<String> = weft_compiler::file_ref::collect_text_refs(&project).into_iter().map(|r| r.path).collect();
    assert_eq!(refs, vec!["https://example.com/p.md".to_string()]);
    assert!(!codes(&project).iter().any(|c| c == "config-type-mismatch"), "{:?}", codes(&project));
}

/// A fallback is a value like any written one: its type and the node's
/// rules check it now. A bare marker's content waits for the instance.
#[test]
fn a_fallback_is_checked_now_and_a_bare_marker_later() {
    let bad = build("c = Cron { cron: @instance_filled(\"0 3 * * *\") }\n");
    let found = validate(&bad, &catalog());
    assert!(found.iter().any(|d| d.message.contains("needs six fields")), "{found:?}");
    let wrong_type = build("t = Text { value: @instance_filled(5) }\nr = Range { from: @instance_filled(\"x\") }\n");
    assert!(codes(&wrong_type).contains(&"config-type-mismatch".to_string()), "{:?}", codes(&wrong_type));
    let bare = build("c = Cron { cron: @instance_filled }\n");
    assert!(validate(&bare, &catalog()).iter().all(|d| !d.message.contains("six fields")));
    assert_eq!(per_instance(&bare, "c"), Some(PerInstance::Filled));
}

/// An instance's list field reads through the connection its run would use:
/// the wire into the widget's access input leads to the access node, whose
/// connection is the instance's value when it is `@instance_filled` and the
/// install's pick otherwise. Across a group's port, the same.
#[test]
fn a_list_field_reads_through_the_instances_connection() {
    use weft_core::frames::Located;
    use weft_core::instance::{lookup_connection, LookupConnection, InstanceValues};
    use weft_core::instance_door::FieldConnection;
    let project = build(
        r#"
google = GoogleAccess { account: @instance_filled }
read = GoogleSheetsRead { spreadsheet: @instance_filled }
read.account = google.access
"#,
    );
    let at = Located::top("read");
    // Not connected yet: the field listing shows no connection (so a new
    // instance's settings page still opens), the lookup refuses with why.
    let pending = lookup_connection(&project, &at, "spreadsheet", &InstanceValues::new(), &InstanceValues::new()).unwrap();
    assert!(matches!(&pending, LookupConnection::NotConnected(why) if why.contains("connect your account at 'google' first")), "{pending:?}");
    assert_eq!(pending.whose(), FieldConnection::None);
    let err = pending.signing().unwrap_err();
    assert!(err.contains("connect your account at 'google' first"), "{err}");
    let id = uuid::Uuid::new_v4();
    let values = InstanceValues::from([("google".to_string(), [("account".to_string(), serde_json::json!({ "id": id }))].into())]);
    let found = lookup_connection(&project, &at, "spreadsheet", &values, &InstanceValues::new()).unwrap().signing().unwrap().expect("a connection");
    assert_eq!((found.id, found.service.as_str()), (id, "google"));
    assert_eq!(found.whose, FieldConnection::Own);
    let project = build(
        "google = GoogleAccess\ng = Group(a: Access) -> () {\n  read = GoogleSheetsRead { spreadsheet: @instance_filled }\n  read.account = self.a\n}\ng.a = google.access\n",
    );
    let unpicked = lookup_connection(&project, &Located::top("g.read"), "spreadsheet", &InstanceValues::new(), &InstanceValues::new()).unwrap();
    assert!(matches!(&unpicked, LookupConnection::NotConnected(why) if why.contains("picked on this install")), "{unpicked:?}");
    let shared = uuid::Uuid::new_v4();
    let picks = InstanceValues::from([("google".to_string(), [("account".to_string(), serde_json::json!({ "id": shared }))].into())]);
    let found = lookup_connection(&project, &Located::top("g.read"), "spreadsheet", &InstanceValues::new(), &picks)
        .unwrap()
        .signing()
        .unwrap()
        .expect("the shared one");
    assert_eq!(found.id, shared);
    assert_eq!(found.whose, FieldConnection::Shared);
    assert!(lookup_connection(&project, &Located::top("g.read"), "hasHeader", &InstanceValues::new(), &InstanceValues::new()).is_err());
}
