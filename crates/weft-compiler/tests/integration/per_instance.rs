//! `@per_instance` and `@instance_filled`: the two starting points, where each
//! may stand, and the slice the compiler grows from them along the wires.

use weft_catalog::{stdlib_root, FsCatalog};
use weft_compiler::validate::validate;
use crate::rig::{compile, enrich};
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

/// The slice crosses a group: the wire runs through the group's port to
/// the member that reads it. An instance's connection starts one like an
/// instance's container does.
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

/// A loop's gate lands on its `LoopIn`, which launches every iteration:
/// a gate reading an instance's value makes the whole inside per
/// instance, a member no wire feeds included.
#[test]
fn the_slice_reaches_a_loop_through_its_gate() {
    let project = build(
        r#"
slack = SlackAccess { account: @instance_filled }
l = Loop(items: List[String]) -> (results: List[String | Null]) {
  over: ["items"]
  t = Text { value: "x" }
  self.results = t.value
}
l.items = ["a"]
l._should_flow = slack.access
"#,
    );
    assert_eq!(per_instance(&project, "l.t"), Some(PerInstance::Derived));
}

/// Only an infra node may carry the mark; an access node is pointed at
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
/// door has to read them from. A shared access node, and a node the slice
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
    let places: Vec<String> = weft_core::project::instance_filled_places(&project).into_iter().map(|filled| filled.spelled).collect();
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

/// One file included at two sites: what one site feeds it is that site's
/// alone, so the per-instance slice is too.
const CARRY: &str = r#"
Group(text: String) -> (out: String) {
  carry = ExecPython(text: String) -> (out: String, parts: List[String]) { code: "x" }
  carry.text = self.text
  each = Loop(items: List[String]) -> (marks: List[String | Null]) {
    over: ["items"]
    mark = ExecPython(c: String) -> (out: String) { code: "x" }
    mark.c = self.items
    self.marks = mark.out
  }
  each.items = carry.parts
  self.out = carry.out
}
"#;

fn build_with(main: &str, files: &[(&str, &str)]) -> weft_core::ProjectDefinition {
    let dir = tempfile::tempdir().expect("tempdir");
    for (path, text) in files {
        let path = dir.path().join(path);
        std::fs::create_dir_all(path.parent().expect("a folder")).unwrap();
        std::fs::write(path, text).unwrap();
    }
    let mut project = compile(main, uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).expect("compile ok");
    enrich(&mut project, &catalog()).expect("enrich ok");
    project
}

/// The node of an included body whose id ends in `.{local}`.
fn body_node<'a>(project: &'a weft_core::ProjectDefinition, local: &str) -> &'a weft_core::project::NodeDefinition {
    let suffix = format!(".{local}");
    project.nodes.iter().find(|n| n.id.ends_with(&suffix)).unwrap_or_else(|| panic!("no body node {local}"))
}

/// The outage this guards: a per-instance part (a bridge per customer)
/// added to a group that includes a file the program's shared path
/// includes too. The file's nodes are per instance under the group's site
/// and shared under the shared one, so the shared trigger's runs reach
/// nothing per instance and are never refused, and the program builds.
#[test]
fn an_included_file_is_per_instance_only_where_an_instance_reaches_it() {
    let project = build_with(
        r#"
tick = Cron { cron: "0 0 3 * * *" }
shared = @include("lib/carry.weft")
shared.text = tick.scheduledTime
space = Group() -> () {
  bridge = BaileyBridge {
    @per_instance
  }
  receive = BaileyReceive
  receive.bridge = bridge.bridge
  mine = @include("lib/carry.weft")
  mine.text = receive.content
}
"#,
        &[("lib/carry.weft", CARRY)],
    );
    for local in ["carry", "mark"] {
        let node = body_node(&project, local);
        assert_eq!(node.per_instance, None, "{local} is not per instance everywhere");
        assert_eq!(node.per_instance_at(&["shared"]), None, "{local} under the shared site");
        assert_eq!(node.per_instance_at(&["space.mine"]), Some(PerInstance::Derived), "{local} under the group's site");
        assert!(node.per_instance_somewhere());
    }
    assert!(!codes(&project).iter().any(|c| c == "shared-trigger-reaches-per-instance"), "{:?}", codes(&project));
    assert!(weft_core::run_spec::instanceless_fire_reaches(&project, "tick", &weft_core::run_spec::instanceless_stand_ins(&project)).expect("cut").is_empty());
    let fire = weft_core::run_spec::compute_trigger_fire(&project, "tick", &serde_json::json!({}), None, &Default::default())
        .expect("the fire's cut");
    assert!(weft_core::run_spec::refuse_instanceless(&fire.program, None).is_ok());
}

/// Nested: a file that includes another, itself included at a shared
/// site and a per-instance one. The inner file's nodes follow the outer
/// site they run under.
#[test]
fn a_nested_include_follows_the_outer_site() {
    let outer = r#"
Group(text: String) -> (out: String) {
  inner = @include("carry.weft")
  inner.text = self.text
  self.out = inner.out
}
"#;
    let project = build_with(
        r#"
tick = Cron { cron: "0 0 3 * * *" }
shared = @include("lib/outer.weft")
shared.text = tick.scheduledTime
bridge = BaileyBridge {
  @per_instance
}
receive = BaileyReceive
receive.bridge = bridge.bridge
mine = @include("lib/outer.weft")
mine.text = receive.content
"#,
        &[("lib/outer.weft", outer), ("lib/carry.weft", CARRY)],
    );
    let inner_site = project
        .groups
        .iter()
        .find(|g| matches!(g.kind, weft_core::project::GroupKind::Call { .. }) && g.id.ends_with(".inner"))
        .map(|g| g.id.clone())
        .expect("the inner site");
    let carry = body_node(&project, "carry");
    assert_eq!(carry.per_instance_at(&["shared".to_string(), inner_site.clone()]), None);
    assert_eq!(carry.per_instance_at(&["mine".to_string(), inner_site]), Some(PerInstance::Derived));
    assert!(!codes(&project).iter().any(|c| c == "shared-trigger-reaches-per-instance"), "{:?}", codes(&project));
}

/// A file every site of which reads an instance's value is per instance
/// everywhere: the one mark, no list of places.
#[test]
fn a_file_per_instance_at_every_site_is_marked_once() {
    let project = build_with(
        r#"
bridge = BaileyBridge {
  @per_instance
}
receive = BaileyReceive
receive.bridge = bridge.bridge
one = @include("lib/carry.weft")
one.text = receive.content
two = @include("lib/carry.weft")
two.text = receive.pushName
"#,
        &[("lib/carry.weft", CARRY)],
    );
    let carry = body_node(&project, "carry");
    assert_eq!(carry.per_instance, Some(PerInstance::Derived));
    assert!(carry.per_instance_paths.is_empty(), "{:?}", carry.per_instance_paths);
}

/// A shared trigger wired straight into what is per instance has no
/// instance to run for: the build refuses it, naming the trigger, what it
/// reaches and why that is per instance.
#[test]
fn a_shared_trigger_reaching_per_instance_places_fails_the_build() {
    let project = build(
        r#"
bridge = BaileyBridge {
  @per_instance
}
receive = BaileyReceive
receive.bridge = bridge.bridge
tick = Cron { cron: "0 0 3 * * *" }
send = BaileySend { message: "hi", to: "x" }
send.bridge = bridge.bridge
send._should_flow = tick.scheduledTime
"#,
    );
    let found = validate(&project, &catalog());
    let refusal = found
        .iter()
        .find(|d| d.code.as_deref() == Some("shared-trigger-reaches-per-instance"))
        .unwrap_or_else(|| panic!("refused: {found:?}"));
    for part in ["Trigger 'tick'", "'send'", "'bridge'", "it reads 'bridge'", "marked `@per_instance`"] {
        assert!(refusal.message.contains(part), "{part}: {}", refusal.message);
    }
    // The per-instance trigger beside it fires for its own instance.
    assert_eq!(found.iter().filter(|d| d.code.as_deref() == Some("shared-trigger-reaches-per-instance")).count(), 1);
}

/// A route names its instance per call (an instance token, or the
/// `Weft-Instance` header on a gated route), so a shared route reaching
/// what is per instance builds: a call naming no instance is refused at
/// run time instead.
#[test]
fn a_shared_route_reaching_per_instance_places_builds() {
    let project = build(
        r#"
bridge = BaileyBridge {
  @per_instance
}
door = Route { path: "send", method: "POST" }
send = BaileySend { message: "hi", to: "x" }
send.bridge = bridge.bridge
send._should_flow = door.fired
reply = Reply
reply.body = send.messageId
"#,
    );
    assert!(!codes(&project).iter().any(|c| c == "shared-trigger-reaches-per-instance"), "{:?}", codes(&project));
    assert!(!weft_core::run_spec::instanceless_fire_reaches(&project, "door", &weft_core::run_spec::instanceless_stand_ins(&project)).expect("cut").is_empty());
}

/// Why a place is per instance is told from that place alone: a file
/// included twice, each include fed by its own per-instance bridge, names
/// its own include's bridge and never the other's.
#[test]
fn each_place_tells_its_own_reason() {
    let project = build_with(
        r#"
first = BaileyBridge {
  @per_instance
}
hearFirst = BaileyReceive
hearFirst.bridge = first.bridge
one = @include("lib/carry.weft")
one.text = hearFirst.content
second = BaileyBridge {
  @per_instance
}
hearSecond = BaileyReceive
hearSecond.bridge = second.bridge
two = @include("lib/carry.weft")
two.text = hearSecond.content
"#,
        &[("lib/carry.weft", CARRY)],
    );
    let carry = body_node(&project, "carry");
    let rules = weft_core::rules::RuleContext::new(&project);
    let at = |site: &str| {
        weft_core::rules::per_instance_reasons_at(&weft_core::frames::Located::new(carry.id.clone(), vec![site.to_string()]), &rules)
    };
    assert_eq!(at("one"), vec!["it reads 'first'".to_string()]);
    assert_eq!(at("two"), vec!["it reads 'second'".to_string()]);
    // Asked of the node as a whole, each reason names its include, once.
    assert_eq!(
        weft_core::rules::per_instance_reasons(carry, &rules),
        vec!["at 'one.carry', it reads 'first'".to_string(), "at 'two.carry', it reads 'second'".to_string()],
    );
}

/// A value deciding a group's gate makes what is inside per instance,
/// and what reads it from outside: each told the way it runs.
#[test]
fn a_gate_is_told_as_a_gate_and_what_reads_past_it_as_a_read() {
    let project = build(
        r#"
bridge = BaileyBridge {
  @per_instance
}
receive = BaileyReceive
receive.bridge = bridge.bridge
g = Group() -> (out: String) {
  m = Text { value: "x" }
  self.out = m.value
}
g._should_flow = receive.isGroup
d = Debug
d.data = g.out
"#,
    );
    let rules = weft_core::rules::RuleContext::new(&project);
    let at = |id: &str| weft_core::rules::per_instance_reasons_at(&weft_core::frames::Located::top(id), &rules);
    assert_eq!(at("g.m"), vec!["it sits inside group 'g', whose gate reads 'bridge'".to_string()]);
    assert_eq!(at("d"), vec!["it reads 'g.m'".to_string()]);
    assert_eq!(at("bridge"), vec!["it is marked `@per_instance`".to_string()]);
}
