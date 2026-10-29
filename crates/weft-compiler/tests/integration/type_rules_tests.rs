//! The rules about what a node returns and when it runs, on real
//! programs against the stdlib: bounded type parameters (on a node's
//! metadata, a group's header and an inline node), a dict keyed by text
//! only, open records, narrowable outputs, placeholders held to inputs,
//! a route's added ports read as its captures, the gate a node with only
//! optional inputs needs, and the words `file` and `asset` as plain
//! values.

use weft_catalog::{stdlib_root, FsCatalog};
use weft_compiler::enrich::enrich_collecting;
use weft_compiler::enrich::EnrichPolicy;
use weft_compiler::validate::validate;
use weft_compiler::weft_compiler::IncludeMode;
use crate::rig::compile_with_mode;
use weft_compiler::{CompileFs, Diagnostic};
use weft_core::weft_type::WeftType;

fn catalog() -> FsCatalog {
    FsCatalog::discover(&stdlib_root().expect("stdlib root")).expect("stdlib catalog")
}

/// Compile, enrich and validate `source` read from `fs`: every message
/// any stage produced, with the project.
fn check_in(source: &str, fs: CompileFs) -> (weft_core::ProjectDefinition, Vec<String>, Vec<Diagnostic>) {
    let catalog = catalog();
    let mut project = match compile_with_mode(source, uuid::Uuid::new_v4(), fs, IncludeMode::Full, None) {
        Ok(project) => project,
        Err(errors) => panic!("compile failed: {errors:?}"),
    };
    let enrich_errors = catalog
        .type_registry()
        .scoped(|| enrich_collecting(&mut project, &catalog, EnrichPolicy::Strict))
        .into_iter()
        .map(|e| e.message)
        .collect();
    let diagnostics = validate(&project, &catalog);
    (project, enrich_errors, diagnostics)
}

fn check(source: &str) -> (weft_core::ProjectDefinition, Vec<String>, Vec<Diagnostic>) {
    check_in(source, CompileFs::none())
}

/// The error codes validate reported.
fn codes(diagnostics: &[Diagnostic]) -> Vec<&str> {
    diagnostics.iter().filter(|d| d.severity == weft_compiler::Severity::Error).filter_map(|d| d.code.as_deref()).collect()
}

fn message_of<'a>(diagnostics: &'a [Diagnostic], code: &str) -> &'a str {
    diagnostics
        .iter()
        .find(|d| d.code.as_deref() == Some(code))
        .map(|d| d.message.as_str())
        .unwrap_or_else(|| panic!("no {code} in {diagnostics:?}"))
}

fn output_type(project: &weft_core::ProjectDefinition, node: &str, port: &str) -> WeftType {
    let node = project.nodes.iter().find(|n| n.id == node).unwrap_or_else(|| panic!("node {node}"));
    node.outputs.iter().find(|p| p.name == port).unwrap_or_else(|| panic!("port {port}")).port_type.clone()
}

/// The type of a group's out port, as enrich resolved it.
fn group_out(project: &weft_core::ProjectDefinition, group: &str, port: &str) -> WeftType {
    let group = project.groups.iter().find(|g| g.id == group).unwrap_or_else(|| panic!("group {group}"));
    group.out_ports.iter().find(|p| p.name == port).unwrap_or_else(|| panic!("port {port}")).port_type.clone()
}

fn ty(text: &str) -> WeftType {
    WeftType::try_parse(text).unwrap_or_else(|why| panic!("`{text}`: {why}"))
}

// ── Bounded type parameters ────────────────────────────────────────────

/// `KeepFile` is `T: File` in and out: a Video kept is a Video, and a
/// String is refused naming the bound.
#[test]
fn a_bounded_node_keeps_the_kind_it_was_given_and_refuses_what_its_bound_does_not_take() {
    let (project, enrich_errors, d) = check(
        r#"
src = ExecPython() -> (clip: Video) { code: "return {}" }
keep = KeepFile
keep.file = src.clip
"#,
    );
    assert!(enrich_errors.is_empty(), "{enrich_errors:?}");
    assert!(!codes(&d).contains(&"type-bound"), "{d:?}");
    assert_eq!(output_type(&project, "keep", "file"), ty("Video"));

    let (_, _, d) = check(
        r#"
src = ExecPython() -> (name: String) { code: "return {}" }
keep = KeepFile
keep.file = src.name
"#,
    );
    let message = message_of(&d, "type-bound");
    assert!(message.contains("`T` has to fit in File") && message.contains("String"), "{message}");
}

/// A constant pins a variable the way a wire does: a picture written as
/// an `@asset` makes `T` an Image.
#[test]
fn a_written_constant_pins_a_type_variable() {
    let (project, enrich_errors, d) = check(
        r#"
keep = KeepFile { file: @asset("cat.png", Image) }
show = Debug
show.data = keep.file
"#,
    );
    assert!(enrich_errors.is_empty(), "{enrich_errors:?}");
    assert!(!codes(&d).contains(&"unresolved-typevar"), "{d:?}");
    assert_eq!(output_type(&project, "keep", "file"), ty("Image"));
}

/// An included file's type variables are worked out per place it is
/// included: one site feeds an Image, another a Video, and each keeps
/// its type. The file's nodes stay one copy each and carry the types of
/// every place.
#[test]
fn an_included_files_type_variables_resolve_per_place() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir(dir.path().join("lib")).expect("mkdir");
    std::fs::write(
        dir.path().join("lib/pass.weft"),
        "Group[T: Media](file: T) -> (out: T) {\n  keep = KeepFile\n  keep.file = self.file\n  self.out = keep.file\n}\n",
    )
    .expect("write");
    let source = r#"
pic = ExecPython() -> (a: Image, b: Video) { code: "return {}" }
img = @include("lib/pass.weft")
img.file = pic.a
vid = @include("lib/pass.weft")
vid.file = pic.b
still = ExecPython(x: Image) { code: "return {}" }
still.x = img.out
clip = ExecPython(x: Video) { code: "return {}" }
clip.x = vid.out
"#;
    let (project, enrich_errors, d) = check_in(source, CompileFs::disk(dir.path()));
    assert!(enrich_errors.is_empty(), "{enrich_errors:?}");
    assert!(codes(&d).is_empty(), "{d:?}");
    let keep = project.nodes.iter().find(|n| n.node_type == "KeepFile").expect("one shared KeepFile");
    assert_eq!(keep.place_types.len(), 2, "{:?}", keep.place_types);
    let at = |site: &str| keep.types_at(&[site]).output("file").cloned();
    let site_of = |name: &str| {
        project.groups.iter().find(|g| g.id.ends_with(name) && matches!(g.kind, weft_core::project::GroupKind::Call { .. })).map(|g| g.id.clone()).expect("site")
    };
    assert_eq!(at(&site_of("img")), Some(ty("Image")));
    assert_eq!(at(&site_of("vid")), Some(ty("Video")));

    // A place still holds its own bound.
    let blob = source.replace("b: Video", "b: Blob").replace("clip = ExecPython(x: Video)", "clip = ExecPython(x: Blob)");
    let (_, _, d) = check_in(&blob, CompileFs::disk(dir.path()));
    assert!(message_of(&d, "type-bound").contains("Blob"), "{d:?}");
}

/// A field each instance fills is one value for every place its node runs,
/// so a field whose type differs between places is refused.
#[test]
fn an_instance_filled_field_takes_one_type_at_every_place() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir(dir.path().join("lib")).expect("mkdir");
    std::fs::write(
        dir.path().join("lib/pick.weft"),
        "Group(v: T) -> (o: T) {\n  p = ExecPython(k: T, v: T) -> (o: T) { code: \"return {}\", k: @instance_filled }\n  p.v = self.v\n  self.o = p.o\n}\n",
    )
    .expect("write");
    let source = r#"
pic = ExecPython() -> (a: Image, b: Video) { code: "return {}" }
img = @include("lib/pick.weft")
img.v = pic.a
vid = @include("lib/pick.weft")
vid.v = pic.b
still = ExecPython(x: Image) { code: "return {}" }
still.x = img.o
clip = ExecPython(x: Video) { code: "return {}" }
clip.x = vid.o
"#;
    let (_, _, d) = check_in(source, CompileFs::disk(dir.path()));
    assert!(message_of(&d, "instance-filled-place-types").contains("Image"), "{d:?}");
}

/// A file included inside an included file is a place under each outer
/// place: `img.inner` and `vid.inner` resolve apart.
#[test]
fn a_nested_include_is_its_own_place_under_each_outer_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir(dir.path().join("lib")).expect("mkdir");
    std::fs::write(
        dir.path().join("lib/inner.weft"),
        "Group[T: Media](file: T) -> (out: T) {\n  self.out = self.file\n}\n",
    )
    .expect("write");
    std::fs::write(
        dir.path().join("lib/outer.weft"),
        "Group[T: Media](file: T) -> (out: T) {\n  inner = @include(\"inner.weft\")\n  inner.file = self.file\n  self.out = inner.out\n}\n",
    )
    .expect("write");
    let source = r#"
pic = ExecPython() -> (a: Image, b: Video) { code: "return {}" }
img = @include("lib/outer.weft")
img.file = pic.a
vid = @include("lib/outer.weft")
vid.file = pic.b
still = ExecPython(x: Image) { code: "return {}" }
still.x = img.out
clip = ExecPython(x: Video) { code: "return {}" }
clip.x = vid.out
"#;
    let (project, enrich_errors, d) = check_in(source, CompileFs::disk(dir.path()));
    assert!(enrich_errors.is_empty(), "{enrich_errors:?}");
    assert!(codes(&d).is_empty(), "{d:?}");
    // The inner file's own group is one group with a port type per place,
    // the way a node inside it would be.
    let inner = project.groups.iter()
        .find(|g| matches!(g.kind, weft_core::project::GroupKind::Body) && g.id.contains("inner"))
        .expect("the inner file's body");
    let outs: std::collections::BTreeSet<String> = inner.place_types.iter().map(|place| place.outputs["out"].to_string()).collect();
    assert_eq!(outs, ["Image".to_string(), "Video".to_string()].into(), "{:?}", inner.place_types);
    for place in &inner.place_types {
        assert_eq!(place.place.len(), 2, "under the outer site, then the inner one: {:?}", place.place);
    }
}

/// A wire out of one call of an included file into a group port nobody
/// reads is typed at that call alone: the other call, typed apart, is
/// never held to it.
#[test]
fn a_wire_to_an_unread_group_port_is_typed_at_its_own_call_only() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir(dir.path().join("lib")).expect("mkdir");
    std::fs::write(
        dir.path().join("lib/f.weft"),
        "Group(x: T) -> (r: T) {\n  n = ExecPython(v: T) -> (r: T) { code: \"return {}\" }\n  n.v = self.x\n  self.r = n.r\n}\n",
    )
    .expect("write");
    let source = r#"
a = ExecPython() -> (s: String, k: Number) { code: "return {}" }
one = @include("lib/f.weft")
one.x = a.s
two = @include("lib/f.weft")
two.x = a.k
show = Debug
show.data = two.r
g = Group(y: String) -> (o: String) {
  self.o = self.y
}
g.y = one.r
"#;
    let (_, enrich_errors, d) = check_in(source, CompileFs::disk(dir.path()));
    assert!(enrich_errors.is_empty(), "{enrich_errors:?}");
    assert!(!codes(&d).contains(&"type-mismatch"), "{d:?}");
}

/// An included file's node fed by the file's port is decided only when
/// every site that includes it decides: one gated site says nothing
/// about another.
#[test]
fn the_gate_rule_asks_every_site_of_an_included_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir(dir.path().join("lib")).expect("mkdir");
    std::fs::write(
        dir.path().join("lib/step.weft"),
        "Group(x: String) -> (r: String) {\n  n = ExecPython(v?: String) -> (r: String) { code: \"return {}\" }\n  n.v = self.x\n  self.r = n.r\n}\n",
    )
    .expect("write");
    let both = r#"
c = ExecPython() -> (ok: String) { code: "return {}" }
one = @include("lib/step.weft")
one.x = c.ok
one._should_flow = c.ok
two = @include("lib/step.weft")
two.x = c.ok
two._should_flow = c.ok
"#;
    let (_, _, d) = check_in(both, CompileFs::disk(dir.path()));
    assert!(!codes(&d).contains(&"run-undecided"), "{d:?}");
    let one_gated = both.replace("two._should_flow = c.ok\n", "");
    let (_, _, d) = check_in(&one_gated, CompileFs::disk(dir.path()));
    assert!(codes(&d).contains(&"run-undecided"), "{d:?}");
}

/// A List's items share one `T`, so an Image and a Video together need
/// the list's type declared: `List[Media]` pins `T` to Media and both fit.
#[test]
fn a_list_mixes_kinds_when_its_type_is_declared() {
    let mixed = r#"
src = ExecPython() -> (a: Image, b: Video) { code: "return {}" }
both = List -> (list: List[Media])
both.first = src.a
both.second = src.b
show = Debug
show.data = both.list
"#;
    let (project, enrich_errors, d) = check(mixed);
    assert!(enrich_errors.is_empty(), "{enrich_errors:?}");
    assert!(codes(&d).is_empty(), "{d:?}");
    assert_eq!(output_type(&project, "both", "list"), ty("List[Media]"));

    let undeclared = mixed.replace(" -> (list: List[Media])", "");
    let (_, _, d) = check(&undeclared);
    assert!(codes(&d).contains(&"type-mismatch"), "one T cannot be an Image and a Video: {d:?}");
}

/// An empty constant says nothing about its items: `[]` leaves `T` for
/// the wire to pin.
#[test]
fn an_empty_constant_pins_nothing() {
    let (project, enrich_errors, d) = check(
        r#"
src = ExecPython() -> (xs: List[Number]) { code: "return {}" }
g = Group(a: List[T], b: List[T]) -> (out: List[T]) {
  self.out = self.b
}
g.a = []
g.b = src.xs
show = Debug
show.data = g.out
"#,
    );
    assert!(enrich_errors.is_empty(), "{enrich_errors:?}");
    assert!(!codes(&d).contains(&"type-mismatch"), "{d:?}");
    assert_eq!(group_out(&project, "g", "out"), ty("List[Number]"));
}

/// A group's header is one: `T` pinned on the way in is the `T` that
/// comes out, and the bound is reported once.
#[test]
fn a_group_type_parameter_is_one_variable_across_its_ports() {
    let (project, enrich_errors, d) = check(
        r#"
src = ExecPython() -> (clip: Video) { code: "return {}" }
g = Group[T: Media](file: T) -> (out: T) {
  self.out = self.file
}
g.file = src.clip
show = Debug
show.data = g.out
"#,
    );
    assert!(enrich_errors.is_empty(), "{enrich_errors:?}");
    assert!(codes(&d).is_empty(), "{d:?}");
    assert_eq!(group_out(&project, "g", "out"), ty("Video"));

    let (_, _, d) = check(
        r#"
src = ExecPython() -> (doc: Blob) { code: "return {}" }
g = Group[T: Media](file: T) -> (out: T) {
  self.out = self.file
}
g.file = src.doc
"#,
    );
    assert_eq!(codes(&d).iter().filter(|c| **c == "type-bound").count(), 1, "{d:?}");

    // One variable: a String in and a Number out of the same `T` cannot
    // both hold.
    let (_, _, d) = check(
        r#"
src = ExecPython() -> (name: String) { code: "return {}" }
g = Group(a: T) -> (b: T) {
  n = ExecPython() -> (n: Number) { code: "return {}" }
  self.b = n.n
}
g.a = src.name
"#,
    );
    assert!(codes(&d).contains(&"type-mismatch"), "{d:?}");
}

/// The same bound written on an inline node checks its own ports.
#[test]
fn an_inline_node_takes_type_parameters() {
    let (_, enrich_errors, d) = check(
        r#"
src = ExecPython() -> (name: String) { code: "return {}" }
p = ExecPython[T: Media](f: T) -> (g: T) { code: "return {}" }
p.f = src.name
"#,
    );
    assert!(enrich_errors.is_empty(), "{enrich_errors:?}");
    assert!(message_of(&d, "type-bound").contains("Media"), "{d:?}");
}

/// A parameter list that cannot be meant is refused where it is written.
#[test]
fn a_bad_type_parameter_list_is_refused_where_written() {
    let refused = |source: &str, needle: &str| {
        let errors = match compile_with_mode(source, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Full, None) {
            Ok(_) => panic!("`{source}` compiled"),
            Err(errors) => errors,
        };
        assert!(
            errors.iter().any(|e| e.message.contains(needle)),
            "`{source}`: expected {needle:?} in {errors:?}"
        );
    };
    refused("g = Group[X: Media](a: X) -> (b: X) {\n  self.b = self.a\n}\n", "not a type variable");
    refused("g = Group[T: Media](a: String) -> (b: String) {\n  self.b = self.a\n}\n", "no port uses it");
    refused(
        "g = Group[T1: List[T2], T2: List[T1]](a: T1, b: T2) -> (c: T1) {\n  self.c = self.a\n}\n",
        "T1 -> T2 -> T1",
    );
    refused("g = Group[T](a: T) -> (b: T) {\n  self.b = self.a\n}\n", "has no bound");

    // On a catalog node the parameter may bound the node's own variable.
    let (_, enrich_errors, _) = check("keep = KeepFile[T: Image] { file: @asset(\"cat.png\", Image) }\n");
    assert!(enrich_errors.is_empty(), "{enrich_errors:?}");
    let (_, enrich_errors, _) = check("keep = KeepFile[T2: Image] { file: @asset(\"cat.png\", Image) }\n");
    assert!(enrich_errors.iter().any(|e| e.contains("no port uses it")), "{enrich_errors:?}");
}

// ── Dicts and records ──────────────────────────────────────────────────

#[test]
fn a_dict_keyed_by_anything_but_text_is_refused_naming_the_shape_that_works() {
    let errors = compile_with_mode(
        "p = ExecPython(d: Dict[Number, String]) { code: \"return {}\" }\n",
        uuid::Uuid::new_v4(),
        CompileFs::none(),
        IncludeMode::Full,
        None,
    )
    .expect_err("a number-keyed dict is no type");
    assert!(
        errors.iter().any(|e| e.message.contains("keys are always text") && e.message.contains("List[{key: Number")),
        "{errors:?}"
    );
}

#[test]
fn an_open_record_in_a_header_flows_into_a_closed_one_only_when_it_declares_less() {
    let (_, _, d) = check(
        r#"
src = ExecPython() -> (signup: { email: String, utm: String }) { code: "return {}" }
use = ExecPython(signup: { email: String, .. }) { code: "return {}" }
use.signup = src.signup
"#,
    );
    assert!(!codes(&d).contains(&"type-mismatch"), "an open target takes the extra field: {d:?}");
    let (_, _, d) = check(
        r#"
src = ExecPython() -> (signup: { email: String, utm: String }) { code: "return {}" }
use = ExecPython(signup: { email: String }) { code: "return {}" }
use.signup = src.signup
"#,
    );
    assert!(codes(&d).contains(&"type-mismatch"), "a closed target refuses it: {d:?}");
}

// ── Narrowable outputs ─────────────────────────────────────────────────

/// A route's `body` takes any type that fits in its bound; a fixed
/// output takes only its own type.
#[test]
fn a_narrowable_output_takes_a_fitting_type_and_a_fixed_one_only_its_own() {
    let (project, enrich_errors, _) = check(
        "r = Route -> (body: { email: String, .. }) { path: \"signup\", method: \"POST\" }\n",
    );
    assert!(enrich_errors.is_empty(), "{enrich_errors:?}");
    assert_eq!(output_type(&project, "r", "body"), ty("{ email: String, .. }"));

    let (_, enrich_errors, _) = check("r = Route -> (body: Number) { path: \"signup\" }\n");
    assert!(enrich_errors.iter().any(|e| e.contains("widest type")), "{enrich_errors:?}");

    let (_, enrich_errors, _) = check("r = Route -> (path: Number) { path: \"signup\" }\n");
    assert!(enrich_errors.iter().any(|e| e.contains("always gives it String")), "{enrich_errors:?}");
}

/// The body's type follows the route's `dataType`, checked at compile
/// time by the route's own rules.
#[test]
fn a_route_body_type_must_suit_its_data_type() {
    let (_, _, d) = check("r = Route { path: \"say\", method: \"POST\", dataType: \"text\" }\n");
    assert!(message_of(&d, "rule-structural").contains("text body"), "{d:?}");
    let (_, _, d) = check("r = Route -> (body: String) { path: \"say\", method: \"POST\", dataType: \"text\" }\n");
    assert!(!codes(&d).contains(&"rule-structural"), "{d:?}");
    let (_, _, d) = check("r = Route -> (body: String) { path: \"say\", method: \"POST\" }\n");
    assert!(message_of(&d, "rule-structural").contains("json body"), "{d:?}");
}

/// A port added to a route reads the path capture of its name, so one
/// the path does not capture, or one typed other than text, is refused.
#[test]
fn a_route_port_reads_a_capture_of_its_path() {
    let (_, _, d) = check("r = Route -> (id: String) { path: \"cards/{id}\", method: \"GET\" }\n");
    assert!(!codes(&d).contains(&"route-output-not-a-capture"), "{d:?}");
    let (_, _, d) = check("r = Route -> (name: String) { path: \"cards/{id}\", method: \"GET\" }\n");
    let message = message_of(&d, "route-output-not-a-capture");
    assert!(message.contains("`name`") && message.contains("`id`"), "{message}");
    let (_, _, d) = check("r = Route -> (id: Number) { path: \"cards/{id}\", method: \"GET\" }\n");
    assert!(message_of(&d, "route-output-not-a-capture").contains("text"), "{d:?}");
}

/// A capture named like one of the route's own outputs could never be
/// read, with or without a declaration of that name.
#[test]
fn a_capture_may_not_share_a_name_with_an_output_of_the_node() {
    for source in [
        "r = Route { path: \"cards/{body}\", method: \"GET\" }\n",
        "r = Route -> (body: { title: String }) { path: \"cards/{body}\", method: \"POST\" }\n",
    ] {
        let (_, _, d) = check(source);
        let message = message_of(&d, "route-capture-shadows-output");
        assert!(message.contains("`body`") && message.contains("rename the capture"), "{d:?}");
    }
}

// ── Placeholders are inputs ────────────────────────────────────────────

#[test]
fn a_templates_placeholders_and_its_inputs_are_held_to_each_other() {
    let (_, _, d) = check(
        r#"
src = ExecPython() -> (name: String) { code: "return {}" }
f = Format(usr: String) { template: "Hi {{user}}, {{{{json}}}} stays text" }
f.usr = src.name
"#,
    );
    let unknown = message_of(&d, "placeholder-not-an-input");
    assert!(unknown.contains("`user`") && unknown.contains("did you mean `usr`"), "{unknown}");
    assert!(message_of(&d, "input-not-a-placeholder").contains("`usr`"), "{d:?}");

    let (_, _, d) = check(
        r#"
src = ExecPython() -> (name: String) { code: "return {}" }
f = Format(user: String) { template: "Hi {{ user }}, {{{{json}}}} stays text" }
f.user = src.name
"#,
    );
    assert!(!codes(&d).contains(&"placeholder-not-an-input"), "{d:?}");
    assert!(!codes(&d).contains(&"input-not-a-placeholder"), "{d:?}");
}

/// A query typo is found at build, knowing nothing of SQL: the node
/// declares its placeholder pattern, and a `$name` inside a string
/// literal is not one.
#[test]
fn a_query_placeholder_typo_is_found_at_build() {
    let (_, _, d) = check(
        r#"
db = PostgresAccess
src = ExecPython() -> (id: String) { code: "return {}" }
q = PostgresExecuteQuery(userId: String) { query: "SELECT '$literal' FROM cards WHERE owner = $userid" }
q.account = db.access
q.userId = src.id
"#,
    );
    let message = message_of(&d, "placeholder-not-an-input");
    assert!(message.contains("$userid") || message.contains("`userid`"), "{message}");
    assert!(!message.contains("literal"), "a string literal holds no placeholder: {d:?}");
}

/// A node that added no input is shown the declaration that adds one,
/// written the way the language declares a node: its name, then its type.
#[test]
fn a_placeholder_on_a_node_with_no_inputs_shows_the_declaration_to_write() {
    let (_, _, d) = check(
        r#"
db = PostgresAccess
q = PostgresExecuteQuery { query: "SELECT * FROM cards WHERE owner = $userId" }
q.account = db.access
"#,
    );
    let message = message_of(&d, "placeholder-not-an-input");
    assert!(message.contains("`q = PostgresExecuteQuery(userId: String)`"), "{message}");
}

/// Text read in through `@file` is held to the inputs the same way.
#[test]
fn a_template_read_from_a_file_is_checked_too() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("greeting.txt"), "Hello {{who}}").expect("write");
    let source = r#"
src = ExecPython() -> (name: String) { code: "return {}" }
f = Format(name: String) { template: @file("greeting.txt") }
f.name = src.name
"#;
    let (_, _, d) = check_in(source, CompileFs::disk(dir.path()));
    assert!(message_of(&d, "placeholder-not-an-input").contains("`who`"), "{d:?}");
}

// ── When a node runs ───────────────────────────────────────────────────

/// A node whose wired inputs are all optional runs whether or not they
/// arrive, so the build asks when it runs: its gate.
#[test]
fn a_node_with_only_optional_inputs_wired_needs_its_gate() {
    let ungated = r#"
r = Route -> (body: { title?: String }) { path: "note", method: "POST" }
o = JsonObject { title?: r.body.title }
"#;
    let (_, _, d) = check(ungated);
    let message = message_of(&d, "run-undecided");
    assert!(message.contains("_should_flow") && message.contains("title"), "{message}");

    let gated = format!("{ungated}o._should_flow = r.fired\n");
    let (_, _, d) = check(&gated);
    assert!(!codes(&d).contains(&"run-undecided"), "{d:?}");
}

/// Not asked of a root, a join, a node using `@require_one_of`, or one
/// whose wired input is required only once wired.
#[test]
fn the_gate_rule_spares_roots_joins_one_of_groups_and_inputs_required_once_wired() {
    let (_, _, d) = check(
        r#"
a = ExecPython() -> (x: String) { code: "return {}" }
b = ExecPython() -> (x: String) { code: "return {}" }
first = FirstInOrder { one: a.x, two: b.x }
root = JsonObject { title?: "fixed" }
pick = ExecPython(p?: String, q?: String, @require_one_of(p, q)) { code: "return {}" }
pick.p = a.x
pick.q = b.x
params = LlmParams
params.systemPrompt = a.x
"#,
    );
    assert!(!codes(&d).contains(&"run-undecided"), "{d:?}");
}

/// A group's gate decides for everything inside it, and a loop body's
/// node fed the current item always has it; an ungated group decides
/// nothing (its ports are optional inside).
#[test]
fn the_gate_rule_reads_the_scope_a_node_sits_in() {
    let gated_group = r#"
seed = Text { value: "x" }
g = Group(x: String) -> (o: String) {
  i = ExecPython(v?: String) -> (r: String) { code: "return {}" }
  i.v = self.x
  self.o = i.r
}
g.x = seed.value
g._should_flow = seed.value
"#;
    let (_, _, d) = check(gated_group);
    assert!(!codes(&d).contains(&"run-undecided"), "{d:?}");

    let loop_item = r#"
seed = ExecPython() -> (items: List[String]) { code: "return {}" }
my = Loop(items: List[String]) -> (results: List[String | Null]) {
  over: ["items"]
  p = ExecPython(v?: String) -> (r: String) { code: "return {}" }
  p.v = self.items
  self.results = p.r
}
my.items = seed.items
"#;
    let (_, _, d) = check(loop_item);
    assert!(!codes(&d).contains(&"run-undecided"), "{d:?}");

    let ungated_group = gated_group.replace("g._should_flow = seed.value\n", "");
    let (_, _, d) = check(&ungated_group);
    assert!(message_of(&d, "run-undecided").contains("'g.i'"), "{d:?}");

    // The group's gate decides what its ports carry, not what a node
    // inside makes: a node fed by another one inside still needs its own.
    let chained = gated_group.replace(
        "  self.o = i.r\n",
        "  j = ExecPython(v?: String) -> (r: String) { code: \"return {}\" }\n  j.v = i.r\n  self.o = j.r\n",
    );
    let (_, _, d) = check(&chained);
    let message = message_of(&d, "run-undecided");
    assert!(message.contains("'g.j'") && !message.contains("'g.i'"), "{d:?}");

    // A constant written on the group's port is always there.
    let constant = ungated_group.replace("g.x = seed.value\n", "g.x = \"hi\"\n");
    let (_, _, d) = check(&constant);
    assert!(!codes(&d).contains(&"run-undecided"), "{d:?}");

    // Not on the output of the same name: nothing writes it, so nothing
    // arrives on it.
    let out_named_alike = r#"
h = Group(x: String) -> (x: String) {
  i = ExecPython(v?: String) -> (r: String) { code: "return {}" }
  i.v = self.x
}
h.x = "hi"
m = ExecPython(v?: String) -> (r: String) { code: "return {}" }
m.v = h.x
"#;
    let (_, _, d) = check(out_named_alike);
    assert!(message_of(&d, "run-undecided").contains("'m'"), "{d:?}");

    // A loop's index is always there too.
    let index = loop_item.replace("p = ExecPython(v?: String)", "p = ExecPython(v?: Number)").replace("p.v = self.items", "p.v = self.index");
    let (_, _, d) = check(&index);
    assert!(!codes(&d).contains(&"run-undecided"), "{d:?}");
}

// ── `file` and `asset` are words ───────────────────────────────────────

#[test]
fn the_words_file_and_asset_are_plain_values() {
    let (_, enrich_errors, d) = check(
        r#"
r = Route { path: "file", method: "GET" }
t = Text { value: "asset" }
"#,
    );
    assert!(enrich_errors.is_empty(), "{enrich_errors:?}");
    assert!(
        d.iter().all(|d| !d.message.contains("@file") && !d.message.contains("@asset")),
        "{d:?}"
    );
}
