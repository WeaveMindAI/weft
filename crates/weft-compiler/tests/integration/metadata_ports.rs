//! Layer 3: metadata ports and metadata wires through the real pipeline
//! (lowering, joining, enrich, validate) against the standard library.

use serde_json::json;
use weft_catalog::{stdlib_root, FsCatalog};
use weft_compiler::validate::ValidationMode;
use weft_compiler::{compile_strict, CompileFs, Diagnostic, Severity};
use weft_core::ProjectDefinition;

fn catalog() -> FsCatalog {
    FsCatalog::discover(&stdlib_root().expect("stdlib root")).expect("stdlib catalog")
}

/// The pipeline `weft validate` runs, on an entry file.
fn check(source: &str) -> (ProjectDefinition, Vec<Diagnostic>) {
    compile_strict(source, uuid::Uuid::new_v4(), CompileFs::none(), &catalog(), ValidationMode::Structural, None)
}

fn error_codes(diagnostics: &[Diagnostic]) -> Vec<&str> {
    diagnostics.iter().filter(|d| d.severity == Severity::Error).filter_map(|d| d.code.as_deref()).collect()
}

fn clean(source: &str) -> ProjectDefinition {
    let (project, diagnostics) = check(source);
    let errors: Vec<&Diagnostic> = diagnostics.iter().filter(|d| d.severity == Severity::Error).collect();
    assert!(errors.is_empty(), "{errors:#?}");
    project
}

fn refused(source: &str, code: &str) -> Vec<Diagnostic> {
    let (_, diagnostics) = check(source);
    assert!(error_codes(&diagnostics).contains(&code), "expected {code}, got {diagnostics:#?}");
    diagnostics
}

fn node<'p>(project: &'p ProjectDefinition, id: &str) -> &'p weft_core::project::NodeDefinition {
    project.nodes.iter().find(|n| n.id == id).unwrap_or_else(|| panic!("no node {id}"))
}

const TOOL: &str = r#"
search = Group(query: String) -> (results: String, *tool: { name: String, description: String }) {
  self.tool = { "name": "search", "description": "Search the web." }
  echo = Text {}
  echo.value = self.query
  self.results = echo.value
}
"#;

#[test]
fn a_metadata_wire_writes_its_value_into_the_input_and_never_runs() {
    let project = clean(&format!(
        "{TOOL}
tools = List {{}}
tools.search = search.tool
search.query = \"cats\"
shown = Debug {{ data: tools.list }}
"
    ));
    let tools = node(&project, "tools");
    assert_eq!(tools.port_literals.get("search"), Some(&json!({ "name": "search", "description": "Search the web." })));
    // The literal's type is the port's, so the list's `T` is that record.
    let list = tools.outputs.iter().find(|p| p.name == "list").expect("list output");
    assert_eq!(list.port_type.to_string(), "List[{name: String, description: String}]");
    assert!(project.edges.iter().all(|e| e.source_port() != "tool"), "{:#?}", project.edges);
    assert_eq!(project.metadata_wires.len(), 1, "{:#?}", project.metadata_wires);
    assert_eq!(project.metadata_wires[0].target, "tools");
    let group = project.groups.iter().find(|g| g.id == "search").expect("group");
    assert_eq!(group.metadata_ports.len(), 1);
    assert!(group.out_ports.iter().all(|p| p.name != "tool"), "a metadata port is no output");
}

#[test]
fn a_metadata_wire_may_point_backwards_without_a_cycle() {
    clean(&format!(
        "{TOOL}
tools = List {{}}
tools.search = search.tool
search.query = \"cats\"
said = Debug {{}}
said.data = tools.list
"
    ));
    // The same shape with a run-time wire back into the group is a cycle;
    // the metadata wire is not one.
    clean(
        r#"
g = Group(items: List[{ name: String }]) -> (*tool: { name: String }) {
  self.tool = { "name": "g" }
  shown = Debug { data: self.items }
}
tools = List {}
tools.g = g.tool
g.items = tools.list
"#,
    );
}

#[test]
fn a_metadata_port_takes_its_value_from_another_one() {
    let project = clean(
        r#"
outer = Group() -> (*tool: { name: String }) {
  inner = Group() -> (*tool: { name: String }) {
    self.tool = { "name": "deep" }
  }
  self.tool = inner.tool
}
tools = List {}
tools.outer = outer.tool
shown = Debug { data: tools.list }
"#,
    );
    assert_eq!(node(&project, "tools").port_literals.get("outer"), Some(&json!({ "name": "deep" })));
    let outer = project.groups.iter().find(|g| g.id == "outer").unwrap();
    assert_eq!(outer.metadata_ports[0].value, Some(json!({ "name": "deep" })));
}

#[test]
fn a_run_time_value_cannot_feed_a_metadata_port() {
    let diagnostics = refused(
        r#"
g = Group() -> (*tool: { name: String }) {
  made = Text { value: "x" }
  self.tool = made.value
}
"#,
        "metadata-value-not-literal",
    );
    assert!(diagnostics.iter().any(|d| d.message.contains("computed during the run")), "{diagnostics:#?}");
}

#[test]
fn a_metadata_port_with_no_value_is_refused_naming_the_line() {
    let diagnostics = refused(
        r#"
g = Group() -> (*tool: { name: String }) {
  shown = Debug { data: "x" }
}
"#,
        "metadata-port-not-written",
    );
    assert!(diagnostics.iter().any(|d| d.message.contains("self.tool = { ... }")), "{diagnostics:#?}");
}

#[test]
fn a_metadata_port_is_never_a_wire_target() {
    refused(
        &format!(
            "{TOOL}
made = Text {{ value: \"x\" }}
search.tool = made.value
"
        ),
        "metadata-port-as-target",
    );
}

#[test]
fn a_metadata_wire_and_a_written_value_on_one_input_are_two_drivers() {
    refused(
        &format!(
            "{TOOL}
tools = List {{ search: {{ \"name\": \"other\", \"description\": \"x\" }} }}
tools.search = search.tool
"
        ),
        "double-driven-port",
    );
    // A run-time wire on the same input is a second driver too.
    refused(
        &format!(
            "{TOOL}
tools = List {{}}
tools.search = search.tool
tools.search = search.results
"
        ),
        "double-driven-port",
    );
}

#[test]
fn a_value_that_does_not_fit_the_input_is_the_literal_mismatch() {
    refused(
        &format!(
            "{TOOL}
said = Text {{}}
said.value = search.tool
"
        ),
        "config-type-mismatch",
    );
}

#[test]
fn a_metadata_wire_into_a_group_input_is_written_on_the_group_port() {
    let project = clean(&format!(
        "{TOOL}
search.query = \"cats\"
h = Group(t: {{ name: String, description: String }}) {{
  shown = Debug {{}}
  shown.data = self.t
}}
h.t = search.tool
"
    ));
    let h = project.groups.iter().find(|g| g.id == "h").unwrap();
    assert_eq!(h.port_literals.get("t"), Some(&json!({ "name": "search", "description": "Search the web." })));
    // What reads `self.t` inside gets it from the group's own port, the way
    // any value written there reaches it.
    let inner = project.edges.iter().find(|e| e.target == "h.shown").expect("wire to the member");
    assert_eq!(inner.source, "h");
    assert!(inner.from_group_port());
    // The editor draws the line the person wrote, up to the group's port.
    assert_eq!(project.metadata_wires.len(), 1);
    assert_eq!(project.metadata_wires[0].target, "h");
    assert_eq!(project.metadata_wires[0].target_port(), "t");
}

#[test]
fn a_loop_carries_a_metadata_port_out_from_its_body() {
    let project = clean(
        r#"
each = Loop(items: List[String]) -> (seen: List[String | Null], *search: { name: String }) {
  over: ["items"]
  tool = Group() -> (*tool: { name: String }) {
    self.tool = { "name": "search" }
  }
  self.search = tool.tool
  echo = Text {}
  echo.value = self.items
  self.seen = echo.value
}
each.items = ["a"]
tools = List {}
tools.search = each.search
shown = Debug { data: tools.list }
"#,
    );
    assert_eq!(node(&project, "tools").port_literals.get("search"), Some(&json!({ "name": "search" })));
}

#[test]
fn metadata_ports_that_take_their_value_from_each_other_are_refused() {
    refused(
        r#"
a = Loop(items: List[String]) -> (*tool: { name: String }) {
  over: ["items"]
  self.tool = b.tool
}
b = Loop(items: List[String]) -> (*tool: { name: String }) {
  over: ["items"]
  self.tool = a.tool
}
a.items = ["x"]
b.items = ["y"]
"#,
        "metadata-reference-cycle",
    );
}

/// Like an output, a metadata port is read from outside its group: the
/// line reading it inside is the one error, and the input it feeds is not
/// reported again as having no driver.
#[test]
fn a_metadata_port_is_not_read_from_inside_its_group() {
    let (_, diagnostics) = check(
        r#"
g = Group() -> (*tool: { name: String }) {
  self.tool = { "name": "g" }
  shown = Debug {}
  shown.data = self.tool
}
"#,
    );
    assert_eq!(error_codes(&diagnostics), ["metadata-port-read-inside"], "{diagnostics:#?}");
    assert!(diagnostics.iter().any(|d| d.message.contains("like an output it is read from outside the group")), "{diagnostics:#?}");
    // Named by the group's own name from a group nested in it, the read is
    // still from inside.
    let (_, diagnostics) = check(
        r#"
g = Group() -> (*tool: { name: String }) {
  self.tool = { "name": "g" }
  m = Group() {
    shown = Debug {}
    shown.data = g.tool
  }
}
"#,
    );
    assert_eq!(error_codes(&diagnostics), ["metadata-port-read-inside"], "{diagnostics:#?}");
}

/// A metadata value written into an ordinary output is said once, however
/// many read the output, and what reads it is not reported again.
#[test]
fn a_metadata_value_cannot_become_a_group_output() {
    let (_, diagnostics) = check(&format!(
        "{TOOL}
search.query = \"x\"
h = Group() -> (out: {{ name: String, description: String }}) {{
  self.out = search.tool
}}
first = Debug {{}}
first.data = h.out
second = Debug {{}}
second.data = h.out
"
    ));
    assert_eq!(error_codes(&diagnostics), ["metadata-into-output"], "{diagnostics:#?}");
}

/// Read from inside, a group's output is told where it is written and read.
#[test]
fn an_output_read_from_inside_its_group_says_where_it_is_read() {
    let (_, diagnostics) = check(
        r#"
g = Group() -> (out: String) {
  value = Text { value: "x" }
  self.out = value.value
  shown = Debug {}
  shown.data = self.out
}
"#,
    );
    assert!(diagnostics.iter().any(|d| d.message.contains("'self.out' is an output of 'g': inside the group it is written")), "{diagnostics:#?}");
}

#[test]
fn the_header_marks_a_metadata_port_only_in_an_output_list_of_a_group() {
    for (source, says) in [
        ("g = Group(*tool: String) { }\n", "sits on the output side"),
        ("t = Text -> (*tool: String) { value: \"x\" }\n", "only a group or a loop"),
        ("g = Group() -> (*tool) { }\n", "needs a concrete type"),
        ("g = Group() -> (* , out: String) { }\n", "goes right before its name"),
        ("g = Group() -> (out: String, %) { }\n", "Unexpected '%'"),
        ("g = Group(tool: String) -> (*tool: String) { self.tool = \"x\" }\n", "has the name of an input"),
    ] {
        let (_, diagnostics) = check(source);
        assert!(diagnostics.iter().any(|d| d.message.contains(says)), "{source}: {diagnostics:#?}");
    }
}

#[test]
fn a_metadata_port_written_twice_is_refused() {
    let (_, diagnostics) = check(
        r#"
g = Group() -> (*tool: String) {
  self.tool = "a"
  self.tool = "b"
}
"#,
    );
    assert!(diagnostics.iter().any(|d| d.message.contains("written twice")), "{diagnostics:#?}");
}

/// The shape a tool-using agent takes: the tool is a group describing
/// itself on `*tool`, the loop over the model's calls dispatches each to
/// it and hands the description out on its own metadata port, and a List
/// gathers that into the model's `tools`, up the graph.
const AGENT: &str = r#"
model = CustomProvider { baseUrl: "http://127.0.0.1:9/v1", model: "m" }

handle = Loop(calls: List[ToolCall]) -> (historyFile: File, *dog: LlmTool) {
  over: ["calls"]
  carry: ["historyFile"]
  dog = Group(call: ToolCall) -> (callId: String, result: String, *tool: LlmTool) {
    self.tool = { "name": "dogName", "description": "The dog's name.", "parameters": { "type": "object" } }
    work = ExecPython(call: ToolCall) -> (callId: String, name: String) {
      code: "return {'callId': call['id'], 'name': 'Biscuit'}"
    }
    work.call = self.call
    self.callId = work.callId
    self.result = work.name
  }
  self.dog = dog.tool
  route = Dispatch
  route.value = self.calls
  route.dog = dog.tool
  dog.call = route.dog
  unknown = Debug
  unknown.data = route.otherwise
  answer = ChatHistoryAppend { role: "tool" }
  answer.historyFile = self.historyFile
  answer.toolCallId = dog.callId
  answer.text = dog.result
  self.historyFile = answer.historyFile
}

tools = List
tools.dog = handle.dog

ask = LlmInference { prompt: "What is my dog's name?" }
ask.provider = model.provider
ask.tools = tools.list
handle.calls = ask.toolCalls
handle.historyFile = ask.historyFile
done = Debug { data: handle.historyFile }
"#;

#[test]
fn the_tool_agent_shape_compiles_with_its_wires_written_in() {
    let project = clean(AGENT);
    let tool = json!({ "name": "dogName", "description": "The dog's name.", "parameters": { "type": "object" } });
    assert_eq!(node(&project, "tools").port_literals.get("dog"), Some(&tool));
    let route = node(&project, "handle.route");
    assert_eq!(route.port_literals.get("dog"), Some(&tool));
    // The branch's output is the compiler's, typed as what it routes.
    let branch = route.outputs.iter().find(|p| p.name == "dog").expect("the paired output");
    assert_eq!(branch.port_type.to_string(), "ToolCall");
    let list = node(&project, "tools").outputs.iter().find(|p| p.name == "list").unwrap();
    assert_eq!(list.port_type.to_string(), "List[LlmTool]");
    // Two metadata wires (the list's and the route's), neither of them
    // run; the loop's own value is written, not wired.
    assert_eq!(project.metadata_wires.len(), 2, "{:#?}", project.metadata_wires);
    assert!(project.edges.iter().all(|e| !["tool", "dog"].contains(&e.source_port()) || e.source == "handle.route"), "{:#?}", project.edges);
}

#[test]
fn a_dispatch_branch_nothing_reads_is_refused() {
    let unread = AGENT.replace("  dog.call = route.dog\n", "  dog.call = self.calls\n");
    let diagnostics = refused(&unread, "unpaired-port");
    assert!(diagnostics.iter().any(|d| d.message.contains("nothing reads")), "{diagnostics:#?}");
}

#[test]
fn a_dispatch_output_with_no_input_of_its_name_is_refused() {
    let unpaired = AGENT.replace("  route = Dispatch\n", "  route = Dispatch -> (cat: ToolCall)\n");
    let diagnostics = refused(&unpaired, "unpaired-port");
    assert!(diagnostics.iter().any(|d| d.message.contains("matches no input")), "{diagnostics:#?}");
}

const SEARCH_FILE: &str = r#"
Group(query: String) -> (results: String, *tool: { name: String }) {
  self.tool = { "name": "search" }
  echo = Text {}
  echo.value = self.query
  self.results = echo.value
}
"#;

const USES_THE_FILE: &str = r#"
one = @include("lib/search.weft")
one.query = "cats"
two = @include("lib/search.weft")
two.query = "dogs"
tools = List {}
tools.one = one.tool
tools.two = two.tool
shown = Debug { data: tools.list }
"#;

fn project_with_search_file() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("temp project");
    std::fs::create_dir_all(dir.path().join("lib")).expect("mkdir");
    std::fs::write(dir.path().join("lib/search.weft"), SEARCH_FILE).expect("write");
    dir
}

/// Every call of an included file hands out what the file writes.
#[test]
fn an_included_files_metadata_port_reaches_through_each_call() {
    let dir = project_with_search_file();
    let (project, diagnostics) =
        compile_strict(USES_THE_FILE, uuid::Uuid::new_v4(), CompileFs::disk(dir.path()), &catalog(), ValidationMode::Structural, None);
    assert!(error_codes(&diagnostics).is_empty(), "{diagnostics:#?}");
    let tools = node(&project, "tools");
    for site in ["one", "two"] {
        assert_eq!(tools.port_literals.get(site), Some(&json!({ "name": "search" })), "{site}");
    }
}

/// The editor's parse keeps each include as one opaque node, which carries
/// the file's metadata ports, so the wires from it resolve there too.
#[test]
fn the_editors_view_of_an_include_carries_its_metadata_ports() {
    let dir = project_with_search_file();
    let (project, diagnostics) =
        weft_compiler::parse_only(USES_THE_FILE, uuid::Uuid::new_v4(), CompileFs::disk(dir.path()), &catalog(), None);
    assert!(error_codes(&diagnostics).is_empty(), "{diagnostics:#?}");
    let one = node(&project, "one");
    assert_eq!(one.metadata_ports.len(), 1);
    assert_eq!(one.metadata_ports[0].value, Some(json!({ "name": "search" })));
    assert!(one.metadata_ports[0].span.is_none() && one.metadata_ports[0].declared_type.is_none(), "the file's, not this one's");
    assert_eq!(node(&project, "tools").port_literals.get("two"), Some(&json!({ "name": "search" })));
    assert_eq!(project.metadata_wires.len(), 2);
}

/// Written in the braces, a metadata wire is the same wire.
#[test]
fn a_metadata_wire_in_the_braces_is_the_same_wire() {
    let project = clean(&format!(
        "{TOOL}
search.query = \"cats\"
tools = List -> (list: List[{{ name: String, description: String }}]) {{ search: search.tool }}
shown = Debug {{ data: tools.list }}
"
    ));
    assert_eq!(node(&project, "tools").port_literals.get("search"), Some(&json!({ "name": "search", "description": "Search the web." })));
    assert_eq!(project.metadata_wires.len(), 1);
}

/// A key read off a metadata port is read off its value, as written.
#[test]
fn a_key_read_off_a_metadata_port_writes_that_key() {
    let project = clean(&format!(
        "{TOOL}
search.query = \"cats\"
said = Text {{}}
said.value = search.tool.name
shown = Debug {{ data: said.value }}
"
    ));
    assert_eq!(node(&project, "said").port_literals.get("value"), Some(&json!("search")));
}

/// A metadata port's value gathered from others: whole values, keys read
/// off them, and lists spliced in, worked out when the program compiles.
#[test]
fn a_metadata_value_gathers_other_metadata_ports() {
    let project = clean(
        r#"
toolbox = Group() -> (*tools: { a: List[{ name: String }], all: List[{ name: String }] }) {
  one = Group() -> (*tools: List[{ name: String }]) {
    self.tools = [{ "name": "one" }, { "name": "uno" }]
  }
  two = Group() -> (*tool: { name: String }) {
    self.tool = { "name": "two" }
  }
  self.tools = {
    "a": [...one.tools, two.tool],
    "all": [two.tool, ...one.tools, { "name": "three" }]
  }
}
tools = List {}
tools.first = toolbox.tools.all
shown = Debug { data: tools.list }
"#,
    );
    let toolbox = project.groups.iter().find(|g| g.id == "toolbox").unwrap();
    assert_eq!(
        toolbox.metadata_ports[0].value,
        Some(json!({
            "a": [{ "name": "one" }, { "name": "uno" }, { "name": "two" }],
            "all": [{ "name": "two" }, { "name": "one" }, { "name": "uno" }, { "name": "three" }]
        }))
    );
    assert_eq!(
        node(&project, "tools").port_literals.get("first"),
        Some(&json!([{ "name": "two" }, { "name": "one" }, { "name": "uno" }, { "name": "three" }]))
    );
    // Where each part comes from is the written value's own business:
    // only the List's input, which a metadata wire fills, has one.
    assert!(project.metadata_wires.iter().all(|e| e.target != "toolbox"), "{:#?}", project.metadata_wires);
    assert!(toolbox.metadata_ports[0].written.is_some());
}

#[test]
fn a_gathered_value_refuses_what_does_not_fit() {
    for (body, code) in [
        ("self.tools = [...two.tool]", "type-mismatch"),
        ("self.tools = [nowhere.tool]", "unknown-source-node"),
        ("self.tools = [two.other]", "unknown-source-port"),
        ("self.tools = [self.tools]", "parse"),
    ] {
        let source = format!(
            "g = Group() -> (*tools: List[{{ name: String }}]) {{\n  two = Group() -> (*tool: {{ name: String }}) {{\n    self.tool = {{ \"name\": \"two\" }}\n  }}\n  {body}\n}}\n"
        );
        refused(&source, code);
    }
}

/// A value an included file gathers from its own members reaches through
/// the include in the build, and in the editor's view, which keeps the
/// include as one opaque node and works the value out from the file.
#[test]
fn a_gathered_value_of_an_included_file_reaches_through_the_include() {
    let dir = tempfile::tempdir().expect("temp project");
    std::fs::create_dir_all(dir.path().join("lib")).expect("mkdir");
    std::fs::write(
        dir.path().join("lib/kit.weft"),
        r#"
Group() -> (*tools: List[{ name: String }]) {
  part = Group() -> (*tool: { name: String }) {
    self.tool = { "name": "part" }
  }
  self.tools = [part.tool]
}
"#,
    )
    .expect("write");
    let main = "kit = @include(\"lib/kit.weft\")\ntools = Debug {}\ntools.data = kit.tools\n";
    let (project, diagnostics) =
        weft_compiler::parse_only(main, uuid::Uuid::new_v4(), CompileFs::disk(dir.path()), &catalog(), None);
    assert!(error_codes(&diagnostics).is_empty(), "{diagnostics:#?}");
    assert_eq!(node(&project, "kit").metadata_ports[0].value, Some(json!([{ "name": "part" }])));
    assert_eq!(node(&project, "tools").port_literals.get("data"), Some(&json!([{ "name": "part" }])));
    let (project, diagnostics) =
        compile_strict(main, uuid::Uuid::new_v4(), CompileFs::disk(dir.path()), &catalog(), ValidationMode::Structural, None);
    assert!(error_codes(&diagnostics).is_empty(), "{diagnostics:#?}");
    assert_eq!(node(&project, "tools").port_literals.get("data"), Some(&json!([{ "name": "part" }])));
}

/// A gathered value is held to the type its port declares, item by item:
/// one spliced item of another shape is refused.
#[test]
fn a_gathered_value_is_held_to_its_declared_type() {
    let diagnostics = refused(
        r#"
g = Group() -> (*tools: List[{ name: String }]) {
  odd = Group() -> (*tools: List[{ id: Number }]) {
    self.tools = [{ "id": 1 }]
  }
  self.tools = [{ "name": "fine" }, ...odd.tools]
}
"#,
        "config-type-mismatch",
    );
    assert!(diagnostics.iter().any(|d| d.message.contains("g.tools")), "{diagnostics:#?}");
}

/// A metadata wire into a group input several members read is one value
/// written once, and every member gets it.
#[test]
fn a_metadata_wire_into_a_group_input_read_twice_is_written_once() {
    let project = clean(&format!(
        "{TOOL}
search.query = \"cats\"
h = Group(t: {{ name: String, description: String }}) {{
  a = Debug {{}}
  a.data = self.t
  b = Debug {{}}
  b.data = self.t
}}
h.t = search.tool
"
    ));
    let h = project.groups.iter().find(|g| g.id == "h").unwrap();
    assert!(h.port_literals.contains_key("t"));
    for member in ["h.a", "h.b"] {
        assert!(project.edges.iter().any(|e| e.target == member && e.source == "h"), "{member}: {:#?}", project.edges);
    }
    assert_eq!(project.metadata_wires.len(), 1, "{:#?}", project.metadata_wires);
}

/// A value that cannot be worked out is said once, where it is written:
/// the input its wire fills is not reported as missing a driver too, and
/// only wires that fill an input are kept for the editor.
#[test]
fn a_refused_value_is_said_once_and_only_filling_wires_are_kept() {
    let (project, diagnostics) = check(
        r#"
g = Group() -> (*tool: { name: String }) {
  self.tool = nowhere.tool
}
shown = Debug {}
shown.data = g.tool
"#,
    );
    assert_eq!(error_codes(&diagnostics), ["unknown-source-node"], "{diagnostics:#?}");
    assert_eq!(project.metadata_wires.len(), 1);
    let (project, _) = check(&format!(
        "{TOOL}
made = Text {{ value: \"x\" }}
search.tool = made.value
"
    ));
    assert!(project.metadata_wires.is_empty(), "a refused wire fills nothing: {:#?}", project.metadata_wires);
}

/// A metadata wire into a loop's input lands where a value typed there
/// does: on its `LoopIn` alone, which the editor reads for the loop box.
#[test]
fn a_metadata_wire_into_a_loop_input_lands_like_a_typed_value() {
    let project = clean(
        r#"
g = Group() -> (*tool: { name: String }) {
  self.tool = { "name": "g" }
}
each = Loop(items: List[String], tool: { name: String }) -> (seen: List[String | Null]) {
  over: ["items"]
  shown = Debug {}
  shown.data = self.tool
  echo = Text {}
  echo.value = self.items
  self.seen = echo.value
}
each.items = ["a"]
each.tool = g.tool
"#,
    );
    let tool = json!({ "name": "g" });
    assert_eq!(node(&project, "each__in").port_literals.get("tool"), Some(&tool));
    let each = project.groups.iter().find(|g| g.id == "each").unwrap();
    assert!(each.port_literals.is_empty(), "the loop keeps no copy: {:?}", each.port_literals);
}

/// A name in a gathered value reads like a name where it is written: a
/// member of the group, else the nearest enclosing scope's, else a
/// top-level one. The value keeps the id it resolved to.
#[test]
fn a_name_in_a_gathered_value_resolves_from_the_nearest_scope_out() {
    let project = clean(
        r#"
outer = Group() -> (*tool: { name: String }) {
  a = Group() -> (*tool: { name: String }) {
    self.tool = b.tool
  }
  b = Group() -> (*tool: { name: String }) {
    self.tool = { "name": "b" }
  }
  self.tool = a.tool
}
shown = Debug {}
shown.data = outer.tool
"#,
    );
    assert_eq!(node(&project, "shown").port_literals.get("data"), Some(&json!({ "name": "b" })));
    let a = project.groups.iter().find(|g| g.id == "outer.a").unwrap();
    assert_eq!(
        a.metadata_ports[0].written,
        Some(weft_core::project::MetadataExpr::Port { source: "outer.b".into(), port: "tool".into(), path: Vec::new() })
    );
}

/// `self.tool.key = ...` lands on a key, which a line never does: refused,
/// never read as the whole value.
#[test]
fn a_value_written_on_a_key_of_a_metadata_port_is_refused() {
    let (_, diagnostics) = check("g = Group() -> (*tool: { name: String }) {\n  self.tool.name = \"x\"\n}\n");
    assert!(diagnostics.iter().any(|d| d.message.contains("never on a key inside one")), "{diagnostics:#?}");
}

/// Two lines into one input are two drivers even when they come from one
/// metadata port, and a line refused on a group input many members read is
/// said once.
#[test]
fn two_lines_from_one_metadata_port_into_one_input_are_two_drivers() {
    refused(
        r#"
g = Group() -> (*tool: { a: String, b: String }) {
  self.tool = { "a": "x", "b": "y" }
}
shown = Debug {}
shown.data = g.tool.a
shown.data = g.tool.b
"#,
        "double-driven-port",
    );
    let (_, diagnostics) = check(
        r#"
g = Group() -> (*tool: String) {
  self.tool = "x"
}
h = Group(x: String) {
  a = Debug {}
  a.data = self.x
  b = Debug {}
  b.data = self.x
}
h.x = "typed"
h.x = g.tool
"#,
    );
    let twice = diagnostics.iter().filter(|d| d.code.as_deref() == Some("double-driven-port")).count();
    assert_eq!(twice, 1, "{diagnostics:#?}");
}

/// An included file is the top for the names in its values: a name it does
/// not declare never reaches into the program that includes it, and what is
/// wrong with its value is said once, however many sites include it.
#[test]
fn a_name_in_an_included_files_value_stays_in_that_file() {
    let dir = tempfile::tempdir().expect("temp project");
    std::fs::create_dir_all(dir.path().join("lib")).expect("mkdir");
    std::fs::write(dir.path().join("lib/a.weft"), "Group() -> (*tool: String) {\n  self.tool = helper.tool\n}\n").expect("write");
    let main = r#"
helper = Group() -> (*tool: String) {
  self.tool = "main's"
}
a = @include("lib/a.weft")
b = @include("lib/a.weft")
shown = Debug {}
shown.data = a.tool
other = Debug {}
other.data = b.tool
"#;
    let (_, diagnostics) =
        compile_strict(main, uuid::Uuid::new_v4(), CompileFs::disk(dir.path()), &catalog(), ValidationMode::Structural, None);
    // Said once, where the file writes it, however many sites read it.
    assert_eq!(error_codes(&diagnostics).iter().filter(|code| **code == "unknown-source-node").count(), 1, "{diagnostics:#?}");
}
