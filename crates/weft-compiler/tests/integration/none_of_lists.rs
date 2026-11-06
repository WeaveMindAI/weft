//! Layer-3: `noneOf` and `noneMatch` rules reading a list another node
//! declares (`"lists"` in its metadata), through the real compiler and a
//! fixture catalog. `Bot` declares the names it cannot give up and the
//! patterns of names it refuses; `Asker` refuses a tool named with one of
//! the names, or matching one of the patterns, naming the bot, when the
//! bot is wired into its `agent`. The patterns are `optional`: `Quiet`
//! declares only names. A list of tools that arrives on a wire is
//! recorded with the bot's lists written in and asked when `Asker` fires.
use weft_compiler::CompileFs;

const REFUSAL: &str = "{wiredNode} cannot give up its built-in tool {noneOfFound}; rename your tool on '{id}'";
const PATTERN_REFUSAL: &str = "{wiredNode} refuses tool names starting `mcp__`, so '{id}' cannot hand it {noneMatchFound}";

fn catalog() -> (tempfile::TempDir, weft_catalog::FsCatalog) {
    let dir = tempfile::tempdir().unwrap();
    let node = |name: &str, metadata: serde_json::Value| {
        let at = dir.path().join(name);
        std::fs::create_dir_all(&at).unwrap();
        std::fs::write(at.join("mod.rs"), "// node impl\n").unwrap();
        std::fs::write(at.join("metadata.json"), metadata.to_string()).unwrap();
    };
    node(
        "bot",
        serde_json::json!({
            "type": "Bot", "label": "Bot", "description": "a coding agent with built-in tools",
            "outputs": [{ "name": "agent", "type": "String" }],
            "lists": { "reservedToolNames": ["Read", "Task"], "reservedToolNamePatterns": ["^mcp__", "[^a-zA-Z0-9_-]"] }
        }),
    );
    node(
        "quiet",
        serde_json::json!({
            "type": "Quiet", "label": "Quiet", "description": "an agent refusing no pattern",
            "outputs": [{ "name": "agent", "type": "String" }],
            "lists": { "reservedToolNames": [] }
        }),
    );
    node(
        "garbled",
        serde_json::json!({
            "type": "Garbled", "label": "Garbled", "description": "an agent listing a pattern that is no regex",
            "outputs": [{ "name": "agent", "type": "String" }],
            "lists": { "reservedToolNames": [], "reservedToolNamePatterns": ["("] }
        }),
    );
    node(
        "asker",
        serde_json::json!({
            "type": "Asker", "label": "Asker", "description": "asks an agent",
            "inputs": [
                { "name": "agent", "type": "String" },
                { "name": "tools", "type": "List[JsonDict]", "required": false }
            ],
            "validate": [{
                "when": { "kind": "all", "of": [
                    { "kind": "value", "field": "tools" },
                    { "kind": "not", "of": { "kind": "value", "field": "tools",
                        "each": { "key": "name", "noneOf": { "list": "reservedToolNames", "ofNodeWiredInto": "agent" } } } }
                ] },
                "then": { "message": REFUSAL, "port": "tools" }
            }, {
                "when": { "kind": "all", "of": [
                    { "kind": "value", "field": "tools" },
                    { "kind": "not", "of": { "kind": "value", "field": "tools",
                        "each": { "key": "name", "noneMatch": { "list": "reservedToolNamePatterns", "ofNodeWiredInto": "agent", "optional": true } } } }
                ] },
                "then": { "message": PATTERN_REFUSAL, "port": "tools" }
            }]
        }),
    );
    node(
        "lister",
        serde_json::json!({
            "type": "Lister", "label": "Lister", "description": "gives tools",
            "outputs": [{ "name": "tools", "type": "List[JsonDict]" }]
        }),
    );
    node(
        "plain",
        serde_json::json!({
            "type": "Plain", "label": "Plain", "description": "an agent declaring no list",
            "outputs": [{ "name": "agent", "type": "String" }]
        }),
    );
    let catalog = weft_catalog::FsCatalog::discover(dir.path()).expect("fixture catalog");
    (dir, catalog)
}

fn compiled(main: &str) -> weft_core::ProjectDefinition {
    let (_nodes, catalog) = catalog();
    let project = tempfile::tempdir().unwrap();
    weft_compiler::compile_program(main, uuid::Uuid::nil(), CompileFs::disk(project.path()), &catalog).expect("compiles")
}

fn broken(main: &str) -> Vec<String> {
    compiled(main).broken.iter().map(|part| part.why.clone()).collect()
}

/// A written tool named like one of the bot's own breaks `ask`, naming
/// the bot, its type and the name; a new name compiles; with no bot wired
/// in there is no list, and nothing to refuse.
#[test]
fn a_tool_the_wired_agent_reserves_is_refused_naming_the_agent() {
    let refused = broken("bot = Bot\nask = Asker { tools: [{ \"name\": \"search\" }, { \"name\": \"Read\" }] }\nask.agent = bot.agent\n");
    assert_eq!(refused.len(), 1, "{refused:?}");
    assert!(refused[0].ends_with("Bot 'bot' cannot give up its built-in tool 'Read'; rename your tool on 'ask'"), "{refused:?}");
    assert!(broken("bot = Bot\nask = Asker { tools: [{ \"name\": \"search\" }] }\nask.agent = bot.agent\n").is_empty());
    assert!(broken("ask = Asker { agent: \"x\", tools: [{ \"name\": \"Read\" }] }\n").is_empty(), "no node wired into `agent`");
}

/// A node wired into `agent` that declares no list of that name breaks
/// the program at both ends, naming the list, the reader and the input.
#[test]
fn a_wired_node_declaring_no_such_list_is_refused() {
    let refused = broken("bare = Plain\nask = Asker { tools: [{ \"name\": \"Read\" }] }\nask.agent = bare.agent\n");
    assert!(
        refused.iter().any(|why| why.ends_with("Plain 'bare' declares no list 'reservedToolNames' that Asker 'ask' reads through `agent`")),
        "{refused:?}"
    );
}

/// Tools on a wire: the rule is recorded with the bot's names written in,
/// and the firing refuses a reserved name that arrived.
#[test]
fn tools_on_a_wire_are_asked_with_the_list_written_in() {
    let program = compiled("bot = Bot\nsrc = Lister\nask = Asker\nask.agent = bot.agent\nask.tools = src.tools\n");
    let ask = program.nodes.iter().find(|n| n.id == "ask").expect("ask");
    assert_eq!(ask.recheck.len(), 2, "the names and the patterns: {:?}", ask.recheck);
    let when = serde_json::to_value(&ask.recheck[0].when).unwrap();
    assert!(when.to_string().contains(r#""noneOf":["Read","Task"]"#), "{when}");
    let arrived = |tools: serde_json::Value| serde_json::json!({ "tools": tools }).as_object().cloned().unwrap();
    match weft_core::rules::recheck(ask, &program, &arrived(serde_json::json!([{ "name": "Task" }]))) {
        Err(weft_core::error::WeftError::Input(m)) => {
            assert_eq!(m, "Bot 'bot' cannot give up its built-in tool 'Task'; rename your tool on 'ask'")
        }
        other => panic!("a reserved name that arrived fails the firing, got {other:?}"),
    }
    assert!(weft_core::rules::recheck(ask, &program, &arrived(serde_json::json!([{ "name": "search" }]))).is_ok());
}

/// A written tool whose name matches one of the bot's patterns breaks
/// `ask`, naming the bot and the tool; an agent declaring no patterns
/// (the list is `optional`) refuses none and breaks nothing.
#[test]
fn a_tool_name_matching_the_wired_agents_pattern_is_refused() {
    let tools = "[{ \"name\": \"search\" }, { \"name\": \"mcp__files\" }, { \"name\": \"read file\" }]";
    let refused = broken(&format!("bot = Bot\nask = Asker {{ tools: {tools} }}\nask.agent = bot.agent\n"));
    assert_eq!(refused.len(), 1, "{refused:?}");
    assert!(
        refused[0].ends_with("Bot 'bot' refuses tool names starting `mcp__`, so 'ask' cannot hand it 'mcp__files', 'read file'"),
        "{refused:?}"
    );
    assert!(broken(&format!("quiet = Quiet\nask = Asker {{ tools: {tools} }}\nask.agent = quiet.agent\n")).is_empty(), "no patterns, nothing refused");
}

/// A pattern the wired node lists that is no regex breaks the program,
/// naming the node, the pattern and the list.
#[test]
fn a_wired_pattern_that_is_no_regex_is_refused() {
    let refused = broken("bad = Garbled\nask = Asker { tools: [{ \"name\": \"x\" }] }\nask.agent = bad.agent\n");
    assert!(
        refused.iter().any(|why| why.contains("Garbled 'bad' lists '(' is no regex (")
            && why.ends_with("in 'reservedToolNamePatterns', which Asker 'ask' matches against through `agent`")),
        "{refused:?}"
    );
}

/// Tools on a wire: the pattern rule is recorded with the bot's patterns
/// written in, and the firing refuses a matching name that arrived.
#[test]
fn tools_on_a_wire_are_matched_with_the_patterns_written_in() {
    let program = compiled("bot = Bot\nsrc = Lister\nask = Asker\nask.agent = bot.agent\nask.tools = src.tools\n");
    let ask = program.nodes.iter().find(|n| n.id == "ask").expect("ask");
    assert_eq!(ask.recheck.len(), 2, "{:?}", ask.recheck);
    let when = serde_json::to_value(&ask.recheck[1].when).unwrap();
    assert!(when.to_string().contains(r#""noneMatch":["^mcp__","[^a-zA-Z0-9_-]"]"#), "{when}");
    let arrived = |tools: serde_json::Value| serde_json::json!({ "tools": tools }).as_object().cloned().unwrap();
    match weft_core::rules::recheck(ask, &program, &arrived(serde_json::json!([{ "name": "mcp__x" }]))) {
        Err(weft_core::error::WeftError::Input(m)) => {
            assert_eq!(m, "Bot 'bot' refuses tool names starting `mcp__`, so 'ask' cannot hand it 'mcp__x'")
        }
        other => panic!("a matching name that arrived fails the firing, got {other:?}"),
    }
}
