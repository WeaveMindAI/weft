//! Layer-3: a `@file` compiled the way a build compiles it
//! (`weft_compiler::compile_program`) and the way the editor reads it
//! (`weft_compiler::parse_only`), with the real compiler and the standard
//! catalog. The build's program names the file's content
//! (`weft_core::content`); the editor's holds the text.
use weft_compiler::CompileFs;
use weft_core::content::ContentRef;

const MAIN: &str = "tick = Cron { cron: \"0 0 * * * *\" }
a = Format(p: String) { template: @file(\"prompts/t.txt\"), p: tick.scheduledTime }
d = Debug { data: a.text }
";

fn catalog() -> weft_catalog::FsCatalog {
    weft_catalog::FsCatalog::discover(&weft_catalog::stdlib_root().unwrap()).unwrap()
}

/// The program a build of `MAIN` makes with `template` as the file.
fn built(template: &str) -> weft_core::ProjectDefinition {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("prompts")).unwrap();
    std::fs::write(dir.path().join("prompts/t.txt"), template).unwrap();
    let catalog = catalog();
    let (program, contents) =
        weft_compiler::compile_program_with_contents(MAIN, uuid::Uuid::nil(), CompileFs::disk(dir.path()), &catalog).expect("compiles");
    // The build's own gate reads the program with its contents put back.
    let left = weft_compiler::unsettled(&program, &contents, &catalog, CompileFs::disk(dir.path()));
    assert!(left.is_empty(), "{left:?}");
    program
}

/// A check that reads a literal's value reads the file's value at the
/// build's gate too: a gate read from a file is a Boolean there, as it
/// was when the program compiled. Without the contents, the gate names
/// the content it was not handed.
#[test]
fn the_builds_gate_reads_the_value_a_file_holds() {
    let main = "a = Text { value: \"hi\" }\noff = Debug\noff.data = a.value\noff._should_flow = @file(\"on.txt\", Boolean)\n";
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("on.txt"), "false").unwrap();
    let catalog = catalog();
    let (program, contents) =
        weft_compiler::compile_program_with_contents(main, uuid::Uuid::nil(), CompileFs::disk(dir.path()), &catalog).expect("compiles");
    assert!(program.broken.is_empty(), "{:?}", program.broken);
    let gate = program.nodes.iter().find(|node| node.id == "off").unwrap().port_literals["_should_flow"].clone();
    assert!(ContentRef::from_value(&gate).is_some(), "the built gate names the file's content: {gate}");
    let left = weft_compiler::unsettled(&program, &contents, &catalog, CompileFs::disk(dir.path()));
    assert!(left.is_empty(), "{left:?}");
    let unread = weft_compiler::unsettled(&program, &Default::default(), &catalog, CompileFs::disk(dir.path()));
    assert!(unread.iter().any(|d| d.message.contains("'on.txt'")), "{unread:?}");
}

/// Each file is named where it was read, by its own path: two files
/// holding the same text are two names, and a value typed in the program
/// that equals a file's text stays the value.
#[test]
fn each_file_is_named_where_it_was_read() {
    let main = "tick = Cron { cron: \"0 0 * * * *\" }\n\
                a = Format(p: String) { template: @file(\"one.txt\"), p: tick.scheduledTime }\n\
                b = Format(p: String) { template: @file(\"two.txt\"), p: tick.scheduledTime }\n\
                c = Format(p: String) { template: \"same {{p}}\", p: tick.scheduledTime }\n";
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("one.txt"), "same {{p}}").unwrap();
    std::fs::write(dir.path().join("two.txt"), "same {{p}}").unwrap();
    let program = weft_compiler::compile_program(main, uuid::Uuid::nil(), CompileFs::disk(dir.path()), &catalog()).expect("compiles");
    let path_of = |id: &str| {
        let node = program.nodes.iter().find(|node| node.id == id).unwrap();
        ContentRef::from_value(node.written_value("template").unwrap()).map(|content| content.path().to_string())
    };
    assert_eq!(path_of("a").as_deref(), Some("one.txt"));
    assert_eq!(path_of("b").as_deref(), Some("two.txt"));
    assert_eq!(path_of("c"), None, "a typed value is no file's");
}

fn template_of(program: &weft_core::ProjectDefinition) -> &serde_json::Value {
    let node = program.nodes.iter().find(|node| node.id == "a").expect("the Format node");
    node.written_value("template").expect("its template")
}

/// A large file costs the built program a hash-sized value, named by the
/// content and the type it is read as; its definition hash follows the
/// content: the same with the same file, another after one byte changed.
#[test]
fn a_built_program_names_a_files_content_and_hashes_by_it() {
    let large = format!("{{{{p}}}}\n{}", "a line of a large template\n".repeat(80_000));
    let program = built(&large);
    let content = ContentRef::from_value(template_of(&program)).expect("the template is named by its content");
    assert_eq!(content.sha256(), weft_core::project::hash::sha256_hex(large.as_bytes()));
    assert_eq!(content.path(), "prompts/t.txt");
    assert_eq!(content.value_of(large.as_bytes()).unwrap(), serde_json::json!(large));
    let json = serde_json::to_string(&program).unwrap();
    assert!(json.len() < large.len() / 10, "the program holds the name, not the text: {} bytes", json.len());

    let hash = |program: &weft_core::ProjectDefinition| weft_core::project::hash::compute_definition_hash(program).unwrap();
    assert_eq!(hash(&program), hash(&built(&large)));
    let mut one_byte = large.clone();
    one_byte.push('!');
    assert_ne!(hash(&program), hash(&built(&one_byte)));
}

/// The editor reads the text: it edits the file, and shows what it holds.
#[test]
fn the_editor_reads_a_files_text() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("prompts")).unwrap();
    std::fs::write(dir.path().join("prompts/t.txt"), "hello {{p}}").unwrap();
    let (program, _) = weft_compiler::parse_only(MAIN, uuid::Uuid::nil(), CompileFs::disk(dir.path()), &catalog(), None);
    assert_eq!(template_of(&program), &serde_json::json!("hello {{p}}"));
    let node = program.nodes.iter().find(|node| node.id == "a").unwrap();
    assert_eq!(node.file_refs["template"].path, "prompts/t.txt", "the field is recorded as the file's");
}

/// A program never writes the marker itself: it would name content its
/// author never stored.
#[test]
fn a_content_marker_written_by_hand_is_refused() {
    let hash = weft_core::project::hash::sha256_hex(b"x");
    let main = format!(
        "tick = Cron {{ cron: \"0 0 * * * *\" }}\na = Format(p: String) {{ template: {{\"__weft_content__\": {{\"sha256\": \"{hash}\", \"type\": \"String\", \"path\": \"x\"}}}}, p: tick.scheduledTime }}\n"
    );
    let dir = tempfile::tempdir().unwrap();
    let program = weft_compiler::compile_program(&main, uuid::Uuid::nil(), CompileFs::disk(dir.path()), &catalog()).expect("compiles around it");
    assert!(
        program.broken.iter().any(|part| part.parts == ["a"] && part.why.contains("only a build writes")),
        "{:?}",
        program.broken
    );
}

/// A loop's `@file` input is read like a node's: the build names it on the
/// loop's `LoopIn`, the editor's parse holds the text there with the field
/// recorded as the file's (what the editor writes an edit back through),
/// and the loop keeps no copy of its own, so no `@file(...)` text is left
/// anywhere in either program.
#[test]
fn a_loops_file_input_is_named_in_the_build_and_read_as_text_by_the_editor() {
    let main = "each = Loop(items: List[String], prefix: String) -> (seen: List[String | Null]) {
  over: [\"items\"]
  echo = Text {}
  echo.value = self.prefix
  self.seen = echo.value
}
each.items = [\"a\"]
each.prefix = @file(\"prefix.txt\")
";
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("prefix.txt"), "hello").unwrap();
    let catalog = catalog();
    let loop_in = |program: &weft_core::ProjectDefinition| program.nodes.iter().find(|node| node.id == "each__in").expect("the loop's LoopIn").clone();
    let no_copy = |program: &weft_core::ProjectDefinition| {
        let each = program.groups.iter().find(|group| group.id == "each").expect("the loop");
        assert!(each.port_literals.is_empty(), "the loop keeps no copy: {:?}", each.port_literals);
        let json = serde_json::to_string(program).unwrap();
        assert!(!json.contains("@file("), "no raw `@file` text: {json}");
    };

    let (program, contents) =
        weft_compiler::compile_program_with_contents(main, uuid::Uuid::nil(), CompileFs::disk(dir.path()), &catalog).expect("compiles");
    assert!(program.broken.is_empty(), "{:?}", program.broken);
    let content = ContentRef::from_value(&loop_in(&program).port_literals["prefix"]).expect("the loop's input is named by its content");
    assert_eq!(content.path(), "prefix.txt");
    assert_eq!(contents.get(&content.id()).map(|value| (**value).clone()), Some(serde_json::json!("hello")));
    no_copy(&program);
    let left = weft_compiler::unsettled(&program, &contents, &catalog, CompileFs::disk(dir.path()));
    assert!(left.is_empty(), "{left:?}");

    let (parsed, _) = weft_compiler::parse_only(main, uuid::Uuid::nil(), CompileFs::disk(dir.path()), &catalog, None);
    let loop_in = loop_in(&parsed);
    assert_eq!(loop_in.port_literals["prefix"], serde_json::json!("hello"));
    assert_eq!(loop_in.file_refs["prefix"].path, "prefix.txt", "the field is recorded as the file's");
    no_copy(&parsed);
}
