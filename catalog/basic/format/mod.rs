//! Format: fill a text template from the values wired into the node.
//!
//! The template names its holes (`{{user}}`) and the node's inline
//! signature declares a port per hole, so a prompt, a message body or
//! a file name assembled from graph values is one node with its
//! inputs visible on the graph, never a Python f-string.

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde_json::Value;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct FormatNode;

#[cfg(feature = "node-tests")]
mod tests;

/// A template is text and holes, in the order they appear.
#[derive(Debug, PartialEq)]
pub enum Piece {
    Text(String),
    Hole(String),
}

/// Read a template into its pieces, or say what is wrong with it.
///
/// A hole is `{{` + a port name + `}}`, spaces around the name
/// allowed. To write braces that stay braces (a prompt telling a model
/// to answer with `{{json}}`, say), double them: `{{{{json}}}}` renders
/// as `{{json}}` and asks for no port. A lone `}}` outside a hole is
/// refused like a lone `{{`: were it text, `}}}}` would be two braces
/// swallowed without a word.
pub fn parse_template(template: &str) -> WeftResult<Vec<Piece>> {
    let mut pieces: Vec<Piece> = Vec::new();
    let mut text = String::new();
    let mut rest = template;
    while !rest.is_empty() {
        // Doubled braces are the escape, and they are checked first so
        // `{{{{` never reads as a hole opening on `{{`.
        if let Some(after) = rest.strip_prefix("{{{{") {
            text.push_str("{{");
            rest = after;
            continue;
        }
        if let Some(after) = rest.strip_prefix("}}}}") {
            text.push_str("}}");
            rest = after;
            continue;
        }
        if rest.starts_with("}}") {
            weft::node_bail!(
                "the template closes a `}}}}` it never opened; every hole is `{{{{name}}}}`, and \
                 braces meant as text are doubled (`}}}}}}}}`)"
            );
        }
        let Some(after) = rest.strip_prefix("{{") else {
            let mut chars = rest.chars();
            match chars.next() {
                Some(c) => text.push(c),
                None => break,
            }
            rest = chars.as_str();
            continue;
        };
        let Some(close) = after.find("}}") else {
            weft::node_bail!(
                "the template opens a `{{{{` it never closes; every hole is `{{{{name}}}}`, and \
                 braces meant as text are doubled (`{{{{{{{{`)"
            );
        };
        let name = after[..close].trim();
        // The name has to be one the node can declare as a port: a
        // hole that swallowed another opening (`{{ a {{b}}`) or holds
        // a space would otherwise be refused later with advice to
        // declare a port that cannot be written.
        if !weft::is_rust_identifier(name) {
            weft::node_bail!(
                "the template holds a hole `{{{{{name}}}}}` that is not a port name; a hole is \
                 `{{{{name}}}}` with one name inside (letters, digits, underscores), and braces \
                 meant as text are doubled (`{{{{{{{{`)"
            );
        }
        if !text.is_empty() {
            pieces.push(Piece::Text(std::mem::take(&mut text)));
        }
        pieces.push(Piece::Hole(name.to_string()));
        rest = &after[close + 2..];
    }
    if !text.is_empty() {
        pieces.push(Piece::Text(text));
    }
    Ok(pieces)
}

/// The names the template reads, in order of first appearance, each
/// once: the ports this instance has to declare.
pub fn holes(pieces: &[Piece]) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for piece in pieces {
        if let Piece::Hole(name) = piece {
            if !names.iter().any(|n| n == name) {
                names.push(name.clone());
            }
        }
    }
    names
}

/// How a value reads inside text: a string as itself, anything else as
/// its JSON (a number as `7`, a list as `["a","b"]`).
fn rendered(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// The filled text. Every hole has a value by construction: the ports
/// were matched against the holes before this ran.
pub fn render(pieces: &[Piece], values: &BTreeMap<String, Value>) -> String {
    let mut out = String::new();
    for piece in pieces {
        match piece {
            Piece::Text(text) => out.push_str(text),
            Piece::Hole(name) => {
                if let Some(value) = values.get(name) {
                    out.push_str(&rendered(value));
                }
            }
        }
    }
    out
}

#[async_trait]
impl Node for FormatNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let template: String = ctx.inputs.get("template")?;
        let pieces = parse_template(&template)?;
        let names = holes(&pieces);
        // The holes and the node's own custom ports have to match both
        // ways; the ctx owns that matching (the SQL node has the same
        // job with `$name` holes) and names whichever side is short.
        let values = ctx.inputs.for_holes(
            &names,
            "template",
            |name| format!("{{{{{name}}}}}"),
            |name| format!("Format({name}: String) {{ ... }}"),
        )?;
        let by_name: BTreeMap<String, Value> =
            names.iter().cloned().zip(values.into_iter().cloned()).collect();
        ctx.pulse_downstream(NodeOutput::new().set("text", render(&pieces, &by_name))).await
    }
}
