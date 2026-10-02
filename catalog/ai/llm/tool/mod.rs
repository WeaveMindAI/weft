//! LlmTool: declare one tool the model may call. No inference happens
//! here. Users wire one or more of these into an LLM node's `tools`
//! input; when the model decides to call one, the LLM node emits the
//! calls on its `toolCalls` output, the graph runs the tool's actual
//! work with its own nodes, and a Chat Message node (role: tool)
//! appends each result to the history for the next LLM call. The
//! emitted object is minillmlib's `ToolDefinition` shape verbatim.
// SYNC: emitted object <-> MiniLLMLibRS/src/tools/mod.rs ToolDefinition

use async_trait::async_trait;
use serde_json::{json, Map, Value};

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct LlmToolNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for LlmToolNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let name: String = ctx.inputs.get("name")?;
        let description: Option<String> = ctx.inputs.opt("description")?;
        // The schema arrives as a JSON object (the port's type holds it
        // to one, written or wired). A tool with no arguments is
        // legitimate (a "get the time" tool); leaving the schema out, or
        // writing `{}`, is the declared way to say so.
        let parameters: Value = match ctx.inputs.opt::<Map<String, Value>>("parameters")? {
            Some(schema) if !schema.is_empty() => Value::Object(schema),
            _ => json!({ "type": "object", "properties": {} }),
        };

        let mut tool = json!({ "name": name, "parameters": parameters });
        if let Some(description) = description {
            tool["description"] = json!(description);
        }
        ctx.pulse_downstream(NodeOutput::new().set("tool", tool)).await
    }
}
