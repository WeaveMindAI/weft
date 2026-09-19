//! JsonObject: the keys you wired, as one object.
//!
//! An object literal in source holds plain values only, so it cannot
//! carry a wire: `{ "job": open.id }` is refused at parse time. That
//! left one way to build an object out of values the graph computed,
//! which was a script whose entire body was `return {'body': {...}}`,
//! and the language calls that plumbing everywhere else. This node is
//! where it goes instead.
//!
//! Key ORDER is the order the inputs were written, and an object a
//! person reads (a reply body in a browser's network tab) reads better
//! with its keys where the author put them. Two things have to hold for
//! that: the keys are read in written order (`ctx.inputs.in_order()`,
//! since the bag's own values are sorted by name), and the object keeps
//! the order they are inserted in, which is serde_json's
//! `preserve_order` and is why this node's `deps.toml` asks for it.
//!
//! A key that never arrives is simply absent from the object, never
//! `null`: the two mean different things to whatever reads the JSON,
//! and a caller checking `"error" in body` must not be told yes by a
//! key we invented. Every created key is required unless it carries
//! `?`, so an absent REQUIRED key skips this node entirely (the engine
//! does that before the body runs) and nothing half-built escapes.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct JsonObjectNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for JsonObjectNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let mut object = serde_json::Map::new();
        // `in_order`, not `custom`: the bag's values are a sorted map, so
        // iterating them hands back alphabetical keys whatever the build
        // does. The written order lives in the port list behind the bag,
        // which is what this node promises to reproduce. (JsonObject has
        // no settings of its own, so every port here is a wired key.)
        for (key, value) in ctx.inputs.in_order()? {
            object.insert(key.clone(), value.clone());
        }
        // An empty object is almost always a node somebody added and
        // never wired, and it would travel on as `{}` with nothing on
        // screen saying why. The one case that wants a literal `{}` can
        // write it as a literal, since no wire is involved.
        if object.is_empty() {
            return Err(weft::node_error(
                "nothing is wired onto this JsonObject, so there are no keys to build. \
                 Wire a value onto it for each key you want, naming the key",
            ));
        }
        ctx.pulse_downstream(NodeOutput::new().set("object", serde_json::Value::Object(object)))
            .await
    }
}
