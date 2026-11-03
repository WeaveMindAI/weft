//! Route: trigger node that turns a weft program into an HTTP route. An
//! outside caller hits the route at the project's live URL; the
//! dispatcher matches the path and method, checks the caller against
//! the route's auth, holds the connection open and routes it to a
//! worker; this trigger fires a fresh run with the request on its
//! ports.
//!
//!   - `setup_trigger`: build a `Route` signal from the node's fields
//!     and register it. The dispatcher mounts the public route.
//!   - `run`: the caller is attached for this run. The opening request
//!     (`ctx.caller_request()`) fans onto the fixed ports, and the body, read off
//!     the caller handle (`HttpCaller::body_as`), lands whole on `body`,
//!     held to the type the program declared there (`-> (body: Signup)`, `JsonDict` when it
//!     declared none, `Json` for a body of any JSON shape): a picture it carries in a file-typed place is
//!     stored on the way in, an open record drops the keys it does not
//!     declare, and a body that does not fit is answered `400` naming
//!     what is wrong, before anything downstream runs. A port the
//!     program adds reads the path capture of its name (`-> (id:
//!     String)` on `cards/{id}`). Nothing else here answers the caller;
//!     Reply, Stream and Close do, or a custom node through
//!     `ctx.http_caller()`.

use async_trait::async_trait;
use serde_json::{Map, Value};
use weft::caller::InboundMessage;
use weft::signal::{DataType, LiveConnectionConfig, Route};
use weft::{RunContext, Node, NodeManifest, WeftResult};

use super::api;

#[derive(NodeManifest)]
pub struct RouteNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for RouteNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn setup_trigger(&self, ctx: RunContext) -> WeftResult<()> {
        let fields = ctx.inputs.object()?;
        // Named the way a person names it, for `weft connect <node>`.
        let place = weft::project::plain_id(&ctx.node_id);
        let common = LiveConnectionConfig::from_node_fields(fields, &place).map_err(weft::node_error)?;
        ctx.register_signal(Route { common }).await
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let request = ctx.caller_request()?;
        let http = ctx.http_caller().await?;
        // A live call leaves its body on the open connection. A `weft run
        // --fire` has none: the author typed the whole request, body
        // included, and it arrives as this trigger's wake payload like
        // every other declared field (`body?` in the metadata), which
        // `body_as` takes when the connection has nothing. A body that
        // does not fit is answered `400` and nothing downstream starts:
        // the caller already has its answer.
        let data_type = ctx.caller_data_type().unwrap_or_default();
        let declared = ctx.output_type("body")?;
        let body = match http.body_as(&ctx, &declared, "body", fired_body(&ctx, data_type)?).await? {
            Ok(body) => body,
            Err(why) => return http.refuse(why).await,
        };
        // A port the program added reads the path capture of its name
        // (the compiler holds every added port to a capture of the
        // path, `claimsRoute.capturesAreOutputs`). The fixed request
        // ports win over a capture of the same name. Route has no
        // `params` port, so a capture called `params` keeps its value.
        let mut fields: Map<String, Value> =
            request.params.iter().map(|(name, value)| (name.clone(), Value::String(value.clone()))).collect();
        let mut fixed = api::request_fields(&request);
        fixed.remove("params");
        fields.extend(fixed);
        let mut output = ctx.fan_declared(&Value::Object(fields));
        if let Some(body) = body {
            output = output.set("body", body);
        }
        ctx.pulse_downstream(output).await
    }
}

/// The body a FIRED run carries, off this trigger's own wake payload.
///
/// `body` is a field this node declares in its `firesWith`, optional
/// because a live call never has one there (it is on the connection)
/// and a GET never has one at all. Reading it here rather than having
/// something upstream serve it back as if a caller had sent it keeps
/// the question where it belongs: only this node knows that its `body`
/// field means the request's body.
///
/// The route's `dataType` says what the fired value is, as it says for
/// a live call: on a json route any value is the JSON body (a string
/// too), on a text route a string is the text and anything else is the
/// author's mistake, answered by `body_as`. A bytes route is not
/// fireable: a fire payload is JSON, and bytes never travel in one.
/// `None` when the payload names no body; a `body` of `null` is none
/// too, as a null is in any optional field of a fire payload.
fn fired_body(ctx: &RunContext, data_type: DataType) -> WeftResult<Option<InboundMessage>> {
    let Some(body) = ctx.wake.opt::<Value>("body")? else {
        return Ok(None);
    };
    Ok(Some(match (data_type, body) {
        (DataType::Text, Value::String(text)) => InboundMessage::Text(text),
        (_, other) => InboundMessage::Json(other),
    }))
}
