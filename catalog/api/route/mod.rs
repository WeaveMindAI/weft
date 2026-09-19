//! Route: trigger node that turns a weft program into an HTTP route. An
//! outside caller hits the route at the project's live URL; the
//! dispatcher matches the path and method, checks the caller against
//! the route's auth, holds the connection open and routes it to a
//! worker; this trigger fires a fresh execution with the request on its
//! ports.
//!
//!   - `setup_trigger`: build a `Route` signal from the node's fields
//!     and register it. The dispatcher mounts the public route.
//!   - `run`: the caller is attached for this run. The opening request
//!     (`ctx.wake`) fans onto the fixed ports; the body, read off the
//!     caller handle, fans onto the ports the author declared: a JSON
//!     body's top-level keys each on their port, never the whole object
//!     (a route declares what it receives, and a picture the body
//!     carries must land on a file port, where it is stored instead of
//!     riding a wire as bytes), a text or byte body whole on the one
//!     declared port. A
//!     declared port named like a path capture (`-> (id: String)` on
//!     `cards/{id}`) reads the capture, over a body key of that name.
//!     On a text or bytes route that one port IS the whole body, so
//!     the same collision is refused instead of eating the payload. A
//!     picture on a file-kind port is stored on the way in
//!     (`wire::inline_files`). Nothing here answers the caller; Reply,
//!     Stream and Close do, or a custom node through `ctx.http_caller()`.

use async_trait::async_trait;
use serde_json::Value;
use weft::caller::InboundMessage;
use weft::node::NodeOutput;
use weft::signal::{DataType, LiveConnectionConfig, Route};
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::wire::{self, REQUEST_PORTS};

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

    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let common = LiveConnectionConfig::from_node_fields(ctx.inputs.object()?).map_err(weft::node_error)?;
        ctx.register_signal(Route { common }).await
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let request = wire::opening_request(&ctx)?;
        let http = ctx.http_caller().await?;
        let parts = http.request_parts()?;
        // Where the body comes from, and why this node decides it.
        //
        // A live call leaves its body on the open connection, and that
        // is `parts.body`. A `weft run --fire` has no connection: the
        // author typed the whole request, body included, and it arrives
        // as this trigger's wake payload like every other declared
        // field (`body?` in the metadata).
        //
        // So: the connection first, the wake when the connection has
        // nothing. Nothing upstream cuts the body out of the payload to
        // feed it back through the connection call, because that would
        // put a caller trigger's field name in the language, where no
        // node's vocabulary belongs.
        let body = match &parts.body {
            InboundMessage::Json(v) if v.is_null() => fired_body(&ctx)?,
            other => other.clone(),
        };
        let data_type = ctx.caller_data_type().unwrap_or_default();
        let body_ports = declared_body_ports(&ctx);
        let output = match (data_type, body) {
            (DataType::Json, InboundMessage::Json(body)) => {
                // A json route fans the body's top-level KEYS onto ports of
                // the same name, so a body that has no keys has nothing to
                // fan: a bare list or string would leave every declared port
                // silent, the branches behind them closed, and the caller
                // with no answer and nothing anywhere saying why. Refused by
                // name instead.
                //
                // NULL is not that. It is how a request with no body at all
                // arrives, which is every GET, and a GET is an ordinary way
                // to call a route: the request ports below still fire, the
                // body ports simply stay silent because there was no body
                // to fill them from. Refusing it would break every route
                // somebody reads rather than writes to.
                if !body.is_null() && !body.is_object() {
                    weft::node_bail!(
                        "this route's body lands key by key on its declared ports, so it has \
                         to be a json object; the caller sent {}. Either send an object, or \
                         set the body shape to `text` and read it whole",
                        shape_of_json(&body)
                    );
                }
                // Top-level keys onto same-named declared ports. The fixed
                // request ports are set after, so they win over a body key.
                ctx.fan_declared(&body)
            }
            (DataType::Json, other) => weft::node_bail!(
                "the trigger declares a json body but the connection delivered {}",
                shape_of(&other)
            ),
            (data_type, message) => {
                // `text` and `bytes`: the whole body on the ONE declared
                // port; none declared means the body is ignored. The port
                // names the stored file and is what the bytes are held
                // to, so an `Image` port refuses a zip here rather than
                // deeper in, where the caller's own words no longer are.
                let (name, declared) = match body_ports.as_slice() {
                    [port] => (
                        port.as_str(),
                        ctx.declared_outputs().get(port).cloned(),
                    ),
                    _ => ("body", None),
                };
                let value = wire::value_of(
                    &ctx,
                    message,
                    request.header("content-type"),
                    name,
                    declared.as_ref(),
                )
                .await?;
                match body_ports.as_slice() {
                    [] => NodeOutput::new(),
                    [port] => {
                        // The capture fold below writes over a body port
                        // of the same name, which is what a json route
                        // wants (a capture beats a body key). Here that
                        // one port is the caller's WHOLE body, so the
                        // same rule would drop the payload without a
                        // word.
                        if request.params.contains_key(port) {
                            weft::node_bail!(
                                "the port '{port}' is both where a {} route's whole body lands \
                                 and the path capture `{{{port}}}`, and one would quietly \
                                 overwrite the other; rename the port, or name the capture in \
                                 `path` something else",
                                data_type.as_wire_str()
                            );
                        }
                        NodeOutput::new().set(port.clone(), value)
                    }
                    many => weft::node_bail!(
                        "a {} route delivers its whole body on one declared port, but {} are \
                         declared ({}); keep one",
                        data_type.as_wire_str(),
                        many.len(),
                        many.join(", ")
                    ),
                }
            }
        };
        // A capture read through a declared port of its name: part of
        // the fixed request, so it wins over a body key, and the fixed
        // ports still win over it.
        let output = request
            .params
            .iter()
            .filter(|(name, _)| body_ports.contains(name))
            .fold(output, |out, (name, value)| out.set(name.clone(), Value::String(value.clone())));
        let output = wire::inline_files(&ctx, output).await?;
        let output = wire::request_ports(&request)
            .into_iter()
            .fold(output, |out, (port, value)| out.set(port, value));
        ctx.pulse_downstream(output).await
    }
}

/// The ports the author declared beyond the fixed request ports: where
/// the body lands.
fn declared_body_ports(ctx: &ExecutionContext) -> Vec<String> {
    let mut ports: Vec<String> = ctx
        .declared_outputs()
        .keys()
        .filter(|name| !REQUEST_PORTS.contains(&name.as_str()))
        .cloned()
        .collect();
    ports.sort();
    ports
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
/// A string is a text body, an object is a json one. Anything else is
/// refused by the fire payload contract before this runs, against the
/// type the metadata declares.
// TODO: once weft types have a `Bytes` primitive, the declaration
// becomes `body?: JsonDict | String | Bytes` and a fired run can carry
// a binary body. Until then a bytes route is not fireable, which the
// contract says by refusing the value rather than by a check here.
fn fired_body(ctx: &ExecutionContext) -> WeftResult<InboundMessage> {
    let Some(body) = ctx.wake.opt::<Value>("body")? else {
        return Ok(InboundMessage::Json(Value::Null));
    };
    Ok(match body {
        Value::String(text) => InboundMessage::Text(text),
        other => InboundMessage::Json(other),
    })
}

/// What a json body turned out to be, for the refusal above. Named for
/// the caller's benefit, not serde's: they sent it and have to recognise
/// it in the message.
fn shape_of_json(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "true or false",
        serde_json::Value::Number(_) => "a number",
        serde_json::Value::String(_) => "a string",
        serde_json::Value::Array(_) => "a list",
        serde_json::Value::Object(_) => "an object",
    }
}

fn shape_of(message: &InboundMessage) -> &'static str {
    match message {
        InboundMessage::Json(_) => "json",
        InboundMessage::Text(_) => "text",
        InboundMessage::Bytes(_) => "bytes",
    }
}
