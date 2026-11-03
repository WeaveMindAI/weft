//! What the api package's nodes share on top of weft's caller layer
//! (`weft::caller`, which turns values into what a caller receives and
//! back): the request fields a trigger fans onto its ports, and the head
//! an answering node sets, which has no meaning behind a socket.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use weft::caller::LiveRequest;
use weft::{NodeErrExt, RunContext, WeftResult};

/// The opening request as the fields a trigger fans onto its ports
/// (`ctx.fan_declared`, which keeps only the ports the trigger declares):
/// `fired` is always true, a port a program can gate on "the trigger was
/// called" without reading any request part. Header names are
/// lowercased, as HTTP reads them. The headers that carry a credential
/// are on `credentials`, as the secrets the door made of them
/// (`LiveRequest::credentials`), never on `headers`.
pub fn request_fields(request: &LiveRequest) -> Map<String, Value> {
    let headers: Map<String, Value> =
        request.headers.iter().map(|(k, v)| (k.to_ascii_lowercase(), Value::String(v.clone()))).collect();
    let credentials: Map<String, Value> =
        request.credentials.iter().map(|(k, secret)| (k.to_ascii_lowercase(), secret.to_value())).collect();
    let mut fields = Map::new();
    fields.insert("fired".into(), Value::Bool(true));
    fields.insert("method".into(), Value::String(request.method.clone()));
    fields.insert("path".into(), Value::String(request.path.clone()));
    fields.insert("params".into(), serde_json::to_value(&request.params).expect("a string map serializes"));
    fields.insert("query".into(), serde_json::to_value(&request.query).expect("a string map serializes"));
    fields.insert("headers".into(), Value::Object(headers));
    fields.insert("credentials".into(), Value::Object(credentials));
    fields.insert("caller".into(), request.caller.clone().unwrap_or(Value::Null));
    fields
}

/// The `headers` input of an answering node, every secret in it revealed
/// (its type, `Dict[String, String | Secret[String]]`, says where): a
/// header the caller receives carries the value, while the run's record
/// keeps the marker. Empty when nothing was set.
pub async fn headers(ctx: &RunContext) -> WeftResult<BTreeMap<String, String>> {
    let Some(headers) = ctx.inputs.opt::<Value>("headers")? else { return Ok(BTreeMap::new()) };
    serde_json::from_value(ctx.reveal_input("headers", headers).await?).node_err("the revealed headers")
}

/// Refuse a status or headers behind a Socket: a socket message has no
/// status line, so a program that set one meant a Route. `status` is
/// the node's default (200) and `headers` empty when nothing was set;
/// anything else is the program's bug, said loud.
pub fn refuse_head_on_socket(node: &str, status: u16, headers: &BTreeMap<String, String>) -> WeftResult<()> {
    if status != 200 || !headers.is_empty() {
        weft::node_bail!(
            "{node} behind a Socket sends messages; a status ({status}) or headers have no \
             meaning on a socket. Drop them, or put the {node} behind a Route"
        );
    }
    Ok(())
}
