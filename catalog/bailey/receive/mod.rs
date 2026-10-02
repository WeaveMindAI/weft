//! BaileyReceive: fires when a WhatsApp message lands at the
//! project's bridge.
//!
//!   - `setup_trigger`: resolve the upstream bridge's handle, register
//!     an SSE signal on its `/events` stream. The
//!     listener subscribes; the dispatcher receives `message.received`
//!     events and fires fresh executions. The node's filter settings
//!     become predicates on the signal, so a message they exclude is
//!     dropped where it arrives and never starts a run.
//!
//!   - `run`: the SSE event delivers a parsed JSON object as the wake
//!     payload. Map the WhatsApp-specific fields to output ports.

use async_trait::async_trait;
use serde_json::Value;

use weft::infra::InfraHandle;
use weft::signal::{Predicate, SseSubscribe};
use weft::{ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct BaileyReceiveNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for BaileyReceiveNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    // Registers the SSE signal; setup emits nothing downstream.
    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let bridge: InfraHandle = ctx.inputs.get("bridge")?;
        let ignore_groups: bool = ctx.inputs.get("ignoreGroups")?;
        let message_types: Option<Vec<String>> = ctx.inputs.opt("messageTypes")?;

        let mut filters = Vec::new();
        if ignore_groups {
            // The bridge sends `isGroup: true` for a group chat and
            // `false` otherwise; `neq` keeps both `false` and an event
            // that omits the field.
            filters.push(Predicate::neq("isGroup", "true"));
        }
        if let Some(types) = message_types.filter(|t| !t.is_empty()) {
            // The `multiselect` widget's options bind every entry (the
            // compiler refuses a written one, the runtime a wired one),
            // so each is one of the bridge's plain words and the
            // alternation needs no escaping.
            filters.push(Predicate::regex("messageType", format!("^({})$", types.join("|"))));
        } else {
            // A reaction, a poll or a protocol message is `unknown` and
            // carries no content; a program that did not ask for it by
            // name must not run on it as if a person had written.
            filters.push(Predicate::neq("messageType", "unknown"));
        }

        let bridge = ctx.endpoint_of(&bridge).await?;
        ctx.register_signal(SseSubscribe {
            url: format!("{}/events", bridge.url().trim_end_matches('/')),
            event_name: "message.received".into(),
            filters,
        })
        .await
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // The SSE listener delivers the parsed `data:` object as the wake
        // payload. Fan the fields this node declares an output for onto
        // those ports (`isGroup` and `chatId` included); a payload field no
        // port declares is dropped. A missing field stays un-mentioned and
        // the engine closes its port at termination (substituting empty
        // strings / `false` would publish data indistinguishable from real
        // user values). A field the payload sends as null is skipped when
        // its port's type has no Null, so that port closes the same way.
        //
        // `file` is a port THIS node computes (a stored-file reference, only
        // on the media path below); it is never a payload field, and the
        // trigger's declared payload does not name it, so an event
        // smuggling one is refused before this body runs at all. Stripping
        // it here is the second line: it costs one statement and it holds
        // whatever the declaration says, so the port can only ever carry a
        // reference this node made.
        let mut data = ctx.wake.object()?.clone();
        data.remove("file");
        let data = Value::Object(data);
        let mut out = ctx.fan_declared(&data);

        // Media messages: stream the bytes from the bridge's media
        // endpoint straight into PROJECT storage under the message's
        // identity (see `media::fetch_media`) and emit the
        // self-describing stored-file reference on `file`. Bytes never ride
        // the pulse path; downstream nodes get/stream/presign via the
        // reference.
        let message_type = data
            .get("messageType")
            .and_then(|v| v.as_str())
            .node_err("message event without a messageType; the bridge always sends one")?;
        if MEDIA_TYPES.contains(&message_type) {
            let bridge: InfraHandle = ctx.inputs.get("bridge")?;
            let message_id = data
                .get("messageId")
                .and_then(|v| v.as_str())
                .node_err("media message without a messageId; cannot fetch its bytes")?;
            let bridge = ctx.endpoint_of(&bridge).await?;
            let file = super::media::fetch_media(&ctx, &bridge, message_id).await?;
            out = out.set("file", file);
        }
        ctx.pulse_downstream(out).await
    }
}

/// Message types whose bytes the bridge can serve via
/// `/media/<messageId>`.
// SYNC: MEDIA_TYPES <-> the media route's message chain in
// catalog/bailey/bridge/images/bridge/src/index.js (a type listed
// here that the route cannot serve is a 404 at fire time).
const MEDIA_TYPES: [&str; 5] = ["image", "video", "audio", "document", "sticker"];
