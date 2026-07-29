//! Persistent bidirectional outbound WebSocket handler: parses the
//! kind's config and drives the shared [`crate::socket_engine`]. The
//! address is either static or minted per cycle by the declared
//! `connect` call; the optional connection (`SignalSpec.access`)
//! supplies the values the mint call and the frames interpolate,
//! resolved freshly on every reconnect. The SERVICE protocol
//! (op-codes, envelopes) stays the author's concern, carried as the
//! literal frames and reply rules in the spec.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use base64::Engine as _;
use dashmap::DashMap;
use futures_util::FutureExt;
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use weft_core::primitive::{AccessRef, SignalAuth, SignalRouting, SignalSpec, SignalSurface};
use weft_core::signal::{Signal, SocketFrame, SocketListen};

use crate::protocol::{ProcessOutcome, ProcessTarget};
use crate::registry::RegisteredSignal;
use crate::socket_engine::{self, CyclePlan, PrepareError};

use super::{KindHandler, SpawnCtx};

pub struct SocketListenHandler;

#[async_trait]
impl KindHandler for SocketListenHandler {
    fn tag(&self) -> &'static str {
        SocketListen::TAG
    }

    fn compute_routing(
        &self,
        _token: &str,
        _spec: &SignalSpec,
        _secret_cache: &Arc<DashMap<String, String>>,
    ) -> Result<SignalRouting> {
        Ok(SignalRouting {
            surface: SignalSurface::Internal,
            auth: SignalAuth::None,
            auth_config: Value::Null,
        })
    }

    async fn spawn_task(
        &self,
        spec: &SignalSpec,
        _kind_state: &Value,
        ctx: SpawnCtx,
    ) -> Result<Option<JoinHandle<()>>> {
        let cfg: SocketListen = serde_json::from_value(spec.config.clone())
            .map_err(|e| anyhow::anyhow!("malformed socket_listen spec: {e}"))?;
        let access = spec.access.clone();
        let fire = ctx.fire.clone();
        let prepare_ctx = ctx.clone();
        let prepare = Box::new(move || {
            let cfg = cfg.clone();
            let access = access.clone();
            let ctx = prepare_ctx.clone();
            async move { prepare_cycle(&cfg, &access, &ctx).await }.boxed()
        });
        let on_event = Box::new(move |payload: Value| {
            let fire = fire.clone();
            async move { fire.fire(payload, "socket_listen").await }.boxed()
        });
        Ok(Some(socket_engine::spawn(prepare, on_event, "socket_listen")))
    }

    fn process_entry(&self, _sig: &RegisteredSignal, payload: Value) -> ProcessOutcome {
        ProcessOutcome { value: payload, target: ProcessTarget::Entry }
    }

    fn render(&self, _token: &str, _sig: &RegisteredSignal) -> Result<Option<Value>> {
        Ok(None)
    }
}

/// Build one connect cycle's plan: resolve the connection (when one
/// is set), mint the address (when the spec declares a mint call),
/// and prepare the frames, interpolating the resolved values into
/// text frames so a handshake can carry a fresh credential.
async fn prepare_cycle(
    cfg: &SocketListen,
    access: &Option<AccessRef>,
    ctx: &SpawnCtx,
) -> Result<CyclePlan, PrepareError> {
    // Resolving through the broker is transient territory (the broker
    // may be briefly unreachable, a refresh may be mid-flight).
    let values: BTreeMap<String, String> = match access {
        None => BTreeMap::new(),
        Some(access) => {
            let source = crate::listener_access::resolve(access, ctx)
                .await
                .map_err(PrepareError::Transient)?;
            let mut v = source.values;
            v.extend(source.recipe_values);
            v
        }
    };

    let url = match &cfg.minted.connect {
        None => cfg.url.clone(),
        Some(_) => socket_engine::mint_socket_url(&cfg.minted, &values).await?,
    };

    Ok(CyclePlan {
        url,
        handshake: prepare_frame(cfg.handshake.as_ref(), &values)?,
        heartbeat: prepare_frame(cfg.heartbeat.as_ref(), &values)?,
        heartbeat_secs: cfg.heartbeat_secs,
        replies: cfg.minted.replies.clone(),
    })
}

/// A spec frame as a wire message. Text bodies interpolate the
/// resolved values through the LENIENT placeholder grammar (frames
/// are JSON text full of literal braces, so the strict template
/// grammar cannot carry them); binary frames carry base64 and
/// interpolate nothing. A bad frame is fatal: retrying spins on the
/// same config.
fn prepare_frame(
    frame: Option<&SocketFrame>,
    values: &BTreeMap<String, String>,
) -> Result<Option<Message>, PrepareError> {
    let Some(frame) = frame else { return Ok(None) };
    Ok(Some(match frame {
        SocketFrame::Text { body } => Message::Text(
            interpolate_frame(body, values)
                .map_err(|e| PrepareError::Fatal(anyhow::anyhow!(e)))?,
        ),
        SocketFrame::Binary { base64 } => {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(base64)
                .map_err(|e| {
                    PrepareError::Fatal(anyhow::anyhow!(
                        "socket_listen binary frame is not valid base64: {e}"
                    ))
                })?;
            Message::Binary(bytes)
        }
    }))
}

/// Interpolate `{name}` placeholders into a frame's text. Unlike the
/// strict [`Template`] grammar, everything that is not a well-formed
/// `{[a-z0-9_]+}` placeholder stays literal (a JSON frame is full of
/// braces that mean JSON). A placeholder naming a value nobody
/// resolved is a loud error: a handshake going out with a literal
/// `{token}` is a silent authentication failure.
pub(crate) fn interpolate_frame(
    body: &str,
    values: &BTreeMap<String, String>,
) -> Result<String, String> {
    static PLACEHOLDER: std::sync::LazyLock<regex::Regex> =
        std::sync::LazyLock::new(|| regex::Regex::new(r"\{([a-z0-9_]+)\}").expect("static regex"));
    let mut err = None;
    let out = PLACEHOLDER.replace_all(body, |caps: &regex::Captures<'_>| {
        let name = &caps[1];
        match values.get(name) {
            Some(v) => v.clone(),
            None => {
                err.get_or_insert_with(|| {
                    format!(
                        "the frame interpolates '{{{name}}}' but the connection resolves \
                         no value named '{name}'"
                    )
                });
                String::new()
            }
        }
    });
    match err {
        Some(e) => Err(e),
        None => Ok(out.into_owned()),
    }
}

inventory::submit!(&SocketListenHandler as &dyn KindHandler);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_frames_interpolate_the_resolved_values() {
        let values = BTreeMap::from([("token".to_string(), "xoxb-1".to_string())]);
        let m = prepare_frame(
            Some(&SocketFrame::Text { body: r#"{"auth":"{token}"}"#.into() }),
            &values,
        )
        .ok()
        .flatten()
        .unwrap();
        assert!(matches!(m, Message::Text(t) if t == r#"{"auth":"xoxb-1"}"#));

        // A frame naming a value nobody resolved is fatal, never a
        // silent literal-brace handshake.
        let bad = prepare_frame(
            Some(&SocketFrame::Text { body: "{missing}".into() }),
            &BTreeMap::new(),
        );
        assert!(matches!(bad, Err(PrepareError::Fatal(_))));
    }

    #[test]
    fn binary_frames_decode_base64() {
        let b64 = base64::engine::general_purpose::STANDARD.encode([1u8, 2, 3]);
        let m = prepare_frame(Some(&SocketFrame::Binary { base64: b64 }), &BTreeMap::new())
            .ok()
            .flatten()
            .unwrap();
        assert!(matches!(m, Message::Binary(b) if b == vec![1, 2, 3]));
    }

    #[test]
    fn inbound_payload_shapes() {
        use crate::socket_engine::inbound_payload;
        assert_eq!(
            inbound_payload(Message::Text("{\"a\":1}".into())).unwrap(),
            serde_json::json!({"a": 1})
        );
        assert_eq!(
            inbound_payload(Message::Text("hello".into())).unwrap(),
            Value::String("hello".into())
        );
        assert!(inbound_payload(Message::Ping(vec![])).is_none());
    }
}
