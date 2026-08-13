//! Raw-pipe handler: parses the kind's config and drives the shared
//! [`crate::stream_engine`]. The optional connection
//! (`SignalSpec.access`) supplies the values the address and frames
//! interpolate, resolved freshly on every reconnect, so credentials
//! ride the dialogue without ever sitting in the spec. The SERVICE
//! protocol (what the frames mean) stays the author's concern.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use base64::Engine as _;
use dashmap::DashMap;
use futures_util::FutureExt;
use serde_json::Value;
use tokio::task::JoinHandle;
use weft_core::primitive::{AccessRef, SignalAuth, SignalRouting, SignalSpec, SignalSurface};
use weft_core::signal::{Signal, SocketFrame, StreamListen};

use crate::protocol::{ProcessOutcome, ProcessTarget};
use crate::registry::RegisteredSignal;
use crate::socket_engine::PrepareError;
use crate::stream_engine::{self, PlanReply, PlanStep, StreamPlan};

use super::socket_listen::interpolate_frame;
use super::{KindHandler, SpawnCtx};

pub struct StreamListenHandler;

#[async_trait]
impl KindHandler for StreamListenHandler {
    fn tag(&self) -> &'static str {
        StreamListen::TAG
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
        let cfg: StreamListen = serde_json::from_value(spec.config.clone())
            .map_err(|e| anyhow::anyhow!("malformed stream_listen spec: {e}"))?;
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
            async move {
                // A stream event has no replay cursor; the delivery
                // outcome is already logged by the fire path.
                let _ = fire.fire(payload, "stream_listen").await;
            }
            .boxed()
        });
        Ok(Some(stream_engine::spawn(prepare, on_event, "stream_listen")))
    }

    fn process_entry(&self, _sig: &RegisteredSignal, payload: Value) -> ProcessOutcome {
        ProcessOutcome { value: payload, target: ProcessTarget::Entry }
    }

    fn render(&self, _token: &str, _sig: &RegisteredSignal) -> Result<Option<Value>> {
        Ok(None)
    }
}

/// Build one connect cycle's plan: resolve the connection (when one
/// is set), interpolate the address and every frame, compile every
/// pattern. Pattern compilation failing here is fatal (validation
/// already vouched for them; a spec reaching here broken would spin).
async fn prepare_cycle(
    cfg: &StreamListen,
    access: &Option<AccessRef>,
    ctx: &SpawnCtx,
) -> Result<StreamPlan, PrepareError> {
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

    let address = interpolate_frame(&cfg.address, &values)
        .map_err(|e| PrepareError::Fatal(anyhow::anyhow!(e)))?;
    let script = cfg
        .script
        .iter()
        .map(|step| {
            Ok(PlanStep {
                send: frame_bytes(&step.send, &values)?,
                until: compiled(&step.until)?,
            })
        })
        .collect::<Result<Vec<_>, PrepareError>>()?;
    let replies = cfg
        .replies
        .iter()
        .map(|reply| {
            Ok(PlanReply {
                when: compiled(&reply.when)?,
                frame: frame_bytes(&reply.frame, &values)?,
            })
        })
        .collect::<Result<Vec<_>, PrepareError>>()?;
    let heartbeat = cfg
        .heartbeat
        .as_ref()
        .map(|f| frame_bytes(f, &values))
        .transpose()?;

    Ok(StreamPlan {
        address,
        tls: cfg.tls,
        framing: cfg.framing.clone(),
        script,
        replies,
        heartbeat,
        heartbeat_secs: cfg.heartbeat_secs,
        fire: compiled(&cfg.fire)?,
    })
}

fn compiled(pattern: &str) -> Result<regex::bytes::Regex, PrepareError> {
    regex::bytes::Regex::new(pattern)
        .map_err(|e| PrepareError::Fatal(anyhow::anyhow!("pattern does not compile: {e}")))
}

/// A spec frame as raw bytes: text interpolates the resolved values
/// (a login line carries a fresh credential every cycle), binary
/// carries base64 verbatim.
fn frame_bytes(
    frame: &SocketFrame,
    values: &BTreeMap<String, String>,
) -> Result<Vec<u8>, PrepareError> {
    Ok(match frame {
        SocketFrame::Text { body } => interpolate_frame(body, values)
            .map_err(|e| PrepareError::Fatal(anyhow::anyhow!(e)))?
            .into_bytes(),
        SocketFrame::Binary { base64 } => base64::engine::general_purpose::STANDARD
            .decode(base64)
            .map_err(|e| {
                PrepareError::Fatal(anyhow::anyhow!(
                    "stream_listen binary frame is not valid base64: {e}"
                ))
            })?,
    })
}

inventory::submit!(&StreamListenHandler as &dyn KindHandler);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_interpolate_and_addresses_too() {
        let values = BTreeMap::from([
            ("user".to_string(), "q@acme.com".to_string()),
            ("password".to_string(), "s3cret".to_string()),
        ]);
        let Ok(bytes) = frame_bytes(
            &SocketFrame::Text { body: "a1 LOGIN {user} {password}\r\n".into() },
            &values,
        ) else {
            panic!("a fully-resolved frame interpolates")
        };
        assert_eq!(bytes, b"a1 LOGIN q@acme.com s3cret\r\n");

        // A frame naming a value nobody resolved is fatal, never a
        // silent literal-brace login.
        assert!(matches!(
            frame_bytes(&SocketFrame::Text { body: "{missing}".into() }, &BTreeMap::new()),
            Err(PrepareError::Fatal(_))
        ));
    }
}
