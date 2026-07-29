//! The raw-pipe engine: dial a TCP (usually TLS) address, run the
//! declared connect dialogue, cut the byte stream into units by the
//! declared framing, answer the declared replies, fire on the
//! declared pattern, reconnect on drop via the shared backoff ladder.
//! The sibling of [`crate::socket_engine`] one level down the stack:
//! where that engine speaks WebSocket, this one speaks nothing at
//! all; every byte that goes out was declared in the spec.
//!
//! The caller supplies the same two closures as the socket engine:
//! `prepare` runs at the start of every connect cycle (values are
//! resolved freshly, so a rotated password heals on reconnect), and
//! `on_event` receives each fired unit's payload.

use std::time::Instant;

use futures_util::future::BoxFuture;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration, MissedTickBehavior};
use tracing::{info, warn};

use weft_core::signal::Framing;

use crate::kinds::event_source::Backoff;

pub use crate::socket_engine::PrepareError;

/// While the connect dialogue is incomplete, say so this often. The
/// problem with a step whose `until` never matches is SILENCE, not
/// duration (a slow sign-in or a peer that answers in its own time is
/// legitimate and user-controlled), so the engine never tears the
/// pipe down for taking long; it keeps waiting and keeps naming what
/// it waits for.
pub const DIALOGUE_BREADCRUMB_SECS: u64 = 60;

/// One compiled dialogue step: the frame to send, and the pattern
/// that completes the step.
pub struct PlanStep {
    pub send: Vec<u8>,
    pub until: regex::bytes::Regex,
}

/// One compiled acknowledgement rule.
pub struct PlanReply {
    pub when: regex::bytes::Regex,
    pub frame: Vec<u8>,
}

/// One connect cycle's plan: everything interpolated and compiled.
pub struct StreamPlan {
    /// `host:port` to dial.
    pub address: String,
    pub tls: bool,
    pub framing: Framing,
    pub script: Vec<PlanStep>,
    pub replies: Vec<PlanReply>,
    pub heartbeat: Option<Vec<u8>>,
    pub heartbeat_secs: u64,
    pub fire: regex::bytes::Regex,
}

type Prepare = Box<dyn FnMut() -> BoxFuture<'static, Result<StreamPlan, PrepareError>> + Send>;
type OnEvent = Box<dyn FnMut(serde_json::Value) -> BoxFuture<'static, ()> + Send>;

/// Either side of the TLS decision, as one read/write object.
trait Pipe: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Pipe for T {}

/// Run the engine until aborted (or a fatal prepare error). `target`
/// names the driving kind in every log line.
pub fn spawn(mut prepare: Prepare, mut on_event: OnEvent, target: &'static str) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut backoff = Backoff::new();
        loop {
            let plan = match prepare().await {
                Ok(p) => p,
                Err(PrepareError::Transient(e)) => {
                    warn!(target: "weft_listener::stream_engine", kind = target, error = %format!("{e:#}"), "connect preparation failed; retrying");
                    backoff.wait_then_climb().await;
                    continue;
                }
                Err(PrepareError::Fatal(e)) => {
                    warn!(target: "weft_listener::stream_engine", kind = target, error = %format!("{e:#}"), "connect preparation is misconfigured; giving up on this pipe");
                    return;
                }
            };

            let mut pipe = match dial(&plan).await {
                Ok(p) => {
                    info!(target: "weft_listener::stream_engine", kind = target, address = %plan.address, "pipe connected");
                    p
                }
                Err(e) => {
                    warn!(target: "weft_listener::stream_engine", kind = target, address = %plan.address, error = %format!("{e:#}"), "connect failed; retrying");
                    backoff.wait_then_climb().await;
                    continue;
                }
            };
            let connected_at = Instant::now();

            run_cycle(&plan, &mut pipe, &mut on_event, target).await;

            backoff.reset_if_healthy(connected_at.elapsed());
            backoff.wait_then_climb().await;
        }
    })
}

/// Drive one connected pipe until it drops or misbehaves; the caller
/// reconnects on the backoff ladder either way. Dialogue first (with
/// a periodic breadcrumb naming what it still waits on), then steady
/// state (heartbeat + fires).
async fn run_cycle(
    plan: &StreamPlan,
    pipe: &mut Box<dyn Pipe>,
    on_event: &mut OnEvent,
    target: &'static str,
) {
    let mut buf: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut step = 0usize;
    let dialogue_started = Instant::now();
    let mut breadcrumb = interval(Duration::from_secs(DIALOGUE_BREADCRUMB_SECS));
    breadcrumb.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // Consume the immediate first tick; the breadcrumb reports a
    // WAIT, not the start.
    breadcrumb.tick().await;

    if let Some(first) = plan.script.first() {
        if let Err(e) = pipe.write_all(&first.send).await {
            warn!(target: "weft_listener::stream_engine", kind = target, error = %e, "dialogue send failed; reconnecting");
            return;
        }
    }

    let mut ticker = interval(Duration::from_secs(plan.heartbeat_secs.max(1)));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // Consume the immediate first tick so the heartbeat does not fire
    // the instant steady state begins.
    ticker.tick().await;

    let mut chunk = [0u8; 16 * 1024];
    loop {
        let script_done = step >= plan.script.len();
        tokio::select! {
            _ = breadcrumb.tick(), if !script_done => {
                warn!(
                    target: "weft_listener::stream_engine", kind = target,
                    step,
                    waiting_on = %plan.script[step].until,
                    outstanding_secs = dialogue_started.elapsed().as_secs(),
                    "the connect dialogue is still waiting on this step's pattern; \
                     holding the pipe and continuing to wait"
                );
            }
            _ = ticker.tick(), if script_done && plan.heartbeat.is_some() => {
                if let Some(frame) = &plan.heartbeat {
                    if let Err(e) = pipe.write_all(frame).await {
                        warn!(target: "weft_listener::stream_engine", kind = target, error = %e, "heartbeat send failed; reconnecting");
                        return;
                    }
                }
            }
            read = pipe.read(&mut chunk) => {
                match read {
                    Ok(0) => {
                        info!(target: "weft_listener::stream_engine", kind = target, "pipe closed; reconnecting");
                        return;
                    }
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(e) => {
                        warn!(target: "weft_listener::stream_engine", kind = target, error = %e, "pipe error; reconnecting");
                        return;
                    }
                }
                loop {
                    let (unit, used) = match plan.framing.split(&buf) {
                        Ok(Some(cut)) => cut,
                        Ok(None) => break,
                        Err(e) => {
                            warn!(target: "weft_listener::stream_engine", kind = target, error = %e, "unframeable stream; reconnecting");
                            return;
                        }
                    };
                    buf.drain(..used);
                    if let Err(e) = handle_unit(plan, pipe, &unit, &mut step, on_event).await {
                        warn!(target: "weft_listener::stream_engine", kind = target, error = %e, "reply/dialogue send failed; reconnecting");
                        return;
                    }
                }
            }
        }
    }
}

/// One received unit: acks first (a slow ack drops lines on strict
/// peers), then dialogue progress, then (dialogue done) the fire.
async fn handle_unit(
    plan: &StreamPlan,
    pipe: &mut Box<dyn Pipe>,
    unit: &[u8],
    step: &mut usize,
    on_event: &mut OnEvent,
) -> std::io::Result<()> {
    for reply in &plan.replies {
        if reply.when.is_match(unit) {
            pipe.write_all(&reply.frame).await?;
        }
    }
    if let Some(current) = plan.script.get(*step) {
        if current.until.is_match(unit) {
            *step += 1;
            if let Some(next) = plan.script.get(*step) {
                pipe.write_all(&next.send).await?;
            }
        }
        // Units during the dialogue are protocol chatter, never
        // events: the fire pattern only listens once signed in.
        return Ok(());
    }
    if plan.fire.is_match(unit) {
        on_event(unit_payload(unit)).await;
    }
    Ok(())
}

/// A fired unit as the JSON payload the fire pipeline carries: UTF-8
/// text as a string, anything else as base64.
pub fn unit_payload(unit: &[u8]) -> serde_json::Value {
    use base64::Engine as _;
    match std::str::from_utf8(unit) {
        Ok(text) => serde_json::Value::String(text.to_string()),
        Err(_) => serde_json::Value::String(
            base64::engine::general_purpose::STANDARD.encode(unit),
        ),
    }
}

/// Dial the plan's address, wrapping in TLS when asked, through the
/// shared pipe helper (system trust roots, ring provider).
async fn dial(plan: &StreamPlan) -> anyhow::Result<Box<dyn Pipe>> {
    if !plan.tls {
        return Ok(Box::new(TcpStream::connect(&plan.address).await?));
    }
    Ok(Box::new(
        weft_core::net::tls_connect(&plan.address)
            .await
            .map_err(|e| anyhow::anyhow!(e))?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_payloads_are_text_or_base64() {
        assert_eq!(
            unit_payload(b"* 3 EXISTS"),
            serde_json::Value::String("* 3 EXISTS".into())
        );
        // Invalid UTF-8 rides as base64.
        assert_eq!(
            unit_payload(&[0x30, 0xff, 0x00]),
            serde_json::Value::String("MP8A".into())
        );
    }
}
