//! A trigger's event handed to the worker's door
//! (`POST /_weft/fire`): what it is, what the worker made of it, and the
//! run it starts. The install's fire path, the listener and the worker
//! all speak it.

/// A trigger's event, handed to a worker of the program its trigger is
/// armed on (`POST /_weft/fire`): what a listener picked up itself (a
/// timer's tick, an event on a connection it holds), a call at one of the
/// install's own addresses (a webhook, a provider's push), or a parked
/// fire replayed. The worker's door decides what becomes of it, the way
/// it decides for a caller: whether the trigger takes work now (it parks
/// the fire while the trigger is parked), the trigger's limits, and what
/// the run reads; then it bears the run the way the trigger keeps its
/// runs and drives it.
// SYNC: the fire call <-> crates/weft-engine/src/worker.rs (fire_handler), crates/weft-dispatcher/src/worker_fire.rs, crates/weft-listener/src/fire_sink.rs
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DoorFire {
    /// The trigger's signal token.
    pub token: String,
    /// The event's identity, the same however often it is handed over
    /// (`run_of_fire`): one event is born once.
    pub fire_id: uuid::Uuid,
    /// What the trigger wakes with.
    pub payload: serde_json::Value,
    /// Who sent it, for a trigger somebody outside calls (`ip:<address>`,
    /// or `id:<identity>`): what its per-caller limit counts. `None` for
    /// an event nobody called.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caller: Option<String>,
    /// The holder a held connection's event was picked up under: taken
    /// only while the signal is still held under that name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub held_by: Option<String>,
    /// How many times this event already failed to become a run (a
    /// parked fire replayed): the next park backs off further.
    #[serde(default)]
    pub attempts: u32,
}

/// How a call to carry on one queued run ended, as the worker answers
/// `/_weft/run/<execution_id>` and the dispatcher reads it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "ended", rename_all = "snake_case")]
pub enum RunAnswer {
    /// The run was driven until it ended or was let go of.
    Completed,
    /// The drive failed, and the run ended Failed with this.
    Failed { error: String },
    /// Nothing to run: the run is not queued (another worker claimed it,
    /// or it ended).
    NothingToRun,
}

/// What a worker made of a [`DoorFire`], on the first line of its answer.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "fired", rename_all = "snake_case")]
pub enum Fired {
    /// Born and going; the rest of the answer stays open until it ends.
    Started { execution_id: crate::ExecutionId },
    /// Born already: the same event was handed over before.
    AlreadyBorn,
    /// Put in the trigger's queue (`parked_fire`) to run later:
    /// the trigger is parked, its runs at once are all going, what the
    /// run reads is not ready, or it is armed on another version of the
    /// program. `instance_gap` when it waits on a value its instance has
    /// not given: the instance's next change of values routes it again.
    Parked {
        reason: String,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        instance_gap: bool,
    },
    /// Not taken, and why: the trigger takes no work, the event is past
    /// its per-minute limit, the signal is gone.
    Dropped { reason: String },
    /// Not taken: the holder that picked the event up no longer holds the
    /// signal (another took it), which serves its own events.
    NotHeld,
    /// A caller past the trigger's per-caller limit: refused, and when to
    /// come back.
    Refused { reason: String, retry_after_secs: u64 },
}

/// The [`Fired`] on the first line of a worker's answer to a fire, and the
/// rest of the answer, which stays open until a run it started ends.
#[cfg(feature = "runtime")]
pub async fn read_fired<S, E>(mut body: S) -> anyhow::Result<(Fired, futures::stream::BoxStream<'static, Result<bytes::Bytes, E>>)>
where
    S: futures::Stream<Item = Result<bytes::Bytes, E>> + Send + Unpin + 'static,
    E: std::fmt::Display + Send + 'static,
{
    use futures::StreamExt;
    let mut line = Vec::new();
    let newline = loop {
        match body.next().await {
            Some(Ok(chunk)) => {
                line.extend_from_slice(&chunk);
                if let Some(at) = line.iter().position(|b| *b == b'\n') {
                    break at;
                }
            }
            Some(Err(e)) => anyhow::bail!("read the worker's answer: {e}"),
            None => anyhow::bail!("the worker ended its answer before saying what became of the fire"),
        }
    };
    let fired: Fired = serde_json::from_slice(&line[..newline]).map_err(|e| anyhow::anyhow!("read the worker's answer: {e}"))?;
    Ok((fired, body.boxed()))
}

/// How long a worker has to say what it made of a fire (the first line of
/// its answer), the call included: it decides at once, so one that says
/// nothing in this long is not answering, and the fire waits in its
/// trigger's queue instead.
pub const FIRST_LINE_WITHIN: std::time::Duration = std::time::Duration::from_secs(30);

/// Hand a fire to a worker (`POST /_weft/fire`, made by `send`) and read
/// what it made of it: the [`Fired`] on its answer's first line, or its
/// refusal, within [`FIRST_LINE_WITHIN`]. The rest of the answer stays open
/// while a run it started goes: it is read to its end on a task of its own,
/// which holds `hold` (whatever keeps the worker up) until then.
// SYNC: the fire call <-> crates/weft-engine/src/worker.rs (fire_handler)
#[cfg(feature = "runtime")]
pub async fn hand_over<F>(send: F, token: &str, hold: impl Send + 'static) -> anyhow::Result<Fired>
where
    F: std::future::Future<Output = anyhow::Result<reqwest::Response>>,
{
    let within = crate::time_scale::scaled(FIRST_LINE_WITHIN);
    let first_line = async {
        let answer = send.await?;
        let status = answer.status();
        if !status.is_success() {
            let body = match answer.text().await {
                Ok(body) => body,
                Err(e) => format!("(its body could not be read: {e})"),
            };
            anyhow::bail!("the worker answered {status}: {body}");
        }
        read_fired(answer.bytes_stream()).await
    };
    let (fired, mut rest) = tokio::time::timeout(within, first_line)
        .await
        .map_err(|_| anyhow::anyhow!("the worker said nothing of the fire within {}s", within.as_secs()))??;
    let token = token.to_string();
    tokio::spawn(async move {
        use futures::StreamExt;
        // What is left is the run going; it ends when the run does.
        while let Some(read) = rest.next().await {
            if let Err(e) = read {
                tracing::debug!(target: "weft_core::door_fire", %token, error = %e, "the call holding a fired run ended early; the run goes on");
                break;
            }
        }
        drop(hold);
    });
    Ok(fired)
}

/// Namespace of the runs fires start: generated once and frozen.
const FIRE_NAMESPACE: uuid::Uuid = uuid::Uuid::from_u128(0x9c4a_e6a4_0b3f_4e8e_a0f1_1d3d_9b2c_5a47);

/// The run fire `fire_id` starts: the same however often the fire is
/// handed over, so one fire never starts two runs.
pub fn run_of_fire(fire_id: uuid::Uuid) -> crate::ExecutionId {
    uuid::Uuid::new_v5(&FIRE_NAMESPACE, fire_id.as_bytes())
}

#[cfg(test)]
mod wire_tests {
    use super::*;

    #[test]
    fn a_fire_and_its_answers_round_trip() {
        let fire = DoorFire {
            token: "t1".into(),
            fire_id: uuid::Uuid::from_u128(1),
            payload: serde_json::json!({ "x": 1 }),
            caller: Some("ip:10.0.0.1".into()),
            held_by: None,
            attempts: 2,
        };
        let back: DoorFire = serde_json::from_value(serde_json::to_value(&fire).unwrap()).unwrap();
        assert_eq!(back, fire);
        for answer in [
            Fired::Started { execution_id: uuid::Uuid::nil() },
            Fired::AlreadyBorn,
            Fired::Parked { reason: "parked".into(), instance_gap: true },
            Fired::Dropped { reason: "gone".into() },
            Fired::NotHeld,
            Fired::Refused { reason: "too many".into(), retry_after_secs: 5 },
        ] {
            assert_eq!(serde_json::from_value::<Fired>(serde_json::to_value(&answer).unwrap()).unwrap(), answer);
        }
    }

    #[test]
    fn a_run_answer_says_how_it_ended() {
        for a in [RunAnswer::Completed, RunAnswer::Failed { error: "boom".into() }, RunAnswer::NothingToRun] {
            assert_eq!(serde_json::from_value::<RunAnswer>(serde_json::to_value(&a).unwrap()).unwrap(), a);
        }
        assert_eq!(serde_json::to_value(RunAnswer::NothingToRun).unwrap(), serde_json::json!({ "ended": "nothing_to_run" }));
    }

    #[test]
    fn a_fire_starts_one_run_however_often_it_is_handed_over() {
        let (one, two) = (uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(2));
        assert_eq!(run_of_fire(one), run_of_fire(one));
        assert_ne!(run_of_fire(one), run_of_fire(two));
    }
}

