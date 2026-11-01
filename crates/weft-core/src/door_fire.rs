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
    /// Handed over from its trigger's queue, where it was held (the
    /// trigger was down, or past its limits): it is admitted under half of
    /// the trigger's own limits (`ResolvedLimits::for_held`), so what is
    /// released never takes all the room new arrivals need.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub released: bool,
}

/// A queued run handed to a worker to carry on (`POST /_weft/run/<run_id>`),
/// with the program it runs (`run.definition_hash`): the worker reads that
/// program, and the files it names, before it claims the run, so a store
/// that does not answer leaves the run queued rather than ending it
/// ([`RunAnswer::NotNow`]).
// SYNC: the run call <-> crates/weft-engine/src/worker.rs (run_handler), crates/weft-dispatcher/src/delivery.rs (run_on_worker)
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunDelivery {
    pub definition_hash: String,
}

/// How a call to carry on one queued run ended, as the worker answers
/// `/_weft/run/<run_id>` and the dispatcher reads it.
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
    /// Not carried on now, and `error` says why: the program the run runs,
    /// or a file it names, could not be read (the store or the broker did
    /// not answer), or the broker could not be asked to claim it. The run
    /// stays queued, to be handed out again; a claim whose answer was lost
    /// on its way may have landed, and the lost-run sweep queues it again
    /// then. A program or file that is gone for good is no such answer:
    /// the worker claims the run and ends it Failed with that.
    NotNow { error: String },
}

/// What a worker made of a [`DoorFire`], on the first line of its answer.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "fired", rename_all = "snake_case")]
pub enum Fired {
    /// Born and going; the rest of the answer stays open until it ends.
    Started { run_id: crate::RunId },
    /// Born already: the same event was handed over before.
    AlreadyBorn,
    /// Put in the trigger's queue (`parked_fire`) to run later, and
    /// what it waits on there ([`WaitsOn`]): the trigger is parked, its
    /// runs at once are all going, what the run reads is not ready, it is
    /// armed on another version of the program, or the run could not be
    /// recorded.
    Parked {
        reason: String,
        /// A worker that does not say waits on room.
        #[serde(default)]
        on: WaitsOn,
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

/// What an event waiting in its trigger's queue waits on, and so what
/// hands it over again.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitsOn {
    /// Room to run: the runs it may have at once are all going, a limit
    /// of its trigger is reached, or the worker it reached runs another
    /// version of the program. Tried again after a backoff that grows.
    #[default]
    Room,
    /// Its trigger to take work again (down with its door holding what
    /// arrives, or being set up): the activation that brings it back hands it over, and while
    /// its trigger is live it is tried after a backoff, as for room.
    Trigger,
    /// A value its instance has not given: the instance's next change of
    /// values hands it over.
    Instance,
    /// Infra the run reads to run again: its coming back hands it over.
    Infra,
    /// Connections the install has not picked: picking them re-arms the
    /// trigger, which hands it over.
    Picks,
    /// A hand-over that failed (the worker could not be reached, the run
    /// could not be recorded): tried twice more shortly, then it stops
    /// and holds its trigger's queue until the trigger is activated
    /// again, a new version registers or the project's worker settings
    /// change.
    Failure,
}

impl WaitsOn {
    /// The word for it, as `parked_fire.waits_for` and the wire spell it.
    pub fn as_str(self) -> &'static str {
        match self {
            WaitsOn::Room => "room",
            WaitsOn::Trigger => "trigger",
            WaitsOn::Instance => "instance",
            WaitsOn::Infra => "infra",
            WaitsOn::Picks => "picks",
            WaitsOn::Failure => "failure",
        }
    }

    pub fn parse(word: &str) -> Option<Self> {
        [WaitsOn::Room, WaitsOn::Trigger, WaitsOn::Instance, WaitsOn::Infra, WaitsOn::Picks, WaitsOn::Failure]
            .into_iter()
            .find(|on| on.as_str() == word)
    }

    /// What it waits on when no clock brings it, `None` for room and its
    /// trigger, which a backoff tries again.
    pub fn unclocked(self) -> Option<UnclockedWait> {
        match self {
            WaitsOn::Room | WaitsOn::Trigger => None,
            WaitsOn::Instance => Some(UnclockedWait::Instance),
            WaitsOn::Infra => Some(UnclockedWait::Infra),
            WaitsOn::Picks => Some(UnclockedWait::Picks),
            WaitsOn::Failure => Some(UnclockedWait::Failure),
        }
    }
}

/// What a queued event waits on that no clock brings ([`WaitsOn`] but
/// room and its trigger): only that changing, or the trigger activated
/// again, hands it over. What `weft status` and the editor show per
/// trigger.
// SYNC: the kinds <-> weft_task_store::parked_fires::WaitsFor (parked_fire.waits_for), packages/weft-graph/src/protocol.ts (WaitingFires.on), crates/weft-core/src/program.rs (WaitingFires.on)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnclockedWait {
    Instance,
    Infra,
    Picks,
    Failure,
}

impl From<UnclockedWait> for WaitsOn {
    fn from(wait: UnclockedWait) -> Self {
        match wait {
            UnclockedWait::Instance => WaitsOn::Instance,
            UnclockedWait::Infra => WaitsOn::Infra,
            UnclockedWait::Picks => WaitsOn::Picks,
            UnclockedWait::Failure => WaitsOn::Failure,
        }
    }
}

impl UnclockedWait {
    /// The word for it, as [`WaitsOn::as_str`] spells it.
    pub fn as_str(self) -> &'static str {
        WaitsOn::from(self).as_str()
    }
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
pub fn run_of_fire(fire_id: uuid::Uuid) -> crate::RunId {
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
            released: true,
        };
        let back: DoorFire = serde_json::from_value(serde_json::to_value(&fire).unwrap()).unwrap();
        assert_eq!(back, fire);
        for answer in [
            Fired::Started { run_id: uuid::Uuid::nil() },
            Fired::AlreadyBorn,
            Fired::Parked { reason: "parked".into(), on: WaitsOn::Instance },
            Fired::Dropped { reason: "gone".into() },
            Fired::NotHeld,
            Fired::Refused { reason: "too many".into(), retry_after_secs: 5 },
        ] {
            assert_eq!(serde_json::from_value::<Fired>(serde_json::to_value(&answer).unwrap()).unwrap(), answer);
        }
        assert_eq!(
            serde_json::from_value::<Fired>(serde_json::json!({ "fired": "parked", "reason": "full" })).unwrap(),
            Fired::Parked { reason: "full".into(), on: WaitsOn::Room },
            "a worker that does not say what a fire waits on parks it for room"
        );
        for on in [WaitsOn::Room, WaitsOn::Trigger, WaitsOn::Instance, WaitsOn::Infra, WaitsOn::Picks, WaitsOn::Failure] {
            assert_eq!(WaitsOn::parse(on.as_str()), Some(on));
            assert_eq!(serde_json::to_value(on).unwrap(), on.as_str());
            // What no clock brings is spelled as its wait is, and turns back
            // into it.
            if let Some(unclocked) = on.unclocked() {
                assert_eq!(serde_json::to_value(unclocked).unwrap(), on.as_str());
                assert_eq!(WaitsOn::from(unclocked), on);
            }
        }
        assert_eq!(WaitsOn::Room.unclocked(), None);
        assert_eq!(WaitsOn::Trigger.unclocked(), None);
    }

    #[test]
    fn a_run_answer_says_how_it_ended() {
        for a in [
            RunAnswer::Completed,
            RunAnswer::Failed { error: "boom".into() },
            RunAnswer::NothingToRun,
            RunAnswer::NotNow { error: "the store did not answer".into() },
        ] {
            assert_eq!(serde_json::from_value::<RunAnswer>(serde_json::to_value(&a).unwrap()).unwrap(), a);
        }
        assert_eq!(serde_json::to_value(RunAnswer::NothingToRun).unwrap(), serde_json::json!({ "ended": "nothing_to_run" }));
        assert_eq!(
            serde_json::to_value(RunAnswer::NotNow { error: "e".into() }).unwrap(),
            serde_json::json!({ "ended": "not_now", "error": "e" })
        );
    }

    #[test]
    fn a_run_delivery_names_its_program_in_camel_case() {
        let delivery = RunDelivery { definition_hash: "h1".into() };
        assert_eq!(serde_json::to_value(&delivery).unwrap(), serde_json::json!({ "definitionHash": "h1" }));
        assert_eq!(serde_json::from_value::<RunDelivery>(serde_json::json!({ "definitionHash": "h1" })).unwrap(), delivery);
    }

    #[test]
    fn a_fire_starts_one_run_however_often_it_is_handed_over() {
        let (one, two) = (uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(2));
        assert_eq!(run_of_fire(one), run_of_fire(one));
        assert_ne!(run_of_fire(one), run_of_fire(two));
    }
}

