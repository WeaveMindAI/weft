//! How a run is kept: chosen where it starts, never per node.
//!
//! The three choices travel as one value, set by whatever starts the run:
//! the trigger whose firing starts it (the language gives every trigger the
//! inputs, and the ctx reads them when the trigger registers its signal),
//! or whoever starts it by hand (`weft run --durable`, `--fast`,
//! `--keep-for`, the editor, a setup run). The run carries the value for its whole life,
//! and every node of it runs under it.
//!
//! - **How it is kept** ([`Keeping`]): a `Fast` run runs in memory and its
//!   record trails behind it, so a worker that dies mid-run ends it
//!   (cancelled, never run again); it waits for its record only where
//!   something else acts on it (a pause, a call to weft that names the
//!   run), and lets go once its ending is handed to the writer. A
//!   `Durable` run has what it did on record before each step of a node
//!   that is not pure starts, and lets anything leave the system (an
//!   answer) only once it is written.
//! - **Whether it is recorded**: off, the run keeps no history. It leaves
//!   its birth and its failure if it fails, and its birth, costs and ending
//!   if it reports a cost or asks weft for something its worker does not
//!   already hold, such as a stored file, a connection or a stop by tag
//!   (`weft_journal::unrecorded`). Otherwise it leaves nothing. A durable
//!   run is a record, so durable and unrecorded together are refused.
//! - **How long it is kept once it ends** ([`KeepFor`]): what started it
//!   says (a trigger's `keepRunsFor`, `weft run --keep-for`), else its
//!   project's `[runs] keep_for`, else a week.
//! - **How long it holds when it cannot pause** ([`RunSettings::hold_secs`]):
//!   a run that cannot pause (its caller is on the line and its route does
//!   not outlive it, it is unrecorded, or a bus between its nodes is open)
//!   keeps its worker while its waits are the only thing left, for at most
//!   this many seconds of nothing moving; then the waiting `ctx` call
//!   fails.

use serde::{Deserialize, Serialize};

/// How a run's record keeps up with the run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Keeping {
    /// The run waits for its record only where something else acts on it
    /// (a pause, a call to weft that names the run): it ends once its
    /// ending is queued, and its record trails behind it. The default.
    #[default]
    Fast,
    /// What the run did is on record before each step of a node that is
    /// not pure starts, and an answer before it leaves.
    Durable,
}

impl Keeping {
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }

    pub fn is_durable(self) -> bool {
        self == Self::Durable
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fast => "fast",
            Self::Durable => "durable",
        }
    }
}

fn is_true(value: &bool) -> bool {
    *value
}

fn yes() -> bool {
    true
}

/// How long a run that cannot pause holds its worker, unless what started
/// it says otherwise.
pub const DEFAULT_HOLD_SECS: u32 = 60;

/// The longest hold a run may ask for: a hold is never unbounded.
// SYNC: MAX_HOLD_SECS <-> packages/weft-graph/src/run-spec.ts MAX_HOLD_SECS
pub const MAX_HOLD_SECS: u32 = 30 * 24 * 3600;

fn is_default_hold(secs: &u32) -> bool {
    *secs == DEFAULT_HOLD_SECS
}

fn default_hold() -> u32 {
    DEFAULT_HOLD_SECS
}

/// How a run is kept: the one value every way of starting a run states.
/// Built only through [`RunSettings::new`] (or the defaults), which
/// refuses a durable run that is not recorded; reading one off the wire
/// goes through it too.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "SettingsWire", into = "SettingsWire")]
pub struct RunSettings {
    keeping: Keeping,
    recorded: bool,
    /// How long the run is kept once it ends, when whatever started it
    /// says; `None` keeps it as long as its project says
    /// ([`RunSettings::kept_for`]).
    keep_for: Option<KeepFor>,
    /// How long, in seconds, the run holds its worker when it cannot pause
    /// and its waits are all that is left ([`RunSettings::hold_secs`]).
    hold_secs: u32,
}

/// [`RunSettings`] as it is written: each default left out.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct SettingsWire {
    #[serde(default, skip_serializing_if = "Keeping::is_default")]
    keeping: Keeping,
    #[serde(default = "yes", skip_serializing_if = "is_true")]
    recorded: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    keep_for: Option<KeepFor>,
    #[serde(default = "default_hold", skip_serializing_if = "is_default_hold")]
    hold_secs: u32,
}

impl TryFrom<SettingsWire> for RunSettings {
    type Error = String;
    fn try_from(wire: SettingsWire) -> Result<Self, String> {
        Self::new(wire.keeping, wire.recorded)?.keeping_for(wire.keep_for).holding_for(wire.hold_secs)
    }
}

impl From<RunSettings> for SettingsWire {
    fn from(settings: RunSettings) -> Self {
        Self { keeping: settings.keeping, recorded: settings.recorded, keep_for: settings.keep_for, hold_secs: settings.hold_secs }
    }
}

impl Default for RunSettings {
    fn default() -> Self {
        Self { keeping: Keeping::default(), recorded: true, keep_for: None, hold_secs: DEFAULT_HOLD_SECS }
    }
}

/// How long an ended run is kept before it is deleted, with its record,
/// its logs, its search entry and its tags. Written `30m`, `12h`, `7d` (a whole
/// number and a unit), or `forever`. A run that has not ended (running,
/// parked, queued) is never deleted, however old.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeepFor {
    Seconds(u64),
    Forever,
}

impl KeepFor {
    /// What weft keeps a run for when neither its start nor its project
    /// says: a week.
    pub const WEFT_DEFAULT: Self = Self::Seconds(7 * 24 * 3600);

    /// The seconds the record keeps the run for (`run.keep_for`), `None`
    /// for ever.
    pub fn seconds(self) -> Option<i64> {
        match self {
            Self::Seconds(secs) => Some(i64::try_from(secs).unwrap_or(i64::MAX)),
            Self::Forever => None,
        }
    }
}

impl std::str::FromStr for KeepFor {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        let text = text.trim();
        if text == "forever" {
            return Ok(Self::Forever);
        }
        let refused = || format!("'{text}' is not how long to keep a run: write a whole number and a unit (`30m`, `12h`, `7d`), or `forever`");
        let split = text.find(|c: char| !c.is_ascii_digit()).ok_or_else(refused)?;
        let (number, unit) = text.split_at(split);
        let number: u64 = number.parse().map_err(|_| refused())?;
        let unit_secs = match unit {
            "m" => 60,
            "h" => 3600,
            "d" => 24 * 3600,
            _ => return Err(refused()),
        };
        number.checked_mul(unit_secs).map(Self::Seconds).ok_or_else(refused)
    }
}

impl std::fmt::Display for KeepFor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::Forever => f.write_str("forever"),
            Self::Seconds(secs) if secs % (24 * 3600) == 0 => write!(f, "{}d", secs / (24 * 3600)),
            Self::Seconds(secs) if secs % 3600 == 0 => write!(f, "{}h", secs / 3600),
            Self::Seconds(secs) => write!(f, "{}m", secs.div_ceil(60)),
        }
    }
}

impl Serialize for KeepFor {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for KeepFor {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?.parse().map_err(serde::de::Error::custom)
    }
}

/// How a run started by hand asks to be kept: each part left out follows
/// what the run fires (the trigger's own `durable`, the way any other fire
/// of it would start), or the default for a run that fires nothing
/// (`weft run --durable --fast --keep-for`).
///
/// A run started by hand is always recorded, whatever its trigger's
/// `recorded` says: it starts as a queued row, and a row must end, so its
/// record is written whole however it ends.
// SYNC: SettingsChoice <-> packages/weft-graph/src/run-spec.ts (RunSettings, its validator, and the flags it maps to)
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettingsChoice {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keeping: Option<Keeping>,
    /// How long the run is kept once it ends (`weft run --keep-for`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_for: Option<KeepFor>,
    /// How long the run holds when it cannot pause (`weft run
    /// --hold-secs`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hold_secs: Option<u32>,
}

impl SettingsChoice {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// The settings of a run whose trigger keeps its runs as `base`, with
    /// what this choice names put over it, recorded (see the type).
    /// Refused when the hold it names is longer than [`MAX_HOLD_SECS`].
    pub fn over(self, base: RunSettings) -> Result<RunSettings, String> {
        RunSettings { keeping: self.keeping.unwrap_or(base.keeping), recorded: true, keep_for: self.keep_for.or(base.keep_for), hold_secs: base.hold_secs }
            .holding_for(self.hold_secs.unwrap_or(base.hold_secs))
    }
}

/// The trigger inputs the language owns, by name.
// SYNC: DURABLE_FIELD, RECORDED_FIELD, KEEP_FOR_FIELD, OUTLIVES_CALLER_FIELD <-> docs/src/language/triggers-and-routes.md (How a run is kept)
pub const DURABLE_FIELD: &str = "durable";
pub const RECORDED_FIELD: &str = "recorded";
/// How long the trigger's runs are kept once they ended.
pub const KEEP_FOR_FIELD: &str = "keepRunsFor";
/// Given only to a trigger that holds a caller on the line
/// (`features.liveConnection`): whether its run may go on once the caller
/// has left.
pub const OUTLIVES_CALLER_FIELD: &str = "outlivesCaller";
/// How long the trigger's runs hold their worker when they cannot pause.
pub const HOLD_SECS_FIELD: &str = "holdSecs";

/// A trigger's switch (`durable`, `recorded`, `outlivesCaller`) off its
/// inputs: absent or null is `default`, a boolean is itself, anything else
/// is refused, never read as off.
pub fn read_switch(fields: &serde_json::Map<String, serde_json::Value>, field: &str, default: bool) -> Result<bool, String> {
    match fields.get(field) {
        None | Some(serde_json::Value::Null) => Ok(default),
        Some(serde_json::Value::Bool(b)) => Ok(*b),
        Some(other) => Err(format!("{field} is on or off (true or false), got {other}")),
    }
}

/// A trigger's run settings refused ([`RunSettings::from_node_fields`]):
/// the input it is about, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettingRefused {
    pub field: &'static str,
    pub why: String,
}

impl std::fmt::Display for SettingRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.why)
    }
}

impl RunSettings {
    /// The one way to build settings: a durable run is a record, so it
    /// cannot also be unrecorded.
    pub fn new(keeping: Keeping, recorded: bool) -> Result<Self, String> {
        if keeping.is_durable() && !recorded {
            return Err(format!(
                "a run cannot be both `{DURABLE_FIELD}` and not `{RECORDED_FIELD}`: a durable run writes \
                 down what it did before its steps start, which is a record. Turn `{RECORDED_FIELD}` \
                 back on, or `{DURABLE_FIELD}` off"
            ));
        }
        Ok(Self { keeping, recorded, keep_for: None, hold_secs: DEFAULT_HOLD_SECS })
    }

    /// The settings of the runtime's own bookkeeping runs (a trigger's
    /// setup, an infra's setup): durable, recorded, kept as long as the
    /// project says.
    pub fn bookkeeping() -> Self {
        Self { keeping: Keeping::Durable, recorded: true, keep_for: None, hold_secs: DEFAULT_HOLD_SECS }
    }

    /// These settings, kept for `keep_for` once the run ended (`None`: as
    /// long as its project says).
    pub fn keeping_for(self, keep_for: Option<KeepFor>) -> Self {
        Self { keep_for, ..self }
    }

    /// These settings, holding for `secs` when the run cannot pause;
    /// refused past [`MAX_HOLD_SECS`].
    pub fn holding_for(self, secs: u32) -> Result<Self, String> {
        if secs > MAX_HOLD_SECS {
            return Err(format!(
                "`{HOLD_SECS_FIELD}` is at most {MAX_HOLD_SECS} seconds (30 days): a run that cannot pause holds \
                 a whole worker while it waits, so the hold is never unbounded; got {secs}"
            ));
        }
        Ok(Self { hold_secs: secs, ..self })
    }

    /// How long, in seconds, the run holds its worker when it cannot pause
    /// (its caller is on the line and its route does not outlive it, it is
    /// unrecorded, or a bus between its nodes is open) and its waits are
    /// all that is left: the clock runs only while nothing moves (no step
    /// working, nothing said on a bus) and starts again when something
    /// does. When it runs out, every `ctx.await_signal` still waiting fails
    /// at the call. `0` fails such a wait at once.
    pub fn hold_secs(&self) -> u32 {
        self.hold_secs
    }

    /// How long the run is kept once it ends: what its start said, else
    /// its project's default (`ProjectDefaults::keep_for`).
    pub fn kept_for(&self, project_default: KeepFor) -> KeepFor {
        self.keep_for.unwrap_or(project_default)
    }

    pub fn keeping(&self) -> Keeping {
        self.keeping
    }

    pub fn recorded(&self) -> bool {
        self.recorded
    }


    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }

    /// The settings a trigger's author chose, read off its inputs: each
    /// absent or null input keeps its default, a boolean sets a switch, a
    /// duration (`7d`, `forever`) how long its runs are kept, and anything
    /// else is refused, naming the input it is about.
    pub fn from_node_fields(fields: &serde_json::Map<String, serde_json::Value>) -> Result<Self, SettingRefused> {
        let refused = |field: &'static str| move |why: String| SettingRefused { field, why };
        let switch = |field: &'static str, default: bool| read_switch(fields, field, default).map_err(refused(field));
        let keeping = if switch(DURABLE_FIELD, false)? { Keeping::Durable } else { Keeping::Fast };
        let keep_for = match fields.get(KEEP_FOR_FIELD) {
            None | Some(serde_json::Value::Null) => None,
            Some(serde_json::Value::String(text)) if text.trim().is_empty() => None,
            Some(serde_json::Value::String(text)) => {
                Some(text.parse::<KeepFor>().map_err(|why| format!("{KEEP_FOR_FIELD}: {why}")).map_err(refused(KEEP_FOR_FIELD))?)
            }
            Some(other) => return Err(refused(KEEP_FOR_FIELD)(format!("{KEEP_FOR_FIELD} is how long to keep a run (`7d`, `forever`), got {other}"))),
        };
        let hold_secs = match fields.get(HOLD_SECS_FIELD) {
            None | Some(serde_json::Value::Null) => DEFAULT_HOLD_SECS,
            Some(serde_json::Value::Number(n)) => n
                .as_u64()
                .and_then(|n| u32::try_from(n).ok())
                .ok_or_else(|| format!("{HOLD_SECS_FIELD} is a whole number of seconds, got {n}"))
                .map_err(refused(HOLD_SECS_FIELD))?,
            Some(other) => return Err(refused(HOLD_SECS_FIELD)(format!("{HOLD_SECS_FIELD} is a whole number of seconds, got {other}"))),
        };
        // A combination refused is about `recorded`: turning it back on is
        // the one change that settles either.
        Self::new(keeping, switch(RECORDED_FIELD, true)?)
            .map_err(refused(RECORDED_FIELD))?
            .outliving_its_caller(switch(OUTLIVES_CALLER_FIELD, false)?)
            .map_err(refused(RECORDED_FIELD))?
            .keeping_for(keep_for)
            .holding_for(hold_secs)
            .map_err(refused(HOLD_SECS_FIELD))
    }

    /// These settings for a run that may go on after its caller left
    /// (`outlives`, its trigger's `outlivesCaller`): such a run is carried
    /// on from its record (it pauses, and weft resumes it under its own
    /// call), so it keeps one. A run started by hand is always recorded
    /// ([`SettingsChoice`]).
    pub fn outliving_its_caller(self, outlives: bool) -> Result<Self, String> {
        if outlives && !self.recorded {
            let outlives = OUTLIVES_CALLER_FIELD;
            return Err(format!(
                "a run that may outlive its caller (`{outlives}`) carries on from its record once the \
                 caller leaves, so it cannot be `{RECORDED_FIELD}: false`. Turn `{RECORDED_FIELD}` back \
                 on, or `{outlives}` off"
            ));
        }
        Ok(self)
    }

    /// The inputs the language gives every trigger
    /// ([`crate::node::NodeMetadata::add_language_ports`]): how its runs
    /// are kept and whether they are recorded, and, for a trigger that
    /// holds a caller on the line (`holds_caller`), whether its run may
    /// outlive that caller.
    pub fn node_inputs(holds_caller: bool) -> Vec<crate::node::InputSpec> {
        let boolean = |name: &str, default: bool, label: &str, description: &str| -> crate::node::InputSpec {
            serde_json::from_value(serde_json::json!({
                "name": name,
                "type": "Boolean",
                "default": default,
                "label": label,
                "description": description,
            }))
            .expect("a run setting input is a valid InputSpec")
        };
        let mut inputs = vec![
            boolean(
                DURABLE_FIELD,
                false,
                "Durable runs",
                "Turn on if a run must carry on when its worker dies: another worker picks it up where it \
                 was, and a step that was running at that moment is failed rather than run twice (a pure \
                 node's step that had not passed a value on is run again instead). To make that possible, \
                 what the run did is written down before each step, except a pure node's, and an answer \
                 before it leaves. \
                 Off (default): the run waits for its record only when it pauses or asks weft for \
                 something on its behalf, and if its worker dies mid-run the run ends cancelled and is not run again.",
            ),
            boolean(
                RECORDED_FIELD,
                true,
                "Record each run",
                "Turn off for a route called every few seconds (a status a page polls). Off: no history of \
                 a run is kept. One that fails is listed in `weft executions` with its failure (which \
                 step, and why); one that reported a cost or asked weft for something its worker does not already hold \
                 (a stored file, a new connection or infrastructure address, a task such as starting or \
                 stopping its infra, a stop through `ctx.stop_tagged`) is listed with its costs and how it ended; any other leaves only a count in `weft status`. \
                 A run started by hand is always recorded. An unrecorded run cannot wait (timer, form), \
                 cannot tag itself, and cannot be durable.",
            ),
            serde_json::from_value(serde_json::json!({
                "name": KEEP_FOR_FIELD,
                "type": "String",
                "label": "Keep runs for",
                "description": "How long a run is kept once it ends, with its record, its logs, its search entry and its tags: a \
                 whole number and a unit (`30m`, `12h`, `7d`), or `forever`. Unset, as long as the project keeps \
                 runs (`[runs] keep_for` in `weft.toml`, a week when it says nothing). A run that has not ended \
                 (running, parked, or waiting for a worker) is never deleted, however old.",
            }))
            .expect("the keep-for input is a valid InputSpec"),
            serde_json::from_value(serde_json::json!({
                "name": HOLD_SECS_FIELD,
                "type": "Number",
                "default": DEFAULT_HOLD_SECS,
                "widget": { "kind": "number", "min": 0, "max": MAX_HOLD_SECS, "step": 1 },
                "label": "Hold seconds when a run cannot pause",
                "description": "How long, in seconds, a wait (a form, a timer) holds the run's worker when \
                 the run cannot pause: its caller is on the line and it does not outlive it, it is not \
                 recorded, or a bus between its nodes is open. The clock runs only while nothing moves in \
                 the run (no step working, nothing said on a bus), and starts over when something does. \
                 When it runs out, the waiting node fails, saying it gave up, and the node may handle that \
                 itself. `0` fails such a wait at once; at most 30 days.",
            }))
            .expect("the hold input is a valid InputSpec"),
        ];
        if holds_caller {
            inputs.push(boolean(
                OUTLIVES_CALLER_FIELD,
                false,
                "May outlive the caller",
                "Off (default): the caller leaving cancels the run, and a wait holds the worker for \
                 `holdSecs` instead of pausing. On: the run can answer early and keep working after the caller has gone, so \
                 whatever runs after the answer must end by itself; anything that watches or loops needs \
                 `maxSessionSecs` or a TagRun/StopTagged pair. It needs `recorded` on, because on a cloud \
                 install the run carries on from its record once the caller leaves.",
            ));
        }
        inputs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields(v: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
        v.as_object().cloned().unwrap()
    }

    #[test]
    fn a_trigger_states_its_settings_by_its_inputs() {
        assert_eq!(RunSettings::from_node_fields(&fields(serde_json::json!({}))).unwrap(), RunSettings::default());
        let durable = RunSettings::from_node_fields(&fields(serde_json::json!({ "durable": true }))).unwrap();
        assert_eq!((durable.keeping(), durable.recorded()), (Keeping::Durable, true));
        let quiet = RunSettings::from_node_fields(&fields(serde_json::json!({ "recorded": false }))).unwrap();
        assert!(!quiet.recorded() && quiet.keeping() == Keeping::Fast);
        assert!(RunSettings::from_node_fields(&fields(serde_json::json!({ "durable": "yes" }))).is_err());
    }

    #[test]
    fn a_trigger_says_how_long_its_runs_are_kept() {
        let kept = RunSettings::from_node_fields(&fields(serde_json::json!({ "keepRunsFor": "30d" }))).unwrap();
        assert_eq!(kept.kept_for(KeepFor::WEFT_DEFAULT), KeepFor::Seconds(30 * 24 * 3600));
        let unset = RunSettings::from_node_fields(&fields(serde_json::json!({ "keepRunsFor": "" }))).unwrap();
        assert_eq!(unset.kept_for(KeepFor::Forever), KeepFor::Forever, "unset follows the project");
        assert!(RunSettings::from_node_fields(&fields(serde_json::json!({ "keepRunsFor": "a week" }))).is_err());
        assert!(RunSettings::from_node_fields(&fields(serde_json::json!({ "keepRunsFor": 7 }))).is_err());
        let choice = SettingsChoice { keep_for: Some(KeepFor::Forever), ..SettingsChoice::default() };
        assert_eq!(choice.over(kept).unwrap().kept_for(KeepFor::WEFT_DEFAULT), KeepFor::Forever, "by hand over the trigger");
        assert_eq!(SettingsChoice::default().over(kept).unwrap(), kept, "nothing by hand follows the trigger");
    }

    /// A trigger says how long its runs hold when they cannot pause: a
    /// whole number of seconds, `0` to fail such a wait at once, never more
    /// than the cap; one started by hand can say otherwise; the default
    /// leaves nothing on the wire.
    #[test]
    fn a_trigger_says_how_long_its_runs_hold() {
        assert_eq!(RunSettings::default().hold_secs(), DEFAULT_HOLD_SECS);
        let held = RunSettings::from_node_fields(&fields(serde_json::json!({ "holdSecs": 0 }))).unwrap();
        assert_eq!(held.hold_secs(), 0);
        assert!(RunSettings::from_node_fields(&fields(serde_json::json!({ "holdSecs": MAX_HOLD_SECS + 1 }))).is_err());
        assert!(RunSettings::from_node_fields(&fields(serde_json::json!({ "holdSecs": "a minute" }))).is_err());
        let refused = RunSettings::from_node_fields(&fields(serde_json::json!({ "durable": true, "holdSecs": "a minute" }))).unwrap_err();
        assert_eq!(refused.field, HOLD_SECS_FIELD, "a refusal names the input it is about: {refused}");
        assert!(RunSettings::from_node_fields(&fields(serde_json::json!({ "holdSecs": -1 }))).is_err());
        let by_hand = SettingsChoice { hold_secs: Some(600), ..SettingsChoice::default() }.over(held).unwrap();
        assert_eq!(by_hand.hold_secs(), 600);
        assert!(SettingsChoice { hold_secs: Some(MAX_HOLD_SECS + 1), ..SettingsChoice::default() }.over(held).is_err());
        assert_eq!(serde_json::to_value(RunSettings::default()).unwrap(), serde_json::json!({}));
        let wire = serde_json::to_value(held).unwrap();
        assert_eq!(wire, serde_json::json!({ "holdSecs": 0 }));
        assert_eq!(serde_json::from_value::<RunSettings>(wire).unwrap(), held);
    }

    #[test]
    fn keep_for_reads_back_as_written() {
        for text in ["30m", "12h", "7d", "forever"] {
            assert_eq!(text.parse::<KeepFor>().unwrap().to_string(), text);
        }
        assert!("7w".parse::<KeepFor>().is_err());
        assert!("".parse::<KeepFor>().is_err());
    }

    #[test]
    fn a_run_that_outlives_its_caller_keeps_its_record() {
        let e = RunSettings::from_node_fields(&fields(serde_json::json!({ "outlivesCaller": true, "recorded": false }))).unwrap_err();
        assert!(e.why.contains("outlivesCaller") && e.why.contains("recorded") && e.field == RECORDED_FIELD, "{e}");
        assert!(RunSettings::from_node_fields(&fields(serde_json::json!({ "outlivesCaller": true }))).is_ok());
    }

    #[test]
    fn a_choice_by_hand_follows_the_trigger_where_it_names_nothing() {
        let durable = RunSettings::new(Keeping::Durable, true).unwrap();
        assert_eq!(SettingsChoice::default().over(durable).unwrap(), durable, "a fire starts the way its trigger says");
        let fast = SettingsChoice { keeping: Some(Keeping::Fast), ..SettingsChoice::default() }.over(durable).unwrap();
        assert_eq!((fast.keeping(), fast.recorded()), (Keeping::Fast, true));
        let quiet = RunSettings::new(Keeping::Fast, false).unwrap();
        assert!(SettingsChoice::default().over(quiet).unwrap().recorded(), "a run started by hand is recorded");
        let durable_by_hand = SettingsChoice { keeping: Some(Keeping::Durable), ..SettingsChoice::default() }.over(quiet).unwrap();
        assert_eq!((durable_by_hand.keeping(), durable_by_hand.recorded()), (Keeping::Durable, true));
        assert_eq!(serde_json::to_value(SettingsChoice::default()).unwrap(), serde_json::json!({}));
        assert!(serde_json::from_value::<SettingsChoice>(serde_json::json!({ "recorded": false })).is_err());
    }

    #[test]
    fn durable_and_unrecorded_together_are_refused() {
        let e = RunSettings::from_node_fields(&fields(serde_json::json!({ "durable": true, "recorded": false }))).unwrap_err();
        assert!(e.why.contains("durable") && e.why.contains("recorded") && e.field == RECORDED_FIELD, "{e}");
        assert!(RunSettings::new(Keeping::Durable, false).is_err());
    }

    #[test]
    fn the_defaults_leave_nothing_on_the_wire() {
        assert_eq!(serde_json::to_value(RunSettings::default()).unwrap(), serde_json::json!({}));
        let wire = serde_json::to_value(RunSettings::new(Keeping::Fast, false).unwrap()).unwrap();
        assert_eq!(wire, serde_json::json!({ "recorded": false }));
        assert!(!serde_json::from_value::<RunSettings>(wire).unwrap().recorded());
        assert!(serde_json::from_value::<RunSettings>(serde_json::json!({ "keeping": "durable", "recorded": false })).is_err());
        assert!(serde_json::from_value::<RunSettings>(serde_json::json!({ "runClass": "long" })).is_err(), "a key no setting has is refused");
    }

    #[test]
    fn every_trigger_gets_the_same_switches_and_a_caller_one_the_lifetime_switch() {
        let names = |holds_caller| RunSettings::node_inputs(holds_caller).into_iter().map(|i| i.name).collect::<Vec<_>>();
        assert_eq!(names(false), vec![DURABLE_FIELD, RECORDED_FIELD, KEEP_FOR_FIELD, HOLD_SECS_FIELD]);
        assert_eq!(names(true), vec![DURABLE_FIELD, RECORDED_FIELD, KEEP_FOR_FIELD, HOLD_SECS_FIELD, OUTLIVES_CALLER_FIELD]);
    }
}
