//! The edits a person asks of a run by hand (`weft run --from ...`), and
//! the spec they make: over a saved example, or over a whole run when
//! there is none. Pure, so the CLI and the dispatcher (a program's run
//! call, `ctx.project(..).code().run()`, a run built from a saved
//! example) read the same flags the same way.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::run_settings::{KeepFor, Keeping, RunSettings};
use crate::run_spec::{parse_port_object, PortValues, RunSpec};

/// The flags `weft run` turns into a spec. Each is written the way the
/// flag takes it (`node=<json>`), and travels that way in a run request.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct RunFlags {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub from: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub target: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub before: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub feed: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub fire: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub emit: Vec<String>,
    /// Which instance the run is in (`--instance`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance: Option<crate::instance::InstanceId>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub clear: Vec<String>,
    /// Write what the run did down before each node starts (`--durable`).
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub durable: bool,
    /// Never wait for the record except to pause (`--fast`), over a
    /// durable trigger.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub fast: bool,
    /// How long the run is kept once it ends (`--keep-for`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keep_for: Option<KeepFor>,
    /// How long a wait holds while the run cannot pause (`--hold-secs`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hold_secs: Option<u32>,
    /// How long the run stays quiet on its worker before it pauses
    /// (`--pause-after-secs`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pause_after_secs: Option<u32>,
}

impl RunFlags {
    pub fn is_empty(&self) -> bool {
        self.from.is_empty()
            && self.target.is_empty()
            && self.before.is_empty()
            && self.group.is_none()
            && self.feed.is_empty()
            && self.fire.is_empty()
            && self.emit.is_empty()
            && self.instance.is_none()
            && self.clear.is_empty()
            && !self.durable
            && !self.fast
            && self.keep_for.is_none()
            && self.hold_secs.is_none()
            && self.pause_after_secs.is_none()
    }
}

/// Parse a node and JSON payload. Dots belong to the node id.
fn parse_node_flag(flag: &str, verb: &str) -> Result<(String, Value), String> {
    let (node, json) = flag.split_once('=').ok_or_else(|| format!("{verb} wants node=<json>, got '{flag}'"))?;
    if node.is_empty() {
        return Err(format!("{verb} requires a node name"));
    }
    let value = serde_json::from_str(json).map_err(|e| format!("{verb} {node}: the value must be JSON: {e}"))?;
    Ok((node.into(), value))
}

/// Repeated flags merge distinct ports; repeating a port is an error.
fn parse_port_flags(flags: &[String], verb: &str, allow_bare: bool) -> Result<PortValues, String> {
    let mut values: PortValues = BTreeMap::new();
    for flag in flags {
        let (node, json) = match flag.split_once('=') {
            Some(parts) => parts,
            None if allow_bare => (flag.as_str(), "{}"),
            None => return Err(format!("{verb} wants node=<json>, got '{flag}'")),
        };
        if node.is_empty() {
            return Err(format!("{verb} requires a node name"));
        }
        let ports = parse_port_object(json)
            .map_err(|e| format!("{verb} {node}: expected an object mapping ports to values; the value must be JSON: {e}"))?;
        let target = values.entry(node.into()).or_default();
        for (port, value) in ports {
            if target.contains_key(&port) {
                return Err(format!("{verb}: duplicate port '{node}.{port}'"));
            }
            target.insert(port, value);
        }
    }
    Ok(values)
}

/// The spec the flags describe, named `name`.
pub fn spec_from_flags(name: &str, flags: &RunFlags) -> Result<RunSpec, String> {
    apply_run_flags(&RunSpec::whole(name), flags)
}

/// Edit one loaded spec before graph validation. Clearing is explicit;
/// supplying a new start replaces the saved start set and its payloads.
pub fn apply_run_flags(base: &RunSpec, flags: &RunFlags) -> Result<RunSpec, String> {
    if flags.fire.len() > 1 {
        return Err("a run can fire only one trigger".into());
    }
    let mut spec = base.clone();
    for field in &flags.clear {
        match field.as_str() {
            "from" => spec.from.clear(),
            "emit" => spec.emit.clear(),
            "target" => spec.target.clear(),
            "before" => spec.before.clear(),
            "group" => spec.group = None,
            "feed" => spec.feed.clear(),
            "fire" => spec.fire = None,
            "instance" => spec.instance = None,
            "keeping" => spec.settings.keeping = None,
            "keep_for" => spec.settings.keep_for = None,
            "hold_secs" => spec.settings.hold_secs = None,
            "pause_after_secs" => spec.settings.pause_after_secs = None,
            _ => {
                return Err(format!(
                    "--clear: unknown setting '{field}'; use from, emit, target, before, group, feed, fire, instance, keeping, \
                     keep_for, hold_secs, or pause_after_secs"
                ))
            }
        }
    }
    if !flags.from.is_empty() {
        spec.from = parse_port_flags(&flags.from, "--from", true)?;
    }
    if !flags.target.is_empty() {
        spec.target = flags.target.clone();
    }
    if !flags.before.is_empty() {
        spec.before = flags.before.clone();
    }
    if !flags.feed.is_empty() {
        spec.feed = flags.feed.clone();
    }
    if let Some(group) = &flags.group {
        spec.group = parse_port_flags(std::slice::from_ref(group), "group", true)?.into_iter().next();
    }
    if let Some(fire) = flags.fire.first() {
        spec.fire = Some(parse_node_flag(fire, "--fire")?);
    }
    if let Some(instance) = &flags.instance {
        spec.instance = Some(instance.clone());
    }
    // What the run asks for by hand goes over the trigger it fires.
    if flags.durable && flags.fast {
        return Err("--durable and --fast ask for opposite things; pass one".into());
    }
    if flags.durable {
        spec.settings.keeping = Some(Keeping::Durable);
    }
    if flags.fast {
        spec.settings.keeping = Some(Keeping::Fast);
    }
    if flags.keep_for.is_some() {
        spec.settings.keep_for = flags.keep_for;
    }
    if let Some(secs) = flags.hold_secs {
        RunSettings::default().holding_for(secs)?;
        spec.settings.hold_secs = Some(secs);
    }
    if let Some(secs) = flags.pause_after_secs {
        RunSettings::default().pausing_after(secs)?;
        spec.settings.pause_after_secs = Some(secs);
    }
    for (node, ports) in parse_port_flags(&flags.emit, "--emit", false)? {
        spec.emit.entry(node).or_default().extend(ports);
    }
    Ok(spec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A run request carries the flags as the wire spells every field
    /// weft produces (camelCase), and leaves out what was not asked, so
    /// no flags at all is `{}`.
    #[test]
    fn run_flags_travel_as_camel_case_json_and_none_is_empty() {
        assert_eq!(serde_json::to_value(RunFlags::default()).unwrap(), json!({}));
        let flags = RunFlags {
            from: vec![r#"a={"in":1}"#.into()],
            group: Some("batch".into()),
            instance: Some(crate::instance::InstanceId::new("acme").unwrap()),
            durable: true,
            keep_for: Some(KeepFor::Forever),
            hold_secs: Some(30),
            pause_after_secs: Some(0),
            ..RunFlags::default()
        };
        let wire = serde_json::to_value(&flags).unwrap();
        assert_eq!(
            wire,
            json!({ "from": [r#"a={"in":1}"#], "group": "batch", "instance": "acme", "durable": true, "keepFor": "forever", "holdSecs": 30, "pauseAfterSecs": 0 })
        );
        assert_eq!(serde_json::from_value::<RunFlags>(wire).unwrap(), flags);
        assert_eq!(serde_json::from_value::<RunFlags>(json!({})).unwrap(), RunFlags::default());
    }

    #[test]
    fn starting_values_and_fire_flags_parse_and_refuse_plainly() {
        let p = parse_port_flags(&[r#"classify={"text":"hi"}"#.into(), r#"g.inner={"count":3}"#.into(), "bare".into()], "--from", true).unwrap();
        assert_eq!(p["classify"]["text"], json!("hi"));
        assert_eq!(p["g.inner"]["count"], json!(3));
        assert!(p["bare"].is_empty());
        assert!(parse_port_flags(&["classify=hi".into()], "--from", true).unwrap_err().contains("must be JSON"));
        assert!(parse_port_flags(&["classify=1".into()], "--from", true).unwrap_err().contains("expected an object"));
        assert!(parse_port_flags(&[r#"a={"x":1}"#.into(), r#"a={"x":2}"#.into()], "--from", true).unwrap_err().contains("duplicate port"));
        assert!(parse_port_flags(&["bare".into()], "--emit", false).is_err());
        let k = parse_node_flag(r#"inbound={"a":1}"#, "--fire").unwrap();
        assert_eq!(k, ("inbound".into(), json!({"a": 1})));
        assert!(parse_node_flag("inbound", "--fire").is_err());
    }

    #[test]
    fn flags_become_a_spec_and_no_flags_become_a_whole_run() {
        let flags = RunFlags { from: vec![r#"a={"in":1}"#.into()], ..RunFlags::default() };
        let spec = spec_from_flags("one-off", &flags).unwrap();
        assert_eq!(spec.from.keys().collect::<Vec<_>>(), vec!["a"]);
        assert_eq!(spec.from["a"]["in"], json!(1));
        let spec = spec_from_flags("plain", &RunFlags::default()).unwrap();
        assert_eq!(spec, RunSpec::whole("plain"));
        let group = spec_from_flags("batch", &RunFlags { group: Some(r#"batch={"items":[1,2]}"#.into()), ..Default::default() }).unwrap();
        assert_eq!(group.group, Some(("batch".into(), [("items".into(), json!([1,2]))].into())));
        let bare = spec_from_flags("batch", &RunFlags { group: Some("batch".into()), ..Default::default() }).unwrap();
        assert_eq!(bare.group, Some(("batch".into(), BTreeMap::new())));
    }

    #[test]
    fn loaded_start_replacement_preserves_other_bounds_and_does_not_move_old_values() {
        let saved: RunSpec = serde_json::from_value(json!({
            "name":"case", "from":{"old":{"text":"saved"}}, "target":["end"],
            "before":["publish"], "caller":["history"]
        })).unwrap();
        let edited = apply_run_flags(&saved, &RunFlags { from:vec!["renamed".into()], ..Default::default() }).unwrap();
        assert_eq!(edited.from, PortValues::from([("renamed".into(), BTreeMap::new())]));
        assert_eq!(edited.target, saved.target);
        assert_eq!(edited.before, saved.before);
        assert_eq!(edited.caller, saved.caller);
        assert_eq!(saved.from["old"]["text"], json!("saved"));
    }

    #[test]
    fn explicit_clearing_happens_before_replacement_and_rejects_unknown_settings() {
        let saved: RunSpec = serde_json::from_value(json!({
            "name":"case", "from":{"old":{}}, "target":["deleted"], "group":["old_group",{}],
            "emit":{"stub":{"out":1}}, "fire":["trigger",null]
        })).unwrap();
        let edited = apply_run_flags(&saved, &RunFlags {
            clear: vec!["from", "target", "group", "emit", "fire"].into_iter().map(String::from).collect(),
            from: vec![r#"new={"text":"now"}"#.into()], ..Default::default()
        }).unwrap();
        assert_eq!(edited.from["new"]["text"], json!("now"));
        assert!(edited.target.is_empty() && edited.emit.is_empty());
        assert!(edited.group.is_none() && edited.fire.is_none());
        for field in ["input", "scope", "expected", "misspelled"] {
            assert!(apply_run_flags(&saved, &RunFlags { clear:vec![field.into()], ..Default::default() }).is_err());
        }
    }

    #[test]
    fn feed_is_saved_with_the_spec_and_cleared_on_request() {
        let spec = spec_from_flags("case", &RunFlags {
            from: vec!["hear.note".into()], feed: vec!["hear.note".into()], ..Default::default()
        }).unwrap();
        assert_eq!(spec.feed, vec!["hear.note".to_string()]);
        let saved: RunSpec = serde_json::from_value(serde_json::to_value(&spec).unwrap()).unwrap();
        assert_eq!(saved.feed, spec.feed, "a saved example keeps it");
        let cleared = apply_run_flags(&saved, &RunFlags { clear: vec!["feed".into()], ..Default::default() }).unwrap();
        assert!(cleared.feed.is_empty());
    }

    #[test]
    fn durable_is_saved_with_the_spec_and_cleared_on_request() {
        let spec = spec_from_flags("case", &RunFlags { durable: true, ..Default::default() }).unwrap();
        assert_eq!(spec.settings.keeping, Some(Keeping::Durable));
        let saved: RunSpec = serde_json::from_value(serde_json::to_value(&spec).unwrap()).unwrap();
        assert_eq!(saved.settings, spec.settings, "a saved example keeps it");
        let cleared = apply_run_flags(&saved, &RunFlags { clear: vec!["keeping".into()], ..Default::default() }).unwrap();
        assert!(cleared.settings.is_empty(), "cleared, the run follows its trigger again");
        assert!(spec_from_flags("case", &RunFlags { durable: true, fast: true, ..Default::default() }).is_err());
    }

    #[test]
    fn keep_for_is_saved_with_the_spec_and_cleared_on_request() {
        let forever = Some(KeepFor::Forever);
        let spec = spec_from_flags("case", &RunFlags { keep_for: forever, ..Default::default() }).unwrap();
        assert_eq!(spec.settings.keep_for, forever);
        let saved: RunSpec = serde_json::from_value(serde_json::to_value(&spec).unwrap()).unwrap();
        assert_eq!(saved.settings, spec.settings, "a saved example keeps it");
        let cleared = apply_run_flags(&saved, &RunFlags { clear: vec!["keep_for".into()], ..Default::default() }).unwrap();
        assert!(cleared.settings.is_empty(), "cleared, the run is kept as its trigger or project says");
    }

    #[test]
    fn hold_secs_is_saved_with_the_spec_checked_and_cleared_on_request() {
        let spec = spec_from_flags("case", &RunFlags { hold_secs: Some(0), ..Default::default() }).unwrap();
        assert_eq!(spec.settings.hold_secs, Some(0));
        let saved: RunSpec = serde_json::from_value(serde_json::to_value(&spec).unwrap()).unwrap();
        assert_eq!(saved.settings, spec.settings, "a saved example keeps it");
        let cleared = apply_run_flags(&saved, &RunFlags { clear: vec!["hold_secs".into()], ..Default::default() }).unwrap();
        assert!(cleared.settings.is_empty(), "cleared, the run holds as its trigger says");
        let too_long = crate::run_settings::MAX_HOLD_SECS + 1;
        assert!(spec_from_flags("case", &RunFlags { hold_secs: Some(too_long), ..Default::default() }).is_err());
    }

    #[test]
    fn pause_after_secs_is_saved_with_the_spec_checked_and_cleared_on_request() {
        let spec = spec_from_flags("case", &RunFlags { pause_after_secs: Some(0), ..Default::default() }).unwrap();
        assert_eq!(spec.settings.pause_after_secs, Some(0));
        let saved: RunSpec = serde_json::from_value(serde_json::to_value(&spec).unwrap()).unwrap();
        assert_eq!(saved.settings, spec.settings, "a saved example keeps it");
        let cleared = apply_run_flags(&saved, &RunFlags { clear: vec!["pause_after_secs".into()], ..Default::default() }).unwrap();
        assert!(cleared.settings.is_empty(), "cleared, the run pauses as its trigger says");
        let too_long = crate::run_settings::MAX_PAUSE_AFTER_SECS + 1;
        assert!(spec_from_flags("case", &RunFlags { pause_after_secs: Some(too_long), ..Default::default() }).is_err());
    }

    #[test]
    fn emitted_port_edits_merge_with_saved_values_but_duplicate_new_ports_are_errors() {
        let saved: RunSpec = serde_json::from_value(json!({"name":"case","emit":{"a":{"x":1,"y":2},"b":{"z":3}}})).unwrap();
        let edited = apply_run_flags(&saved, &RunFlags { emit:vec![r#"a={"x":9}"#.into()], ..Default::default() }).unwrap();
        assert_eq!(serde_json::to_value(&edited.emit).unwrap(), json!({"a":{"x":9,"y":2},"b":{"z":3}}));
        assert!(apply_run_flags(&saved, &RunFlags {
            emit:vec![r#"a={"x":9}"#.into(), r#"a={"x":10}"#.into()], ..Default::default()
        }).is_err());
    }
}
