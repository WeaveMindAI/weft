//! Every trigger in the shipped catalog declares what it wakes with,
//! and every declaration is a real weft type.
//!
//! `firesWith` is read by nothing at build time (the engine checks a
//! payload against it at run time, and `weft run --fire` prints it),
//! so a typo in a type string would otherwise sit there until somebody
//! fired that trigger and got a confusing refusal. This walks the
//! catalog on disk and parses every one.

use std::path::{Path, PathBuf};

/// Every `metadata.json` under `catalog/`.
fn metadata_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read catalog dir") {
        let entry = entry.expect("catalog entry");
        let path = entry.path();
        if entry.file_type().expect("file type").is_dir() {
            metadata_files(&path, out);
        } else if path.file_name().is_some_and(|n| n == "metadata.json") {
            out.push(path);
        }
    }
}

/// The two triggers that deliberately declare nothing, and why. Both
/// are checked by name so that REMOVING a declaration from any other
/// trigger fails here instead of going unnoticed.
///
/// `receive_email` never reads its wake payload at all: it opens its
/// own IMAP session and reads the mail itself, so there is no field to
/// declare. `human/trigger`'s payload keys are the form fields the
/// author typed into the node, so they differ per instance and no
/// static declaration can name them.
const NOTHING_TO_DECLARE: [&str; 2] = ["ReceiveEmail", "HumanTrigger"];

#[test]
fn every_catalog_trigger_declares_a_fire_payload_that_parses() {
    let catalog = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../catalog");
    assert!(catalog.is_dir(), "catalog missing at {}", catalog.display());
    let mut files = Vec::new();
    metadata_files(&catalog, &mut files);
    assert!(files.len() > 50, "expected the whole catalog, found {}", files.len());

    let mut triggers = 0;
    for path in &files {
        let text = std::fs::read_to_string(path).expect("read metadata");
        let raw: serde_json::Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            // A package root's partial metadata is merged into its
            // members and need not stand alone.
            Err(_) => continue,
        };
        let is_trigger = raw
            .get("features")
            .and_then(|f| f.get("isTrigger"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        if !is_trigger {
            assert!(
                raw.get("firesWith").is_none(),
                "{} is not a trigger, so it wakes with nothing and must not declare firesWith",
                path.display()
            );
            continue;
        }
        triggers += 1;
        let node_type = raw.get("type").and_then(serde_json::Value::as_str).unwrap_or_default();
        let fires: std::collections::BTreeMap<String, String> = match raw.get("firesWith") {
            Some(v) => serde_json::from_value(v.clone())
                .unwrap_or_else(|e| panic!("{}: firesWith is not a map of name to type: {e}", path.display())),
            None => {
                assert!(
                    NOTHING_TO_DECLARE.contains(&node_type),
                    "trigger {node_type} ({}) declares no firesWith; every trigger should say \
                     what it wakes with, and the two that genuinely cannot are listed in \
                     NOTHING_TO_DECLARE with the reason",
                    path.display()
                );
                continue;
            }
        };
        assert!(
            !NOTHING_TO_DECLARE.contains(&node_type),
            "{node_type} is listed as having nothing to declare but now declares a payload; \
             drop it from NOTHING_TO_DECLARE"
        );
        let parsed = weft_core::node::fire_payload_type(&fires)
            .unwrap_or_else(|why| panic!("{}: {why}", path.display()));
        assert!(parsed.is_some(), "{}: a declared firesWith parsed as nothing", path.display());
    }
    assert!(triggers >= 17, "expected the catalog's triggers, found {triggers}");
}

/// A trigger fed by a provider's events declares EVERY field that
/// provider's topic can send, no more and no less.
///
/// The payload a provider trigger wakes with is built by
/// `EventsSpec::named_event`, which reads exactly the keys the
/// service's topic names (`fields`) and drops the ones the raw event
/// did not carry. So the set of keys that can ever arrive is precisely
/// that topic's field names, and a declaration that omits one is a
/// trigger that dies the first time the provider sends it.
///
/// That is not hypothetical: `SlackReceiveMessage` declared five of the
/// `messages` topic's eight fields, and every real Slack message
/// carries `type`, so every run of it failed its own contract before
/// its body started.
///
/// Whether a field takes `?` is a judgement about the provider that no
/// test can make, so this checks the NAMES only. Getting that wrong is
/// visible and quick to fix; a missing name is neither.
#[test]
fn a_provider_trigger_declares_every_field_its_topic_can_send() {
    let catalog = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../catalog");
    let mut files = Vec::new();
    metadata_files(&catalog, &mut files);

    let mut checked = 0;
    for path in &files {
        let dir = path.parent().expect("metadata has a directory");
        let Ok(body) = std::fs::read_to_string(dir.join("mod.rs")) else { continue };
        // Which topic this node subscribes to, from its own source:
        // `ProviderEvents::new(&account, "messages", ...)`.
        let Some(topic) = body
            .split_once("ProviderEvents::new(")
            .and_then(|(_, rest)| rest.split_once('"'))
            .and_then(|(_, rest)| rest.split_once('"'))
            .map(|(topic, _)| topic.to_string())
        else {
            continue;
        };
        let raw: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(path).expect("read metadata"))
                .expect("a node's metadata parses");
        if !raw
            .get("features")
            .and_then(|f| f.get("isTrigger"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            // An `await_signal` on a provider topic is a resume, not a
            // firing, and wakes through its own await rather than here.
            continue;
        }
        let node_type = raw.get("type").and_then(serde_json::Value::as_str).unwrap_or_default();

        // The service's own access node holds the topic list. It sits
        // beside the trigger, one level up, under `access/`.
        let access = dir.parent().expect("a node lives in its package").join("access/metadata.json");
        let access: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&access).unwrap_or_else(|e| {
                panic!("{node_type} subscribes to '{topic}' but its service has no access node at {}: {e}", access.display())
            }))
            .expect("the access metadata parses");
        let fields = find_topic_fields(&access, &topic).unwrap_or_else(|| {
            panic!("{node_type} subscribes to '{topic}', which its service does not declare")
        });

        let declared: std::collections::BTreeSet<String> = raw
            .get("firesWith")
            .and_then(serde_json::Value::as_object)
            .map(|m| m.keys().map(|k| k.trim_end_matches('?').to_string()).collect())
            .unwrap_or_default();

        let missing: Vec<&String> = fields.difference(&declared).collect();
        assert!(
            missing.is_empty(),
            "{node_type} wakes on the '{topic}' topic, which can send {missing:?}, and its \
             firesWith does not name them. A payload carrying one is refused, so the trigger \
             fails the moment the provider sends it. Add each to firesWith, with `?` when it \
             only sometimes arrives."
        );
        let invented: Vec<&String> = declared.difference(&fields).collect();
        assert!(
            invented.is_empty(),
            "{node_type} declares {invented:?}, which the '{topic}' topic never sends: a \
             required one can never be satisfied, and an optional one is a field nobody \
             will ever see"
        );
        checked += 1;
    }
    assert!(checked >= 5, "expected the provider-event triggers, found {checked}");
}

/// The field names one topic can send, wherever the service's access
/// metadata keeps its `events` map.
fn find_topic_fields(
    access: &serde_json::Value,
    topic: &str,
) -> Option<std::collections::BTreeSet<String>> {
    match access {
        serde_json::Value::Object(map) => {
            if let Some(fields) = map
                .get("events")
                .and_then(serde_json::Value::as_object)
                .and_then(|events| events.get(topic))
                .and_then(|t| t.get("fields"))
                .and_then(serde_json::Value::as_object)
            {
                return Some(fields.keys().cloned().collect());
            }
            map.values().find_map(|v| find_topic_fields(v, topic))
        }
        serde_json::Value::Array(items) => items.iter().find_map(|v| find_topic_fields(v, topic)),
        _ => None,
    }
}
