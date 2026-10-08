//! A node's card from the terminal: `weft infra show <node>` reads it,
//! `weft infra press <node> <action>` presses one of its buttons.
//!
//! Generic over nodes: the card is whatever the node's display (`/live`)
//! serves, in the shape of `weft_core::live`. A `secret` item is never
//! printed, not even under `--json`; `weft infra env` is the one way
//! its value leaves the card, and it goes into a file.

use anyhow::{bail, Context, Result};
use serde_json::Value;
use weft_core::live::{LiveAction, LiveFeed, LiveItem, LiveItemKind};

use super::Ctx;
use crate::client::DispatcherClient;

/// One node's card: where to read it and where a press goes.
pub(crate) struct Card {
    client: DispatcherClient,
    base: String,
    query: String,
    /// The node's place as the daemon keys it (`one.db`).
    pub place: String,
}

impl Card {
    pub async fn open(ctx: &Ctx, node: &str, instance: Option<&weft_core::instance::InstanceId>) -> Result<Self> {
        let place = super::infra::place_named(ctx, node).await?;
        let (client, project_id, _) = super::resolve_project(ctx)?;
        let query = match instance {
            Some(instance) => format!("?instance={instance}"),
            None => String::new(),
        };
        let base = format!("/projects/{project_id}/infra/nodes/{place}");
        Ok(Self { client, base, query, place })
    }

    pub async fn read(&self) -> Result<LiveFeed> {
        let answer = self.client.get_json(&format!("{}/live{}", self.base, self.query)).await?;
        serde_json::from_value::<LiveFeed>(answer).context("read the node's display")
    }

    /// Press `action` and hand back what the node answered.
    pub async fn press(&self, action: &LiveAction) -> Result<Value> {
        self.client.post_json(&format!("{}/action{}", self.base, self.query), &serde_json::to_value(action.press())?).await
    }
}

/// `weft infra rebake <node>`: make the node's baked outputs again, and
/// answer once its infra setup ended.
pub async fn run_rebake(ctx: Ctx, node: &str, instance: Option<&weft_core::instance::InstanceId>) -> Result<()> {
    let card = Card::open(&ctx, node, instance).await?;
    card.client.post_json(&format!("{}/rebake{}", card.base, card.query), &serde_json::json!({})).await?;
    if !ctx.json_out(&serde_json::json!({ "node": card.place, "rebaked": true }))? {
        println!("baked {} again: every fire reads its new values from now on", card.place);
    }
    Ok(())
}

/// An item's own text, as a reader of the display would see it.
pub(crate) fn item_text(item: &LiveItem) -> String {
    match item.data.as_str() {
        Some(text) => text.to_string(),
        None => item.data.to_string(),
    }
}

/// What `show` prints for an item's value: a secret is never its value.
const SECRET_SHOWN: &str = "(hidden; `weft infra env` writes it into a file)";

pub struct ShowArgs {
    pub node: String,
    pub instance: Option<weft_core::instance::InstanceId>,
}

pub struct PressArgs {
    pub node: String,
    pub action: String,
    pub instance: Option<weft_core::instance::InstanceId>,
}

/// The card as `--json` reports it: every item and button, a secret
/// item as present with no value.
fn card_json(place: &str, feed: &LiveFeed) -> Value {
    let items: Vec<Value> = feed
        .items
        .iter()
        .map(|item| {
            let mut out = serde_json::json!({ "label": item.label, "kind": item.kind.wire() });
            if item.kind == LiveItemKind::Secret {
                out["secret"] = Value::Bool(true);
            } else {
                out["value"] = item.data.clone();
            }
            if let Some(action) = &item.action {
                out["button"] = serde_json::json!({
                    "label": action.label,
                    "action": action.action_kind,
                    "confirm": action.confirm,
                });
            }
            out
        })
        .collect();
    serde_json::json!({ "node": place, "items": items })
}

/// The card as a person reads it: one line per item, its button under it.
fn card_text(place: &str, feed: &LiveFeed) -> String {
    if feed.is_empty() {
        return format!("{place} shows nothing right now\n");
    }
    let mut out = String::new();
    for item in &feed.items {
        let value = if item.kind == LiveItemKind::Secret { SECRET_SHOWN.to_string() } else { item_text(item) };
        out.push_str(&format!("{} ({}): {value}\n", item.label, item.kind.wire()));
        if let Some(action) = &item.action {
            out.push_str(&format!(
                "  button '{}': weft infra press {place} {}\n",
                action.label, action.action_kind
            ));
            if let Some(confirm) = &action.confirm {
                out.push_str(&format!("    it warns: {confirm}\n"));
            }
        }
    }
    out
}

pub async fn run_show(ctx: Ctx, args: ShowArgs) -> Result<()> {
    let card = Card::open(&ctx, &args.node, args.instance.as_ref()).await?;
    let feed = card.read().await?;
    if !ctx.json_out(&card_json(&card.place, &feed))? {
        print!("{}", card_text(&card.place, &feed));
    }
    Ok(())
}

/// The button whose action kind is `kind`, or a refusal listing the ones
/// the card has.
fn find_button<'a>(place: &str, feed: &'a LiveFeed, kind: &str) -> Result<&'a LiveAction> {
    let buttons: Vec<&LiveAction> = feed.items.iter().filter_map(|i| i.action.as_ref()).collect();
    if let Some(found) = buttons.iter().find(|a| a.action_kind == kind) {
        return Ok(found);
    }
    if buttons.is_empty() {
        bail!("{place}'s card has no button right now, so there is no '{kind}' to press");
    }
    let listed = buttons
        .iter()
        .map(|a| format!("{} ('{}')", a.action_kind, a.label))
        .collect::<Vec<_>>()
        .join(", ");
    bail!("{place}'s card has no '{kind}' button; it has {listed}")
}

/// `answer` with every string equal to one of `secrets` masked, so a
/// node that answers a press with the value it shows as a secret does
/// not get it printed.
fn redact(answer: Value, secrets: &[String]) -> Value {
    match answer {
        Value::String(s) if secrets.contains(&s) => Value::String("(hidden)".into()),
        Value::Array(items) => Value::Array(items.into_iter().map(|v| redact(v, secrets)).collect()),
        Value::Object(map) => Value::Object(map.into_iter().map(|(k, v)| (k, redact(v, secrets))).collect()),
        other => other,
    }
}

pub async fn run_press(ctx: Ctx, args: PressArgs) -> Result<()> {
    let card = Card::open(&ctx, &args.node, args.instance.as_ref()).await?;
    let feed = card.read().await?;
    let action = find_button(&card.place, &feed, &args.action)?;
    // Naming the button's action on the command line is the choice the
    // editor's confirm box asks for, so the press never asks again; the
    // button's warning is printed with the result instead.
    let answer = card.press(action).await?;
    // A node that answers a press with the value its card shows as a
    // secret does not get it printed. The mask comes from the card as it
    // was read before the press: once the press is done, nothing here may
    // fail and make it look undone.
    let secrets: Vec<String> =
        feed.items.iter().filter(|i| i.kind == LiveItemKind::Secret).map(item_text).collect();
    let answer = redact(answer, &secrets);
    let report = serde_json::json!({
        "node": card.place,
        "pressed": action.action_kind,
        "answer": answer,
        "warning": action.confirm,
    });
    if !ctx.json_out(&report)? {
        println!("pressed '{}' on {}; it answered {answer}", action.label, card.place);
        if let Some(confirm) = &action.confirm {
            println!("the button warns: {confirm}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn card() -> LiveFeed {
        LiveFeed::new(vec![
            LiveItem::text("User", "weft"),
            LiveItem::secret("Password", "s3cret")
                .with_action(LiveAction::new("Reset password", "reset_password").confirm("Break every connection?")),
        ])
    }

    #[test]
    fn show_never_prints_a_secret() {
        let feed = card();
        let text = card_text("db", &feed);
        assert!(!text.contains("s3cret"), "{text}");
        assert!(text.contains("weft infra press db reset_password"), "{text}");
        assert!(text.contains("Break every connection?"), "{text}");
        let json = card_json("db", &feed).to_string();
        assert!(!json.contains("s3cret"), "{json}");
        assert!(json.contains("\"secret\":true"), "{json}");
        assert!(json.contains("reset_password"), "{json}");
    }

    #[test]
    fn a_missing_button_lists_the_ones_there_are() {
        let feed = card();
        assert_eq!(find_button("db", &feed, "reset_password").unwrap().label, "Reset password");
        let err = find_button("db", &feed, "nope").unwrap_err().to_string();
        assert!(err.contains("reset_password ('Reset password')"), "{err}");
        let empty = LiveFeed::new(vec![LiveItem::text("User", "weft")]);
        assert!(find_button("db", &empty, "x").unwrap_err().to_string().contains("no button"));
    }

    #[test]
    fn an_answer_echoing_a_secret_is_masked() {
        let answer = serde_json::json!({ "reset": true, "password": "s3cret", "list": ["s3cret", "ok"] });
        let out = redact(answer, &["s3cret".to_string()]);
        assert_eq!(out, serde_json::json!({ "reset": true, "password": "(hidden)", "list": ["(hidden)", "ok"] }));
    }
}
