//! `weft member-values`: what one member of the program provides for the
//! fields it writes `@member_filled`, read and changed from the terminal
//! through the member door, exactly as that member's own page would. For
//! trying a program's member path before any website exists.
//!
//! Also home to [`as_member`], the one way the CLI speaks at the member
//! door: a member token minted for the command alone, revoked when it
//! ends, whatever the outcome (`weft connect --member` uses it too).

use std::future::Future;

use anyhow::{bail, Context, Result};
use serde_json::Value;

use weft_core::member::MemberId;
use weft_core::member_door::{MemberField, ValuesChanged};

use crate::client::DispatcherClient;
use crate::commands::Ctx;

/// Run `work` as `member` of the project here, at the member door, with a
/// member token minted for it and revoked after, even when `work` fails.
pub(crate) async fn as_member<T, F, Fut>(
    ctx: &Ctx,
    client: &DispatcherClient,
    member: &MemberId,
    work: F,
) -> Result<T>
where
    F: FnOnce(DispatcherClient) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let project = ctx.project()?.id().to_string();
    let minted = client
        .post_json(
            "/signal-tokens",
            &serde_json::json!({
                "name": format!("weft CLI acting as member {member}"),
                "allowedProjects": [project],
                "member": member,
                "expiresInSecs": 3600,
            }),
        )
        .await
        .context("mint a member token for this command (is the project registered? `weft build` does it)")?;
    let (Some(token), Some(token_id)) =
        (minted.get("token").and_then(Value::as_str), minted.get("id").and_then(Value::as_str))
    else {
        bail!("the member token mint answered a shape this CLI cannot read: {minted}");
    };
    let outcome = work(client.with_bearer(token)?).await;
    let revoked = client.delete(&format!("/signal-tokens/{token_id}")).await;
    match (outcome, revoked) {
        (Ok(value), Ok(_)) => Ok(value),
        (Ok(_), Err(revoke)) => Err(revoke.context(format!(
            "revoke the member token this command minted (`weft token revoke {token_id}`)"
        ))),
        (Err(work), Ok(_)) => Err(work),
        (Err(work), Err(revoke)) => Err(work.context(format!(
            "and the member token this command minted ({token_id}) could not be revoked either ({revoke:#}); \
             revoke it with `weft token revoke {token_id}`"
        ))),
    }
}

/// `node.field` (the node spelled the way the program reads it, which may
/// hold dots of its own: `one.read.spreadsheet`) split at its last dot.
fn field_of(raw: &str) -> Result<(String, String)> {
    match raw.rsplit_once('.') {
        Some((step, field)) if !step.is_empty() && !field.is_empty() => Ok((step.to_string(), field.to_string())),
        _ => bail!("'{raw}' is not `<node>.<field>` (`read.spreadsheet`, `one.read.spreadsheet`)"),
    }
}

/// `--set node.field=<value>`: the value is JSON when it reads as JSON
/// (`5`, `true`, `{"id": ".."}`), a string otherwise.
fn set_of(raw: &str) -> Result<Value> {
    let (target, value) = raw.split_once('=').with_context(|| format!("--set '{raw}' is not `<node>.<field>=<value>`"))?;
    let (step, field) = field_of(target)?;
    let value = serde_json::from_str::<Value>(value).unwrap_or_else(|_| Value::String(value.to_string()));
    Ok(serde_json::json!({ "step": step, "field": field, "value": value }))
}

pub async fn run(ctx: Ctx, member: MemberId, set: Vec<String>, clear: Vec<String>) -> Result<()> {
    let set: Vec<Value> = set.iter().map(|raw| set_of(raw)).collect::<Result<_>>()?;
    let clear: Vec<Value> = clear
        .iter()
        .map(|raw| field_of(raw).map(|(step, field)| serde_json::json!({ "step": step, "field": field })))
        .collect::<Result<_>>()?;
    let json = ctx.json();
    let client = ctx.client();
    let who = member.clone();
    as_member(&ctx, &client, &who, |door| async move {
        if !set.is_empty() || !clear.is_empty() {
            let changed: ValuesChanged =
                serde_json::from_value(door.put_json("/member/values", &serde_json::json!({ "set": set, "clear": clear })).await?)
                    .context("read the dispatcher's answer to the change; upgrade the dispatcher or this CLI so the versions match")?;
            let rearmed = changed.rearmed;
            if json {
                println!("{}", serde_json::json!({ "member": member, "rearmed": rearmed }));
            } else {
                println!("Stored for member '{member}'.");
                if !rearmed.is_empty() {
                    println!("Their trigger(s) reading it were set up again: {}.", rearmed.join(", "));
                }
            }
            return Ok(());
        }
        let answer = door.get_json("/member/fields").await?;
        let fields: Vec<MemberField> = serde_json::from_value(answer.clone())
            .context("read the member's fields; upgrade the dispatcher or this CLI so the versions match")?;
        if json {
            println!("{}", serde_json::json!({ "member": member, "fields": answer }));
            return Ok(());
        }
        if fields.is_empty() {
            println!("The program has no field a member fills (none is `@member_filled`).");
        }
        for field in fields {
            let name = format!("{}.{}", field.step, field.field);
            let shown = match (&field.value, &field.fallback) {
                (Some(value), _) => value.to_string(),
                (None, Some(fallback)) => format!("(not filled; {fallback} stands in)"),
                (None, None) if field.needed => "(not filled; a run needs it)".to_string(),
                (None, None) => "(not filled)".to_string(),
            };
            println!("  {name}  {shown}");
        }
        Ok(())
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_field_splits_at_its_last_dot_and_a_value_reads_as_json_first() {
        assert_eq!(field_of("one.read.sheet").unwrap(), ("one.read".to_string(), "sheet".to_string()));
        assert!(field_of("read").is_err());
        assert_eq!(set_of("c.cron=0 0 3 * * *").unwrap()["value"], serde_json::json!("0 0 3 * * *"));
        assert_eq!(set_of("n.count=5").unwrap()["value"], serde_json::json!(5));
        assert!(set_of("n.count").is_err());
    }
}
