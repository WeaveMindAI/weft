//! `weft instance-values`: what one instance of the program holds for the
//! fields it writes `@instance_filled`, read and changed from the
//! terminal through the instance door, exactly as that instance's own
//! page would. For trying a program's per-instance path before any
//! website exists.
//!
//! Also home to [`as_instance`], the one way the CLI speaks at the
//! instance door: an instance token minted for the command alone,
//! revoked when it ends, whatever the outcome (`weft connect --instance`
//! uses it too).

use std::future::Future;

use anyhow::{bail, Context, Result};
use serde_json::Value;

use weft_core::instance::InstanceId;
use weft_core::instance_door::{InstanceField, ValuesChanged, ValuesRequest};
use weft_core::run_spec::{InstanceFieldRef, InstanceValueInput};
use weft_core::signal_token::{MintTokenRequest, MintedToken};

use crate::client::DispatcherClient;
use crate::commands::Ctx;

/// How long each instance token this CLI mints works. The command may
/// wait on a person for hours (a browser sign-in), so a fresh token is
/// minted every `RENEW_EVERY` for as long as the command runs; the
/// expiry only bounds a token left behind by a process killed before
/// it could revoke.
const INSTANCE_TOKEN_SECS: u64 = 60 * 60;
/// The arithmetic: a token minted at t works until t+60min. Its
/// successor comes at t+20min, and it is revoked at the renewal after
/// that (t+40min), so a request or stream still holding it gets a whole
/// period of grace with 20 minutes to spare. If renewals start failing
/// at t+20min, the token in use still works until t+60min: 40 minutes
/// of retries every `RETRY_EVERY` before anything is refused.
const RENEW_EVERY: std::time::Duration = std::time::Duration::from_secs(20 * 60);
const RETRY_EVERY: std::time::Duration = std::time::Duration::from_secs(15);

/// How long the renewer waits before its next attempt, given how the
/// last one went.
fn next_wait(last_renewal_ok: bool) -> std::time::Duration {
    if last_renewal_ok { RENEW_EVERY } else { RETRY_EVERY }
}

/// The tokens a successful renewal revokes, out of every id still live
/// in mint order: all but the newest (the bearer now) and the one it
/// replaced (in its grace period).
fn past_grace(live: &[uuid::Uuid]) -> &[uuid::Uuid] {
    &live[..live.len().saturating_sub(2)]
}

/// Every minted token id not yet revoked, oldest first, shared by the
/// command and its renewer.
type LiveTokens = std::sync::Arc<std::sync::Mutex<Vec<uuid::Uuid>>>;

fn live_ids(live: &LiveTokens) -> Result<std::sync::MutexGuard<'_, Vec<uuid::Uuid>>> {
    live.lock().map_err(|_| anyhow::anyhow!("the token list lock was poisoned"))
}

/// Run `work` inside `instance` of the project here, at the instance
/// door, with an instance token minted for it, renewed while `work`
/// runs (so the command has no deadline of its own), and every token
/// revoked after: when `work` succeeds, fails, or the person presses
/// Ctrl+C.
pub(crate) async fn as_instance<T, F, Fut>(
    ctx: &Ctx,
    client: &DispatcherClient,
    instance: &InstanceId,
    work: F,
) -> Result<T>
where
    F: FnOnce(DispatcherClient) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let request = MintTokenRequest {
        allowed_projects: vec![ctx.project()?.id()],
        instance: Some(instance.clone()),
        expires_in_secs: Some(INSTANCE_TOKEN_SECS),
        ..MintTokenRequest::caller(format!("weft CLI acting in instance {instance}"))
    };
    let first = mint(client, &request).await.with_context(|| {
        format!("mint an instance token for this command (is the project registered? `{}` does it)", ctx.weft("build"))
    })?;
    let door = client.with_bearer(&first.token);
    let live: LiveTokens = std::sync::Arc::new(std::sync::Mutex::new(vec![first.id]));
    let (stop, stopped) = tokio::sync::watch::channel(());
    let renewer = tokio::spawn(renew_until_stopped(client.clone(), door.clone(), request, live.clone(), stopped));
    let outcome = tokio::select! {
        outcome = work(door) => outcome,
        _ = tokio::signal::ctrl_c() => Err(anyhow::anyhow!("interrupted (Ctrl+C)")),
    };
    // The renewer finishes the step it is in (a token it minted is on
    // the list before it looks again), then sees the signal and ends.
    // No deadline on the cleanup: a second Ctrl+C is the way out, and it
    // names every token left behind.
    let _ = stop.send(());
    eprintln!("revoking this command's instance token(s); Ctrl+C again to stop and leave them");
    let mut renewer = renewer;
    let cleanup = async {
        let renewer_ended = (&mut renewer).await.context("the token renewer crashed");
        let ids = live_ids(&live)?.clone();
        let failed = revoke_listed(client, &live, ids).await?;
        anyhow::Ok((renewer_ended, failed))
    };
    let (renewer_ended, failed) = tokio::select! {
        done = cleanup => done?,
        _ = tokio::signal::ctrl_c() => {
            renewer.abort();
            let left = live_ids(&live)?.clone();
            for id in &left {
                eprintln!("left live: instance token {id}; revoke it with `{}`", ctx.weft(&format!("token revoke {id}")));
            }
            bail!(
                "interrupted (Ctrl+C) while revoking; {} instance token(s) left live (named above), and one being \
                 minted at that moment may have survived too: `{}` shows it",
                left.len(),
                ctx.weft("token list")
            );
        }
    };
    for (id, e) in &failed {
        eprintln!("warning: could not revoke instance token {id} ({e:#}); revoke it with `{}`", ctx.weft(&format!("token revoke {id}")));
    }
    let value = outcome?;
    renewer_ended?;
    if !failed.is_empty() {
        bail!("{} instance token(s) this command minted are still live (named above)", failed.len());
    }
    Ok(value)
}

/// Revoke every one of `ids`, trying all of them, and drop each one
/// revoked (or already gone: revoked elsewhere, or gone with its
/// project) from `live`. What comes back is every id that failed, with
/// why; those stay on `live`.
async fn revoke_listed(
    client: &DispatcherClient,
    live: &LiveTokens,
    ids: Vec<uuid::Uuid>,
) -> Result<Vec<(uuid::Uuid, anyhow::Error)>> {
    let mut failed = Vec::new();
    for id in ids {
        match client.delete_idempotent(&format!("/signal-tokens/{id}")).await {
            Ok(()) => live_ids(live)?.retain(|live_id| *live_id != id),
            Err(e) => failed.push((id, e)),
        }
    }
    Ok(failed)
}

async fn mint(client: &DispatcherClient, request: &MintTokenRequest) -> Result<MintedToken> {
    let minted = client
        .post_json("/signal-tokens", &serde_json::to_value(request)?)
        .await
        .context("mint an instance token")?;
    serde_json::from_value(minted).context("read the instance token the install minted")
}

/// Renew on `next_wait`'s schedule until `stopped` fires. The signal is
/// only looked at between renewals, never in the middle of one.
async fn renew_until_stopped(
    client: DispatcherClient,
    door: DispatcherClient,
    request: MintTokenRequest,
    live: LiveTokens,
    mut stopped: tokio::sync::watch::Receiver<()>,
) {
    let mut wait = next_wait(true);
    loop {
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = stopped.changed() => return,
        }
        let renewed = renew_once(&client, &door, &request, &live).await;
        if let Err(e) = &renewed {
            eprintln!(
                "warning: could not renew this command's instance token ({e:#}); trying again in {} seconds",
                RETRY_EVERY.as_secs()
            );
        }
        wait = next_wait(renewed.is_ok());
    }
}

/// Mint a fresh token, add it to `live` and put it on `door` (every
/// clone sees the swap): that is the renewal. Then, best effort, revoke
/// the tokens past their grace; one that fails is said and stays on the
/// list, for the next renewal or the command's final cleanup.
async fn renew_once(client: &DispatcherClient, door: &DispatcherClient, request: &MintTokenRequest, live: &LiveTokens) -> Result<()> {
    let fresh = mint(client, request).await?;
    live_ids(live)?.push(fresh.id);
    door.replace_bearer(&fresh.token)?;
    let expired: Vec<uuid::Uuid> = past_grace(&live_ids(live)?).to_vec();
    for (id, e) in revoke_listed(client, live, expired).await? {
        eprintln!("warning: could not revoke instance token {id} past its grace ({e:#}); the next renewal tries again");
    }
    Ok(())
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
fn set_of(raw: &str) -> Result<InstanceValueInput> {
    let (target, value) = raw.split_once('=').with_context(|| format!("--set '{raw}' is not `<node>.<field>=<value>`"))?;
    let (step, field) = field_of(target)?;
    let value = serde_json::from_str::<Value>(value).unwrap_or_else(|_| Value::String(value.to_string()));
    Ok(InstanceValueInput { step, field, value })
}

pub async fn run(ctx: Ctx, instance: InstanceId, set: Vec<String>, clear: Vec<String>) -> Result<()> {
    let set: Vec<InstanceValueInput> = set.iter().map(|raw| set_of(raw)).collect::<Result<_>>()?;
    let clear: Vec<InstanceFieldRef> =
        clear.iter().map(|raw| field_of(raw).map(|(step, field)| InstanceFieldRef { step, field })).collect::<Result<_>>()?;
    let json = ctx.json();
    let client = ctx.client()?;
    let which = instance.clone();
    as_instance(&ctx, &client, &which, |door| async move {
        if !set.is_empty() || !clear.is_empty() {
            let changed: ValuesChanged =
                serde_json::from_value(door.put_json("/instance/values", &serde_json::to_value(ValuesRequest { set, clear })?).await?)
                    .context("read the dispatcher's answer to the change; upgrade the dispatcher or this CLI so the versions match")?;
            let rearmed = changed.rearmed;
            if json {
                println!("{}", serde_json::json!({ "instance": instance, "rearmed": rearmed }));
            } else {
                println!("Stored for instance '{instance}'.");
                if !rearmed.is_empty() {
                    println!("Its trigger(s) reading it were set up again: {}.", rearmed.join(", "));
                }
            }
            return Ok(());
        }
        let answer = door.get_json("/instance/fields").await?;
        let fields: Vec<InstanceField> = serde_json::from_value(answer.clone())
            .context("read the instance's fields; upgrade the dispatcher or this CLI so the versions match")?;
        if json {
            println!("{}", serde_json::json!({ "instance": instance, "fields": answer }));
            return Ok(());
        }
        if fields.is_empty() {
            println!("The program has no field an instance fills (none is `@instance_filled`).");
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

    /// A failed renewal retries soon; a good one waits the full period,
    /// and the retries fit many times inside a token's remaining life.
    #[test]
    fn renewal_waits_a_period_after_success_and_retries_soon_after_failure() {
        assert_eq!(next_wait(true), RENEW_EVERY);
        assert_eq!(next_wait(false), RETRY_EVERY);
        let life = std::time::Duration::from_secs(INSTANCE_TOKEN_SECS);
        assert!(2 * RENEW_EVERY < life, "the grace token must outlive its revoke");
        assert!((life - RENEW_EVERY).as_secs() / RETRY_EVERY.as_secs() >= 100);
    }

    /// A renewal keeps the bearer and the token it replaced, and revokes
    /// everything older.
    #[test]
    fn a_renewal_revokes_only_tokens_past_their_grace() {
        let ids: Vec<uuid::Uuid> = (0..4).map(|_| uuid::Uuid::new_v4()).collect();
        assert!(past_grace(&ids[..1]).is_empty());
        assert!(past_grace(&ids[..2]).is_empty());
        assert_eq!(past_grace(&ids), &ids[..2]);
    }

    #[test]
    fn a_field_splits_at_its_last_dot_and_a_value_reads_as_json_first() {
        assert_eq!(field_of("one.read.sheet").unwrap(), ("one.read".to_string(), "sheet".to_string()));
        assert!(field_of("read").is_err());
        assert_eq!(set_of("c.cron=0 0 3 * * *").unwrap().value, serde_json::json!("0 0 3 * * *"));
        assert_eq!(set_of("n.count=5").unwrap().value, serde_json::json!(5));
        assert!(set_of("n.count").is_err());
    }
}
