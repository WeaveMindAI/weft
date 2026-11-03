//! Pulling a WhatsApp message's media out of the project's bridge.
//!
//! The bridge serves a message's bytes at `/media/<messageId>` and the
//! account they came through at `/outputs`; this is the one place that
//! turns the two into a stored file, so a WhatsApp file is stored the
//! same way whichever node fetched it.

use serde_json::Value;

use weft::access::client;
use weft::storage::StorageScope;
use weft::{EndpointHandle, EndpointMethod, RunContext, WeftResult};

/// Pull a message's media bytes out of the bridge (`/media/<messageId>`)
/// into PROJECT storage, under the message's identity on its ACCOUNT
/// (message ids are per account, a project can run two, and the
/// account's jid is what stays the same when the bridge is torn down
/// and provisioned again under a new address), and hand
/// back the stored-file value (the marker a `file` port carries;
/// `StoredFile::from_value` reads its meta back). The one media path
/// both the live receive and the history fetch use, so a WhatsApp file
/// is stored the same way whichever door it came in by.
///
/// Project scope and an identity together make the fetch idempotent:
/// the same message asked for by three runs is one download and one
/// file, and it outlives the run that first pulled it (the bridge's own
/// copy ages out, so the bytes could not be re-fetched later anyway).
/// The language capability does the GET, derives the mime, streams in
/// bounded memory, and takes the name the bridge puts in its
/// `Content-Disposition` (WhatsApp's own filename, extension and all).
pub async fn fetch_media(
    ctx: &RunContext,
    bridge: &EndpointHandle,
    message_id: &str,
) -> WeftResult<Value> {
    let jid = account_jid(bridge).await?;
    let url = client::url(bridge.url(), &["media", message_id])?;
    ctx.storage(StorageScope::Project)
        .identified(format!("whatsapp:{jid}:{message_id}"))
        .put_from_url(url.as_str(), None, None, None)
        .await
}

/// The WhatsApp account behind a bridge (`/outputs`' `jid`), the one
/// name for it that survives a redeploy. A bridge not yet paired has
/// none, and no message can have come through it either.
async fn account_jid(bridge: &EndpointHandle) -> WeftResult<String> {
    let outputs = bridge.call(EndpointMethod::Get, "/outputs", None).await?;
    match outputs["jid"].as_str() {
        Some(jid) if !jid.is_empty() => Ok(jid.to_string()),
        _ => weft::node_bail!("the bridge behind {} is not paired with a WhatsApp account", bridge.infra_handle()),
    }
}
