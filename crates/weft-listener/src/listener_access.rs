//! Resolving a signal's connection (`SignalSpec.access`) into an HTTP
//! client whose requests are signed in: the shared plumbing that makes
//! authed polls, authed streams, and minted sockets exist for EVERY
//! kind, resolved through the broker at each use so a credential is
//! never frozen into a loop.

use anyhow::{Context, Result};
use weft_broker_client::protocol::{ListenerResolveRequest, ListenerResolvedSource};
use weft_core::access::client::{authed_client, plain_client, resolve_steps};
use weft_core::primitive::AccessRef;
use weft_core::reqwest_middleware;

use crate::kinds::SpawnCtx;

/// Resolve the signal's connection freshly through the broker. The
/// tenant is the signal's (it traveled with the registration, the
/// same trust the listener's fires already ride on).
pub async fn resolve(
    access: &AccessRef,
    ctx: &SpawnCtx,
) -> Result<ListenerResolvedSource> {
    ctx.events_broker
        .listener_resolve(&ListenerResolveRequest {
            tenant: ctx.fire.tenant_id().to_string(),
            access_id: access.id.clone(),
            service: access.service.clone(),
            required_values: access.required_values.clone(),
        })
        .await
        .with_context(|| format!("resolving the '{}' connection", access.service))
}

/// The client one outbound use of this signal makes its calls with:
/// signed in through the signal's connection when one is set, plain
/// otherwise. Resolves per call site so refresh happens store-side.
pub async fn client_for(
    access: &Option<AccessRef>,
    ctx: &SpawnCtx,
) -> Result<reqwest_middleware::ClientWithMiddleware> {
    let Some(access) = access else { return Ok(plain_client()) };
    let source = resolve(access, ctx).await?;
    let steps = resolve_steps(&source.auth, &source.values)
        .map_err(|e| anyhow::anyhow!("applying the connection's sign-in: {e}"))?;
    Ok(authed_client(steps))
}
