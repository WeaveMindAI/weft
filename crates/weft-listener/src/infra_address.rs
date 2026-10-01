//! Where the listener reaches an address a trigger was given.
//!
//! A trigger wired from an infra node (a bridge's event stream, a
//! database's port) is given the address the project's workers reach the
//! endpoint at. The listener is one of weft's own roles, and those may sit
//! on another network: on a local install the runtime runs on the
//! machine, the workers and the units in Docker's network, and the unit's
//! port reaches the machine on a loopback port of its own. So before
//! every connect the listener asks the broker for the same endpoint's
//! address as weft's roles reach it; an address that is no infra endpoint
//! comes back unchanged. Asked per connect, never cached: a local unit's
//! loopback port moves when the unit is made again.

use anyhow::{Context, Result};
use weft_broker_client::protocol::ListenerInfraAddressRequest;

use crate::kinds::SpawnCtx;

/// `url` (`scheme://host:port/...`, or a bare `host:port`, with or
/// without `user:pass@`), with its `host:port` as the listener reaches it.
pub async fn for_listener(url: &str, ctx: &SpawnCtx) -> Result<String> {
    let (before, authority, after) = split(url);
    let answer = ctx
        .events_broker
        .listener_infra_address(&ListenerInfraAddressRequest {
            signal_token: ctx.fire.token().to_string(),
            authority: authority.to_string(),
        })
        .await
        .with_context(|| format!("ask where the listener reaches {authority}"))?;
    Ok(format!("{before}{}{after}", answer.authority))
}

/// `(everything before host:port, host:port, everything after)`.
fn split(url: &str) -> (&str, &str, &str) {
    let start = url.find("://").map_or(0, |i| i + 3);
    let rest = &url[start..];
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let userinfo = rest[..end].rfind('@').map_or(0, |i| i + 1);
    let from = start + userinfo;
    let to = start + end;
    (&url[..from], &url[from..to], &url[to..])
}

#[cfg(test)]
mod tests {
    use super::split;

    #[test]
    fn the_host_and_port_are_cut_out_of_every_shape_of_address() {
        assert_eq!(split("http://wi-a:8080/events?x=1"), ("http://", "wi-a:8080", "/events?x=1"));
        assert_eq!(split("ws://u:p@wi-a:9/s"), ("ws://u:p@", "wi-a:9", "/s"));
        assert_eq!(split("wi-db:5432"), ("", "wi-db:5432", ""));
        assert_eq!(split("user:pw@wi-db:5432"), ("user:pw@", "wi-db:5432", ""));
    }
}
