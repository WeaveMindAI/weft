use serde::Deserialize;

use super::Ctx;

/// One row of `GET /listener/inspect`: a live listener pod, how many
/// signal rows the dispatcher has placed on it, and what the pod
/// itself holds in RAM (or why it could not be asked).
#[derive(Deserialize)]
struct InspectRow {
    pod_name: String,
    listener_url: String,
    placed_signal_count: u64,
    /// Decoded by [`registry`] rather than as an enum: an untagged
    /// enum reports a bad entry as "matched no variant" and hides which
    /// field broke.
    listener_registry: serde_json::Value,
}

/// What the dispatcher got when it asked the pod for its registry:
/// the signals it holds, or one of the three ways the ask failed.
enum Registry {
    Held(Vec<HeldSignal>),
    /// The pod answered and the answer was not a registry.
    DecodeError(String),
    /// The pod answered with an error status.
    HttpError(u16),
    /// The pod could not be reached at all.
    NetworkError(String),
}

/// The dispatcher sends the pod's list, or an object with exactly one
/// of three error keys; anything else is a contract break named here.
fn registry(value: serde_json::Value) -> anyhow::Result<Registry> {
    if value.is_array() {
        let held = serde_json::from_value(value)
            .map_err(|e| anyhow::anyhow!("unexpected registry entry shape: {e}"))?;
        return Ok(Registry::Held(held));
    }
    let Some(object) = value.as_object() else {
        anyhow::bail!("unexpected listener_registry shape: {value}");
    };
    let (key, inner) = match object.iter().collect::<Vec<_>>().as_slice() {
        [(key, inner)] => (key.as_str(), *inner),
        _ => anyhow::bail!("unexpected listener_registry shape: {value}"),
    };
    let text = || {
        inner
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| anyhow::anyhow!("listener_registry.{key} is not a string: {inner}"))
    };
    Ok(match key {
        "decode_error" => Registry::DecodeError(text()?),
        "network_error" => Registry::NetworkError(text()?),
        "http_error" => Registry::HttpError(
            inner
                .as_u64()
                .and_then(|n| u16::try_from(n).ok())
                .ok_or_else(|| anyhow::anyhow!("listener_registry.http_error is not a status: {inner}"))?,
        ),
        _ => anyhow::bail!("unexpected listener_registry shape: {value}"),
    })
}

/// One entry of a pod's registry, as its `/signals` lists it.
#[derive(Deserialize)]
struct HeldSignal {
    token: String,
    node_id: String,
    kind: serde_json::Value,
}

pub async fn inspect(ctx: Ctx) -> anyhow::Result<()> {
    let client = ctx.client();
    let raw: serde_json::Value = client.get_json("/listener/inspect").await?;
    // Decode strictly in both modes: a renamed field must fail here,
    // never print a zero that reads as a healthy count, and never
    // reach a script as a row missing the field it was promised.
    let rows: Vec<InspectRow> = serde_json::from_value(raw.clone())
        .map_err(|e| anyhow::anyhow!("unexpected /listener/inspect row shape: {e}"))?;
    let rows: Vec<(InspectRow, Registry)> = rows
        .into_iter()
        .map(|row| registry(row.listener_registry.clone()).map(|r| (row, r)))
        .collect::<anyhow::Result<_>>()?;
    if ctx.json_out(&raw)? {
        return Ok(());
    }
    if rows.is_empty() {
        println!("no active listeners");
        return Ok(());
    }
    for (row, registry) in rows {
        println!("pod: {}", row.pod_name);
        println!("  url:               {}", row.listener_url);
        println!("  placed signals:    {}", row.placed_signal_count);
        match registry {
            Registry::Held(held) => {
                println!("  listener signals:  {}", held.len());
                if row.placed_signal_count != held.len() as u64 {
                    println!("  ⚠ DRIFT: the placement table and the pod's registry disagree.");
                }
                if held.is_empty() && row.placed_signal_count == 0 {
                    println!("  (idle: should be reaped on next cleanup tick)");
                }
                for sig in held {
                    println!("    - token={}  node={}  kind={}", sig.token, sig.node_id, sig.kind);
                }
            }
            Registry::DecodeError(e) => {
                println!("  listener signals:  (the pod answered, but not with a registry: {e})")
            }
            Registry::HttpError(status) => {
                println!("  listener signals:  (the pod answered HTTP {status})")
            }
            Registry::NetworkError(e) => println!("  listener signals:  (unreachable: {e})"),
        }
    }
    Ok(())
}

