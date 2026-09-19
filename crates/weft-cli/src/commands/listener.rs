use serde::Deserialize;

use super::Ctx;

/// One row of `GET /listener/inspect`: a live listener pod, the signal
/// rows the dispatcher has placed on it, what the pod itself holds in
/// RAM (or why it could not be asked), and where the two disagree.
/// SYNC: InspectRow <-> crates/weft-dispatcher/src/api/signal.rs listener_inspect
#[derive(Deserialize)]
struct InspectRow {
    pod_name: String,
    listener_url: String,
    /// Rows of activating or active projects: every one is expected in
    /// the registry.
    placed_signals: Vec<PlacedSignal>,
    /// Rows of hibernated or parked projects: kept so reactivate can
    /// rehydrate them, and absent from the registry on purpose.
    preserved_signals: Vec<PlacedSignal>,
    /// Decoded by [`registry`] rather than as an enum: an untagged
    /// enum reports a bad entry as "matched no variant" and hides which
    /// field broke.
    listener_registry: serde_json::Value,
    /// The dispatcher's comparison of the two sides; absent exactly
    /// when the registry could not be asked.
    drift: Option<Drift>,
}

/// One placed `signal` row.
#[derive(Deserialize)]
struct PlacedSignal {
    token: String,
    node_id: String,
    project_id: String,
}

/// Where the placement table and the pod disagree, each group naming
/// the signals that make it up.
#[derive(Deserialize)]
struct Drift {
    on_pod_not_placed: Vec<HeldSignal>,
    placed_not_on_pod: Vec<PlacedSignal>,
    preserved_on_pod: Vec<PlacedSignal>,
}

impl Drift {
    fn is_empty(&self) -> bool {
        self.on_pod_not_placed.is_empty() && self.placed_not_on_pod.is_empty() && self.preserved_on_pod.is_empty()
    }
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
        println!("  placed signals:    {}", row.placed_signals.len());
        if !row.preserved_signals.is_empty() {
            println!(
                "  preserved signals: {} (hibernated or parked projects; kept for reactivate, not on the pod)",
                row.preserved_signals.len()
            );
        }
        match registry {
            Registry::Held(held) => {
                println!("  listener signals:  {}", held.len());
                for sig in &held {
                    println!("    - token={}  node={}  kind={}", sig.token, spelled(&sig.node_id), sig.kind);
                }
                if held.is_empty() && row.placed_signals.is_empty() && row.preserved_signals.is_empty() {
                    println!("  (idle: should be reaped on next cleanup tick)");
                }
                let drift = row.drift.as_ref().ok_or_else(|| {
                    anyhow::anyhow!("/listener/inspect handed a registry for {} but no drift", row.pod_name)
                })?;
                print_drift(drift);
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

/// A signal's node as a person reads it. Nothing here holds the
/// project the signal belongs to (one pod carries many), so the
/// compiler's id is read the way it reads without one: a node inside an
/// included file shows as the file's own name and the rest
/// (`sweep.key`), never as the path the runtime keys it by.
fn spelled(node_id: &str) -> String {
    weft_core::project::plain_id(node_id)
}

/// The three ways the table and the pod disagree, each listing only
/// the signals in it, so the reader chases a token and never a count.
fn print_drift(drift: &Drift) {
    if drift.is_empty() {
        return;
    }
    println!("  ⚠ DRIFT: the placement table and the pod's registry disagree.");
    if !drift.on_pod_not_placed.is_empty() {
        println!("    on the pod with no row behind it (the table forgot it; the pod still serves it):");
        for sig in &drift.on_pod_not_placed {
            println!("      - token={}  node={}  kind={}", sig.token, spelled(&sig.node_id), sig.kind);
        }
    }
    if !drift.placed_not_on_pod.is_empty() {
        println!("    placed for an active project but missing from the pod (its trigger cannot fire):");
        for sig in &drift.placed_not_on_pod {
            println!("      - token={}  node={}  project={}", sig.token, spelled(&sig.node_id), sig.project_id);
        }
    }
    if !drift.preserved_on_pod.is_empty() {
        println!("    preserved for a hibernated or parked project yet still on the pod (deactivate told it to forget):");
        for sig in &drift.preserved_on_pod {
            println!("      - token={}  node={}  project={}", sig.token, spelled(&sig.node_id), sig.project_id);
        }
    }
}
