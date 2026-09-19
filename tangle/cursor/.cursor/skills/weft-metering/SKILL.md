---
name: weft-metering
description: "Read when the user asks what a run costs, or when a node is wired to a paid API weft does not already price: the three pieces of a custom paid service, a meter to copy, and the rules that keep a number honest."
---

# What a call costs

A node never computes what its API call cost. Every cost figure in weft comes from a [meter]: separate code that watches the request go out and the answer come back, and works out the real number. The number lands on [the trail], the durable record of what an execution spent, read with `weft events <color> --kind cost_reported`.

## Two kinds of meter, and only one of them is yours

When a user wires a node to a paid API of their own and wants to know what a run costs, you write the [meter]. It lives in their project, next to the nodes that call the API, and measures spend on the user's own key, so a wrong number costs them their own money. It is allowed to be rough: it answers "roughly what did that run cost me".

Weft also ships meters for the providers it supports. Those will run on the key weft is building now, the one weft holds itself and bills against a user's balance, so they have more rules to meet, and you do not write them. When a user wants a provider shipped in weft itself, you tell them they can write the meter and send a PR, or you offer to open a request for it.

## A custom paid service is three pieces

All three carry one string, the [service name], and a [meter] whose [service name] differs from the node's is never found.

**1. The node's `metadata.json` declares the service**: its name, how the user hands over a key, and how that key rides on the request.

```json
{
  "type": "AskAcme",
  "label": "Ask Acme",
  "description": "One generation call to Acme's API, on your own key.",
  "inputs": [
    { "name": "prompt", "type": "String", "required": true },
    {
      "name": "connection",
      "type": "Access",
      "widget": { "kind": "access" },
      "label": "Connection"
    }
  ],
  "outputs": [{ "name": "answer", "type": "String" }],
  "service": {
    "service": "acme",
    "label": "Acme",
    "acquisition": {
      "kind": "static",
      "fields": [{ "name": "key", "label": "API key", "placeholder": "acme-..." }]
    },
    "auth": [{ "kind": "header", "name": "Authorization", "value": "Bearer {key}" }]
  }
}
```

The `access` widget gives the user a "Connect Acme..." field in the graph. A project-defined service has no shared door, so the user always brings their own key.

**2. The node opens the connection and calls on its client.** Every call on that client goes through the [meter]; a call on any other client is not measured.

```rust
use weft::{NodeErrExt, WeftResult};

// ...inside `run`:
let prompt: String = ctx.inputs.get("prompt")?;
let account = ctx.inputs.get("connection")?;
let conn = ctx.open(&account).await?;
let answer: serde_json::Value = conn
    .client()
    .post("https://api.acme.example/v1/generate")
    .json(&serde_json::json!({ "prompt": prompt }))
    .send()
    .await
    .node_err("acme: generate")?
    .json()
    .await
    .node_err("acme: read the answer")?;
```

**3. The [meter] prices it.** In a package it goes in a shared `.rs` file at the package root; in a bare node, at the bottom of its own `mod.rs`. A shared file is compiled whether or not a node imports it, so `register_meter!` runs either way.

## A meter you can copy

This one charges a cent a call. It is complete: every method the trait has no default for.

```rust
use async_trait::async_trait;
use weft_providers::providers::JsonBodyObservation;
use weft_providers::{
    CallObservation, FollowUp, MeasuredCost, ObservedCall, Pricing, ProviderMeter, RouteClass,
};

struct AcmeMeter;
static ACME: AcmeMeter = AcmeMeter;

#[async_trait]
impl ProviderMeter for AcmeMeter {
    /// Must equal the `service` string in metadata.json.
    fn service(&self) -> &'static str {
        "acme"
    }

    /// Where the provider lives. Every path below is relative to this,
    /// and a call aimed anywhere else is not this service's call.
    fn base_url(&self) -> &'static str {
        "https://api.acme.example/v1"
    }

    /// Exact match on the raw path. Anything unlisted is `Unknown`.
    fn classify(&self, method: &str, path: &str) -> RouteClass {
        match (method, path) {
            ("POST", "generate") => RouteClass::Billable(Pricing::Fixed { usd: 0.01 }),
            ("GET", "usage") => RouteClass::Free,
            _ => RouteClass::Unknown,
        }
    }

    /// Buffer the JSON answer so `resolve` can read it.
    fn observe(&self, _path: &str, _query: &str, _body: &[u8]) -> Box<dyn CallObservation> {
        Box::new(JsonBodyObservation::default())
    }

    /// Turn the answer into dollars. A fixed price still reads the
    /// response, because a provider can answer 200 on something it never
    /// billed, and that call cost zero.
    async fn resolve(
        &self,
        _path: &str,
        observed: ObservedCall,
        _follow_up: FollowUp<'_>,
    ) -> MeasuredCost {
        // Three outcomes, never two: `cost_from_status` answers for every
        // non-2xx, and it is the one place the rule lives.
        if let Some(cost) = weft_providers::providers::cost_from_status(observed.status, "the call") {
            return cost;
        }
        MeasuredCost {
            amount_usd: Some(0.01),
            model: None,
            metadata: serde_json::json!({ "status": observed.status }),
        }
    }
}

weft_providers::register_meter!(ACME);
```

`weft`, `weft-providers`, `async-trait`, `anyhow`, `serde` and `serde_json` are already on every node's path. Only the header observation below needs a line, because it names the `http` crate in its own signature.

```toml
http = "1"
```

In a bare node that goes in `deps.toml`; at a package root, in the package's `package.toml` under `[dependencies]`, because the whole package compiles as one crate.

Without `register_meter!` the calls still go out on the user's own key and land on [the trail] with no cost on them.

## When the price is in the answer

You swap `Fixed` for `Metered` and read the provider's own figure out of the observation.

```rust
("POST", "generate") => RouteClass::Billable(Pricing::Metered),
```

```rust
async fn resolve(
    &self,
    _path: &str,
    observed: ObservedCall,
    _follow_up: FollowUp<'_>,
) -> MeasuredCost {
    // Whatever the provider reports. `observed.data` is the parsed body.
    let amount_usd = observed.data["usage"]["cost_usd"].as_f64();
    MeasuredCost {
        amount_usd,
        model: observed.data["model"].as_str().map(str::to_string),
        metadata: observed.data,
    }
}
```

If the figure rides in a header, you declare it. You never write your own observation: the shared one buffers the body under a size cap, and a hand-rolled tap is how that cap gets forgotten.

```rust
fn observe(&self, _path: &str, _query: &str, _body: &[u8]) -> Box<dyn CallObservation> {
    Box::new(JsonBodyObservation::new().header_f64("x-acme-cost-usd", "costUsd"))
}

async fn resolve(
    &self,
    _path: &str,
    observed: ObservedCall,
    _follow_up: FollowUp<'_>,
) -> MeasuredCost {
    MeasuredCost {
        amount_usd: observed.data["costUsd"].as_f64(),
        model: None,
        metadata: observed.data,
    }
}
```

`header_f64` observes null for a header that is missing, unparseable, negative or not a number, so a missing header can never read as a zero. The other declarations: `seed` carries facts you already know into the observation, `field` lifts one value out of the body by JSON pointer, and `note_outcome` adds whether the answer was a 2xx and whether the stream was cut.

## Rules that hold for every meter

**You read what the provider charged, never what the request asked for.** They differ, and not by rounding: one image provider we metered bills a whole megapixel for a request asking for a quarter of one, because it bills in units of its own. The number is the one the provider reports, in the body, in a header, or behind a lookup the [meter] makes with `follow_up`; you dump the headers of a real response before you conclude there is none. If you catch yourself pricing from the request, stop and write: "Wait. What did the provider say it charged?" Then find that figure, or record the spend as unknown.

**An unknown is recorded as unknown, never as zero.** `amount_usd: None` says nobody knows. `Some(0.0)` says the call was free, which is a different claim.

**A 5xx is not a refusal.** A status in the 500s can mean the provider did the work, billed for it, and then something in front of it fell over. Three outcomes, and `weft_providers::providers::cost_from_status` is the one place they live: a 2xx is yours to price, a 4xx spent nothing, anything in the 500s is unknown.

**Every parameter that changes the price is part of the price.** When the request can ask for something dearer (a stealth proxy, structured extraction, a higher resolution, more images), you read it and use it. "Our node does not send that" does not justify a flat rate: the [meter] sits on the connection, and any program with that connection can send it. When the interface cannot carry what the price depends on, you extend the interface; when you know the flat figure is wrong but not the right one, you record the spend as unknown and say which option made it so. A wrong number on [the trail] is worse than an honest gap.

**The cost-lookup routes are `Free`.** A billable one would charge a node for checking its own spend, and charge the [meter]'s own lookup in its turn, recursively.

**Every route the node actually calls is classified.** A route the [meter] does not classify comes back `Unknown` and goes out unmeasured on the user's own key, which on [the trail] looks exactly like a call that cost nothing.

**You know which routes you left unpriced.** A key opens on all of a provider's routes. On the user's own key a partial [meter] is a fair trade when you tell the user which calls are not counted. Weft will not ship one.

## When the amount arrives after the call

A queued API takes the money on the submit and says how much on a later call: the cost exists only once the job has run. The [meter] never polls for it. A submit opens a [charge], held under the [service name] plus the provider's id until a later report closes it. These methods go on a [meter] that otherwise looks like the one above: `service`, `base_url` and `observe` are unchanged, and `resolve` now only ever sees a submit the provider refused.

```rust
fn classify(&self, method: &str, path: &str) -> RouteClass {
    match (method, path) {
        ("POST", "jobs") => RouteClass::Billable(Pricing::Metered),
        ("GET", p) if p.starts_with("jobs/") => RouteClass::Reports,
        _ => RouteClass::Unknown,
    }
}

/// The id this spend will be reported under, read off the submit's own
/// answer. No I/O here: the charge has to be open before the node's next
/// request can report on it.
///
/// It has to be an id the PROVIDER assigned and you read out of the
/// response. Never one you made up, and never one taken from the
/// request. A worker holds the charges of every run it is driving under
/// the service name plus this id, and a later report arrives carrying
/// nothing but its own response, so two runs whose ids the meter chose
/// rather than read will collide and one run's figure lands on the
/// other's spend.
fn opens_charge(&self, path: &str, observed: &ObservedCall) -> Option<String> {
    if path != "jobs" || !(200..300).contains(&observed.status) {
        return None;
    }
    observed.data["id"].as_str().map(str::to_string)
}

/// Which open charge this later response speaks for.
/// The id is the FIRST segment and nothing more: `jobs/{id}`,
/// `jobs/{id}/status` and `jobs/{id}/errors` all report on one job, and
/// carrying the rest of the path into the id names a charge nothing is
/// held under, so the job never closes and its spend books as unknown
/// while its own answer was stating the figure.
fn charge_reported_on(&self, path: &str, _observed: &ObservedCall) -> Option<String> {
    path.strip_prefix("jobs/")?.split('/').next().filter(|s| !s.is_empty()).map(str::to_string)
}

/// Read the figure. `scratch` starts as the submit's own observation data
/// and survives from one report to the next, so a provider that answers
/// across two responses can stash the first half there. `None` leaves the
/// charge open for the next report.
async fn fold_report(
    &self,
    _path: &str,
    observed: ObservedCall,
    scratch: &mut serde_json::Value,
    _follow_up: FollowUp<'_>,
) -> Option<MeasuredCost> {
    if observed.data["status"].as_str()? != "done" {
        return None;
    }
    Some(MeasuredCost {
        amount_usd: observed.data["cost_usd"].as_f64(),
        model: None,
        metadata: scratch.clone(),
    })
}

/// A call that opened a charge never reaches `resolve`, so this only ever
/// sees a submit that came back with no job id.
async fn resolve(
    &self,
    _path: &str,
    observed: ObservedCall,
    _follow_up: FollowUp<'_>,
) -> MeasuredCost {
    // A refusal spends nothing. A 5xx may have queued the job anyway,
    // which is then running and spending with no id for anyone to report
    // on, so that one is unknown.
    weft_providers::providers::cost_from_status(observed.status, "the submit").unwrap_or(MeasuredCost {
        amount_usd: None,
        model: None,
        metadata: serde_json::json!({
            "resolution": "the submit answered without a job id, so nothing can report what it spent",
        }),
    })
}
```

The worker sees the node's own polls, because they ride the same connection client. A job submitted and never read back is still recorded: money went out with no figure on it.

## Refuse what you could never price

A call whose price exists nowhere can never be measured however it turns out; fal's case is a model its pricing catalog lists no price for. The [meter] finds that out by asking the catalog, which is free and cached.

```rust
async fn priceable(&self, path: &str, follow_up: FollowUp<'_>) -> anyhow::Result<()> {
    let model = path.trim_start_matches("models/");
    let catalog: serde_json::Value = follow_up
        .http
        .get(format!("{}/pricing", follow_up.base_url))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    if catalog[model]["unit_price"].as_f64().is_none() {
        anyhow::bail!(
            "acme lists no price for '{model}', so a call on it could never be \
             priced; pick a model the catalog prices"
        );
    }
    Ok(())
}
```

`priceable` runs before a billable call goes out, so an `Err` here is the last point at which the money stays unspent. Unlike the methods below, it admits by default: a [meter] that says nothing here lets everything through.

## Checking it worked

You run the program, then read [the trail]:

```text
weft events <color> --kind cost_reported
```

Each measured call prints a `cost_reported` line carrying `service=` and `amount_usd=`, plus whatever metadata your `resolve` returned. No line at all means the route classified `Unknown`, or `register_meter!` never ran.

## The rest of the trait

`ceiling_usd` bounds the worst case before a call runs, and the session methods (`observe_session`, `session_slice_usd`, `session_max_frame_bytes`) price a long-lived socket that accrues as it goes. All refuse loudly by default, which is correct on the user's own key: you leave them alone unless the user asks for a spend cap or wires up a realtime socket. For those, the full trait, and what a [meter] must do before weft ships it, go and read [Measuring what a call costs](https://weavemindai.github.io/weft/connections/meters.html).

Every snippet on this page is compiled in weft's own `crates/weft-providers/tests/skill_example_meter.rs`, and all but `priceable` are exercised there too. If you change one here, you change it there.
