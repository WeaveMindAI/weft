# Authoring a provider meter

A **meter** is the per-provider Rust impl that computes the REAL cost of a
paid API call from the bytes of the request and the response.

## Where a meter lives: weft, or your own project

A meter can live in either of two places, and it runs the same way in both
(the worker runs it; there is no proxy or central service that has to know
about it):

- **In weft**, one file under `crates/weft-providers/src/providers/`. This is
  a provider weft ships and reviews. It is the ONLY place a meter can be that
  lets the platform key pay for the provider (see "Platform key" below).
- **In your own project**, alongside the nodes that call the provider. In a
  package, that is a shared `.rs` file at the package root; in a bare node (a
  lone node with no package), it is the bottom of the node's own `mod.rs`.
  Either way it ends in `weft_providers::register_meter!(...)` (see the package
  layout in `authoring-nodes.md`). This lets a project support a provider weft
  does not ship yet: it works with a key you set yourself, right away, no
  waiting on anyone. The node and the meter connect only through the provider
  name string, so the node never imports the meter.

Either way, adding a provider is a file plus one line, nothing central to
update:

1. Write the file with your `ProviderMeter` impl and a `'static` meter value.
2. Register it at the bottom of that file: `weft_providers::register_meter!(MY_METER);`
   (inside weft's own crate, write `crate::register_meter!(MY_METER);`).
3. If the file is under `weft-providers/src/providers/`, also declare the
   module in `providers/mod.rs`: `pub mod <name>;`. A project's shared `.rs`
   file needs no such line; the package picks it up on its own.

The registry collects every `register_meter!` at link time (weft's meters and
your project's alike, since your project compiles into the same worker), so
there is no central array to edit and nothing to keep in sync. If you forget
the `register_meter!` line the provider is simply unsupported (a loud refusal
wherever a measured call is required, never a silent wrong number).

## Platform key vs your own key

The place a meter lives decides one thing: who can pay for the provider.

- **Your own key** (you set it on the node) works with ANY meter, wherever it
  lives. A project-defined provider is used exactly this way.
- **The platform key** (the deployment pays, the user sets no key) is only
  ever spent on a provider weft ships a meter for. A project-defined provider
  is refused the platform key, with a message pointing at how to request it.
  The reason is honesty: the platform can only bill for a spend it can
  measure with a meter it has reviewed. Getting a provider onto the platform
  key IS submitting its meter to weft to be reviewed and shipped, and a meter
  you already wrote in your project is most of that work.

Meters are the trusted artifact of the whole paid-call system. A node never
states a cost and has no way to: the runtime runs the provider's meter
around every call made on `ctx.metered_client`, and every cost figure in
the system is a meter's output. **The meter author must be careful so that
the node author can be careless**: a node cannot produce an incorrect bill,
no matter what it does, because it is never asked.

Adding a provider's meter here is also what makes it a **supported
provider**: a deployment-held key is only ever spent on a provider with a
meter (there would be no honest way to account for it otherwise). A
provider without a meter still works on a key the user sets themselves; its
calls simply carry no cost figure.

## The trait, verb by verb

```rust
#[async_trait::async_trait]
impl ProviderMeter for MyProviderMeter {
    fn provider(&self) -> &'static str;   // the key identity: <NAME>_API_KEY
    fn base_url(&self) -> &'static str;   // where the provider REALLY lives
    fn classify(&self, method: &str, path: &str) -> RouteClass;
    fn prepare(&self, path: &str, body: &[u8]) -> anyhow::Result<Option<Vec<u8>>>;
    async fn ceiling_usd(&self, path: &str, body: &[u8], http: &reqwest::Client)
        -> anyhow::Result<f64>;
    fn observe(&self) -> Box<dyn CallObservation>;
    async fn resolve(&self, observed: ObservedCall, follow_up: FollowUp<'_>)
        -> MeasuredCost;
}
```

- **`base_url`** is the single authority for where the provider lives. No
  caller ever accepts a host from a request instead; requests are rebuilt
  against this base. That is what makes it impossible to aim a
  deployment-held key at another host.
- **`classify`** maps `(method, relative path)` to a `RouteClass`. Matching
  is EXACT string matching on the raw path, and that is a security rule,
  not a style choice: an unknown route can be REFUSED by the caller's
  policy, so
  traversal (`../`), encoded traversal (`%2e%2e`), userinfo (`@host`) and
  backslash tricks must all fail to match and come back `Unknown`. Exact
  matching gives you that for free; never "normalize then match".
- **`prepare`** rewrites a Billable call's outgoing body so its cost becomes
  reportable at all (e.g. forcing the provider's usage-accounting opt-in,
  OVERRIDING whatever the caller set), and sheds anything internal that has
  no business going upstream (e.g. the media estimation metadata below). An
  unparseable body on a route that needs rewriting is a loud error: an
  unpreparable call would be an unmeasurable spend.
- **`ceiling_usd`** is a worst-case price for the call, computable BEFORE
  the call goes out. It must be computed ONLY from the request bytes (never
  from anything else the caller could hand over separately: a side channel
  for "here is my conversation, for estimation purposes" would let a caller
  understate what it is about to spend). Lean high; the measured actual is
  the figure that counts. A call that cannot be priced (unknown model, no
  output bound) is a loud error, never a guess.
- **`observe`** mints a fresh per-call tap for the response. The tap sees
  every byte AS IT FLOWS THROUGH to the real consumer: it must never
  buffer, delay, or reorder chunks, and it must stay O(small) in memory no
  matter how long the stream runs (see `sse::DataLineScanner` for the
  incremental SSE pattern).
- **`resolve`** turns the observation into dollars. If the provider
  reported the cost inline, this is pure. If the provider only answers
  out-of-band (OpenRouter's `/generation?id=...`), THE METER makes that
  follow-up query itself through `follow_up`; the node and its client
  library are never involved and never trusted to do it. A cost that
  genuinely cannot be resolved is an honest `amount_usd: None`, recorded
  as unknown, **never a fake $0** (a fake zero silently leaks money).

## Route classification, and the double-charge trap

**This is the one thing a meter author can get catastrophically wrong.**

A cost-lookup route LOOKS like a call but must cost nothing. Work through
OpenRouter's routes:

| Route | Class | Why |
| --- | --- | --- |
| `POST chat/completions` | `Billable(Metered)` | the actual spend; only measurement prices it |
| `GET generation` | `Free` | the cost LOOKUP for a spend |
| `GET models` | `Free` | the public price catalog |
| anything else | `Unknown` | cannot be measured, so cannot be billed |

A `Billable` route also declares HOW it prices, which doubles as the
policy for a cost that genuinely cannot be resolved:

- **`Pricing::Fixed { usd }`** (one search = one credit): the price is
  known without measurement. If the call confirmably went out but the
  figure could not be resolved, the fixed price stands: exact, not a
  guess.
- **`Pricing::Metered`** (LLM tokens): there is no honest number without
  measurement. An unresolvable metered cost is recorded as unknown,
  never guessed.

If `GET generation` were classified `Billable`, then a node re-querying its
own cost would be billed a second time, and the meter's OWN follow-up query
would be billed too, recursively. Classifying it `Free` kills the
double-charge structurally: a node that re-queries its cost by hand is
harmless (it gets its number, billed $0), and the meter's follow-up is
harmless for the same reason. **No "internal call" flag is needed**; the
route table already answers it.

`Unknown` is not a value judgement about the route; it means "this meter
cannot measure it". Whether an unknown route is refused or passed through
unmeasured is the caller's policy, not the meter's.

## Media estimation metadata

A request's media parts may carry estimation metadata the node's client
library kept on the wire (`duration_secs` for audio/video, `width`/`height`
for images/video; minillmlib emits these on wires that tolerate unknown
keys). `ceiling_usd` should use it: a declared 90-second clip prices like
a 90-second clip, not like a default guess. Two rules:

- It only ever SHARPENS the ceiling. Lying in it (or omitting it) changes
  that pre-call estimate, never the cost figure: the figure is always the
  measured actual, so the metadata is not a trust surface.
- `prepare` sheds it before the bytes go upstream. Internal breadcrumbs do
  not ride to the provider, even where the provider would tolerate them.

## Tests a meter must ship

Look at `providers/openrouter.rs` for the shapes; every meter needs its own:

- **Route classification** (Layer 1): every route in the table, plus the
  trick paths (`../`, `%2e%2e`, `@host`, backslash, trailing slash, case
  changes) all classifying `Unknown`.
- **The double-charge pin** (Layer 1): the cost-lookup route is `Free`.
- **`prepare`** (Layer 1): the accounting opt-in is forced even when the
  caller opted out; estimation metadata is shed; garbage bodies error loud.
- **Observation + resolve against RECORDED REAL responses** (Layer 2): a
  meter is a function of bytes, so record a real non-streaming response, a
  real streaming response (fed in awkward chunk splits, to prove
  reassembly), a refused call, and assert the exact dollars. This is how a
  meter is proved correct against the provider's actual billing, and it is
  the main defence against an under-counting meter, which would leak money
  on every call it prices low.
- **Interruption honesty** (Layer 2): an interrupted observation with
  nothing to anchor a lookup on resolves to `None`, never `Some(0.0)`.

## Write it as a pure function of bytes

A meter must not assume anything about the process running it. Write it as
a pure function of the request/response bytes plus its own follow-up query,
and it measures correctly wherever a paid call is measured: no globals
beyond your own rate caches, no environment reads beyond what `FollowUp`
hands you.
