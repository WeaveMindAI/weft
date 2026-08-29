# Measuring what a call costs

A **meter** is the per-provider Rust that computes the real cost of a paid API
call from the bytes of the request and the response.

The runtime runs the provider's meter around every call made on an opened
connection's client, so every cost figure in the system is a meter's output. A
node never states a cost and cannot reach that path.

## Where a meter lives

A meter can live in two places and runs the same way in both. The worker runs
it, so nothing central has to know it exists.

**In weft**, one file under `crates/weft-providers/src/providers/`. This is a
provider weft ships and reviews.

**In your own project**, beside the nodes that call the provider: a shared
`.rs` file at a package root, or the bottom of a bare node's `mod.rs`. That is
how a project supports a provider weft does not ship yet, with a key you set
yourself.

Either way, adding a provider is a file plus one line:

```rust
weft_providers::register_meter!(MY_METER);
// inside weft's own crate: crate::register_meter!(MY_METER);
```

The registry collects every registration at link time, weft's meters and your
project's alike, since your project compiles into the same worker. Forget the
line and the provider is simply unsupported: a loud refusal wherever a measured
call is required, never a silent wrong number.

The node and the meter connect only through the provider name string, so the
node never imports the meter.

## Who can pay

The place a meter lives decides exactly one thing.

**Your own connection**, your key or your signed-in account, works with **any**
meter wherever it lives.

**The platform key**, where app.weavemind.ai pays and the user sets no key, is
only ever spent on a provider weft ships a meter for, because it can only bill
for spend it can measure with a meter it has reviewed. Getting a provider onto
that key means getting its meter shipped in weft, and what that takes is at the
[end of this page](#getting-a-meter-shipped-in-weft).

## The trait

```rust
#[async_trait::async_trait]
impl ProviderMeter for MyProviderMeter {
    fn service(&self) -> &'static str;
    fn base_url(&self) -> &'static str;
    fn classify(&self, method: &str, path: &str) -> RouteClass;
    fn prepare(&self, path: &str, body: &[u8]) -> anyhow::Result<Option<Vec<u8>>>;
    async fn ceiling_usd(&self, path: &str, body: &[u8], follow_up: FollowUp<'_>)
        -> anyhow::Result<f64>;
    fn observe(&self, path: &str, query: &str, request_body: &[u8])
        -> Box<dyn CallObservation>;
    async fn resolve(&self, path: &str, observed: ObservedCall, follow_up: FollowUp<'_>)
        -> MeasuredCost;
    fn observe_session(&self, path: &str, query: &str)
        -> anyhow::Result<Box<dyn SessionObservation>>;
    fn session_slice_usd(&self, path: &str) -> anyhow::Result<f64>;
    fn session_max_frame_bytes(&self, path: &str) -> anyhow::Result<usize>;
}
```

**`base_url`** is the single authority for where the provider lives. No caller
ever accepts a host from a request instead; requests are rebuilt against this
base, so a request cannot be aimed at a host the meter did not name.

**`classify`** maps a method and relative path to a class, matching against the
**raw path** and never a normalized one. An unknown route can be refused by the
caller's policy, so traversal (`../`), encoded traversal (`%2e%2e`), userinfo
(`@host`), and backslash tricks all have to fail to match and come back
`Unknown`. Matching raw is what gives you that. If a route has a parameterized
segment, match its prefix and guard the segment's character set, the way
`elevenlabs.rs` does for `text-to-speech/{voice}`; normalizing first would take
the protection away.

**`prepare`** rewrites a billable call's outgoing body so its cost becomes
reportable at all, for example forcing the provider's usage-accounting opt-in,
overriding whatever the caller set. It also sheds anything internal with no
business going upstream. An unparseable body on a route needing a rewrite is a
loud error, because an unpreparable call would be an unmeasurable spend.

**`ceiling_usd`** is a worst-case price computable **before** the call goes out.
It must be computed **only** from the request bytes, never from anything the
caller could hand over separately: a side channel for "here is my conversation,
for estimation purposes" would let a caller understate what it is about to
spend. Lean high; the measured actual is the figure that counts. A call that
cannot be priced is a loud error, never a guess. When the rates live behind the
provider's own authenticated API, `follow_up` is how the meter asks for them: a
signed request the meter makes on its own, outside the call it is pricing.

You only need this if you want the provider on the shared keys. On the user's
own credential nothing has to be bounded before the call, so the default, which
refuses, is the right one to leave in place. `session_slice_usd` works the same
way.

**`observe`** mints a fresh observer for one call. It is handed the query
string and the request body as well as the path, because some routes are priced
from what was sent rather than what came back: a text-to-speech call prices its
text's characters, and an output format in the query decides bytes per second.
The observer then sees every byte **as it flows through** to the real consumer,
so it must never buffer, delay, or reorder chunks, and it must stay small in
memory however long the stream runs.

**`resolve`** turns the observation into dollars. If the provider reported the
cost inline, this is pure. If the provider only answers out of band, **the
meter** makes that follow-up query itself; the node and its client library are
never involved and never trusted to do it. A cost that genuinely cannot be
resolved is an honest `None`, recorded as unknown, **never a fake zero**,
because a zero would read as a call that cost nothing.

**`observe_session`** mints the per-session tap for a route classified as a
session. It is fed every frame in both directions, answers the running accrued
cost, and closes into a measured cost. Required for any meter classifying a
route as a session; the default refuses loudly.

**`session_slice_usd`** is the session's version of a ceiling. A session has no
knowable total before it runs, so instead of reserving the whole thing the
runtime reserves a slice at a time (price the dearest configuration for a fixed
span, say one minute) and reserves another as the accrued cost catches up. It
is only read where a call has to be paid for before it is allowed to start.

**`session_max_frame_bytes`** bounds one frame, sized so a single frame can
never accrue more than one slice's worth at the route's dearest rate. Account
for the wire form, such as base64 expansion.

## Route classification, and the double-charge trap

A cost-lookup route looks like a call and must cost nothing.

| Route | Class | Why |
|---|---|---|
| `POST chat/completions` | `Billable(Metered)` | the actual spend |
| `GET generation` | `Free` | the cost **lookup** for a spend |
| `GET models` | `Free` | the public price catalog |
| `GET speech-to-text/realtime` | `BillableSession` | a long-lived two-way channel; no total knowable up front |
| anything else | `Unknown` | cannot be measured, so cannot be billed |

If the cost-lookup route were billable, a node re-querying its own cost would
be billed a second time, and the meter's **own** follow-up query would be
billed too, recursively. Classifying it `Free` makes that impossible, and
**no "internal call" flag is needed**, because the route table already answers
it.

A billable route also declares **how** it prices, which doubles as the policy
for an unresolvable cost:

- **Fixed**: one search equals one credit. The price is known without
  measurement, but whether the call was actually charged is not, since a
  provider may answer 200 with a failure body it never bills. So fixed routes
  still resolve through the meter: read the observed status and body and answer
  the declared price, zero for an unbilled failure, or unknown when the outcome
  was unreadable.
- **Metered**: LLM tokens. There is no honest number without measurement, and
  an unresolvable metered cost is recorded as unknown, never guessed.

`Unknown` means this meter cannot measure the route, and whether that is
refused or passed through unmeasured is the caller's policy.

## The runtime-credential allowlist

A runtime-supplied credential only ever travels on routes its meter
**explicitly** classifies: billable, or declared `Free`. An `Unknown` route, or
a URL outside the meter's base, is refused loudly before the credential is
attached. Nothing is sent.

Three consequences:

- A service cannot open a shared door without a registered meter. **The meter
  is the allowlist**, so a service whose calls all cost nothing still registers
  one classifying its routes `Free`.
- Adding a node that calls a new provider route on a shared door means adding
  that route to the meter first, with its pricing.
- A key the **user** pasted is theirs, so unknown routes pass through
  unmeasured. The gate keys off the credential's origin, never the service.

A multi-route meter opens each method with a match on the route and delegates
to per-route functions. It never infers the route from a response's shape.

## Cover the provider's whole surface, dynamically

A meter must cover everything the provider's **nodes** let the user ask for.

If a node exposes a model picker, the meter covers **every** model that picker
can produce. A hard-coded priced-models table turns a valid user choice into a
refusal, and it is stale the day the provider ships a model.

So fetch the provider's own rate catalog at call time, cached with a TTL,
instead of copying numbers into constants. Two shipped patterns to copy:

- **OpenRouter**: the billable route accepts any model; rates come from the
  provider's public price catalog, fetched and cached by the estimator.
- **fal**: any well-formed model submit is billable; the unit price and billing
  unit come from fal's authenticated pricing catalog through the meter's signed
  side-query lane, and the billed quantity is read from the request per unit
  kind (images, megapixels, seconds), so no per-model knowledge exists
  anywhere.

Hard-coded rates are acceptable only when the provider publishes no
machine-readable catalog **and** the rate is a property of the route rather than
a user-selectable model: a per-page OCR price, a per-hour transcription price.
Even then, price by family or prefix where the provider versions its models, and
refuse rather than guess on a name the mapping does not recognize.

## Ceilings are estimates, not blanket caps

A prepaid balance admits a call only if it can cover the ceiling. So a lazy
worst-case cap blocks users whose budget would comfortably cover the real cost.

Squeeze every pre-call signal before falling back to a provider-wide maximum:

- Price at the **request's** model and tier rate, fetched from the provider's
  catalog, never at "the dearest model we carry".
- Count what the request actually asks for: its token estimate, its result
  count, its page selection, its crawl limit, the content types it enables.
- When the priced quantity only exists in the response, look for a cheap
  pre-call proxy (a HEAD for the document's byte size) before reaching for the
  per-call cap. The cap is the last resort.

Over-estimation is still correct, since the measured figure settles the charge.
The overshoot just has to shrink as the request tells you more.

## Media estimation metadata

A request's media parts may carry estimation metadata the node's client library
kept on the wire: duration for audio and video, dimensions for images.
Read it in `ceiling_usd`, so a declared 90-second clip prices like a
90-second clip rather than a default guess.

It only ever **sharpens** the ceiling. Lying in it, or omitting it, moves the
pre-call estimate and never the cost figure, which is always the measured
actual, so it is not a trust surface.

`prepare` sheds it before the bytes go upstream.

## Tests a meter must ship

**Route classification.** Every route in the table, plus the trick paths
(`../`, `%2e%2e`, `@host`, backslash, trailing slash, case changes) all
classifying `Unknown`.

**The double-charge pin.** The cost-lookup route is `Free`.

**`prepare`.** The accounting opt-in is forced even when the caller opted out,
estimation metadata is shed, garbage bodies error loudly.

**Observation and resolve against recorded real responses.** A meter is a
function of bytes, so record a real non-streaming response, a real streaming
response fed in awkward chunk splits to prove reassembly, and a refused call,
then assert the exact dollars. This is what catches a meter that prices low on
every call.

**Interruption honesty.** An interrupted observation with nothing to anchor a
lookup on resolves to unknown.

**Sessions, when the meter has any.** The session route classifies as a
session; an observation fed recorded frames in both directions accrues and
closes to the expected dollars; the slice prices the dearest configuration.

## Getting a meter shipped in weft

A meter you already wrote for your own project is most of the work. To be
shipped in weft, and so usable on the shared keys, it also holds all four of
these:

1. **Every billable route is priced from published rates or a reported
   figure**, never guessed. A route whose real cost cannot be known stays
   `Unknown`, meaning own-key only, with a comment saying why.
2. **Cost-lookup and status routes are `Free`**, so nothing double-charges.
3. **No account-asset route is reachable by the platform key.** A route that
   creates or modifies durable things inside the credential's account (minting
   a voice, registering an agent, adding a webhook) classifies `Unknown`, never
   `Free` and never `Billable`, because on the shared key those assets would
   land in one account everybody shares. Listing routes that only serve pickers
   may be `Free`. Pair the refusal with an own-account-only capability on the
   service's permission catalogue, so the editor guides the user to their own
   account instead of failing at run time.
4. **The ceiling is the tightest bound the request allows**, and every test
   below is present, including resolves against recorded real responses.

## Write it as a pure function of bytes

A meter must assume nothing about the process running it.

Write it as a pure function of the request and response bytes plus its own
follow-up query, and it measures correctly wherever a paid call is measured. No
globals beyond your own rate caches, no environment reads beyond what the
follow-up lane hands you.

A meter never touches a credential, so the same meter works on a pasted key and
on a sign-in.
