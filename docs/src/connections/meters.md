# Measuring what a call costs

A **meter** is the per-provider Rust that computes the real cost of a paid API
call from the bytes of the request and the response.

The runtime runs the provider's meter around every call made on an opened
connection's client. So every cost figure in the system is a meter's output. A
node never states a cost, and it cannot reach that path.

## Where a meter lives

A meter can live in two places, and it runs the same way in both. The worker
runs it, so nothing central has to know it exists.

**In weft**, one file under `crates/weft-providers/src/providers/`. This is a
provider weft ships and reviews.

**In your own project**, beside the nodes that call the provider: a shared
`.rs` file at a package root, or the bottom of a bare node's `mod.rs`. This is
how a project supports a provider weft does not ship yet, with a key you set
yourself.

Either way, adding a provider is a file plus one line:

```rust
weft_providers::register_meter!(MY_METER);
// inside weft's own crate: crate::register_meter!(MY_METER);
```

The registry collects every registration at link time, weft's meters and your
project's alike, because your project compiles into the same worker. Forget
the line and the provider is simply unsupported: a loud refusal wherever a
measured call is required, never a silent wrong number.

The node and the meter connect only through the provider name string. The
node never imports the meter.

A meter in your project measures spend on **your** key. If it gets a number
wrong, the money it got wrong is yours. A meter shipped in weft runs wherever
a shipped credential does; the platform-key rule is in [Who can pay](#who-can-pay).

If you cannot find where the provider reports what it charged, keep the meter
in your project, on your own key. Do not ship a meter that prices only some
of its billable routes. The key is accepted on every billable route, so the
unpriced calls spend real money and land in the trail as unknown.

## Who can pay

**Your own connection**, your key or your signed-in account, works with
**any** meter wherever it lives.

The platform key, where weft holds the credential and bills a user's balance,
is being designed now. It will only spend on a provider weft ships a reviewed
meter for, which is why the bar below applies to your meter today. For what
getting a provider onto that key takes, go and read [Getting a meter shipped in
weft](#getting-a-meter-shipped-in-weft).

## The trait

```rust
#[async_trait::async_trait]
impl ProviderMeter for MyProviderMeter {
    fn service(&self) -> &'static str;
    fn base_url(&self) -> &'static str;
    fn classify(&self, method: &str, path: &str) -> RouteClass;
    fn prepare(&self, path: &str, body: &[u8]) -> anyhow::Result<Option<Vec<u8>>> { Ok(None) }
    async fn ceiling_usd(&self, path: &str, body: &[u8], follow_up: FollowUp<'_>)
        -> anyhow::Result<f64>;
    fn observe(&self, path: &str, query: &str, request_body: &[u8])
        -> Box<dyn CallObservation>;
    async fn resolve(&self, path: &str, observed: ObservedCall, follow_up: FollowUp<'_>)
        -> MeasuredCost;
    async fn priceable(&self, path: &str, follow_up: FollowUp<'_>) -> anyhow::Result<()>;
    fn opens_charge(&self, path: &str, observed: &ObservedCall) -> Option<String>;
    fn charge_reported_on(&self, path: &str, observed: &ObservedCall) -> Option<String>;
    async fn fold_report(&self, path: &str, observed: ObservedCall,
        scratch: &mut Value, follow_up: FollowUp<'_>) -> Option<MeasuredCost>;
    fn observe_session(&self, path: &str, query: &str)
        -> anyhow::Result<Box<dyn SessionObservation>>;
    fn session_slice_usd(&self, path: &str) -> anyhow::Result<f64>;
    fn session_max_frame_bytes(&self, path: &str) -> anyhow::Result<usize>;
}
```

**`base_url`** is the single authority for where the provider lives. No caller
ever accepts a host from a request instead. Requests are rebuilt against this
base, so a request cannot be aimed at a host the meter did not name.

**`classify`** maps a method and relative path to a class. It matches against
the **raw path**, never a normalized one. An unknown route can be refused by
the caller's policy, so traversal (`../`), encoded traversal (`%2e%2e`),
userinfo (`@host`), and backslash tricks all have to fail to match and come
back `Unknown`. Matching raw is what gives you that. If a route has a
parameterized segment, match its prefix and guard the segment's character
set, the way `elevenlabs.rs` does for `text-to-speech/{voice}`. Normalizing
first would take the protection away.

**`prepare`** rewrites a billable call's outgoing body so its cost becomes
reportable at all. For example, it can force the provider's usage-accounting
opt-in, overriding whatever the caller set. It also sheds anything internal
with no business going upstream. An unparseable body on a route needing a
rewrite is a loud error, because an unpreparable call would be unmeasurable
spend. Most providers report what a call cost without being asked, so this
one has a default that sends the body untouched. Write it only if you have
something to opt into.

**`ceiling_usd`** is a worst-case price computable **before** the call goes
out. It must be computed **only** from the request bytes, never from anything
the caller could hand over separately. A side channel for "here is my
conversation, for estimation purposes" would let a caller understate what it
is about to spend. Lean high; the measured actual is the figure that counts.
A call that cannot be priced is a loud error, never a guess. When the rates
live behind the provider's own authenticated API, `follow_up` is how the
meter asks for them: a signed request the meter makes on its own, outside the
call it is pricing.

You only need this if you want the provider on the shared keys. On the user's
own credential nothing has to be bounded before the call, so the default,
which refuses, is the right one to leave in place. `session_slice_usd` works
the same way.

**`observe`** mints a fresh observer for one call. It is handed the query
string and the request body as well as the path, because some routes are
priced from what was sent rather than what came back. A text-to-speech call
prices its text's characters, and an output format in the query decides bytes
per second. The observer then sees every byte **as it flows through** to the
real consumer. It must never buffer, delay, or reorder chunks, and it must
stay small in memory however long the stream runs.

**`resolve`** turns the observation into dollars. If the provider reported
the cost inline, this is pure. If the provider only answers out of band, the
**meter** makes that follow-up query itself. The node and its client library
are never involved and never trusted to do it. A cost that genuinely cannot
be resolved is an honest `None`, recorded as unknown, **never a fake zero**,
because a zero would read as a call that cost nothing.

### Read what the provider charged, never what the request implies

Find the number the provider itself reports, and price on that number. It may
be in the response body, in a header, or behind a follow-up query on the
provider's own ledger. Read the provider's billing docs, and dump the headers
of a real response, before you conclude there is no such number.

Do not price a call by re-deriving the quantity from the request. A model
priced by tokens whose `quality` setting moves the token count cannot be
priced from its request at all.

fal reports in two different ways, depending on the model. If fal can count
what a model produced, it puts that count in the
`X-Fal-Billable-Units` header on the result fetch, in the
same unit the catalog prices, so the cost is that number times the catalog's
`unit_price`. A model billed by GPU time has nothing countable to state. It
sends no such header, and reports its measured run time as
`metrics.inference_time` on the status route instead. Its catalog `unit` is
`compute seconds`, so that run time is the quantity the unit price
multiplies.

Every fal model measured so far is one or the other. The header is optional
by fal's design, though. So a finished job that states no count on a model
priced by anything other than `compute seconds` has no figure it can stand
behind and books as unknown.

Measured against fal on 2026-09-10: ask `fal-ai/flux/dev` for one 512x512
image and the request says a quarter of a megapixel, while fal bills one
whole megapixel. A request-derived price is four times under, and nothing in
the trail says so.

**`observe_session`** mints the per-session tap for a route classified as a
session. It is fed every frame in both directions, answers the running
accrued cost, and closes into a measured cost. It is required for any meter
classifying a route as a session; the default refuses loudly.

**`session_slice_usd`** is the session's version of a ceiling. A session has
no knowable total before it runs, so instead of reserving the whole thing the
runtime reserves a slice at a time. Price the dearest configuration for a
fixed span, say one minute, and reserve another as the accrued cost catches
up. It is only read where a call has to be paid for before it is allowed to
start.

**`session_max_frame_bytes`** bounds one frame. It is sized so a single frame
can never accrue more than one slice's worth at the route's dearest rate.
Account for the wire form, such as base64 expansion.

### Three answers a status gives you, and only one of them is zero

If the provider answered in the 200s, the amount is yours to work out. If it
refused in the 400s, nothing was billed, and zero is a fact you can write
down. If it answered in the 500s, you do not know. The work may have run and
been billed, with something in front of it falling over afterwards. And a
gateway timing out over a job that is queued and spending looks exactly the
same from here. Writing zero there claims a real spend was free.

`weft_providers::providers::cost_from_status` is that rule, in one place.
Call it first and price only what it hands back to you:

```rust
if let Some(cost) = weft_providers::providers::cost_from_status(observed.status, "the call") {
    return cost;
}
```

### Price on every parameter that moves the price

If the request can ask for something dearer, read it and use it. Firecrawl
bills a plain page fetch at one credit and several for a stealth proxy or for
structured extraction; fal bills a higher resolution above the unit price.
All of that is in the request body your meter is handed.

The tempting shortcut is a flat rate justified by what one node happens to
send. That is not a fact about the call. A meter sits on the CONNECTION, so
any program holding that connection can send the dear options, and the flat
rate then understates every one of those calls. If the interface cannot carry
what the price depends on, extend the interface rather than approximate
around it.

And if you know the flat figure is wrong but not what the right one is,
record the spend as unknown and name the option that made it so.

### Refuse what you could never price, before the call goes out

`priceable` runs before a billable call is sent. Say `Err` and the call never
happens, and the caller is told why.

This is for the one case nothing downstream can recover from: a call your
meter could never put a figure on, whatever the provider answers. fal's is a
model its pricing catalog lists no price for, because the unit price is the
only place a fal price exists. Asking costs nothing, since the catalog is
free and cached. And it is the last moment the spend can still be prevented.

It is a different question from `ceiling_usd`, which asks what the worst case
costs. A call can be perfectly priceable afterwards and have no bound before.
A model billed by GPU time can run as long as it likes, and its run time
still prices it exactly. So the two gates are separate: a call can fail one
and pass the other, in either direction.

### When the amount arrives after the call

Some providers commit the money on one call and only state the amount on a
later one. A queue is the usual shape: you submit a job and get an id back,
and the amount only exists once the job has run.

The node's client polls the queue the way it always has. Because those polls
ride the same connection client, the worker sees every one of them. Your
meter never polls anything itself. Classify the routes that report as
`Reports`, a class that costs nothing to call but is still watched (it is in
the route table below), and answer three things.

A **charge** is one spend the worker is holding on to because its amount is
not known yet. It is opened by the billable call and closed by whichever
later response states the figure.

The id has to be one the PROVIDER assigned, read out of the response: a job
id, a request id, a task id. Never one you invented, and never one taken from
the request. A worker holds the charges of every run it is driving under
`(service, id)`, and a report arrives carrying nothing but its own response.
So two runs whose ids the meter chose rather than read will collide, and the
figure one reports is booked against the other run's spend.

- **`opens_charge`** on the billable call: the id that ties this spend to the
  responses that will report on it, read straight off the observation. Say
  `Some(id)` and the worker holds that charge open; `resolve` is never called
  for the submit itself. Say `None` (the default) and the call is priced
  inline as usual.
- **`charge_reported_on`** on a reporting response: which open charge it
  speaks for. If the provider has several routes for one job (`/{id}`,
  `/{id}/status`, `/{id}/errors`), every one of them has to answer the same
  id. Otherwise the job never closes, and its spend books as unknown while
  its own response was stating the figure.

- **`fold_report`**: read the figure off this response. Return `Some(cost)`
   and the charge closes and is booked; return `None` and it stays open for
the next report. A finished job you have no figure you can stand behind is
still `Some`, with `amount_usd: None`, so it books as unknown rather than
staying open forever.

Each charge carries a scratch: a small JSON blob only your meter reads and
writes. It starts as whatever the billable call's observation recorded (the
model, for one). It survives from one report to the next, so a provider that
states its figure across two responses can stash the first half and wait for
the second.

The worker matches each report to the charge it names and writes the figure
to the cost trail once your `fold_report` answers one. If the pod exits with
a charge still open, that charge is booked as unknown. A job you submitted
and never read back still leaves a row saying money went out.

## Route classification, and the double-charge trap

A cost-lookup route looks like a call and must cost nothing.

| Route | Class | Why |
|---|---|---|
| `POST chat/completions` | `Billable(Metered)` | the actual spend |
| `GET generation` | `Free` | the cost **lookup** for a spend |
| `GET models` | `Free` | the public price catalog |
| `GET <app>/requests/<id>` and `.../status` | `Reports` | costs nothing itself; its response is where the amount of an earlier spend shows up |
| `speech-to-text/realtime` (a WebSocket) | `BillableSession` | a long-lived two-way channel; no total knowable up front |
| anything else | `Unknown` | cannot be measured, so cannot be billed |

If the cost-lookup route were billable, a node re-querying its own cost would
be billed a second time. The meter's **own** follow-up query would be billed
too, recursively. Classifying it `Free` makes that impossible: the route
table already says the call bills nothing, wherever it came from.

A billable route also declares **how** it prices, which doubles as the policy
for an unresolvable cost:

- **Fixed**: one search equals one credit. The price is known without
  measurement, but whether the call was actually charged is not, since a
  provider may answer 200 with a failure body it never bills. So fixed routes
  still resolve through the meter: read the observed status and body and
  answer the declared price, zero for an unbilled failure, or unknown when
  the outcome was unreadable.
- **Metered**: the price is only knowable from the response, as with LLM
  tokens or fal's billed units. An unresolvable metered cost is recorded as
  unknown. If you are about to record unknown, go and read [reading what the
  provider
  charged](#read-what-the-provider-charged-never-what-the-request-implies)
  first.

## The runtime-credential allowlist

A runtime-supplied credential only ever travels on routes its meter
**explicitly** classifies: billable, or declared `Free`. An `Unknown` route,
or a URL outside the meter's base, is refused loudly before the credential is
attached. Nothing is sent.

Three consequences:

- A service cannot open a shared door without a registered meter. **The meter
  is the allowlist**, so a service whose calls all cost nothing still
  registers one classifying its routes `Free`.
- Adding a node that calls a new provider route on a shared door means adding
  that route to the meter first, with its pricing.
- A key the **user** pasted is theirs, so unknown routes pass through
  unmeasured. The gate keys off the credential's origin, never the service.

A multi-route meter opens each method with a match on the route and delegates
to per-route functions. It never infers the route from a response's shape.

## Cover the provider's whole surface, dynamically

A meter must cover everything the provider's **nodes** let the user ask for.

If a node exposes a model picker, the meter covers **every** model that
picker can produce. A hard-coded priced-models table turns a valid user
choice into a refusal, and it is stale the day the provider ships a model.

So fetch the provider's own rate catalog at call time, cached with a TTL,
instead of copying numbers into constants. Two shipped patterns to copy:

- **OpenRouter**: the billable route accepts any model; rates come from the
  provider's public price catalog, fetched and cached by the estimator.
- **fal**: a `POST` to any model path is billable, with no model list gating
  it; the unit price and billing unit come from fal's own pricing catalog at
  `api.fal.ai`, asked through `follow_up` and cached for an hour. That
  catalog is on a different host from the queue the calls ride, which is
  fine: `base_url` bounds where a *caller's* request may be aimed, and a
  `follow_up` is the meter's own call to one of the provider's origins.

Hard-coded rates are acceptable only when the provider publishes no
machine-readable catalog **and** the rate is a property of the route rather
than a user-selectable model: a per-page OCR price, a per-hour transcription
price. Even then, price by family or prefix where the provider versions its
models, and refuse rather than guess on a name the mapping does not
recognize.

## Ceilings are estimates, not blanket caps

A prepaid balance admits a call only if it can cover the ceiling. So a lazy
worst-case cap blocks users whose budget would comfortably cover the real
cost.

Squeeze every pre-call signal before falling back to a provider-wide maximum:

- Price at the **request's** model and tier rate, fetched from the provider's
  catalog, never at "the dearest model we carry".
- Count what the request actually asks for: its token estimate, its result
  count, its page selection, its crawl limit, the content types it enables.
- When the priced quantity only exists in the response, look for a cheap
  pre-call proxy (a HEAD for the document's byte size) before reaching for
  the per-call cap. The cap is the last resort.

Over-estimation is still correct, since the measured figure settles the
charge. The overshoot just has to shrink as the request tells you more.

Under-estimation is not correct, and it is the easy mistake. A ceiling is
what a prepaid balance reserves against, so every step of it rounds UP:
whole units rather than fractions of one where the provider counts whole
units, the resolution multiplier wherever it applies, and a refusal rather
than a clamp when the request asks for more than you can bound. Clamping a
two minute video down to the one minute your bound knows about is a ceiling
that is half the real charge, which is the one thing a ceiling may never be.

## Media estimation metadata

A request's media parts may carry estimation metadata the node's client
library kept on the wire: duration for audio and video, dimensions for
images. Read it in `ceiling_usd`, so a declared 90-second clip prices like a
90-second clip rather than a default guess.

It only ever **sharpens** the ceiling. Lying in it, or omitting it, moves the
pre-call estimate and never the cost figure, which is always the measured
actual. So it is not a trust surface.

`prepare` sheds it before the bytes go upstream.

## Tests a meter must ship

**Route classification.** Every route in the table, plus the trick paths
(`../`, `%2e%2e`, `@host`, backslash, trailing slash, case changes) all
classifying `Unknown`.

**The double-charge pin.** The cost-lookup route is `Free`.

**`prepare`.** The accounting opt-in is forced even when the caller opted
out, estimation metadata is shed, garbage bodies error loudly.

**Observation and resolve against recorded real responses.** A meter is a
function of bytes, so record a real non-streaming response, a real streaming
response fed in awkward chunk splits to prove reassembly, and a refused call,
then assert the exact dollars. This is what catches a meter that prices low
on every call.

**Interruption honesty.** An interrupted observation with nothing to anchor a
lookup on resolves to unknown.

**Sessions, when the meter has any.** The session route classifies as a
session; an observation fed recorded frames in both directions accrues and
closes to the expected dollars; the slice prices the dearest configuration.

## Getting a meter shipped in weft

To be shipped in weft, and so usable on the shared keys, a meter also holds
all four of these:

1. **Every billable route reads the figure the provider reports**: a field in
   the response, a header, or the provider's ledger through `follow_up`. A
   route whose real cost the provider will not report stays `Unknown`,
   meaning own-key only, with a comment naming what you checked and what it
   returned. For why a request-derived quantity is not that figure, go and
   read [reading what the provider
   charged](#read-what-the-provider-charged-never-what-the-request-implies).
2. **Cost-lookup and status routes are `Free`**, so nothing double-charges.
3. **No account-asset route is reachable on a shared key.** A route that
   creates or modifies durable things inside the credential's account
   (minting a voice, registering an agent, adding a webhook) classifies
   `Unknown`, never `Free` and never `Billable`, because on the shared key
   those assets would land in one account everybody shares. Listing routes
   that only serve pickers may be `Free`. Pair the refusal with an
   own-account-only capability on the service's permission catalog, so the
   editor guides the user to their own account instead of failing at run
   time.
4. **The ceiling is the tightest bound the request allows**, and every test
   below is present, including resolves against recorded real responses.

## Write it as a pure function of bytes

A meter must assume nothing about the process running it.

Write it as a pure function of the request and response bytes plus its own
follow-up query, and it measures correctly wherever a paid call is measured.
No globals beyond your own rate caches, no environment reads beyond what the
follow-up lane hands you.

A meter never touches a credential, so the same meter works on a pasted key
and on a sign-in.
