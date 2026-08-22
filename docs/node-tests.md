# Node self-tests

Every node can carry its own tests, in Rust, next to its code. They
run without building the project, without a valid graph, and (for the
free tiers) without any credentials or infrastructure. The guiding
rule: **e2es test mechanisms, node tests test nodes.** Write one e2e
per runtime mechanism (riding some real node as its vehicle); put
node-level correctness here.

## The three tiers

- **basic**: pure logic. No ctx, no I/O. A plain function with
  assertions.
- **fake**: the node's full `run` (or `setup_trigger`) body against a
  fake ctx: canned provider responses, in-memory storage, canned
  signal payloads. No credentials, no network, no cost. This is the
  TOP tier for trigger and infra nodes (there is nothing meaningful to
  test live without the provider pushing real events at real
  infrastructure); a live declaration on them is refused by the
  runner.
- **live**: the node's body through the PRODUCTION access path: real
  connection resolution, real provider calls, metered and billed like
  any run. Needs a grant for the declared service and can spend money,
  so runners require an explicit opt-in and confirm before the first
  run.

## Writing tests

Tests live in a `tests.rs` inside the node's folder, never in
`mod.rs`. `mod.rs` carries only the bridge, gated behind the
`node-tests` feature (the gate is what keeps tests out of every
project binary):

```rust
// mod.rs
#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for MyNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> { ... }
}
```

`tests.rs` declares a list; each entry is independently named, tiered,
and runnable:

```rust
// tests.rs
use serde_json::json;
use weft::{FakeRig, LiveRig, NodeTest, WeftResult};
use super::MyNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("parses_the_answer", || {
            assert_eq!(super::parse("x=1")?, 1);
            Ok(())
        }),
        NodeTest::fake("posts_and_emits", posts_and_emits),
        NodeTest::live("one_real_call", "myservice", one_real_call),
    ]
}

async fn posts_and_emits(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/api/send", json!({ "ok": true, "id": "m1" }));
    let outcome = rig
        .run(&MyNode, json!({ "account": rig.access("myservice"), "text": "hi" }))
        .await
        .success()?;
    assert_eq!(outcome.outputs["id"], json!("m1"));
    rig.assert_sent("POST", "/api/send");
    Ok(())
}

async fn one_real_call(rig: LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(&MyNode, json!({ "account": rig.access(), "text": "hi" }))
        .await
        .success()?;
    assert!(outcome.output("id")?.is_string());
    Ok(())
}
```

Fake and live are SEPARATE declarations by design: a fake test asserts
exact payloads against canned responses; a live test asserts loosely
against real provider output. One shared function would force mushy
assertions on both.

Assertions may panic (`assert!`, `assert_eq!`): a panic fails that one
test with its message, never the whole run.

### The fake rig surface

- `rig.respond(method, path, json)`: a canned 200 for requests the
  node sends on a rig-opened connection. Matching tries the request's
  `path?query` first, then the bare path. `respond_status` sets a
  refusal status; `respond_raw` serves XML/CSV/binary bodies.
- `rig.access(service)`: a connection marker to place on the node's
  access input. Opening it hands the node a client whose calls are
  answered from the canned routes and recorded.
- `rig.connection_value(name, value)`: a stored value the opened
  connection answers (`conn.value(name)`).
- `rig.signal(payload)`: queue a payload for the node's next
  `ctx.await_signal` (mid-flow suspensions). An await with an empty
  queue fails loud. The rig also refuses `await_signal` exactly where
  an execution would, whatever the queue holds: after the body has
  emitted on or closed any output port, and always for a node that
  declares a `Generator` input (a stream consumer cannot durably
  suspend).
- `rig.wake(payload)`: the wake payload (`ctx.wake`) for the next run;
  how a trigger's firing is emulated.
- `rig.run(node, inputs)` / `rig.run_setup_trigger(node, inputs)`:
  run the body. `inputs` is a JSON object of input name to value;
  declared metadata defaults fill anything absent. A `Generator[T]`
  input takes its value as a plain JSON ARRAY of items: the rig
  pre-loads a live, already-finished feed with them, so the body's
  `ctx.inputs.get::<Generator<T>>` pull loop runs unmodified. Returns
  a `RunOutcome`: the body's result, the emitted outputs by port, the
  closed ports.
- `rig.requests()` / `rig.assert_sent(method, path)`: the request log.
- `rig.registered_signals()` / `rig.logs()`: what `register_signal`
  and `ctx.log` recorded.
- `rig.store_file(filename, mime, bytes)`: seed a stored file and get
  its stored-file value for a file input.
- `rig.output_type(port, type)`: declare an output port's resolved
  type, playing the compiler's role for `MustOverride` and
  form/inline-derived ports.
- `rig.bus(&outcome.outputs["port"])`: the live bus behind an emitted
  marker, for reading what the run sent (buses are the REAL in-process
  primitive, held in the rig).
- `rig.run_provision_infra(node, inputs)`: run an infra node's
  `provision_infra` body and get the declared `InfraSpec`.

Storage (`ctx.storage`) is an in-memory map; `ctx.run` memo steps run
fresh (there is no journal). Anything the fake does not support yet
(buses, infra endpoints, provider sockets, URL-fetching storage verbs)
fails loud naming the gap, never a silent no-op. A fake run can never
reach the real network: `ctx.http()` and a connection-less
`ctx.client(None)` answer from the same canned routes as rig-opened
connections, so an undeclared plain call fails loud too.

The rig enforces the production emission contract: undeclared output
ports and double emissions fail exactly like they would in a run.

### The live rig surface

`rig.access()` is the resolved grant's marker for the test's declared
service; `rig.run(node, inputs)` mirrors the fake rig but every
capability behind the ctx is the production one. Outputs are captured
at the seam (a node test has no downstream graph); each run mints a
throwaway execution identity, so its cost records attribute to a real
color of their own, reported with the result.

- `rig.connect()`: the test's declared-service grant, opened as a real
  connection for the test's OWN setup/teardown calls
  (`conn.client()`): create a resource before running the node, delete
  what the node created after. The node itself is still driven through
  `rig.run`; the runner's settle releases the lease.
- `rig.bus(opts)`: a real bus as `(writer, marker)`: place the marker
  on a Bus-typed input, register a name on the writer, `send` /
  `send_bytes` frames, then `close` so the node sees the stream end.

Live tests spend real money, so drive the cost of each one as low as
the provider allows: pick the CHEAPEST model/tier that still exercises
the node's real path (query the provider's price catalog rather than
assuming; the fal tests run their videos on a two-cent model, not the
default), the smallest inputs (one image, the shortest clip, a
one-line prompt, the fastest quality settings), and mint any needed
input material on the cheapest route too. A live test that costs
dollars per run stops being run; a test nobody runs protects nothing.

At the same time, the live tier must COVER the node: every node whose
run body talks to a provider gets at least one live test, and the set
of live tests across a package should touch the node's main use cases
(the primary action, plus any second path with its own provider
behavior, e.g. a masked edit vs a plain edit) rather than only the
happy path of one configuration. Cheap and covering beats expensive
and singular: two one-cent tests on two paths are worth more than one
dollar test on one.

Live tests prefer self-provisioned targets: act on a resource named
`weft-node-tests` in the connected account, creating (and cleaning up)
what the API allows. A live test cleans up after itself wherever the
API allows it: everything it creates in the connected account
(uploads, temporary resources) is deleted before the test returns, so
repeated runs never pile artifacts onto the account. Deliberate
exceptions only where the artifact IS the proof (a sent email, a chat
message). The runtime side (test pod, execution color, an ephemeral
key grant) is cleaned up by the runner regardless. Where that is impossible, `rig.fixture("NAME")`
reads `WEFT_NODE_TEST_<NAME>` (for example
`WEFT_NODE_TEST_TELEGRAM_CHAT_ID`): every such variable in the
caller's environment is forwarded into the test run, and a missing one
fails the test naming exactly what to set.

Every fixture a live test reads is also DECLARED on the test, so it is
a visible parameter instead of a surprise at run time:

```rust
NodeTest::live("one_real_send", "telegram", live_send).with_fixture(fixture_spec(
    "TELEGRAM_CHAT_ID",
    "Chat id",
    "The chat the test sends into.",
)),
```

A fixture is described with the same `InputSpec` shape node inputs
use, so any runner can render and collect it with the machinery it
already has for inputs, widgets included. Two ways to build the spec:

- `fixture_spec(name, label, description)`: a plain text parameter.
- `fixture_spec_like(manifest, input_name, fixture_name)`: clone one
  of the node's own inputs and rename it, so the fixture inherits that
  input's widget (the sheets tests declare `GOOGLE_SHEET_ID` off the
  node's `spreadsheet` input and inherit its picker).

One `with_fixture` per `rig.fixture` the body reads; `with_fixture` on
a basic or fake test panics (those tiers read no fixtures). The
declarations ride in the test binary's `list` output, and the CLI
checks them before a live run starts: every required fixture must be
set (non-empty) in the environment, and a miss fails up front listing
every missing variable at once, before any grant or pod. The env
variables stay the local prefill; `rig.fixture` in the pod remains the
runtime backstop.

## Running tests

```
weft test-node                 # every package's basic + fake tests
weft test-node slack           # one package
weft test-node SlackSendMessage        # one node
weft test-node SlackSendMessage --test posts_to_a_channel_and_emits_the_permalink
weft test-node web --tier live         # the live tier (confirms first)
weft test-node web --tier fake --tier live     # several tiers in one run
weft test-node web --tier live --key exa       # paste a throwaway key, deleted after
weft test-node web --tier live --connection <grant-id>
```

`--tier` is repeatable; without it the basic and fake tiers run. Live
never runs implicitly.

`--parallel` runs tests concurrently: bare, everything at once;
`--parallel N` caps in-flight tests at N; absent, one at a time. Free
tiers interleave freely (in-process, no shared state); each live test
runs in its own pod with its own execution identity, so they
interleave too. The report keeps declaration order either way.

The whole catalog sweeps through `scripts/run-node-tests.sh`, which
runs package by package and stops at the first failure (mirroring
`run-e2e.sh`: `--from <package>` resumes a broken run, bare names
scope it, `--tier` picks tiers; on a live run the script passes
`--yes`, since `--tier live` is already the opt-in and a sweep cannot
stop on a prompt). `--parallel` runs every package at once and
`--parallel N` runs batches of N packages, with N also forwarded as
each package's in-flight test cap; parallel outputs are
buffered per package and printed in suite order, and a failure stops
after its whole batch settles (so every failure in that batch is
visible). It runs inside a scratch project at `target/node-tests`
whose base catalog re-syncs from the checkout on every run. The suite
shares nothing with the e2e suite (own scratch project, own build
dirs, own projects on the cluster), so the two can run at the same
time.

Basic/fake runs compile the package's test crate on the host (a Rust
toolchain is required; the first build compiles the engine once, then
everything is cached under `.weft/target/test/`) and run it directly.
No cluster, no daemon, no project build, no valid `main.weft`.

Live runs go through the runtime: the CLI builds the package's test
image, and each test runs in a short-lived `weft-test-` pod with a
worker's identity, so connection resolution, relaying, and metering
are exactly the production path. Live needs the project registered
(`weft run`/`weft activate` once) and a grant for the service: your
existing connection, `--key <service>` for an ephemeral pasted key, or
`--connection <id>` to pick one of several. For scripted runs, each
pasted-key field can come from the environment instead of a prompt:
`WEFT_NODE_TEST_<SERVICE>_<FIELD>` (uppercased, non-alphanumerics as
`_`; for example `WEFT_NODE_TEST_EXA_KEY`), which also stands in
for `--key` when no connection exists. Before the first live run
the CLI asks a yes/no confirmation; answer the follow-up to persist
"don't ask again" (stored in `~/.config/weft/cli.toml`).

Every basic + fake test of the bundled catalog also runs through one
sweep test in the workspace, part of the ordinary workspace test
suite, so a test written once is covered by the normal test run.

Tests are never interactive. A test body must not read stdin or wait
on a human: every input it needs comes from its own inputs, from
self-provisioning, or from a `rig.fixture` environment variable, and a
missing one fails loud naming what to set. The same holds for the
machinery around a scripted run: once any grant field for a service
comes from the environment, the whole grant is env-driven (a missing
required field fails naming its variable, a missing optional field is
skipped), so a sweep never stops on a prompt. The only prompt left is
the one-time live-spend confirmation, and its "don't ask again" answer
silences it for good.

## What NOT to write

- No per-node e2es. If a test needs the dispatcher, the journal, or a
  real graph, it is testing a mechanism: write (or extend) the ONE e2e
  for that mechanism instead.
- No tests in `mod.rs`. A node folder's unit tests belong in
  `tests.rs` as `NodeTest::basic` entries; package-level shared helper
  files (not nodes) keep ordinary `#[cfg(test)]` blocks.
- No live tests on trigger or infra nodes (the runner refuses them).
