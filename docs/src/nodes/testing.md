# Testing a node

Every node can carry its own tests, in Rust, next to its code. They run
without building a project and without a valid graph, and all but the last
tier below need no credentials and no network.

End-to-end tests cover runtime mechanisms, one per mechanism, riding some real
node as a vehicle. Node-level correctness goes here.

## The three tiers

**`basic`** is for pure logic: no ctx, no I/O, just a function and some
assertions.

**`fake`** runs the node's full `run` (or `setup_trigger`) body against a fake
ctx: canned provider responses, in-memory storage, canned signal payloads. No
credentials, no network, no cost.

**`live`** runs the node's body through the production access path: real
connection resolution, real provider calls, metered and billed like any run.
It needs a grant for the declared service and it can spend money, so runners
require an explicit opt-in and confirm before the first run.

## Where they live

In a `tests.rs` inside the node's folder, never in `mod.rs`. `mod.rs` carries
only the bridge, gated behind the `node-tests` feature, which is what keeps
tests out of every project binary.

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
        .ok()?;

    assert_eq!(outcome.outputs["id"], json!("m1"));
    rig.assert_sent("POST", "/api/send");
    Ok(())
}

async fn one_real_call(rig: LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(&MyNode, json!({ "account": rig.access("myservice"), "text": "hi" }))
        .await
        .ok()?;

    assert!(outcome.output("id")?.is_string());
    Ok(())
}
```

Test names are snake_case sentences:
`posts_to_a_channel_and_emits_the_permalink`,
`a_failed_permalink_read_never_fails_the_post`.

Fake and live are **separate declarations** by design. A fake test asserts
exact payloads against canned responses; a live test asserts loosely against
real provider output. One shared function would force mushy assertions on
both.

Assertions may panic. A panic fails that one test with its message, never the
whole run.

## The fake rig

| Call | What it does |
|---|---|
| `rig.respond(method, path, json)` | a canned 200. Matching tries `path?query` first, then the bare path. |
| `rig.respond_status(...)` | a refusal status |
| `rig.respond_raw(...)` | XML, CSV, binary bodies |
| `rig.access(service)` | a connection marker to place on the node's access input |
| `rig.connection_value(service, name, value)` | a stored value the opened connection answers |
| `rig.connection_permissions(service, granted)` | the scopes the grant came back with |
| `rig.signal(payload)` | queue a payload for the node's next `await_signal` |
| `rig.wake(payload)` | the wake payload for the next run: how a trigger fire is emulated |
| `rig.run(node, inputs)` | run the body. `inputs` is a JSON object; declared defaults fill anything absent. |
| `rig.run_setup_trigger(node, inputs)` | the trigger registration body |
| `rig.requests()` / `rig.assert_sent(method, path)` | the request log |
| `rig.registered_signals()` / `rig.logs()` | what registration and `ctx.log` recorded |
| `rig.execution_tags()` / `rig.stops()` | what `ctx.tag_execution` and `ctx.stop_tagged` asked for (nothing is stopped: a fake run has no siblings) |
| `rig.store_file(filename, mime, bytes)` | seed a stored file, get its value for a file input |
| `rig.output_type(port, type)` | declare a port's resolved type, for ports whose type the compiler normally works out from the `.weft` source |
| `rig.bus(&outcome.outputs["port"])` | the live bus behind an emitted marker |
| `rig.run_provision_infra(node, inputs)` | run an infra node's provision body and get the spec |

A `Generator[T]` input takes its value as a plain JSON **array**. The rig
pre-loads a live, already-finished feed with those items, so the body's pull
loop runs unmodified.

Storage is an in-memory map. `ctx.run` memo steps run fresh, because there is
no journal.

Two properties to rely on:

**A fake run can never reach the real network.** `ctx.http()` and a
connection-less `ctx.client(None)` answer from the same canned routes as
rig-opened connections, so an undeclared plain call fails loudly rather than
quietly hitting the internet from your test suite.

**Anything the fake does not support yet fails loudly naming the gap**, never
as a silent no-op.

The rig also enforces the production emission contract: undeclared output
ports and double emissions fail exactly as they would in a real run.

## The live rig

`rig.access(service)` hands back the resolved grant's marker for that service.
`rig.run` mirrors the fake rig, but every capability behind the ctx is the
production one. Each run mints a throwaway execution identity, so its cost
records belong to that run alone and are reported with the result.

- `rig.connect()` opens the grant as a real connection for the **test's own**
  setup and teardown: create a resource before running the node, delete what
  the node created after.
- `rig.bus(opts)` is a real bus as a writer plus a marker, for driving a
  node's bus input.

### Live tests spend real money

So drive the cost of each one as low as the provider allows.

Pick the **cheapest** model or tier that still exercises the node's real path,
and query the provider's price catalog rather than assuming. Use the smallest
inputs: one image, the shortest clip, a one-line prompt, the fastest quality
setting. Mint any needed input material on the cheapest route too.

The live tier must still **cover** the node. Every node whose body
talks to a provider gets at least one live test, and the set across a package
should touch the node's main use cases rather than one configuration's happy
path.

### Clean up after yourself

Live tests prefer self-provisioned targets: act on a resource named
`weft-node-tests` in the connected account, creating and cleaning up whatever
the API allows. Everything a test creates is deleted before it returns, so
repeated runs never pile artifacts onto someone's account.

Deliberate exceptions only where the artifact **is** the proof: a sent email, a
chat message.

Where cleanup is impossible, `rig.fixture("NAME")` reads a value you supply:
see [giving the live tier what it needs](#giving-the-live-tier-what-it-needs).

### Fixtures are declared

You declare every fixture your live test reads, on the test itself.

```rust
NodeTest::live("one_real_send", "telegram", live_send).with_fixture(fixture_spec(
    "TELEGRAM_CHAT_ID",
    "Chat id",
    "The chat the test sends into.",
)),
```

A fixture is described with the same shape node inputs use, so a runner renders
it with the machinery it already has, widgets included.

- `fixture_spec(name, label, description)` for a plain text parameter.
- `fixture_spec_like(manifest, input_name, fixture_name)` clones one of the
  node's own inputs and renames it, so the fixture inherits that input's
  widget. The Sheets tests declare `GOOGLE_SHEET_ID` off the node's
  `spreadsheet` input and inherit its picker.

The CLI checks them **before** a live run starts, failing up front with every
missing variable at once, before any grant or pod.

## Giving the live tier what it needs

A live test talks to a real account, so weft has to resolve a connection for
the service it declares before it can run one. Three ways to give it one, and
the easiest depends on the service.

**Connect the account in the editor**, on any project of yours, and
`weft test-node --tier live` picks that connection up by itself. This is the
normal path, and the only one for a service you sign in to rather than paste a
key for, such as Slack or Google. If you have several and want a specific one,
`--connection <service>=<grant-id>`.

**Paste a throwaway key** with `--key <service>`, which prompts for each of
that service's fields on stdin and deletes the connection it made when the run
finishes. Right for a key you do not want stored.

**Put the key in the environment** as `WEFT_NODE_TEST_<SERVICE>_<FIELD>`, for
example `WEFT_NODE_TEST_EXA_KEY`. Same throwaway connection as `--key`, without
the prompt, which is what a scripted run wants.

If none of the three is there, the run stops before touching anything and names
all three.

**Fixtures come from the environment too.** A test that needs a target it
cannot create for itself reads `WEFT_NODE_TEST_<NAME>`, so a declared
`SLACK_CHANNEL_ID` fixture reads `WEFT_NODE_TEST_SLACK_CHANNEL_ID`. Every
missing one is reported before the run starts rather than as you hit it.

These are ordinary environment variables, and the CLI reads the nearest `.env`
walking up from wherever you ran it, so your project's own `.env` is where they
normally live. Gitignore it.

## Running them

```bash
weft test-node                                  # every package's basic + fake tests
weft test-node slack                            # one package
weft test-node SlackSendMessage                 # one node
weft test-node SlackSendMessage --test posts_to_a_channel
weft test-node web --tier live                  # the live tier (confirms first)
weft test-node web --tier fake --tier live      # several tiers
weft test-node web --tier live --key exa        # a throwaway key, deleted after
weft test-node web --tier live --connection <grant-id>
```

`--tier` is repeatable. Without it, basic and fake run. **Live never runs
implicitly.**

`--parallel` runs tests concurrently: bare for everything at once, `--parallel N`
to cap in-flight tests. The report keeps declaration order either way.

Basic and fake runs compile the package's test crate on the host and run it
directly: no cluster, no daemon, no project build. Live runs go through the
runtime, each test in a short-lived pod with a worker's identity, so connection
resolution and metering are exactly the production path.

That covers writing and running your own nodes. Sweeping the **whole shipped
catalog**, which is what you do after changing something in weft that every
node sits on, is a different job with its own runner:
[CONTRIBUTING](https://github.com/WeaveMindAI/weft/blob/main/CONTRIBUTING.md#tests).

## Tests are never interactive

A test body must not read stdin or wait on a human. Every input comes from its
own inputs, from self-provisioning, or from a declared fixture, and a missing
one fails loudly naming what to set.

Once any grant field for a service comes from the environment, the whole grant
is environment driven, so a sweep never stops on a prompt. The only prompt left
is the one-time live-spend confirmation, and its "don't ask again" answer
silences it for good.

## What not to write

- **No per-node end-to-end tests.** If a test needs the dispatcher, the
  journal, or a real graph, it is testing a mechanism. Write or extend the one
  end-to-end test for that mechanism instead.
- **No tests in `mod.rs`.** A node folder's unit tests belong in `tests.rs` as
  basic entries. Package-level shared helper files, which are not nodes, keep
  ordinary `#[cfg(test)]` blocks.
- **No live tests on trigger or infra nodes.** There is nothing to test live
  without the provider pushing real events at real infrastructure, so the
  runner refuses them and `fake` is the top tier for those.
