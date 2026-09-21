# Testing a node

Tests live beside the node, in a `tests.rs` in its own folder. An access node
has none: the `access_node!` macro is its whole body, so there is nothing of
yours to test.

`tests.rs`:

```rust
pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("counts_words", counts_words)]
}

async fn counts_words(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&WordCountNode, json!({ "text": "one two three" })).await.ok()?;
    assert_eq!(outcome.outputs["count"], json!(3.0));
    Ok(())
}
```

`mod.rs` bridges it in:

```rust
#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for WordCountNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }
    // ...
}
```

There is no registry and no list to maintain. The runner walks the nodes the
binary already has and asks each one for its tests.

```bash
weft test-node WordCount        # one node
weft test-node slack            # one package
weft test-node                  # everything
```

## Three tiers

| Tier | What it gets | Costs |
|---|---|---|
| `basic` | Nothing. A plain function testing your own logic | Nothing |
| `fake` | Canned HTTP, in-memory storage, canned signals, fake connections | Nothing |
| `live` | The real runtime: real connections, real calls, real metering | Real money |

Basic and fake run by default. **Live never runs unless you ask**, and the
runner confirms before it does.

Basic and fake compile and run right there with cargo, so no docker, no
cluster, no daemon, and only the packages you targeted get built.

Two things to know. A `basic` test is a plain sync function and the runner is
already inside an async runtime, so building one inside it panics. And for a
trigger or an infra node, `fake` is the top tier: the live rig drives a plain
`run` body, so a live test on those is refused.

A fake test that never finishes fails by name after 30 seconds, and tells you
where to look:

```text
test 'reads the stream' did not finish within 30s. Something in it is waiting
for what it never gets: a cursor reading a bus nothing closes, a caller nobody
attaches, a signal nobody answers.
```

## The fake rig

| You want | Call |
|---|---|
| Run the node | `rig.run(&node, json!({...}))` |
| Run its trigger setup, or its infra | `rig.run_setup_trigger(...)`, `rig.run_provision_infra(...)` |
| Canned HTTP | `rig.respond(method, path, body)`, `rig.respond_status(...)`, `rig.respond_raw(...)`, `rig.respond_with_headers(...)` for an answer that lives partly in a header (a created id, an ETag) |
| Check what it sent | `rig.requests()`, `rig.assert_sent(method, path)` |
| What `ctx.run` recorded | `rig.recorded_steps(&node)`. Run the node twice on one rig and the second run replays the first's steps and the signals it was answered with, like a resumed execution, so a publish that must happen once is something you can prove: two runs, one request. Each node keeps its own, so two nodes on one rig never replay each other's |
| A connection | `rig.access("slack")`, `rig.connection_value(...)`, `rig.connection_permissions(...)` |
| A trigger's event | `rig.wake(payload)`, `rig.signal(payload)` |
| A live caller | `rig.attach_caller(conn)` |
| A bus going in | `rig.seed_bus(opts)`, giving you a writer and a marker |
| A bus it emitted | `rig.bus(&outcome.outputs["channel"])` |
| A stored file | `rig.store_file(filename, mime, bytes)`, `rig.stored_meta(key)` |
| An infra endpoint | `rig.declare_endpoint(name, url)`, `rig.answer_endpoint(...)`, `rig.refuse_endpoint(...)` |
| What it asked for | `rig.registered_signals()`, `rig.awaited_signals()`, `rig.logs()`, `rig.execution_tags()`, `rig.stops()` |
| A port's type, for a node that takes added ports | `rig.output_type(port, ty)`, `rig.input_type(port, ty)` |

The outcome gives you `.ok()` for the happy path, `.failure()` for the error
string, `.output(port)` for one port, `.infra_spec()` for an infra run, and
`.outputs` for the map.

Note the last group: in a fake test nothing is really stopped and nothing is
really tagged. The rig records what your node **asked for**, and that is what
you assert on.

## Live tests

```bash
weft test-node SlackSendMessage --tier live
```

The runner tells you it will spend money and asks. `--yes` skips that, and it
can remember your answer.

For credentials, `--connection slack=<grant-id>` uses one you already have, and
`--key slack` takes a throwaway one: each field comes from
`WEFT_NODE_TEST_SLACK_<FIELD>` if it is set, or a prompt, never from the
command line where other processes can read it. The grant is deleted
afterwards.

Every prompt happens before any credential exists and before any signal handler
is armed, so Ctrl+C at a prompt leaks nothing.

`--fixture` values, declared with `.with_fixture(...)` on a live test, are how
a test names the real thing it needs: a channel it may post in, an account it
may read. Read them with `rig.fixture("NAME")`.

## What to write

Ship a `fake` test for every node. If it talks to a provider, ship a `live` one
on the cheapest real path you can find.

A panic fails that test, carrying its message, and never takes the runner down.
