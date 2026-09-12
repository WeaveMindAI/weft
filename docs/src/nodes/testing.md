# Testing a node

You can test a node's Rust body without building a graph or starting the
weft runtime. Give it inputs, run it with a test context and check what it
emits. The tests live beside the implementation, so the person changing the
node can check that job on its own.

## Test the word counter

For the `WordCount` from [Your first node](your-first-node.md), add this
declaration to `nodes/word_count/mod.rs`, outside the trait implementation:

```rust
#[cfg(feature = "node-tests")]
mod tests;
```

Inside `impl Node for WordCountNode`, add:

```rust
#[cfg(feature = "node-tests")]
fn tests(&self) -> Vec<weft::NodeTest> {
    tests::tests()
}
```

Keep the existing `run` method. The feature guard includes these tests in
the test build and leaves them out of the program's normal build.

Create `nodes/word_count/tests.rs`:

```rust
use serde_json::json;
use weft::{FakeRig, NodeTest, WeftResult};
use super::WordCountNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("counts_whitespace_separated_words", counts_words)]
}

async fn counts_words(rig: FakeRig) -> WeftResult<()> {
    for (text, expected) in [
        ("hello from weft", 3),
        ("  \n\t", 0),
        ("one,two  three", 2),
    ] {
        let outcome = rig
            .run(&WordCountNode, json!({ "text": text }))
            .await
            .ok()?;
        assert_eq!(outcome.outputs["count"], json!(expected), "input: {text:?}");
    }
    Ok(())
}
```

The cases pin down what “word” means: blank text has no words, repeated
whitespace does not add words, and a comma does not split one.

From the project root, run:

```bash
weft test-node WordCount --test counts_whitespace_separated_words
```

This compiles the test on your machine, so you need a Rust toolchain.
It does not need the daemon. After changing the node, run all its tests
with `weft test-node WordCount`.

## Choose what the test exercises

| Tier | Use it for | What runs |
|---|---|---|
| `basic` | A parser or other helper with no context | A Rust function and its assertions |
| `fake` | The node body, including the requests it builds | The body with canned responses and in-memory services |
| `live` | Checking the integration against a real provider | The body through the runtime's connection path |

A basic test looks like
`NodeTest::basic("parses_the_answer", || { /* assertions */ Ok(()) })`.
Fake tests receive a `FakeRig`, as the word counter does. A live test
declares the service it needs:
`NodeTest::live("sends_a_message", "telegram", sends_a_message)`.
Its function receives a `LiveRig`.

An assertion panic fails that test and appears in the report.
Use assertions that catch the mistake you care about. A fake provider
response can prove your parser handled that response; a live call checks
whether the provider still speaks the protocol you expect.

## Fake provider calls and events

Before running a node that calls a service, register a response with the rig:

```rust
rig.respond("POST", "/api/send", json!({ "ok": true, "id": "m1" }));
```

Give the node `rig.access("myservice")` on its connection input. Calls
through the context's HTTP clients are answered by the fake routes;
unmatched requests fail. You can inspect the request with
`rig.requests()` or assert that it happened with
`rig.assert_sent("POST", "/api/send")`.

The fake clients do not reach the real network. Arbitrary Rust that creates
its own client is outside that interception, which is another reason to
use the framework's clients in your node.

| If the test needs… | Use |
|---|---|
| An HTTP error response | `rig.respond_status(method, path, status, body)` |
| A non-JSON body | `rig.respond_raw(method, path, status, content_type, bytes)` |
| Stored connection values | `rig.connection_value(service, name, value)` |
| Known granted scopes | `rig.connection_permissions(service, granted)` |
| A durable wait's next payload | `rig.signal(payload)` |
| An event delivered to a trigger's body | `rig.wake(payload)` |
| The trigger registration method | `rig.run_setup_trigger(node, inputs)` |
| An infrastructure declaration | `rig.run_provision_infra(node, inputs)` |
| A file input | `rig.store_file(filename, mime, bytes)` |
| A type normally resolved from the graph | `rig.output_type(port, type)` |

Response matching tries the path with its query string first, then the
bare path. The rig does not simulate redirects: register the final response
directly. Declaring a 3xx response panics.

A `Generator[T]` input takes a JSON array; the rig presents its
items as an already-finished stream.

You can inspect registrations with `rig.registered_signals()`, log entries
with `rig.logs()`, and steering requests with `rig.execution_tags()` and
`rig.stops()`. A fake stop records the request; it has no other executions
to stop.

Storage lives in memory. `ctx.run` executes its closure fresh because this
rig has no durable journal. To test recovery or interactions between nodes,
use a runtime test for that mechanism. For the distinction and repository
test commands, read
[Contributing](https://github.com/WeavemindAI/weft/blob/mvp/CONTRIBUTING.md#tests).

## Run a live test deliberately

Live tests use a real account and can create resources or incur provider
charges. Use a test account and small inputs, and clean up the resources
your test creates. If a message or email is the intended result, choose a
recipient who expects it.

A live test uses `rig.access(service)` and `rig.run` like the fake test.
Use `rig.connect()` when the test itself needs a connection for setup or
cleanup. Assertions should check the behavior you need without depending
on an exact response that the provider is free to vary.

To supply the account:

- Connect it in the editor. The runner can use that stored connection.
  If there are several, select one with
  `--connection service=grant-id`. A bare grant ID also works when the
  selected tests need exactly one service.
- For a key-based service, pass `--key service` to enter a temporary key.
  The runner deletes the temporary connection afterwards. If cleanup fails,
  it prints the command to remove it.
- For an unattended run, set the credential fields as
  `WEFT_NODE_TEST_<SERVICE>_<FIELD>`, such as `WEFT_NODE_TEST_EXA_KEY`.
  This also uses a temporary connection.

The CLI loads the nearest `.env` while walking up from the working
directory. Keep files containing credentials out of Git.

### Giving the live tier what it needs

A live test may need something it cannot create, such as a destination chat.
Declare that fixture on the test:

```rust
NodeTest::live("one_real_send", "telegram", live_send)
    .with_fixture(weft::fixture_spec(
        "TELEGRAM_CHAT_ID",
        "Chat id",
        "The chat the test sends into.",
    ))
```

Read it inside the test with `rig.fixture("TELEGRAM_CHAT_ID")?`.
Supply it as `WEFT_NODE_TEST_TELEGRAM_CHAT_ID`. The CLI checks required
fixtures before launching the live tests and reports missing values
together.

If the fixture should use the same picker as a node input,
`fixture_spec_like(manifest, input_name, fixture_name)` copies that input's
declaration.

## Select the tests to run

```bash
weft test-node WordCount
weft test-node slack
weft test-node SlackSendMessage --test posts_to_a_channel_and_emits_the_permalink
weft test-node web --tier live
weft test-node web --tier fake --tier live
```

Without `--tier`, the runner selects basic and fake. Supplying `--tier`
replaces that default, so `--tier live` runs only live tests. Repeat the
option when you want several tiers.

Live runs ask for confirmation unless you have saved the preference to
skip it or pass `--yes` for that invocation. Test bodies should get their
inputs from fixtures or setup code; they should not prompt the person
running them.

Use `--parallel N` to limit concurrent tests to `N`. Bare `--parallel`
removes the concurrency limit. Local packages still run one at a time;
tests within the package can run together. Reports keep declaration order.
If you omit the target entirely, the command selects all test-declaring
packages in the project's catalog.
