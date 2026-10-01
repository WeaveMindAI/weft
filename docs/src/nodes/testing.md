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

Basic and fake run right there with cargo: they need no Docker and no
runtime, and only the packages you targeted get built.

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
| A server it cannot reach at all | `rig.fail_connection(method, path)`: the call errors with no status and no body |
| Check what it sent | `rig.requests()`, `rig.assert_sent(method, path)` |
| What `ctx.run` recorded | `rig.recorded_steps(&node)`. Run the node twice on one rig and the second run replays the first's steps and the signals it was answered with, like a resumed execution, so a publish that must happen once is something you can prove: two runs, one request. Each node keeps its own, so two nodes on one rig never replay each other's |
| A connection | `rig.access("slack")`, `rig.connection_value(...)`, `rig.connection_permissions(...)` |
| A trigger's event | `rig.wake(payload)`, `rig.signal(payload)` |
| A live caller | `rig.attach_caller(conn)` |
| A bus going in | `rig.seed_bus(opts)`, giving you a writer and a marker |
| A bus it emitted | `rig.bus(&outcome.outputs["channel"])` |
| A stored file | `rig.store_file(filename, mime, bytes)` going in; `rig.stored_bytes(key)`, `rig.stored_meta(key)` and `rig.stored_files(scope)` for what the node stored; `rig.file_edits()` for every in-place change, with the file, the versions and the diff |
| An infra endpoint | `rig.declare_endpoint(name, url)`, `rig.answer_endpoint(...)`, `rig.refuse_endpoint(...)` |
| What it asked for | `rig.registered_signals()`, `rig.awaited_signals()`, `rig.logs()`, `rig.execution_tags()`, `rig.stops()` |
| A port's type, for a node that takes added ports | `rig.output_type(port, ty)`, `rig.input_type(port, ty)` |
| An output something downstream reads | `rig.wire_output(port)`. Nothing is wired unless you say so. On a node with `catchErrors`, `rig.wire_output("error")` makes a failure come out on `error`, as it would in a run |
| A server that is there and will not talk, for a node that opens its own socket | `rig.hang_up_server()`, a local port that hangs up on every connection. It is for raw socket clients (a database driver, a mail client): web calls in a fake test never reach the network, so for those use `rig.fail_connection`. If you want a refused connection, use this rather than a closed port: on some machines a closed port hangs until a timeout |

The outcome gives you `.ok()` for the happy path, `.failure()` for the error
string, `.output(port)` for one port, `.infra_spec()` for an infra run, and
`.outputs` for the map.

Note the last group: in a fake test nothing is really stopped and nothing is
really tagged. The rig records what your node **asked for**, and that is what
you assert on.

## Rig reference

If you need a rig call's exact signature, or the fields of what it hands back,
it is here.

### The fake rig

Setting the run up:

| Signature | What it does |
|---|---|
| `respond(&self, method: &str, path: &str, body: Value)` | Answers a matching request with `200` and `body` as JSON. `path` may carry a query; parameter order and encoding do not matter |
| `respond_status(&self, method: &str, path: &str, status: u16, body: Value)` | The same with your status |
| `respond_raw(&self, method: &str, path: &str, status: u16, content_type: &str, body: impl Into<Bytes>)` | Raw bytes with a content type (XML, CSV, a picture, an event stream) |
| `respond_with_headers(&self, method: &str, path: &str, status: u16, content_type: &str, headers: &[(&str, &str)], body: impl Into<Bytes>)` | The same plus response headers. The content type is its own argument, never a header here |
| `fail_connection(&self, method: &str, path: &str)` | A matching request fails before any response, like a server that is down |
| `signal(&self, payload: Value)` | Queues the answer to the node's next `ctx.await_signal`, in order |
| `wake(&self, payload: Value)` | The next run's `ctx.wake`: what a firing trigger received |
| `instance(&self, id: &str)` | Makes every run on the rig a run for that instance |
| `attach_caller(&self, conn: Arc<FakeCallerConnection>)` | Puts a scripted live caller on the runs, what a Route or Socket run carries |
| `seed_bus(&self, opts: BusOptions) -> WeftResult<(BusHandle, Value)>` | A bus you write into, and its marker to put on a `Bus` input |
| `access(&self, service: &str) -> Value` | A connection marker for an access input |
| `connection_value(&self, service: &str, name: &str, value: &str)` | A value the opened connection answers (`conn.value(name)`) |
| `connection_permissions(&self, service: &str, granted: &[&str])` | What the connection has granted, checked against what the input requires |
| `published_connection(&self, service: &str, values: &[(&str, &str)])` | Says the node already published this connection on an earlier run |
| `output_type(&self, port: &str, ty: WeftType)` | The type the compiler would give a port declared `MustOverride` |
| `input_type(&self, port: &str, ty: WeftType)` | Declares an added input port and its type |
| `wire_output(&self, port: &str)` | Wires an output downstream. Nothing is wired unless you say so |
| `hang_up_server(&self) -> u16` | A local port that hangs up on every connection, for raw socket clients |
| `declare_endpoint(&self, name: &str, url: &str)` | Where the node's own infrastructure endpoint `name` answers |
| `declare_public_url(&self, name: &str, url: &str)` | Makes a declared endpoint public at `url` |
| `answer_endpoint(&self, endpoint: &str, method: EndpointMethod, path: &str, answer: Value)` | The next call to that endpoint and path answers `answer` |
| `refuse_endpoint(&self, endpoint: &str, method: EndpointMethod, path: &str, status: u16, body: &str)` | The next call to that endpoint and path is refused |
| `answer_program_call(&self, name: &str, value: Value)` | What the next program call of that name answers (`weft.infra.status`, ...), in order |
| `store_file(&self, filename: &str, mime_type: &str, bytes: impl Into<Vec<u8>>) -> Value` | Stores a file in this run's scope and gives its value, for a file input |
| `store_file_in(&self, scope: &StorageScope, filename: &str, mime_type: &str, bytes: impl Into<Vec<u8>>) -> Value` | The same in another scope |

Running it:

| Signature | What it does |
|---|---|
| `run(&self, node: &dyn Node, inputs: Value) -> RunOutcome` | Runs the node's body with `inputs` (a JSON object of input name to value) |
| `run_setup_trigger(&self, node: &dyn Node, inputs: Value) -> RunOutcome` | Runs a trigger's setup |
| `run_provision_infra(&self, node: &dyn Node, inputs: Value) -> RunOutcome` | Runs an infra node's `provision_infra` |

Looking at what happened:

| Signature | What it gives you |
|---|---|
| `requests(&self) -> Vec<SentRequest>` | Every web request the node sent, in order, answered or not |
| `assert_sent(&self, method: &str, path: &str)` | Panics, listing what was sent, unless one request matched |
| `stored_bytes(&self, key: &str) -> WeftResult<Bytes>` | The content stored under a key |
| `stored_meta(&self, key: &str) -> WeftResult<StoredFileMeta>` | What storage records for it: `keep`, `keep_ttl_secs`, the size |
| `stored_files(&self, scope: &StorageScope) -> WeftResult<Vec<StoredFileMeta>>` | Every file stored in a scope |
| `file_edits(&self) -> Vec<FileEdit>` | Every change made to a stored file in place, in order: the file, the versions, the diff |
| `bus(&self, marker: &Value) -> WeftResult<BusHandle>` | The bus behind a marker the node emitted |
| `endpoint_calls(&self) -> Vec<EndpointCall>` | Every call to the node's own infrastructure, in order |
| `registered_signals(&self) -> Vec<(SignalSpec, Value)>` | What a trigger's setup registered |
| `awaited_signals(&self) -> Vec<SignalSpec>` | Every signal the node parked on |
| `recorded_steps(&self, node: &dyn Node) -> Vec<AwaitedEntry>` | Every `ctx.run` step and signal the node's run recorded |
| `logs(&self) -> Vec<(LogLevel, String)>` | Every `ctx.log` line |
| `execution_tags(&self) -> Vec<Vec<String>>` | The tags, one list per `ctx.tag_execution` |
| `stops(&self) -> Vec<(String, StopSelf)>` | Every `ctx.stop_tagged` asked for |
| `program_calls(&self) -> Vec<(ProgramCall, StopSelf)>` | Every program call made |
| `minted_tokens(&self) -> Vec<MintedToken>` | Every instance token minted: `instance`, `expires_in_secs`, `id` |
| `published_values(&self, service: &str) -> Option<BTreeMap<String, String>>` | What the node published as that service's connection |


### The live rig

| Signature | What it does |
|---|---|
| `access(&self, service: &str) -> Value` | The test's connection marker. The service must be the one the test declared |
| `fixture(&self, name: &str) -> WeftResult<String>` | A `--fixture` value, or a failure naming the variable to set |
| `store_file(&self, filename: &str, mime_type: &str, bytes: impl Into<Vec<u8>>) -> WeftResult<Value>` (async) | Stores a real file and gives its value |
| `connect(&self) -> WeftResult<OpenedConnection>` (async) | Opens the test's connection, for setup and cleanup calls of your own around the node |
| `bus(&self, opts: BusOptions) -> WeftResult<(BusHandle, Value)>` | A real bus you write into, and its marker |
| `wire_output(&self, port: &str)` | Wires an output downstream |
| `run(&self, node: &dyn Node, inputs: Value) -> RunOutcome` (async) | Runs the node against the real runtime |

`LiveRig::new(..)` is built by the runner, never by a test.

### What comes back

`RunOutcome`, from every `run`:

| Field or method | What it is |
|---|---|
| `result: WeftResult<()>` | The body's own result |
| `outputs: serde_json::Map<String, Value>` | What each port emitted |
| `closed_ports: Vec<String>` | Ports the body closed with `ctx.close_port` |
| `infra_spec: Option<InfraSpec>` | The spec, on a `run_provision_infra` outcome |
| `ok(self) -> WeftResult<Self>` | The outcome, or the body's failure |
| `failure(self) -> WeftResult<String>` | The failure's message, or an error when the run succeeded |
| `output(&self, port: &str) -> WeftResult<&Value>` | One port's value, or an error naming the ports that did emit |
| `infra_spec(&self) -> WeftResult<&InfraSpec>` | The spec, or an error when this outcome has none |

`SentRequest`, one per request:

| Field or method | What it is |
|---|---|
| `method: String` | Uppercase, `"POST"` |
| `path: String` | The path, no query |
| `query: Option<String>` | The raw query string |
| `body_text: Option<String>` | The body as text, when it was sent whole |
| `body: Option<Value>` | The body parsed as JSON, when it parses |
| `body_streamed: bool` | True when the body was streamed, which the rig cannot read: `body` and `body_text` are then `None` even though there was a body |
| `headers: Vec<(String, String)>` | The headers, names lowercased |
| `header(&self, name: &str) -> Option<&str>` | One header's first value, any case |

`EndpointCall`, one per call to the node's own infrastructure:

| Field | What it is |
|---|---|
| `endpoint: String` | The endpoint's name |
| `method: EndpointMethod` | `Get` or `Post` |
| `path: String` | The path called |
| `body: Option<Value>` | The body sent |

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
