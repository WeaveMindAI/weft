# The ctx

`ctx` is everything your node's code can reach: its inputs, and every service
weft provides. The runtime hands it to `run`, and to a trigger's
`setup_trigger`.

Every call here returns `WeftResult`, and your node never names a weft error
type. Wrap an outside error with `.node_err("what you were doing")`, and fail
on something you detected yourself with `node_bail!`.

## Six rules that save you a debugging session

**Calls on a connection go through `ctx.open` or `ctx.client`.** That is what
signs the request and records what it cost. `ctx.http()` is for everything
else. A `reqwest` client you built yourself skips both.

**If you want to send something on before a long wait, send it, then wait.**
A resume replays the body from the top, and a port this firing already sent on
keeps its first value: the replayed emit does nothing, the way a finished
`ctx.run` hands back its recorded value. Streams are the exception. Once you
yielded on or closed a `Generator` output, `ctx.await_signal` is refused,
because those items went out live and cannot be sent again. It is also refused
on a node with a `Generator` input, because a stream cannot be replayed.

**`ctx.run` keys on call order, not on the name you gave it.** The name is for
reading logs. If your body can call it a different number of times on a replay,
wrap the thing that varies.

**`put` with no keep policy is run-scoped**, and the file is swept shortly
after the run ends. Right for scratch, wrong for anything a person will open
later.

**`ctx.stop_tagged` returns when the stop is queued, not when the other runs
are gone.** Do not write a node whose next step assumes they stopped.

**`ctx.register_signal` is once per node per setup.** A second call is a loud
error rather than a trigger that quietly never fires.

## Which firing am I

| Field | Type | What it is |
|---|---|---|
| `run_id` | `RunId` (a UUID) | This run's id |
| `project_id` | `Uuid` | The project it belongs to |
| `node_id` | `String` | This node's id in the graph |
| `node_type` | `String` | Its catalog type, such as `ExecPython` |
| `node_label` | `Option<String>` | The title shown on the box, if it has one |
| `frames` | `LoopFrames` | Which loop iterations this firing sits inside |
| `instance()` | `Option<&InstanceId>` | Which instance this run is for, when it is for one instance of the program (see [programs with instances](../running/instances.md)) |

## Reading inputs

`ctx.inputs` holds this firing's inputs, whether they arrived on a wire, were
written into the body, or came from a declared default. `ctx.wake` holds the
event payload, and only on the trigger that actually fired. Both are a
`ValueBag` with the same methods.

| Call | What you get | It fails when |
|---|---|---|
| `get::<T>("name")` | A required value | It is absent, or it does not fit `T` |
| `opt::<T>("name")` | `Option<T>`; absent or null gives `None` | A value is there and does not fit `T` |
| `get_or::<T>("name", default)` | The value, or your default | A value is there and does not fit `T` |
| `list::<T>("name")` | `Vec<T>`; absent gives `[]`, and one value gives a one-item list | Any item does not fit `T` |
| `access("name")` | The connection picked on an access input | Nobody picked one and the service requires it |
| `raw("name")` | `Option<&Value>`, the JSON as it arrived | never |
| `object()` / `record()` | The whole bag as one record | On a wake bag whose event was missing or not an object |
| `nested("name")` | The object under `name`, as its own bag | The value is there and is not an object |
| `iter()` | Every named value | never |
| `custom()` | Only the ports this node added in the program, not the type's own settings | never |
| `declared()` | Only the type's own declared settings | never |
| `in_order()` | Every input that arrived, in source order | On a wake or nested bag |
| `declares("name")` | Whether the node declares this input at all | never |
| `for_holes(holes, what, spell, declare)` | Values matched to named holes in a text, such as `$userId` in SQL | A hole names no port, or a wired port the text never reads |

Never write `get(...).unwrap_or(...)`. It turns a real type error into your
default and you will not find out. `get_or` is the same shape and fails
honestly.

## Emitting

| Call | What it does | It fails when |
|---|---|---|
| `pulse_downstream(output).await` | Sends values on | A port is mentioned twice, is not declared, gets a value its type does not allow or one over 100 KB, or is `_error` |
| `yield_downstream(output).await` | The same, but waits until the value was taken | Same, plus a delivery that can never happen, and an empty output |
| `close_port("port").await` | Closes one output. On a stream port this is the end of the stream | The port was already mentioned this firing, or is `_error` |
| `set_max_buffered_items("port", n)` | Raises the un-taken cap on a stream port above 4096 | The port is not a stream output, or `n` is 0 |
| `fan_declared(&value)` | Builds an output by matching an object's keys to your declared ports, skipping the rest. It never fills `_error`: a key called `_error` in the data is data | never |
| `data_outputs()` | Your declared ports minus `_error`: the ones a node fills from data (a row's columns, a response's fields) | never |
| `output_type("port")?` | The resolved type of one output | The node does not declare that output: a type error, so it fails the run even with `_error` wired |
| `declared_outputs()` / `declared_inputs()` | Every port this node declares in the program, with its type | never |
| `is_output_wired("port")` | Whether anything downstream reads that output in this run | never |

An ordinary port emits at most once per firing. Whatever you never mention is
closed for you when the body returns. A task you spawned with a clone of `ctx`
can emit only while the body is running: an emit from it after the body
returned, or after it paused on `ctx.await_signal`, fails with an error naming
the node and the port. A `Generator[T]` port emits as often as
you like until you close it.

`NodeOutput` is the payload you hand those calls:

| Call | What it does |
|---|---|
| `NodeOutput::new()` | An empty one |
| `.set("port", value)` | Sets one port |
| `.set_serialized("port", &value)?` | Sets one port to any `Serialize` value (a struct, a list of them). Fails naming the port when the value has no JSON form |
| `.extend_from_object(&value)` | Fans every top-level key onto a same-named port. Last write wins |
| `NodeOutput::stored_file(stored)` | The four ports a stored file travels as: `file`, `filename`, `mimeType`, `sizeBytes` |
| `.get("port")` | Reads back what you already set |

## Waiting, and surviving a restart

| Call | What it does | It fails when |
|---|---|---|
| `await_signal(kind).await` | Parks this firing until the signal fires, releasing the worker. Returns the payload | The body already yielded on or closed a stream output, or the node has a stream input, or the body's call order changed between replays |
| `await_signal_with_cancel(kind, call).await` | The same, and if the run is cancelled while it waits, weft sends `call` once: a `CancelCall`, usually to the provider's cancel address for the job | Same, plus a `call` whose address is empty or not `http` or `https` |
| `register_signal(kind).await` | Sets up a trigger that starts a new run on every fire. Called during setup | Called twice for one node in one setup |
| `run("name", closure).await` | Runs the closure and writes down its result, or gives back what was written down last time | The closure itself fails |

`ctx.run` is how you stop a replay from doing an expensive thing twice. A body
replays only after a wait. If the worker dies while the body is running, the
node is not replayed: a durable run fails it, and a fast run ends cancelled
(go and read [surviving a restart](durable-execution.md)).

If your node waits on a job a provider runs, go and read [waiting on a job a
service runs](durable-execution.md#waiting-on-a-job-a-service-runs), which
also covers the cancel call.

## Connections

| Call | What you get | It fails when |
|---|---|---|
| `open(&access).await` | The connection, leased for this firing, with its token refreshed | It needs reconnecting, the service does not match, or the door refuses |
| `open_within(&access, window).await` | The same, with a window you choose instead of the default 15 minutes | Same |
| `client(access).await` | Straight to the signed-in HTTP client. Takes `None` and gives you a plain one | Same, when it is `Some` |
| `http()` | The shared plain HTTP client, no credentials | never |
| `publish_access(values).await` | Publishes a connection to something this node runs itself, such as a database it provisioned | A field the service does not declare, or a required one missing |
| `published_access().await` | What this node published before, or `None` | The broker cannot answer |
| `endpoint("name").await` | A handle on one of this node's declared infra endpoints, once something answers there | The endpoint is not declared, or the infra is not running |

On an opened connection: `client()`, `credential()`, `value(name)`,
`identity()`, `owner()`. On an endpoint handle: `url()`,
`host_and_port()`, and `call(method, path, body)`.

## Secrets

If your node hands out a value the run's record must never show, like a
session key a voice app gets for a few minutes, make it a secret. What flows
on the wire is a marker naming it. For what the record keeps and for how
long, go and read [secrets](../language/types.md#secrets).

```rust
let session = ctx.secret(&key)?;
ctx.pulse_downstream(NodeOutput::new().set("session", session)).await
```

Declare that output as `Secret[String]` in your `metadata.json`. A node that
needs the value declares a `Secret[String]` input and asks for it:

```rust
let session: weft::secret::Secret = ctx.inputs.get("session")?;
let key: String = ctx.reveal(&session).await?;
```

| Call | What you get | It fails when |
|---|---|---|
| `secret(&value)` | A `Secret` to put on an output typed `Secret[T]` | The value is not JSON |
| `reveal::<T>(&secret).await` | The value, as `T` | The record of the run that made it is gone, or the value is not a `T` |
| `reveal_input(port, value).await` | The value of your input `port` with every secret its declared type says it holds revealed, for handing a whole value outside weft (a script, a request, a caller) | As `reveal_in`, naming the input |
| `reveal_in(value, &ty).await` | The same for a value of type `ty` that is no input of yours | A secret sits where `ty` declares none, a place `ty` says is a secret holds a plain value, or the record of the run that made one is gone |
| `secret_in(value, &ty)` | `value` with every plain value at a place `ty` says is a secret made one, for a node whose code hands back plain values | never |

A secret this run made, or revealed already, comes back from the worker's
memory. Any other comes from its sealed copy, which any run of the same
project can read: the same run picked up after a pause, a seeded run, a
marker your program stored somewhere and reads back later. A run with
`recorded` off writes nothing down, so only that run reveals its secrets.

If a value holds a secret its type does not declare (a token inside a
`JsonDict`), `reveal_in` and `reveal_input` fail rather than hand it on as
its marker, saying where the secret is and that the program should declare
the type, like `{ token: Secret[String] }`. If you emit a revealed value on a
plain output, the record holds it like any other value. For which types fit a secret, go and read [what fits into
what](../language/types.md#what-fits-into-what).

## Sharing something between runs

If your node builds something on every run that is slow to build (a pool of database
connections, a process to hand work to), let the worker's runs share one
instead:

```rust
let pool = ctx.shared(&access, |opened| async move { Pool::new(&opened) }).await?;
```

The closure is async and its result is a `WeftResult` of the thing (`Pool`
here is your own type, whose `new` returns a `WeftResult<Pool>`).

The first argument is the key weft keeps the thing under. If you pass a
connection (`&access`), the closure gets the opened connection, and once one
of its values changes (a new password), the next run that asks gets your
thing built again from the new values. To notice the change, weft opens the
connection every time a run calls `ctx.shared`. For a connection a person made or an infra node
published, the worker answers that open from memory, so a reused thing costs
no extra call. A connection set to **Use ours** (weft's own key) costs one
network call to weft each time.

If you pass a name of your own instead, the closure gets `()`:

```rust
let python = ctx.shared("exec_python", |()| async { Ok(Interpreters::new()) }).await?;
```

On one worker, every node that asks under the same name for the same Rust
type gets the same thing, so pick a name that is your node's own, the way
`ExecPython` uses `"exec_python"`. A worker serves one project, so nothing
another project builds can collide with yours.

`ctx.shared` hands back a `weft::shared::SharedHandle<T>`, which derefs to the
thing. If two runs ask at the same moment, it is built only once. A build that
fails keeps nothing, so the next run tries again.

If the thing can only serve so many runs at once, say so with `with_limit`:

```rust
let pool = ctx.shared(&access, |opened| async move { Pool::new(&opened) }).with_limit(100).await?;
```

Then at most that many handles to it exist at once on one worker, and the
next run that calls `ctx.shared` waits until one is dropped. A run that asks again while it still
holds a handle can end up waiting on itself once the limit is reached, so
keep one handle per run. Only the limit of the first call that asks counts, until weft lets the thing go (after it goes idle, or after its
connection changes), so pass the same limit everywhere you call it. The limit counts the handles to one thing on one worker: two workers can go
past it between them, and so can the old thing and the new one for a moment
after a connection's values change, while runs still hold handles to the old
one. The Postgres nodes that query through the
shared pool ask for 20.

`with_limit` only caps what one worker takes, so that its runs never leave
its sibling workers without a connection. It cannot keep a database
under its connection limit, because it cannot see how many workers there are. If your node talks to something that only
serves so much (a database with a fixed number of connections, an API with a
rate limit), assume any number of workers run your node at once: Cloud Run
starts copies up to the project's `--max-instances` (set with `weft workers
set`), and a change of program
or worker settings runs the old copies beside the new ones for a while. Two
things follow:

- **Hold nothing you are not using.** Close an idle connection within
  seconds, because one a copy keeps between requests is one no other copy can
  have. On Cloud Run a copy has no CPU between requests (unless the
  project turned on `weft workers set --cpu-always-allocated true`), so a timer of yours that closes it may not
  fire for minutes: check how long a connection sat idle when a run takes it,
  too. The Postgres nodes close a pooled connection after 5 idle seconds and
  never hand out one idle for longer.
- **Let the other end do the counting.** It is the one place that sees every
  copy, so that is where a client past the limit should wait its turn, or be
  refused with a clear answer. Your node then waits, or fails with a message
  that says why. A PostgresDatabase does this with a pooler in front of
  Postgres, where a client that finds every connection busy waits for one,
  and Postgres closes a session that sat idle for a minute. If your node
  keeps a connection idle on purpose (one that `LISTEN`s), lift that limit
  for its own session when it connects, the way PostgresWatchQuery sends
  `SELECT set_config('idle_session_timeout', '0', false) FROM pg_settings
  WHERE name = 'idle_session_timeout'` (the `FROM pg_settings` part makes it
  do nothing on a Postgres that has no such setting, instead of failing).

If a task you spawn needs the thing after your run returns (handing a
connection back to its pool later), move the handle into the task: it keeps
its place under `with_limit` until the task drops it, which is what the
Postgres nodes do. If your task should not count against the limit, take `handle.arc()`
instead: the `Arc` keeps the value alive but holds no place under
`with_limit`, and weft can let go of the thing while you hold it, so the next
run may build a new one beside yours.

By default, weft lets go of a shared thing about five minutes after the last
run asked for it or dropped its handle, once no handle is left. Your
thing's `Drop` runs once weft has let go of it and the last handle and `Arc`
are gone, which is when a pool closes its connections. When the worker
stops, it lets go of every shared thing as soon as its last run has ended,
used recently or not, so a pool's connections go back to the database while
the worker is still writing its last records.

If you want a quiet stretch not to cost a rebuild, raise the five minutes
with `weft workers set --shared-idle-seconds <n>` (or lower it to let go
sooner). It is one setting for the whole project, covering every
shared thing on its workers. The change applies from the next run: weft starts workers with the
new value (a new revision on Cloud Run), and the old ones finish what they
are running and stop.

## Storage

`ctx.storage(scope)` gives you a handle. The scope decides where new files go
and what `list` sees; `get`, `delete`, `keep` and the links act on whatever
scope the key itself belongs to, so a later node can read a file without
knowing where it came from.

For every scope, go and read [Storage](storage.md).

| Call | What it does |
|---|---|
| `identified(id)` | Names what the next put is a copy of, so storing it twice stores it once |
| `link_lifetime(duration)` | Makes every link to the files this handle stores live that long instead of 15 minutes (at most seven days). The value carries it as `linkSecs` |
| `put(bytes, mime, filename, keep)` | Stores bytes and gives back the stored-file value |
| `put_stream(stream, mime, filename, keep)` | The same without holding the file in memory |
| `put_response(resp, what, mime, filename, keep)` | Streams an HTTP response you already made into storage |
| `put_from_url(url, filename, keep)` | Fetches a URL straight into storage |
| `copy(&file, keep)` | Copies a file into this handle's scope, leaving the original |
| `edit(&file, \|old\| ...)` | Changes the file's content in place: reads it, runs your function on the bytes, writes back what it returns. Another write of the same file is never lost (it reads again and reruns the function), so your function must use nothing but the bytes it is given. Same key, name, type and lifetime, new size and version |
| `replace(&file, bytes)` | Overwrites the file's content in place, whatever it holds now |
| `get(&file)` | The file's bytes, as a stream |
| `get_range(&file, range)` | Part of the file |
| `get_bytes(&file)` | The whole file in memory. Only for small ones |
| `delete(&file)` | Deletes it. Stored files only, not URL-backed ones |
| `list()` | Everything under this scope |
| `keep(&file, ttl)` | Sets how long the file lives from now on, and saves a run-scoped one from the sweep (that part cannot be undone) |
| `internal_link(&file)` | A `FileLink { url, public }` for code inside the install (a script your node runs), straight to storage |
| `external_link(&file)` | A `FileLink { url, public }` for anything outside the install. `public: false` means only this machine (or its network) opens it, so an internet service needs the bytes instead |
| `external_file(&file)` | The external link when it is public, else a `data:` URL of the bytes, with the file's mime type and filename |
| `upload_slot(max_bytes, accepts, minutes, keep)` | A `weft-upload:...` string a page's visitor uploads one file through, with `uploadFile` from `@weft/connect` |
| `claim_upload(slot, &ty)`, `claim_uploads(&[(slot, &ty)])` | The file uploaded through each slot, received once into a place of type `ty`. Several slots are received together or not at all, which is what a Route does for the file fields of one call |
| `externalize(&value, &ty, policy)` | Turns every file inside a typed value into a link or inline bytes, for handing out |
| `internalize(&value, &ty, keep)` | The reverse: pulls URLs and inline data into stored files |

`keep` takes `KeepTtl::Default` (30 days, pushed back each time the file is
read), `Secs { secs }`, or `Never`, in every scope but `Asset`. For what it
means in each scope, go and read [how long a file lives](storage.md#how-long-a-file-lives).

What comes out of `externalize` is no longer a value of that type, because the
links expire. Hand it to whoever asked and do not store it.

## Streams and buses

| Call | What it does | It fails when |
|---|---|---|
| `journal_policy()` | What your node's users set on `journalEphemeral` and `journalWindowMs`, as the `journal` option of a bus. Only a node with `features.journalSettings` (or a live-connection trigger) has them; for any other node it is the default | A setting is out of range or the wrong shape |
| `create_bus(opts)` | Makes a bus and gives you a handle and a marker to emit. The bus ends when your body does | The options do not make sense, or `opts.journal` has a window outside 100 to 60000 ms |
| `bus(&marker)` | Turns a marker value back into a live handle | The bus is gone, or that is not a marker |
| `open_bus("port", opts, "name").await` | The whole producer move: make it, emit the marker, register your name. The bus ends when your body does | Emitting or registering fails |
| `pending_bus("port", opts, "name")` | A bus that opens only on your first `open().await`, so the work that can fail runs first and its failure can still reach `_error` | never; `open()` fails as `open_bus` does |
| `join_bus("port", "name")` | The consumer twin: registers your name, and the bus ends when your body does | The input is not a live bus |
| `bus_from_input("port")` | A handle whose bus does **not** end with your body. For an observer | Same |

For how a bus ends and what a reader does about it, go and read [streams and
buses in Rust](streams-and-buses.md#how-a-bus-ends).

Read a stream input like any other value: `ctx.inputs.get::<Generator<Row>>("rows")?`.

| Call | What it does |
|---|---|
| `next().await` | The next item, waiting. `None` once, at a clean end |
| `try_next()` | `Item`, `Empty` or `Finished`, without waiting |
| `drain().await` | Everything, waiting for the end. A failed stream errors instead of returning a partial list |
| `end()` | Whether the producer finished or failed, or `None` while it is open |

## Live callers

| Call | What you get |
|---|---|
| `is_api_call()` / `is_websocket()` | Whether a caller of that kind is attached |
| `caller_data_type()` | Whether this connection speaks bytes or JSON |
| `caller()` | The caller as a handle, or `None` |
| `http_caller().await` | The HTTP caller, attached and connected, or a loud error |
| `ws_caller().await` | The same for a WebSocket |
| `live_caller().await` | Whichever one is connected |
| `caller_request()` | What the caller sent to open the exchange, without waiting for the connection |

## Logging, tags, cancelling

| Call | What it does | It fails when |
|---|---|---|
| `log(level, message).await` | Writes a line into the run's log. `Trace`, `Debug`, `Info`, `Warn`, `Error` | The write fails |
| `tag_run(["user_7"]).await` | Tags this run. Additive and safe to repeat. Any string works: one that is not already a tag is turned into one (see below) | The list is empty, or a tag is empty |
| `stop_tagged("user_7", StopSelf::Keep).await` | Queues a stop for every live run of this project carrying that tag, turned into a tag the same way | The tag is empty |
| `is_cancelled()` | Whether this run was cancelled. Cheap enough to poll | never |
| `cancellation()` | The flag itself, to `select!` against your own work | never |

`StopSelf::Keep` only reaches runs that took the tag before this one did, so
two runs racing to stop each other leave the later one alive. `StopSelf::Include`
ends this run too.

## Your project, and the owner's other projects

A node can do from code what a person does with the CLI: build and run a
project, write its files, turn its triggers on, start its infra, read its runs.
The runtime carries each call out on behalf of this run, with the same
parameters and the same refusals the CLI gets, and writes the answer into the
run's journal, so a replay reads the answer back instead of asking again.

The calls below act on this run's own project. If you want the same call on
another project of the same owner, put `ctx.project(id)` in front of it, `id`
being the project's id (the one in its `weft.toml`):
`ctx.project(shop_id).infra("bridge").start().await`. A project of another
owner is answered as if it did not exist. `ctx.projects().list()` lists the
owner's projects. If you want a new, empty one, call
`ctx.projects().declare("shop")` and take its id from the answer (weft makes
the id inside the call, so a replay gets the same id back), then write its
files: at least a `weft.toml` naming it and carrying that id, and
`nodes/base_catalog/` too if it uses the standard library, since a build only
sees the files the project holds. If you want to copy files the owner
already holds, such as the standard library of this run's own project, read
their hashes with `state()` and name each one with `edit().blob(path, hash)`:
nothing is downloaded or stored again, so the copy is one read and one write
however many files it holds.

```rust
let shop = ctx.projects().declare("shop").await?;
let shop_id: uuid::Uuid = shop.id.parse().node_err("read the new project's id")?;
let weft_toml = format!("[package]\nname = \"shop\"\nid = \"{shop_id}\"\n");
let source = "# your program here\n";
let own = ctx.code().state().await?;
let mut edit = ctx.project(shop_id).code().edit()
    .text("weft.toml", weft_toml)
    .text("src/main.weft", source);
for (path, hash) in own.working.iter().filter(|(path, _)| path.starts_with("nodes/base_catalog/")) {
    edit = edit.blob(path.clone(), hash)?;
}
edit.apply().await?;
```

Three things a program cannot do to its own project, each refused with the
reason:

| Refused on its own project | Why |
|---|---|
| Starting a run (`ctx.code().run()...start()`) | Every run of it would start the next, with nothing to end the chain. It is refused before anything is recorded as a version or built. Start it from another project, or reach the part you want with a trigger |
| `resync` | The resync waits for the runs on the old version to end, and this run is one of them, so it would wait on itself |
| `remove` | The removal cancels every run of the project, this one among them, before it finishes |

If you want to hand out a token, mint an instance token with
`tokens().mint_for_instance`, which hands the value to the run alone. Only a
person can mint a signal token or a frontend's new token, because its value
would sit in the journal.

### The project's infra, triggers, instances and settings

Most of these take an optional `.instance(id)`, which picks that instance's
copy instead of the shared one; `values()`, `connections()` and
`tokens().instance(..)` always name an instance.

| Call | What it does |
|---|---|
| `infra("bridge").start().await` | Brings the shared copy up. Returns once it runs, parking the run between looks; fails with the reason if it does not come up, or if somebody stops it while this waits |
| `infra("bridge").request_start().await` | Asks for the copy to come up and returns once weft has accepted, without waiting for it to run. If a route should answer its caller at once (a 202), call this and let a later `status()` say whether the copy came up; until then it reads `provisioning` |
| `infra("bridge").stop(take_down, stop_self).await` | Stops the copy, keeping its disks (a unit whose `onStop` keeps it running stays up until terminate). It returns once the stop is under way, and `status()` reads `stopping` until the copy is down. Answers an `InfraDownAnswer`: `TakenDown`; `AlreadyDown` when the copy was already stopped or stopping; or `NoCopy` (a copy never started, a mistyped instance id, or a copy terminated earlier) |
| `infra("bridge").terminate(take_down, stop_self).await` | Deletes the copy and its disks, except the ones the node lists in `keep_on_terminate`. Answers as `stop` does, except that `AlreadyDown` means it was already being terminated |
| `infra("bridge").instance(id).wipe(take_down, stop_self).await` | Deletes the instance's copy and every one of its disks, the kept ones too, even if the copy was already terminated. Always answers `TakenDown` |
| `infra("bridge").upgrade(take_down, stop_self).await` | Moves a running copy onto the last build weft made (a stop, then a start; nothing is built). A copy already on that build, stopped or missing is left as it is. It returns once the upgrade is under way; its `command_ids` hold one command per owner (none when nothing was upgraded), and `all_infra().follow_command(id)` waits for one to end |
| `infra("bridge").status().await` | The copy's state, `None` when there is none. A start or stop on its way reads `provisioning` or `stopping` at once, the same answer `weft status` gives |
| `infra("bridge").copies().await` | Every copy: the shared one and each instance's |
| `infra("bridge").logs(tail)`, `.show()`, `.press(kind, payload)`, `.rebake()` | `weft infra logs`, `show`, `press` and `rebake` for that node |
| `all_infra().cancel(instance)`, `.look()`, `.doors()`, `.logs(tail)`, `.command(id)` | What reaches every copy at once: `weft infra cancel`, `look`, `list-doors` and `logs`, and where an upgrade's command stands |
| `trigger("receive").activate(held).await` | Turns one trigger on if it is off. While it was down it may have held work that arrived: `ReleaseHeld::Run` runs that work, at half the trigger's own limits so new arrivals still get through, and `ReleaseHeld::Drop` deletes it and cancels the runs paused on it |
| `triggers().deactivate(take_down, stop_self).await` | Turns off every shared trigger of the program, or an instance's triggers with `.instance(id)` (chain it again for each further instance), or every instance's with `.all_instances()`, which you cannot combine with `.instance(..)`. If you want only some triggers, name them with `.only([..])`. It returns once they are down, without waiting for the runs they started, even with `Running::Wait` |
| `triggers().bake()`, `.resync(take_down)`, `.cancel_activate()` | `weft bake`, `weft resync` and `weft cancel-activate`. A resync returns once it has ended, parking the run between looks |
| `status()`, `cancel_build()`, `remove()` | `weft status`, `weft cancel-build` and `weft rm` |
| `values().instance(id).get().await` | The instance's values for the program's `@instance_filled` fields, by node and field |
| `values().instance(id).set(node, field, value).clear(node, field).apply().await` | Sets and clears values in one change, all or none, each checked against its field and its node's rules first. Any live trigger of the instance that reads one is set up again, and `apply` returns those triggers' names |
| `values().instance(id).forget().await` | Forgets every value the instance was given |
| `connections().instance(id).list() / forget()` | Lists an instance's connections, or forgets all of them (the values naming them go too) |
| `instances().list().await` | Every instance weft holds anything for, one entry each: how many values, connections and live tokens, its infra copies, and its triggers, each with the events waiting on a field not yet filled and why |
| `costs().instance(id).service(s).since(t).list().await` | The cost records, each saying whose credential paid, narrowed by instance, node, service, run, `paid_by` or `since` (unix seconds). A run's costs are deleted whenever the run is, whether its keep time ran out or something cleaned it (for how long runs are kept, go and read [how long a run is kept](../running/the-journal.md#how-long-a-run-is-kept)) |
| `runs().instance(id).status(s)?.older_than(d).clean(running, stop_self).await` | Deletes the matching runs and answers how many it deleted, cancelled and left running. If a run is still going, `StillGoing::Leave` leaves it alone and `StillGoing::Cancel` cancels it (its rows go at the next clean). This run is never deleted. If you pass `StopSelf::Include` and this run matches the filters, it is stopped once the rest is cleaned, and the call does not return |
| `runs()...list(limit).await` / `.count().await` | The newest `limit` matching runs (1 to 200) with how many match in all, or just the count; same filters as `clean` |
| `tokens().mint_for_instance(id, expires_in).await` | An instance token, value shown once. A replay past this call mints it again under the same id, so the value handed out before the replay stops working |
| `tokens().instance(id).revoke().await` | Revokes an instance's tokens |
| `tokens().list()`, `.revoke(id)` | `weft token ls` and `weft token revoke` |
| `frontends()`, `workers()`, `picks()` | `weft frontend`, `weft workers` and the picks `weft connect` makes |

`take_down` is a `TakeDownChoice` and `stop_self` a `StopSelf`. For what each
one can say, with an example, go and read [taking something down from a
run](../running/instances.md#taking-something-down-from-a-run).

If you want `status()`, `cancel_build()`, `get()`, `tree()`,
`running_source()`, `prune()`, `working_copies()`, `forget_working_copy()` or `set_run_example()` on
this run's own project, go through `ctx.own_project()` (for example
`ctx.own_project().status()`), because `ctx` has no short form for them.

### One run, by its id

`ctx.run_by_id(run)` reaches any run of the owner's, this one included
(`ctx.run_id`), and a run whose project was removed since.

| Call | What it does |
|---|---|
| `stop()` | `weft stop`. Called on this run, it stops it: the call does not return |
| `wake(node)` / `wake_in(node, &[3])` | `weft wake`: resolves a time wait now, and refuses one that expects a value |
| `logs(lines)`, `events()...read()` | `weft logs` and `weft events` |
| `outputs(&[..])`, `detail()` | Everything the run produced, as `weft freeze` reads it, and the run with the waits it is parked on |
| `node_state(node)` | Where one node of the run stands: `pending`, `running`, `waiting` (with the last answer its wait got), `completed`, `failed` (with the error), `skipped` or `cancelled`. Name the node the way `weft run --from` takes it: `render`, `work.render` inside a group, `one.render` inside the file the site `one` includes. A node inside a loop is refused (name the loop) |
| `delete()` | `weft clean <run>`. A run still going is refused, and so is this one |

`ctx.resolve_run("3f2a")` finds the one run whose id starts that way, the way
`weft logs 3f2a` does.

### A project's code

`ctx.project(id).code()` reads and writes a project's files and builds and runs
them, the way you do from a folder. A program writes into a working copy (for
what that is, go and read [your folder is a
copy](../running/versions.md#your-folder-is-a-copy)). If you name none, it
writes into the project's most recently changed folder: on an install on that
person's machine the files land on their disk right away, and on a cloud
install at their next command that syncs. When the project has no
folder, it writes into the program copy. If you want a particular working
copy, `ctx.project(id).working_copies()` lists them, each with its `id`, and
`ctx.project(id).code().working_copy(copy)` acts on that one. If a prune is
refused because of a copy's head, move it with
`.code().working_copy(copy).switch_to(version)` to a version outside that
history, or call `ctx.project(id).forget_working_copy(copy)`, which records
the copy's files as a version (named in `saved`, `None` when it held no file)
and removes the copy, as `weft forget-copy` does. A folder that still exists
registers again at its next command that syncs it.

```rust
let shop = ctx.project(shop_id).code();
shop.write_text("prompts/tone.txt", "Answer in two sentences.").await?;
let started = shop.run().example("angry-customer").start().await?;
ctx.log(LogLevel::Info, format!("run {} on version {}", started.run_id, started.version)).await?;
```

| Call | What it does |
|---|---|
| `state()` | The working copy, and every file it holds by path and hash |
| `read_text(path)` / `file(path)` | One file's text, or the file as a stored file to read with `ctx.storage(..)`; `None` when there is none |
| `write_text(path, text)` / `remove(path)` | Writes or removes one file. Text is capped at 4 MiB; a larger or binary file goes through `edit().stored(path, key)` |
| `edit().text(..).stored(..).blob(..)?.remove(..).apply()` | Several files changed together, all or none. `.blob(path, hash)` puts a file the owner already holds (a hash from `state()` or a version's files) at `path` without downloading it; a hash that is no sha256 fails the `blob` call, and one the install does not hold refuses the write. `.expecting(Some(hash))` after a change (`None`: no file there) lands it only while the file still holds what you read, and answers `Applied::Stale` naming the paths otherwise, so two writers never overwrite each other unseen |
| `checkpoint(Some(label))` | `weft checkpoint` |
| `switch_to(version)` / `switch_to_run(run)` | `weft branch`, given a full version or run id. What the copy held is recorded as a version first, named in `saved` |
| `build().start()` | `weft build`, returning once the build registered. Between looks at it the run parks on a timer |
| `run()...start()` | `weft run`: `.example(name)` is the example `weft run <name>` takes, `.seed_run(run)` is `--seed=<run>`, and each other setter is the flag of the same name (`.from(..)`, `.target(..)`, `.seed()`, `.instance(..)?`, `.durable()`...). It builds the code, checking the run against the compiled program before any image is built, so a run that could not start is refused before you wait on images. It returns once the run has started |

If you want a project's version tree, `ctx.project(id).tree()` answers what
`weft tree --json` prints, without `workingCopy` (the CLI adds that to say
which folder it ran in). If you want what the project
runs, `.running_source()` answers the version of its build and that build's
files by path and hash.

`.prune(version, frozen, plan_only)` is `weft prune`. In `frozen`, pass the
`frozenFrom.version` of every `examples/*.json` in the project (read each with
`read_text`): the prune is refused if it would delete a version a frozen
example came from, or an ancestor of one, and an origin you leave out has no
such protection. Set `plan_only` to see what it would remove without removing
anything. If you froze a run as an example yourself,
`.set_run_example(run, Some(name))` records it the way `weft freeze` does, and
`None` forgets it.

## What is not here

`ContextHandle`, the trait underneath, is the seam the runtime implements. Your
node only ever sees the `RunContext` wrappers on this page.
