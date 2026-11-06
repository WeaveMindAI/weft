# The ctx

`ctx` is everything your node's code can reach: its inputs, and every service
weft provides. The runtime hands it to `run`, and to a trigger's
`setup_trigger`.

Every call here returns `WeftResult`, and your node never names a weft error
type. Wrap an outside error with `.node_err("what you were doing")`, and fail
on something you detected yourself with `node_bail!`. If what you detected is
a mistake in the program (a value of the wrong shape, SQL that does not
parse), fail with `input_bail!("...")` instead. A wired `_error` catches what
`node_bail!` raises but never this: the fix is in the program, so the run
fails.

## Five rules that save you a debugging session

**Calls on a connection go through `ctx.open` or `ctx.client`.** That is what
signs the request and records what it cost. `ctx.http()` is for everything
else. A `reqwest` client you built yourself skips both.

**If your node can pause, keep a state that says where it is.** After a pause
the body runs again from its first line, and `ctx.keep` hands it back the
state it saved: a `once` whose field is filled runs nothing, a `wait` whose
field is filled waits for nothing. A port it already sent on sends nothing a
second time. For the whole story, go and read [surviving a
restart](durable-execution.md).

**A file in `StorageScope::Run` goes with its run**, when the run's record is
deleted (its keep time runs out, `weft clean`, `weft rm`).
Right for what one run makes, wrong for something a project keeps using: store
that in `StorageScope::Project`.

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
| `firing_key()` | `String` | A key for this firing: the same every time its body runs again (after a pause or a crash), and different on every loop iteration, at every place an included file is used, and in every other run. If a service must not see your call twice, send it this as the idempotency key (see [doing something only once](durable-execution.md#doing-something-only-once)) |
| `instance()` | `Option<&InstanceId>` | Which instance this run is for, when it is for one instance of the program (see [programs with instances](../running/instances.md)) |
| `is_setup()` | `bool` | `true` when this body runs in a setup: right after an apply, or while the program's triggers are set up. `false` in every run a trigger, a call or `weft run` starts (see [baking](infrastructure.md#outputs-saved-with-the-infrastructure-baking)) |

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
| `yield_downstream(output, &mut state).await` | The same, but waits until the value was taken. The run can pause there, so it takes your state ([surviving a restart](durable-execution.md#a-stream-producer-that-pauses)) | Same, plus a delivery that can never happen, and an empty output |
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
returned, or after it paused, fails with an error naming
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
| `keep(initial).await` | The state this firing keeps across a pause: `initial` on its first run, the last one it saved on every run after | Called a second time in one firing, or the saved state no longer reads as your type |
| `state.once(\|s\| &mut s.field, async \|st\| work).await` | The value in the field if it is filled, running nothing; otherwise runs `work`, which gets the state as `st` (so it can read a bus or a stream with it), stores its result there and saves | `work` fails, or its result saves as JSON null (a `()`, a `None`) |
| `state.read(\|s\| &mut s.field, &mut reader).await` | The item in the field if it is filled, reading nothing; otherwise reads the next one off a bus reader (a `BusEntry`) or a stream (its item), stores it there and hands it back, `None` at the end. Saves nothing by itself: the field goes on record at the next pause, `save()`, or end of a `once` or `wait` ([reading across a pause](durable-execution.md#reading-a-bus-or-a-stream-across-a-pause)) | The read fails |
| `state.wait(\|s\| &mut s.field, signal).await` | The answer in the field if it is filled; otherwise waits on the signal (pausing the run when it can), stores the answer there and saves. `signal.cancel_with(call)` also sends `call` once if the run is cancelled while it waits | The wait was given up (`WeftError::WaitGaveUp`), the answer is JSON null or does not read as the field's type, or the body read a bus or a stream directly before reaching this wait the first time in its run ([reading across a pause](durable-execution.md#reading-a-bus-or-a-stream-across-a-pause)). A wait somebody skipped ends the firing skipped |
| `state.wait(\|s\| &mut s.field, FirstOf::either(a, b)).await` | Waits on both signals and ends with whichever answers first, as `Either::First(answer)` or `Either::Second(answer)`; the other is taken down at that moment ([whichever comes first](durable-execution.md#waiting-on-whichever-comes-first)) | As `state.wait`; a person skipping either one skips the wait |
| `state.save().await` | Writes the state now | The write fails |
| `hold()` | A `Holding` to hand bus reads, a bus's `wait_for`, stream pulls and yields instead of your state, for work that cannot be picked up from the middle: while it exists, no wait of your node pauses (its `state.wait`s and helpers' waits included), and once it is dropped the node can pause again. It saves nothing ([holding the run](durable-execution.md#holding-the-run-instead-of-keeping-a-state)) | never |
| `register_signal(kind).await` | Sets up a trigger that starts a new run on every fire. Called during setup | Called twice for one node in one setup |
| `after_node(call, &access).await` | Puts `call` on the run's record, to send through `access` (`None`: plain) when this node ends: however it ends with `.always()`, only on a cancel without it ([cleaning up](durable-execution.md#cleaning-up-what-your-node-made-at-a-provider)) | The node is pure, or the call's address is empty or not http(s) |

For how the state carries a node through a pause, and through its worker
dying, go and read [surviving a restart](durable-execution.md).

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
| `renew(&access).await` | The connection opened again and renewed, after the values this firing was handed stopped working. Renewed once however many nodes ask, and only while the connection still holds what this firing was handed; otherwise you get the values that replaced them ([when the service refuses the values](../connections/using-one-in-a-node.md#when-the-service-refuses-the-values)) | This firing has not opened the connection, weft has nothing to renew it with (a pasted key), or the provider refuses the renewal |
| `endpoint("name").await` | A handle on one of this node's declared infra endpoints, once something answers there | The endpoint is not declared, or the infra is not running |

On an opened connection: `client()`, `credential()`, `value(name)`,
`identity()`, `owner()`, and `way()`, which way to sign in it was made with
([telling the credentials apart](../connections/using-one-in-a-node.md#what-an-opened-connection-knows)). On an endpoint handle: `url()`,
`host_and_port()`, and `call(method, path, body)`, whose `path` starts with
`/`.

### Putting a value into an address

If an id or a search term goes into a URL, build the URL with `client::url`,
so that an id holding `/`, `?` or `#` stays one piece of the address you
meant instead of changing which address you call:

```rust
use weft::access::client;

let url = client::url(API, &["files", &id, "copy"])?
    .query("q", &search)
    .query("pageSize", 50)
    .query_opt("pageToken", page_token.as_deref());
let answer = client::get_json(&http, url.as_str(), "copy the file").await?;
```

Every path segment, and both halves of every query pair, are percent-encoded:
anything but letters, digits and `-_.~` (a space becomes `%20`). The base is
taken as written, with a trailing `/` dropped, so it has to be a constant of
your node or an address weft handed you, never a value. A segment that is
empty, `.` or `..` fails with an input error, because an address reads even
the encoded `%2E%2E` as `..`, so a wired id of `..` would climb to another
address; that is why `url` returns a `Result`. `.segment(s)?` adds one more
segment, before the first query pair only (after one, it panics). If a
value's `/` must stay, like an object store key (`photos/2024/a.jpg`),
add it with `.key_path(key)?`, which encodes each part between the `/` and
refuses a `.` or `..` part. `.query_opt` adds the pair only when the value is `Some`, for an optional
input. Read the result with `.as_str()`, `format!("{url}")` or
`String::from(url)`.

For an endpoint, start from its address, `client::url(endpoint.url(),
&["media", &id])?`, or build only the path for `call` with
`client::url("", &["media", &id])?`, which gives `/media/<id>`.

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

If your node builds something on every run that is slow to build, let the
worker's runs share it instead. There are two calls for that:

- if each run needs one to itself for a while (a database connection a run
  may hold a transaction on, a process that runs one script at a time), take
  one from a pool with `ctx.pool`;
- if runs can all use the same one at once (a client, a cache, a pool library
  you already use), ask for it with `ctx.shared`.

### One each, from a pool

```rust
use std::time::Duration;

let mut conn = ctx
    .pool(&access, |opened| async move { connect(&opened).await })
    .max(20)
    .idle_close(Duration::from_secs(5))
    .check(|c| !c.is_closed())
    .reset(|c| async move { clear_session(&c).await.map(|()| c) })
    .take()
    .await?;
```

You write only how to make one (`connect` here is your own function), and
weft does the rest. `take` hands you the newest idle one that is still fresh
and passes your `check`, letting go of every one that fails on the way, and
calls your closure to make a new one when none is left. What comes back is a
`weft::shared::pool::Lease<T>`: it derefs to the item, and goes back to the
pool when you drop it.

The first argument is the key, the same as for [`ctx.shared`](#one-for-everybody):
a connection (your closure gets it opened, and the pool is built again once
the connection's values change) or a name of your own (your closure gets
`()`).

| Setting | Set it when | What it does |
|---|---|---|
| `.max(n)` | The other end only serves so many at once | At most `n` items out at once on this worker. A take past that waits for one to come back, and the wait ends with the cancelled error if the run is cancelled. `0` is refused. Without it, nobody waits |
| `.idle_close(d)` | The other end closes what sits idle, or should not be held idle | Lets an item go once it sat idle for `d`, and never hands out one idle for longer, even when the timer that closes it ran late. Without it, an idle item is kept for as long as the pool is |
| `.check(\|c\| ..)` | An idle item can die on its own (a closed connection, a process that exited) | Says whether an idle item is still good. One that is not is let go, and the next is tried |
| `.reset(\|c\| async move { .. })` | A run can leave something on the item (a session setting, an open transaction) | Runs when a lease is dropped, before the item goes back. Hand the item back if it is fit for the next run, or an error to let it go (the error goes to the worker's log). The item keeps its place under `max` while this runs |

Every ask of one pool has to give the same `max` and the same `idle_close`.
An ask that gives others fails with `the pool '<key>' is asked for with max
.. and idle close .., and it is held with max .. and idle close ..: every ask
of one pool says the same`, so keep the settings in one function your nodes
call, the way the Postgres nodes do.

If an item is no good for the next run (a process that stopped answering, a
connection whose state your run cannot vouch for), call `lease.discard()`
and weft lets it go instead of giving it back. weft does the same on its own
for a lease dropped while its run is cancelled, since whatever the run was
doing may still be under way on it. A make that fails takes no place under
`max`.

### One for everybody

```rust
let client = ctx.shared(&access, |opened| async move { Client::new(&opened) }).await?;
```

The closure is async and its result is a `WeftResult` of the thing (`Client`
here is your own type, whose `new` returns a `WeftResult<Client>`).

The first argument is the key weft keeps the thing under. If you pass a
connection (`&access`), the closure gets the opened connection, and once one
of its values changes (a new password), the next run that asks gets your
thing built again from the new values. To notice the change, weft opens the
connection every time a run calls `ctx.shared` (or `ctx.pool`). For a
connection a person made or an infra node published, the worker answers
that open from memory, so a reused thing costs no extra call. A connection
set to **Use ours** (weft's own key) costs one network call to weft each
time.

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
let client = ctx.shared(&access, |opened| async move { Client::new(&opened) }).with_limit(100).await?;
```

Then at most that many handles to it exist at once on one worker, and the
next run that calls `ctx.shared` waits until one is dropped. A run that asks again while it still
holds a handle can end up waiting on itself once the limit is reached, so
keep one handle per run. Every call for one shared thing has to pass the
same limit: a call that passes another, or none, fails, naming both. The limit counts the handles to one thing on one worker: two workers can go
past it between them, and so can the old thing and the new one for a moment
after a connection's values change, while runs still hold handles to the old
one.

If a task you spawn needs the thing after your run returns, move the handle
into the task: it keeps its place under `with_limit` until the task drops
it. If your task should not count against the limit, take `handle.arc()`
instead: the `Arc` keeps the value alive but holds no place under
`with_limit`, and weft can let go of the thing while you hold it, so the next
run may build a new one beside yours.

### How long weft keeps them

By default, weft lets go of a shared thing (a pool is one too) about five
minutes after the last run asked for it or dropped its handle, once no
handle is left. Your thing's `Drop` runs once weft has let go of it and the
last handle and `Arc` are gone, which is when a pool closes its connections.
When the worker stops, it lets go of every shared thing as soon as its last
run has ended, used recently or not, so a pool's connections go back to the
database while the worker is still writing its last records.

If you want a quiet stretch not to cost a rebuild, raise the five minutes
with `weft workers set --shared-idle-seconds <n>` (or lower it to let go
sooner). It is one setting for the whole project, covering every
shared thing on its workers. The change applies from the next run: weft starts workers with the
new value (a new revision on Cloud Run), and the old ones finish what they
are running and stop.

### When the other end only serves so many

`max` and `with_limit` only cap what one worker takes, so that its runs
never leave its sibling workers without a connection. They cannot keep a
database under its connection limit, because they cannot see how many
workers there are. If your node talks to something that only serves so much
(a database with a fixed number of connections, an API with a rate limit),
assume any number of workers run your node at once: Cloud Run starts copies
up to the project's `--max-instances` (set with `weft workers set`), and a
change of program or worker settings runs the old copies beside the new
ones for a while. Two things follow:

- **Hold nothing you are not using.** Close an idle connection within
  seconds, because one a copy keeps between requests is one no other copy can
  have. On Cloud Run a copy has no CPU between requests (unless the
  project turned on `weft workers set --cpu-always-allocated true`), so a
  timer that closes it may not fire for minutes, which is why `idle_close`
  also refuses to hand out an item idle for longer. The Postgres nodes use
  an `idle_close` of 5 seconds and a `max` of 20.
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

## Storage

`ctx.storage(scope)` gives you a handle. The scope decides where new files go,
how long they live, and what `list` sees; `get`, `delete`, `keep` and the links
act on whatever scope the key itself belongs to. Any run of any project in your
account can read any stored file it holds a reference to, for as long as the
file exists.

For every scope, go and read [Storage](storage.md).

| Call | What it does |
|---|---|
| `identified(id)` | Names what the next put is a copy of, so storing it twice stores it once |
| `link_lifetime(duration)` | Makes every link to the files this handle stores live that long instead of 15 minutes (at most seven days). The value carries it as `linkSecs` |
| `put(bytes, mime, filename, keep)` | Stores bytes and gives back the stored-file value |
| `put_stream(stream, mime, filename, keep)` | The same without holding the file in memory |
| `put_response(resp, what, mime, filename, keep)` | Streams an HTTP response you already made into storage |
| `put_from_url(url, mime, filename, keep)` | Fetches a URL straight into storage, as `mime` when you pass one |
| `copy(&file, keep)` | Copies a file into this handle's scope, leaving the original |
| `edit(&file, \|old\| ...)` | Changes the file's content in place: reads it, runs your function on the bytes, writes back what it returns. Another write of the same file is never lost (it reads again and reruns the function), so your function must use nothing but the bytes it is given. Same key, name, type and lifetime, new size and version |
| `replace(&file, bytes)` | Overwrites the file's content in place, whatever it holds now |
| `get(&file)` | The file's bytes, as a stream |
| `get_range(&file, range)` | Part of the file |
| `get_bytes(&file)` | The whole file in memory. Only for small ones |
| `delete(&file)` | Deletes it. Stored files only, not URL-backed ones |
| `list()` | Everything under this scope |
| `keep(&file, ttl)` | Sets the file's deadline from now on (`Never` takes it away). It does not move the file, so a run file still goes with its run |
| `internal_link(&file)` | A `FileLink { url, public }` for code inside the install (a script your node runs), straight to storage |
| `external_link(&file)` | A `FileLink { url, public }` for anything outside the install. `public: false` means only this machine (or its network) opens it, so an internet service needs the bytes instead |
| `external_file(&file)` | The external link when it is public, else a `data:` URL of the bytes, with the file's mime type and filename |
| `upload_slot(max_bytes, accepts, minutes, keep)` | A `weft-upload:...` string a page's visitor uploads one file through, with `uploadFile` from `@weft/connect` |
| `claim_upload(slot, &ty)`, `claim_uploads(&[(slot, &ty)])` | The file uploaded through each slot, received once into a place of type `ty`. Several slots are received together or not at all, which is what a Route does for the file fields of one call |
| `externalize(&value, &ty, policy)` | Turns every file inside a typed value into a link or inline bytes, for handing out |
| `internalize(&value, &ty, keep)` | The reverse: pulls URLs and inline data into stored files |

`keep` takes `KeepTtl::Default` (30 days, pushed back each time the file is
read), `Secs { secs }`, or `Never` (no deadline), in every scope but `Asset`. For what it
means in each scope, go and read [how long a file lives](storage.md#how-long-a-file-lives).

What comes out of `externalize` is no longer a value of that type, because the
links expire. Hand it to whoever asked and do not store it.

## Streams and buses

| Call | What it does | It fails when |
|---|---|---|
| `journal_policy()` | What your node's users set on `journalEphemeral` and `journalWindowMs`, as the `journal` option of a bus. Only a node with `features.journalSettings` (or a live-connection trigger) has them; for any other node it is the default | A setting is out of range or the wrong shape |
| `create_bus("port", opts)` | Makes a bus whose marker goes out on that output, and gives you a handle and the marker to emit. A body run again after a pause gets the same bus back. Making it does not put your node on it: registering a name does. A bus nobody ever registers on ends finished once its marker is nowhere a node could still join from ([how a bus ends](streams-and-buses.md#how-a-bus-ends)) | `port` is not one of your `Bus` outputs, the options do not make sense, or `opts.journal` has a window outside 100 to 60000 ms |
| `bus(&marker)` | Turns a marker value back into a live handle | The bus is gone, or that is not a marker |
| `open_bus("port", opts, "name").await` | The whole producer move: make it, emit the marker, register your name, which makes your node a member until your body returns. The bus ends once nobody is on it and no node can still join it, so you can return at once ([how a bus ends](streams-and-buses.md#how-a-bus-ends)) | Emitting or registering fails |
| `pending_bus("port", opts, "name")` | A bus that opens only on your first `open().await`, so the work that can fail runs first and its failure can still reach `_error` | never; `open()` fails as `open_bus` does |
| `join_bus("port", "name")` | The consumer twin: registers your name on the bus on that input, which makes your node a member like the one that made it. When your body returns, however it returns, your name leaves, and the bus stays open for whoever is still on it. Call it before you read or wait on the bus: once your node has done either without joining, it no longer keeps the bus open, so with nobody else on it the bus can end first and this call is refused | The input is not a live bus, the bus already ended, or another member holds that name |
| `bus_from_input("port")` | A handle that registers nothing, so your node is no member, and once it reads or waits it keeps nothing open. For an observer | The input is not a live bus |

On the bus handle:

| Call | What it does | It fails when |
|---|---|---|
| `cursor()?`, `cursor_from_start()?`, `cursor_at(offset)?` | Your reader on the bus: from now (from your own join, once you registered), from the oldest message the bus still holds, or from that offset. A body run again after a pause gets its reader back where it stopped | Your body already opened a reader on this bus ([one reader per bus](streams-and-buses.md#one-reader-per-bus)) |
| `until_end(&mut state).await` | Waits until the bus ends, reading nothing (the run can pause there): for a node that stays on a bus while others use it. Not a reader, so it keeps no place on the bus ([how a bus ends](streams-and-buses.md#how-a-bus-ends)) | The bus ended failed (`the bus ended failed: <why>`), or the run was cancelled |
| `wait_for(name, &mut state).await` | Waits until that name has joined the bus (the run can pause there). A body run again after a pause gets past it at once: the list of who joined comes back with the bus | The bus closes first |
| `reader.next(&mut state).await`, `next_json(kind, &mut state)`, `next_message(..)`, `next_bytes(..)` | The next entry, or the next message of that kind, waiting (the run can pause there). `None` once the bus ended finished | The bus ended failed (`the bus ended failed: <why>`), the payload is the other shape (`next_json` on a bytes bus), the read breaks the rule of [reading across a pause](durable-execution.md#reading-a-bus-or-a-stream-across-a-pause), or the bus came back from a pause without messages this reader had not read yet |
| `entry.as_json()` | On an entry a reader handed you: a JSON message's kind and value, or `None` for a join, a leave or a bytes message | never |

For how a bus ends and what a reader does about it, go and read [streams and
buses in Rust](streams-and-buses.md#how-a-bus-ends).

Read a stream input like any other value: `ctx.inputs.get::<Generator<Row>>("rows")?`.

| Call | What it does |
|---|---|
| `next(&mut state).await` | The next item, waiting (the run can pause there). `None` once, at a clean end |
| `try_next(&state)` | `Item`, `Empty` or `Finished`, without waiting |
| `drain(&mut state).await` | Everything, waiting for the end (the run can pause there). A failed stream errors instead of returning a partial list |
| `end()` | Whether the producer finished or failed, or `None` while it is open |

## Live callers

| Call | What you get |
|---|---|
| `is_api_call()` / `is_websocket()` | Whether a caller of that kind is attached |
| `caller_data_type()` | Whether this connection speaks `json`, `text` or `bytes` (the trigger's `dataType`), or `None` when nobody is attached |
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
| `cancellation()` | The flag itself, to end your own work with the run ([below](#when-the-run-is-cancelled)) | never |

`StopSelf::Keep` only reaches runs that took the tag before this one did, so
two runs racing to stop each other leave the later one alive. `StopSelf::Include`
ends this run too.

## When the run is cancelled

You do not have to watch for a cancel around weft's own calls. When the run
is cancelled, each of these ends at once with the cancelled error, even in
the middle of waiting on a slow server:

- a request sent through a client from `ctx.http()`, `ctx.client(..)` or an
  opened connection, and the read of its answer's body;
- an endpoint handle's `call`;
- a provider socket's `send` and `recv`;
- a storage read, and a write of bytes or a stream (the calls listed in
  [Storage](storage.md#storing); `put_from_url` and `copy` are not
  among them);
- a bus reader's read.

Pass that error up with `?` and the node ends cancelled instead of failed.

If your node waits on something of its own (a child process, a timer, a
library with its own client), wrap it in `race`, which hands back the work's
own output, or the cancelled error the moment the run is cancelled:

```rust
let status = ctx.cancellation().race(child.wait()).await?.node_err("wait for the script")?;
```

The work is dropped where it stands, so a child process you spawned with
`kill_on_drop(true)` is killed with it.

If you turn a request's error into a weft error yourself, check
`weft::cancellation::is_cancel(&e)` first: `.node_err` makes any outside error
a failure, a cancel included. `client::send(req, what)` keeps a cancel a
cancel and hands back any status, for a node whose answer is the status
itself. `checked_send` does the same and also refuses a non-success status,
quoting the provider, and `get_json` and `post_json` add the parse.
`client::body_error(what, e)` does it for a failed read of a body.

## Your project, and the owner's other projects

A node can do from code what a person does with the CLI: build and run a
project, write its files, turn its triggers on, start its infra, read its runs.
The runtime carries each call out on behalf of this run, with the same
parameters and the same refusals the CLI gets. Each call is numbered within
the firing; for what a body that runs again gets back from one, go and read
[the ctx helpers that wait](durable-execution.md#the-ctx-helpers-that-wait).

The calls below act on this run's own project. If you want the same call on
another project of the same owner, put `ctx.project(id)` in front of it, `id`
being the project's id (the one in its `weft.toml`):
`ctx.project(shop_id).infra("bridge").start(&mut state).await`. A project of another
owner is answered as if it did not exist. `ctx.projects().list()` lists the
owner's projects. If you want a new, empty one, call
`ctx.projects().declare("shop")` and take its id from the answer (the id comes from the call's number in the firing, so if the worker dies
after the declare and before the node's next save, the body run again finds
the project the first call made; every other time the body runs again, the
declare is a new call and makes a new project, so put it in a `state.once` or
keep the id in your state), then write its
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
| Starting a run (`ctx.code().run()...start(&mut state)`) | Every run of it would start the next, with nothing to end the chain. It is refused before anything is recorded as a version or built. Start it from another project, or reach the part you want with a trigger |
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
| `infra("bridge").start(&mut state).await` | Brings the shared copy up. Returns once it runs, and the run can pause between looks; fails with the reason if it does not come up, or if somebody stops it while this waits |
| `infra("bridge").request_start(&mut state).await` | Asks for the copy to come up and returns once weft has accepted, without waiting for it to run. If a route should answer its caller at once (a 202), call this and let a later `status()` say whether the copy came up; until then it reads `provisioning` |
| `infra("bridge").stop(take_down, stop_self).await` | Stops the copy, keeping its disks (a unit whose `onStop` keeps it running stays up until terminate). It returns once the stop is under way, and `status()` reads `stopping` until the copy is down. Answers an `InfraDownAnswer`: `TakenDown`; `AlreadyDown` when the copy was already stopped or stopping; or `NoCopy` (a copy never started, a mistyped instance id, or a copy terminated earlier) |
| `infra("bridge").terminate(take_down, stop_self).await` | Deletes the copy and its disks, except the ones the node lists in `keep_on_terminate`. Answers as `stop` does, except that `AlreadyDown` means it was already being terminated |
| `infra("bridge").instance(id).wipe(take_down, stop_self).await` | Deletes the instance's copy and every one of its disks, the kept ones too, even if the copy was already terminated. Always answers `TakenDown` |
| `infra("bridge").upgrade(take_down, stop_self).await` | Moves a running copy onto the last build weft made (a stop, then a start; nothing is built). A copy already on that build, stopped or missing is left as it is. It returns once the upgrade is under way; its `command_ids` hold one command per owner (none when nothing was upgraded), and `all_infra().follow_command(id, &mut state)` waits for one to end |
| `infra("bridge").status().await` | The copy's state, `None` when there is none. A start or stop on its way reads `provisioning` or `stopping` at once, the same answer `weft status` gives |
| `infra("bridge").copies().await` | Every copy: the shared one and each instance's |
| `infra("bridge").logs(tail)`, `.show()`, `.press(kind, payload)`, `.rebake(&mut state)` | `weft infra logs`, `show`, `press` and `rebake` for that node |
| `all_infra().cancel(instance)`, `.look()`, `.doors()`, `.logs(tail)`, `.command(id)` | What reaches every copy at once: `weft infra cancel`, `look`, `list-doors` and `logs`, and where an upgrade's command stands |
| `trigger("receive").activate(held).await` | Turns one trigger on if it is off. While it was down it may have held work that arrived: `ReleaseHeld::Run` runs that work, at half the trigger's own limits so new arrivals still get through, and `ReleaseHeld::Drop` deletes it and cancels the runs paused on it |
| `triggers().deactivate(take_down, stop_self).await` | Turns off every shared trigger of the program, or an instance's triggers with `.instance(id)` (chain it again for each further instance), or every instance's with `.all_instances()`, which you cannot combine with `.instance(..)`. If you want only some triggers, name them with `.only([..])`. It returns once they are down, without waiting for the runs they started, even with `Running::Wait` |
| `triggers().bake()`, `.resync(take_down, &mut state)`, `.cancel_activate()` | `weft bake`, `weft resync` and `weft cancel-activate`. A resync returns once it has ended, and the run can pause between looks |
| `status()`, `cancel_build()`, `remove(&mut state)` | `weft status`, `weft cancel-build` and `weft rm` |
| `values().instance(id).get().await` | The instance's values for the program's `@instance_filled` fields, by node and field |
| `values().instance(id).set(node, field, value).clear(node, field).apply().await` | Sets and clears values in one change, all or none, each checked against its field and its node's rules first. Any live trigger of the instance that reads one is set up again, and `apply` returns those triggers' names |
| `values().instance(id).forget().await` | Forgets every value the instance was given |
| `connections().instance(id).list() / forget()` | Lists an instance's connections, or forgets all of them (the values naming them go too) |
| `instances().list().await` | Every instance weft holds anything for, one entry each: how many values, connections and live tokens, its infra copies, and its triggers, each with the events waiting on a field not yet filled and why |
| `costs().instance(id).service(s).since(t).list().await` | The cost records, each saying whose credential paid, narrowed by instance, node, service, run, `paid_by` or `since` (unix seconds). A run's costs are deleted whenever the run is, whether its keep time ran out or something cleaned it (for how long runs are kept, go and read [how long a run is kept](../running/the-journal.md#how-long-a-run-is-kept)) |
| `runs().instance(id).status(s)?.older_than(d).clean(running, stop_self).await` | Deletes the matching runs and answers how many it deleted, cancelled and left running. If a run is still going, `StillGoing::Leave` leaves it alone and `StillGoing::Cancel` cancels it (its rows go at the next clean). This run is never deleted. If you pass `StopSelf::Include` and this run matches the filters, it is stopped once the rest is cleaned, and the call does not return |
| `runs()...list(limit).await` / `.count().await` | The newest `limit` matching runs (1 to 200) with how many match in all, or just the count; same filters as `clean` |
| `tokens().mint_for_instance(id, expires_in).await` | An instance token, value shown once. Made again after a crash, from a state saved before it, it mints a fresh value for the same token, and the value handed out first stops working |
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
| `node_state(node)` | Where one node of the run stands: `pending`, `running`, `waiting` (with the last answer its wait got), `paused` (no wait of its own: its run paused while it was inside it, and it carries on when the run does), `completed`, `failed` (with the error), `skipped` or `cancelled`. Name the node the way `weft run --from` takes it: `render`, `work.render` inside a group, `one.render` inside the file the site `one` includes. A node inside a loop is refused (name the loop), except in a run of part of that loop's body, where it ran once |
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
let started = shop.run().example("angry-customer").start(&mut state).await?;
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
| `build().start(&mut state)` | `weft build`, returning once the build registered. Between looks at it the run can pause on a timer |
| `run()...start(&mut state)` | `weft run`: `.example(name)` is the example `weft run <name>` takes, `.seed_run(run)` is `--seed=<run>`, and each other setter is the flag of the same name (`.from(..)`, `.target(..)`, `.seed()`, `.instance(..)?`, `.durable()`...). It builds the code, checking the run against the compiled program before any image is built, so a run that could not start is refused before you wait on images. It returns once the run has started |

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
