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

**`ctx.await_signal` is refused once your node has emitted anything**, because
a resume replays the body from the top and would emit again. It is also refused
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
| `execution_id` | `ExecutionId` (a UUID) | This run's id |
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
| `for_holes(holes, what, spell, declare)` | Values matched to named holes in a text, such as `$user_id` in SQL | A hole names no port, or a wired port the text never reads |

Never write `get(...).unwrap_or(...)`. It turns a real type error into your
default and you will not find out. `get_or` is the same shape and fails
honestly.

## Emitting

| Call | What it does | It fails when |
|---|---|---|
| `pulse_downstream(output).await` | Sends values on | A port is mentioned twice, is not declared, gets a value its type does not allow, or is `error` on a node with `catchErrors` |
| `yield_downstream(output).await` | The same, but waits until the value was taken | Same, plus a delivery that can never happen, and an empty output |
| `close_port("port").await` | Closes one output. On a stream port this is the end of the stream | The port was already mentioned this firing, or is `error` on a node with `catchErrors` |
| `set_max_buffered_items("port", n)` | Raises the un-taken cap on a stream port above 4096 | The port is not a stream output, or `n` is 0 |
| `fan_declared(&value)` | Builds an output by matching an object's keys to your declared ports, skipping the rest. On a node with `catchErrors` it never fills `error`: a key called `error` in the data is data | never |
| `data_outputs()` | Your declared ports, minus `error` on a node with `catchErrors`: the ones a node fills from data (a row's columns, a response's fields) | never |
| `output_type("port")?` | The resolved type of one output | The node does not declare that output: a type error, so it fails the run even with `error` wired |
| `declared_outputs()` / `declared_inputs()` | Every port this node declares in the program, with its type | never |
| `is_output_wired("port")` | Whether anything downstream reads that output in this run | never |

An ordinary port emits at most once per firing. Whatever you never mention is
closed for you when the body returns. A `Generator[T]` port emits as often as
you like until you close it.

`NodeOutput` is the payload you hand those calls:

| Call | What it does |
|---|---|
| `NodeOutput::new()` | An empty one |
| `.set("port", value)` | Sets one port |
| `.extend_from_object(&value)` | Fans every top-level key onto a same-named port. Last write wins |
| `NodeOutput::stored_file(stored)` | The four ports a stored file travels as: `file`, `filename`, `mimeType`, `sizeBytes` |
| `.get("port")` | Reads back what you already set |

## Waiting, and surviving a restart

| Call | What it does | It fails when |
|---|---|---|
| `await_signal(kind).await` | Parks this firing until the signal fires, releasing the worker. Returns the payload | The body already emitted, or the node has a stream input, or the body's call order changed between replays |
| `register_signal(kind).await` | Sets up a trigger that starts a new run on every fire. Called during setup | Called twice for one node in one setup |
| `run("name", closure).await` | Runs the closure and writes down its result, or gives back what was written down last time | The closure itself fails |

`ctx.run` is how you stop a replay from doing an expensive thing twice. A body
only replays after a wait; if the worker dies while it is running, the step is
failed instead (go and read [surviving a restart](durable-execution.md)).

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

## Storage

`ctx.storage(scope)` gives you a handle. The scope decides where new files go
and what `list` sees; `get`, `delete`, `keep` and `presign` act on whatever
scope the key itself belongs to, so a later node can read a file without
knowing where it came from.

| Scope | Where it lives, and how long |
|---|---|
| `Execution` (the default) | This run only, swept after it ends unless you keep it |
| `Project` | Outlives runs, gone on `weft clean` or `weft rm`, or when a lifetime you gave it runs out |
| `Shared { name }` | Shared across projects that name the same space, until removed or its lifetime runs out |
| `Asset` | The project's published assets. Readable, and the worker refuses writes |

| Call | What it does |
|---|---|
| `identified(id)` | Names what the next put is a copy of, so storing it twice stores it once |
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
| `presign(&file, ttl)` | A temporary link. `None` means the default, about 15 minutes |
| `public_link(&file, ttl)` | A link the open internet can fetch, or `None` if this install serves none |
| `caller_link(&file, ttl)` | A link the caller of this run can fetch, on the address its request came in on |
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
| `create_bus(opts)` | Makes a bus and gives you a handle and a marker to emit | The options do not make sense |
| `bus(&marker)` | Turns a marker value back into a live handle | The bus is gone, or that is not a marker |
| `open_bus("port", opts, "name").await` | The whole producer move: make it, emit the marker, register your name. Closes on drop | Emitting or registering fails |
| `join_bus("port", "name")` | The consumer twin, also closing on drop | The input is not a live bus |
| `bus_from_input("port")` | A handle that does **not** close the bus. For an observer | Same |

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
| `tag_execution(["user_7"]).await` | Tags this run. Additive and safe to repeat | The list is empty, or a tag is not `[A-Za-z0-9_-]`, 1 to 64 characters |
| `stop_tagged("user_7", StopSelf::Keep).await` | Queues a stop for every live run of this project carrying that tag | The tag is invalid |
| `is_cancelled()` | Whether this run was cancelled. Cheap enough to poll | never |
| `cancellation()` | The flag itself, to `select!` against your own work | never |

`StopSelf::Keep` only reaches runs that took the tag before this one did, so
two runs racing to stop each other leave the later one alive. `StopSelf::Include`
ends this run too.

## Your project beyond this run

The runtime answers each of these calls on behalf of this run and writes the
answer into the run's journal, so a replay reads the answer back instead of
asking again. Most of them take an optional `.instance(id)`, which picks that
instance's copy instead of the shared one; `values()`, `connections()` and
`tokens().instance(..)` always name an instance.

| Call | What it does |
|---|---|
| `infra("bridge").start().await` | Brings the shared copy up. Returns once it runs, parking the run between looks; fails with the reason if it does not come up, or if somebody stops it while this waits |
| `infra("bridge").stop(spec, stop_self).await` | Scales the copy down, keeping its disk |
| `infra("bridge").terminate(spec, stop_self).await` | Deletes the copy and its disks, except the ones listed in `keepOnTerminate` |
| `infra("bridge").instance(id).wipe(spec, stop_self).await` | Deletes the instance's copy and every one of its disks, the kept ones too, even if the copy was already terminated |
| `infra("bridge").status().await` | The copy's state, `None` when there is none. A start or stop on its way reads `provisioning` or `stopping` at once, the same answer `weft status` gives |
| `infra("bridge").copies().await` | Every copy: the shared one and each instance's |
| `trigger("receive").activate().await` | Turns one trigger on |
| `triggers().deactivate(spec, stop_self).await` | Turns off every trigger of the program (or of one instance, with `.instance(id)`); if you want only some, name them with `.only([..])` |
| `values().instance(id).get().await` | The instance's values for the program's `@instance_filled` fields, by step and field |
| `values().instance(id).set(step, field, value).clear(step, field).apply().await` | Gives and clears values in one change, each checked against its node's rules; the instance's live triggers reading one are set up again, and their names come back |
| `values().instance(id).forget().await` | Forgets every value the instance was given |
| `connections().instance(id).list() / forget()` | Lists an instance's connections, or forgets all of them (the values naming them go too) |
| `instances().list().await` | Every instance weft holds anything for, one entry each: how many values, connections and live tokens, its infra copies, and its triggers, each with the events waiting on a field not yet filled and why |
| `costs().instance(id).service(s).since(t).list().await` | The cost records, each saying whose credential paid, narrowed by instance, node, service, run, `paid_by` or `since` (unix seconds) |
| `runs().instance(id).status(s).older_than(d).clean(running, stop_self).await` | Deletes runs; runs still going follow `running` |
| `runs()...list(limit).await` / `.count().await` | The newest `limit` matching runs (1 to 200) with how many match in all, or just the count; same filters as `clean` |
| `tokens().mint_for_instance(id, expires_in).await` | An instance token, value shown once |
| `tokens().instance(id).revoke().await` | Revokes an instance's tokens |

For what `spec` and `stop_self` decide, go and read
[taking something down from a run](../running/instances.md#taking-something-down-from-a-run).

## What is not here

`ContextHandle`, the trait underneath, is the seam the runtime implements. Your
node only ever sees the `ExecutionContext` wrappers on this page.
