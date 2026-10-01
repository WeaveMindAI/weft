# Storage

A value on a wire is capped at 100 KB. Anything bigger goes into storage, and
what travels the wire is a small marker saying where it is.

```rust
let storage = ctx.storage(StorageScope::Execution);
let stored = storage.put(bytes, "image/png", "chart.png", None).await?;
ctx.pulse_downstream(NodeOutput::stored_file(stored)).await
```

## The five scopes

The scope decides where new files go, and how long they live.

| Scope | Lives | Gone when |
|---|---|---|
| `Execution` (the default) | This run | Five minutes after the run ends, so its output is still downloadable, unless you kept it |
| `Project` | Across runs of this project | `weft clean` or `weft rm`, or once its lifetime runs out if you gave it one |
| `Shared { name }` | Across projects that name the same space | Explicit removal, or once its lifetime runs out if you gave it one |
| `Asset` | The project's `@asset` copies | Readable by your node, and the worker refuses writes to it |
| `Instance { of }` | One instance's space in this project (`StorageScope::instance()` for the run's own instance, `instance_of(id)` for any) | Until removed. For what removes them and who reaches them, go and read [an instance's files](../running/instances.md#an-instances-files) |

The scope governs **writes and lists**. Reading, deleting, keeping and signing
act on whatever scope the key itself belongs to, so a later node can `get` a
file without knowing where it came from. `copy` is the one that does both,
reading from the key's scope and writing into yours, which is how a file
crosses.

## How long a file lives

An execution-scoped file is swept shortly after the run ends. If you want it to
outlive the run (anything a person will open later), give it a lifetime:

```rust
storage.put(bytes, "image/png", "chart.png", Some(KeepTtl::Default)).await?;
```

| `KeepTtl` | Meaning |
|---|---|
| `Default` | 30 days |
| `Secs { secs }` | That long |
| `Never` | No expiry. Only `weft files rm` or `weft clean` removes it |

The lifetime counts from the last time something touched the file: every read,
link or replace pushes the expiry back, so a file something still uses does not
vanish underneath it.

Project, shared and instance files take a lifetime too. There, `None` means the
file lives until somebody deletes it. A cache of fetched pages, say:

```rust
ctx.storage(StorageScope::Project)
    .put(page, "text/html", "page.html", Some(KeepTtl::Secs { secs: 7 * 24 * 3600 }))
    .await?;
```

For a file that already exists, `keep(&file, ttl)` sets its lifetime from now
on. On an execution file it also marks the file to outlive its run, and that
mark cannot be taken back. On the other scopes `keep(&file, KeepTtl::Never)`
takes a lifetime away again. An asset lives as long as a version of the project
still names it (once none does, it expires on its own), so you cannot give one
a lifetime.

## Storing

| Call | Use it when |
|---|---|
| `put(bytes, mime, filename, keep)` | You have the bytes |
| `put_stream(stream, mime, filename, keep)` | You do not want the whole file in memory |
| `put_response(resp, what, mime, filename, keep)` | You already made an authenticated request and want its body |
| `put_from_url(url, filename, keep)` | A plain URL, fetched straight in |
| `copy(&file, keep)` | A file that already exists, into this scope |

Every one of these makes a new file under a fresh key, so two puts never
collide, whatever their filenames.

### Changing a file in place

If your node keeps something that grows with use (a conversation, a log, a
document it keeps adding to), keep it in a stored file and change that file. A
port carries at most 100 KB, and a growing value will pass that. Have the node
take the file in and pass the file on, and do not also offer the same content
as a plain value, so a program has only one thing to wire.

To change what the file holds, use `edit`. It reads the content, hands it to
your function, and writes back what you return:

```rust
let updated = storage.edit(&file, |old| {
    let mut lines = old.to_vec();
    lines.extend_from_slice(b"one more line\n");
    Ok(lines)
}).await?;
```

If two writers change the same file at once (two runs on one project file, the
iterations of a parallel loop), neither loses the other's change. If the file
changed between the read and the write, `edit` reads it again and runs your
function again on the new content. Because it can run more than once, the
function must use only the bytes it is given: no counter it bumps, no call
out, no other read. Returning the bytes unchanged writes nothing.

The file keeps its key, so every reference already handed out now reads the
new content. It also keeps its scope, name, type and lifetime. A reader sees
either the old bytes or the new ones, never a mix. What comes back is the file's value
with its new `sizeBytes` and `version`.

Every edit is recorded on the run, and the node's panel in the graph lists it
under "Files edited": the file, the versions it moved between (`v4 → v5`), and
the lines that changed (a diff longer than 100 KB is cut short at a line
boundary). A file that is not text shows its old and new size.

If you want to overwrite the file whatever it holds now, without reading it,
use `replace(&file, bytes)`. It shows under "Files edited" too, with the new
size instead of the changed lines.

An asset cannot be edited or replaced, and neither can a file value that points
at a URL.

### Versions

Every stored file value carries a `version`: 1 when the file is made, going up
every time the content is written. A key always names the same file,
so a key plus a version always means the same bytes.

If you seed a run that would reuse a step whose file has changed since, the run
is refused; for what to do then, go and read
[when a reused step's file changed](../running/versions.md#when-a-reused-steps-file-changed).

### Storing the same thing twice

```rust
let storage = ctx.storage(StorageScope::Project).identified("slack:F123456");
```

`identified` names what the file is a copy **of**. Put it twice and it stores
once, and with `put_from_url` a source you already have costs no request at
all.

Name the source, like `<service>:<id>`, not the content.

## Reading

| Call | Gives you |
|---|---|
| `get(&file)` | The bytes, as a stream |
| `get_range(&file, range)` | Part of them |
| `get_bytes(&file)` | The whole thing in memory. Small files only |
| `list()` | Everything in this scope |
| `delete(&file)` | Gone. Stored files only, not URL-backed ones |

## Handing a file out

Three ways, for three audiences.

| Call | The link reaches |
|---|---|
| `presign(&file, ttl)` | Whoever you give it to, for about 15 minutes by default |
| `public_link(&file, ttl)` | The open internet, or `None` when this install serves no public address |
| `caller_link(&file, ttl)` | The caller of this run, on the address its request came in on. This is what a route's answer carries in place of a file |

`public_link` returning `None` is not a failure. It means the store is private
and nothing is relaying it, so hand out the bytes instead.

If your install answers on more than one address (the loopback port, a tunnel,
a domain), `caller_link` builds the link on the one the caller used, so a
browser on `http://127.0.0.1:14111` gets a loopback link even while the tunnel
is open. If no request started the run (`weft run --fire`), the link uses the
install's internet address, or its own address when it has none.

## Whole values full of files

A chat history with three images in it is a typed value with three file markers
buried inside it. Two calls handle that.

`externalize(&value, &ty, policy)` walks every file slot the type names, at any
depth, and turns each into a link or inline bytes depending on what the
consumer takes. A provider that accepts image URLs but only inline audio gets
exactly that.

`internalize(&value, &ty, keep)` is the reverse: it takes a value full of
`data:` URLs and external links and stores them, giving you back something you
can emit and that will still work tomorrow.

The rule that makes it safe: **the link never leaves your node, and the marker
never leaves weft.** Anything you emit, park on, or memoize is stripped back to
the stored form for you. What `externalize` gives you is no longer a value of
that type, because presigned links expire, so hand it to whoever asked and do
not store it.

## What a stored file looks like

It is a marker naming its kind, and the kind comes from the mime type when it
was stored.

| Weft type | For |
|---|---|
| `Image` | `image/*` |
| `Video` | `video/*` |
| `Audio` | `audio/*` |
| `Blob` | Everything else: a PDF, a zip |

Inside is the key, the mime type, the size and the filename. **No URL**, which
is deliberate: a link expires and a marker does not, so the marker is what goes
on wires and into the journal.

The marker is a one-key object, and the key names the kind: `__weft_image__`,
`__weft_video__`, `__weft_audio__` or `__weft_blob__`. A stored PNG looks like
this on a wire:

```json
{
  "__weft_image__": {
    "key": "acme/project/3f2a9c1e-7b4d-4e0a-9c55-2d1f0e8a6b17/5b0c2e44-1a9f-4c3e-8d2b-6f7a9e0c1d23",
    "mimeType": "image/png",
    "sizeBytes": 48213,
    "filename": "chart.png"
  }
}
```

`File` is shorthand for all four and `Media` is shorthand for the first three.
Both are fine on a port. Neither can be the type on an `@asset`, because a
value carries exactly one marker and those leave it open.

`NodeOutput::stored_file(stored)` fills the four ports a file travels as at
once: `file`, `filename`, `mimeType` and `sizeBytes`.
