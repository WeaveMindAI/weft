# Storage

A value on a wire is capped at 100 KB. Anything bigger goes into storage, and
what travels the wire is a small marker saying where it is.

```rust
let storage = ctx.storage(StorageScope::Execution);
let stored = storage.put(bytes, "image/png", "chart.png", None).await?;
ctx.pulse_downstream(NodeOutput::stored_file(stored)).await
```

## The four scopes

The scope decides where new files go, and how long they live.

| Scope | Lives | Gone when |
|---|---|---|
| `Execution` (the default) | This run | Five minutes after the run ends, so its output is still downloadable, unless you kept it |
| `Project` | Across runs of this project | `weft clean` or `weft rm` |
| `Shared { name }` | Across projects that name the same space | Explicit removal only |
| `Asset` | The project's `@asset` copies | Readable by your node, and the worker refuses writes to it |

The scope governs **writes and lists**. Reading, deleting, keeping and signing
act on whatever scope the key itself belongs to, so a later node can `get` a
file without knowing where it came from. `copy` is the one that does both,
reading from the key's scope and writing into yours, which is how a file
crosses.

## Keeping a file

An execution-scoped file is swept shortly after the run ends. That is right for
scratch and wrong for anything a person will open later.

```rust
storage.put(bytes, "image/png", "chart.png", Some(KeepTtl::Default)).await?;
```

| `KeepTtl` | Meaning |
|---|---|
| `Default` | 30 days |
| `Secs { secs }` | That long |
| `Never` | No expiry. Only `weft files rm` or `weft clean` removes it |

Every read pushes the expiry back, so a file something still uses does not
vanish underneath it.

`keep` is additive and there is no un-keep. And it only applies to execution
scope: project and shared files have no expiry, so asking to keep one there is
refused rather than quietly ignored.

## Storing

| Call | Use it when |
|---|---|
| `put(bytes, mime, filename, keep)` | You have the bytes |
| `put_stream(stream, mime, filename, keep)` | You do not want the whole file in memory |
| `put_response(resp, what, mime, filename, keep)` | You already made an authenticated request and want its body |
| `put_from_url(url, filename, keep)` | A plain URL, fetched straight in |
| `copy(&file, keep)` | A file that already exists, into this scope |

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
| `caller_link(&file, ttl)` | A caller of this install. This is what a route's answer carries in place of a file |

`public_link` returning `None` is not a failure. It means the store is private
and nothing is relaying it, so hand out the bytes instead.

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

`File` is shorthand for all four and `Media` is shorthand for the first three.
Both are fine on a port. Neither can be the type on an `@asset`, because a
value carries exactly one marker and those leave it open.

`NodeOutput::stored_file(stored)` fills the four ports a file travels as at
once: `file`, `filename`, `mimeType` and `sizeBytes`.
