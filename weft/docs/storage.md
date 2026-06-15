# Storage: storing and exchanging files

Weft workflows often handle data too big to pass around inline: a
WhatsApp voice note, a generated video, an uploaded PDF, a model's
audio output. Putting those bytes directly on the wire between nodes
(or into the execution history) does not scale. Weft's storage plane
solves this: a node stores the bytes once, gets back a small
**file reference**, and that reference is what flows between nodes.

This document covers:

- Storing and reading files from a node (`ctx.storage(...)`).
- The three scopes a file can live in (per-run, per-project, shared).
- Keeping execution files past the end of a run, and how long they
  live.
- Handing a stored file to an external API without re-uploading it.
- Managing your files from the CLI (`weft files`).
- Configuring the backing disks (`weft storage config`).

## The shape: a reference, not the bytes

When a node stores a file, it gets back a **stored-file value**: a
small, self-describing handle that names the file (its key) and
carries its type, size, and filename, but NOT the bytes and NOT a
public link. The value tags itself with its CONCRETE type, an
`Image`, `Video`, `Audio`, or `Blob` (the catch-all for anything that
is not image/video/audio, e.g. a PDF or a zip), so the type system
knows exactly what is flowing without inspecting the bytes. Two union
types name groups of these in port signatures: `Media` (`Image | Video
| Audio`, "a picture, clip, or sound") and `File` (`Media | Blob`,
"any stored file"). A port declared `Media` rejects a `Blob`; a port
declared `File` accepts anything. Passing a stored file from one node
to the next is just passing this value along an edge.

The actual bytes live in your storage box and only move when a node
asks for them (worker to box, directly) or when you download them
(your machine to box, directly). They never travel through the
execution history or the coordination layer, so a multi-gigabyte file
costs nothing on those paths.

## Storing a file

Inside a node's `execute`, open a storage handle and `put`:

```rust
let file = ctx
    .storage(StorageScope::Execution)
    .put(bytes, "audio/ogg", "clip.ogg", None)
    .await?;
// `file` is a self-describing reference; emit it downstream.
ctx.pulse_downstream(NodeOutput::with("audio", file)).await?;
```

`put` returns the file reference. For a large file you are
streaming in (an HTTP response body, a transform's output), use
`put_stream` so the bytes never buffer in memory:

```rust
let file = ctx
    .storage(StorageScope::Execution)
    .put_stream(response_byte_stream, "video/mp4", "out.mp4", None)
    .await?;
```

## Reading a file

A downstream node reads a stored file by handing the stored-file value
(or a raw key it kept) to `get`:

```rust
let (meta, mut stream) = ctx.storage(StorageScope::Execution).get(&file).await?;
// stream the bytes...
```

`get` streams. For a huge file you only need part of (splitting an
audio file into chunks to send to a speech-to-text API, say), read
just a byte range:

```rust
let range = ByteRange { start: 0, end: Some(1_000_000) };
let (_, chunk) = ctx.storage(scope).get_range(&file, range).await?;
```

This lets a node process a file far larger than memory, piece by
piece, without ever downloading the whole thing.

## Scopes: who can reach a file

`ctx.storage(scope)` picks one of three walls:

- **`StorageScope::Execution`** (the default) holds files for ONE
  run. It is the home for temporary working files. When the run
  finishes, these are deleted unless you keep them (see below).
- **`StorageScope::Project`** holds files that outlive a run and are
  shared across every execution of the same project. Removed by
  `weft clean <project>` / `weft rm`.
- **`StorageScope::Shared { name }`** holds files in a named space
  shared across your projects. Two of your projects that pass the
  same `name` meet in the same space; a project that never names it
  cannot reach it. It is opt-in by agreement: naming the space is
  how a project joins it.

The walls are enforced by who is calling, not by any credential a
node carries: a run can only ever reach its own files, its own
project's files, and shared spaces it has named. There is nothing to
configure and no key to leak.

## Keeping execution files

Execution files are deleted when the run ends. To make one survive,
mark it KEEP, either at store time or later:

```rust
// Survive from the moment it is stored.
ctx.storage(StorageScope::Execution)
    .put(bytes, mime, filename, Some(KeepTtl::Default))
    .await?;

// Or keep an existing file late in the pipeline (e.g. after a
// create-and-validate flow decides which file is the keeper).
ctx.storage(StorageScope::Execution).keep(&file, KeepTtl::Default).await?;
```

Keeping is additive: there is no "un-keep" and no "keep only mine"
(that would let two nodes race to delete each other's files).

A kept file has a **time-to-live**: by default 30 days, and any
access (download, read, re-reference) pushes the expiry back to now +
TTL, so a file you keep using never expires. You can choose the TTL:

- `KeepTtl::Default` is 30 days, access-extended.
- `KeepTtl::Secs { secs }` is a shorter or longer window.
- `KeepTtl::Never` never expires; only an explicit `weft files rm` /
  `weft clean` removes it.

Project and shared files are persistent: they have no TTL and are
removed only by explicit cleanup.

A spent run's files stay browsable at their original address (under
`exec/<run>/`) until their TTL expires or you clean them; `weft files`
lists them under a "past-execution survivors" heading.

## Handing a file to an external service

A common need is giving a stored file to a third-party API. Two
cases, both without re-uploading the bytes through your node:

- **The API accepts a URL** (the common "fetch this for me" field on
  media APIs). Mint a temporary signed URL and hand it over:

  ```rust
  let url = ctx.storage(scope).presign(&file, Some(900)).await?; // 15 min
  // pass `url` to the external API; it fetches the bytes directly.
  ```

  The node chooses how long the URL lives. The stored-file value itself
  never carries a URL; a presigned URL is an explicit, expiring thing
  you mint on purpose. Minting one counts as an access, so it extends
  a kept file's TTL.

- **The API takes the raw bytes** (and you must transform or chunk
  them first). Use streaming `get` / `get_range` to read the file
  piece by piece and feed each piece to the API, never holding the
  whole file in memory.

## Managing your files: `weft files`

```
weft files ls [PREFIX]      # list, grouped by space
weft files inspect <KEY>    # full metadata for one file
weft files download <KEY>   # download to your machine
weft files rm <KEY>         # remove one file
weft files rm <SPACE>/      # remove a whole space (trailing slash)
weft files usage            # stored bytes + per-disk usage
```

Downloads stream directly from your storage box to your machine; the
bytes do not pass through the coordination layer. If a file has been
swept or expired, its metadata still shows in the run's history, but
the download reports "expired or deleted".

## Configuring the backing disks

Your files are spread across one or more backing disks that grow and
shrink automatically: Weft adds a disk before the pool fills and
releases one (reclaiming real cost) when the pool has stayed
under-used for a while. You do not manage this; it happens behind the
scenes.

The only knob is the disk **profile**: which kind of disk to use and
how large each unit is. Cloud providers offer faster disks at higher
cost; the profile is where you opt into them:

```
weft storage config                       # show the current profile
weft storage config --class fast-ssd      # use a faster disk class
weft storage config --disk-gib 20         # larger disk units
weft storage config --class default       # back to the cluster default
```

A profile change applies to disks provisioned from then on; the pool
migrates to the new profile naturally as it grows and shrinks.

## Authoring a file-input node

A node that takes a file from the user in the editor declares a
`FileDrop` field. The editor shows a file picker; the dropped file is
uploaded to project storage and the field's value becomes a file
reference, ready to flow downstream exactly like a stored file.

## True scale-to-zero

If your tenant stores nothing, no disk is held and nothing costs you.
When your storage becomes completely empty (every execution file
swept, nothing kept, no project or shared data), the whole storage
box is torn down within about 30 minutes, releasing every disk. It is
re-provisioned automatically the next time a file is stored. For
sporadic use (a once-a-day workflow that writes a temporary file),
this means you hold a disk only while you are actually using it.
