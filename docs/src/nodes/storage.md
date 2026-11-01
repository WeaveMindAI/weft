# Storage

A value on a wire is capped at 100 KB. Anything bigger goes into storage, and
what travels the wire is a small marker saying where it is.

```rust
let storage = ctx.storage(StorageScope::Run);
let stored = storage.put(bytes, "image/png", "chart.png", None).await?;
ctx.pulse_downstream(NodeOutput::stored_file(StoredFile::from_value(&stored)?)).await
```

## The five scopes

Any run of any project in your account can read any stored file, whatever its
scope, for as long as the file exists. The scope decides only where a new file
goes and how long it lives.

| Scope | Holds | Gone when |
|---|---|---|
| `Run` (the default) | What this run made | Its run is deleted, that is when the run's record goes (its keep time runs out, `weft clean`, `weft rm`). The run ending does not delete it |
| `Project` | Files of this project | The project is deleted (`weft rm`) |
| `Shared { name }` | Files several projects meet in by naming the same space | Somebody deletes them (`weft files rm`) |
| `Asset` | The project's `@asset` copies | No version of the project names it any more, after a countdown. Your node reads it, and the worker refuses writes to it |
| `Instance { of }` | One instance's files in this project (`StorageScope::instance()` for the run's own instance, `instance_of(id)` for any) | Somebody deletes them (`weft files rm`, or wiping the instance with `WipeInstance`), or the project is removed. For more, go and read [an instance's files](../running/instances.md#an-instances-files) |

The scope governs **writes and lists**. Reading, deleting, keeping and linking
act on whatever scope the key itself belongs to, so a later node, or a later
run that kept the reference, can `get` a file without knowing where it came
from. `copy` is the one that does both, reading from the key's scope and
writing into yours, which is how a file moves to a scope that lives longer.

## How long a file lives

A file can also have a deadline. It is deleted when the deadline arrives,
whatever its scope, or with its owner (its run, its project) if that comes
first. Every read, link or replace pushes the deadline back, so a file
something still uses does not vanish underneath it. A file with no deadline
lives as long as its scope says.

```rust
storage.put(bytes, "image/png", "chart.png", Some(KeepTtl::Default)).await?;
```

| `KeepTtl` | Meaning |
|---|---|
| `Default` | 30 days since it was last touched |
| `Secs { secs }` | That long since it was last touched |
| `Never` | No deadline: it lives as long as its scope says |

`None` gives no deadline either. A cache of fetched pages that should go once
nobody has read it for a week, say:

```rust
ctx.storage(StorageScope::Project)
    .put(page, "text/html", "page.html", Some(KeepTtl::Secs { secs: 7 * 24 * 3600 }))
    .await?;
```

For a file that already exists, `keep(&file, ttl)` sets its deadline from now
on, and `keep(&file, KeepTtl::Never)` takes it away. Neither moves the file, so
a run file still goes with its run. If you want a file to outlive the run that
made it, `copy` it into the project (the `KeepFile` node with `scope: project`
does that). An asset lives as long as a version of the project still names it
(once none does, it expires on its own), so you cannot give one a deadline.

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
new content. It also keeps its scope, name, type and deadline. A reader sees
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

If you seed a run that would hand a node's saved file to a node that runs
again, and that file has changed since, the run is refused; for what to do then, go and read
[when a reused node's file changed](../running/versions.md#when-a-reused-nodes-file-changed).

### Storing the same thing twice

```rust
let storage = ctx.storage(StorageScope::Project).identified("slack:F123456");
```

`identified` names what the file is a copy **of**. Put it twice and it stores
once, and with `put_from_url` a source you already have costs no request at
all.

Name the source, like `<service>:<id>`, not the content.

### A file from a page's visitor

If a visitor of your site needs to send the program a big file (a video, say),
give the page an upload slot. `upload_slot(max_bytes, accepts, minutes, keep)`
(or the `UploadSlot` node) answers a plain string, `weft-upload:...`, that is
safe to hand to the page. The page sends the file straight to storage with
`uploadFile(slot, file)` from `@weft/connect`, in parts that resume after a
dropped connection. Then it calls a Route with the slot in a field typed as a
file (`{ "video": "weft-upload:..." }`), and the Route receives the stored
file. `accepts` is a weft type (`Video`, `Image | Video`, `File`), checked
against the file's first bytes. The file lands in the handle's scope, a slot
takes one file and is received once, and an upload nobody received within
`minutes` goes.

## Reading

| Call | Gives you |
|---|---|
| `get(&file)` | The bytes, as a stream |
| `get_range(&file, range)` | Part of them |
| `get_bytes(&file)` | The whole thing in memory. Small files only |
| `list()` | Everything in this scope |
| `delete(&file)` | Gone. Stored files only, not URL-backed ones. In a program, the `DeleteFile` node does the same |

## Handing a file out

A file value carries no link. If you want one, ask for it, and pick the call by
who will open it:

| Call | Use it when |
|---|---|
| `internal_link(&file)` | Code inside the install opens it: a script or a process your node runs on the worker. The link goes straight to storage, never out through a tunnel and back |
| `container_link(&file)` | One of your project's own containers opens it: an infra unit your node hands the file to. Inside a container `127.0.0.1` is the container itself, so on a local install this link reaches your machine at `host.docker.internal`, through weft's internal port. On a cloud install it is the same link as `internal_link` |
| `external_link(&file)` | Anything outside the install opens it: a browser, an app, a provider's API. This is the link a `Reply` puts in its answer in place of a file |
| `external_file(&file)` | You are sending the file to an outside service: its external link when the open internet can open it, else a `data:` URL carrying the bytes, with the file's mime type and filename beside it |

Every link comes back as a `FileLink { url, public }`, where `public` says
whether the open internet can open `url`. An external link goes the first of
these ways that exists:

1. On a cloud install, the cloud store's own link. Public.
2. Through the project's visitor address, when the project has a site whose
   server forwards requests to weft. Public.
3. Through weft's tunnel, when it is open. Public.
4. The install's own address, which only this machine opens (and the devices
   on its network, if the install has a network address set). `public` is
   `false`.

Your node does not pick between these, and neither does the program: the
person running the install sets them up. If you want to know how, go and
read [files at run time](../running/files.md) and [a public
address](../build/public-address.md).

If you are handing a file to a provider's API (an image to edit, a video to
dub) and the link says `public: false`, the provider cannot fetch it, so send
the bytes in your request instead. `external_file` makes that choice for you.

Unless you set a lifetime, a link lives at least 15 minutes from when it was
asked for. If you want the links to a file you store to live longer (a video
somebody will watch for an hour), or shorter, set it on the handle:

```rust
ctx.storage(StorageScope::Project)
    .link_lifetime(Duration::from_secs(2 * 3600))?
    .put(bytes, "video/mp4", "clip.mp4", None)
    .await?;
```

The value that comes back carries `linkSecs`, and every link any node makes
from that value lives at least that long. For a file a program already has, the
`LinkOptions` node sets it. A link lives at most seven days: a longer lifetime
is refused right where you set it. Handing a link out also keeps the file
stored at least as long as the link works.

Every link is a download: opening it in a tab saves the file under its name,
while an `<img>`, `<video>` or `<audio>` tag still shows or plays it. A link
answers range requests, so a video seeks and a broken download picks up where
it stopped, with nothing for your node or a page to add.

If a page asks for the same file again, it gets the same link, so its video
keeps playing: weft cuts time into windows as long as the link's lifetime
(quarter hours, for a 15 minute link) and hands out one link per file per
window. Each link works until its window ends plus one lifetime, so a 15
minute link works for between 15 and 30 minutes from when it was asked for.
For a lifetime over three and a half days the window is seven days minus that
lifetime, so a seven day link is a new one every time it is asked for.

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

The rule that makes it safe: **the marker never leaves weft, and a file value
never holds a link.** So nothing you emit, park on or memoize carries one, and
neither does the journal. What `externalize` gives you is no longer a value of
that type, and its links expire, so hand it to whoever asked and do not store
it.

## What a stored file looks like

It is a marker naming its kind, and the kind comes from the mime type when it
was stored.

| Weft type | For |
|---|---|
| `Image` | `image/*` |
| `Video` | `video/*` |
| `Audio` | `audio/*` |
| `Blob` | Everything else: a PDF, a zip |

Inside is the key, the mime type, the size, the filename, the `version`, and
`linkSecs` when somebody set a link lifetime. **No URL**, which
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
