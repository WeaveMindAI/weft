# Storage

Use `ctx.storage(...)` to save a file and pass its reference to another node.
Choose execution storage for a run's files, project storage for data reused
by that project, or shared storage for data used by several projects.

## Save an output

Inside a node with a `file: Blob` output:

```rust
use weft::node::NodeOutput;
use weft::storage::{KeepTtl, StorageScope};

let storage = ctx.storage(StorageScope::Execution);
let file = storage.put(
    b"The report is ready.".to_vec(),
    "text/plain",
    "report.txt",
    Some(KeepTtl::Default),
).await?;
ctx.pulse_downstream(NodeOutput::new().set("file", file)).await?;
```

The output contains a stored-file reference. `Some(KeepTtl::Default)` keeps
its bytes available after the execution so the report can be opened later.

## Choose the scope

| Scope | Storage space | Lifetime |
|---|---|---|
| `StorageScope::Execution` | `exec/<run>/` | Cleaned up after the run, unless kept |
| `StorageScope::Project` | `project/<project_id>/` | Until deleted individually or with the project |
| `StorageScope::Shared { name }` | `shared/<name>/` | Until explicitly deleted; survives project deletion |
| `StorageScope::Asset` | Uploaded source assets | Workers can read these but cannot write them |

Use `Execution` with no keep setting for temporary work. Use `Project` for
an index or cache shared across that project's runs. Projects belonging to
the same owner can use the same named `Shared` space, including after one
of those projects has been deleted.

The scope you pass controls writes and listings. Reads follow the handle's
key, but the worker must still have access: execution files belong to their
run, project files and assets belong to their project, and shared files
belong to their owner. Keeping an execution file does not make it available
to nodes in another execution.

## Read or stream a file

Read a file input with `ctx.inputs.get::<FileHandle>("file")?`, then choose
how much to load:

```rust
let (metadata, bytes) = storage.get_bytes(&handle).await?;
let (metadata, stream) = storage.get(&handle).await?;
let (metadata, stream) = storage.get_range(&handle, range).await?;
```

Use `get_bytes` when the whole file fits in memory. Use `get` to process a
stream, or `get_range` for a byte range. `ByteRange` uses an inclusive start
and exclusive end; an absent end reads through the end of the file.

For writes, `put_stream` accepts a byte stream. `put_response` copies an
HTTP response into storage and returns a `StoredFile`; `put` and
`put_from_url` return a file marker ready to emit. `put_from_url` asks the
runtime to fetch the URL. You can also use `list()` to inspect your selected
space and `delete(&handle)` to remove a file.

### Reuse a downloaded file

If the source has a stable identity, supply it before fetching:

```rust
let storage = ctx.storage(StorageScope::Project);
let file = storage.identified("whatsapp:message-123")
    .put_from_url(url, None, None).await?;
```

While a file with that identity exists in the chosen storage space,
`put_from_url` reuses it without downloading another copy. Project scope
allows reuse across runs. Choose an identity that distinguishes the source
file, such as the message ID of an attachment.

## The keep rule

Keep an execution file when it needs to remain usable after the run.
Temporary files can pass between nodes without being kept, but an old
execution's reference cannot display bytes that have already been deleted.

Execution writes take `keep: Option<KeepTtl>`:

| Setting | Retention |
|---|---|
| `None` | Cleanup after the execution |
| `Some(KeepTtl::Default)` | 30 days, renewed by reads through weft or fresh download links |
| `Some(KeepTtl::Secs { secs })` | The chosen number of seconds, renewed the same way |
| `Some(KeepTtl::Never)` | Until explicitly deleted |

Listing files, inspecting metadata or reusing an already-issued URL does
not renew the countdown. You can change an execution file's retention with
`storage.keep(&handle, ttl).await?` or the catalog's `KeepFile` node.
Project, Shared and Asset files reject keep settings because their scopes
have different retention rules.

## Files from the graph

Declare a file input, for example `Image`, and read it as a `FileHandle`.
The type determines which files the editor accepts; a `file_drop` widget's
`accept` setting can narrow the selection further.

A selected local file appears in source as an asset reference:

```weft
show = Debug { data: @asset("assets/photo.png", Image) }
```

For upload timing and old-version retention, read
[Files at run time](../running/files.md#files-your-source-refers-to).

## Send a file to an external service

Before your body runs, the runtime adds a temporary `url` inside each
stored-file input marker, requesting a one-hour link. Code that needs a URL
can read that field directly, but an external provider must be able to
reach the address.

Call `storage.public_link(&handle, ttl_secs).await?` when the recipient is
outside the installation. It returns an internet-reachable link or `None`.
If it returns `None`, send the bytes in a format the service accepts.
`presign` can also return a link reachable only inside the installation.
For link expiry and file retention, read
[Share a file outside weft](../running/files.md#handing-a-file-to-somebody-else).

Convert file markers into the recipient's expected format before sending
them. For files nested in a typed value, use
[`externalize`](custom-types.md#files-inside-a-record).

The runtime removes the added URL when a stored-file marker leaves the node
or is saved. If you copy the URL into a plain string, the runtime cannot
remove it for you. Save the file reference when a form needs to display the
file later; the browser can request a fresh link when the form opens,
provided the stored file still exists.

To list, download or delete files from a terminal, read
[Files at run time](../running/files.md#find-one).
