# Files at run time

Two different things are called "files" in weft.

**Project assets** live with your source: an image you dropped onto a node, a
prompt in its own file, a CSV a program reads. They are referenced with
`@asset` or `@file` and they are part of the project.

**Runtime files** are written by running programs: a generated image, a
transcription, a cache a project builds.

The [asset sync](../language/files-and-reuse.md#the-asset-sync) is the bridge:
before every build it makes storage mirror exactly what the code references.

## How long a runtime file lasts

If you want to know when something you wrote will disappear, look at the
[scope](../nodes/storage.md) it was written with.

| Scope | Path | Deleted when |
|---|---|---|
| Execution | `exec/<run>/` | shortly after the run ends, unless kept |
| Project | `project/<project_id>/` | the project is deleted |
| Shared | `shared/<name>/` | the owner deletes it |

If you want a file your node emits to survive its own run, mark it kept.
Otherwise it is swept shortly afterwards and turns up in the editor weeks later
as expired media.

`KeepTtl::Default` is 30 days and every access bumps the clock, so artifacts
still in use never expire while abandoned ones age out. The rule and the
`KeepFile` node are in [Storage](../nodes/storage.md#the-keep-rule).

## Finding one afterwards

If you want to see what a project has written, or pull one file down:

```bash
weft files ls
weft files inspect <key>
weft files download <key>
weft files rm <key>
weft files usage
```

The editor has the same thing as a browser, and if you want a stored file's
address in your source, its picker will paste it in for you.

## Public links

If you want somebody outside weft to be able to fetch a stored file, whether
that is a person you are sharing a generated image with or a provider you are
handing media to, there are two ways and they are for different jobs.

- **Presigned**, a signed URL carrying its own credentials. This is the one for
  handing a provider bytes during a single call.
- **A public link**, a shorter address protected by an unguessable token and
  served through the same filtered surface as the trigger paths. This is the
  one for sharing with a person. See
  [what the proxy passes](../connections/events.md#what---public-url-actually-does).

Both take a time to live and **both expire**: 15 minutes if you do not say, 7
days at the most. So never emit either on a port, because the URL outlives its
own validity in the journal and turns into a broken artifact later. Emit the
stored file itself and mint the link where it is used.

## Media inside typed values

If your type has file-shaped fields inside it, you do not have to walk it. The
runtime converts the whole value at a provider boundary, which is what keeps a
conversation carrying forty images cheap to journal.

See [media inside a custom type](../nodes/custom-types.md#media-inside-a-custom-type).

## The object store

Underneath, files live in an S3-compatible object store. Locally that is a
container the installer runs; elsewhere it is whatever S3 endpoint is
configured.

Nothing in the language or the node API depends on which, because a node writes
through `ctx.storage` and never names a bucket.
