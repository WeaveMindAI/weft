# Files at run time

A picture a program made can still be there in an old run months later, or it
can be gone in five minutes. Which one depends on where the step put it and
whether anything asked to keep it.

## Find one

```bash
weft files ls
weft files inspect <key>
weft files download <key> --output picture.png
weft files usage
```

The key is the space heading plus the file id underneath it, so
`project/<project-id>/<file-id>`. Without `--output` you get the stored
filename. These work from anywhere, not just inside a project. The editor has a
browser for the same thing, which can also pick a file for a program input.

`weft files rm <key>` deletes one, after asking. A key ending in `/` means a
whole space, so read it twice before confirming.

## How long they last

| Where it was written | How long it lives |
|---|---|
| Execution, not kept | Cleaned up once that run ends |
| Execution, kept | The keep period, or forever |
| Project | Until you delete it, or the project |
| Shared | Until you delete it, even if the project goes |

Unkept execution files get a five minute grace period before a sweep takes
them, so do not count on them when you open an old run.

The default keep is 30 days, and it resets whenever the file is read through
weft or a fresh download link is made. Listing files, looking at their
metadata, or reusing a link you already have does not reset anything.

To keep an execution file after the fact, put a `KeepFile` step in. Its
`ttl_days` is 30 by default and zero means forever. It only applies to
execution files: project and shared files have their own lifetimes and refuse
it.

For choosing where to write in the first place, read
[storage](../nodes/storage.md).

## Files your source refers to

Before a build, weft uploads any local asset that is new or changed, and it
protects whatever the current program points at.

Swapping a picture in your source does not delete the old upload straight away,
because old runs still refer to that version. Uploads nothing points at any
more get a 30 day expiry, reset by reads through weft or fresh links. Deleting
the project takes its assets with it.

For the markers and when each is read, read
[files and reuse](../language/files-and-reuse.md).

## Handing a file to somebody else

A step can ask for a temporary link. `presign` gives one the step can use,
though whether it works from the internet depends on the installation.
`public_link` gives one that does, or nothing at all if the installation cannot
provide it.

Links last 15 minutes by default and seven days at most. A signed bucket URL
can outlive the file it points at. A public relay link holds off the file's own
expiry until the link dies, which is not the same as keeping it.

So keep the file *reference* in your program's outputs and make a link at the
moment you hand it over. Save the link as the output instead and every old run
is left pointing at something expired.

For setting up the public relay, read
[a public address](../connections/public-address.md).
