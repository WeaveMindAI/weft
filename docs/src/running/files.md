# Files at run time

A value on a wire is capped at 100 KB, so anything bigger is stored and what
travels is a marker saying where it is.

This page is about the files a running program leaves behind and what to do
with them. For putting one there from a node, go and read
[storage](../nodes/storage.md).

## Where they live

| Space | Holds | Gone when |
|---|---|---|
| Run | What one run made | Five minutes after the run ends, so its output is still downloadable, unless something kept it |
| Project | Things that outlive a run | `weft clean` or `weft rm` |
| Shared | Things several projects meet in, by name | You remove them |
| Assets | Your `@asset` copies | They follow your source |
| Instance | One instance's files in this project | Your program deletes them, you wipe the instance (the `WipeInstance` node), or `weft rm` |

The five minute window matters. A run that makes a picture and ends has not
thrown it away yet, and a link you handed somebody still works for a little
while.

## Keeping something

A node stores a file with a keep policy, and that is what saves it from the
sweep: thirty days by default, a span you choose, or forever.

Reading a file postpones its expiry, so one that is still in use stays.

In a graph, the storage nodes `TextToFile`, `FetchToStorage` and `KeepFile`
take the same two settings: `scope` (`run`, this run only, or
`project`, every later run of the project can read it) and `ttlDays` (how
long it lives once nobody touches it, `0` for never expiring). Every other
node that makes a file, a download from S3, Slack or Drive included, stores
it for its run only. If you want such a file longer, or in a later run, wire
it through `KeepFile`.

If a later run needs the file, `scope: run` is the wrong keep however many
days you give it: the file lives on, but only the run that made it can read
it, so a later run that reads its reference back (from a table row, from an
instance's values) is refused with `run-scoped file belongs to a different
run`. A test inside one run passes, which is what makes this easy to miss. Use
`scope: project` instead. If a storage node keeps a file with `scope: run` (or
that default) and the file is wired straight into a node that keeps what it is
given past the run, the compiler warns (`run-file-kept-past-run`). It does not
see a file another node made, or one packed into a larger value on the way.

There is no un-keep. A file marked to survive survives until somebody removes
it.

## Looking at them

```bash
weft files ls
weft files ls project/
weft files inspect <key>
weft files usage
```

`ls` groups by space (`run/<run-id>/`, `project/<project-id>/`,
`instance/<project-id>/<instance>/`, `shared/<name>/`, and `asset/`, where each
asset is named by its content hash) and, for each file, shows its id, the size, the filename, and whether it is kept and when it
expires. A file's key is its space followed by its id, such as
`run/9b81d0a2-4e17-4a33-bc90-51d8e2f4a7c1/3f6c0e1a-77d2-4b8e-9a41-2c5d8e0b9f13`,
and that key is what `inspect`, `download` and `rm` take. Give `ls` the start
of a key (`project/`, or one run's `run/<run-id>/`) and it lists only the files
under it. `usage` prints how much you have stored in all, and across how many
files.

## Getting one out

```bash
weft files download <key>
weft files download <key> -o ~/Desktop/chart.png
```

Without `-o`, it lands in the folder you run it from, under the file's own
name. It streams straight from storage into a temporary file and renames it only
after the size checks out, so a download that fails never leaves you a
plausible half file, and never overwrites a good copy with a broken one. If
the connection drops partway, it picks up from where it stopped, and if the
file changes on the install meanwhile, it stops and says so.

## Removing them

```bash
weft files rm <key>
weft files rm project/<project-id>/
```

The second form removes a whole space, **including files somebody deliberately
kept**. It tells you how many of those there are before it does anything:

```text
About to remove the whole space 'project/<project-id>/': 48 file(s), 12 of them KEPT (persisted on purpose).
Type 'yes' to confirm:
```

`--yes` skips the question, and is required when there is no terminal to ask.

## Links

A node hands out a link when somebody outside the install has to open a file:
a route's answer carries one in place of the file, for instance. For how a
node asks for one, how long it lives and what opening it does, go and read
[handing a file out](../nodes/storage.md#handing-a-file-out). A link expires,
which is why a stored file travels as a marker rather than a URL.

If you want to choose who can open those links (anyone on the internet, the
devices on your network, or only this machine), go and read [which address a
file link gets](../build/public-address.md#which-address-a-file-link-gets).

## When a file goes missing

If a download or a node says a key is not there, it was almost certainly swept:
a run-scoped file that nothing kept, past its window.

`weft files ls` tells you what is actually there. If a program's output needs
to outlive its run, the node that made it has to say so when it stores it.
