# Files at run time

A value on a wire is capped at 100 KB, so anything bigger is stored and what
travels is a marker saying where it is.

This page is about the files a running program leaves behind and what to do
with them. For putting one there from a node, go and read
[storage](../nodes/storage.md).

## Where they live

Any run of any project in your account can read any stored file, for as long as
the file exists. The space a file is in decides only how long that is.

| Space | Holds | Gone when |
|---|---|---|
| Run | What one run made | Its run is deleted: the run's keep time runs out, `weft clean`, or `weft rm`. The run ending leaves it |
| Project | What the project keeps using | `weft rm` |
| Shared | Things several projects meet in, by name | You remove them |
| Assets | Your `@asset` copies | They follow your source |
| Instance | One instance's files in this project | Your program deletes them, you remove them, you wipe the instance (the `WipeInstance` node), or `weft rm` |

A file can also have a deadline, and then it goes when nobody has touched it
for that long, or with its run or project if that comes first. Every read,
download or link pushes the deadline back, so a file still in use stays.

## Keeping something

In a graph, the storage nodes `TextToFile` and `FetchToStorage` take two
settings: `scope` (`run` or `project`) and `ttlDays` (the deadline in idle
days, empty or `0` for none). Every other node that makes a file, a download
from S3, Slack or Drive or a generated image included, stores it in the run's
space with no deadline, so it lives as long as its run is kept, unless its
description says it keeps the file in the project's files (a chat bridge's
media, kept under the message it came with, is one).

If you want a file to outlive the run that made it (an upload one request
receives and the next one serves, a file a table row points at), wire it
through `KeepFile`. With `scope: project`, its default, it copies the file into
the project's files and hands on the new reference, which lasts until `weft rm`.
With `scope: run` it leaves the file where it is and only sets its deadline.
If you would rather keep the whole run longer, set the trigger's `keepRunsFor`
or run it with `weft run --keep-for`.

## Looking at them

```bash
weft files ls
weft files ls project/
weft files inspect <key>
weft files usage
```

`ls` groups by space (`run/<run-id>/`, `project/<project-id>/`,
`instance/<project-id>/<instance>/`, `shared/<name>/`, and `asset/`, where each
asset is named by its content hash) and, for each file, shows its id, the size, the filename, and when it is deleted if it has a
deadline. A file's key is its space followed by its id, such as
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

The second form removes a whole space. It tells you how many files that is
before it does anything:

```text
About to remove the whole space 'project/<project-id>/': 48 file(s).
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

If a download or a node says a file is no longer available, the message says,
for that file's space, what can have deleted it: its run, its project, its
deadline, or somebody by hand. It also says how to make one last: `KeepFile`
with `scope: project`, keeping runs longer (`keepRunsFor`, `weft run
--keep-for`), or `ttlDays: 0` to take a deadline away.

`weft files ls` tells you what is actually there.
