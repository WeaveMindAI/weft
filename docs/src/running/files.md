# Files at run time

A value on a wire is capped at 100 KB, so anything bigger is stored and what
travels is a marker saying where it is.

This page is about the files a running program leaves behind and what to do
with them. For putting one there from a node, go and read
[storage](../nodes/storage.md).

## Where they live

| Space | Holds | Gone when |
|---|---|---|
| Execution | What one run made | Five minutes after the run ends, so its output is still downloadable, unless something kept it |
| Project | Things that outlive a run | `weft clean` or `weft rm` |
| Shared | Things several projects meet in, by name | You remove them |
| Assets | Your `@asset` copies | They follow your source |

The five minute window matters. A run that makes a picture and ends has not
thrown it away yet, and a link you handed somebody still works for a little
while.

## Keeping something

A node stores a file with a keep policy, and that is what saves it from the
sweep: thirty days by default, a span you choose, or forever.

Reading a file postpones its expiry, so one that is still in use stays.

There is no un-keep. A file marked to survive survives until somebody removes
it.

## Looking at them

```bash
weft files ls
weft files ls project/
weft files inspect exec/9b81d0a2/chart.png
weft files usage
```

`ls` groups by space and shows the size, the filename, and whether a file is
kept and when it expires. `usage` is the total.

## Getting one out

```bash
weft files download exec/9b81d0a2/chart.png
weft files download exec/9b81d0a2/chart.png -o ~/Desktop/chart.png
```

It streams straight from storage into a temporary file and renames it only
after the size checks out, so a download that fails never leaves you a
plausible half file, and never overwrites a good copy with a broken one.

## Removing them

```bash
weft files rm exec/9b81d0a2/chart.png
weft files rm project/
```

The second form removes a whole space, **including files somebody deliberately
kept**. It tells you how many of those there are before it does anything:

```text
About to remove the whole space 'project/': 48 file(s), 12 of them KEPT
(persisted on purpose).
Type 'yes' to confirm:
```

`--yes` skips the question, and is required when there is no terminal to ask.

## Links

Three kinds, for three audiences, and a node picks the right one:

- a temporary signed link, about fifteen minutes by default
- a link the open internet can fetch, when this install serves a public address
- a link a caller of this install can fetch, which is what a route's answer
  carries in place of a file

They all expire, which is why a stored file travels as a marker rather than a
URL. The marker keeps working; a link does not.

## When a file goes missing

If a download or a node says a key is not there, it was almost certainly swept:
an execution-scoped file that nothing kept, past its window.

`weft files ls` tells you what is actually there. If a program's output needs
to outlive its run, the node that made it has to say so when it stores it.
