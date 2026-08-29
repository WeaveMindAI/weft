# Files and reuse

Four markers pull something from disk into a program. They differ in whether
edits ever flow **back** to the file.

| Marker | Pulls in | Writes back |
|---|---|---|
| `@include("x.weft")` | another program as a group | no |
| `@file("x.txt")` | a file's contents as a value | **yes** |
| `@asset("x.png", Image)` | a file's contents as a value | no |
| `@asset("x.txt")` | a text file's contents inline | no |

## `@include`

```weft
triage = @include("triage.weft")

triage.email = inbox.message
alert.data = triage.severity
```

The included file must be exactly one anonymous top-level group:

```weft
# triage.weft
Group(email: JsonDict) -> (severity: String) {
  # Classify an inbound ticket
  ...
}
```

Its ports become `triage`'s ports and you wire it like any node. An include is
the same boundary as an ordinary [group](groups.md), so the same guarantees
hold: children reach each other and `self`, and nothing else.

## `@file`

```weft
prompt = Text { value: @file("prompts/triage.md") }
```

Reads the file's contents as a config value. With a type:

```weft
triage = LlmParams {
  systemPrompt: @file("prompts/triage.md")
}
```

A marker goes wherever a literal goes, so it follows that port's `exposure`
like any other literal: [Literals on a connection line](syntax.md#literals-on-a-connection-line).

**`@file` is bidirectional.** If you edit that field in the editor, the file on
disk changes too. That is the point: a long prompt lives in its own file where
a writer can work on it, and you can still edit it from the node that uses
it.

Because it writes back, it accepts only types that survive the round trip,
which rules out the binary ones.

## `@asset`

`@asset` is the same idea, one direction only: nothing ever writes back.

```weft
send = TelegramSendMedia {
  file: @asset("assets/photo.png", Image)
}
```

With a **file type** (`Image`, `Video`, `Audio`, `Blob`, or the `Media` and
`File` aliases) the file resolves through the build's asset sync, and its bytes
never ride the compile. With no type, or a text type, `@asset` reads inline
exactly like `@file` and differs only in being read-only.

Several files go in a list, which is how a port that takes many (an
email's attachments, the media on an LLM call) is written:

```weft
send.attachments = [@asset("assets/report.pdf", Blob), @asset("assets/logo.png", Image)]
```

A marker is a value, so it sits wherever a value sits, and each one in
that list is synced and resolved exactly like a marker standing alone.

The source can also be:

- a path outside the project, for local runs,
- an `http(s)` URL, which is never uploaded; the worker fetches it at run time,
- a stored runtime file's short address, `project/<project-id>/<file-id>`,
  picked from the editor's stored-files browser.

### The asset sync

Right before every build, weft makes storage mirror exactly what the code
references: it hashes each referenced file, uploads what is new or changed,
deletes what nothing references any more, and the compile substitutes the
resolved stored-file value. The content hash **is** the identity, so an
unchanged file is never re-uploaded and two identical files are one object.

Your node code never sees any of this. At run time the value on the port is an
ordinary media value.

## What lands in source

The source gets one line, with no storage key or encoded blob in it.

```weft
send = TelegramSendMedia {
  file: @asset("assets/photo.png", Image)
}
```

When you drop a file onto a node in the editor, it writes the file under
`assets/` and writes exactly that line, so copying a file there by hand and
typing the line gets an identical result.

## The project's own nodes

Reusing node **types**, rather than graph fragments, is a folder: anything
under `nodes/` is available by its `type` name.

```
my-project/
  main.weft
  nodes/
    base_catalog/     the standard library, copied in at `weft new`
    reply/            a node you wrote
    scoring/          a package you wrote
```

`weft catalog update` re-syncs `base_catalog/` to the installed weft's standard
library. Pulling a package from git is not built yet, so a package somebody
else wrote gets into your project by being copied there.

Every line of code a build compiles comes from inside the project folder, so
the project directory is portable and upgrading weft cannot silently change
what an existing program does. An `@asset` pointing at a path outside the
project is the one thing a build reaches for elsewhere, which is why it is for
local runs and does not travel with the project.
