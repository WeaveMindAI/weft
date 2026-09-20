# Files and reuse

Four markers pull something from disk into a program. They differ in whether
edits ever flow **back** to the file.

| Marker | Pulls in | Writes back |
|---|---|---|
| `@include("x.weft")` | another program as a group | no |
| `@file("x.txt")` | a file's contents as a value | **yes** |
| `@asset("x.png", Image)` | a file's contents as a value | no |
| `@asset("x.txt", String)` | a text file's contents inline | no |

## `@include`

```weft
triage = @include("triage.weft")

triage.email = inbox.message
alert.data = triage.severity
```

The included file must be exactly one anonymous top-level group. That shape
belongs to included files only: an anonymous group in `src/main.weft` is a
compile error, because the entry file is the program and its top-level
groups carry names.

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

An included file is compiled once, however many places include it. Each
`@include` is a call: when a run reaches `triage`, the values on its ports go
into the file's one body under a frame that names the call site, the body
runs, and its results come back to that site alone. Ten includes of the same
file are one body and ten frames, the way ten iterations of a loop are one
body and ten frames, and the two nest freely: a loop inside an include inside
a loop is simply a deeper stack.

An included file has no name you write or read. Its nodes are named the way
the source reads, through the site: `triage.classify` is the node `classify`
of the file `triage` includes, and only that use of it. That spelling is what
`weft events` prints and what `--node triage.classify` filters on;
`weft run --group triage` runs that call, and a run is cut inside the file
the same way (`--from triage.classify`, `--target triage.classify`): the cut
runs inside that one call, and a frozen example keeps the spelling. Include
the same file from two places and the two read apart (`triage.classify`,
`again.classify`).

## `@file`

```weft
prompt = Text { value: @file("assets/prompts/triage.md") }
```

Reads the file's contents as a config value. With a type:

```weft
triage = LlmParams {
  systemPrompt: @file("assets/prompts/triage.md")
}
```

A marker is a constant like any other, so it goes wherever a literal goes,
in either spelling: [Literals on a connection line](syntax.md#literals-on-a-connection-line).
The type defaults to `String` and can be any type whose value is text
(`@file("n.txt", Number)` casts the file's text, and a text that will not
cast is a compile error on that line).

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

`@asset` always names its type, because the type is what the value carries:
a file value holds exactly one marker (`Image`, `Video`, `Audio`, or `Blob`),
and nothing ever guesses it from the name or the bytes. `@asset("a.png")` is
refused and the message spells the fix; so is `File` or `Media`, which leave
the kind open. Declare `Blob` for bytes of any shape.

With a file type the file resolves through the build's asset sync, and its
bytes never ride the compile. The sync reads the file's first bytes and holds
them to the declaration: an `Image` over an mp3, or over bytes with no
signature weft knows, fails the build naming the file and both kinds. `Blob`
checks nothing. A file-typed `@asset` from a URL is checked the same way when
the worker fetches it at run time, and one picked from stored files against
the kind the upload recorded.

With a text type (`String`, `Number`, a JSON shape) `@asset` puts the
file's text in the value, like `@file`, with two differences: it is
read-only, and the file may sit anywhere (a path outside the project,
a URL, a stored file), since the build reads it, once, and casts it; the
graph shows the source, not the text. In the editor, the badge next to
the field flips a text-backed value between `@file` and `@asset`.

Several files go in a list, which is how a port that takes many (an
email's attachments, the media on an LLM call) is written:

```weft
send.attachments = [@asset("assets/report.pdf", Blob), @asset("assets/logo.png", Image)]
```

A marker is a value, so it sits wherever a value sits, and each one in
that list is synced and resolved exactly like a marker standing alone.

The source can also be:

- a path outside the project, for local runs,
- an `http(s)` URL, which is never uploaded; a file-typed one the worker
  fetches at run time, a text-typed one the build fetches once,
- a stored runtime file's short address, `project/<project-id>/<file-id>`,
  picked from the editor's stored-files browser.

`@file` keeps pure disk paths: a project directory literally named
`project/` stays readable through it, and a URL is refused (there is no file
to write edits back to; use `@asset`).

A path in `@file` or `@asset` is relative to the project root wherever it
is written: `@asset("assets/logo.png", Image)` names the same file from
`src/main.weft` and from any included file. There is nothing above the root
a relative path could mean, so `../` out of the project is refused. A file
outside the project is named where it sits, by an absolute path or one
under `~` (your home directory, expanded at compile), and only for local
runs; that is what the editor's file picker writes for a file picked from
outside, so a large file is never copied into the project. An `@include` path is the one exception: it is relative to the file
that writes it, the way an import is, so `@include("../lib/auth.weft")`
reaches a sibling folder.

### The asset sync

Right before every build, weft uploads new or changed files and puts their
stored references into the compiled workflow. Identical content shares one
uploaded copy, so an unchanged file is not uploaded again.

Uploaded files stay available while the current workflow uses them. An
upload starts on a 30-day countdown, and a successful build clears it for
every file the workflow references; a build that failed after uploading
leaves its files to expire on their own. Once a successful build or status
check sees that a file was replaced or removed, its old uploaded copy gets
the same 30-day countdown. Opening or using that copy
restarts the countdown; building or checking status again does not. Using
the file in the current workflow again removes the countdown.

A waiting run keeps the reference to its original file. Waiting alone does
not extend the file's life: after 30 days without access, the old copy can
expire. If that run later needs it, the node fails with the file's name,
an explanation that it may have expired or been deleted, and instructions
to upload or create the file again and start a new run. It does not silently
skip the node. Files created by nodes keep their own chosen lifetime rules.

Your node code never sees any of this. At run time the value on the port is an
ordinary media value, and inside the running node its marker also carries a
`url` minted for that firing (an hour), so a Python snippet or a provider that
only takes URLs can fetch the bytes straight off it. The link never leaves the
node: what it emits is the stored form again, and the journal never holds one.

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
under `nodes/`, or beside the code under `src/`, is available by its `type`
name. The catalog knows a node by the `metadata.json` at its folder's root
and a package by its `package.toml`, in either tree, and the two trees form
one catalog, so a type name is unique across them. `nodes/` is where the
standard library lives and where a node several modules share goes; a node
one module alone uses sits next to that module's file.

```
my-project/
  src/
    main.weft
  nodes/
    base_catalog/     the standard library, copied in at `weft new`
    reply/            a node you wrote
    scoring/          a package you wrote
```

## The whole folder

A project is laid out like any other language's, so nothing here should be
new:

```
my-project/
  weft.toml           the manifest: name, id, version
  src/
    main.weft         the entry point
    triage.weft       a module: one group per file, pulled in by @include
    billing/          a package of modules, grouped by what they are about
      charge.weft
  nodes/              dependencies: base_catalog plus your own node types
  assets/             anything pulled in by @file or @asset (prompts, scripts, images)
  examples/           frozen runs, from `weft freeze`
  front/              a frontend if you have one, with its own toolchain; weft ignores it
  layouts/            generated: where the editor put each node
  .weft/              generated: build state
```

Start with `src/main.weft` alone; an image workflow or a small bot never
needs more. A group earns its own file the way a module does elsewhere: it
got big, or two places use it. Folders under `src/` are yours to name by
topic, the same call you make in any repo; nothing about the graph's nesting
dictates them. A node one module alone uses sits beside that module
(`src/billing/charge.weft` next to `src/billing/stripe_charge/`), and the
catalog finds it there exactly as it does under `nodes/`.

`weft catalog update` re-syncs `base_catalog/` to the installed weft's standard
library. Pulling a package from git is not built yet, so a package somebody
else wrote gets into your project by being copied there.

Every line of code a build compiles comes from inside the project folder, so
the project directory is portable and upgrading weft cannot silently change
what an existing program does. An `@asset` pointing at a path outside the
project is the one thing a build reaches for elsewhere, which is why it is for
local runs and does not travel with the project.
