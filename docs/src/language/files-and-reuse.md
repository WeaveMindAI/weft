# Files and reuse

Three markers pull something in from outside the line you are writing.

| | What it brings in | Writes back? |
|---|---|---|
| `@include("triage.weft")` | Another weft file, as one group | No. The editor opens that file instead |
| `@file("assets/prompts/triage.md")` | A file's text, as a value | **Yes.** Editing the field edits the file |
| `@asset("assets/logo.png", Image)` | A file, a URL or a stored file, as a value | No. Pull only |

## Where a path is measured from

This catches people, so it is worth stating on its own.

`@file` and `@asset` are measured **from the project root**, wherever you write
them. `@file("assets/prompts/triage.md")` means the same file in
`src/main.weft`, in `src/billing/charge.weft`, and in a file included from
either.

`@include` is measured **from the file that writes it**, like an import in any
other language. So `@include("../lib/clean.weft")` inside
`src/billing/charge.weft` climbs one level from `src/billing/`.

Both refuse a path that climbs out of the project, and `@include` refuses it on
the spelling before it even looks, so a `../` that escapes is refused whether
or not anything is there.

A path starting with `__catalog/` is measured from neither place: it is
measured from the project's `nodes/` folder, where the standard library and
your shared nodes live, and it may not climb out of `nodes/`. If you want to
include `nodes/my_pkg/triage.weft`, write
`@include("__catalog/my_pkg/triage.weft")`, and that spelling reaches it from
any file in the project. In `@file` and `@asset`, which are already measured
from the root, `__catalog/x` is `nodes/x`, so a file under `nodes/` can name
its neighbours the same way in all three.

If you put a `.weft` file under `nodes/` for others to include, make its
first line a `#` comment saying what it does: `weft describe-nodes --list`
ends with every such file, spelled the way you include it, with that line
beside it.

`@asset` is the one exception to the rule that a path stays inside the
project: it may name a file anywhere on your machine by an absolute path, or
under `~`. For when that works and when it is refused, see
[where an @asset is read](#where-an-asset-is-read).

## @file

```weft
write = LlmInference {
  prompt: @file("assets/prompts/image_brief.md")
}
```

The type is `String` unless you say otherwise, and a `String` is taken
verbatim: no parsing, no trimming. That is the common case, which is a prompt
or a document. Give it another type and the text is parsed and checked:
`@file("limit.txt", Number)`.

**It goes both ways.** The editor shows that field as backed by a file, and
editing it writes the file rather than pasting the text into your `.weft`. So a
prompt lives in a real file that a person can open, and the graph is still the
place you edit it.

That is also why a `@file` cannot name a URL or a file type. There would be
nothing to write back to. The error points you at `@asset`.

The built program does not carry the text. It carries the file's name and a
hash of what the file holds, and the version stores the file, so a 2 MB SQL
script costs the program a few bytes, and a file two versions share is stored
once. The worker that runs the program reads the file and gives the node the
value, as if you had typed it in.

## @asset

```weft
terms = Text {
  value: @asset("https://example.com/terms.txt", String)
}
fal = FalAccess
sharper = FalUpscaleImage {
  account: fal.access
  image: @asset("assets/logo.png", Image)
}
```

The type is **required**, because weft will not guess it from the extension
or from the bytes. For text, it is the type to read the text as
(`String`, `Number`, `JsonDict`, a record). For a file, it names one concrete
kind: `Image`, `Video`, `Audio` or `Blob`. `File` and `Media` are refused:
each covers several kinds, and weft has to know which one the file is.

It is resolved when you build. If it is a URL with a text type, the build
fetches it once and stores the text, which the program names the way it
names a `@file`'s. If it is a URL with a file
type, the node gets the address, and the file is fetched when the run reads it.

### Where an @asset is read

The install builds from a version, so it looks for the file there first. A file the version holds
(anything under `assets/`, `prompts/` and the other folders a
[version](../running/versions.md#a-version) covers) is already stored with the
version, so nothing is read off your disk or uploaded for it.

If the version does not hold the file (an absolute path, `~/...`, or a project
file outside those folders), an install on your machine reads it off your disk
(a relative path from your project folder) and stores it, with a file type and
a text type alike. A cloud install
cannot reach your machine, so it refuses the build:

```text
assets that cannot be used:
  @asset("/home/me/logo.png", Image): '/home/me/logo.png' is not in the version, and this install runs in the cloud, so it cannot read files off a machine: put the file in the project, under `assets/`, and name it from there
```

On an install on your machine, a program building a project that has no
folder can still use an absolute or `~` path, but a relative one has nowhere
to be read from, so that build is refused.

`@file` in your program can only name a file the version holds, because the
install compiles the version.

In the editor, a file you choose with the file picker is used where it is,
and a file you drag onto a node is copied into your project's `assets/` (with
`-1`, `-2` added if the name is taken).

## @include

An included file is one group, and its header is the interface:

```weft
# triage.weft
Group(email: JsonDict) -> (severity: String) {
  # Classify an inbound ticket
  classify = ExecPython(email: JsonDict) -> (severity: String) {
    email: self.email
    code: "return {'severity': 'high' if 'urgent' in str(email) else 'low'}"
  }
  self.severity = classify.severity
}
```

```weft
# main.weft
triage = @include("triage.weft")
triage.email = inbox.message
```

The file has to be **exactly one anonymous group**, with nothing beside it. No
loose nodes, no top-level connections, no top-level includes, not two groups,
and not a named one. The group's header is the file's interface and the file
name is its identity, so anything else would be quietly thrown away, which is
why weft checks rather than assumes.

Includes inside that group's body are fine, and they resolve against that
file's own folder. A file that ends up including itself, through however many
hops, is a compile error naming the cycle.

### Ten includes, one body

A file is compiled **once**, however many places include it. Each `@include` is
a call: the values on its ports go into the one body under a frame naming the
call site, the body runs, and the results come back to that site alone.

Ten includes of the same file are one body and ten frames, the way ten
iterations of a loop are one body and ten frames. The two nest freely, so a
loop inside an include inside a loop is just a deeper stack.

Only the type check runs once per include. If `blur.weft` holds
`Group[T: Media](file: T) -> (out: T)` (`[T: Media]` limits `T` to kinds of
`Media`; without it, `T` takes any type),
you can include it once fed an `Image` and once fed a `Video`, and each `out`
keeps the type that went in. That holds however deep you go: for a group
nested inside the file, for a node's type variable, and for another file the
file includes. If `img` and `vid` are your two includes of `blur.weft`, and it
includes a file as `inner`, `img.inner` and `vid.inner` each get their own
types. The nodes and their code stay one copy each.

Whether a node runs once per instance is decided per include too. A file
included under a per-instance part and also somewhere shared is per instance
only under the first; for what that means, go and read
[programs with instances](../running/instances.md#what-an-instance-fills-instance_filled).

### Naming a node inside one

An included file has no name you write. Its nodes are named through the site:
`triage.classify` is the node `classify` of the file that `triage` includes,
and only that use of it.

That spelling is the same everywhere. It is what `weft events` prints, what
`--node triage.classify` filters on, what `weft run --group triage` runs, and
what the box header shows in the editor. Include the same file twice and the
two read apart, as `triage.classify` and `again.classify`.

Cutting a run works inside a call too, so `--target triage.classify` runs
inside that one call.

### Types reach down into an included file

A type declared above an `@include` is visible inside the file it pulls in, the
same way it is visible inside a group written in place. Declare `Settings` once
at the top of `main.weft`, and `bot.weft` and `facts.weft` can both use it. It
keeps going down: a file `bot.weft` includes sees it too.

It only goes down. A type declared inside a group reaches the includes inside
that group and nothing beside it, and a type an included file declares stays in
that file. Since nothing shadows, an included file can't declare a name its
including file already has.

A file included from several places has to find each type it uses at every one
of them. If one place doesn't have it, the build says so on the file's own line.
When you open an included file on its own, weft reads it with the types of the
first place that includes it, so it looks the way the build compiles it.

The including file doesn't see an included file's types, but the call site's
ports still type-check, because a named type carries its shape along with its
name.

If two files use the same type name with different shapes, that is the
`named-type-conflict` error. A named type has one body everywhere.

## Where each belongs

Put a prompt, a SQL query or a script in `assets/` and pull it in with `@file`,
so somebody can open it, read it and edit it without reading your graph.

Put a picture, a sound or a document in with `@asset`.

Pull out a group into its own file with `@include` when it gets big enough to
stop fitting on a screen, or when two places need it. For what that costs at
run time, which is nothing, go and read [groups](groups.md).
