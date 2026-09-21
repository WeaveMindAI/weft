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

`@asset` is the exception: it may name a file anywhere on your machine by an
absolute path, or under `~`, and it is read from where it sits and uploaded
when you build.

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

## @asset

```weft
badge = Text {
  value: @asset("https://example.com/terms.txt")
}
stamp = Watermark {
  image: @asset("assets/logo.png", Image)
}
```

The type is **required** for a file, and it has to name one concrete kind:
`Image`, `Video`, `Audio` or `Blob`. `File` and `Media` are refused, because a
value carries exactly one marker and those leave it open. weft will not guess
the kind from the extension or from the bytes.

It is fetched or read when you build, not when you write it, so a URL is
resolved once into your project rather than every run.

In the editor, dropping a file onto a node picks it up in place if it has a
path on disk, and stores it under your project's `assets/` if it arrived as
bytes with no path.

## @include

An included file is one group, and its header is the interface:

```weft
# triage.weft
Group(email: JsonDict) -> (severity: String) {
  # Classify an inbound ticket
  ...
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

### Types do not cross an include

In either direction. An included file has to declare, or get from the catalog,
every type name it uses, and it cannot see the names of the file that included
it.

What does cross is the resolved type itself, because a named type carries its
shape along with its name. So the call site's ports type-check without the
declaration being visible anywhere near them.

If two files use the same type name with different shapes, that is the
`named-type-conflict` error. A named type has one body everywhere.

## Where each belongs

Put a prompt, a SQL query or a script in `assets/` and pull it in with `@file`,
so somebody can open it, read it and edit it without reading your graph.

Put a picture, a sound or a document in with `@asset`.

Pull out a group into its own file with `@include` when it gets big enough to
stop fitting on a screen, or when two places need it. For what that costs at
run time, which is nothing, go and read [groups](groups.md).
