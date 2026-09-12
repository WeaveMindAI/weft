# Files and reuse

Three markers, for three jobs:

| Marker | Use it for |
|---|---|
| `@include("reply.weft")` | Reusing a group from another `.weft` file |
| `@file("prompt.md")` | Text you want to edit either on disk or in the graph |
| `@asset("photo.png", Image)` | A file the program uses, read-only in the graph |

## Reuse a group

Put one unnamed top-level group in `clean-text.weft`:

```weft
Group(text: String) -> (result: String) {
  # Remove whitespace around the message
  trim = ExecPython(text: String) -> (out: String) {
    text: self.text
    code: "return {'out': text.strip()}"
  }
  self.result = trim.out
}
```

Then pull it in:

```weft
message = Text { value: "  hello  " }
clean = @include("clean-text.weft")
clean.text = message.value
show = Debug { data: clean.result }
```

`clean` gets the group's ports, and the name belongs to this use of the file.
The compiler expands the group when it builds. In the graph you see its
interface, and you can open the file to work on its insides.

An included file holds that one group and nothing else: no loose steps, no
connections beside it, and no name on the `Group`. Nested includes go inside
the group, and a chain of includes cannot come back round to a file already
being included.

Paths are relative to the file the marker is in, so `@file("prompt.md")` inside
`parts/reply.weft` reads `parts/prompt.md`. An included file also cannot reach
above its own directory: `parts/reply.weft` cannot include `../shared.weft`,
even inside the same project, and a symlink will not get you round it.

An included file sees its own type declarations and the catalog's, not the
types of the file that included it. For that, read
[giving a type a name](types.md#giving-a-type-a-name).

## Keep a prompt in its own file

```weft
prompt = Text { value: @file("prompts/reply.md") }
```

The text is read in at parse time. In the graph you can edit it right on the
step, and the editor writes your change back to `prompts/reply.md` while
leaving the `@file` marker in your source. Empty the field and it empties the
file.

`@file` gives you a `String` unless you ask for something else, and it can be
anything that survives a round trip through text:

```weft
count: @file("limit.txt", Number)
```

If the text will not convert, you get an error at the reference. `@file` is for
local text files only: it refuses URLs and binary files, because the editor
needs something it can write back to.

## Use a file without editing it from the graph

`@asset` is the read-only version:

```weft
prompt = Text { value: @asset("prompts/reply.md", String) }
```

For local text, the badge beside the field switches between `@asset` and
`@file`, which decides whether editing in the graph writes to disk.

For a picture, name the kind:

```weft
show = Debug { data: @asset("assets/photo.png", Image) }
```

`@asset` always wants an explicit type, and a file-valued one has to be
`Image`, `Video`, `Audio` or `Blob`. `Media` and `File` are too vague and are
refused. Several files go in a list.

Dragging a file into a file field saves the bytes under `assets/` and writes
the marker for you. The native file picker instead leaves the file where it is
and writes a reference to that path.

## When the content is read

| Reference | Read |
|---|---|
| Local text, `@file` or `@asset` | While parsing, and converted to the declared type |
| Local file-valued `@asset` | Just before the build, when changed assets are uploaded |
| URL or stored file, with a text type | During the build, fetched and converted once |
| URL with a file type | At run time, when the value is resolved |
| Stored file with a file type | During the build, checked against the recorded kind |

An asset URL can be HTTP or HTTPS. A stored file looks like
`project/<project-id>/<file-id>`, and the editor can insert one from its
stored-files browser. Neither has a local file to edit, so neither can switch
to `@file`.

A local file-valued asset is allowed to sit outside the project, unlike text
references. Anyone else opening your project will need those files too.

Before uploading a local asset, weft checks its bytes against the kind you
declared, and a mismatch fails with the file's name. `Blob` takes anything.

What travels through the graph is a reference, a storage key or the original
URL, never the bytes. For how long uploaded assets stay readable to old runs,
read [files at run time](../running/files.md).

## Reuse your own steps

Reusable step types live under `nodes/`:

```text
my-project/
  main.weft
  clean-text.weft
  nodes/
    base_catalog/
    reply/
    scoring/
```

`weft new` puts the standard steps in `base_catalog/`. Keep yours anywhere
else under `nodes/`, because `weft catalog update` throws that folder away and
copies fresh ones in.

To share several step types that use common Rust code, read
[packaging](../nodes/packaging.md).
