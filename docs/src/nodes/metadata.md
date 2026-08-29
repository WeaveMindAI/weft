# metadata.json

The node's declared surface. Ports, config, presentation, and the handful of
flags the engine reads.

It is data rather than code because the compiler has to know a node's shape
without compiling its Rust, which is what lets the editor draw a graph and the
validator check every wire in milliseconds.

Unknown keys are rejected everywhere, so a typo is a load error rather than a
setting that silently does nothing.

## The top level

| Key | What it is |
|---|---|
| `type` | the node type name. PascalCase. This is the node's identity. |
| `label` | what the editor shows on the box |
| `description` | one paragraph, saying what the node emits |
| `tags` | strings, for search and grouping |
| `icon` | a [Lucide](https://lucide.dev/icons) icon name in PascalCase |
| `color` | a hex string for the node's accent |
| `inputs` | the input ports |
| `outputs` | the output ports |
| `types` | named custom types this node declares. See [Custom types](custom-types.md). |
| `requires_infra` | true for [infra nodes](infrastructure.md) |
| `images` | container image directories, for infra nodes |
| `publishes` | the service name this node hands out a connection to |
| `service` | the whole [service recipe](../connections/writing-a-service.md), for access nodes |
| `accessApps` | a public, secretless OAuth app this project ships |
| `portsFromConfig` | where a node's ports come from, when they come from its own config |
| `features` | boolean-ish flags |
| `display` | what the editor renders inline on the node body |
| `validate` | declarative validation rules |

`features` holds flags. Anything with structure gets its own top-level key.

A name Lucide does not ship renders as a plain square, with the reason in the
console.

## Inputs

```json
{
  "name": "method",
  "type": "String",
  "required": true,
  "exposure": "config",
  "widget": { "kind": "select", "options": ["GET", "POST"] },
  "default": "GET",
  "label": "Method",
  "placeholder": "...",
  "description": "The HTTP verb to use."
}
```

### `exposure`

Where a literal for this input may live. Whether a wire may drive it is a
separate question, answered in the last column.

| Value | Braces literal `M { x: 5 }` | Statement literal `n.x = 5` | Wire |
|---|---|---|---|
| `all` | yes | yes | yes |
| `assignment` | no | yes | yes |
| `config` | yes | no | **no** |
| `wire` | no | no | yes |

`all` is the default for plain data, `assignment` for file types so
`n.image = @asset("i.png", Image)` reads naturally, and `wire` for `Bus`
inputs. Declare `config` for a pure design-time setting, what a select or a
form builder configures, since no type defaults to it. Declare `wire` for an
input that needs a real node rather than a value, such as an inference node's
`provider`.

An input has exactly one driver. Two is `double-driven-port`, and a wire on a
`config` input is `input-not-wireable`.

### `widget`

Overrides the editor control. Absent, the control derives from the type
through one central mapping: file types get a drop control, `Boolean` a
checkbox, `Number` a number box, a `List[String]` the add-one-at-a-time
list, a `String` a single-line box, and everything else a text area
holding the value as JSON text. A String field that really holds prose
(a prompt, a message body) declares `"widget": { "kind": "textarea" }`.

A port that holds SEVERAL files (`List[Audio]`, or a `Media | List[Media]`
that takes one or many) gets the same drop control keeping a list: it
picks several at once, adds one at a time, and writes one `@asset(...)`
per file.

| Kind | For |
|---|---|
| `select` / `multiselect` | a small fixed vocabulary; takes `options` |
| `textarea` | a multi-line box, for a String that holds prose |
| `code` | a code editor; takes `language` |
| `number` | takes `min` / `max` / `step`, enforced by both the editor and the compiler |
| `password` | a masked field |
| `file_drop` | narrows the file filter beyond the type; takes `accept` |
| `text_list` | a list of short text values, added one at a time |
| `entry_list` | build the list a node's ports come from (see below) |
| `access` | the connection picker. See [Using a connection](../connections/using-a-connection.md). |
| `remote_select` | pick a resource inside the connected account |

> **A field whose value names something enumerable is never a bare text
> field.** If the vocabulary is small and fixed, declare `select`. If the
> provider can list the choices (model ids, voices, channels, databases,
> repos), declare `remote_select` so the user searches instead of copying an
> id out of the provider's docs. Add `free_text: true` when a pasted id is
> also valid, for example a model route the list has not caught up with.
> Plain text is for free-form values: a prompt, a URL, a message
> body.

`remote_select` gets its own treatment in
[Using a connection](../connections/using-a-connection.md#picking-a-resource).

### `default`

The value the runtime supplies when nothing else drives the input. Consulted
at run time, rendered by the editor as the effective value, and **never
written into source**. `required` plus `default` is satisfiable with no
driver at all.

### `requiresScopes` and `requiresValues`

Only on `Access` inputs. They state what this node needs from a connection.
See [Using a connection](../connections/using-a-connection.md#declaring-what-your-node-needs).

## Outputs

```json
{ "name": "ts", "type": "String", "required": false,
  "description": "The posted message's timestamp." }
```

Simpler than inputs: no exposure, no widget, no default. A port not present in
a firing's output emits no pulse, which closes it and skips what is downstream:
[the closure rule](../language/mental-model.md#the-closed-pulse).

## `features`

The complete set, `hidden` aside, which only catalog nodes use:

| Flag | Meaning |
|---|---|
| `isOutputDefault` | this node's firing is the deliverable |
| `isTrigger` | this node starts executions from outside |
| `optionalCustomInputs` | ports created by a wire on this node are optional by default |
| `customInputType` | the type every created port takes; a shared variable (`T`) makes them one type |
| `liveEndpoint` | names the endpoint serving `/live` for an infra node |
| `canAddInputPorts` | the `.weft` author may add input ports to this node, by declaring them or by wiring a config key that names no declared port. Without it, an extra port is a compile error. |
| `canAddOutputPorts` | the same for outputs |
| `showDebugPreview` | the editor renders the node's latest output inline on its body |
| `oneOfRequired` | groups of ports where at least one of each group has to arrive, or the node is **skipped**. `[["message", "attachment"]]` means a send needs one or the other. |
| `castPorts` | this node converts a named input into a named output's declared type, checked against the conversion table at compile time |

### `isOutputDefault`

Decide this one per node; there is no safe default. A run starts from the
output nodes and walks upstream, so a node nothing downstream asks for does not
execute at all: this flag is what lets a user drop yours at the end of a chain
and hit run. See
[what actually runs](../language/mental-model.md#what-actually-runs).

Set it true if your node's firing **is** the deliverable:

- it generates an artifact: an image, a video, speech,
- or it performs the outward effect: sends the message, creates the record,
  uploads the file.

Leave it unset for reads, transforms, lookups, and triggers.

Any project can override it per instance with `_is_output` in the node's
config.

## `display`

For a node whose firing produces or receives a file worth seeing.

```json
"display": { "kind": "media", "output": "image" }
```

`kind` is `media`, which renders the file by its own mime type (an image
inline, audio and video with a real player, anything unplayable as a file card
with a save button), or `link`, which renders the metadata and download card
only. There is no flag per media type: a newly playable format is a renderer
change.

Name the port **with its side**, exactly one of `output` (a generator showing
what it made) or `input` (a display sink showing what was wired in), which is
what keeps a node with a same-named input and output unambiguous. Declaring
both, neither, or a port that does not exist is refused when the catalog
loads.

## Package defaults

A package (a directory with `package.toml`) may hold a **partial**
`metadata.json` at its root: defaults every member node inherits.

The merge is top-level and key by key: a member gets each key unless its own
`metadata.json` carries that key, in which case the member's value wins
wholesale. There is no deep merge.

`type`, `label`, and `description` can never be defaults, because they are one
node's identity.

If a package's nodes share a `portsFromConfig` vocabulary, a `types` block, or a
`provider` name, this is where it goes.

```
catalog/human/
  package.toml
  metadata.json        PARTIAL: { "portsFromConfig": {...} }, shared by both
  form_helpers.rs      shared code
  trigger/  metadata.json    HumanTrigger
  query/    metadata.json    HumanQuery
```

## Ports that come from a node's own config

A node can grow its ports from a LIST in its own config: `HumanQuery` from the
form fields somebody configured, `Switch` from its cases. It declares which
config key holds that list, and the entry kinds the list may use, under
`portsFromConfig`. The real ports are derived at compile time from what the
graph author wrote.

```json
{
  "portsFromConfig": {
    "field": "fields",
    "specs": [
      {
        "kind": "approve_reject",
        "label": "Approve / Reject",
        "render": { "component": "buttons", "source": "static" },
        "fields": [
          "label",
          { "key": "approveLabel", "label": "Approve button text", "shape": "typed", "valueType": "String" },
          { "key": "rejectLabel", "label": "Reject button text", "shape": "typed", "valueType": "String" }
        ],
        "addsOutputs": [
          { "nameTemplate": "{key}_approved", "portType": "Boolean" },
          { "nameTemplate": "{key}_rejected", "portType": "Boolean" }
        ]
      },
      {
        "kind": "text_input",
        "label": "Text input",
        "render": { "component": "text" },
        "addsOutputs": [ { "nameTemplate": "{key}", "portType": "String" } ]
      }
    ]
  }
}
```

Every entry names its `kind`, and the port it adds under the key its spec asks
for: `keyField` defaults to `"key"` (a form field), and `Switch` sets it to
`"port"` so a case reads in its own vocabulary. `{key}` is substituted with
that name. `T_Auto` as a port type requests a per-entry type variable.

Derivation reads **only** each entry's `kind` and its port name. It does not
read an entry's `render` from the graph source; that is inherited from the
spec. An entry may override `render`, but it need not, and the editor emits
the minimal entry so the source stays lean.

A port name has to be a legal identifier.

### What a kind asks the author for

A kind lists under `fields` the values an entry of that kind has to carry: a
select's `options`, a case's `value`. Each one declares the SHAPE its value
must have, which is what the compiler holds it to, and the editor draws a
control for it from its widget. Nothing between the metadata and the graph
learns what any key means.

```json
{ "key": "options", "label": "Options", "required": true,
  "shape": "typed", "valueType": "List[String]" }
```

An entry may also be a bare NAME, which is the same declaration with
everything obvious filled in: a one-line String box, keyed and labelled
after the name, optional.

```json
"fields": ["label", "placeholder"]
```

is exactly

```json
"fields": [
  { "key": "label", "label": "Label", "shape": "typed", "valueType": "String" },
  { "key": "placeholder", "label": "Placeholder", "shape": "typed", "valueType": "String" }
]
```

A name of several words reads as words, so `minLength` and `min_length`
both label as "Min Length". Write the whole declaration when the label is
not the key (`approveLabel` wants "Approve button text"), when the value
is not a String, or when it is required.

`shape: "typed"` names a plain weft type in `valueType`. The other shapes are
measured against the input the entries are matched on, so they only make sense
on a list whose entries compete (below): `value` (something that input could
hold), `valueList`, `number`, `element` (one item of a list, or a piece of the
text), and `regex`, which has to compile.

The control comes from the shape unless the field names a `widget`, using the
same widget vocabulary an input port uses. A `List[String]` gets `text_list`,
a `number` shape gets a number box, everything else a text box.

### Entries that compete

When the entries are TRIED IN ORDER and only one wins (a switch's cases, never
a form's fields), the KIND is the test: one kind per way of matching, each
asking for the value it compares against. `matchInput` names the input they are
all matched against, and the kind that takes anything says `catchAll`.

```json
{
  "field": "cases",
  "matchInput": "value",
  "specs": [
    { "kind": "equals", "keyField": "port", "label": "is exactly this value",
      "fields": [{ "key": "value", "label": "Value", "required": true, "shape": "value" }],
      "addsOutputs": [{ "nameTemplate": "{key}", "portType": "Boolean" }] },
    { "kind": "between", "keyField": "port", "label": "between these two numbers",
      "fields": [
        { "key": "min", "label": "Lowest", "required": true, "shape": "number" },
        { "key": "max", "label": "Highest", "required": true, "shape": "number" }
      ],
      "addsOutputs": [{ "nameTemplate": "{key}", "portType": "Boolean" }] },
    { "kind": "otherwise", "keyField": "port", "label": "anything else",
      "catchAll": true,
      "addsOutputs": [{ "nameTemplate": "{key}", "portType": "Boolean" }] }
  ]
}
```

The compiler holds a `catchAll` entry to being unique and last, since anything
after it can never be reached. An entry carrying a key its kind never declared
is a compile error.
