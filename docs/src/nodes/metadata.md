# metadata.json

Use `metadata.json` to tell weft what your node takes, what it produces
and how to present it in the editor. The compiler reads this declaration
without compiling Rust, so the graph can be checked while someone is
still writing the implementation.

For a complete first example, use [Your first node](your-first-node.md).
This page is the reference for extending that declaration.

## The top level

`type`, `label` and `description` identify the node and are required.
The other fields add ports, controls or capabilities. Unknown schema keys
produce a metadata load error.

| Key | Use it to declare |
|---|---|
| `type` | The unique node type used in source, such as `WordCount` |
| `label` | Its name in the editor |
| `description` | What the node does, shown to people and coding assistants |
| `tags` | Search terms |
| `icon` | A Lucide icon name, such as `Type` |
| `color` | An accent color: a hex value, CSS variable or supported palette token |
| `inputs` / `outputs` | The port declarations below |
| `types` | Named [custom types](custom-types.md) |
| `features` | The behavior flags listed below |
| `display` | A file to show on the node |
| `validate` | Additional declarative checks |
| `portsFromConfig` | Ports derived from a list in the graph's configuration |
| `requires_infra` | Whether the node provisions infrastructure |
| `images` | Local image directories, relative to the package root |
| `publishes` | The service name for a connection the node publishes |
| `service` | The [connection recipe](../connections/writing-a-service.md) owned by an access node |
| `accessApps` | Public OAuth app registrations keyed by service name; confidential client secrets are rejected |

The compiler treats `color` as a presentation hint. An unknown icon name
renders as a square and logs the missing name in the editor console.

For container declarations and image layout, read
[Infrastructure nodes](infrastructure.md).

## Inputs

Put both wired values and configuration in `inputs`. They reach the body
through the same `ctx.inputs` accessors.

```json
{
  "name": "method",
  "type": "String",
  "required": true,
  "default": "GET",
  "widget": { "kind": "select", "options": ["GET", "POST"] },
  "label": "Method",
  "description": "The HTTP verb to use."
}
```

`name` and `type` are required. `required` defaults to false.
`label`, `description` and `placeholder` help someone fill in the input.

When the node takes a set of values chosen by the program's author, such
as a script's variables, enable `features.canAddInputPorts`. Each variable
can then have its own port. In Rust, read those extra values with
`ctx.inputs.custom()`.

### `accepts`

Most inputs accept either a literal or a wire. Narrow that choice only
when the node needs it:

```json
"accepts": ["wire"]
```

| Declaration | Allowed source |
|---|---|
| Omitted | A literal or a wire |
| `["wire"]` | A value produced by another node |
| `["literal"]` | A value written in the source |

`@file` and `@asset` count as literals. A dotted reference in the node's
braces counts as a wire. An input gets one source; supplying two produces
`double-driven-port`.

Live `Bus` and `Generator` handles are wire-only. Inputs the compiler
must read to determine the node's shape, including an access picker and
the list named by `portsFromConfig`, require an inline value. `accepts`
cannot loosen those rules.

### `widget`

If you omit `widget`, the editor chooses a control from the type:
a checkbox for `Boolean`, a number field for `Number`, a single-line
field for `String`, a text list for `List[String]`, and a file picker
for file types. Other values use a text area.

For prose, choose a text area explicitly:

```json
"widget": { "kind": "textarea" }
```

| Kind | Options and behavior |
|---|---|
| `text` / `textarea` | Single-line or multiline text |
| `checkbox` | A Boolean control |
| `number` | `min` and `max` bound values. A positive whole-number `step` requires integers; fractional steps only control the editor increment |
| `select` / `multiselect` | Fixed `options` offered by the editor; use a validation rule or node code for other restrictions |
| `code` | A code editor with a `language` |
| `datetime` | A date and time stored with the selected timezone offset |
| `password` | Mask the displayed text |
| `text_list` | Add and remove short strings |
| `file_drop` | File selection; `type`, `accept` and `multiple` control the accepted files |
| `entry_list` | Edit the entries used by `portsFromConfig` |
| `access` | Pick an account connection |
| `remote_select` | Pick a resource on a connected service |

A password widget only masks the display. It does not make a value safe to
store as configuration. Use an access node for credentials.

If the provider can list channels or models, a `remote_select` saves the
user from finding and copying an ID. Set `free_text: true` when an ID
outside the fetched list is also valid. For its sources and dependent
pickers, read
[Pick a resource inside the account](../connections/using-a-connection.md#pick-a-resource-inside-the-account).

### `default`

The runtime uses `default` when no wire or literal supplies the input.
The editor can display that effective value without writing it into the
source. A required input with a default can therefore be left unwired.

A default does not replace a wired input that closes without a value.
For that distinction, read
[The closed pulse](../language/mental-model.md#how-a-branch-stops-the-steps-after-it).

### `requiresScopes` and `requiresValues`

On an `Access` input, these declare which permissions and stored values
the consumer needs. The source contains a connection reference, so the
compiler alone cannot check the real account's permissions. For the
declarations and checks, read
[Declare the requirements on the input](../connections/using-a-connection.md#declare-the-requirements-on-the-input).

## Outputs

```json
{
  "name": "count",
  "type": "Number",
  "description": "The number of whitespace-separated words."
}
```

Outputs have a `name`, a `type` and an optional `description`.
They have no widget, default or `required` field.

An ordinary port can emit once per firing. If the body finishes without
emitting on that port, the runtime closes it. A downstream node that needs
that value may then skip; an optional input can remain absent. A generator
port can emit repeatedly and closes when its sequence ends.

Leaving a port out of one emission does not close it immediately.
For emission and explicit closure, read
[Reading inputs, emitting outputs](values-and-emission.md).

## `features`

```json
"features": {
  "canAddInputPorts": true,
  "optionalCustomInputs": true
}
```

| Field | Meaning |
|---|---|
| `isTrigger` | The node registers an event source that can start executions |
| `canAddInputPorts` | Source may add inputs beyond the metadata declarations |
| `canAddOutputPorts` | Source may add outputs |
| `optionalCustomInputs` | Created inputs are optional by default |
| `customInputType` | The type used for created wired inputs; a shared variable such as `T` ties them together |
| `oneOfRequired` | Lists of inputs where at least one in each list must have a non-null value; otherwise the node skips |
| `showDebugPreview` | Show the latest value on the node |
| `liveEndpoint` | The named infrastructure endpoint that serves `/live` |
| `castPorts` | The `input` and `output` ports whose types must allow a checked conversion |
| `hidden` | Hide an internal catalog type from the picker and refuse it in authored source |

For example, `"oneOfRequired": [["message", "attachment"]]` lets a node
run with either input. A custom input created from a literal takes that
literal's inferred type; `customInputType` does not override it.

## `display`

To display a file on the node, name its port and which side it is on:

```json
"display": { "kind": "media", "output": "image" }
```

Use `input` instead of `output` for a display sink. Supply exactly one
side and name a declared port; the catalog rejects an ambiguous or missing
port.

`media` shows images and playable audio/video, with a file card for other
formats. `link` always shows the file card. For previews and live status
panels, read [What your node shows in the graph](showing-things-in-the-graph.md).

## Package defaults

A directory with `package.toml` can also have a partial
`metadata.json`. Each member inherits keys it does not define itself:

```text
nodes/my_package/
  package.toml
  metadata.json         shared types or other defaults
  first/
    metadata.json      first node's identity and ports
    mod.rs
  second/
    metadata.json
    mod.rs
```

The merge replaces whole top-level values. If a member defines `types`,
its entire `types` object wins; the loader does not merge its entries
with the package's object.

`type`, `label` and `description` cannot be package defaults.
For how the package's Rust code is shared, read [Packaging](packaging.md).

## Ports that come from a node's own config

A form needs an output for each field the program author adds.
`portsFromConfig` declares how entries in that list become ports.

This fragment adds a String output for each `text_input` entry in
the node's `fields` input:

```json
"portsFromConfig": {
  "field": "fields",
  "specs": [
    {
      "kind": "text_input",
      "label": "Text input",
      "render": { "component": "text" },
      "fields": ["label", "placeholder"],
      "addsOutputs": [
        { "nameTemplate": "{key}", "portType": "String" }
      ]
    }
  ]
}
```

The metadata also needs a declared `fields` input. In the graph, an entry
such as `{ "kind": "text_input", "key": "reason" }` adds a `reason`
output. The default `keyField` is `key`; a switch can choose `port`
instead. Port names must be legal identifiers.

Use `addsInputs` for input ports. Each template substitutes the entry's
name into `{key}`. A `portType` of `T_Auto` gives the entry its own
type variable.

### What a kind asks the author for

The `fields` list describes values the author can supply on an entry.
A bare `"label"` declares an optional String field named `label`.
Use a full declaration for another type, a custom label or a required field:

```json
{
  "key": "options",
  "label": "Options",
  "required": true,
  "shape": "typed",
  "valueType": "List[String]"
}
```

`shape: "typed"` checks the value against `valueType`. You can override
its editor control with `widget`, using the input widget vocabulary.

The spec's `render` supplies the default form renderer. An entry may
override it in the graph; it does not need to repeat it.

### Entries that compete

For switch-like entries, `matchInput` names the input they compare against.
The field shapes can then be relative to that input:

| Shape | Checks |
|---|---|
| `value` | One value compatible with the matched input |
| `valueList` | A list of compatible values |
| `number` | A number |
| `element` | One element of the matched list, or a piece of matched text |
| `regex` | A regular expression that compiles |

Set `catchAll: true` on the fallback kind. The compiler permits one
catch-all entry and requires it to be last. It also rejects entry keys
the kind did not declare.

For complete declarations, read the
[human form metadata](https://github.com/WeavemindAI/weft/blob/mvp/catalog/human/metadata.json)
and [Switch metadata](https://github.com/WeavemindAI/weft/blob/mvp/catalog/logic/switch/metadata.json).

## Additional validation rules

Use `validate` for a constraint that types alone do not express.
Each rule says when to report a diagnostic:

```json
"validate": [
  {
    "when": {
      "kind": "not",
      "of": { "kind": "config_nonempty", "field": "query" }
    },
    "then": {
      "message": "Write a query before running this node.",
      "field": "query",
      "level": "runtime"
    }
  }
]
```

This fragment assumes the node declares a `query` input. The rule checks
its written value, so it is appropriate when that input takes a literal.
It cannot inspect the result of an upstream node.

Each condition uses `kind` plus the fields below.

| `kind` | Other fields | True when |
|---|---|---|
| `input_satisfied` | `port` | The input has a wire or a written value that supplies data |
| `input_wired` | `port` | The input has an incoming wire |
| `input_source_type` | `port`, `equals` | Every incoming wire comes from the named node type; also true with no wires |
| `config_present` | `field` | The written value exists and is non-null |
| `config_nonempty` | `field` | The written value is nonempty; whitespace-only strings count as empty |
| `config_equals` | `field`, `equals` | The written value equals the supplied JSON value |
| `config_in_set` | `field`, `values` | The written string appears in `values` |
| `config_matches` | `field`, `regex` | The written string matches the regular expression |
| `all` / `any` | `of`, a condition list | All or any of the conditions hold |
| `not` | `of`, one condition | That condition is false |

Combine `input_source_type` with `input_wired` when the wire itself is
required.

The default level is `structural`, checked while editing.
`runtime` defers the diagnostic to validation for a run, including
`weft validate`. Severity defaults to `error`; it can also be
`warning`, `info` or `hint`.
