# metadata.json

Every node has one, beside its `mod.rs`. It declares the node's name, its ports,
and everything the editor and the compiler need to know about it without
reading your Rust.

Unknown keys are refused at every level, so a typo fails the build rather than
being quietly ignored. Types are parsed when the file loads, so a misspelled
type name fails then too, not on some later run.

Three keys are required: `type`, `label` and `description`.

## The top level

| Key | Type | Default | What it does |
|---|---|---|---|
| `type` | String | **required** | The node's catalog id. Has to be a valid Rust identifier |
| `label` | String | **required** | The name shown in the editor |
| `description` | String | **required** | One line, shown to people and given to the AI builder |
| `tags` | List[String] | `[]` | Search words for the node picker |
| `icon` | String | none | A lucide icon name |
| `color` | String | none | Hex, a CSS variable, or a palette token |
| `inputs` | List | `[]` | Everything the node takes, wired data and written settings alike |
| `outputs` | List | `[]` | The ports it emits on |
| `requires_infra` | Boolean | `false` | The node provisions containers and must implement `provision_infra` |
| `images` | List[String] | `[]` | Directories under the package root, each holding a Dockerfile to build |
| `publishes` | String | none | The service whose connection this node hands out itself. Needs an `Access` output too |
| `features` | Object | all off | Small flags about what the node is |
| `display` | Object | none | What to render on the node's body after a firing |
| `validate` | List | `[]` | Rules the compiler checks against the graph |
| `portsFromConfig` | Object | none | Says this node's ports come from a list in its own config |
| `types` | Map | `{}` | Named types this node contributes to the project |
| `firesWith` | Map | `{}` | For a trigger: the shape of the event it wakes with |
| `claimsRoute` | Object | none | Which config fields hold the public address this node claims |
| `accessApps` | Map | `{}` | Public OAuth apps this node ships, by service |
| `service` | Object | none | The connect recipe, on an access node. Go and read [declaring a service](../connections/declaring-a-service.md) |

## inputs

| Key | Type | Default | What it does |
|---|---|---|---|
| `name` | String | **required** | The port name, unique among inputs |
| `type` | type string | **required** | What it takes |
| `required` | Boolean | `false` | A firing with nothing here skips the node. Write it only when true |
| `accepts` | List | both | `["wire"]` refuses a written value, `["literal"]` refuses an arrow. A `Bus` or `Generator` port is wire-only whatever you say |
| `widget` | Object | from the type | The editor control |
| `default` | any | none | What the runtime supplies when nothing drives the input. Never written into source |
| `label` | String | the name | The field's name on the form |
| `placeholder` | String | none | Hint text in the empty field |
| `description` | String | none | A sentence next to the field |
| `requiresScopes` | List[String] | none | Permissions this consumer needs on the wired connection. `Access` inputs only |
| `requiresValues` | List[String] | none | Stored values it needs. `Access` inputs only |

## outputs

| Key | Type | Default | What it does |
|---|---|---|---|
| `name` | String | **required** | The port name |
| `type` | type string | **required** | What it emits |
| `description` | String | none | A sentence next to the port |

An output has no `required`. A firing that emits nothing on a port closes it,
which is what downstream reads.

## features

| Key | Type | Default | What it does |
|---|---|---|---|
| `isTrigger` | Boolean | `false` | This node starts executions from outside instead of running inside one |
| `oneOfRequired` | List[List[String]] | `[]` | Each inner list is a group where at least one port must arrive, or the node skips |
| `canAddInputPorts` | Boolean | `false` | Source may declare extra inputs on it |
| `canAddOutputPorts` | Boolean | `false` | Source may declare extra outputs |
| `optionalCustomInputs` | Boolean | `false` | Ports people add are optional rather than required |
| `customInputType` | type string | none | The type every added input takes. Naming one variable makes them all one type |
| `showDebugPreview` | Boolean | `false` | Render the last output as JSON on the node body |
| `liveEndpoint` | String | none | The declared endpoint the dispatcher proxies `/live` to |
| `castPorts` | `{input, output}` | none | Declares the node a checked cast between those two ports |
| `hidden` | Boolean | `false` | Keep it out of the picker and out of `describe-nodes` |

## display

| Key | Type | What it does |
|---|---|---|
| `kind` | `"media"` or `"link"` | `media` renders by type: images inline, a real player for audio and video, a file card otherwise. `link` is always a file card with a download button |
| `input` | String | Show this input's value |
| `output` | String | Show this output's value |

Exactly one of `input` and `output`, never both and never neither.

## widget

Every widget, by its `kind`:

| `kind` | Other keys | What it draws |
|---|---|---|
| `text` | | One line of text. The default for `String` |
| `textarea` | | A wrapping box. The default for anything JSON-shaped |
| `code` | `language` | A code box with highlighting |
| `number` | `min`, `max`, `step` | A number box. `min` and `max` are real rules the runtime enforces, and a whole-number `step` means the input takes whole numbers |
| `checkbox` | | A tick box. The default for `Boolean` |
| `datetime` | | A date and time picker, stored as ISO 8601 |
| `select` | `options` | A dropdown. `options` cannot be empty |
| `multiselect` | `options` | Multiple choice over a `List[String]` |
| `text_list` | | Short strings added one at a time. The default for `List[String]` |
| `password` | | A masked box |
| `file_drop` | `accept`, `type`, `multiple` | A file picker that writes an `@asset(...)` reference |
| `entry_list` | | Builds the config list this node's ports come from. Only legal on the input `portsFromConfig` names |
| `access` | | The connection picker. Write just `{"kind": "access"}`: the service and whether it is optional are filled in from your `service` recipe |
| `remote_select` | `access`, `sources`, `depends_on`, `free_text` | Pick a resource on the connected service |

Leave `widget` out and weft picks one from the type: a file type gets
`file_drop`, `Boolean` gets `checkbox`, `Number` gets `number`, `List[String]`
gets `text_list`, `String` gets `text`, and everything else gets `textarea`.

### remote_select sources

| `kind` | Keys | Where the options come from |
|---|---|---|
| `granted` | `from`, `label`, `value` | The connection row itself, recorded at sign-in. Costs no call |
| `list` | a `Lookup`, plus `requires` | Calling the service and enumerating |
| `picker` | `script`, `code`, `grants`, `mime_types` | The provider's own chooser, opened on a weft page |
| `from_url` | `pattern` | The person pastes a link and the first capture group is the id |

A `Lookup` takes `get` (a URL with `{query}` in it), `items` (a dotted path to
the array), `label`, `value`, optional `page` (`cursor_param` and
`cursor_path`), and `public` when the call carries no credential at all.

## validate

Each rule is `{ "when": <condition>, "then": <diagnostic> }`.

The diagnostic takes `message` (with `{id}`, `{port}`, `{field}` and
`{custom_outputs}` filled in), `level` (`structural`, the default, or
`runtime`), `severity` (`error` by default, or `warning`, `info`, `hint`), and
optionally `port` or `field` to say what the finding is about.

A `structural` rule is checked every time the project is parsed or built. A
`runtime` rule is only checked by `weft validate` and by the editor just before
you Run, so a half-wired sketch still builds.

The conditions are a closed list:

| `kind` | Keys | What it asks |
|---|---|---|
| `input_satisfied` | `port` | Does this input have a value, from a wire or written in |
| `input_wired` | `port` | Does it have a wire specifically |
| `output_wired` | `port` | Does anybody downstream read this output |
| `input_source_type` | `port`, `equals` | Do all wires into this port come from that node type |
| `config_present` | `field` | Is this field set to anything non-null |
| `config_nonempty` | `field` | Is it set and not blank or empty |
| `config_equals` | `field`, `equals` | Does it equal this exact value |
| `config_in_set` | `field`, `values` | Is its string one of these |
| `config_matches` | `field`, `regex` | Does its string match. False if absent, not a string, or the regex is broken |
| `custom_outputs_declared` | | Did the source add outputs beyond the metadata's own |
| `run_reaches` | `direction`, `types` | Does this node's run contain one of these types, upstream or downstream |
| `all` / `any` | `of` | Every one, or at least one |
| `not` | `of` | The opposite |

## portsFromConfig

For a node whose ports come from a list the author writes, like a form's fields
or a switch's cases.

| Key | Default | What it does |
|---|---|---|
| `field` | **required** | The config key holding the list. Must be a declared input |
| `matchInput` | none | The input the entries are matched against, when only one entry wins |
| `specs` | **required** | The entry kinds you accept, and the ports each one adds |

Each spec takes `kind` (**required**), `keyField` (default `"key"`, the entry
key holding the port name), `label`, `render`, `fields`, `catchAll` (at most
one, and it goes last), `addsInputs` and `addsOutputs`.

A port template is `{ "nameTemplate": "...", "portType": "..." }`, with `{key}`
replaced by the entry's key.

A spec field is `{ "key", "label", "required", "shape", "valueType",
"widget" }`, where `shape` is one of `typed` (needs `valueType`), `value`,
`valueList`, `number`, `element` or `regex`. Every shape but `typed` needs
`matchInput` set.

## types

A flat map from a name to a type. Nominal and project-wide: any node's ports
can use a name any node declared. The same declaration in two files is fine and
absorbs, so a package root can declare its shared types once.

```json
"types": { "ChatMessage": "{ role: String, content: String }" }
```

## firesWith

For a trigger. A flat map from field name to type, saying exactly what the
event carries. A `?` goes on the **name**, not the type:

```json
"firesWith": { "chatId": "String", "caller?": "JsonDict" }
```

It is exhaustive both ways. A payload missing a field you declared without `?`
is refused, and so is a payload carrying a field you never declared, at the top
level and nested.

## claimsRoute

`pathField` names the config field holding the route pattern, and the optional
`methodField` names the one holding the HTTP method. Leave the method out, or
leave it empty on the instance, and the node claims every method.

## accessApps

Public OAuth applications your node ships, keyed by service. Each takes
`label`, `client_id`, and any extra field the service's recipe asks for.

A `client_secret` here is refused. Node metadata is source, and source never
holds secrets. An app with a secret belongs in
[the apps file](../connections/the-apps-file.md).

## Package defaults

A package root can hold a partial `metadata.json` whose keys every member
inherits. Members win, key by key, at the top level only, with no deep merge: a
member's `types` replaces the package's rather than adding to it.

Three keys can never be defaults, because they are one node's identity:

```text
package-level metadata.json must not set `type`: it is one node's identity, not a package default
```

## Three keys that no longer exist

If you are reading older node code, the loader names these rather than saying
"unknown field":

| What you wrote | What to do now |
|---|---|
| `inputs[].exposure` | Use `accepts`. `["wire"]` refuses written values, `["literal"]` refuses arrows, and leaving it out allows both |
| `outputs[].required` | Remove it. An output has no optionality |
| `features.isOutputDefault` | Remove it. Every node runs when it is reached, and `weft run --target <node>` narrows a run |

## A folder with no mod.rs

That is not an error. The node is discovered, reported as pending, and left out
of the build. A program that never names it builds fine, and a program that does
gets told which folder is waiting for its code. Writing the metadata first and
the Rust second is a supported way to work.
