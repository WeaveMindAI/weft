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
| `baked` | Boolean | `false` | On an infrastructure node: the value is worked out when the node's infra is applied, and saved, so a run that reads nothing else from the node uses the saved value instead of running the node. For how, go and read [baking](infrastructure.md#outputs-saved-with-the-infrastructure-baking) |

An output has no `required`. A firing that emits nothing on a port closes it,
which is what downstream reads.

## features

| Key | Type | Default | What it does |
|---|---|---|---|
| `isTrigger` | Boolean | `false` | This node starts executions from outside instead of running inside one. Weft gives it `callsPerMinute`, `callsAtOnce`, `durable`, `recorded` and `keepRunsFor` inputs (and `outlivesCaller` too when it sets `liveConnection`), so do not declare those |
| `liveConnection` | `"http"` or `"websocket"` | none | On a trigger whose run answers a caller holding the connection open: `"http"` for a request waiting on its response (a route), `"websocket"` for a socket. It also gets `callsPerMinutePerCaller` |
| `answersCaller` | `"whole"`, `"stream"` or `"end"` | none | This node answers its run's live caller. `"whole"`: the response in one go (a Reply). `"stream"`: the response head, then the body piece by piece (a Stream). `"end"`: the end of the exchange (a Close). Set it on your own node too if it answers through the ctx, so a route's "never answers its caller" warning counts it |
| `calledFromOutside` | Boolean | `false` | On a trigger somebody outside calls whose call ends there (a form somebody submits). It also gets `callsPerMinutePerCaller`. A `liveConnection` trigger is called from outside already, so it never sets this; a trigger that picks its events up itself (a schedule, a feed, a provider's push) has no caller and does not either |
| `catchErrors` | Boolean | `false` | Set it when the node reaches outside and a program may want to handle its failures. Weft gives it an `error` output, and when that output is wired the failure's message goes there; unwired, the failure stops the run. The node needs no error handling of its own. Declaring its own `error` output is refused when the node loads. For the details, go and read [letting the program handle a failure](values-and-emission.md#letting-the-program-handle-a-failure) |
| `pure` | Boolean | `false` | Set it when the node's body does nothing outside its run that weft does not put on record first: no network, no connection, no `ctx.run`, no `ctx.await_signal`, no tag, no storage beyond the run's own files. It may answer its caller (a durable run's answer leaves only once its record is written), read stored files, store files for this run alone, hand its caller links to them, and use a bus to another node of the run. A durable run then starts its step without waiting for the database, and may run it again after a crash (for when, go and read [surviving a restart](durable-execution.md#when-the-worker-dies-mid-step)). A pure node that reaches outside through the ctx anyway fails at that call, naming this flag; weft cannot see a body that opens a socket or a file on its own, so if you mark one of those pure, a durable run may do what it does twice after a crash. `Text`, `JsonObject`, `Switch`, `Route` and `Reply` are pure; `HttpRequest` and `PostgresExecuteQuery` are not, because each one reaches outside the run on its own |
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
| `code` | `language` | A code box with highlighting. `language` is one of `python`, `javascript`, `sql`, `json`, and any other word fails to load. `json` also fits a structured input (`JsonDict`, a list), edited as JSON text; the others edit a `String` |
| `number` | `min`, `max`, `step` | A number box. `min` and `max` are real rules the runtime enforces, and a whole-number `step` (`"step": 1`) means the input takes whole numbers: a written value is checked when the program compiles, a wired one when it arrives. A fractional `step` is only the arrow keys' increment |
| `checkbox` | | A tick box. The default for `Boolean` |
| `datetime` | | A date and time picker, stored as ISO 8601 |
| `select` | `options`, `free_text` | A dropdown. `options` cannot be empty, and they are a real rule: a written, wired or instance-provided value outside them is refused, and so is a default. If the list is only suggestions (model ids, voices, anything the provider adds to), set `"free_text": true`: the editor still offers the list, lets the user type anything else, and nothing is refused |
| `multiselect` | `options`, `free_text` | Multiple choice over a `List[String]`, each item held to `options` the same way, unless `free_text` is set |
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
`{custom_outputs}` filled in, plus the three the conditions below name), `level` (`structural`, the default, or
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
| `run_reaches` | `direction`, `with` | Does this node's run hold a node `with` a feature. `downstream`: every run this node starts holds one. `upstream`: this node is in the run of one |
| `downstream_of` | `with` | Is a node `with` a feature wired upstream of this one, so it runs first. `{with}` in the message names those nodes |
| `per_instance` | | Does this node exist once per instance: marked `@per_instance`, with an `@instance_filled` field, or reading one of those (a group that receives a per-instance value on any port counts for everything reading its ports). `{per_instance_reason}` in the message says why, path included: "it reads 'blender'", or "it sits inside group 'work', which receives 'blender'" |
| `input_names` | `port`, `names` | Is every name written on this input something of this program. The names are the String, each String of a list, or each key of an object. `names` says what they name: `{"node": {}}` is a node spelled the way the program writes it (`bridge`, `one.bridge`), narrowed with `role` (`infra` or `trigger`) and `per_instance` (`true` or `false`); `{"field": {}}` is a node's field written `node.field` (`send.model`), narrowed with `instance_filled` (`true` or `false`). Nothing written is fine; a wired name is only known at run time, so it is not checked. `{names}` in the message lists the ones that fail |
| `all` / `any` | `of` | Every one, or at least one |
| `not` | `of` | The opposite |

`with` picks nodes by one feature they declare, never by type, so your own
node counts the same as a catalog one: `{"answersCaller": true}` is any node
that answers its caller, `{"answersCaller": ["stream"]}` only a Stream-like
one, `{"liveConnection": ["http"]}` a trigger with an HTTP caller.

Two examples from the catalog. A route warns when nothing in its run answers
its caller, and refuses to become per instance, since its public address is
one for everybody:

```json
{ "when": { "kind": "not", "of": { "kind": "run_reaches", "direction": "downstream", "with": { "answersCaller": true } } },
  "then": { "message": "Route '{id}' starts a run that never answers its caller...", "severity": "warning" } }
{ "when": { "kind": "per_instance" },
  "then": { "message": "Route '{id}' would exist once per instance, because {per_instance_reason}..." } }
```

A node that starts an instance's copy of an infra node refuses a name that is
no such node, and `SetInstanceValues` refuses a key that is no field an
instance fills:

```json
{ "when": { "kind": "not", "of": { "kind": "input_names", "port": "node", "names": { "node": { "role": "infra", "per_instance": true } } } },
  "then": { "message": "StartInstanceInfra '{id}' names {names} in `node`...", "port": "node" } }
{ "when": { "kind": "not", "of": { "kind": "input_names", "port": "values", "names": { "field": { "instance_filled": true } } } },
  "then": { "message": "SetInstanceValues '{id}' gives {names} in `values`...", "port": "values" } }
```

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
leave it empty on the node in your program, and the node claims every method.

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
