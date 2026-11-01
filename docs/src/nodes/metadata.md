# metadata.json

Every node has one, beside its `mod.rs`. It declares the node's type, its ports,
and everything the editor and the compiler need to know about it without
reading your Rust.

If you write a key weft does not define, the node fails to load: it is left out
of the catalog, with a warning naming the error, and a program that uses it
fails to compile, saying the node failed to load and why. Places that hold
someone else's names, like `authParams` or an input's `default`, take any key.
Under `accessApps`, an entry takes `label`, `client_id` and the registration
keys its service declares; for how that is checked, go and read
[accessApps](#accessapps). A port's type is
parsed when the file loads, so a misspelled type (`Strng`) fails then too.

Three keys are required: `type`, `label` and `description`.

## Spelling

The node's `type` and any types you declare under `types` are PascalCase
(`WordCount`), and a type variable under `typeParams` is `T`, or `T` and a
number (`T1`). Every key weft defines (`requiresInfra`) is camelCase, and so
is every port, setting and record field you name. So are the fields under `firesWith`, and the names under each
event's `fields` in a service's `events` (for what those are, go and read
[Events from a service](../connections/events.md#what-a-topic-declares)).
The names of the values a connection stores (`imapHost`) are camelCase too,
both where a service declares them (the `name` of each entry in `fields`, `captures` and
`registrationFields`) and where an input asks for them in `requiresValues`.

Two kinds of key are snake_case: a service's event topics (`drive_changes`)
and the keys under `accessApps` (`google`, the service's name). They are
snake_case because a node also writes them as values (a trigger picks a topic
by name, and a node names its service as `"service": "google"`), and every
value from a fixed list is snake_case. A name somebody else chose keeps their
spelling, like an OAuth parameter under `authParams` (`access_type`), or a
field of a request body you send to a provider. Inside an `accessApps` entry,
`client_id` keeps OAuth's spelling, and any other registration key is spelled
exactly as the service names it in its `registrationFields`.

If you write a key weft renamed in its old snake_case spelling
(`requires_infra`), the node fails to load, and the error names the new
spelling: "`requires_infra` is spelled `requiresInfra` now ... Rename it". In
a package root's `metadata.json` (the defaults its members inherit), the error
is reported against the root's file, and the package's members are left out.
For how those defaults work, go and read
[Package defaults](#package-defaults).

If any other name in a `metadata.json` breaks these rules, the node still loads
and you get a catalog warning. Your project's own nodes get this check
too. It also covers the field names and port templates a `portsFromConfig`
spec declares. The check never reads a node's data, so it says nothing about a
`select`'s `options` or an input's `default`.

If you have the graph open in the editor, these warnings show up in the graph
view's problems list as "node spelling warning". On the command line,
`weft validate < src/main.weft` and `weft describe-nodes --list` print them on
stderr, and the JSON from `weft validate` and from bare `weft describe-nodes`
lists them under `warnings`, each as `{"kind": "spelling", "message": ...}`.
On stderr a warning looks like this, and the JSON `message` is the same line
without `warning: `:

```text
warning: <NodeType> (<path to metadata.json>): <message>
```

If a name is off in a package root's `metadata.json`, including a port it gives
its members and that port's record fields, the warning names the root, not
each member, and starts `package '<name>' defaults (<path>)` instead of a node
type. A declared type's fields are checked once, where it is declared.

If you are changing a node that ships with weft, the catalog's `casing` test
fails on any such warning. For the command, go and read the Code style section
of [CONTRIBUTING](https://github.com/WeaveMindAI/weft/blob/main/CONTRIBUTING.md#code-style).

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
| `metadataPorts` | List | `[]` | Ports whose value you write here, which no run computes. Go and read [metadataPorts](#metadataports) |
| `requiresInfra` | Boolean | `false` | The node provisions containers and must implement `provision_infra` |
| `images` | List[String] | `[]` | Directories under the package root, each holding a Dockerfile to build |
| `publishes` | String | none | The service whose connection this node hands out itself. Needs an `Access` output too |
| `features` | Object | all off | Small flags about what the node is |
| `display` | Object | none | What to render on the node's body after a firing |
| `validate` | List | `[]` | Rules the compiler checks against the graph |
| `portsFromConfig` | Object | none | Says this node's ports come from a list in its own config |
| `types` | Map | `{}` | Named types this node contributes to the project. Go and read [custom types](custom-types.md) |
| `typeParams` | Map | `{}` | What each type variable of the ports may stand for. Go and read [typeParams](#typeparams) |
| `placeholdersAreInputs` | Object | none | Which text setting's placeholders name the node's inputs. Go and read [placeholdersAreInputs](#placeholdersareinputs) |
| `firesWith` | Map | `{}` | For a trigger: the shape of the event it wakes with |
| `claimsRoute` | Object | none | Which config fields hold the public address this node claims |
| `accessApps` | Map | `{}` | Public OAuth apps this node ships, by service |
| `service` | Object | none | The connect recipe, on an access node. Go and read [declaring a service](../connections/declaring-a-service.md) |

## inputs

| Key | Type | Default | What it does |
|---|---|---|---|
| `name` | String | **required** | The port name, unique among inputs |
| `type` | type string | **required** | What it takes |
| `required` | Boolean | `false` | Write it only when true. `true`: the node cannot run without it, and a program cannot make it optional. Left out: unwired, the node runs without it; wired, it waits for the wire and skips if the wire closes, and a program can change this where it uses the node. For how, go and read [optional and required inputs](../language/syntax.md#optional-and-required-inputs) |
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
| `narrowable` | `true` or a type string | none | For an output whose shape only the program knows (a model's JSON answer, a request's body, a query's rows): type it broadly and set this, and a program declares what it holds (`-> (response: Verdict)`). `true` lets a program declare any type that fits inside `type`. A type string lets it declare anything that fits inside that type instead, which may be wider than `type`. Each value is checked against the declared type at run time, and one that does not fit fails the node, a failure the program can catch on `_error`. Without it, a program can only restate the output's own type |
| `baked` | Boolean | `false` | On an infrastructure node: the value is worked out when the node's infra is applied, and saved, so a run that reads nothing else from the node uses the saved value instead of running the node. For how, go and read [baking](infrastructure.md#outputs-saved-with-the-infrastructure-baking) |

An output has no `required`. A firing that emits nothing on a port closes it,
which is what downstream reads.

Do not declare `_error`: weft gives every node that output for its failures,
and the loader refuses a `metadata.json` that declares it. For what
comes out on it, go and read [which failures a program can
handle](values-and-emission.md#which-failures-a-program-can-handle).

## metadataPorts

A metadata port holds a value written here, by hand, and the node never
computes it. A wire a program draws from it is a metadata wire: before the
program runs, the compiler writes the value into the input the wire points at.
For what that is for, go and read [metadata ports](../language/groups.md#metadata-ports).

```json
"metadataPorts": [
  { "name": "tool", "type": "{ name: String }", "description": "What this node is, for a model", "value": { "name": "pick" } }
]
```

| Key | Type | Default | What it does |
|---|---|---|---|
| `name` | String | **required** | The port name. It may not be the name of another of the node's ports: an input, an output, another metadata port, `_error`, `_should_flow` or `_should_not_flow` |
| `type` | type string | **required** | What the value is. No type variable: nothing a program wires can tell what one would stand for |
| `description` | String | none | What the port holds, for people and for the AI builder; `weft describe-nodes` shows it |
| `value` | any | **required** | The value, which has to fit `type` |

If one of these rules is broken, the node fails to load, and the message
names the port.

## features

| Key | Type | Default | What it does |
|---|---|---|---|
| `isTrigger` | Boolean | `false` | This node starts runs from outside instead of running inside one. Weft gives it `callsPerMinute`, `callsAtOnce`, `durable`, `recorded`, `keepRunsFor` and `holdSecs` inputs (and `outlivesCaller` too when it sets `liveConnection`), so do not declare those |
| `liveConnection` | `"http"` or `"websocket"` | none | On a trigger whose run answers a caller holding the connection open: `"http"` for a request waiting on its response (a route), `"websocket"` for a socket. It also gets `callsPerMinutePerCaller`, `journalEphemeral` and `journalWindowMs` |
| `journalSettings` | Boolean | `false` | Set it on a node that opens a bus and wants its users to choose how the run's record keeps it: the node gets the `journalEphemeral` and `journalWindowMs` inputs, and reads them with `ctx.journal_policy()?`. Weft refuses it together with `liveConnection` (which gives the same two inputs) and refuses a node that declares either input itself. For what they do, go and read [two modes](../language/streams-and-buses.md#two-modes) |
| `answersCaller` | `"whole"`, `"stream"` or `"end"` | none | This node answers its run's live caller. `"whole"`: the response in one go (a Reply). `"stream"`: the response head, then the body piece by piece (a Stream). `"end"`: the end of the exchange (a Close). Set it on your own node too if it answers through the ctx, so a route's "never answers its caller" warning counts it |
| `calledFromOutside` | Boolean | `false` | On a trigger somebody outside calls whose call ends there (a form somebody submits). It also gets `callsPerMinutePerCaller`. A `liveConnection` trigger is called from outside already, so it never sets this; a trigger that picks its events up itself (a schedule, a feed, a provider's push) has no caller and does not either |
| `pure` | Boolean | `false` | Set it when the node's body does nothing outside its run that weft does not put on record first: no network, no connection, no `ctx.run`, no `ctx.await_signal`, no tag, no storage beyond the run's own files. It may answer its caller (a durable run's answer leaves only once its record is written), read stored files, store files for this run alone, hand its caller links to them, and use a bus to another node of the run. A durable run then starts it without waiting for the database, and may run it again after a crash (for when, go and read [surviving a restart](durable-execution.md#when-the-worker-dies-mid-run)). A pure node that reaches outside through the ctx anyway fails at that call, naming this flag; weft cannot see a body that opens a socket or a file on its own, so if you mark one of those pure, a durable run may do what it does twice after a crash. `Text`, `JsonObject`, `Switch`, `Route` and `Reply` are pure; `HttpRequest` and `PostgresExecuteQuery` are not, because each one reaches outside the run on its own |
| `oneOfRequired` | List[List[String]] | `[]` | Each inner list is a group where at least one port must arrive, or the node skips |
| `canAddInputPorts` | Boolean | `false` | Source may declare extra inputs on it |
| `canAddOutputPorts` | Boolean | `false` | Source may declare extra outputs |
| `addedPortsAreDataKeys` | Boolean | `false` | Set it when the ports a program adds are named after data from outside weft: a path's captures, a query's parameters, a template's ids, the keys of a JSON object it builds. The compiler's `not-camel-case` warning then leaves every port a program adds to the node alone, and still checks the name the program gives the node. The nodes that ship with weft and set it are `Route`, `JsonObject`, `ElevenLabsRunTemplate`, `PostgresExecuteQuery`, `PostgresWatchQuery` and `PostgresUpdateRows` |
| `join` | Boolean | `false` | Set it when the node meets several branches and goes on with whichever arrived, like `FirstInOrder`. The inputs a program adds are optional (a closure on one does not skip the node), it runs once everything it waits on arrived or closed, and when nothing arrived it closes its output itself. The compiler's `run-undecided` check skips it, so it needs no `_should_flow` even when every wired input is optional |
| `customInputType` | type string | none | The type every added input takes. Naming one variable makes them all one type |
| `customOutputType` | type string | none | The type every output a program adds takes, when the program does not give it one. If you name a type variable that one of the node's own inputs uses (`T` on `Dispatch`'s `value`), each added output carries whatever that input received: a router's branches carry what it routes. Needs `canAddOutputPorts` |
| `pairsAddedPorts` | Boolean | `false` | Set it when each input a program adds should come with an output of the same name: the compiler makes that output, so the program does not have to declare it. An added output with no input of its name, an added output nothing reads, and an added input named after one of the node's own outputs are each the `unpaired-port` error. `Dispatch` sets it: `route.search = search.tool` adds a branch, and `route.search` is where it sends what that branch claims. Needs `canAddInputPorts` and `canAddOutputPorts` |
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
| `select` | `options`, `freeText` | A dropdown. `options` cannot be empty, and they are a real rule: a written, wired or instance-provided value outside them is refused, and so is a default. If the list is only suggestions (model ids, voices, anything the provider adds to), set `"freeText": true`: the editor still offers the list, lets the user type anything else, and nothing is refused |
| `multiselect` | `options`, `freeText` | Multiple choice over a `List[String]`, each item held to `options` the same way, unless `freeText` is set |
| `text_list` | | Short strings added one at a time. The default for `List[String]` |
| `password` | | A masked box |
| `file_drop` | `accept`, `type`, `multiple` | A file picker that writes an `@asset(...)` reference |
| `entry_list` | | Builds the config list this node's ports come from. Only legal on the input `portsFromConfig` names |
| `access` | | The connection picker. Write just `{"kind": "access"}`: the service and whether it is optional are filled in from your `service` recipe |
| `remote_select` | `access`, `sources`, `dependsOn`, `freeText` | Pick a resource on the connected service |

Leave `widget` out and weft picks one from the type: a file type gets
`file_drop`, `Boolean` gets `checkbox`, `Number` gets `number`, `List[String]`
gets `text_list`, `String` gets `text`, and everything else gets `textarea`.

### remote_select sources

| `kind` | Keys | Where the options come from |
|---|---|---|
| `granted` | `from`, `label`, `value` | The connection row itself, recorded at sign-in. Costs no call |
| `list` | a `Lookup`, plus `requires` | Calling the service and enumerating |
| `picker` | `script`, `code`, `grants`, `mimeTypes` | The provider's own chooser, opened on a weft page |
| `from_url` | `pattern` | The person pastes a link and the first capture group is the id |

A `Lookup` takes `get`, `items`, `label` and `value`, and optionally `page` (an
object holding `cursorParam` and `cursorPath`), `public`, `where`, `errorPath`
and `okPath`. `get` is the URL to
call. `items` is the dotted path to the array in the answer (`data.channels` for
`{"data": {"channels": [...]}}`), or `""` when the answer is the array itself.
`label` and `value` are dotted paths inside one item: the text the person sees
and the id that gets saved.

If you want the service to do the searching, put `{query}` in `get`, and it
becomes what the person typed in the search box. If `get` has no `{query}`, the
service never sees the search text, and the list only shows the items whose
label or id contains it, ignoring case. If you want another input's value
in the URL, list that input in the widget's `dependsOn` and write its name in
braces: `{team}` becomes whatever is set on `team`. Weft percent-encodes every
value it puts into the path or the query. A value before the path, like a
`{base}` holding `https://...`, goes in as it is. If a brace in `get` is never
closed, the node fails to load and says so.

If the endpoint is https and needs no credential, add `"public": true`. The list
then works before anyone picks a connection, and the call never carries the
connection's credential, even after they pick one.

If you only want some of the items the service sends back, add `where`. Each
key is a dotted path inside one item, and its value is what the item must hold
there. An item is kept only if every path matches, type included: `true` does
not match `"true"`. An item with nothing at a path is dropped, so `null` only
matches a field the service sends as `null`.

ElevenLabs' models endpoint answers with a bare array of every model, each
carrying flags like `can_do_text_to_speech`, so a text-to-speech node sets
`"items": ""` and keeps the ones that can speak:

```json
{ "kind": "list", "get": "https://api.elevenlabs.io/v1/models", "items": "",
  "label": "name", "value": "model_id", "where": { "can_do_text_to_speech": true } }
```

If the lookup also has a `page`, `where` filters each page as it arrives: a page where
nothing matches adds nothing to the list, and the editor still shows "More..."
for the next one.

If the service answers a failed lookup with a 200 and says so in the body, like
Slack's `{"ok": false, "error": "missing_scope"}`, add `errorPath` and `okPath`
so the lookup fails with the service's own words. For what they read, go and
read [refusals answered with a 200](../connections/declaring-a-service.md#refusals-answered-with-a-200).

## validate

Each rule is `{ "when": <condition>, "then": <diagnostic> }`.

The diagnostic takes `message` (with `{id}`, `{port}` and `{field}` filled
in, plus the ones the conditions below name),
`level` (`structural`, the default, or `runtime`), `severity` (`error` by default, or `warning`, `info`, `hint`), and
optionally `port` or `field` to say what the finding is about.

A `structural` rule is checked every time the project is parsed or built. Pick
`runtime` for a rule that only matters once the project runs, like a
provider with no model named yet. The build skips it; for when it is checked
instead, go and read [What the compiler refuses](../language/diagnostics.md).

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
| `custom_inputs_declared` | | Did the source add inputs beyond the metadata's own. `{customInputs}` in the message names them |
| `custom_outputs_declared` | | Did the source add outputs beyond the metadata's own. `{customOutputs}` in the message names them |
| `output_type_fits` | `port`, `within` | Does this output, as the program declared it (or as your metadata types it, if the program declared nothing), fit in the type `within`. Use it when the types that make sense on a narrowable output depend on a setting: a route whose `dataType` is `text` takes only a `String` body. False when the node has no such output |
| `custom_inputs_fit` | `oneOf` | Does every input the source added beyond the metadata's own fit in one of the types `oneOf` lists. Each input is held to one of them, never to their union, so your node can count on each added input being one of the shapes it handles. Use it when your node sends its added inputs somewhere that takes only some shapes: a model that reads a JSON value or a file lists `["Json", "File | Null"]` (the `Null` lets an input typed `Image | Null` through), and then a record that holds an `Image` fits neither, and neither does `String` or `Image` written as one type. An added input whose type is still open fits, and so does adding none. `{names}` in the message lists the added inputs that do not fit |
| `run_reaches` | `direction`, `with` | Does this node's run hold a node `with` a feature. `downstream`: every run this node starts holds one. `upstream`: this node is in the run of one |
| `downstream_of` | `with` | Is a node `with` a feature wired upstream of this one, so it runs first. `{with}` in the message names those nodes |
| `per_instance` | | Does this node exist once per instance: marked `@per_instance`, with an `@instance_filled` field, or reading one of those (a group that receives a per-instance value on any port counts for everything reading its ports). `{perInstanceReason}` in the message says why, path included: "it reads 'blender'", or "it sits inside group 'work', which receives 'blender'" |
| `input_names` | `port`, `names` | Does every name written on this input name a node or a field of this program. If the input holds a String, that is the name; if a list, each String in it; if an object, each of its keys. `names` says what they name: `{"node": {}}` is a node spelled the way the program writes it (`bridge`, `one.bridge`), narrowed with `role` (`infra` or `trigger`) and `perInstance` (`true` or `false`); `{"field": {}}` is a node's field written `node.field` (`send.model`), narrowed with `instanceFilled` (`true` or `false`). Nothing written is fine. A wired or `@instance_filled` input holds names only known at run time, so it is not checked. `{names}` in the message lists the ones that fail |
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
  "then": { "message": "Route '{id}' would exist once per instance, because {perInstanceReason}..." } }
```

A node that starts an instance's copy of an infra node, when it is given an
instance, refuses a name that is no such node, and `SetInstanceValues` refuses
a key that is no field an instance fills:

```json
{ "when": { "kind": "all", "of": [{ "kind": "input_satisfied", "port": "instance" }, { "kind": "not", "of": { "kind": "input_names", "port": "node", "names": { "node": { "role": "infra", "perInstance": true } } } }] },
  "then": { "message": "StartInfra '{id}' names {names} in `node`...", "port": "node" } }
{ "when": { "kind": "not", "of": { "kind": "input_names", "port": "values", "names": { "field": { "instanceFilled": true } } } },
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
one, and it goes last), `addsInputs` and `addsOutputs` (at least one of them).

A port template is `{ "nameTemplate": "...", "portType": "..." }`, with `{key}`
replaced by the entry's key.

A spec field is `{ "key", "label", "required", "shape", "valueType",
"widget" }`, where `shape` is one of `typed` (needs `valueType`), `value`,
`value_list`, `number`, `element` or `regex`. Every shape but `typed` needs
`matchInput` set. You can also write a field as a bare string, such as
`"email"`: that is a `typed` String field with a `text` widget, labelled from
its key.

## types

A flat map from a name to a type, usable by every node in the project:

```json
"types": { "ChatMessage": "{ role: String, content: String }" }
```

For how the names are shared and compared, go and read [custom
types](custom-types.md).

## typeParams

If a port is typed with a type variable (`T`, or `T` and a number), it takes
whatever type is wired to it. If you want to limit what that can be, give the
variable a bound here:

```json
"typeParams": { "T": "File" }
```

`KeepFile` does this with `"T": "File"`. Its `file` input and its `file` output are
both `T`, so a `Video` in gives a `Video` out. Wire a `String` into it and the
build stops with `type-bound`. A bound never picks the type: the compiler works
the variable out from what is connected, then checks the result against the
bound. A bound may name another variable that one of your ports uses
(`"T": "List[T2]"`). The file fails to
load if a variable here is used by no port, or if two bounds name each other
in a cycle.

## placeholdersAreInputs

If your node takes a text setting whose placeholders name inputs the program
adds to it (a query's `$userId`, a template's `{{name}}`), say so here, and
the compiler checks that the placeholders and those inputs match. The node
needs `features.canAddInputPorts`, or the file fails to load.

```json
"placeholdersAreInputs": { "in": "template", "pattern": "\\{\\{\\s*(?<name>[A-Za-z_][A-Za-z0-9_]*)\\s*\\}\\}" }
```

`in` is the input holding the text, and it must hold a `String`. `pattern` is
a regular expression finding one placeholder, whose group named `name` is the
input it stands for. If a match leaves the `name` group unmatched, weft ignores
it, which is how a pattern skips text that only looks like a placeholder: the
pattern `'[^']*'|\$(?<name>[A-Za-z_]\w*)` skips `'$userId'` inside a string literal and finds the bare `$userId`.
Back-references are allowed (`\k<tag>`), so a pattern can skip everything up
to the delimiter that matches its opening one, like SQL's `$tag$ ... $tag$`.

When the program writes the text itself, in the node's settings or through
`@file` ([files and reuse](../language/files-and-reuse.md)),
every placeholder in it must be an input the program added
(`placeholder-not-an-input`), and every input it added must appear in it
(`input-not-a-placeholder`). Text that arrives on a wire or through `@asset`
is only known when the node runs, so your node checks it then: find the
placeholders in the text yourself and pass their names to
`ctx.inputs.for_holes` (it calls them holes), which matches them to the inputs
and fails on either mismatch. For its arguments, go and read [reading
inputs](ctx.md#reading-inputs).

## firesWith

`firesWith` says exactly what a trigger's event carries: a flat map from field
name to type. Only a trigger may have one; on any other node, the file is
refused when it loads. Each type is parsed when the file loads, so a misspelled
type fails then too. A `?`
goes on the **name**, not the type:

```json
"firesWith": { "chatId": "String", "caller?": "JsonDict" }
```

The payload has to match it exactly. Whenever the trigger fires, whether a
listener sends the event or you fire it by hand with `weft run --fire`, the
payload is checked before the trigger's body runs. A payload missing a field
you declared without `?` is refused, and so is one carrying a field you never
declared, whether at the top level or inside a field whose type is a record. The refusal
names the field and ends with `It wants { ... }`, the shape you declared. If a
trigger has no `firesWith`, its payloads go through unchecked.

## claimsRoute

`pathField` names the config field holding the route pattern, and the optional
`methodField` names the one holding the HTTP method. Leave the method out, or
leave it empty on the node in your program, and the node claims every method.

If your node puts the path's captures on outputs a program adds (`-> (id:
String)` on `cards/{id}`), set `capturesAreOutputs: true`; the node also needs
`features.canAddOutputPorts`. The compiler then refuses an added output the
path does not capture, or one not typed `String`, with
`route-output-not-a-capture`, and a capture named like one of the node's own
outputs with `route-capture-shadows-output`. Filling each port is your node's
code: emit the capture under the port's name.

## accessApps

If you want people to connect your node's service without registering an OAuth
application of their own, ship a public one here, keyed by service: when
somebody connects through the `own` door (the "Your own" page, where people
paste their own app) and fills in none of the app fields, it signs in with this
one. For what the doors are, go and read
[How connections work](../connections/how-they-work.md#shared-or-your-own).

Each app needs a `label` (the name shown in the connection list) and a
`client_id`, plus every `registrationFields` entry the service does not mark
`optional`:

```json
"accessApps": { "google": { "label": "Acme", "client_id": "1234.apps.googleusercontent.com" } }
```

Every check on an app here runs when the catalog loads, and an app that fails
one leaves its node out of the catalog, with a message naming what is wrong:
- the key under `accessApps` names no service any node of the project
  declares (`no node in this project declares a service named 'slak'`);
- the service connects without an app, because it does not sign in through
  OAuth;
- `label` or `client_id` is missing or empty, a required registration field
  is missing or empty, or the app has a key the service does not declare (the
  message lists the keys it knows).

An app a package root gives its members is reported once, against the root's
file, unless it leaves out `label` or `client_id` entirely: then every member
that inherits it fails to load, each against its own file. A project's catalog holds its copy of the standard nodes
(`nodes/base_catalog`, which `weft new` copies in), so the standard services
count as declared.

An app here is a public client, one that signs in with no secret (OAuth's PKCE
flow). A `metadata.json` ships with the node to everyone who uses it, so a
secret written here would go to all of them: a `client_secret` is refused, and
so is any registration field the service marks secret (a field is secret
unless the service marks it `"secret": false`). A service that needs a secret
field in every app cannot ship a public app at all. An app with a secret goes in
the install's [apps file](../connections/the-apps-file.md) (`access-apps.json`),
or is pasted on the "Your own" page when somebody connects.

## Package defaults

A package root can hold a partial `metadata.json` whose keys every member
inherits. Members win, key by key, at the top level only, with no deep merge: a
member's `types` replaces the package's rather than adding to it.

Three keys can never be defaults, because they are one node's identity:

```text
package-level metadata.json must not set `type`: it is one node's identity, not a package default
```

## Keys that no longer exist

If you are reading older node code, the loader names these rather than saying
"unknown field":

| What you wrote | What to do now |
|---|---|
| `inputs[].exposure` | Use `accepts`. `["wire"]` refuses written values, `["literal"]` refuses arrows, and leaving it out allows both |
| `inputs[].requiredWhenWired` | Remove it. An input without `required` already waits for its wire once it is wired, and runs without it when it is not |
| `outputs[].required` | Remove it. An output has no optionality |
| `features.optionalCustomInputs` | Write `features.join` instead: a node whose added inputs are optional is a join, which goes on with whichever of several branches arrived |
| `features.isOutputDefault` | Remove it. Every node runs when it is reached, and `weft run --target <node>` narrows a run |
| `features.catchErrors` | Remove it. Every node has the `_error` output, and a failure goes there when a program wires it |
