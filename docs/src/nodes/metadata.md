# metadata.json

If you are writing a node, `metadata.json` is where you tell weft what it takes in, what it sends out and how it shows up in the editor. The compiler reads this file without compiling any Rust, so the graph can be checked while you are still writing the implementation.

For a complete first example, use [Your first node](your-first-node.md). This page is the reference for extending that declaration.

## The top level

`type`, `label` and `description` are required. They identify the node. Everything else adds ports, controls or capabilities. An unknown key is a metadata load error.

| Key | What it declares |
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
| `firesWith` | For a trigger, what a firing has to carry to start one |
| `requires_infra` | Whether the node provisions infrastructure |
| `images` | Local image directories, relative to the directory containing this `metadata.json` |
| `publishes` | The service name for a connection the node publishes |
| `service` | The [connection recipe](../connections/writing-a-service.md) owned by an access node |
| `accessApps` | Public OAuth app registrations keyed by service name; confidential client secrets are rejected |
| `claimsRoute` | Which config fields hold the public address the node claims, with `pathField` and an optional `methodField` |

`color` is a presentation hint only. An unknown icon name renders as a square and logs the missing name in the editor console.

For container declarations and image layout, read [Infrastructure nodes](infrastructure.md).

## Declaring inputs

Put both wired values and configuration in `inputs`. They reach the body through the same `ctx.inputs` accessors, so you do not handle them differently in Rust.

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

`name` and `type` are required. `required` defaults to false. `label`, `description` and `placeholder` help someone fill in the input.

If the node takes a set of values chosen by the program's author, such as a script's variables, enable `features.canAddInputPorts`. Each variable can then have its own port. In Rust, read those extra values with `ctx.inputs.custom()`.

### Letting an input be wired, or filled in

Most inputs accept either a literal or a wire. Narrow that choice only when the node needs it:

```json
"accepts": ["wire"]
```

| Declaration | Allowed source |
|---|---|
| Omitted | A literal or a wire |
| `["wire"]` | A value produced by another node |
| `["literal"]` | A value written in the source |

`@file` and `@asset` count as literals. A dotted reference in the node's braces counts as a wire. An input gets one source; supplying two produces `double-driven-port`.

Three restrictions the compiler enforces and `accepts` cannot loosen:

- Live `Bus` and `Generator` handles are wire-only.
- An access picker needs an inline value.
- The list named by `portsFromConfig` needs an inline value, because the compiler must read it to determine the node's shape.

### Choosing the editor control

If you omit `widget`, the editor chooses a control from the type: a checkbox for `Boolean`, a number field for `Number`, a single-line field for `String`, a text list for `List[String]`, and a file picker for file types. Other values use a text area.

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

A password widget only masks the display. It does not make a value safe to store as configuration. Use an access node for credentials.

If the provider can list channels or models, a `remote_select` saves the user from finding and copying an ID. Set `free_text: true` when an ID outside the fetched list is also valid. For its sources and dependent pickers, read
[Pick a resource inside the account](../connections/using-a-connection.md#pick-a-resource-inside-the-account).

### When to mark an input required

Write `"required": true` on an input the node cannot run without. Leave the key off everywhere else: absent already means optional, so `"required": false` says nothing and reads as though somebody meant something by it. Outputs never carry it at all (metadata load refuses one that does).

### Giving an input a default

The runtime uses `default` when no wire or literal supplies the input. The editor can display that effective value without writing it into the source. A required input with a default can therefore be left unwired.

A default does not replace a wired input that closes without a value. For that distinction, read
[The closed pulse](../language/mental-model.md#how-a-branch-stops-the-steps-after-it).

### Requiring permissions and values on an access input

On an `Access` input, `requiresScopes` and `requiresValues` declare which permissions and stored values the consumer needs. The source contains a connection reference, so the compiler alone cannot check the real account's permissions. For the declarations and checks, read
[Declare the requirements on the input](../connections/using-a-connection.md#declare-the-requirements-on-the-input).

## Declaring outputs

```json
{
  "name": "count",
  "type": "Number",
  "description": "The number of whitespace-separated words."
}
```

Outputs have a `name`, a `type` and an optional `description`. They have no widget, default or `required` field.

An ordinary port can emit once per firing. If the body finishes without emitting on that port, the runtime closes it. A downstream node that needs that value may then skip; an optional input can remain absent. A generator port can emit repeatedly and closes when its sequence ends.

Leaving a port out of one emission does not close it immediately. For emission and explicit closure, read
[Reading inputs, emitting outputs](values-and-emission.md).

## If you are writing a trigger: `firesWith`

A trigger starts an execution from outside: a listener wakes it, or somebody types `weft run --fire`. Whatever they hand it is the fire payload, and it is neither the node's inputs nor its outputs: it is the one thing that has to exist before the node's own logic can even begin.

`firesWith` writes down the shape of that payload, as a flat object from field name to a weft type. Convention: declare it just above `features`, next to the trigger flag.

```json
"firesWith": {
  "scheduledTime": "String",
  "actualTime": "String"
},
```

A `?` on the end of a field NAME, not the type, marks it optional:

```json
"firesWith": {
  "method": "String",
  "caller?": "JsonDict"
},
```

The engine checks a real firing against this shape before the node's body runs, and `weft run --fire` checks a hand-typed payload against it before building or starting anything. Either way, a listener whose fields moved, or a typo in a payload you typed yourself, is refused by naming the exact field that is wrong or missing, instead of failing somewhere inside the node.

The convention is: only a trigger declares it, and every trigger has one, so `weft run --fire` can print the shape when a payload does not match, which is otherwise the only way an author learns what to send. No compiler code enforces any of that; the repository test `weft-compiler/tests/fires_with.rs` checks only that shipped triggers declare it, not where it sits, and walks the catalog; a node outside the shipped catalog is not held to it by the compiler.

### Name every field, including the ones that only sometimes arrive

A payload carrying a field you did not name is refused, exactly like one missing a field you did. So name every field that can arrive, not only the ones you put on ports, and put a `?` on the ones that only sometimes come.

This is strict on purpose. A trigger's payload is the one value a node did not compute and cannot check for itself, and this is where it gets checked, once, before anything runs. Let an unnamed field through and what the node actually receives quietly stops being what the node says it receives, and the day that matters is the day some code reads a field nobody wrote down.

The cost is that a provider adding a field stops that trigger until somebody adds the field here. That is one line in a JSON file, and the refusal names the field, so the fix is obvious and takes a minute.

If your trigger is fed by a connection's events, you do not have to guess the list: the service's recipe declares it, per topic, under `events.<topic>.fields` ([writing a service](../connections/writing-a-service.md)). A firing carries those names and no others, minus any the provider's event did not have, so that map is exactly what belongs here. `weft-compiler/tests/fires_with.rs` holds every shipped trigger to its topic's list, in both directions, so a field you forget fails there rather than in front of somebody's users.

The type strings are ordinary weft types, and a field can be a record nested to any depth:

```json
"item": "List[{ id: String, tags: List[String] }]"
```

Two triggers in the catalog declare nothing, both for reasons no declaration could fix: `ReceiveEmail` opens its own IMAP session and reads the mail itself, so there is no payload field to name. `HumanTrigger`'s fields are the form fields somebody typed into that instance, which differ per node, so no static declaration could name them either.

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
| `liveEndpoint` | The named infrastructure endpoint that serves `/live`, which is what opts the node into having a [display](showing-things-in-the-graph.md#its-display) |
| `castPorts` | The `input` and `output` ports whose types must allow a checked conversion |
| `hidden` | Hide an internal catalog type from the picker and refuse it in authored source |

For example, `"oneOfRequired": [["message", "attachment"]]` lets a node run with either input. A custom input created from a literal takes that literal's inferred type; `customInputType` does not override it.

## Showing a file on the node: `display`

To display a file on the node, name its port and which side it is on:

```json
"display": { "kind": "media", "output": "image" }
```

Use `input` instead of `output` for a display sink. Supply exactly one side and name a declared port; the catalog rejects an ambiguous or missing port.

`media` shows images and playable audio/video, with a file card for other formats. `link` always shows the file card. For previews and live status panels, read [What your node shows in the graph](showing-things-in-the-graph.md).

## Package defaults

A directory with `package.toml` can also have a partial `metadata.json`. Each member inherits keys it does not define itself:

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

The merge replaces whole top-level values. If a member defines `types`, its entire `types` object wins; the loader does not merge its entries with the package's object.

`type`, `label` and `description` cannot be package defaults. For how the package's Rust code is shared, read [Packaging](packaging.md).

## Ports that come from a node's own config

A form needs an output for each field the program author adds. `portsFromConfig` declares how entries in that list become ports.

This fragment adds a String output for each `text_input` entry in the node's `fields` input:

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

The metadata also needs a declared `fields` input. In the graph, an entry such as `{ "kind": "text_input", "key": "reason" }` adds a `reason` output. The default `keyField` is `key`; set `keyField` to `port` to use the entry's `port` field instead. Port names must be legal identifiers.

Use `addsInputs` for input ports. Each template substitutes the entry's name into `{key}`. A `portType` of `T_Auto` gives the entry its own type variable.

### What a kind asks the author for

The `fields` list describes values the author can supply on an entry. A bare `"label"` declares an optional String field named `label`. Use a full declaration for another type, a custom label or a required field:

```json
{
  "key": "options",
  "label": "Options",
  "required": true,
  "shape": "typed",
  "valueType": "List[String]"
}
```

`shape: "typed"` checks the value against `valueType`. You can override its editor control with `widget`, using the input widget vocabulary.

The spec's `render` supplies the default form renderer. An entry may override it in the graph; it does not need to repeat it.

### Entries that compete

For switch-like entries, `matchInput` names the input they compare against. The field shapes can then be relative to that input:

| Shape | Checks |
|---|---|
| `value` | One value compatible with the matched input |
| `valueList` | A list of compatible values |
| `number` | A number |
| `element` | One element of the matched list, or a piece of matched text |
| `regex` | A regular expression that compiles |

Set `catchAll: true` on the fallback kind. The compiler permits one catch-all entry and requires it to be last. It also rejects entry keys the kind did not declare.

For complete declarations, read the
[human form metadata](https://github.com/WeaveMindAI/weft/blob/mvp/catalog/human/metadata.json)
and [Switch metadata](https://github.com/WeaveMindAI/weft/blob/mvp/catalog/logic/switch/metadata.json).

## Rules types cannot express: `validate`

Use `validate` when a constraint is about values or wiring rather than types. Each rule says when to report a diagnostic:

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

This fragment assumes the node declares a `query` input. The rule checks its written value, so it is appropriate when that input takes a literal. It cannot inspect the result of an upstream node.

Each condition uses `kind` plus the fields below.

| `kind` | Other fields | True when |
|---|---|---|
| `input_satisfied` | `port` | The input has a wire or a written value that supplies data |
| `input_wired` | `port` | The input has an incoming wire |
| `output_wired` | `port` | The output has at least one wire leaving it: somebody downstream reads this value |
| `input_source_type` | `port`, `equals` | Every incoming wire comes from the named node type; also true with no wires |
| `config_present` | `field` | The written value exists and is non-null |
| `config_nonempty` | `field` | The written value is nonempty; whitespace-only strings count as empty |
| `config_equals` | `field`, `equals` | The written value equals the supplied JSON value |
| `config_in_set` | `field`, `values` | The written string appears in `values` |
| `config_matches` | `field`, `regex` | The written string matches the regular expression |
| `custom_outputs_declared` | (no fields) | The source declared output ports beyond the metadata's own, on a node with `canAddOutputPorts` |
| `run_reaches` | `direction`, `types` | The run this node is part of holds a node of one of `types`. With `downstream`, the runs started from this node contain one; with `upstream`, this node is in the run of one |
| `all` / `any` | `of`, a condition list | All or any of the conditions hold |
| `not` | `of`, one condition | That condition is false |

Combine `input_source_type` with `input_wired` when the wire itself is required.

The default level is `structural`, checked while editing. `runtime` defers the diagnostic to validation for a run, including `weft validate`. Severity defaults to `error`; it can also be `warning`, `info` or `hint`.
