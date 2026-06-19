# The Weft Language

This is the reference for the Weft LANGUAGE itself: the `.weft` source syntax,
the type system, and the built-in constructs (nodes, connections, groups, loops,
triggers). It is the dual of [`authoring-nodes.md`](./authoring-nodes.md), which
is the FRAMEWORK guide (how to write the Rust that implements a node). This guide
is for the surface a person (or an AI builder) writes a `.weft` program in.

A `.weft` program describes a directed graph: nodes connected by typed edges.
The compiler validates the whole graph before it runs: every connection's types
must match, every required input must be wired, every node's config must be
valid. If it compiles, the wiring is sound.

## A complete tiny program

```weft
# Project: hello
# Description: emit a literal and show it

greeting = Text { value: "hello world" }
out = Debug

out.data = greeting.value
```

Three things: two node declarations and one connection. `Text` emits a literal
string on its `value` output; `Debug` receives it on its `data` input.

## Comments and headers

- `# ...` is a line comment.
- `# Project: <name>` and `# Description: <text>` at the top set project
  metadata. (The project id lives in `weft.toml`, not the source.)
- A comment on the first line(s) inside a group body becomes that group's
  description.

## Nodes

A node declaration binds a name to a node type:

```weft
name = NodeType
name = NodeType { config_field: value, ... }
name = NodeType {}
```

The name is the node's id (unique within its scope). The type must exist in the
project's catalog (`nodes/`). Config goes in `{ }`.

### Config values

Config fields are typed JSON-like literals:

```weft
t = Text { value: "a string" }
n = Range { to: 10, step: 2 }            # numbers
flag = SomeNode { enabled: true }        # booleans
arr = SomeNode { items: [1, 2, 3] }      # JSON arrays
obj = SomeNode { opts: { "k": "v" } }    # JSON objects
multi = SomeNode {
  fields: [
    { "fieldType": "text_input", "key": "name" }
  ]                                       # multi-line JSON arrays/objects are fine
}
```

A special config key, `_label: "..."`, sets a display label without affecting
behavior. `_tags: ["a", "b"]` attaches tags (used by signal scoping).

### Multi-line string config

Triple-backtick blocks carry multi-line values (e.g. code, templates):

````weft
step = ExecPython() -> (out: Number) {
  code: ```
    return {'out': 42}
  ```
}
````

### Inline-declared ports (the arrow form)

Some nodes let the author declare the node's inputs and outputs inline, in the
declaration, with an arrow. `ExecPython` is the canonical example: its ports are
whatever you declare.

```weft
calc = ExecPython(a: Number, b: Number) -> (sum: Number, diff: Number) {
  code: "return {'sum': a + b, 'diff': a - b}"
}
```

Inputs arrive in the code as variables named after each port; the code returns a
dict keyed by output port name. A returned key set to `None` (or missing) emits
no pulse on that port.

## Connections

An edge wires one node's output port to another node's input port:

```weft
target.input_port = source.output_port
```

Read it right-to-left: the value flows FROM `source.output_port` INTO
`target.input_port`. The two port types must be compatible or it is a compile
error. Every required input must be wired (an unwired required input is a compile
error); an unwired optional input (`port?`) is fine.

## Types

Port types the compiler understands:

- **Primitives**: `String`, `Number`, `Boolean`, `Image`, `File` (plus media
  aliases like `Video`, `Audio`), `Bus`, `Null`.
- **Containers**: `List[T]` (e.g. `List[Number]`, `List[List[String]]`),
  `Dict[K, V]` (e.g. `Dict[String, String]`).
- **Unions**: `A | B` (e.g. `String | Number`, `Number | Null`).
- **Type variables**: a bare capitalized name like `T` is a generic that unifies
  across the node's ports. A node with input `T` and output `T` carries whatever
  concrete type flows in. A type variable must be pinned to something concrete
  somewhere in the graph, or the compiler rejects it as unresolved.
- **Optional**: a trailing `?` on an input port lets it accept `null` (it opts
  into receiving "no value"). Without `?`, a required input refuses to run on
  null and the node is skipped.

## Null propagation (how branching works)

Every required input refuses to run on null. When an upstream node produces
nothing (closes a port), the downstream node is SKIPPED, and that skip cascades
until it hits an optional port or a node that handles absence. There is no
try/catch: branching is just "the inactive branch produces null, and everything
on it skips." A `Gate` node, for instance, closes its `value` output when `pass`
is false, so the downstream of a closed gate simply does not run.

## Groups

Any subgraph can be a group: a box with typed input and output ports that, from
the outside, looks like a single node. Groups nest arbitrarily.

```weft
preprocessor = Group(raw: String) -> (result: String) {
  # Cleans and transforms text   (this comment is the group's description)

  clean = ExecPython(text: String) -> (out: String) {
    code: "return {'out': text.strip()}"
  }
  clean.text = self.raw           # read the group's own input via `self`
  self.result = clean.out         # write the group's own output via `self`
}

preprocessor.raw = input.value    # wire the group like any node
output.data = preprocessor.result
```

Inside a group, `self.<port>` refers to the group's own interface ports: reading
`self.<input>` pulls the group's input; writing `self.<output>` sets the group's
output. A group's children can only talk to each other and to `self`; there is
no reach across the group boundary. This recursive scoping is what keeps large
graphs from becoming spaghetti: a 100-node system is still a handful of boxes at
the top level.

## Loops

`Loop` is a built-in like `Group`, but its body runs multiple times. Full detail
is in [`loops.md`](./loops.md); the shape:

```weft
doubler = Loop(values: List[Number]) -> (results: List[Number | Null]) {
  parallel: false        # sequential (default) or true (parallel map)
  over: ["values"]       # iterate this List[T] input as T per iteration

  step = ExecPython(n: Number) -> (out: Number) {
    code: "return {'out': n * 2}"
  }
  step.n = self.values   # inside the body, self.<iter-port> is one element
  self.results = step.out
}
doubler.values = nums.values
```

The four port roles (iter input `over`, carry `carry`, gather output, broadcast
input), the drive modes, and the "a loop is a launcher, not a lifecycle owner"
semantic are all in `loops.md`. A gather output must be typed `List[T | Null]`
explicitly (a slot is null when that iteration produced nothing).

## Triggers

A trigger node is what starts an execution from the outside world. It is a
normal node whose metadata sets `isTrigger: true`; the language drives it through
two phases (trigger setup, then per-fire). You wire its outputs downstream like
any node, the difference is that an external event (a web request, a timer, a
form submission, an event feed) fires a fresh execution carrying the event data.

The built-in / common trigger kinds (see `authoring-nodes.md` for the full
table and how to author your own):

- `ApiEndpoint { path }` / `LiveSocket { path }`: an outside caller holds a live
  HTTP / WebSocket connection; nodes talk back over it.
- `Cron { cron }`: fires on a schedule.
- `HumanTrigger { ... }`: a person submits a form to start a flow.
- Feed-style triggers a node author writes (subscribe to an event stream, poll a
  URL, hold an outbound socket) via the framework's signal kinds.

Mid-flow, a `HumanQuery` node suspends the execution waiting for a human form;
the rest of the graph at other points keeps running, and the execution resumes
when the form is answered. (Suspension / resume is a framework capability, see
`authoring-nodes.md`.)

## Reusing other files

- `@include("path.weft")` pulls another `.weft` file in as a group (its graph
  becomes a subgraph under the including node's name).
- `@file("path")` / `@file("path", Type)` reads a file's contents as a config
  value (optionally cast to a type).

## Validation the compiler enforces

- **Type match** on every connection.
- **Completeness**: every required input wired; orphan nodes flagged.
- **Unique names** per scope (duplicate node ids in the same scope are an error).
- **Node self-validation**: each node checks its own config (required fields,
  shapes) at compile time.
- **`@require_one_of`**: a node can require that at least one of a set of
  all-optional inputs is wired.
- Unresolved type variables, reserved-name misuse (`self`, type keywords,
  loop-reserved `index`/`done`), and malformed config are all compile errors.

The only failures left after a successful compile are external: a service is
down, an API errors, a human never answers. The wiring itself is proven.
