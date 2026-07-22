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
greeting = Text { value: "hello world" }
out = Debug

out.data = greeting.value
```

Three things: two node declarations and one connection. `Text` emits a literal
string on its `value` output; `Debug` receives it on its `data` input.

## Comments and headers

- `# ...` is a line comment.
- If the FIRST line inside a group or loop body is a plain comment, that
  single line is the group's description (keep it short; a second comment
  line is an ordinary comment). Tooling shows it when the group is rendered
  or collapsed. For an included `.weft` file, the description lives inside
  the file's top-level group body, same rule; comments at the top of a file
  outside any group have no special meaning. (The project name and id live
  in `weft.toml`, not the source.)

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

### Config sugar on connection lines

When the target is a NODE's own port and the right-hand side is a literal, the
line fills that node's config instead of creating an edge:

```weft
prompt = Text
prompt.value = "summarize the input"    # same as Text { value: "..." }
```

Only a node's own config port takes a literal this way. A group boundary port
(`self.x`), an include alias port, or an undeclared target cannot be assigned a
literal; those are driven by wiring or by an inline expression.

### Inline expressions (anonymous nodes)

A node literal can appear directly as a value, with a MANDATORY trailing
`.port` naming which of its outputs feeds the target:

```weft
out.data = Text { value: "hi" }.value
```

This synthesizes an anonymous child node (id `{host}__{field}`, here
`out__data`) plus the edge into `out.data`. The same form works as a config
field's value inside a node body. Inline expressions carry full node syntax
(inline port signatures, config, nesting); omitting the trailing `.port` is a
compile error.

## Types

Port types the compiler understands:

- **Primitives**: `String`, `Number`, `Boolean`, `Null`, `Image`, `Video`,
  `Audio`, `Blob` (the catch-all stored-file primitive: any bytes whose mime
  is not image/video/audio, e.g. a pdf or a zip), `Empty` (the bottom type: a
  port that never carries a value; `Number | Empty` simplifies to `Number`).
- **Union aliases**: `Media` = `Image | Video | Audio`; `File` = `Image |
  Video | Audio | Blob`. These are shorthand names for those unions, not
  primitives of their own.
- **Containers**: `List[T]` (e.g. `List[Number]`, `List[List[String]]`),
  `Dict[K, V]` (e.g. `Dict[String, String]`).
- **`JsonDict`**: an opaque `Dict[String, *]` whose value types are unchecked;
  compatible with any `Dict[String, V]` in both directions. Use it for raw API
  responses where the shape is unknown or too complex to declare.
- **Unions**: `A | B` (e.g. `String | Number`, `Number | Null`).
- **`Bus`**: a message-bus handle, an in-process channel between co-alive
  nodes. A `Bus` output connects only to a `Bus` input; message payloads are
  not type-checked by the language. Wired-only: the value is a live runtime
  handle, never a config literal.
- **Type variables**: a bare capitalized name like `T` is a generic that unifies
  across the node's ports. A node with input `T` and output `T` carries whatever
  concrete type flows in. A type variable must be pinned to something concrete
  somewhere in the graph, or the compiler rejects it as unresolved.
- **`MustOverride`**: a node whose metadata cannot determine a port's type
  declares it `MustOverride`; the `.weft` author must pin it (via the inline
  port signature). Any `MustOverride` remaining at compile time is an error.
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

`Loop` is a built-in like `Group`, but its body runs multiple times: once per
element of a list, or until the body says "stop", with parallelism control and
accumulator support.

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

### The four port roles

Every port on a loop is in exactly one of four roles, derived from the config:

1. **Iter input** (named in `over`). Outside type is `List[T]`, inside type is
   `T`; the body sees one element per iteration via `self.<port>`. Multiple
   ports in `over` zip together in lockstep.
2. **Carry port** (named in `carry`). Declared on the OUTPUT side of the
   signature; the compiler auto-creates a matching input port (same name, same
   type) for the initial value. Inside the body, reading `self.<port>` gives
   the previous iteration's value (or the initial value); writing it sets the
   next iteration's value. At termination the final value is emitted outward.
3. **Gather output** (in the output signature, not in `carry`). Outside type
   MUST be `List[T | Null]` explicitly (`gather-output-must-be-nullable`);
   inside, the write port is `T?`. One value per iteration; an iteration that
   fails to write leaves `null` at its slot. Parallel ordering is preserved by
   iteration index.
4. **Broadcast input** (in the input signature, not in `over`). Same type
   inside and out; the value is available unchanged to every iteration.

Two implicit body ports exist on every loop, never declared: `self.index:
Number` (read-only, the 0-indexed iteration) and `self.done: Boolean`
(write-only; the body writes `true` to stop launching new iterations,
sequential mode only). `index` and `done` are reserved port names
(`reserved-port-name`).

### Drive modes and the config knobs

`parallel` defaults to `false` (sequential: carry and `self.done` work, no
ordering surprises). `over` / `carry` default to empty lists; no `max_iters`
means no cap. A non-boolean `parallel` is a compile error
(`loop-parallel-not-boolean`); unknown config keys are rejected
(`loop-unknown-config-field`). A loop terminates on whichever comes first: the
`over` lists are exhausted, the body wrote `self.done = true` (sequential
only), or `max_iters` is reached.

Invalid combinations, all compile errors: `parallel: true` with a non-empty
`carry` (carry implies sequential), `parallel: true` with an empty `over` (the
iteration count must be known upfront), and `parallel: true` with any
`self.done` write (`parallel-with-done`). Empty `over` AND empty `carry` is
allowed: the loop runs purely until `self.done` / `max_iters` (the pure
side-effect shape). But a sequential loop with no `over`, no `max_iters`, AND
no `self.done` write anywhere in its body is provably infinite and rejected at
compile time (`loop-unbounded-no-termination`): give it something to exhaust,
a cap, or a stop vote. A port listed in both `over` and `carry` is an error
(`over-and-carry-overlap`).

The five base shapes:

| Shape | `parallel` | `over` | `carry` | Terminated by |
|--|--|--|--|--|
| Parallel map | `true` | `[...]` | `[]` | Over exhausted |
| Sequential map | `false` | `[...]` | `[]` | Over exhausted |
| Fold (sequential reduce) | `false` | `[...]` | `[acc]` | Over exhausted |
| While | `false` | `[]` | `[acc?]` | `self.done = true` |
| Side-effect | `false` | `[]` | `[]` | `self.done = true` |

For a count-based loop ("run N times"), feed a `Range` catalog node into a map
loop's `over` input.

### A loop is a wrapper, not a lifecycle owner

In mainstream languages a `for` loop OWNS its body's lifetime. In Weft it does
not. The loop is only (1) a launcher that decides how/when/how-many iterations
start, and (2) a single outward emitter that, at termination, assembles gathers
and carries and emits them at the parent frame stack. The body is a SCOPE
addressed by frame stacks: body work launched at an iteration keeps running
until it naturally drains, even PAST the loop's outward emit. Only the branch
wired to the loop's outputs gates the outward emit; other body branches keep
going (a body node can launch a sub-agent or open a bus that stays alive for
hours after the loop emitted). Body work is bounded in SPACE (edges cannot
cross the loop boundary) but unbounded in TIME; the execution as a whole
terminates only when all body work has drained. The canonical pattern: a
parallel loop launches N agents, gathers their bus markers as
`List[Bus | Null]`, and an outside coordinator wires to that list to talk to
the still-running agents.

### Failure and nesting

Loops are not a kill-switch. A failing body branch cascades only through that
branch: a gather port that received a closure yields `null` at that index (the
`List[T | Null]` type forces downstream handling), a closed carry write keeps
the previous carry value, a closed `self.done` reads as `false`. The outward
emit is gated by "every launched iteration fired the loop boundary", not by
"every body has fully drained." Nested loops add one frame per level to the
iteration frame stack; outer-loop termination does not cascade to inner loops.

## Triggers

A trigger node is what starts an execution from the outside world. It is a
normal node whose metadata sets `isTrigger: true`; the language drives it through
two phases (trigger setup, then per-fire). You wire its outputs downstream like
any node, the difference is that an external event (a web request, a timer, a
form submission, an event feed) fires a fresh execution carrying the event data.

What runs on a fire, precisely: the fired trigger's reachable outputs, plus
everything those outputs depend on, stopping at trigger nodes. Sibling branches
the fired trigger cannot reach do not run. A trigger's own INPUTS are read once,
at activation: whatever its upstream delivered during trigger setup is saved and
replayed onto the trigger at every fire (re-activating refreshes the values).
The fire's event data itself arrives separately, on the trigger's wake payload.
If the fire subgraph contains other triggers, they do not run: their output
ports close, so a node fed by several triggers proceeds with the firing branch
and treats the idle ones as dead.

Manually running a project that contains triggers behaves the same with no
firing trigger at all: every trigger closes, and the run exercises only the
paths that don't need one. To exercise a trigger's path, fire the trigger (the
editor can send a hand-written payload).

The compiler enforces the shapes that keep this well-defined, each a compile
error: a cycle in the wire graph (`graph-cycle`; iterate with a `Loop`, exchange
feedback over a bus), a trigger inside a `Loop` (`trigger-in-loop`), a trigger
wired into another trigger (`trigger-into-trigger`), and a trigger wired into an
infra node (`trigger-into-infra`; provisioning happens before any fire exists).

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
