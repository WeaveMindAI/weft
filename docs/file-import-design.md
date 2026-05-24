# File composition: `@file` and `@include`

Design for splitting a Weft project across multiple files. Two
operations, one principle: **the source you write stays the source of
truth, the compiler splices external files in at compile time, and the
whole project still compiles to one binary.** Nothing here is a runtime
module system; both operations are compile-time text/graph injection.

This replaces the "File import / multi-file projects" entry in
`TODO.md`. That entry was a partial sketch; this is the real shape.

## Why

A project is one `main.weft`. Two things don't scale:

1. **Inline content that isn't graph structure.** A long LLM system
   prompt, a JSON schema, a fixed document. Today it lives as a
   triple-backtick heredoc inside a node's config. That bloats the
   source, can't be edited with the right tooling (a `.json` wants JSON
   editing, not weft-heredoc editing), and can't be shared between
   nodes or projects.

2. **Graph structure that wants to be a reusable component.** A
   preprocessing subgraph, a "summarize and verify" block, anything you
   want to define once and drop into many projects. Today the only unit
   of reuse is copy-paste.

`@file` solves (1): inject a file's content as a typed value. `@include`
solves (2): inject another `.weft` file as a group. They share the
`@`-keyword family but are mechanically different operations and should
be read as such at the call site.

## The two operations

### `@file("path", Type = String)` — value injection

Replaces a config value (or a literal wired into a port) with the
content of an external file, cast to a declared type.

```weft
system = LlmConfig {
  systemPrompt: @file("prompts/system.txt")          # Type defaults to String
}

validator = LlmConfig {
  schema: @file("schemas/output.json", JsonDict)      # parsed as a dict
}

retries = SomeNode {
  maxRetries: @file("config/retries.txt", Number)
}
```

**Semantics.** At compile time the compiler reads the file (resolved
relative to the project root, the directory holding `main.weft`), casts
its text to `Type`, and uses the result as the field value. The node
never knows the value came from a file: by the time it runs, the config
holds a plain `String`, `Number`, parsed object, whatever `Type`
produced. This is pure compile-time injection.

**The type vocabulary is the existing one.** `Type` is parsed with
`WeftType::parse` (`crates/weft-core/src/weft_type.rs:42`), the exact
function port declarations already use (`weft_compiler.rs:1093`). So the
author writes the same type names they write everywhere else
(`String`, `Number`, `Boolean`, `JsonDict`, `List[String]`, ...). No new
type-name surface to learn. `String` is the default because the common
case (a prompt, a document) is "drop this text in verbatim."

**Casting.** Text-to-`Type` is a small, total function:
- `String` — the file's content verbatim (the default; no parsing).
- `Number` / `Boolean` — trim and parse; non-parseable content is a
  loud compile error.
- `JsonDict` / `List[T]` / `Dict[K,V]` — parse the file as JSON, then
  the existing value validation applies.

**Validation reuses what exists.** After the cast, the value is checked
the same way any inline config value is checked: against the wired
port's type if the field feeds a port, or against the config field's
`field_type` (`crates/weft-core/src/node.rs:493`) if it's a plain config
field. A `@file("notes.txt", Number)` whose file says `hello` fails at
compile, not at runtime. Consistent with "if it compiles, the
architecture is sound."

**Editing (the graph IS the code).** The graph node renders the
*resolved file content* in the field, fully editable. On edit:
debounced autosave writes the new content **back to the target file**
(`prompts/system.txt`), not into `main.weft`. The `@file(...)` token in
`main.weft` is never rewritten by a value edit; only the referenced
file's bytes change. This is the existing debounced config-write path
with the write target redirected from "the field's span in main.weft"
to "the whole content of the referenced file." No span model needed:
you are editing a value's backing file, not weft source.

### `@include("path.weft")` — group injection

Splices another `.weft` file into the current scope as a group.

```weft
clean = @include("components/cleaner.weft")
clean.raw = input.value
output.text = clean.cleaned
```

**The file must declare a single top-level `Group`.** An included file
is authored to be included: its entire content is one top-level group
with a declared interface.

```weft
# components/cleaner.weft
Group(raw: String) -> (cleaned: String) {
  strip = Template { ... }
  strip.input = self.raw
  self.cleaned = strip.output
}
```

If the file is not exactly one top-level `Group`, it's a loud compile
error ("an included file must declare a single top-level Group"). We do
**not** infer an interface from loose nodes, and we do **not**
auto-synthesize boundary ports from `self.*` usage. Both were
considered and rejected (see Rejected alternatives): an inferred or
implicit interface is invisible at the file, which is exactly the
"can't see what's in/out" problem this feature exists to avoid. The
`Group(...)` header **is** the interface, visible on line one of the
file.

**Mechanically it is a nested group.** This is the crux: an included
group maps exactly onto the machinery groups already use. The compiler:
1. Parses the included file, confirms a single top-level `Group`.
2. Rescopes its contents under the call-site alias (`clean`), the same
   `groupId.child` prefixing nested groups already get
   (`weft_compiler.rs:1332`).
3. Hands it to the existing `flatten_group` pipeline, which produces the
   `clean__in` / `clean__out` Passthrough boundary nodes
   (`weft_compiler.rs:2268`).

The boundary contract is therefore enforced for free: the
`check_scope_reachability` validator (`validate.rs:275`) already makes
any cross-scope edge that doesn't pass through a Passthrough a hard
error. An included group's internals are reachable from outside *only*
through its declared ports. No new scoping primitive, no new validation.

**Editing: navigate, don't inline.** When the user expands an included
group in the graph, the extension **opens the referenced file** and
renders it as a normal single-file graph (see Navigation model). There
is no special multi-file editing: once you're in `cleaner.weft`, you are
editing `cleaner.weft` with the existing single-file surgical editor and
the existing single-file spans. You never see two files' nodes on one
canvas, so there is never more than one file's span coordinate space in
play. **This is why there is no multi-file span model in this design.**

A file included from many places is one shared definition: editing it
changes every use site, because it's one file. That is the point of
reuse, and it's what keeps a 100-node system legible.

## Two resolution depths

`@include` resolves at two different depths depending on who's asking.
Keep these separate; conflating them is how the interactive loop gets
slow.

- **Interactive parse (per-keystroke, the extension's `/parse`).**
  Resolve an `@include` to its **interface only**. Open the file,
  confirm single top-level `Group`, extract its `Group(...)` port
  signature, present the call-site node as an opaque typed block with
  those ports. The body is *not* inlined. The parent view shows one
  file's nodes plus opaque blocks for its includes. Cheap, and it keeps
  every interactive view single-file.

- **Build (`weft build`, codegen).** Resolve `@include` **fully**:
  recursively inline every included file's flattened body into one
  `ProjectDefinition`, walk the whole tree, produce one binary. This is
  a build step, not a render step, so the cost of walking the full tree
  is paid once at build, never per-keystroke.

`@file` has no such split: it always resolves fully (read + cast) at
parse, because the resolved value is cheap and the field needs it to
render.

## Navigation model

The graph is the whole program, traversed **one file per view**.

- A view always renders exactly one file. Included groups render as
  opaque collapsed blocks (with their interface ports).
- Expanding an included group **replaces the current view** with the
  referenced file's graph and **pushes the previous view onto a
  back-stack**. The open document in the editor follows.
- A **return button** pops the stack back to the previous file. Not a
  breadcrumb: inclusion is a graph, not a tree (a file is reachable from
  many places, by many paths), so a single path label would lie. The
  stack is just "files I walked through to get here," like a browser
  back button or IDE go-to-definition + back.
- Same panel, replace-and-push. We don't open a new panel per
  navigation (that accumulates tabs and breaks "one view = one file").

`@file` involves no navigation: it's a value, edited in place in the
field.

This is what makes the editing story trivial: navigation = open the real
file; editing = the existing single-file editor on that file; return =
pop the stack.

## Execution following with nested files

The user can follow a live execution or replay a past one and watch
values flow through the graph. This must work when navigating into
included files: walk into `cleaner.weft` mid-execution, see the actual
values its nodes produced.

It works with almost no new machinery, because of how identity is keyed:

**The journal is a flat log keyed by `(color, node_id, lane)`, and
`node_id` is the fully-qualified post-flatten id.** A node `strip`
inside an included group aliased `clean` becomes `clean.strip` after
rescope+flatten, and *that* string is what the worker writes to the
`exec_event` journal, what SSE streams as `DispatcherEvent.node`, and
what the extension keys `executionState.nodeExecutions` by. The journal
has no concept of files; every node's input/output is reconstructable by
`(color, node_id)` regardless of nesting depth
(`crates/weft-journal/src/events.rs`, `journal_bridge.rs:242`).

**The extension already buffers every event,** including events whose
`node_id` isn't currently rendered. While `clean` is an opaque block,
`clean.strip` events accumulate in `nodeExecutions['clean.strip']`
unrendered (the render effect does `nodeExecutions[n.id] || []` and
`clean.strip` isn't a visible node). The instant you navigate into
`cleaner.weft` and it renders nodes with those ids, the buffered values
paint. No re-fetch, no reconstruction: the data was never lost.

**The one new piece: the navigation carries an alias prefix.** Opened
standalone, `cleaner.weft`'s nodes are bare (`strip`). Navigated into *as
the `clean` instance of a running execution*, they must render as
`clean.strip` to match the journal keys. So navigation carries the alias
path descended through, applied as an id prefix at render-and-lookup
time. Two render modes for the same file, distinguished by whether you
arrived through an execution-instance (prefixed) or opened it standalone
(bare). Purely a lookup-key concern; no editing implication.

**Replay is identical.** `/executions/{color}/replay` returns *all* node
events for the color in one payload (`execution.rs:298`). Populating a
sub-view is a client-side filter on the id prefix
(`e.node.startsWith('clean.')`). Nothing server-side changes.

## Action bar

The action bar (activate / run / deactivate / infra lifecycle) shows
**only on the project's `main.weft` view**, never inside an included
file.

**Why main.weft only.** The action bar manages the lifecycle of the
*whole program*. An included file is a component: it has no independent
lifecycle (it can't be activated on its own; it only runs as part of
whoever includes it). Showing lifecycle controls while viewing a
component would imply you can act on the component, which is a lie.
Managing the system from the system's root view, and inspecting
components by navigating in, is the honest model and the clearer one.

**It survives navigation for free.** The action bar state
(`ActionBarStore`, `extension-vscode/src/actionBarState.ts`) is keyed
**purely by project id** (the UUID from `weft.toml` at the project
root). It has zero file awareness. An included file is the same project,
same id. Navigate into `cleaner.weft` and the project id is unchanged,
the store slot is untouched, a running-color overlay keeps ticking. The
bar's *rendering* is hidden inside sub-files; the state machine
underneath never pauses. Return to `main.weft` and a mid-transition
action bar is exactly where it was.

**A file opened standalone** (to author it, not navigated-into from a
project) has no project context: no action bar (nothing to manage), no
execution overlay (it isn't running as part of anything). The signal
that shows the action bar is "this view is rooted at a project root,"
the same signal as everywhere else.

## The identity principle

Both the execution-following answer and the action-bar answer reduce to
one fact: **the project is the unit of identity, files are syntactic
subdivisions of it.**

- Execution values are keyed by qualified `node_id` → survive nesting.
- Action-bar state is keyed by project id → survives navigation.
- Files never enter either keying.

That's why it composes.

## Rejected alternatives

**Inline expansion (one continuous graph).** Included groups expand
in-place; the parent canvas holds nodes from many files at once; edits
route to whichever file a node's span belongs to. Rejected: it forces a
multi-file span coordinate space and per-edit file-routing (every
surgical edit, undo, re-parse has to stay coherent across N files), and
editing a shared component through one use site silently mutates the
others. The hard span problem this design avoids was entirely an
artifact of this option.

**Breadcrumb navigation.** A breadcrumb implies a single canonical
nesting path. Inclusion is a graph (a file is reachable from many
callers, by many paths), so a breadcrumb would lie. A return-stack is
truthful and simpler.

**Auto-infer the group interface from loose nodes.** Let an included
file be any set of nodes; synthesize the boundary ports from unconnected
edges. Rejected: unenforceable (the file is independent, we can't make
it declare anything), and it makes the file's contract invisible until
something breaks at the call site, the exact problem this feature
exists to kill.

**Implicit interface from `self.*` at file root.** Let `self.x` usage at
the top level auto-construct the in/out ports. Rejected: black magic. A
forgotten `self.port` silently produces a half-wrong interface. The
contract should be a visible declaration, not an inference.

**`@file` extension-dispatch (no `Type` arg).** Guess "string vs
structured" from the file extension (`.txt` → string, `.json` →
object). Rejected: overloads one keyword with two semantics by a hidden
signal. `@file("x.json", String)` (raw text) vs `@file("x.json",
JsonDict)` (parsed) is a real, legitimate distinction the author should
state. The explicit `Type` arg makes it legible and reuses the type
system for validation.

## Implementation order

Dependency order, not shippable phases. One delivery.

1. **`WeftType` text cast** (`weft-core`): the total text→`Type`
   function `@file` needs. Pure, unit-testable (Layer 1).
2. **`@file` parser production** (`weft_compiler.rs`): recognize
   `@file("path", Type)` as a config/literal RHS, resolve relative to
   project root, read + cast, set the value. Validation falls out of the
   existing config/port checks.
3. **`@include` parser production + single-Group check + rescope**
   (`weft_compiler.rs`): recognize the include, parse the target,
   enforce single top-level `Group`, rescope under the alias, feed the
   existing `flatten_group` pipeline.
4. **Two resolution depths**: interface-only resolution for the
   interactive `/parse` path; full recursive inline for `build`/codegen.
5. **Extension navigation**: opaque-block render for includes,
   expand-replaces-view + back-stack + return button, open the real file
   on navigate.
6. **`@file` write-back**: debounced autosave redirecting the field
   write to the referenced file.
7. **Execution-following alias prefix**: apply the descended alias path
   as an id prefix at render-and-lookup time so buffered journal events
   paint in sub-views; replay filters client-side by prefix.

## Open questions

- **Cycle detection.** `a.weft` includes `b.weft` includes `a.weft`.
  The full-inline build must detect include cycles and fail loudly. The
  interface-only interactive resolution terminates naturally (it never
  recurses into bodies), but build does. Needs a visited-set during the
  recursive inline.
- **Path resolution root.** "Relative to the project root" is stated
  above; confirm against how `Project` resolves paths today
  (`crates/weft-compiler/src/project.rs`) and whether includes can reach
  outside the project root (they should not, for the same
  namespace-escape reasons infra specs are constrained).
- **`@file` on a multi-line / heredoc field in the editor.** The field
  renders resolved content; confirm the textarea path handles large
  files without the heredoc re-wrap logic kicking in (the value isn't
  going back into main.weft, so heredoc formatting shouldn't apply).
