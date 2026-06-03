# Graph ↔ Edit Redesign: scope-as-structure, render-as-merge

## The disease

A node's identity today is a **flat dotted string** that fuses *who* with *where*:
`MyGroup_2.MyGroup.debug_4` = local name `debug_4` + scope `["MyGroup_2","MyGroup"]`
collapsed into one mutable token. `NodeDefinition` already carries `scope: Vec<String>`
(project.rs:139), but `id` is still the fused string, and EVERYTHING keys on it:
edges, the layout file (`scopedId @layout ...`), edit-ops, the dispatcher journal.

So a **move** (reparent) has to rewrite that string for the node and every descendant,
and rewrite the layout file's keys in lockstep (`renameLayoutPrefix`). String surgery on a
flat namespace is the bug factory:
- double-prefix compounding (`MyGroup.MyGroup_2.MyGroup.MyGroup_2...` observed in logs),
- rename-desync between code and layout,
- a rebuilt group comes back at height 0 → min-height auto-enforce fires → misread as a
  user resize → spurious ELK → ELK rewrites layout under the renamed ids → corruption.

Every graph glitch this session traces to this one fact: **identity is mutable, and a move
mutates it.**

## The model (what the user actually wants)

The graph is a **pure merge of two sources of truth**:
- **weft code** owns *structure*: which nodes/edges/groups exist, their types, their config,
  and which scope each node lives in. Scope is structural nesting, not a string.
- **the `.layout` file** owns *view state*: position, size, expanded/collapsed, per node.

`render(code, layout) -> graph`. Pure function. An edit mutates code and/or layout, then we
re-render the merge. No optimistic-mutate-then-reconcile, no carrying state across a rebuild,
no id-remap heuristics.

**A move never renames anything.** Moving `debug_4` from `MyGroup` to `MyGroup_2` changes
which scope it is in. Its identity (`debug_4` within its scope) is unchanged in spirit; its
*address* changes because the address is derived from scope. The fix is to stop storing the
fused string as identity and instead derive the address from `(scope, local_id)` on demand,
and key the layout by that structural address.

### Identity decision (settled)

Per discussion, the constraint is: identity must be **invisible** (no new `.weft` syntax) AND
**survive dynamic text edits** (the AI/user rewrites the text and we re-parse). Those two rule
out a persistent uuid (it would have to live in the text to survive a re-parse, which is not
invisible). So:

**Identity = the structural address `{ scope: Vec<local_id>, local_id }`, re-derived from the
parse every time. Never stored, never renamed.** The flat dotted string stops being an
identity; at most it is a *display address* computed on demand. The layout file is keyed by
this structural address.

Accepted tradeoff: a **text rename** of a node (`debug_4` → `foo`) reads as delete+add, so it
loses its layout entry (position/size) and gets re-placed by ELK or a default. Renames are
rare and v1 behaved the same. This is far better than rename-surgery.

### Scope-as-tree decision (settled)

Groups own their members (a real tree), not a flat node list with a `scope` field that
tooling re-derives. The compiled/flattened form can still be a flat list for the runtime, but
the **structural representation the editor and edit-ops work against is a tree**: a group has
`children: Vec<NodeRef>`, a node knows its parent. A move is a single re-parent (detach from
one children list, attach to another) plus a position write. No string rewrite anywhere.

## Scope correction after mapping the blast radius

A full-codebase map (core, lowering, edit-ops, cst-nodes, wire, dispatcher, TS) found that the
flat dotted id is the **runtime/execution address** at ~62 sites: the pulse table, the node
execution table, the journal events, the infra-node Postgres key, the edge index, the
validator's node index. That usage is CORRECT: the runtime executes a *flattened* graph and a
worker has no tree, it needs a flat stable address. Ripping flat ids out of the runtime would
be huge, risky, and unrelated to the graph-editing bug.

So the redesign is **surgical, not total**, and lands on the right boundary:

- The flat dotted id STAYS as the runtime/wire address. Lowering produces it from the tree on
  every parse (it already does this correctly via `scoped()`/`prefix_node_ids`). The runtime,
  journal, infra DB, edges, validator: untouched.
- The disease is purely that the **editor + layout** treat that runtime address as a node's
  IDENTITY, so a move rewrites it with regex prefix surgery (`renameLayoutPrefix`) and the
  render tries to carry view-state across the rewrite. That is what corrupts and shuffles.

Honest constraint we accept: because identity encodes scope, and a move changes scope, a move
MUST re-key the moved subtree's layout entries. The fix is to make that an EXACT structural
re-key (a known set of oldKey->newKey for the moved nodes, derived from the structural delta),
NOT a regex prefix-sweep over the whole file (which compounds: `A.B.A.B...`). And to make
render a pure merge so nothing is carried across the re-parse. A scope-independent key (a
per-node uuid) would make a move touch zero layout, but that needs identity stored in the text
(not invisible) or in memory (does not survive a re-parse), both ruled out. Exact structural
re-key is the correct best given the constraints.

Net: this is a **TS-editor + layout-keying redesign**. No core/wire/runtime change needed; the
wire already sends `scope: string[]` and the flat `id`, from which the editor derives the
structural identity `(scope, localId)`.

## Target shapes

### Core (`weft-core`, `weft-compiler`)

- A node's stored identity is `(scope_path, local_id)`, where `scope_path` is the vec of
  enclosing group local-ids. The flat `id: String` field is removed from the structural model;
  any place that needs a flat address calls a single `fn scoped_address(scope, local) -> String`
  helper (the ONE place the dotted form is produced, for display/wire only).
- `ProjectDefinition` exposes the group tree as the structural source; the flat `nodes` list
  remains as the *flattened runtime form* produced by lowering, clearly separated from the
  *structural form* the editor edits.
- Edit-ops (`edit/ops.rs`) operate on the tree: `moveScope` = reparent (change a node's place
  in the tree), never a string rewrite. `removeGroup`/`addNode`/etc. become tree mutations.
  The CST text edits remain (the `.weft` file is still text), but the IDENTITY logic stops
  doing prefix surgery: a move rewrites the node's *position in the source text* (which group
  block it sits in) without renaming it, because its name never changed.

### Wire types (`weft-core` ↔ TS)

- The wire `NodeDefinition` carries `scope: string[]` + `localId: string` (it already has
  `scope`; add `localId`, drop reliance on the fused `id` as identity). A derived `address`
  (dotted) may still be sent for convenience but is never the key.
- Edges reference endpoints by `(scope, local, port)` structurally, not by the fused string.

### Layout file

- Keyed by the **structural address** (scope-path + local_id), serialized stably. A move
  re-keys cleanly as a structural op (drop old address line, write new), never a regex
  prefix-rewrite. `renameLayoutPrefix` is deleted.
- Format unchanged on disk (`<address> @layout x y [WxH] [state]`); only how the address is
  computed/rekeyed changes.

### Render (`ProjectEditorInner.svelte`)

- `render(project, layout) -> {nodes, edges}` is a pure builder (today's `buildNodes`/
  `buildEdges`, made truly pure: no side effects, no ELK, no optimistic state).
- An edit: compute the structural delta (code) + the view delta (layout), apply both, then
  re-render. Delete `patchFromProject`'s id-carry, the `pendingMovePrefixes` remap, the
  size-carry, the `liveIdOf` translation. They are all compensations for the mutable-id model
  and become dead.
- Optimistic application stays for responsiveness BUT is expressed as the same structural
  delta applied to the in-memory tree, so the subsequent re-parse render is identical to it
  (no divergence to reconcile). Because identity is structural and stable across the move, the
  optimistic tree and the re-parsed tree agree by construction.

### ELK trigger rules (explicit, exhaustive)

ELK runs ONLY on:
1. Open a file/source where some nodes have **no layout entry** (compute their positions).
2. **Resize** (user drags the resize handle; flagged explicitly, never inferred from a
   width/height write).
3. **Expand/collapse** (footprint changes).
4. **Source text changed** by typing/AI: debounced (~0.5s) re-render; ELK fills only nodes
   lacking a layout entry, preserving existing positions.

A plain **move** never triggers ELK: it writes the node's new position to layout and
re-renders the merge. A programmatic dimension write (min-height auto-enforce, a rebuild)
never triggers ELK.

## Implementation order (single delivery, ordered for clean build)

1. **Core node model**: introduce structural identity `(scope, local_id)` + the single
   `scoped_address` helper; make the group tree the structural source. Keep the flat runtime
   list as the lowering output.
2. **Lowering**: stop composing/rewriting the fused id as identity; derive addresses via the
   helper. `prefix_node_ids`/`scoped`/`local_of` string surgery is removed from the identity
   path (kept only if still needed to render the runtime flat form).
3. **Edit-ops**: `moveScope` and the other ops become tree mutations; remove prefix-rename
   logic. The no-op-move and empty-group fixes from earlier collapse into the tree model
   (a no-op move is "same parent", trivially detected; an empty group ungroup is a clean
   subtree splice).
4. **Wire types + dispatcher**: add `localId`/`scope` to the wire, update consumers. Verify
   the journal/store don't depend on the fused id as a stable key in a way that breaks
   (pre-prod, fresh DB, so no migration).
5. **Layout**: key by structural address; delete `renameLayoutPrefix`; a move re-keys cleanly.
6. **Render**: pure merge; delete the id-carry/remap/size-carry compensations; wire the four
   ELK triggers exactly.
7. **Tests**: layer-1 for `scoped_address` + tree mutations; edit-ops move/ungroup/no-op;
   wire round-trip; and the graph render-merge invariants.

## What gets deleted (the compensations)

- `pendingMovePrefixes`, `liveIdOf`, the size-carry block in `patchFromProject`.
- `renameLayoutPrefix` and every caller.
- The `resized` flag becomes unnecessary IF the min-height-at-0 cause is removed by carrying
  size structurally; keep the explicit resize signal anyway (it is the honest way to say "user
  resized" vs "layout changed"), but it is no longer load-bearing against a bug.
- Prefix string-surgery in lowering/edit-ops identity paths.

## Non-goals (this delivery)

- Progressive ELK (v1 had it). Not needed now that included files keep `.weft` files small;
  revisit if large single files reappear.
- Author-visible node ids. Identity stays invisible/structural.
