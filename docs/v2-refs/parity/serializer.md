# Surgical Serializer Parity

**v1 source**: `dashboard-v1/src/lib/ai/weft-editor.ts` (1637 lines).

v1 runs ALL surgical edits in the frontend (TypeScript). v2 runs
them in the dispatcher (Rust surgical layer,
`extension-vscode/src/surgical.ts` + eventually a Rust
equivalent). This doc captures every operation we need to
reproduce on the v2 side.

## Exported functions (surface API)

| function | purpose |
|----------|---------|
| `updateNodeConfig(code, nodeId, key, value)` | Set/remove a single config field on a node. Expands one-liners as needed. |
| `updateNodeLabel(code, nodeId, newLabel)` | Alias for `updateNodeConfig(... 'label', ...)`. |
| `addNode(code, nodeType, nodeId, parentGroupId?)` | Append a new node declaration at the right scope. |
| `addGroup(code, label, parentGroupId?)` | Append a new empty group scope. |
| `renameGroup(code, oldLabel, newLabel)` | Rename group + rewrite all connection refs. |
| `removeGroup(code, groupLabel)` | Remove group block; children are hoisted up one scope (de-indented). |
| `removeNode(code, nodeId)` | Remove node + all connections referencing it. |
| `addEdge(code, srcId, srcPort, tgtId, tgtPort, scopeGroupId?)` | Add a connection. Replaces any existing driver on the target port. |
| `removeEdge(code, srcId, srcPort, tgtId, tgtPort)` | Remove a connection. If it's an inline-anon binding, materialize the anon into a standalone declaration at the same scope. |
| `moveNodeScope(code, nodeId, targetGroupLabel?)` | Move a node between scopes (with re-indent, connection rewrite, self-ref handling). |
| `moveGroupScope(code, groupLabel, targetGroupLabel?)` | Same for a group. |
| `updateNodePorts(code, nodeId, inputs, outputs)` | Rewrite the port signature on a node declaration. |
| `updateGroupPorts(code, groupLabel, inputs, outputs)` | Same for a group header. |
| `updateProjectMeta(code, name?, description?)` | Rewrite the file's `# Name: ...` and `# Description: ...` header lines. |

## Layout code functions (separate string, `.layout` file in v2)

| function | purpose |
|----------|---------|
| `parseLayoutCode(layoutCode)` | Parse `@layout` directives into a Record<id, {x, y, w?, h?, expanded?}>. |
| `updateLayoutEntry(code, id, x, y, w?, h?, expanded?)` | Update or insert one layout entry. |
| `removeLayoutEntry(code, id)` | Remove one entry. |
| `renameLayoutPrefix(code, oldScopedId, newScopedId)` | Rename all entries whose id starts with `oldScopedId + '.'` or equals `oldScopedId`. Used when a group is renamed. |

## Key implementation techniques

### Parse-per-edit

Every `updateX` call starts with `parseForEdit(code)` which calls
`parseRawWeft` once. The surgical edit uses `configSpans`,
`startLine`, `endLine`, `rawLines` from the parsed structures to
locate the right bytes. **There's no persistent AST.** Each edit
reparses.

For the v2 dispatcher, same approach: every mutation kicks off a
reparse via `weft-compiler::parse_weft`. Cheap enough on localhost
(<1ms for files <10k nodes).

### One-liner expansion (line 417-463)

If a node is written as `id = Type { key: val }` or
`id = Type.port`, expand to multi-line before editing:

```
id = Type {
  key: val
}
```

This gives every field its own line, so `configSpans` has
per-field granularity and the edit can splice without touching
the declaration header. Three cases handled:
- A: one-liner with `{body}`
- B: inline anon `host.data = Type.port`
- C: bare canonical node `id = Type` (no body)

### Config field spans

`ParsedNode.configSpans: Record<key, ConfigFieldSpan>` carries
per-field line ranges with origin tag (`inline` vs `connection`).
`updateNodeConfig` uses the span to replace ONLY those lines, not
the whole node.

v2's Rust compiler needs to start producing these spans. Backend
todo already queued.

### Connection insertion point

`addEdge` picks where to insert based on scope:
- If `scopeGroupId` provided: find the group, insert before its
  closing `}` at `innerIndent`.
- Else: append at end of file, after the last non-blank line.

### Connection replacement (1-driver-per-input)

When adding an edge, first check if the target port already has a
driver. If so:
- If same src/tgt: no-op (idempotent).
- Else: remove the old edge first (via `removeEdge`, which
  handles inline-anon materialization), then append the new one.

### Inline anon materialization

Weft supports inline anonymous nodes: `host.data = Type { body }`.
When you disconnect the anon's binding, the anon has to survive
as a standalone declaration. `tryMaterializeAnon` (line 769-886)
detects the anon by id convention (`{hostLocal}__{field}`), then
rewrites the source so the anon becomes a top-level `id = Type {
body }` at the same scope.

### De-indent on `removeGroup`

Children are promoted up one scope. `innerIndent` is stripped to
`indent` on each child line. Self-connections (`self.x = ...`)
are dropped because they reference ports that no longer exist.
External connections referencing the group's id are also removed.

### Group rename: regex + scope

`renameGroup` rewrites the group header line + every line
matching `\b{oldLabel}\.` (word-boundary + dot). This catches
both `oldLabel.out = ...` and `child.input = oldLabel.out`, but
is scope-agnostic — the whole file is walked.

**Caveat**: this can hit false positives if a same-named local
identifier exists elsewhere. v1 accepts the risk (users don't
usually reuse labels).

### Move node scope

`moveNodeScope(code, nodeId, targetGroupLabel?)`:
1. Find the node.
2. Remove it from its current scope.
3. Find the target group (or top-level).
4. Adjust indentation.
5. Re-insert.
6. Update all connection references: local-id paths become
   fully-qualified if crossing a scope boundary; `self.x` refs
   get rewritten if the destination scope lacks matching ports;
   etc.

Similar flow for `moveGroupScope`.

### Update ports

`updateNodePorts` / `updateGroupPorts` rebuild the declaration
line(s) with a fresh port signature:

```
id = Type(*port1: T, port2: U?) -> (out: V, err: MustOverride) {
  ...
}
```

Steps:
1. Extract current port names from the declaration header +
   post-config arrow (via `extractSignaturePortNames` +
   `extractPostConfigPortNames`).
2. Compare to target port list.
3. Rebuild signature via `buildSignature(inputs, outputs)`.
4. Replace only the signature region (keep body intact).
5. `invalidateOrphanedConnections` drops any connection that
   references a port that no longer exists.

## Layout directive format

```
@layout {
  nodeId: x=100 y=200 w=300 h=150
  anotherNode: x=400 y=200
  Outer.Inner: x=0 y=0 w=500 h=400 expanded=true
}
```

`parseLayoutCode` reads this into a map. `updateLayoutEntry`
replaces or inserts one line. `renameLayoutPrefix` is the
interesting one: when a group is renamed `Outer` → `Bigger`,
every entry starting with `Outer.` becomes `Bigger.`.

v2 uses a separate `<doc>.layout.json` file instead of in-source
@layout, which is the design decision from `getting-started.md`.
The ops are equivalent; storage differs.

## v2 port status

`extension-vscode/src/surgical.ts` has:
- `addNode`, `removeNode`, `addEdge`, `removeEdge`,
  `updateConfig`, `updateLabel`, `duplicateNode`: DONE.

Missing:
- `addGroup`, `removeGroup`, `renameGroup`, `updateGroupPorts`,
  `moveNodeScope`, `moveGroupScope`, `updateNodePorts`,
  `updateProjectMeta`.
- Per-field config-span granularity: currently `updateConfig`
  rewrites the whole node. Blocked on backend adding
  per-field `config_spans`.
- `invalidateOrphanedConnections` when updating ports.
- Inline anon materialization when removing a binding edge.
- One-liner expansion when editing a field.

### Port plan

Stage 1: land the simple ops (addGroup, renameGroup, removeGroup,
updateGroupPorts, moveNodeScope, moveGroupScope, updatePorts,
updateProjectMeta). These only need whole-region replacements +
regex rewrites — no per-field spans.

Stage 2: land per-field config spans from the backend + port
`updateNodeConfig`'s fine-grained splice (including one-liner
expansion + field insertion at the right indent).

Stage 3: inline anon materialization (complex; defer until
use case arises).

## Divergences

- v2 stores layout in a sidecar JSON file; v1 stores as @layout
  directive in the weft source. Functionally equivalent, not
  porting @layout syntax.
- v2 runs ALL these ops in the dispatcher (Rust), not the
  extension (TypeScript). The surgical.ts file in the extension
  is a thin shim that builds `vscode.WorkspaceEdit` from the
  returned text diff; the rewrite logic lives serverside. This
  keeps the single-parser guarantee.
