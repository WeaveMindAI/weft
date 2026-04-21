# Surgical Serializer Parity

**v1 source**: `dashboard-v1/src/lib/ai/weft-editor.ts` (1637 lines).

v1 runs ALL surgical edits in the frontend (TypeScript). v2 runs
them in the dispatcher (Rust surgical layer). The extension host
file `extension-vscode/src/surgical.ts` is a thin shim that
applies the dispatcher's edits as `vscode.WorkspaceEdit` ops.
This doc captures every operation we need to reproduce in the
Rust dispatcher.

## Full exported API (line number in v1)

| function | line | purpose |
|---|---|---|
| `updateNodeConfig(code, nodeId, key, value)` | 355 | Set/remove a single config field. Expands one-liners. |
| `updateNodeLabel(code, nodeId, newLabel)` | 507 | Alias for updateConfig('label'). |
| `addNode(code, nodeType, nodeId, parentGroupId?)` | 514 | Append a new node declaration at the right scope. |
| `addGroup(code, label, parentGroupId?)` | 543 | Append an empty group. |
| `renameGroup(code, oldLabel, newLabel)` | 576 | Rename + rewrite all connection refs. |
| `removeGroup(code, groupLabel)` | 600 | Remove group; children hoisted up one scope. |
| `removeNode(code, nodeId)` | 644 | Remove node + all connections referencing it. |
| `addEdge(code, srcId, srcPort, tgtId, tgtPort, scopeGroupId?)` | 672 | Add edge. Replaces existing driver. Handles inline-anon binding. |
| `removeEdge(code, srcId, srcPort, tgtId, tgtPort)` | 740 | Remove edge. Materializes anon if it was a binding. |
| `moveNodeScope(code, nodeId, targetGroupLabel?)` | 888 | Move node between scopes. Rejects on edges. |
| `moveGroupScope(code, groupLabel, targetGroupLabel?)` | 969 | Move group between scopes. Rejects on boundary edges. |
| `updateNodePorts(code, nodeId, inputs, outputs)` | 1336 | Rewrite port signature on a node declaration. |
| `updateGroupPorts(code, groupLabel, inputs, outputs)` | 1432 | Same for a group header. |
| `updateProjectMeta(code, name?, description?)` | 1500 | Rewrite `# Name:` / `# Description:` header lines. |

Layout code (separate string, `.layout.json` file in v2):

| function | line | purpose |
|---|---|---|
| `parseLayoutCode(layoutCode)` | 1531 | Parse `@layout` directives into a map. |
| `updateLayoutEntry(...)` | 1557 | Update/insert one entry. |
| `removeLayoutEntry(layoutCode, scopedId)` | 1580 | Remove one entry. |
| `renameLayoutPrefix(layoutCode, oldScopedId, newScopedId)` | 1607 | Rename all entries starting with `oldScopedId.` or equal. |

Internal helpers:

| function | line | purpose |
|---|---|---|
| `parseForEdit(code)` | 28 | Wraps parseRawWeft, returns nodes+groups+connections. |
| `nodeToLocation(lines, node)` | 40 | ParsedNode → { startLine, endLine, indent }. |
| `groupToLocation(lines, group)` | 50 | Same for groups + contentStart + innerIndent + isOneLiner. |
| `findNode(lines, nodeId, scopeGroupId?)` | 72 | Locate a node by local or scoped id. |
| `findNodeScoped(lines, scopedId)` | 160 | Fully-scoped variant. |
| `findGroup(lines, groupName)` | 108 | Locate by label or scoped id. |
| `findConnection(lines, srcId, srcPort, tgtId, tgtPort)` | 122 | Return line number of matching conn, or null. |
| `expandOneLinerGroup(lines, group)` | 165 | Inline `= Group() -> () {}` → multi-line form. |
| `escapeRegex(s)` | 181 | RegExp-escape string. |
| `removeConnectionsReferencing(lines, identifier)` | 186 | Filter out all conn lines referencing identifier. |
| `extractInlineBodySpan(line)` | 213 | Parse one-liner body `Type { body }.port` forms. |
| `splitBodyPairs(body)` | 264 | Split body by top-level commas, respecting brackets. |
| `formatConfigValue(value)` | 291 | unknown → weft-source string (JSON for objects, heredoc for multi-line). |
| `findParsedNode(parsed, nodeId)` | 319 | Lookup in parsed.nodes + all groups.nodes. |
| `expandNodeToMultiLine(code, node)` | 417 | One-liner `id = Type {body}` → multi-line. 3 cases (A/B/C). |
| `buildFieldLines(prefix, formattedValue, indent)` | 472 | Assemble field line(s), handling multi-line values. |
| `insertFieldInNode(lines, node, key, formattedValue)` | 489 | Insert a new field in node body before `}`. |
| `tryMaterializeAnon(lines, anonId)` | 769 | Detach inline-anon binding, materialize standalone. |
| `formatPort(p)` | 1053 | PortDefinition → port source syntax (`name: Type?`, etc). |
| `buildSignature(inputs, outputs)` | 1060 | Assemble `(inputs) -> (outputs)` declaration string. |
| `extractSignaturePortNames(lines, startLine)` | 1073 | Parse current port names + sigEndLine from declaration. |
| `extractPostConfigPortNames(lines, node)` | 1161 | Parse `} -> (outputs)` post-config output ports. |
| `extractPortNamesFromArrow(afterBrace, lines, startLine)` | 1192 | Helper for post-config. |
| `extractPortNamesFromText(text)` | 1208 | Extract port names from arbitrary paren-wrapped text. |
| `rebuildDeclarationWithPorts(...)` | 1226 | Rewrite node declaration + post-config with new ports. |
| `rewriteInlinePrefix(...)` | 1269 | Rewrite the prefix of an inline-anon body's first line. |
| `updateAnonPorts(...)` | 1295 | Update an inline anon's port signature. |
| `updateAnonPortsMultiLine(...)` | 1316 | Same for multi-line anon. |
| `invalidateOrphanedConnections(code, nodeId, inputs, outputs)` | 1452 | Drop conn lines that reference ports no longer in the signature. |
| `cleanBlankLines(lines)` | 1624 | Collapse 3+ blank lines to 2. |
| `isSelfConnection(trimmed)` | 636 | Detect `self.x = ...` or `... = self.x`. |
| `formatLayoutStr(x, y, w?, h?, expanded?)` | 1590 | Produce `@layout` entry string. |

## Key algorithms

### `updateNodeConfig(code, nodeId, key, value)` — the most complex

```ts
1. Reject reserved keys: ['textareaHeights', '_opaqueChildren'].
   (Other reserved keys: 'parentId', 'width', 'height', 'expanded'
   are layout; handled by layoutUpdateAny, not this function.)
2. parseForEdit(code) → find node.
3. If node is a one-liner (startLine === endLine):
   expandNodeToMultiLine(code, node); reparse; refresh node.
4. removing = (value === undefined || value === null).
5. oldSpan = node.configSpans[key].

6. If oldSpan exists:
   - If removing: lines.splice(startLine-1, count). cleanBlankLines.
   - Else (replace):
     - formattedValue = formatConfigValue(value).
     - oldIndent = extract from lines[startLine-1].
     - prefix = origin === 'connection'
       ? existing `${srcRef} = ` from the line
       : `${key}: `.
     - buildFieldLines(prefix, formattedValue, oldIndent) → new lines.
     - lines.splice(startLine-1, count, ...newLines).

7. If no oldSpan (inserting fresh):
   - If removing: noop.
   - Else: insertFieldInNode(lines, node, key, formattedValue).

8. cleanBlankLines + join.
```

Origin preservation matters: when a field was originally set via
a connection line (`node.key = "val"`), the replacement keeps
that form. Rewriting as an inline `key: val` would be
visually-equivalent but semantically-different (different
`configSpans.origin`).

### `expandNodeToMultiLine(code, node)` (line 417-463)

Three cases:

**Case A — inline brace body**: `id = Type { body }` →
```
id = Type {
  body-pair-1
  body-pair-2
}
```
`extractInlineBodySpan(line)` identifies the prefix before `{`,
the body content, and the suffix after `}`. Pairs split by
`splitBodyPairs` (respecting brackets/quotes).

**Case B — bare inline anon**: `host.data = Type.port` →
```
host.data = Type {
}.port
```

**Case C — bare canonical node**: `id = Type` or
`id = Type(ports) -> (ports)` →
```
id = Type(ports) -> (ports) {
}
```

All three preserve indentation + suffix.

### `formatConfigValue(value)` (line 291-318)

```ts
if (value === null || value === undefined) → 'null';
if (typeof === 'boolean' || 'number') → String(value);
if (typeof === 'string'):
  if contains '\n' → triple-backtick heredoc;
  else → JSON-quoted.
if (Array.isArray || typeof === 'object') → JSON.stringify(value, null, 2).
```

Multi-line strings use the heredoc:
```
key: ```
line 1
line 2
```
```

Escaping: if the string contains `\``\``, escape as `\\\`\`\``
in the output (parser decodes on read).

### `buildFieldLines(prefix, formattedValue, indent)` (line 472-488)

```ts
const valueLines = formattedValue.split('\n');
const out = [`${indent}${prefix}${valueLines[0]}`];

// For multi-line values:
// - heredoc continuation lines stay at column 0 (parser's dedent strips common whitespace)
// - JSON/list continuation lines match the body indent so the closing ] / } lines up
const continuationIndent = valueLines[0].endsWith('```') ? '' : indent;
for (let i = 1; i < valueLines.length; i++) {
  out.push(`${continuationIndent}${valueLines[i]}`);
}
return out;
```

### `tryMaterializeAnon(lines, anonId)` (line 769-878)

Critical correctness path. When removing a binding edge (where
the source is an inline anon), the anon mustn't silently
disappear. It gets materialized into a standalone declaration at
the same scope.

Two forms to handle:

**Connection-line form**: anon lives AFTER the parent:
```
parent = Parent { ... }
parent.field = Type { ... }.port
```
→
```
parent = Parent { ... }
parent__field = Type { ... }
```

Rewrite: replace `parent.field = ` with `parent__field = `; strip
the trailing `.port`.

**Config-block form**: anon lives INSIDE the parent's body:
```
parent = Parent {
  field: Type { ... }.port
}
```
→ remove the anon lines from the parent, then insert standalone
after the parent's `}`:
```
parent = Parent {
}

parent__field = Type { ... }
```

Multi-line anon bodies are handled by walking from startLine to
endLine, stripping the leading `field:` prefix on the first line
and the trailing `}.port` suffix on the last line.

### `addEdge(code, src, srcPort, tgt, tgtPort, scopeGroupId?)` (line 672-734)

```ts
1. parseForEdit(code). Find any existing connection with the
   same target (targetId.targetPort or self-flag match).

2. If existing found:
   - If exact duplicate (same src/tgt/ports): return code unchanged.
   - Else: removeEdge(existing...) to materialize anon bindings if
     applicable, then continue.

3. Build connection line: `${tgtId}.${tgtPort} = ${srcId}.${srcPort}`.

4. If scopeGroupId: findGroup, insert before the closing `}` at
   innerIndent.
   Else: append at end of file (after last non-blank line).
```

The existing-edge check is the "one-driver-per-input" enforcement
at the source level. `parseForEdit` walks BOTH `parsed.connections`
AND `parsed.groups.flatMap(g => g.connections)` so connections
inside groups are checked too.

### `removeEdge(code, src, srcPort, tgt, tgtPort)` (line 740-768)

```ts
1. Detect if the source is an inline anon whose id matches
   `${tgtLocal}__${tgtPort}` (the convention for anon bindings).

2. If it's a binding edge: tryMaterializeAnon → if successful,
   return that result.

3. Else: findConnection → lines.splice the connection line.
   cleanBlankLines.
```

### `moveNodeScope(code, nodeId, targetGroupLabel?)` (line 888-964)

```ts
1. findNodeScoped(lines, nodeId) → location.
2. parseForEdit(code) → find current scope (enclosing group id, or
   undefined for root).
3. Normalize targetGroupLabel → targetScopeId.
4. If same scope: return unchanged.
5. Connected-node guard: if the node has ANY edge whatsoever
   (looking at all connections in all scopes), return unchanged.
   (Conservative rule. Disconnect first, then move.)
6. Extract the node block via lines.splice.
7. Strip old indent, apply new indent for target scope.
8. If target is a group:
   - If the target is a one-liner `{}`: expandOneLinerGroup first.
   - Insert before group's closing `}` at innerIndent.
   Else: append at end of file.
9. cleanBlankLines.
```

The "any edge" guard is stricter than necessary (scope-reachability
analysis could allow moves when edges stay legal), but v1 takes
the safe path.

### `moveGroupScope(code, groupLabel, targetGroupLabel?)` (line 969-1052)

Similar logic but stricter: any edge touching the group's
BOUNDARY PORTS blocks the move. Internal edges stay with the
group when it moves; external edges would break.

### `updateNodePorts(code, nodeId, inputs, outputs)` (line 1336-1431)

```ts
1. Find node.
2. Extract current port names via extractSignaturePortNames +
   extractPostConfigPortNames.
3. Compare to target inputs/outputs. If no change, return.
4. rebuildDeclarationWithPorts(...) rewrites the declaration line(s):
   - Choose form based on context: if current is `Type(inputs) -> (outputs)` keep that form, else use minimal form.
   - buildSignature(inputs, outputs) produces the new `(...)` block.
   - If post-config outputs were used (`} -> (...)`), preserve that.
   - Replace lines [startLine .. sigEndLine].
5. invalidateOrphanedConnections(code, nodeId, inputs, outputs).
   - Walk all conn lines referencing the node.
   - If a conn references a port that's not in the new port list,
     remove the line.
```

**Inline anon port updates** (line 1295-1335): if the node is an
inline anon (living inside another's body), call `updateAnonPorts`
or `updateAnonPortsMultiLine`. The anon's "declaration" is spread
across the parent's body; we rewrite the Type signature in place.

### `updateGroupPorts(code, groupLabel, inputs, outputs)` (line 1432-1451)

Same idea, targets a group header:
`groupLabel = Group(inputs) -> (outputs) { ... }`. Rewrites the
signature in the header line (multi-line tolerant), then calls
`invalidateOrphanedConnections` for both the group's id and its
`self.` refs.

### `renameGroup(code, oldLabel, newLabel)` (line 576-597)

```ts
1. findGroup(lines, oldLabel).
2. Rewrite header line: regex replace
   `^(\s*)${escapedOld}(\s*=\s*Group)` → `$1${newLabel}$2`.
3. Walk ALL other lines (except the header): regex replace
   `\b${escapedOld}\.` → `${newLabel}.`.
```

**Caveat**: step 3 is regex-global; a same-named local identifier
elsewhere would be false-positively renamed. v1 accepts this;
users rarely reuse labels.

### `removeGroup(code, groupLabel)` (line 600-633)

```ts
1. findGroup.
2. If one-liner: splice the single line, remove connections,
   cleanBlankLines.
3. Else:
   - Collect child lines between contentStart and endLine.
   - SKIP self-connections (self.x = ...) — they reference ports
     that no longer exist.
   - De-indent children by one level (strip innerIndent, re-add indent).
   - lines.splice(startLine, endLine - startLine + 1, ...deindented).
   - removeConnectionsReferencing(lines, groupLabel) for external refs.
```

### `removeConnectionsReferencing(lines, identifier)` (line 186-211)

```ts
const re = new RegExp(`\\b${escapedIdentifier}\\.`);
return lines.filter(l => {
  const t = l.trim();
  if (!t || t.startsWith('#')) return true;  // keep blank + comments
  // Only filter connection lines
  const isConn = /^\w[\w.]*\.\w+\s*=\s*\w[\w.]*\.\w+/.test(t);
  if (!isConn) return true;
  return !re.test(t);  // drop if references the identifier
});
```

### `updateProjectMeta(code, name?, description?)` (line 1500-1530)

```ts
// Walk header lines (before first declaration).
// Update or insert:
//   # Name: ${name}
//   # Description: ${description}
//
// If name/description is explicitly null/undefined: remove.
// If unchanged: noop.
// Description can be multi-line:
//   # Description: foo
//   #              bar
```

### Layout code operations (line 1531-1622)

`@layout` directive format:
```
@layout {
  nodeId: x=100 y=200 w=300 h=150 expanded=true
  Outer.Inner: x=0 y=0 w=500 h=400
  childNode: x=50 y=30
}
```

`parseLayoutCode(layoutCode)` regex-parses each entry into
`{x, y, w?, h?, expanded?}`.

`updateLayoutEntry(code, scopedId, x, y, w?, h?, expanded?)`:
- If `@layout` block doesn't exist: append one.
- If id exists: replace the line.
- Else: insert in the block.

`renameLayoutPrefix(code, oldScopedId, newScopedId)`:
- Walk entries; if an id equals `oldScopedId` or starts with
  `${oldScopedId}.`, replace the prefix with `newScopedId`.
- Used by group rename to rename all descendants' layout entries.

**v2 uses a separate `.layout.json` file**, not in-source. Same
operations (parse/update/remove/rename-prefix), different storage.
`getting-started.md` covers this decision.

### `cleanBlankLines(lines)` (line 1624-1634)

Collapse 3+ consecutive blank lines to 2. Idempotent. Called at
the end of almost every edit so the output doesn't accumulate
blank lines.

## Parse-per-edit contract

Every `updateX` call starts with `parseForEdit(code)` which runs
`parseRawWeft`. The surgical edit uses `configSpans`, `startLine`,
`endLine`, `rawLines` from the parsed structures. **No persistent
AST.** Each edit reparses.

On localhost, v1 parses a 1k-line .weft in <5ms. Roundtrip is
parse-edit-emit. The v2 Rust parser will be faster.

## Round-trip fidelity

All operations above are designed to preserve:
- Item ordering (via `itemOrder` preservation).
- Blank-line spacing (`itemGaps`).
- Opaque blocks (verbatim).
- Unedited config field text (spans prevent rewriting).
- Comments (kept by not touching non-declaration lines).
- Inline anons (materialized only on explicit disconnect).

## v2 port status (Rust dispatcher)

### Done (extension-vscode/src/surgical.ts)
- `addNode`, `removeNode`, `addEdge`, `removeEdge`,
  `updateConfig`, `updateLabel`, `duplicateNode`.

### Missing (queued)
- `addGroup`, `removeGroup`, `renameGroup`, `updateGroupPorts`.
- `moveNodeScope`, `moveGroupScope`.
- `updateNodePorts` + `invalidateOrphanedConnections`.
- `updateProjectMeta`.
- Per-field `config_spans` in the Rust parser (blocks
  granular `updateConfig`; currently rewrites whole node).
- `expandNodeToMultiLine` (blocks granular updateConfig on
  one-liners).
- `tryMaterializeAnon` (blocks safe `removeEdge` for anon
  bindings).
- Layout ops: `parseLayoutCode`, `updateLayoutEntry`,
  `removeLayoutEntry`, `renameLayoutPrefix`. v2 uses
  `.layout.json` sidecar, ops are trivial JSON operations.

### Port plan stages

**Stage 1 (simple ops, whole-region replacement + regex)**:
addGroup, removeGroup, renameGroup, updateGroupPorts,
moveNodeScope, moveGroupScope, updateNodePorts, updateProjectMeta,
invalidateOrphanedConnections.

**Stage 2 (requires backend parser work)**: per-field
config_spans. Once Rust parser emits them, port
`updateNodeConfig` to do granular splices instead of whole-node
rewrites. Includes expandNodeToMultiLine.

**Stage 3 (complex; defer until use case)**: tryMaterializeAnon
inline-anon materialization. Only needed when a user disconnects
an edge whose source is an inline anon node. Current fallback:
edge removal drops the anon too. Acceptable in phase A.

## Divergences

- v2 stores layout in a sidecar JSON file; v1 stores as @layout
  directive in the weft source. Same semantics, different
  storage.
- v2 runs ALL these ops in the dispatcher (Rust), not the
  extension (TypeScript). Single parser, single serializer.
- Inline anon materialization is deferred to stage 3.
