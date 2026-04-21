# Parser Parity (v1 frontend parser)

**v1 source**: `dashboard-v1/src/lib/ai/weft-parser.ts` (5461 lines).
**v2 source**: `crates/weft-compiler/src/weft_compiler.rs`. v2
parses in Rust; the webview consumes `/parse` output only. This
doc captures the v1 parser's BEHAVIOR so we know what shape of
data the webview needs (and, by mirror, what the Rust parser
must expose).

## Three-stage pipeline

```
parseWeft(raw) →                                         // entry, line 5444
  extractAllWeftBlocks(raw)                              // split on ````weft fences
  for each block:
    parseRawWeft(block) → ParseResult                   // lexical parse, line 3030
    validateAndBuild(result) → ProjectDefinition         // line 4074
  autoOrganize(project.nodes, edges)                    // separate ELK pass, line 4714
```

`extractAllWeftBlocks` uses `/````weft\s*\n/` as opener and
`\n````` as closer. A text document can contain multiple blocks;
each becomes its own project.

## Top-level data shapes

### ParseResult (line 2988-2998)

```ts
{
  name: string;              // from `# Name:` header at root
  description: string;       // from leading # comment block
  nodes: ParsedNode[];
  connections: ParsedConnection[];
  groups: ParsedGroup[];     // recursive; includes nested via parentGroupId
  opaqueBlocks: OpaqueBlock[];
  nodeOrder: string[];       // declaration order, for serializer round-trip
  itemOrder: string[];       // "node:ID" | "group:ID" | "conn:ID" | "opaque:N"
  itemGaps: number[];        // blank-line count before each item
}
```

`itemOrder` + `itemGaps` together preserve the user's exact source
ordering (blank lines between items matter for readability; the
serializer reproduces them).

### ParsedNode (line 1614-1635)

```ts
{
  id: string;                  // scoped for nested: "Outer.child"
  nodeType: string;
  label: string | null;        // from `label: "..."` config line
  config: Record<string, unknown>;
  parentId?: string;           // scopeId of containing group
  startLine: number;           // 1-indexed, inclusive
  endLine: number;
  rawLines: string[];          // verbatim slice of lines
  inPorts: ParsedInterfacePort[];   // from `in()`/inline signature
  outPorts: ParsedInterfacePort[];
  oneOfRequired: string[][];
  configSpans: Record<string, ConfigFieldSpan>;  // per-field line ranges
}
```

### ParsedConnection (line 1637-1647)

```ts
{
  sourceId: string;
  sourcePort: string;
  targetId: string;
  targetPort: string;
  line: number;                // 1-indexed
  rawText: string;
  sourceIsSelf?: boolean;      // true when source was "self.x" inside a group
  targetIsSelf?: boolean;
  scopeId?: string;            // group scope this connection lives in
}
```

`sourceIsSelf` + `sourceId === scopeId` is how the parser encodes
"the source of this edge is the group's own boundary port (not
any child)". Propagates into edge construction, where `__inner`
is appended to the handle id.

### ParsedInterfacePort (line 1649-1654)

```ts
{
  name: string;
  portType: string;            // raw type string, not parsed WeftType
  required: boolean;           // default true unless `?` suffix or optional keyword
  laneMode: LaneMode | null;   // 'Single' | 'Expand' | 'Gather'
}
```

### ParsedGroup (line 1656-1673)

```ts
{
  id: string;                       // scoped: "Outer.Inner"
  originalName?: string;            // local name; for collision renames
  description?: string;             // leading # comment block in group body
  inPorts: ParsedInterfacePort[];
  outPorts: ParsedInterfacePort[];
  oneOfRequired: string[][];
  nodes: ParsedNode[];              // direct children only
  connections: ParsedConnection[];  // connections inside this scope
  startLine: number;
  endLine: number;
  parentGroupId?: string;           // for nested groups
  rawLines: string[];
}
```

**Group ids use dotted scoping**: `Outer.Inner.Inner2`. Child
node ids carry the prefix: `Outer.Inner.someNode`. `buildScopeChain`
(line 352) expands `"Outer.Inner"` → `["Outer", "Outer.Inner"]`.

### OpaqueBlock (line 2980-2986)

```ts
{
  startLine: number;
  endLine: number;
  text: string;
  error: string;
  anchorAfter: string | null;   // "node:ID" | "group:ID" | "conn:ID" | null
}
```

Unparseable regions that the editor keeps verbatim so save → load
round-trips without losing user text. `anchorAfter` preserves
position relative to surrounding nodes.

## `parseScope` recursion (line 1682)

Unified parser for root scope AND group scopes. Differences
controlled by `isRoot`:
- Root: no `}` terminator, no `in:`/`out:` section, has `# Name:` /
  `# Description:` headers.
- Group: terminated by `}`, has `in:`/`out:` section, description
  captured from first `#` comment block.

State machine tracks:

- Current node block (when accumulating `id = Type { ... }` body).
- Current multi-line heredoc (when inside ```...```).
- Current port section (when inside `in(` or `out(`).
- Current opaque block (accumulated until a valid declaration flushes).
- Blank line count (for itemGaps).
- `scopedIdMap` for collision-renamed ids.

### Node declaration parse (line 2099-2360)

```ts
const declMatch = trimmed.match(/^([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*([A-Z][a-zA-Z0-9]*)(.*)$/);
```

Matches `id = Type(...)...`. Reserves `self` as special (line
2102) so `self = Type` is not parsed as a node.

After the header, collects the port signature across possibly
multiple lines if parens aren't balanced. Handles:
- `id = Type(inputs) -> (outputs) { body }`
- `id = Type -> (outputs) { body }` (no inputs)
- `id = Type { body }` (no signature)
- `id = Type` (bare; empty body)
- Multi-line signatures with `->` on a subsequent line.

### Group declaration (line 2277-2360)

```ts
if (declType === 'Group') {
  // ...flush pending node/opaque...
  const groupName = declId;
  let groupId = isRoot ? groupName : `${scopeId}.${groupName}`;
  // Collision: if another group already has this id, suffix __2, __3, ...
  if (allGroups.some(g => g.id === groupId)) {
    let suffix = 2;
    let candidate = `${groupId}__${suffix}`;
    while (allGroups.some(g => g.id === candidate)) {
      candidate = `${groupId}__${suffix++}`;
    }
    groupId = candidate;
  }
  // Recurse into body via parseScope.
  // `scopedIdMap` maps pre-scope id to final id for rewrites later.
}
```

Handles three body shapes:
- `{}` → empty
- `{` → start multi-line scope via `parseScope`
- unexpected content → error

### Config field parsing inside a node body (line 1382-1600)

`parseNodeBlockBody` loops through body lines:

1. **Multi-line heredoc** handling: if a line starts with
   `key: \``\``, enter heredoc mode. Close on a line that's
   exactly the delimiter OR ends with it not preceded by `\`.
   Literal `\``\`` backticks stay in the content; the close
   rewrites `\\\`\`\`` → `\`\`\``. Dedent on close (line 1421).
2. **`}`**: end of block.
3. **`} -> (outputs)`**: post-config output ports (line 1440).
   Collects multi-line if parens not balanced.
4. Blank / comment lines: skip.
5. **`in(...)`, `out(...)` port sections** via `tryParsePortLine`.
   Handles inline + multi-line. See below for port syntax.
6. **`label:`** — extracted to `label` field, not config (line 1478).
7. **Inline expression check** (line 1489): `key: Type { ... }.port`
   or bare `Type.port` → emits a child node + edge in `inlineScope`.
8. **Port wiring** (line 1501): `key: source.port` where
   source.port is an unquoted dotted ref → emits an edge.
9. **Triple backtick multiline** (line 1517): single-line + multi-
   line both handled.
10. **Regular config**: `key: value`. Value parsed via
    `parseConfigValue`.

### `parseConfigValue` (line 791-809)

```ts
if (rawValue === 'true') return true;
if (rawValue === 'false') return false;
if (rawValue === 'null') return null;
if (/^-?\d+(\.\d+)?$/.test(rawValue)) return Number(rawValue);
if (rawValue.startsWith('"') && rawValue.endsWith('"')) {
  return unescapeWeftString(rawValue.slice(1, -1));
}
if (rawValue.startsWith('[') || rawValue.startsWith('{')) {
  try { return JSON.parse(rawValue); }
  catch (e) { /* emit error, return raw */ }
}
return rawValue;  // bare identifier, kept as-is
```

JSON values can span multiple lines. `isJsonBalanced` + `looksLikeJson`
tracks depth; once balanced, `JSON.parse` runs. Unbalanced JSON
accumulates lines.

### `parseInlinePortList(s)` (line 179-205)

Parses `port1: String, port2: Number?`:
- Split top-level on comma (respecting `[]`, `()`).
- Skip `@require_one_of(...)` (handled separately).
- Match `name: Type` with optional `?` suffix for nullable.
- Name must match `/^[a-zA-Z_][a-zA-Z0-9_]*$/`.
- Returns PortParseResult with port or error.

### Port section (line 228-325, `tryParsePortLine`)

Three syntax forms supported:

1. **Inline single-line**: `in(*port1, *port2: String)`. The `*`
   is accepted for v1 legacy (required prefix) but ports default
   to required anyway.
2. **Multi-line opener**: `in(` starts a section that lasts until
   `)`.
3. **Bare port lines inside a multi-line section**: `name` or
   `name: Type` (with optional `?` suffix).

Validates port names, flags duplicates, parses type string.

### `@require_one_of(a, b)` directive (line 2256-2260)

Inside the input port list:
```ts
const body = item.slice(item.indexOf('(') + 1, -1);
const group = body.split(',').map(s => s.trim()).filter(s => s.length > 0);
if (group.length > 0) parsedOneOfRequired.push(group);
```

Only valid in input port lists; emits error for output side.

### Inline expressions (line 811-1381)

v1 supports inline anonymous nodes as config values:

```
target.port = Template { template: "hi" }.text
my_llm = Llm {
  systemPrompt: Template { template: "{{x}}", x: other.value }.text
}
```

`tryParseInlineExpression` handles:
- Full: `Type ( ... ) -> ( ... ) { ... }.port`
- Config-only: `Type { ... }.port`
- Ports-only: `Type ( ... ) -> ( ... ).port`
- Bare: `Type.port` (default config)

Emits:
- An anon ParsedNode with id `{hostId}__{field}` (merged into
  scope by parent).
- A ParsedConnection from `anonId.port` to `hostId.field`.

**Materialization**: when the user disconnects the binding edge,
`weft-editor.ts::tryMaterializeAnon` rewrites the source so the
anon becomes a standalone declaration. v2 serializer needs this
too.

### Connection line parse (line 1880-2096)

`target.port = source.port` OR `self.port = source.port` OR
`target.port = self.port`. The `self` keyword means "the
enclosing group's boundary". Sets `sourceIsSelf` / `targetIsSelf`
flags, emits a ParsedConnection.

### Self-reference scoping (seen in parseScope line 2421-2431)

When a node with an inline body references outer-scope nodes:
```
group = Group { in: String } {
  child = Node { value: outerNode.result }
}
```

The inline-body parser emits `outerNode.result` as a
ParsedConnection with `sourceId = 'outerNode'`. Later, when
merging into the group scope, `isLocalRef(srcId, localChildIds)`
checks if `outerNode` is a direct child. If not, it's left bare
(outer-scope ref). If yes, the parser prefixes it with the scope:
`scopeId.outerNode`.

### Opaque block accumulation

When a line doesn't fit any recognized pattern AND the parser
can't recover, it accumulates into a pending opaque. Fresh
declarations flush the pending opaque as an OpaqueBlock with
`anchorAfter = lastAnchor`. On round-trip, the serializer
re-inserts each opaque block after its anchor node/group/conn so
the user's original text stays put.

## `validateAndBuild` (line 4074)

Turns `ParseResult` into `ProjectDefinition`:

### Step 1: validate and dedupe nodes (line 4085-4106)

- Unknown `nodeType` → error + opaque block.
- Duplicate node id → error + opaque block.
- `coerceConfigValues(node, typeConfig)` forces each config value
  to the declared field type (string↔number, bool, etc).

### Step 2: validate groups (line 4111-4164)

- Duplicate group names in the same scope: mark ALL duplicates as
  errors and opaque blocks. This is stricter than node dedup;
  nested dupes also go into `nestedOpaqueByParent` for rendering
  inside the parent group.

### Step 3: `buildNodeInstance` per valid node (line 4167-4353)

For each node, produce a NodeInstance:

- Start with catalog's `defaultInputs`/`defaultOutputs`.
- If `hasFormSchema` feature: derive ports from `config.fields`
  via `nodeFormSpecMap`. Errors on port-name collision with
  catalog ports.
- Merge weft-declared ports (`in:`/`out:` block): catalog ports
  get overridden on `required` / `portType` (with subtype
  compatibility check), others get added if `canAddInputPorts` /
  `canAddOutputPorts` is true; else error.
- **Literal-driven port synthesis** (line 4276-4328):
  For each config key that isn't a declared input port, catalog
  field, or reserved metadata key:
  - `null` value → warning "null has no type, use Type? syntax".
  - Otherwise: `inferTypeFromValue(v)` → `portType`.
  - Synthesizes a new input port with `required: false`.
  - Gated on `canAddInputPorts || hasFormSchema`.
  - Error on assigning to an output port.
  - Error on untyped key when node doesn't accept custom ports.
- Position set to `{x: 0, y: 0}` (autoOrganize fills this later).
- `scope: buildScopeChain(parentId)`.

### Step 4: Group NodeInstance synthesis (line 4362-4416)

Each valid group becomes a `nodeType: 'Group'` NodeInstance.
- `id = group.id` (scoped).
- `label = group.originalName || group.id`.
- `config = { expanded: true, parentId: group.parentGroupId?, description: group.description?, _opaqueChildren: nestedOpaqueByParent.get(group.id)? }`.
- `inputs`/`outputs` from interface ports.
- `features: { oneOfRequired: group.oneOfRequired }`.
- Plus: walks `group.nodes` and calls `buildNodeInstance` per
  child with `parentIdOverride = group.id`.
- Pushes group connections into `parsed.connections` for edge
  construction later.

### Step 5: edge-driven port synthesis (line 4418-4449)

For each connection targeting an undeclared port on a
`canAddInputPorts` node, synthesize the port with `required: true`
and a fresh TypeVar `T__{sanitized_id}_{port}`. The TypeVar
unifies with the edge source's type during `resolveAndValidateTypes`.

### Step 6: edge construction + scope validation (line 4451-4555)

For each connection:
- Validate source + target exist.
- Scope validation (line 4471-4496): non-self refs must be
  reachable in the connection's scope. "Reachable" = direct
  member OR ancestor-scope member. Cross-scope refs error with
  `"Node 'X' is not in this scope"`.
- Port existence: source port must be in `sourceNode.outputs`,
  OR `_raw` on non-Group, OR `sourceIsSelf` on a Group (input
  port). Target port must be in `targetNode.inputs`, OR
  `targetIsSelf` on a Group (output port).
- **1-driver-per-input**: check `connectedInputs` set keyed by
  `${target}.${port}${targetIsSelf ? '__inner' : ''}`.
- **`__inner` suffix** (line 4543-4545):
  ```ts
  sourceHandle = conn.sourceIsSelf ? `${sourcePort}__inner` : sourcePort;
  targetHandle = conn.targetIsSelf ? `${targetPort}__inner` : targetPort;
  ```
  Source/target are ALWAYS the group's id. Handle suffix is how
  we distinguish internal vs external side of a boundary port.

### Step 7: type resolution (line 4557-4572)

- `resolveAndValidateTypes(nodes, edges, errors, warnings)` (line 3525):
  resolves TypeVars against connected edges, checks MustOverride
  has a concrete type, validates edge type compatibility.
- `validateConfigFilledPorts(nodes, edges, errors)`: every
  configurable input port with a same-named config value gets its
  value type-checked against the port type.
- `validateRequiredPorts(nodes, edges, errors)`: every required
  port must be wired OR filled from config. Groups excluded
  (their boundary is validated in `expandGroupsForValidation`).

### Step 8: infra subgraph validation (line 4573-4577)

`extractInfraSubgraph(nodes, edges)` walks infrastructure nodes
and checks actionEndpoint connectivity. Non-fatal.

## `expandGroupsForValidation` (line 3285-3403)

Temporary passthrough expansion for uniform type checking. For
each `nodeType: 'Group'` NodeInstance:

```ts
// Create input passthrough: {id}__in
{
  id: `${node.id}__in`,
  nodeType: 'Passthrough',
  inputs: node.inputs,                    // group's external in-ports
  outputs: node.inputs.map(lane-normalize), // post-transform
}
// Create output passthrough: {id}__out
{
  id: `${node.id}__out`,
  nodeType: 'Passthrough',
  inputs: node.outputs.map(lane-pretransform),
  outputs: node.outputs,
}
// Rewrite edges:
// - srcHandle ends with __inner: strip, route to {id}__in
// - tgtHandle ends with __inner: strip, route to {id}__out
// - external edge with src = Group: reroute to {id}__out
// - external edge with tgt = Group: reroute to {id}__in
```

**This is the exact same shape the v2 Rust compiler's
`flatten_group` produces.** v1 uses it for validation then
throws it away; v2 ships it as the runtime model.

## `_raw` synthetic output (line 4502-4503)

Every non-Group node has an implicit `_raw` output, added at
runtime by the executor. Parser accepts edges sourced from `_raw`
without a catalog entry:

```ts
const sourcePort =
  sourceNode.outputs.find(p => p.name === conn.sourcePort)
  || (conn.sourcePort === '_raw' && sourceNode.nodeType !== 'Group' ? { name: '_raw', portType: 'T', required: false } : null)
  || (conn.sourceIsSelf && sourceNode.nodeType === 'Group' ? sourceNode.inputs.find(p => p.name === conn.sourcePort) : null);
```

Groups do NOT have `_raw`. The input-port fallback is for
`sourceIsSelf` (group emitting from its own in-port to a child).

## `autoOrganize` ELK layout (line 4714-5442)

See `layout.md` for the full spec.

## Configuration field spans

`ConfigFieldSpan` (line 1608-1612):
```ts
{
  startLine: number;
  endLine: number;
  origin: 'inline' | 'connection';
}
```

Per-field spans are populated during `parseNodeBlockBody` /
`parseScope` via `setConfigField` (line 372-385). For
connection-origin fields (`node.key = "value"`), origin =
`'connection'`; the span covers the external connection line.
For inline (inside a `{ }` body), origin = `'inline'`.

The serializer uses these to surgically replace one field
without rewriting the node. `origin === 'connection'` tells the
serializer to preserve the `node.key = ` prefix instead of
producing an inline `key: value` line.

**v2 status**: `config_spans` field exists on NodeDefinition but
is not populated by the Rust compiler yet. Until it lands,
`updateConfig` in `surgical.ts` rewrites the whole node.

## Round-trip fidelity

`itemOrder` + `itemGaps` + `rawLines` + `configSpans` +
`opaqueBlocks` together let the serializer reproduce the source
exactly when no edits are applied:
- Item order is preserved per scope.
- Blank-line spacing between items is preserved.
- Unparseable regions are kept verbatim.
- Config field text is kept verbatim unless explicitly edited.

This is the property that makes AI-generated edits safe: the AI
modifies one field, the serializer changes only those bytes,
every other character stays put.

## v2 port plan

### The parser itself

Lives in Rust (`weft-compiler`). v2 doesn't need a TypeScript
parser in the webview.

### What the v2 Rust parser already has

- ParsedGroup/ParsedNode/ParsedConnection equivalents in Rust.
- Group flattening (`flatten_group` ≡ v1's
  `expandGroupsForValidation`), but ship in the runtime model.
- Scope chain + dotted ids + group boundary passthroughs.
- Config values parsed (bool/null/number/string/JSON).
- `_raw` synthetic output.
- TypeVar resolution (last pass).

### What's missing in the Rust parser (queued)

1. **`GroupDefinition` metadata**: span, header_span,
   originalName, rawLines. The `GroupDefinition` struct exists
   (I added it this turn) but these fields are `None`. Needed
   for surgical group rename / move / port updates.
2. **Per-field `config_spans`**. Needed for surgical per-field
   edits.
3. **Inline anonymous node materialization** when
   disconnecting a binding edge (currently no equivalent of
   `tryMaterializeAnon`).
4. **`itemOrder` / `itemGaps` / `rawLines`** for round-trip
   fidelity. Unclear if the Rust parser preserves these today;
   needs audit.
5. **Opaque block support**. Same.
6. **Post-config output ports** (`} -> (outputs)` syntax). Same.

### What the webview consumes

- `project.nodes` (flattened, with passthroughs).
- `project.edges` (with `__inner` handles for self-refs).
- `project.groups` (new; added this turn — carries the
  pre-flatten tree).
- Diagnostics separately via `project.diagnostics`.

The webview reconstructs v1's structured view by:
1. Reading `project.groups` directly.
2. Synthesizing a virtual Group NodeInstance per group.
3. Hiding `__in`/`__out` passthroughs from rendering (they stay
   in data for execution tracking).
4. Rewriting edges that touch passthroughs to connect to the
   group's id with `__inner` handle suffixes as appropriate.

End visible state = v1's public output.
