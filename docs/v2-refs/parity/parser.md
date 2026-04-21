# Parser Parity

**v1 source**: `dashboard-v1/src/lib/ai/weft-parser.ts` (5461 lines).
**v2 source**: `crates/weft-compiler/src/weft_compiler.rs`. We use the
Rust parser via the dispatcher's `/parse` endpoint. This doc
captures the v1 parser's behavior so we know what shape of output
the v2 webview has to consume.

## Three-stage pipeline

```
parseWeft(raw) →
  extractAllWeftBlocks(raw)            // split on ````weft fences
  for each block:
    parseRawWeft(block) → ParseResult  // lexical parse
    validateAndBuild(result)           // build NodeInstances + Edges, type-check
  autoOrganize(project.nodes, edges)   // ELK layout (separate)
```

## ParseResult (line 2988-2998)

```ts
{
  name: string;
  description: string;
  nodes: ParsedNode[];
  connections: ParsedConnection[];
  groups: ParsedGroup[];
  opaqueBlocks: OpaqueBlock[];
  nodeOrder: string[];
  itemOrder: string[];
  itemGaps: number[];
}
```

`itemOrder` tracks the declaration order as the user wrote it;
used by the serializer to reproduce order on edits. `itemGaps` is
the count of blank lines between items. `opaqueBlocks` are
un-parseable regions that the editor keeps verbatim so save → load
round-trips without losing user text.

## ParsedGroup (line 1656-1673)

```ts
{
  id: string;               // scoped: "Outer.Inner" for nested
  originalName?: string;    // local name; label for nested groups
  description?: string;     // from leading # comment block
  inPorts: ParsedInterfacePort[];
  outPorts: ParsedInterfacePort[];
  oneOfRequired: string[][];// @require_one_of directives
  nodes: ParsedNode[];      // direct children (not grand-descendants)
  connections: ParsedConnection[];
  startLine: number;
  endLine: number;
  parentGroupId?: string;
  rawLines: string[];       // source text, verbatim, for rename ops
}
```

Groups are first-class. Child nodes inside a group have
`parentId: scopeId` set during `flushNode` at line 1807.

## ParsedConnection (line 1637-1647)

```ts
{
  sourceId: string;
  sourcePort: string;
  targetId: string;
  targetPort: string;
  line: number;
  rawText: string;
  sourceIsSelf?: boolean;   // `source` is the group itself, port is boundary
  targetIsSelf?: boolean;
  scopeId?: string;         // group scope this connection lives in
}
```

`self.x = child.y` inside a group produces `sourceIsSelf: true,
sourceId: scopeId` (see the `parseScope` line 2421-2431 rescoping
block). This propagates into edge construction where `__inner`
gets appended.

## validateAndBuild: the crucial build step (line 4074-4619)

1. Validates node types against `NODE_TYPE_CONFIG`.
2. Detects duplicate group names in same scope; marks them as opaque.
3. For each valid node, calls `buildNodeInstance`: merges catalog
   defaults with weft-declared in/out blocks, runs literal-driven
   port synthesis (untyped config keys become input ports on
   `canAddInputPorts` nodes), sets scope chain.
4. For each valid group, synthesizes a `nodeType: 'Group'`
   NodeInstance with its in/out ports (line 4362-4398).
5. Runs edge-driven port synthesis: an edge whose target is an
   undeclared port on a `canAddInputPorts` node synthesizes the
   port with a fresh TypeVar.
6. Builds edges, applying `__inner` suffix for self-references
   (line 4543-4554).
7. Runs `resolveAndValidateTypes`, `validateConfigFilledPorts`,
   `validateRequiredPorts` for type-level and signature-level
   errors.

## The `__inner` handle suffix (line 4543-4554)

```ts
const sourceHandle = conn.sourceIsSelf ? `${conn.sourcePort}__inner` : conn.sourcePort;
const targetHandle = conn.targetIsSelf ? `${conn.targetPort}__inner` : conn.targetPort;
edges.push({
  id: `e-${conn.sourceId}-${conn.sourcePort}-${conn.targetId}-${conn.targetPort}`,
  source: conn.sourceId,
  target: conn.targetId,
  sourceHandle,
  targetHandle,
});
```

Source/target are ALWAYS the group's id (not a synthesized
passthrough id). The handle suffix disambiguates:
- `{portName}` = external side of the boundary port (outside the
  group connects here)
- `{portName}__inner` = internal side (children inside the group
  connect here)

GroupNode.svelte renders TWO Handle elements per port with these
ids. Edges connect to whichever the handle id matches.

## `_raw` synthetic output (line 4502-4503)

Every non-Group node has an implicit `_raw` output port, added at
runtime by the executor. Carries the full output record. Parser
accepts edges sourced from `_raw` on non-Group nodes without
catalog declaration:

```ts
const sourcePort = sourceNode.outputs.find(p => p.name === conn.sourcePort)
  || (conn.sourcePort === '_raw' && sourceNode.nodeType !== 'Group' ? { name: '_raw', portType: 'T', required: false } : null)
  || (conn.sourceIsSelf && sourceNode.nodeType === 'Group' ? sourceNode.inputs.find(p => p.name === conn.sourcePort) : null);
```

ProjectNode.svelte renders `_raw` as a separate square handle in
the top-right corner.

## expandGroupsForValidation (line 3285-3403)

This is the exact same transform our v2 Rust compiler performs in
`flatten_group`. For each Group NodeInstance:
- Emit `{id}__in` Passthrough with inputs = group.inputs, outputs
  = group.inputs post-type-transform.
- Emit `{id}__out` Passthrough with inputs = group.outputs
  pre-transform, outputs = group.outputs.
- Rewrite edges: `__inner` handles route to passthrough ports.
  External edges into/out of the group route through `__in`/`__out`.

**v1 USES this for validation only and throws it away.** The
returned flat shape never reaches the UI. Groups stay as
`nodeType: 'Group'` NodeInstances in the public `project.nodes`.

## How v2 differs and what we compensate for

**v2 compiler exposes the flattened shape as the public /parse
output.** Boundary Passthroughs with `groupBoundary: {groupId,
role}` are present in `project.nodes`. Children have
`scope: [groupId, ...]` and `groupBoundary: null`.

The v2 webview reverses this at render time:
1. Read `project.groups: GroupDefinition[]` (new field we added
   to the compiler; carries the pre-flatten tree).
2. For each group: synthesize a virtual `weftGroup` NodeInstance
   with inputs = group.inPorts, outputs = group.outPorts.
3. Hide `{id}__in` and `{id}__out` Passthrough nodes from
   rendering (they stay in the data for execution tracking — see
   `execution.md`).
4. Rewrite edges:
   - `source = "{gid}__in"` → `source = "{gid}"`, `sourceHandle
     += "__inner"` (internal side of the group's in-port).
   - `target = "{gid}__out"` → `target = "{gid}"`, `targetHandle
     += "__inner"`.
   - `source = external, target = "{gid}__in"` → `target =
     "{gid}"`, handle stays bare (external in-port).
   - `source = "{gid}__out", target = external` → `source =
     "{gid}"`, handle stays bare (external out-port).
5. Children of the group get `parentId: gid` in xyflow. Scope
   chain builds from `node.scope`.

**The visible end state is identical to v1's public output.**
That's the point.

## Configuration spans (line 1608-1612, 1634)

```ts
interface ConfigFieldSpan {
  startLine: number;
  endLine: number;
  origin: 'inline' | 'connection';
}
```

Each config field's source span is tracked so the editor knows
where to patch when a field value changes. `origin = 'connection'`
means the value was set via a `.x = lit` line rather than inside
a `{ ... }` block. Surgical edits need these spans to avoid
rewriting the whole node.

Our v2 compiler carries `header_span` and `config_spans` on
`NodeDefinition`, but the granularity isn't fully populated yet.
See `surgical.ts` TODO comment: `updateConfig` still does a
coarse whole-node re-render. Adding per-field span tracking in
the compiler is a separate item.

## Parser → serializer round-trip contract

- `rawLines` on every ParsedNode / ParsedGroup holds the exact
  source text. Used by opaque-block preservation and rename
  operations.
- `itemOrder` and `itemGaps` preserve blank-line separations
  between declarations so the serialized output matches the
  user's style.
- `configSpans` is the indirection the editor uses for any edit
  that changes a single field value: it locates the byte range
  and replaces only that, never rebuilds the node.

V2's dispatcher owns serialization via `surgical.ts`. The v2 spec
for the serializer is in `serializer.md` (pending).

## v2 port status

Parser itself is in Rust and doesn't need porting. The webview
consumes `/parse` output via the protocol already defined in
`extension-vscode/src/shared/protocol.ts`. The only webview-side
parser-adjacent work is:
- `GroupDefinition` mirror (done).
- Group virtual-node synthesis in Graph.svelte (pending).
- Edge rewriting for boundary passthroughs (pending).
