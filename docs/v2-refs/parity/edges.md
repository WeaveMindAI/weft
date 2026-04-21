# Edges Parity

**v1 sources**:
- `dashboard-v1/src/lib/components/project/CustomEdge.svelte` (37 lines)
- Edge construction + styling: `ProjectEditorInner.svelte:971-1011` (buildEdges), `397-407` (getEdgeColor), `1977-2031` (onBeforeConnect), `1896-1928` (onReconnect), `1865-1870` (isValidConnection), `1817-1848` (wouldCreateCycle), `2244-2257` (delete)

## CustomEdge component (37 lines)

```svelte
<script>
  import { BaseEdge, EdgeReconnectAnchor, getBezierPath, type EdgeProps } from '@xyflow/svelte';

  let {
    id,
    sourceX, sourceY, targetX, targetY,
    sourcePosition, targetPosition,
    style, markerEnd,
    targetHandleId,
  }: EdgeProps = $props();

  let reconnecting = $state(false);
  let edgePath = $derived(
    getBezierPath({ sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition })[0]
  );
</script>

{#if !reconnecting}
  <BaseEdge {id} path={edgePath} {style} />
{/if}

<EdgeReconnectAnchor
  bind:reconnecting
  type="target"
  position={{ x: targetX, y: targetY }}
  size={20}
/>
```

That's the whole component. Key points:

- **Smooth bezier path** via `getBezierPath(...)[0]`.
- **No edge label**. v1 does not render labels. Handle-id
  information lives in the handle visuals on the endpoints.
- **No arrowhead drawn by the path itself**. `markerEnd` is
  applied by `BaseEdge` and comes from `buildEdges` (not from
  CustomEdge's logic).
- **Hide while reconnecting**: when the user drags the edge's
  target endpoint, `reconnecting` becomes true and the path
  stops rendering. `EdgeReconnectAnchor` is what triggers this;
  it binds both ways.
- **20px grab zone at target end**: `size={20}` on the anchor
  gives a generous drop-target overlapping the handle.

Only the TARGET end is draggable. v1 doesn't support dragging
the source end (by design: re-sourcing means creating a different
edge; clearer to delete + create anew).

## Edge styling pipeline (not in CustomEdge)

Every visual attribute on an edge is set by `buildEdges` in
ProjectEditorInner. CustomEdge is a dumb renderer.

### `getEdgeColor(source, sourceHandle)` (line 397-407)

```ts
const sourceNode = nodes.find(n => n.id === source);
if (!sourceNode) return PORT_TYPE_COLORS.Any;
const outputs = sourceNode.data.outputs;
if (!outputs) return PORT_TYPE_COLORS.Any;

// Strip __inner suffix before looking up the port
const cleanHandle = sourceHandle?.endsWith('__inner')
  ? sourceHandle.slice(0, -7)
  : sourceHandle;
const port = outputs.find(p => p.name === cleanHandle);
return port ? (PORT_TYPE_COLORS[port.portType] ?? PORT_TYPE_COLORS.Any) : PORT_TYPE_COLORS.Any;
```

Important:
- The `__inner` suffix MUST be stripped before matching against
  port.name. Otherwise boundary edges (those that go from a
  group's internal source to a child's input) would get the
  fallback color.
- `PORT_TYPE_COLORS[port.portType]` does NOT parse compound types.
  `List[String]` falls back to 'Any' unless added to the table.
  In practice v2 uses `getPortTypeColor(portType)` (which parses
  recursively — see `colors.md`) and is stricter. When we port,
  use `getPortTypeColor` for consistency.

### `buildEdges` per-edge assembly (line 971-1011)

```ts
// Dedup by (target, targetHandle) — last edge wins.
const seenTargets = new Map<string, Edge>();
for (const e of projectEdges) {
  const key = `${e.target}:${e.targetHandle || 'default'}`;
  seenTargets.set(key, e);
}
const deduped = [...seenTargets.values()];

return deduped.map(e => {
  const edgeColor = getEdgeColor(e.source, e.sourceHandle);
  const active = activeEdges.has(e.id);

  return {
    id: e.id,
    source: e.source,
    target: e.target,
    sourceHandle: e.sourceHandle,    // passes through, INCLUDING __inner
    targetHandle: e.targetHandle,    // passes through, INCLUDING __inner
    type: 'custom',
    animated: active,
    zIndex: 5,
    style: `stroke-width: ${active ? 3 : 2}px; stroke: ${edgeColor};`,
    markerEnd: {
      type: MarkerType.ArrowClosed,
      width: 20,
      height: 20,
      color: edgeColor,
    },
    className: active ? 'edge-active' : '',
  };
});
```

- `animated: true` gives xyflow's built-in dash-flow animation.
- `className: 'edge-active'` hooks the `:global(.edge-active
  .svelte-flow__edge-path)` rule for stronger stroke.
- `zIndex: 5` is below raised nodes (`nextNodeZ` starts at 6) so
  edges stay behind focused nodes by default.
- `markerEnd` color matches `stroke` color — arrowhead blends in.

## Active edges

Set via `executionState.activeEdges: Set<string>`. Dispatcher
emits events when a pulse is in transit; the effect at line
1016-1111 re-maps edges on every state change:

```ts
edges = edges.map(e => ({
  ...e,
  animated: activeEdges.has(e.id),
  style: activeEdges.has(e.id)
    ? e.style?.replace(/stroke-width: \d+px/, 'stroke-width: 3px')
    : e.style?.replace(/stroke-width: \d+px/, 'stroke-width: 2px'),
  class: activeEdges.has(e.id) ? 'edge-active' : '',
}));
```

**Gotcha**: `class` is set here (not `className`). xyflow respects
both. Redundant with the `className` in buildEdges but the mask
by regex on stroke-width means we're not just recomputing from
scratch; we're patching the existing style string in place.

## Edge click → raise z-index

When a node is clicked, edges connected to it get raised
alongside (line 2281-2285):
```ts
edges = edges.map(e =>
  (e.source === node.id || e.target === node.id)
    ? { ...e, zIndex: nextNodeZ + 1 }
    : e
);
nextNodeZ++;
```

This keeps the reconnect anchor visible above the raised node
when the user wants to drag it.

## `:global(.edge-active)` CSS

Defined in ProjectEditorInner's `<style>` block. Adds a stronger
stroke color to active edges. Stroke width is already bumped via
the inline style; the class adds the color accent.

Exact rule used in v1:
```css
:global(.edge-active .svelte-flow__edge-path) {
  stroke-dasharray: 4 4;
}
```

(There's also a subtle glow; exact CSS is in the `<style>` block
around line 3280+, covered in the main file if needed. For
parity, mirror the dasharray + stronger stroke, exact color can
match the source port type via the inline style we already set.)

## Connection lifecycle

### `onConnectStart(event, params)` (line 1875-1881)

Fires when the user grabs a Handle to start dragging a new edge.
- Sets `currentConnectionColor` based on source port type so the
  live preview line matches.

### `onBeforeConnect(connection)` (line 1977-2031)

Runs BEFORE xyflow adds the edge. Returns `Edge | null` (null
rejects the connection).

1. Clear `pendingConnection` (from a prior `onConnectEnd`).
2. If `structuralLock`, reject.
3. `wouldCreateCycle(source, target)` → alert `"Cannot create
   this connection - it would create a cycle (infinite loop)"`,
   return null.
4. **1-driver-per-input**: filter edges to remove any with
   `(target, targetHandle)` matching the incoming connection.
   This is the visual-side enforcement; the serializer separately
   enforces it via `weftAddEdge` removing the old line.
5. Build the new Edge object (color, marker, zIndex 5).
6. Schedule `setTimeout(() => weftAddEdge + saveToHistory +
   saveProject, 0)` — weft code sync happens AFTER xyflow
   commits the edge.
7. Return newEdge.

### `onReconnect(oldEdge, newConnection)` (line 1896-1928)

Atomic replace when the user drags an edge endpoint:
```ts
reconnectSuccessful = true;  // prevents delete-on-drop

// 1. Remove old edge in weft source
const oldRef = toWeftEdgeRef(oldEdge.source, oldEdge.sourceHandle || 'value',
                             oldEdge.target, oldEdge.targetHandle || 'value');
weftCode = weftRemoveEdge(weftCode, oldRef.srcRef, oldRef.srcPort,
                          oldRef.tgtRef, oldRef.tgtPort);

// 2. Add new edge
const newRef = toWeftEdgeRef(newConnection.source, newConnection.sourceHandle || 'value',
                             newConnection.target, newConnection.targetHandle || 'value');
weftCode = weftAddEdge(weftCode, newRef.srcRef, newRef.srcPort,
                       newRef.tgtRef, newRef.tgtPort, newRef.scopeGroupLabel);

// 3. Update visual edge in place
edges = edges.map(e =>
  e.id === oldEdge.id
    ? { ...e, source: newConn.source, sourceHandle, target, targetHandle }
    : e
);
```

`toWeftEdgeRef` (line 174-218) translates xyflow's ids + handles
into weft's local references, handling:
- `__inner` handles → `self.port` syntax
- Scoped ids → local names with the appropriate scope group
- Top-level connections

### `onReconnectStart(event, edge)` (line 1887-1893)

Sets `currentConnectionColor` from the old edge's source port
(so the live drag line matches the color being reconnected).

### `onConnectEnd(event, connectionState)` (line 1939-1975)

Fires when the drag ends, regardless of success. If
`!connectionState.isValid` (dropped on empty pane):
```ts
pendingConnection = {
  sourceNodeId: connectionState.fromNode.id,
  sourceHandle: connectionState.fromHandle?.id || null,
};
contextMenu = {
  x: clientX, y: clientY, flowX, flowY,
  nodeId: null,  // "add node" mode, not "edit"
};
```

Picking a node type from the context-menu-opened palette adds
the new node at `contextMenuFlowPos` AND wires the pending
connection to it. v1's "drop-edge-on-empty-pane ⇒ add a node"
UX pattern (from React Flow's docs).

## `isValidConnection(connection)` (line 1865-1870)

```ts
function getHandleScope(nodeId, handleId):
  const node = nodes.find(n => n.id === nodeId);
  const isGroup = node.type === 'group' || node.type === 'groupCollapsed';
  if (isGroup && handleId?.endsWith('__inner')) {
    return nodeId;  // inner handle: scope is inside this group
  }
  return node.parentId || '__root__';

return getHandleScope(src, srcHandle) === getHandleScope(tgt, tgtHandle);
```

Rejects connections that cross scope boundaries improperly:
- Regular node at root → regular node inside a group: disallowed
  (unless through a group boundary handle).
- `__inner` source → `__inner` target on different groups:
  disallowed.
- `__inner` source on Group A → child of Group A: allowed (same
  scope).

## `wouldCreateCycle(source, target)` (line 1817-1848)

DFS over a graph built from edges:
```ts
const adjacency = new Map<string, string[]>();
for (const edge of edges) {
  // CRITICAL: skip edges with __inner handles on either end.
  // They represent data flowing through a group boundary, not
  // real dependency edges. Without this skip, any edge into a
  // group creates a "cycle" through the group's internal wiring.
  if (edge.sourceHandle?.endsWith('__inner') || edge.targetHandle?.endsWith('__inner')) continue;
  adjacency.get(edge.source)?.push(edge.target);
}
// Add the candidate edge source → target, then DFS for back-edges.
```

## Delete (line 2244-2257)

```ts
const selectedEdges = edges.filter(e => e.selected);
if (selectedEdges.length > 0) {
  for (const e of selectedEdges) {
    const ref = toWeftEdgeRef(e.source, e.sourceHandle || 'value',
                              e.target, e.targetHandle || 'value');
    weftCode = weftRemoveEdge(weftCode, ref.srcRef, ref.srcPort,
                              ref.tgtRef, ref.tgtPort);
  }
  edges = edges.filter(e => !e.selected);
  saveToHistory();
  saveProject();
  return;
}
```

Edge delete happens BEFORE node delete in the keyboard handler
so that pressing Delete with an edge selected doesn't also kill
its endpoints.

## v2 port plan

### CustomEdge component

Minimal. Already have bezier, add `EdgeReconnectAnchor` + hide
while reconnecting. No label (my earlier port had labels —
remove).

### Edge construction (in Graph.svelte's `buildEdges` analog)

- Dedup by (target, targetHandle).
- Apply color from `getPortTypeColor(port.portType)` (v2 uses
  full recursive color resolution).
- markerEnd with matching color.
- className `edge-active` when the dispatcher reports the edge
  as active.
- zIndex 5 baseline, raise to nextNodeZ+1 on endpoint click.
- Handles pass through including `__inner`.

### Handlers

- `onBeforeConnect`: cycle check (skip `__inner`), 1:1 enforcement,
  color, schedule surgical addEdge.
- `onReconnect`: atomic removeEdge + addEdge + in-place visual
  swap.
- `onConnectEnd`: if invalid, open command palette at drop point
  with `pendingConnection`, addNode wires it.
- `isValidConnection`: scope match via getHandleScope.
- Delete: surgical removeEdge per selected edge.

### `:global(.edge-active)`

Add the dasharray + stroke color rule in `app.css`.

### Dispatcher side

- Emit `edge-active` / `edge-inactive` SSE events when pulses
  enter / leave an edge. Or simpler: map `NodeStarted(X)` →
  "all edges into X are active for 200ms", `NodeCompleted(X)` →
  "all edges out of X are active for 200ms". v1 has exact
  tracking; v2 phase-A can approximate.

### Divergences

None. Match v1 exactly for visual behavior. The backend side of
active-edge tracking is phase-A-approximate; upgrade later.
