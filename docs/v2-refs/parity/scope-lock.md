# Scope-Lock on Drag Parity

**v1 source**: `ProjectEditorInner.svelte:2385-2629`.

## The rule

A node cannot change scope (enter or leave a group) while it has
any edge connecting it to another node in its current scope. The
drag reverts with a debounced toast:

> "Cannot change scope. Disconnect this node from other nodes in
> its current scope first."

This is lexical scoping for dataflow: the group is a visual
analog of a lexical block. Dangling references are disallowed,
same as a closure that can't be moved past the vars it captures.

## State the editor maintains

- `preDragPositions: Map<string, {x, y}>` captured at
  `onNodeDragStart` for every node being dragged (line 1224,
  2316-2318). Used for snap-back.
- `lastScopeBlockToastTime: number` 3s debounce on the toast
  (line 2385-2394).

## Three drag-stop paths

### 1. `checkNodeLeavesGroup(node)` (lines 2410-2451)

Called first on nodedragstop, only if node currently has
`parentId`. Checks whether the node was dragged outside its
parent group's rect.

```ts
const stillInGroup =
  node.position.x >= 0 &&
  node.position.x <= groupWidth &&
  node.position.y >= 0 &&
  node.position.y <= groupHeight;
```

`node.position` is relative to parent when `parentId` is set (xyflow
convention).

If not still in group:
- If `nodeHasConnectionsInScope(node.id, node.parentId)` → revert
  to `preDragPositions`, show toast.
- Else: convert position to absolute (via `getAbsolutePosition`
  walking parentId chain), clear parentId, update config, call
  `weftMoveScopeAny(node, undefined)` to rewrite the Weft source.

### 2. `checkNodeCapturedByGroup(node)` (lines 2480-2547)

Called after `checkNodeLeavesGroup`. Checks whether the node was
dragged INTO an expanded group's rect. Picks the deepest-nested +
smallest group if multiple candidates match (so inner groups win
over outer when hovering both). Line 2506-2514.

Exclusions:
- Same node
- Descendant of this node (can't capture your own ancestor)
- Collapsed groups
- Hidden groups (`display: none`)

If a candidate is found AND it's not already the current parent:
- If `nodeHasConnectionsInScope(node.id, node.parentId)` → snap
  back + toast.
- Else: convert absolute position to group-relative, set parentId,
  update config, `weftMoveScopeAny(node, groupLabel, groupId)`,
  then `ensureParentBeforeChild()`.

### 3. `checkGroupCapturesNodes(group)` (lines 2580-2629)

Called when the dragged thing IS a group (not collapsed). Scans
all non-grouped, non-nested nodes whose absolute position falls
inside the group's rect. For each candidate:
- If it has in-scope connections: blocked (toast at end of loop,
  but capture of others may still succeed, line 2587 `blocked`
  flag).
- Else: captured, reparent, rewrite Weft.

## The shared test: `nodeHasConnectionsInScope(nodeId, scopeParentId)` (lines 2396-2408)

```ts
const sameScope = new Set(
  nodes
    .filter(n => n.id !== nodeId && n.parentId === scopeParentId)
    .map(n => n.id)
);
if (scopeParentId) sameScope.add(scopeParentId);  // include the group itself
for (const edge of edges) {
  if (edge.source === nodeId && sameScope.has(edge.target)) return true;
  if (edge.target === nodeId && sameScope.has(edge.source)) return true;
}
return false;
```

**Important**: the group itself counts as being "in the same scope"
as its children. If node A is inside group G and has an edge
`G.foo__inner → A.input`, that edge locks A inside G. You can't
drag A out until you delete that edge.

## `ensureParentBeforeChild()` (lines 2549-2578)

xyflow requires parent nodes to appear in the `nodes` array before
their children. After a reparent, topo-sort if needed. Recursive
walk that places parent first.

## `getAbsolutePosition(n)` (lines 2453-2459)

Recursive: `absolute = parent.absolute + n.position`. Stops when
`parentId` is undefined.

## `getGroupDimensions(group)` (used but not in the above snippet)

Reads `width` / `height` from style string, falling back to
`measured.width/height` from xyflow. Same shape as `getNodeRect`
(line 510-518).

## Drag-stop orchestration (lines 2327-2353)

```ts
function onNodeDragStop({ targetNode, nodes: draggedNodes }) {
  if (!targetNode) return;
  let currentNode = nodes.find(n => n.id === targetNode.id);
  if (currentNode) {
    if (currentNode.parentId) {
      checkNodeLeavesGroup(currentNode);
      currentNode = nodes.find(n => n.id === targetNode.id);
    }
    if (currentNode) {
      checkNodeCapturedByGroup(currentNode);
      currentNode = nodes.find(n => n.id === targetNode.id);
    }
    if (currentNode?.type === 'group' || currentNode?.type === 'groupCollapsed') {
      checkGroupCapturesNodes(currentNode);
    }
  }
  for (const dn of draggedNodes) {
    const n = nodes.find(nd => nd.id === dn.id);
    if (!n) continue;
    layoutUpdateAny(n);
  }
  saveToHistory();
  saveProject();
}
```

Pattern: run check, then re-read currentNode (in case the check
mutated it). Each check mutates the nodes array if it takes effect.

## `onSelectionDragStop` (lines 2355-2383)

Runs the same three checks per selected node. Iterates the whole
selection; each node's reparenting is independent.

## v2 port plan

**Direct port.** All three functions translate to the VS Code
webview almost unchanged because they operate on xyflow state
(positions + parentId + edges) and don't depend on v1's parser
model. I need:

- `preDragPositions` state in Graph.svelte.
- `onNodeDragStart` handler that captures positions for every
  selected or dragged node.
- `onNodeDragStop` that runs the three-check pipeline.
- `onSelectionDragStop` same pattern.
- `nodeHasConnectionsInScope` reads `edges` + `nodes`.
- `weftMoveScopeAny` becomes a mutation sent to the host: the
  surgical layer rewrites the .weft source. Need to add
  `moveNodeScope` and `moveGroupScope` mutations to the shared
  protocol and implement them in `surgical.ts`.
- Toast: VS Code webview doesn't have svelte-sonner; either add it
  as a small inline warning banner in Graph.svelte, or send the
  message to the host via `send({ kind: 'log', level: 'warn', ... })`
  and let the host show `vscode.window.showWarningMessage`. I
  lean webview-native toast because it's faster feedback (no RPC
  roundtrip).

## Edge cases

- **Dragging a node that's partially inside a group.** v1 uses
  the node's absolute position ONLY; does not check bounding box
  overlap. The whole-node rect is checked only in
  `checkGroupCapturesNodes`, where the dragged thing is the group.
  I'll keep that asymmetry.

- **Hidden descendants during drag.** A collapsed group's
  descendants have `parentId: undefined` and `display: none`
  (expand/collapse walk). They can't be dragged because they're
  invisible.

- **Group containing a group.** If Outer contains Inner, and Inner
  contains A, then A.parentId = Inner.id. Dragging A out of Inner
  into Outer: `checkNodeLeavesGroup` runs with node.parentId =
  Inner.id, check the A.position vs Inner's dims. Then
  `checkNodeCapturedByGroup` picks Outer (deepest group containing
  the absolute-position point where A ended up, after Inner was
  excluded because of the "descendant" check... actually no, A is
  a descendant of Inner not Outer, so Outer is fair game). The
  rule holds.

- **Selection drag blocks atomically per-node.** v1's
  `onSelectionDragStop` runs per node, each independently. A
  dragged pair where only one has in-scope edges: the free one
  moves, the locked one snaps back. I'll preserve this.
