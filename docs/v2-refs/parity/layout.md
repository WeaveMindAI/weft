# Layout (ELK) Parity

**v1 source**: `dashboard-v1/src/lib/ai/weft-parser.ts` lines
4714-5442 (`autoOrganize`).

## Entry point

```ts
autoOrganize(projectNodes, projectEdges, nodeSizes?, portPositions?) →
  { positions: Map<nodeId, {x,y}>, groupSizes: Map<nodeId, {w,h}> }
```

Called:
- Initial graph load (if `autoOrganizeOnMount` true).
- After expand/collapse toggle.
- After streaming weft generation ends.
- After group capture / release (drag into/out of a group).
- Command palette action "Auto Organize Layout".

## Constants

```ts
NODE_BASE_HEIGHT = 90
PORT_ROW_HEIGHT = 22
NODE_WIDTH = 280
GROUP_PADDING = 40
GROUP_TOP_PADDING = 80
GROUP_SIDE_PADDING = 60
GROUP_BOTTOM_PADDING = 40
COLLAPSED_GROUP_WIDTH = 200
COLLAPSED_GROUP_HEIGHT = 80
```

Port Y positions **must match CSS** for handles to align with
edge endpoints:

```ts
GROUP_PORT_START_Y = 44   // expanded group: top(40) + padding(4)
GROUP_PORT_HEIGHT = 30
GROUP_PORT_GAP = 6
NODE_PORT_START_Y = 58    // accent(2) + header(32) + content-padding(16) + label(8)
NODE_PORT_HEIGHT = 25
NODE_PORT_GAP = 4
```

Annotation sizing (pre-computed):
```ts
ANNOTATION_TARGET_W = 280
ANNOTATION_MIN_W / MAX_W = 200 / 420
ANNOTATION_MIN_H / MAX_H = 80 / 320
ANNOTATION_CHAR_WIDTH = 7.5
ANNOTATION_LINE_HEIGHT = 20
ANNOTATION_PADDING = 24
```

If existing DOM measurement is available, use it. Else compute
from content length (wraps at 280 * 0.857 ≈ 240px effective).

## ELK options

```ts
{
  'elk.algorithm': 'layered',
  'elk.direction': 'RIGHT',
  'elk.layered.spacing.nodeNodeBetweenLayers': '50',
  'elk.spacing.nodeNode': '25',
  'elk.layered.spacing.edgeNodeBetweenLayers': '15',
  'elk.layered.nodePlacement.strategy': 'NETWORK_SIMPLEX',
  'elk.layered.crossingMinimization.strategy': 'LAYER_SWEEP',
  'elk.layered.crossingMinimization.greedySwitch.type': 'TWO_SIDED',
  'elk.layered.crossingMinimization.thoroughness': '100',
  'elk.layered.considerModelOrder.strategy': 'NODES_AND_EDGES',
  'elk.layered.considerModelOrder.crossingCounterNodeInfluence': '0.5',
  'elk.layered.considerModelOrder.crossingCounterPortInfluence': '0.5',
  'elk.layered.crossingMinimization.forceNodeModelOrder': 'true',
  'elk.layered.nodePromotion.strategy': 'DUMMYNODE_PERCENTAGE',
  'elk.separateConnectedComponents': 'true',
}
```

Why model order matters: children are fed into ELK in `sourceLine`
order so the layout stays left-to-right matching the weft source.
`considerModelOrder` + `forceNodeModelOrder` make ELK treat source
order as a strong tie-breaker during crossing minimization.

## Bottom-up scope layout

```
1. Compute depth of each group (deepest first for bottom-up).
2. For each depth 0..maxDepth:
   for each group at this depth:
     - Split children into connected components (findConnectedComponents).
     - Layout each component independently within the group scope
       (layoutScope with SEPARATE_CHILDREN wrapper).
     - Arrange disconnected components side by side
       (arrangeDisconnectedComponents).
3. Layout root scope last (top-level nodes, with groups' final sizes baked in).
4. Arrange root disconnected components.
```

### `SEPARATE_CHILDREN` wrapper

Group scopes wrap the group as a child of a dummy root graph with
`elk.hierarchyHandling: SEPARATE_CHILDREN`. This lets ELK handle
the group's own ports natively (ports on boundary are respected)
while laying out the children. Code at line 5266-5303.

### Connected components

Two nodes in the same scope are "connected" if:
- An edge links them, OR
- They share a group-boundary port (both wire into or out of the
  same group in-port / out-port).

Port-sharing adjacency (line 4996-5020) makes sibling nodes that
wire into the same interface port stay in the same component.

Component sort: score 0 if connected to group's input ports
(→ left), score 2 if connected only to output ports (→ right),
score 1 otherwise. Within same score: weft source order.

### Port Y fallback

`getPortY(nodeId, handleId, isGroup, portIndex)`:
- If `portPositions` has a measured value: use it.
- Else: compute from constants above.

This lets layout work during streaming (before DOM is rendered).

## Viewport anchoring (expand/collapse)

After `runAutoOrganize`, adjust viewport so the toggled node's
top-right corner in screen space stays constant. Code in
ProjectEditorInner.svelte:682-731. Requires knowing
pre-toggle and post-toggle absolute positions.

Formula:
```ts
oldScreenXY = oldAbsTopRight * oldZoom + oldViewport
newVpX = oldScreenX - newAbsTopRight.x * newZoom
newVpY = oldScreenY - newAbsTopRight.y * newZoom
setViewport({x: newVpX, y: newVpY, zoom: newZoom})
```

## v2 port status

- ELK is already a dependency in the extension (`elkjs`).
- `autoLayout` helper in Graph.svelte exists but runs a flat
  layered layout without scope awareness.

### What to port

- `autoOrganize` bottom-up per-scope algorithm.
- Constants matching v2's CSS (some may differ; audit once
  components ship).
- `getPortY` with the measured-position fallback.
- `SEPARATE_CHILDREN` wrapper for group scopes.
- Connected component finder + arrangement.
- Disconnected component arrangement with port-role scoring.
- Viewport anchoring (partially ported in my ProjectNode.svelte,
  needs to follow toggle → layout → re-read → adjust flow).

### Deferred

- Animation during layout transitions. v1 doesn't animate; we
  won't either.
- Incremental layout (only reposition affected subtree on small
  edits). Premature.
