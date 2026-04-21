# Edges Parity

**v1 source**: `dashboard-v1/src/lib/components/project/CustomEdge.svelte`
(37 lines).

## What CustomEdge does

1. Renders a bezier path via `getBezierPath({ sourceX, sourceY,
   targetX, targetY, sourcePosition, targetPosition })[0]`.
2. Hides the path while the user is reconnecting it
   (`reconnecting` state, bound from EdgeReconnectAnchor).
3. Places a 20px `EdgeReconnectAnchor` at the target end for a
   generous grab zone.

No labels. No arrowheads on the path itself. The arrowhead is
driven by `markerEnd`, set in `buildEdges` on the edge's prop:
```ts
markerEnd: { type: MarkerType.ArrowClosed, width: 20, height: 20, color: edgeColor }
```

## Where the visual styling comes from

Not from CustomEdge. From `buildEdges` in ProjectEditorInner
(line 981-1009):
- `style`: `"stroke-width: ${active ? 3 : 2}px; stroke: ${edgeColor};"`.
- `className`: `"edge-active"` when `activeEdges.has(e.id)`.
- `animated: active`.
- `zIndex: 5`, raised to `nextNodeZ + 1` when either endpoint is
  clicked (line 2281-2285).

`edgeColor` is the source port's type color (via
`getEdgeColor(edge.source, edge.sourceHandle)`, strips `__inner`
suffix first, reads `sourceNode.data.outputs[...].portType`, looks
up in `PORT_TYPE_COLORS`, fallback to 'Any').

## `.edge-active` CSS rule

Defined in ProjectEditorInner :global block:
```css
:global(.edge-active .svelte-flow__edge-path) {
  /* stronger stroke; exact color depends on theme */
}
```

Combined with `animated: true`, xyflow adds its built-in dashed
animation.

## Edge handles that carry `__inner`

The source/target handles passed through verbatim. When xyflow
renders, it finds the Handle with matching id inside the source/
target node. A group with `Handle id="in__inner"` receives edges
whose `sourceHandle === "in__inner"`. The GroupNode renders both
the bare-named handle and the __inner-suffixed handle so both
sides of a boundary port are wire-able (see `group-node.md`).

## Edge dedup

In `buildEdges` (line 972-978): dedup by `(target, targetHandle)`
keeping the last one. Enforces the "one driver per input" rule
visually even if the source code has duplicates.

## v2 port plan

### Direct port
- `CustomEdge.svelte` bezier path + EdgeReconnectAnchor.
- Graph.svelte `buildEdges` logic applies styling (color from port
  type, stroke width 2/3, animated + class when active).

### What my earlier port got right
- Bezier via `getBezierPath`.

### What my earlier port got wrong / is missing
- I put an edge label on every edge showing `src → tgt`. v1 does
  NOT render labels. Remove.
- No reconnect anchor at the target end. Add.
- No edgeColor from port type. Apply in Graph.svelte, not here.
- No arrowhead via markerEnd. Add.

### Divergences
None. Match v1 exactly.
