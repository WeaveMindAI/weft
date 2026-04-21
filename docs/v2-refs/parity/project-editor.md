# Project Editor Orchestrator Parity

**v1 source**: `dashboard-v1/src/lib/components/project/ProjectEditorInner.svelte`
(3356 lines). This file is the root of the graph view: node+edge
composition, weft code sync, viewport, selection, keyboard, undo
stack, command palette wiring, context menus, execution overlay,
validation errors, code panel resize, publishing / test dialogs.

For the v2 VS Code extension we split its job across the extension
host (project management, SSE bus, code panel, save) and the
webview's `Graph.svelte`. This doc focuses on the behaviors we
must preserve regardless of where they land.

## Node/edge composition pipeline

Three stages, all reactive:

```
weftCode (string) ──parseWeftCode──> ParseResult ──buildNodes──> Node[]
                                                 └─buildEdges──> Edge[]
```

Plus `layoutCode` (a separate `@layout` directive block) that
positions nodes without touching weft source.

### `buildNodes(projectNodes, projectEdges, layoutMap)` (line 835-966)

1. Filters to valid types: `SPECIAL_NODE_TYPES.has(n.nodeType) ||
   NODE_TYPE_CONFIG[n.nodeType]`. `SPECIAL_NODE_TYPES = ['Group',
   'Annotation']` (line 833).
2. **Topo-sort**: groups first, with `visitGroup` placing
   ancestors before descendants. Non-groups after. xyflow requires
   this order (line 842-859).
3. For each node, compute render state:
   - `rawParentId = n.config.parentId`.
   - Walk full ancestor chain; if any ancestor is collapsed, set
     `hiddenByCollapsedGroup = true` and hide via
     `style: 'display: none;'` (line 960-962).
   - `parentId` (xyflow) = `rawParentId` only if parent is
     expanded AND node isn't hidden.
   - `nestingDepth` counts ancestor groups (for z-index).
   - **z-index formula** (line 903):
     ```
     zIndex = isAnnotation ? -1
            : isGroup && isExpanded ? -1 + nestingDepth
            : isGroup ? 4               // collapsed groups
            : 4;                         // regular nodes
     ```
   - **xyflow type**: `isGroup ? (isExpanded ? 'group' :
     'groupCollapsed') : isAnnotation ? 'annotation' : 'project'`.
   - **style** computed from config.width/config.height or sane
     defaults per type.
4. Each node's `data` carries: `label, nodeType, config, inputs,
   outputs, features, sourceLine, onUpdate,
   infraNodeStatus`. `onUpdate` is a per-node handler created via
   `createNodeUpdateHandler(n.id)`.

### `buildEdges(projectEdges, projectNodes)` (line 971-1011)

1. **Dedupe by `(target, targetHandle)`**. Last edge wins. This
   enforces "one driver per input port" at render time even if
   the source code somehow has duplicates.
2. For each deduped edge:
   - `edgeColor = getEdgeColor(source, sourceHandle)` strips any
     `__inner` suffix, looks up the source port's portType, maps
     to PORT_TYPE_COLORS. Returns fallback 'Any' if unknown.
   - `active = activeEdges.has(e.id)`.
   - `style = "stroke-width: ${active ? 3 : 2}px; stroke:
     ${edgeColor};"`.
   - `markerEnd = { type: MarkerType.ArrowClosed, width: 20,
     height: 20, color: edgeColor }`.
   - `className = active ? 'edge-active' : ''`.
   - `zIndex = 5` (below raised nodes, above bg).
   - Source/target handles pass through verbatim (INCLUDING
     `__inner`).

### Execution state → node/edge decoration (line 1016-1111)

Re-runs every time `executionState` changes. Walks `nodes` and
`edges` to attach:
- `data.debugData = nodeOutputs[id]` if `features.showDebugPreview`
- `data.executions = nodeExecutions[id]` (or synthesized group
  execution, see `execution.md`)
- `data.executionCount`
- `class = 'node-running' | 'node-completed' | 'node-failed' | ''`
  depending on latest exec status.
- Edge `animated`, `style`, `className`.

## Two-way sync weft code ↔ graph

### Sources of truth

- **weftCode**: string, the canonical source.
- **layoutCode**: string, `@layout` directives for positions.
- **nodes / edges**: xyflow's in-memory Node/Edge arrays.

### Debounce rules

- `handleWeftCodeChange` (line 331-356): user typing in the code
  panel. Debounce `WEFT_SYNC_DEBOUNCE_MS = 500`. After the pause:
  parse, `patchFromProject`, save.
- `configEditTimer` (line 493): user typing in a config field.
  Debounce 500ms for history, 5000ms for save.
- `saveProjectTimer` (line 280): `SAVE_DEBOUNCE_MS = 1000` for
  most operations.

### The `weftSyncDirection` guard (line 272-275)

Prevents feedback loops. `'to-code' | 'to-editor' | 'none'`.
- When user edits a node in the graph: direction = 'to-code',
  weftCode updates via `weftUpdate*` helpers.
- When user types in the code panel: direction = 'to-editor',
  nodes rebuild from the parse.
- Only one at a time.

## `createNodeUpdateHandler(nodeId)` (line 521-810)

Central mutation pipeline when a node is edited from the graph.
Cases:
- `label` change: `weftUpdateLabel(weftCode, nodeId, label)` or
  `weftRenameGroup(weftCode, oldLabel, newLabel)` if it's a group.
- `config` change: iterate entries, skip
  `['parentId', 'textareaHeights', '_opaqueChildren']`, route
  layout keys through `layoutUpdateAny`, everything else through
  `weftUpdateConfig`.
- `inputs`/`outputs`: `weftUpdatePorts` (node) or
  `weftUpdateGroupPorts` (group).
- `expanded` toggle: visibility recomputation walk across all
  descendants, ELK re-layout, viewport anchoring (see
  `group-node.md`).

**Width/height detection for expand**:
```ts
let oldWidth = 0, oldHeight = 0, oldPosition = {x:0, y:0};
if (isExpandToggle) {
  const current = nodes.find(n => n.id === nodeId);
  const rect = getNodeRect(current);
  oldWidth = rect.width;
  oldHeight = rect.height;
  oldPosition = getAbsolutePosition(current);
}
```

## Keyboard shortcuts (line 2184-2268)

Global `keydown` handler:
- **Ctrl/Cmd+S**: save. Works in all elements including code
  editors. Flushes pending weft debounce first so the editor and
  graph are in sync before the save fires.
- (Skip all below if focus is an input/textarea/contenteditable
  or closest element has role="dialog" or `.edit-textarea` class
  or `.annotation-node.editing`.)
- **Escape**: close context menu, cancel pending connection.
- **Ctrl+Z**: undo.
- **Ctrl+Shift+Z / Ctrl+Y**: redo.
- **Delete/Backspace**: delete selected edges first (if any),
  else delete selected nodes. `deleteNodes` handles cascade
  (removing child nodes + re-parenting grandchildren).

CommandPalette binds its own global keydown in capture phase so
`Ctrl+P` fires even when focus is elsewhere. See
`command-palette.md` for that side.

## Connection model

### `onConnectStart` (line 1875-1881)

Sets `currentConnectionColor` based on the source port's type so
the drag preview line uses the right color.

### `isValidConnection` (line 1865-1870)

Rejects if the source/target scopes don't match:
```ts
getHandleScope(nodeId, handleId):
  if node is group + handle ends with __inner: return node.id
  return node.parentId || '__root__';
```

A regular node at root can't connect to a group's inner handle
because they're in different scopes. Only internal children can
reach `__inner`.

### `wouldCreateCycle(source, target)` (line 1817-1848)

DFS over edges. IMPORTANT: skips edges with `__inner` handles on
either end, because those represent data flowing through a group
boundary, not actual dependency cycles. Inner handles are
"fake-through" for purposes of cycle detection.

### `onReconnect` (line 1896-1928)

Updates the Weft source atomically: remove old edge line, add new
edge line. Both via `weftRemoveEdge` / `weftAddEdge`.

## `addNode(type)` (line 2052-2100)

Generates a fresh id via `generateNodeId` (snake-case + numeric
suffix that doesn't collide). Places at `contextMenuFlowPos` if
right-click-add, else `getViewportCenter`.

Groups: default `{ width: 400, height: 300, expanded: true }`,
label auto-generated via `generateUniqueGroupLabel`.
Annotations: `{ width: 250, height: 120, content: '' }`.
Regular: empty config.

Pushes to `weftCode` via `weftAddNode` / `weftAddGroup`, and to
`layoutCode` via `updateLayoutEntry`.

## `deleteNodes(nodeIds[])` (line 2102-2182)

Per-node logic:
- **Group**: children get re-parented to grandparent (or root).
  Position converted via `deletedGroup.position + child.position`.
  All connected edges dropped.
- **Regular**: just drop the node and all its edges.

Weft-code edits: delete non-groups first (so children are still
in-scope when removed), then groups. Each as a separate
`weftRemoveNode` / `weftRemoveGroup`.

## Selection and z-index raising

Every click on a node increments `nextNodeZ` (starts at 6, above
edge default 5) and raises the clicked node + its edges. This is
how v1 avoids "my clicked node is hidden behind a group". Line
2273-2288.

## Initial layout (line 1732-1750)

On first render:
- If `layoutCode` exists and `autoOrganizeOnMount` is false:
  `doFitView()` after 100ms.
- Else: run `autoOrganize`, save @layout entries, fit view.
- `canvasReady` state hides the canvas until initial layout
  settles, avoiding a flash of ugly default positions.

## Viewport: wheel + zoom (line 418-491)

`handleWheel` distinguishes pinch-zoom (synthetic Ctrl+wheel, no
real Ctrl) from actual Ctrl+wheel mouse zoom:
- Pinch: multiplier 0.03 (aggressive).
- Mouse Ctrl+wheel: multiplier 0.002 (gentle).

Normalizes `deltaMode` line/page to pixels via a redispatched
synthetic WheelEvent (for browsers that emit line-mode deltas
after losing focus).

Zoom bounds: `[0.05, 2]`.

## Selection

- `selectionKey = "Shift"`, `multiSelectionKey = "Shift"`, same key
  for both.
- `SelectionMode.Partial`.
- `selectionOnDrag = false` (plain drag doesn't box-select).
- `selectedNodeId` tracks last single-clicked node (for config
  panel).
- Shift+click adds to selection. Plain click clears + selects.

## Drag

- `onNodeDragStart` captures `preDragPositions` for all dragged
  nodes.
- `onNodeDragStop` runs the scope-lock pipeline (see
  `scope-lock.md`), then calls `layoutUpdateAny` per dragged node
  to persist positions in `layoutCode`.
- `onSelectionDragStop` same pattern for multi-select.

## `patchFromProject(newProject, andFitView)` (line 1474)

Called when the weft code is externally updated (AI edits, undo,
etc). Compares new project state to current xyflow state and
updates in place. Preserves user-modified config (width/height
from ELK) so we don't snap back on every parse.

## v2 port plan

The webview port distributes this orchestrator:

### Lives in the VS Code extension host

- **Weft source parsing** — done via dispatcher `/parse`.
- **Surgical edits** — `extension-vscode/src/surgical.ts` already
  handles updateConfig/updateLabel/addEdge/removeEdge/addNode/
  removeNode/duplicateNode. Missing: `addGroup`, `removeGroup`,
  `renameGroup`, `updatePorts`, `updateGroupPorts`,
  `moveNodeScope`, `moveGroupScope`. Tracked separately.
- **Layout file** — host reads/writes `<doc>.layout.json`. Done.

### Lives in Graph.svelte

- Node/edge composition from `project.nodes`, `project.edges`,
  `project.groups`, saved positions.
- Group virtual-node synthesis.
- Boundary passthrough hiding + edge rewriting.
- Topo-sort groups before children.
- Z-index rule.
- parentId assignment with ancestor-chain visibility walk.
- Expand/collapse with viewport anchoring.
- Cycle detection (`wouldCreateCycle`), scope-match validation
  (`isValidConnection`), edge dedup.
- Execution overlay (group synthesis).
- Drag scope-lock pipeline.
- Keyboard handler.
- CommandPalette.
- Right-click canvas/node context menus.
- Connection color preview.
- Wheel handler w/ pinch normalization and deltaMode fix.

### Lives in ProjectNode / GroupNode

- Per-node UI (already covered by their spec files).

### Not ported

- History / undo / redo inside the webview: VS Code provides
  Ctrl+Z on the .weft text buffer natively. We don't duplicate
  it. Accept: undo ≠ the "undo last graph mutation in memory",
  but undo the text edit instead. Same end-result because every
  mutation fires a surgical .weft edit.
- `weftSyncDirection` guard: not needed since VS Code already
  handles concurrent edits via its OT layer. The webview edits
  always flow through surgical → .weft → reparse → webview, a
  single direction.
- Publish modal, infra state panel, right sidebar, test config
  modal: those are playground / cloud dashboard-only features.
  See `design-decisions.md` for the list of deferred surfaces.

## Divergences

- **No undo stack in the webview.** Use VS Code's text undo.
- **No right sidebar / config panel.** Per-node config lives in
  the graph's ProjectNode body (as v1 already supports via its
  displayedFields path).
- **No history panel.** Execution history lives in
  ExecutionInspector modal.
- **Weft code panel.** VS Code is the weft code editor. The
  webview doesn't embed a second one.

All other behaviors should land as described above.
