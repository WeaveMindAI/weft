# Project Editor Orchestrator Parity

**v1 source**: `dashboard-v1/src/lib/components/project/ProjectEditorInner.svelte`
(3356 lines). This file is the root of the graph view. It
composes nodes/edges, syncs weft code ↔ graph, runs ELK,
handles selection, keyboard, undo/redo, streaming, context
menus, connection flow, and execution overlay.

v2 splits its job: extension host owns weft source parsing +
surgical edits + SSE bus; webview's `Graph.svelte` owns
rendering, interaction, and the execution overlay rendering.
This doc captures v1's end-to-end behavior so we know what v2
must preserve.

## Top-level state

```ts
// Core reactive state
let weftCode = $state(project.weftCode ?? '');
let layoutCode = $state(project.layoutCode ?? '');
let nodes = $state.raw<Node[]>(buildNodes(...));
let edges = $state.raw<Edge[]>(buildEdges(...));

// Sync control
let weftSyncDirection: 'none' | 'to-code' | 'to-editor' = 'none';
let weftSyncTimer: Timeout | null = null;
let codeEditInFlight = false;
let saveProjectTimer: Timeout | null = null;

// Selection + context
let selectedNodeId: string | null = null;
let pendingConnection: { sourceNodeId, sourceHandle } | null = null;
let contextMenuFlowPos: { x, y } | null = null;
let contextMenu: { x, y, flowX, flowY, nodeId: string | null } | null = null;

// Drag + scope-lock
let preDragPositions = new Map<string, { x, y }>();
let nextNodeZ = 6;  // counter for click-to-raise
let realCtrlDown = false;  // for pinch-zoom detection

// Undo stack
let history = $state<HistoryState[]>([]);
let historyIndex = $state(-1);
let isUndoRedo = false;
let lastPushTime = 0;  // debounce push
let lastHistoryHash = '';

// Layout/viewport
let currentViewport = $state({ x: 100, y: 100, zoom: 1 });
let hasFitView = false;
let canvasReady = false;

// Streaming
let weftStreaming = false;
let streamLastNodeIds = new Set<string>();
let streamLastEdgeIds = new Set<string>();
let streamOrganizePending = false;

// Code panel
let codePanelWidth = 480;
let isResizingCodePanel = false;
let codePanelMaximized = false;
let showCodePanel = !!playground;

// UI
let rightPanelTab: 'config' | 'executions' | 'history' = 'config';
let rightPanelCollapsed = ...;
let editingName = false;
let commandPaletteOpen = false;
let saveStatus: 'idle' | 'saved' = 'idle';
```

## Node/Edge composition pipeline

```
weftCode (string)
  │
  ├─ parseWeftCode() → ParseResult
  │    (applies layoutCode positions into result)
  │
  ├─ validateAndBuild(result) → ProjectDefinition
  │
  ├─ buildNodes(project.nodes, project.edges, layoutMap) → Node[]
  │    • filter to valid nodeType (SPECIAL_NODE_TYPES ∪ NODE_TYPE_CONFIG)
  │    • topo-sort groups before non-groups
  │    • compute visibility via ancestor-chain walk
  │    • compute z-index nesting depth
  │    • set xyflow type: 'group' | 'groupCollapsed' | 'annotation' | 'project'
  │    • assemble style (width/height) per expanded state
  │    • attach data.{nodeType, config, inputs, outputs, features,
  │      sourceLine, onUpdate, infraNodeStatus}
  │
  └─ buildEdges(project.edges, project.nodes) → Edge[]
       • dedup by (target, targetHandle) — enforces 1-driver-per-input
       • getEdgeColor(source, sourceHandle) — strips __inner, looks up
         port.portType color
       • style: "stroke-width: {2|3}px; stroke: {edgeColor};"
       • markerEnd: MarkerType.ArrowClosed, width 20, height 20
       • className: 'edge-active' when activeEdges.has(id)
       • zIndex: 5 (raised to nextNodeZ+1 when either endpoint clicked)
```

## Visibility walk (buildNodes line 864-883)

For every node with `rawParentId`:
```ts
let hiddenByCollapsedGroup = false;
let pid = rawParentId;
while (pid) {
  const ancestor = projectNodes.find(g => g.id === pid);
  if (!ancestor) break;
  if ((ancestor.config as Record<string, boolean>)?.expanded === false) {
    hiddenByCollapsedGroup = true;
    break;
  }
  pid = (ancestor.config as Record<string, string>)?.parentId;
}
const directParent = projectNodes.find(g => g.id === rawParentId);
const parentGroupExpanded = directParent
  ? (directParent.config as Record<string, boolean>)?.expanded ?? true
  : false;
const parentId = (rawParentId && parentGroupExpanded && !hiddenByCollapsedGroup)
  ? rawParentId
  : undefined;
// Hidden nodes: style 'display: none;'.
```

Key detail: a node can have `rawParentId` = "Outer" but xyflow
`parentId = undefined` if ANY ancestor in the chain is collapsed.
That lets the node render directly (not scoped inside any
group's coordinate system) but still "disappear" via
`display: none`.

## Z-index formula (line 903)

```ts
const zIndex = isAnnotation ? -1
  : isGroup && isExpanded ? -1 + nestingDepth
  : isGroup ? 4                           // collapsed groups
  : 4;                                    // regular nodes
// clicked nodes: raised via nextNodeZ++
// edges: 5 by default; raised to nextNodeZ+1 when a connected node is raised
// dragging: class 'dragging' adds z-index: 1 !important
// connecting: class 'connecting' adds z-index: 2
// display:none: z-index: 0 !important
```

`nestingDepth` counted by walking parentId chain of groups only.
Root expanded group: -1. Child expanded group: 0. Grandchild: 1.
Collapsed groups float above all expanded ones at z=4.

## createNodeUpdateHandler(nodeId)  (line 521-810)

Central mutation sink. Called by all node components via
`data.onUpdate({ label?, config?, inputs?, outputs? })`.

### Detection

```ts
const isExpandToggle = updates.config && 'expanded' in updates.config;
const oldGroupLabel = 'label' in updates ? nodes.find(n.id === nodeId)?.data.label : undefined;
```

### Pre-toggle capture (when expanding/collapsing)

```ts
if (isExpandToggle) {
  const current = nodes.find(n => n.id === nodeId);
  if (current) {
    const rect = getNodeRect(current);
    oldWidth = rect.width;
    oldHeight = rect.height;
    oldPosition = getAbsolutePosition(current);
  }
}
```

### Node data mutation

Walks `nodes` array, finds the node, mutates per type:
- **Annotation**: `width/height` fixed from config or default 250/120.
- **Group**: expanded → `type: 'group', zIndex: -1`, style with w/h;
  collapsed → `type: 'groupCollapsed', zIndex: 4`, style with
  `computeMinNodeWidth` and `height: auto`, unset `width`/`height`.
- **Regular**: collapsed → `computeMinNodeWidth` + `height: auto`;
  expanded with saved width/height → use both, clamped by
  `computeMinNodeWidth`; expanded without saved → width =
  `Math.max(320, computeMinNodeWidth)`, `height: auto`.

### Post-toggle visibility walk

When isExpandToggle, rebuild every node's `parentId` and `style`
based on the new ancestor expanded states. Loop logic same as
`buildNodes` ancestor walk; output is ENTIRE `nodes` array
remapped, with all edges touching hidden nodes set to
`hidden: true`.

Edge hide logic:
```ts
edges = edges.map(e => {
  const touchesHidden = hiddenNodeIds.has(e.source) || hiddenNodeIds.has(e.target);
  if (touchesHidden) return { ...e, hidden: true };
  if (e.hidden) return { ...e, hidden: false };
  return e;
});
```

### Viewport anchoring (line 682-731)

```ts
// Before toggle: capture screen-space top-right of the toggled node.
const oldAbsTopRight = { x: absPos.x + oldWidth, y: absPos.y };
const vp = getViewport();
const oldScreenX = oldAbsTopRight.x * vp.zoom + vp.x;
const oldScreenY = oldAbsTopRight.y * vp.zoom + vp.y;

// Toggle → wait tick + 2 × rAF → runAutoOrganize → wait for positions
tick().then(() => requestAnimationFrame(() => requestAnimationFrame(() => {
  runAutoOrganize(false).then(() => {
    const postNode = nodes.find(n => n.id === pinnedNodeId);
    if (postNode) {
      const postAbs = getAbsolutePosition(postNode);
      const postRect = getNodeRect(postNode);
      const newAbsTopRight = { x: postAbs.x + postRect.width, y: postAbs.y };
      const currentVp = getViewport();
      const newVpX = oldScreenX - newAbsTopRight.x * currentVp.zoom;
      const newVpY = oldScreenY - newAbsTopRight.y * currentVp.zoom;
      if (Math.abs(newVpX - currentVp.x) > 1 || Math.abs(newVpY - currentVp.y) > 1) {
        setViewport({ x: newVpX, y: newVpY, zoom: currentVp.zoom });
      }
    }
    // Re-hide edges (runAutoOrganize unhides all edges as a side effect)
    // then `layoutUpdateAny` each node to persist positions.
    // saveToHistory() + saveProject().
  });
})));
```

This keeps the expand toggle button on a group under the cursor
when the group grows or shrinks.

### Port update side-effect (line 734-736)

```ts
if ('inputs' in updates || 'outputs' in updates) {
  tick().then(() => updateNodeInternals(nodeId));
}
```

`updateNodeInternals` is xyflow's API to rescan Handle positions.
Required after adding ports; without it the new handles aren't
connectable.

### Weft code update branches

```ts
if ('label' in updates) {
  if (isGroup && oldGroupLabel && newLabel) {
    weftCode = weftRenameGroup(weftCode, oldGroupLabel, newLabel);
    // Plus rename layoutCode entries: scoped id renames propagate
    const parts = nodeId.split('.');
    parts[parts.length - 1] = newLabel;
    const newScopedId = parts.join('.');
    layoutCode = renameLayoutPrefix(layoutCode, nodeId, newScopedId);
  } else {
    weftCode = weftUpdateLabel(weftCode, nodeId, updates.label ?? null);
  }
}

if ('config' in updates) {
  for (const [key, value] of Object.entries(updates.config)) {
    if (['parentId', 'textareaHeights', '_opaqueChildren'].includes(key)) continue;
    if (['width', 'height', 'expanded'].includes(key)) {
      // Layout keys: write to layoutCode via layoutUpdateAny(), not weftCode
      layoutUpdateAny(...);
    } else {
      weftCode = weftUpdateConfig(weftCode, nodeId, key, value);
    }
  }
}

if ('inputs' in updates || 'outputs' in updates) {
  if (isGroup) {
    weftCode = weftUpdateGroupPorts(weftCode, groupLabel, inputs, outputs);
  } else {
    weftCode = weftUpdatePorts(weftCode, nodeId, inputs, outputs);
  }
}
```

### History + save debouncing

```ts
const isResize = updates.config && ('width' in updates.config || 'height' in updates.config);

if ('config' in updates && !isResize) {
  // Text typing: debounce history 500ms, API save 5000ms
  if (configEditTimer) clearTimeout(configEditTimer);
  configEditTimer = setTimeout(() => saveToHistory(), 500);
  if (saveProjectTimer) clearTimeout(saveProjectTimer);
  saveProjectTimer = setTimeout(() => saveProject(), 5000);
} else {
  saveToHistory();
  saveProject();
}
```

## Execution state overlay (line 1016-1111)

Re-runs on every `executionState` prop change. Walks nodes and
edges.

### Node overlay

```ts
const debugData = features.showDebugPreview ? nodeOutputs[n.id] : undefined;
const executions = nodeType === 'Group'
  ? synthesizeGroupExecutions(groupId, nodeExecutions, project.nodes)
  : nodeExecutions[n.id] ?? [];
const latestExec = executions[executions.length - 1];
const nodeClass =
  (latestExec?.status === 'running' || latestExec?.status === 'waiting_for_input') ? 'node-running'
  : latestExec?.status === 'failed' ? 'node-failed'
  : (latestExec?.status === 'completed' || latestExec?.status === 'skipped') ? 'node-completed'
  : '';
// Update n.data.{debugData, executions, executionCount} + n.class
```

### Group synthesis (details in execution.md)

Walks `__in.output` / `__out.output` / internal children's exec
records, folds them into virtual Group executions.

### Edge overlay

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

## Two-way sync weft code ↔ graph

### `handleWeftCodeChange(newCode)` (line 331-356)

User-edit handler for the code panel. Debounced 500ms via
`WEFT_SYNC_DEBOUNCE_MS`. After pause:
1. Set `weftSyncDirection = 'to-editor'`.
2. `parseWeftCode(weftCode)` → ProjectDefinition.
3. `applyParseResult(w)` — stores opaque blocks + errors.
4. Sync `project.name` / `project.description`.
5. `patchFromProject(w.project)` — diffs into nodes/edges,
   preserves user positions, queues ELK for new nodes.
6. Restore saved opaque blocks (they were reset).
7. `saveProject()`.
8. Await tick() to settle reactivity.
9. Clear `weftSyncDirection`.

### `patchFromProject(newProject, andFitView)` (line 1474-1502)

Diffs a new project snapshot into the existing xyflow state:

```ts
// 1. Build position map from current state
const currentPositions = new Map(nodes.map(n => [n.id, n.position]));

// 2. Build new nodes from the new project
const newNodes = buildNodes(newProject.nodes, newProject.edges, parseLayoutCode(layoutCode))
  .map(n => {
    const existingPos = currentPositions.get(n.id);
    if (existingPos) return { ...n, position: existingPos };
    // New node: invisible but rendered so xyflow can measure it
    return { ...n, class: ((n.class ?? '') + ' node-pending-layout').trim() };
  });

// 3. New edges: hidden + pendingLayout flag
const currentEdgeIds = new Set(edges.map(e => e.id));
edges = buildEdges(newProject.edges, newProject.nodes).map(e =>
  currentEdgeIds.has(e.id) ? e : { ...e, hidden: true, data: { ...e.data, pendingLayout: true } }
);

// 4. Wait for xyflow to measure new nodes (.measured)
await tick();
await new Promise(r => setTimeout(r, 300));

// 5. Run ELK, ELK removes pending classes after laying out
await runAutoOrganize(andFitView);
```

### The `weftSyncDirection` guard

Prevents feedback loops. `'to-code' | 'to-editor' | 'none'`.
- `to-code`: a user edit in the graph is being pushed into
  weftCode via `weftUpdate*`. During this phase, changes to
  weftCode don't trigger `handleWeftCodeChange` side-effects.
- `to-editor`: a user edit in the code panel is being parsed
  and pushed into xyflow state.

Setting it before each mutation and clearing it after (via tick +
flag) is the mechanism that keeps the two sides from fighting.

## Streaming pipeline (line 1504-1728)

AI streaming via fenced code blocks. Three modes:
- **weft**: full document, clear editor and start fresh.
- **weft-patch**: apply patches to existing weft; preserve state.
- **weft-continue**: append to end; preserve state.

```ts
weftStreamStart(mode):
  weftStreaming = true;
  weftSyncDirection = 'to-code';
  // Cancel pending debounce from user edits
  // Force-blur active element to avoid focus-related sync bugs
  // Initialize stream state per mode

weftStreamDelta(delta, mode, at?):
  // Positional insert if `at`, else append
  weftCode = ...;
  // Debounce incremental parse 100ms
  streamParseTimer = setTimeout(streamTryIncrementalParse, 100);

streamTryIncrementalParse():
  const parsed = parseWeft(weftCode).projects[0].project;
  // Diff node/edge sets
  if (nodesDiffer || edgesDiffer || contentChanged) {
    streamLastNodeIds = ...;
    streamLastEdgeIds = ...;
    streamSyncVisual(parsed);
  }

streamSyncVisual(parsed):
  // Preserve existing positions + group widths/heights
  // New nodes placed to the right of existingRightEdge, stacked vertically
  // New edges hidden until ELK runs
  // Debounce runAutoOrganize 400ms (don't ELK on every single item)
```

The streaming pipeline is invisible-by-default: new nodes are
marked `node-pending-layout` (opacity 0). ELK runs, removes the
class, makes them visible. Prevents flash-to-wrong-position.

## Keyboard handler (line 2184-2268)

```ts
function handleKeyDown(event) {
  const isEditable =
    target.tagName === 'INPUT' || target.tagName === 'TEXTAREA'
    || target.isContentEditable
    || target.closest('[role="dialog"]')
    || target.closest('.edit-textarea')
    || target.closest('.annotation-node.editing');

  // Ctrl+S ALWAYS works, even in editable
  if (ctrl+s) {
    event.preventDefault();
    if (weftSyncTimer && codeEditInFlight) {
      // Flush pending code panel debounce: parse + patch + save
    }
    saveProject();
    return;
  }

  if (isEditable) return;

  if (Escape) close context menu + pending connection;
  if (ctrl+z && !shift) undo();
  if (ctrl+y || ctrl+shift+z) redo();
  if (Delete || Backspace) {
    if (selectedEdges.length) {
      // Remove each edge from weftCode
      for (const e of selectedEdges) {
        const ref = toWeftEdgeRef(...);
        weftCode = weftRemoveEdge(weftCode, ref.srcRef, ref.srcPort, ref.tgtRef, ref.tgtPort);
      }
      edges = edges.filter(e => !e.selected);
      saveToHistory(); saveProject();
    } else {
      deleteNodes(selected.map(n => n.id));
    }
  }
}
```

CommandPalette owns Ctrl+P + action shortcuts in its own
capture-phase handler.

## Undo stack (line 1225-1313)

```ts
const MAX_HISTORY = 50;
type HistoryState = { nodes: Node[]; edges: Edge[]; weftCode: string };

saveToHistory():
  if (isUndoRedo) return;
  // Debounce: prevent multiple pushes within DEBOUNCE_MS (100ms)
  const now = Date.now();
  if (now - lastPushTime < DEBOUNCE_MS) return;
  lastPushTime = now;
  const state = cloneState();
  // Dedup by fast hash
  const hash = hashState(state);
  if (hash === lastHistoryHash) return;
  lastHistoryHash = hash;
  // Truncate redo history, push new state
  history = history.slice(0, historyIndex + 1);
  history.push(state);
  if (history.length > MAX_HISTORY) history.shift();
  historyIndex = history.length - 1;

undo():
  if (historyIndex <= 0) return;
  isUndoRedo = true;
  historyIndex--;
  restoreFromHistory(history[historyIndex]);
  isUndoRedo = false;
  saveProject();

redo():
  if (historyIndex >= history.length - 1) return;
  isUndoRedo = true;
  historyIndex++;
  restoreFromHistory(history[historyIndex]);
  isUndoRedo = false;
  saveProject();

restoreFromHistory(state):
  // Deep clone nodes from state
  const restoredNodes = JSON.parse(JSON.stringify(state.nodes));
  // RE-ATTACH callbacks lost during serialization
  for (const n of restoredNodes) {
    n.data.onUpdate = createNodeUpdateHandler(n.id);
  }
  nodes = restoredNodes;
  edges = JSON.parse(JSON.stringify(state.edges));
  weftCode = state.weftCode;
```

The callback re-attachment is important: JSON.parse/stringify
destroys functions, so after deserialization we have to re-bind
the `onUpdate` for each node.

## Connection flow

### `onConnectStart` (line 1875-1881)

Sets `currentConnectionColor` from source port's type. The
connection preview line uses this color.

### `onBeforeConnect(connection)` (line 1977-2031)

Runs BEFORE xyflow adds the edge. Returns `Edge | null` (null
rejects).
- `wouldCreateCycle(source, target)` → alert + null.
- **Remove any existing edge with same `(target, targetHandle)`**.
- Build the new Edge object with color from source port.
- Schedule `setTimeout(() => weftAddEdge + save, 0)` to run after
  xyflow commits the edge visually.

### `isValidConnection(connection)` (line 1865-1870)

Used by xyflow to validate connection attempts interactively.
Scope-based:
```ts
function getHandleScope(nodeId, handleId):
  const node = nodes.find(n => n.id === nodeId);
  const isGroup = node.type === 'group' || node.type === 'groupCollapsed';
  if (isGroup && handleId?.endsWith('__inner')) {
    return nodeId;  // inner handle: scope is inside this group
  }
  return node.parentId || '__root__';  // outer/regular: parent's scope

return getHandleScope(src, srcHandle) === getHandleScope(tgt, tgtHandle);
```

### `wouldCreateCycle(source, target)` (line 1817-1848)

DFS over edges. CRITICAL: skips edges with `__inner` on either
handle — those represent data flowing through a group boundary,
not real dependencies.

### `onReconnect(oldEdge, newConnection)` (line 1896-1928)

`EdgeReconnectAnchor` in CustomEdge triggers this when user drags
an edge endpoint. Atomic replace:
```ts
weftCode = weftRemoveEdge(weftCode, oldRef);
weftCode = weftAddEdge(weftCode, newRef);
edges = edges.map(e => e.id === oldEdge.id ? {...e, source, sourceHandle, target, targetHandle} : e);
```

### `onConnectEnd(event, connectionState)` (line 1939-1975)

Triggered on EVERY connection drop. If `!connectionState.isValid`
(e.g. released on empty pane), opens the node-picker context
menu at the drop point:
```ts
contextMenu = { x, y, flowX, flowY, nodeId: null };
pendingConnection = { sourceNodeId, sourceHandle };
```

Selecting a node type from the palette adds a new node at
`contextMenuFlowPos` AND wires the pending connection to it. See
`addNode` for details.

## `addNode(type)` (line 2052-2100)

```ts
const id = generateNodeId(type);
const pos = contextMenuFlowPos ?? getViewportCenter();

const newNode = {
  id,
  type: isGroup ? 'group' : isAnnotation ? 'annotation' : 'project',
  position: pos,
  selected: true,  // select new node
  data: {
    label: isGroup ? generateUniqueGroupLabel(typeConfig.label) : null,
    nodeType: type,
    config: isGroup ? { width: 400, height: 300, expanded: true }
      : isAnnotation ? { width: 250, height: 120, content: '' }
      : {},
    inputs: [...typeConfig.defaultInputs],
    outputs: [...typeConfig.defaultOutputs],
    features: typeConfig.features || {},
    onUpdate: createNodeUpdateHandler(id),
  },
};

// Deselect all other nodes
const deselected = nodes.map(n => ({ ...n, selected: false }));

// Groups + annotations insert BEFORE non-groups (xyflow parent-before-child)
if (isGroup || isAnnotation) {
  const specialNodes = deselected.filter(n => n.type is group/groupCollapsed/annotation);
  const otherNodes = deselected.filter(n => ... not);
  nodes = [...specialNodes, newNode, ...otherNodes];
} else {
  nodes = [...deselected, newNode];
}

// Weft code + layout
if (isGroup) {
  weftCode = weftAddGroup(weftCode, label);
  layoutCode = updateLayoutEntry(layoutCode, label, pos.x, pos.y, 400, 300);
} else {
  weftCode = weftAddNode(weftCode, type, id);
  layoutCode = updateLayoutEntry(layoutCode, id, pos.x, pos.y);
}
saveToHistory();
saveProject();
```

`generateUniqueGroupLabel(base)`: if `base` is taken, tries
`{base}_2`, `{base}_3`, etc.

## `deleteNodes(nodeIds[])` (line 2102-2182)

Multi-node delete with group-hoisting:
```ts
for (const nodeId of nodeIds) {
  const nodeBeingDeleted = nodes.find(n => n.id === nodeId);
  const isGroup = nodeBeingDeleted?.type in ('group', 'groupCollapsed');

  if (isGroup) {
    // Hoist children to grandparent (or root)
    const grandparentId = nodeBeingDeleted.data.config?.parentId;
    nodes = nodes
      .filter(n => n.id !== nodeId)
      .map(n => {
        if (n.parentId === nodeId) {
          // Re-parent + convert position to grandparent's coord system
          const absX = deletedGroup.position.x + n.position.x;
          const absY = deletedGroup.position.y + n.position.y;
          return { ...n, position: { x: absX, y: absY }, parentId: grandparentId || undefined, data: {...n.data, config: {...config, parentId: grandparentId}} };
        }
        return n;
      });
    edges = edges.filter(e => e.source !== nodeId && e.target !== nodeId);
  } else {
    nodes = nodes.filter(n => n.id !== nodeId);
    edges = edges.filter(e => e.source !== nodeId && e.target !== nodeId);
  }
}

// Weft code: remove non-groups FIRST (children while still in scope),
// then groups.
for (non-group nodeId) weftRemoveNode;
for (group nodeId) weftRemoveGroup;
```

## `duplicateNode(nodeId)` (line 2658-2705)

```ts
const original = nodes.find(n => n.id === nodeId);
const newId = generateNodeId(nodeType);
const newPos = { x: original.position.x + 50, y: original.position.y + 50 };

const newNode = {
  ...original,
  id: newId,
  position: newPos,
  data: {
    ...original.data,
    label: isGroup ? generateUniqueGroupLabel(...) : original.data.label,
    onUpdate: createNodeUpdateHandler(newId),
  },
};

nodes = [...nodes, newNode];
selectedNodeId = newId;

// Weft sync: recreate the declaration
if (isGroup) {
  weftCode = weftAddGroup(weftCode, groupLabel);
  // Note: does NOT recursively duplicate children.
} else {
  weftCode = weftAddNode(weftCode, nodeType, newId);
  // Copy config fields (excluding layout + reserved)
  for (const [k, v] of Object.entries(original.config)) {
    if (['parentId', 'textareaHeights', 'width', 'height', 'expanded'].includes(k)) continue;
    if (v === undefined || v === null || v === '') continue;
    weftCode = weftUpdateConfig(weftCode, newId, k, v);
  }
}
```

## Viewport management

### `handleWheel(e)` (line 433-491)

Custom wheel handler, runs in capture phase:
- **Pinch-zoom detection**: browser sends synthetic Ctrl+wheel on
  pinch; real Ctrl+wheel has `realCtrlDown` set by keyboard
  listener. Different multipliers:
  - Pinch (no real Ctrl): `multiplier = 0.03`.
  - Real Ctrl+wheel: `multiplier = 0.002`.
- Zoom math: `zoomDelta = -e.deltaY * multiplier; newZoom = viewport.zoom * (1 + zoomDelta)`. Clamped to [0.05, 2].
- Target the mouse position as the zoom anchor.
- **Line/page deltaMode normalization** (line 460-490): if deltaMode != 0, convert to pixels (LINE_HEIGHT 16, PAGE_HEIGHT 800), redispatch a synthetic WheelEvent with `__weftNormalized` flag so xyflow sees pixel values.

### `doFitView(padding = 0.2)` (line 1315-1369)

Custom fit-view because xyflow's built-in doesn't walk parentId
chains:
```ts
const visibleNodes = nodes.filter(n => n.style !== 'display: none;' && n.measured?.width && n.measured?.height);

function getAbsPos(node):
  let x = node.position.x, y = node.position.y;
  if (node.parentId) {
    const parent = nodes.find(n => n.id === node.parentId);
    if (parent) {
      const parentAbs = getAbsPos(parent);
      x += parentAbs.x;
      y += parentAbs.y;
    }
  }
  return {x, y};

// Compute bbox over absolute positions + measured sizes
let minX, minY, maxX, maxY = ...;
const contentW = maxX - minX;
const contentH = maxY - minY;
const zoom = Math.min((container.w - padW) / contentW, (container.h - padH) / contentH, 2);
const clampedZoom = Math.max(0.05, Math.min(zoom, 2));
// center viewport on bbox center
setViewport({ x, y, zoom: clampedZoom });
```

## `runAutoOrganize(andFitView)` (line 1390-1472)

Wraps the ELK `autoOrganize`:
1. Collect measured sizes from xyflow (`n.measured.width/height`).
2. Measure actual port Y positions from the DOM via
   `measurePortPositions(nodeId)`:
   ```ts
   const nodeEl = document.querySelector(`[data-id="${nodeId}"]`);
   const nodeRect = nodeEl.getBoundingClientRect();
   for (const handle of nodeEl.querySelectorAll('.svelte-flow__handle')) {
     const handleId = handle.getAttribute('data-handleid');
     const relY = handleRect.top + handleRect.height / 2 - nodeRect.top;
     portYMap.set(handleId, relY);
   }
   ```
3. Build currentNodes / currentEdges snapshots from xyflow state
   (not the stale `project` prop).
4. `autoOrganize(currentNodes, currentEdges, sizes, portPositions)`
   → `{ positions, groupSizes }`.
5. Map results back onto `nodes`:
   - Set `position` per positions map.
   - Set group `style = width: Wpx; height: Hpx;` and update
     `config.width/height` per groupSizes.
   - Remove `node-pending-layout` class.
6. Unhide edges with `pendingLayout` flag.
7. `layoutUpdateAny(n)` per node to persist positions into
   layoutCode.
8. Optionally `doFitView()`.

## Context menu (line 2626-2639)

Right-click on canvas or node:
```ts
function onContextMenu(event):
  event.preventDefault();
  const flowPos = screenToFlowPosition({ x: event.clientX, y: event.clientY });
  const clickedNodeId = findNodeAtPosition(event.clientX, event.clientY);
  contextMenu = { x: event.clientX, y: event.clientY, flowX, flowY, nodeId };
```

`findNodeAtPosition` walks `.svelte-flow__node` elements checking
bounding rects. Returns the node id at the click, or null.

Rendered context menu (line 3127) has:
- Add Node... (opens command palette with position pre-set)
  (when nodeId is null)
- Duplicate / Delete (when nodeId is set)
- Undo / Redo

## Code panel resize (line 360-383)

Mouse-down on the resize handle starts a drag. Tracks
`codePanelWidth` between `CODE_PANEL_MIN_WIDTH = 320` (actually
`CODE_PANEL_MIN_WIDTH = 280` per line 276, both exist depending
on mode) and `CODE_PANEL_MAX_WIDTH = 1200`.

`codePanelMaximized`: full-width override.

On mobile: code panel is full-width when open, hidden when
diagram is visible (`mobileForceEditor` state).

## Initial layout (line 1732-1750)

```ts
$effect(() => {
  if (!hasFitView && nodes.length > 0) {
    hasFitView = true;
    if (!layoutCode || autoOrganizeOnMount) {
      // No saved layout: run ELK after 300ms
      hasAutoOrganized = true;
      setTimeout(() => runAutoOrganize(true).then(() => { canvasReady = true; }), 300);
    } else {
      // Saved layout: just fit view after 100ms
      setTimeout(() => { doFitView(); canvasReady = true; }, 100);
    }
  } else if (!hasFitView && nodes.length === 0) {
    canvasReady = true;
  }
});
```

`canvasReady` gates CSS visibility of the flow container. Hides
the canvas until initial ELK settles, preventing flash of
unorganized positions.

## Selection

- `selectionKey = "Shift"`, `multiSelectionKey = "Shift"`, same
  key for both box-select and multi-pick.
- `SelectionMode.Partial`: box-select accepts partial overlap.
- `selectionOnDrag = false`: plain drag doesn't box-select.
- Click raises z-index (line 2273-2288):
  ```ts
  tick().then(() => {
    nodes = nodes.map(n => n.id === node.id ? { ...n, zIndex: nextNodeZ } : n);
    edges = edges.map(e =>
      (e.source === node.id || e.target === node.id)
        ? { ...e, zIndex: nextNodeZ + 1 }
        : e
    );
    nextNodeZ++;
  });
  ```

## Drag handlers

- `onNodeDragStart` (line 2312-2325): captures `preDragPositions`
  per dragged node, raises target z-index.
- `onNodeDragStop` (line 2327-2353): runs scope-lock pipeline
  (checkNodeLeavesGroup → checkNodeCapturedByGroup →
  checkGroupCapturesNodes; details in `scope-lock.md`), then
  `layoutUpdateAny` per dragged node, `saveToHistory`, `saveProject`.
- `onSelectionDragStop` (line 2355-2383): same but per-node across
  the selection.

## Public exports (for parent component)

```ts
export async function patchFromProject(p: ProjectDefinition, andFitView?: boolean): Promise<void>
export function weftStreamStart(mode: 'weft' | 'weft-patch' | 'weft-continue')
export function weftStreamDelta(delta: string, mode, at?: number)
export function weftStreamEnd(mode)
export function getWeftCode(): string
export function getRawWeftCode(): string
export function getLayoutCode(): string
export function isStreaming(): boolean
```

## v2 port plan

Same as before (see earlier in this doc's original v2 port plan
section + scope-lock.md + execution.md). The additions from this
deeper pass:

- Viewport anchoring requires `tick() + 2×rAF + runAutoOrganize`
  to work reliably. The double-rAF is NOT cosmetic: without it
  xyflow's size measurements aren't settled when we compute
  post-toggle position.
- `updateNodeInternals` call after port changes.
- Cycle detection MUST skip `__inner` handles.
- `isValidConnection` scope check.
- `onConnectEnd` with `isValid=false` → context menu + pending
  connection → addNode wires to pending.
- Deep-clone + callback re-attach on undo/redo.
- `measurePortPositions` from DOM for accurate ELK port Y.
- `doFitView` walking parentId chain for absolute positions.
- `node-pending-layout` class for invisible-until-ELK-positioned.
- Line/page deltaMode wheel normalization.

## Divergences in v2

- **Undo stack**: VS Code's native text undo covers .weft edits.
  We don't duplicate the JS-side history stack. Implication:
  undo in the graph view actually undoes the last text edit, not
  necessarily "the last visual mutation". Close enough because
  every mutation routes through surgical → .weft.
- **Right sidebar / config panel**: config lives inline in each
  ProjectNode body (same as v1 supports via displayedFields).
- **History panel**: execution history lives in ExecutionInspector
  modal.
- **Code panel**: VS Code is the code panel. Webview just
  visualizes.
- **Streaming**: the AI edit flow lives in the extension host,
  not the webview. Webview just receives updated `project` after
  each streaming parse.
