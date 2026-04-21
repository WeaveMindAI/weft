# Group Node Parity

**v1 source**: `dashboard-v1/src/lib/components/project/GroupNode.svelte` (751 lines).

## Two rendering modes

Single component, two top-level branches:

- **Expanded** (`{#if isExpanded}` at line 214): full container
  with dashed border, side ports, resizer.
- **Collapsed** (`{:else}` at line 369): looks like a regular
  ProjectNode, auto-sized.

`isExpanded = (data.config?.expanded as boolean) ?? true`.
Line 38. **Groups default to expanded.** (Regular nodes default
to collapsed.)

## Data prop shape

```ts
data: {
  label: string | null;
  nodeType: string;       // 'Group'
  config: Record<string, unknown>;
  inputs?: PortDefinition[];   // interface in-ports
  outputs?: PortDefinition[];  // interface out-ports
  features?: { oneOfRequired?: string[][] };
  onUpdate?: (updates: NodeDataUpdates) => void;
  executions?: NodeExecution[];
  executionCount?: number;
}
```

Config keys:
- `expanded: boolean` (default true)
- `width: number`, `height: number` (expanded only)
- `description: string` (optional; rendered as line-clamped text
  in collapsed mode with a Show more/less toggle if > 80 chars or
  contains a newline)
- `parentId: string` (if nested)

## Expanded mode

### Visuals (lines 527-644)

Container:
```css
.expanded-container {
  width: 100%;
  height: 100%;
  background: rgba(148, 163, 184, 0.06);
  border: 2px dashed rgba(148, 163, 184, 0.4);
  border-radius: 12px;
  min-width: 250px;
  min-height: 200px;
}
.expanded-container.selected {
  border-color: hsl(var(--primary));
  border-style: solid;
  background: hsl(var(--primary) / 0.04);
}
```

Header bar (line 546-558): frosted white ribbon at top, 11px
semi-bold #52525b, 6px padding, 1px bottom border.

### NodeResizer (lines 216-223)

```svelte
<NodeResizer
  minWidth={250}
  minHeight={Math.max(200, minExpandedHeight)}
  isVisible={selected}
  lineStyle="border-color: hsl(var(--primary)); border-width: 2px;"
  handleStyle="background-color: hsl(var(--primary)); width: 10px; height: 10px; border-radius: 2px;"
  onResizeEnd={handleResizeEnd}
/>
```

`minExpandedHeight = computeMinHeight(inputs.length, outputs.length)`
= `36 + 8 + max(numInputs, numOutputs) * 30 + 24 + 128` (line 168-170).

`handleResizeEnd` writes `width`, `height`, `expanded: true` back
into config (line 66-72).

### Min-height auto-enforcement (lines 47-56)

When port count grows past current height, automatically bump
height to `minExpandedHeight`. Guarded by `lastEnforcedMinH` so
it doesn't loop.

### Side ports

Two absolute-positioned columns:

```css
.expanded-side-ports { position: absolute; top: 40px; display: flex;
  flex-direction: column; gap: 6px; z-index: 1; padding: 4px 0; }
.expanded-side-left { left: 6px; }
.expanded-side-right { right: 6px; }
```

**Each port is rendered as a two-handle block.** Lines 250-307 for
inputs, 309-366 for outputs. This is the core of the group model.

#### Input port block (lines 263-283)

```svelte
<div class="expanded-port-dots">
  <!-- External handle (target), outside connections -->
  <Handle
    type="target"
    position={Position.Left}
    id={input.name}
    style={pMarker.style}
    class={pMarker.class}
  />
  <!-- Internal handle (source), child connections -->
  <Handle
    type="source"
    position={Position.Right}
    id="{input.name}__inner"
    style="background-color: {getPortTypeColor(input.portType)};"
    class="!w-2.5 !h-2.5 !border !border-white !rounded-full !relative !inset-auto !transform-none"
  />
</div>
```

Semantically:
- The **external handle** is `target`, positioned Left. An outside
  node's output edge ends here. Handle id = port name.
- The **internal handle** is `source`, positioned Right. A
  child-node's input reads from here. Handle id = `{port}__inner`.
- Both handles are rendered IN the same DOM block, side by side,
  so visually the port row reads `[dot]  label  [inner-dot]`.
- The internal handle gets the `!relative !inset-auto !transform-none`
  overrides so xyflow doesn't absolute-position it.

#### Output port block (lines 324-341)

Mirror of input:
- **Internal handle** type=`target`, position Left, id=`{port}__inner`.
  A child node's output edge ends here.
- **External handle** type=`source`, position Right, id = port name.
  The group's output edge starts here.

### Port context menu (lines 147-166)

Right-click on a port opens a floating menu on document.body.
`buildPortMenuItems` from `utils/port-context-menu.ts`. Items:
- toggle required (inputs only)
- set type (prompt)
- remove port

Group interface ports are always `isCustom: true`, `canAddPorts: true`
(line 158-159). The menu writes back via `onUpdate({ inputs, outputs })`.

### Add port UI (lines 286-306, 345-365)

Per-side: a `[+]` button. On click → text input appears. Enter
to commit, Escape to cancel, blur to cancel-empty or commit if
filled. Duplicate names are rejected silently. New ports default
to `portType: 'MustOverride', required: false` (line 179, 182).

### Header (lines 226-247)

Left: group icon + label (editable).
Right: ExecutionInspector + collapse button (Minimize2 icon).

### Label editing (lines 74-107)

Sanitized. Line 78-82:
```ts
return val.replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_]/g, '');
```
Then strip leading digits (line 94). Only commits if non-empty AND
different from current. Enter commits, Escape cancels.

## Collapsed mode (lines 369-524)

### Outer visuals (lines 646-697)

```css
.collapsed-node {
  background: white;
  border: 1px solid #e4e4e7;
  border-radius: 8px;
  min-width: 160px;
  width: 100%;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.06);
  overflow: hidden;
}
.collapsed-node.selected {
  border-color: hsl(var(--primary));
  box-shadow: 0 0 0 2px hsl(var(--primary) / 0.15);
}
```

### Execution glows (lines 662-670)

```css
:global(.node-running) .collapsed-node {
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08), 0 0 0 2px rgba(245, 158, 11, 0.4);
}
:global(.node-completed) .collapsed-node { ... rgba(16, 185, 129, 0.3); }
:global(.node-failed) .collapsed-node { ... rgba(239, 68, 68, 0.4); }
```

### Structure

1. **Accent bar** (3px tall, #52525b). Matches the "group" category
   color.
2. **Header**: `GROUP` type label + expand toggle.
3. **Label row** (same editable pattern as expanded).
4. **Description row** (if `config.description` is set): text with
   `line-clamp-2`, Show more/less toggle if >80 chars or newline.
5. **Port rows**: two columns (inputs left, outputs right),
   **only external handles**. Internal `__inner` handles are NOT
   rendered, because children aren't visible. But the edges still
   exist; they just don't connect to visible handles until
   expanded.

### `computeMinNodeWidth(inputs, outputs)` — collapsed width

Defined in ProjectEditorInner.svelte line 811-830 (shared with
regular ProjectNode when collapsed):

```ts
const MIN_WIDTH = 200;
const CHAR_WIDTH = 6.5;   // px per char at text-[10px]
const PADDING = 60;       // 2 × 12px handle + gaps + px padding
const GAP = 20;           // minimum gap between input and output labels

const inputNames = inputs.map(p => p.name + (p.required ? '*' : ''));
const outputNames = outputs.map(p => p.name);

let maxRowWidth = 0;
for (let i = 0; i < Math.max(inputNames.length, outputNames.length); i++) {
  const leftLen = i < inputNames.length ? inputNames[i].length : 0;
  const rightLen = i < outputNames.length ? outputNames[i].length : 0;
  const rowWidth = (leftLen + rightLen) * CHAR_WIDTH + GAP;
  if (rowWidth > maxRowWidth) maxRowWidth = rowWidth;
}

return Math.max(MIN_WIDTH, Math.ceil(maxRowWidth + PADDING));
```

Port this verbatim — it's what makes collapsed groups size to
their port labels instead of flopping between 160px and 400px.

### Port layout (lines 431-522)

Same shape as ProjectNode's port rows: `flex justify-between`,
`text-[10px] text-zinc-500`. Each input row:
- Left-side Handle (target, position Left, id=port name)
- truncated name
- group-hover delete button

Each output row mirror.

The `[+]` add-port buttons appear at the bottom of each column.

## Group id semantics

- Top-level group: `id = "MyGroup"` (usually equal to label).
- Nested group: `id = "Outer.Inner"`. Parser `.`-joins the scope
  chain (weft-parser.ts:1806-1808).
- Child of a group: `id = "Outer.nodeInstance"`. Same scoping.

`data.label` is the **original local name** (line 4389 in parser:
`label: group.originalName || group.id`). So for a nested group
`Outer.Inner`, label="Inner", id="Outer.Inner". This is why
`weftUpdateGroupPorts` uses `node.data.label` (not `node.id`) to
find the block in the source.

## v2 Port Plan

v2 has `project.groups: GroupDefinition[]` from the compiler
(added in previous commit). The Graph.svelte layer needs to:

1. **Hide boundary passthroughs from rendering.** For every group
   `g` in `project.groups`, remove nodes with id matching
   `{g.id}__in` or `{g.id}__out` from the rendered array.

2. **Synthesize a virtual group NodeInstance** with:
   - id = `g.id`
   - type = `g.config.expanded !== false ? 'weftGroup' : 'weftGroupCollapsed'`
   - inputs = `g.inPorts`, outputs = `g.outPorts`
   - features = `{ oneOfRequired: g.oneOfRequired }`
   - label = `g.label ?? g.id`
   - scope = computed from `g.parentGroupId` chain
   - parentId = `g.parentGroupId`
   - config includes `expanded`, `width`, `height`, `description`,
     `parentId` (wired at render time by the visibility walker).

3. **Rewrite edges touching passthroughs.**
   - Edge `source = g.id__in` → `source = g.id`, `sourceHandle += __inner` if it was bare (because that's the internal source side of an in-port).
   - Edge `target = g.id__out` → `target = g.id`, `targetHandle += __inner` if it was bare.
   - Edge `source = external` with `target = g.id__in` → rewrite
     target to `g.id`, handle stays bare (external in-port).
   - Edge `source = g.id__out` with `target = external` → rewrite
     source to `g.id`, handle stays bare (external out-port).
   - Walk all project.edges, apply rules. Collapse duplicates.

4. **Assign children `parentId`** when group is expanded. Every
   node whose `scope[0] === g.id` becomes a child of g (direct
   child; deeper-nested groups are captured by their own Group's
   visibility walk).

5. **Topo-sort**: xyflow requires parent nodes to appear in the
   array BEFORE their children. Sort groups by depth (shallowest
   first), push them first, then non-group nodes.

6. **Z-index**: expanded groups `-1 + nestingDepth`, collapsed
   groups 4, regular nodes 4, annotations -1. Matches v1
   ProjectEditorInner line 903.

7. **Visibility walk on expand/collapse**: when a group toggles,
   walk descendants. If any ancestor in the chain is collapsed, set
   `parentId = undefined` + `style = 'display: none;'`. Edges
   touching hidden nodes get `hidden: true`.

8. **Viewport anchoring**: on toggle, capture the group's
   top-right corner in flow coordinates, run ELK, compute new
   top-right, offset viewport by delta so the cursor stays over
   the toggle button. Matches ProjectEditorInner line 679-731.

## Deferred to later sections

- Scope-lock drag (`scope-lock.md`).
- ELK per-scope layout and bottom-up resolution (`layout.md`).
- Execution tracking through groups (`execution.md`) — when a group
  boundary passthrough runs in v2, the NodeExecution should
  surface on the virtual group node's `executions` array, so the
  ExecutionInspector shows group-level input/output.

## v2 port status

Not started. The parity spec is the input for the v2 port.
