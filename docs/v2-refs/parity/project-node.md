# ProjectNode Parity

**v1 source**: `dashboard-v1/src/lib/components/project/ProjectNode.svelte`
(1222 lines).

## Data prop shape

```ts
data: {
  label: string | null;
  nodeType: NodeType;
  config: Record<string, unknown>;
  inputs?: PortDefinition[];
  outputs?: PortDefinition[];
  features?: NodeFeatures;
  onUpdate?: (updates: NodeDataUpdates) => void;
  infraNodeStatus?: string;        // live infra badge
  debugData?: unknown;             // for Debug node
  executions?: NodeExecution[];    // history, not just latest
  executionCount?: number;
  liveDataItems?: LiveDataItem[];  // text/image/progress streams
}
```

## Global layout (line 574-1097)

Top-down:

1. **NodeResizer** (conditional; shows only when expanded AND
   selected). minWidth=200, minHeight=`minResizeHeight` formula
   (line 297):
   ```
   2 + 32 + 16 + 24 + 8 + max(inputs, outputs) * 25 + 80
   ```
2. **Outer container** (line 576-589):
   - CSS classes: `project-node rounded min-w-[200px] select-none
     transition-all duration-200`
   - Status glow classes (see below)
   - Inline style: `width: 100%; height: 100%; display: flex;
     flex-direction: column; overflow: hidden; background:
     rgba(255, 255, 255, 0.95); border: 1px solid {selected ?
     typeConfig.color : 'rgba(0,0,0,0.08)'}; box-shadow: 0 1px 3px
     rgba(0,0,0,0.08), 0 4px 12px rgba(0,0,0,0.05){selected ? ','
     + typeConfig.color + '20' : ''}; backdrop-filter: blur(8px);`
3. **Accent bar** (line 592-595): `h-0.5 rounded-t` with
   `background: typeConfig.color`.
4. **Header row** (line 598-639):
   - `px-3 py-2 flex items-center justify-between border-b border-black/5`
   - Left: status icon (animate-pulse when running/waiting) +
     uppercase type label + infra status badge (if any).
   - Right: `<ExecutionInspector>` + expand toggle (Minimize2 /
     Maximize2 icon).
5. **Body wrapper** (line 641): `px-3 py-2 flex-1 overflow-hidden
   min-h-0 nodrag nopan flex flex-col`. Contains label + ports +
   live data + (expanded) config fields.
6. **Label row** (line 643-660):
   - Editing mode: `<input class="w-full text-sm font-medium
     bg-zinc-100 text-zinc-900 px-2 py-1 rounded border border-
     zinc-200 outline-none focus:border-zinc-400">`.
   - Display mode: `<p class="text-sm font-medium text-zinc-800
     cursor-text hover:bg-black/5 px-1 py-0.5 rounded -mx-1
     truncate">`, `ondblclick={startEditLabel}`, placeholder `{typeConfig.label} Node`.
   - Enter commits; Escape cancels.
7. **Port rows** (line 663-776): see below.
8. **Live data** (line 779-805): rendered regardless of
   `expanded`. See below.
9. **Expanded block** (line 808-1095): Setup Guide, Run Location
   selector, then per-field editor. Each field has its own
   rendering branch by `field.type`.
10. **Raw output handle** (line 1099-1122): positioned absolutely
    at top: 18px, right edge. 10x10 SVG square, filled black when
    connected, white otherwise. `Handle id="_raw"`.
11. **Port context menu** lives on document.body via `$effect`
    (line 209-234).

## Status glow classes

Applied to the outer container based on `displayedStatus`:

```
node-running-glow     ← running OR waiting_for_input
node-completed-glow   ← completed
node-failed-glow      ← failed
(none)                ← skipped / cancelled / pending / idle
```

Defined in `:global` styles (line 1133-1141):
```css
:global(.node-running-glow) { box-shadow: 0 1px 3px rgba(0,0,0,0.08), 0 4px 12px rgba(0,0,0,0.05), 0 0 0 2px rgba(245,158,11,0.4) !important; }
:global(.node-completed-glow) { box-shadow: 0 1px 3px rgba(0,0,0,0.08), 0 4px 12px rgba(0,0,0,0.05), 0 0 0 2px rgba(16,185,129,0.3) !important; }
:global(.node-failed-glow) { box-shadow: 0 1px 3px rgba(0,0,0,0.08), 0 4px 12px rgba(0,0,0,0.05), 0 0 0 2px rgba(239,68,68,0.4) !important; }
```

## Port rows (line 663-776)

`flex justify-between text-[10px] text-zinc-500 w-full`. Two
columns:

**Inputs column** (line 665-719):
- `space-y-1 min-w-0 flex-1`.
- Per port: `relative flex items-center gap-1 group pl-3`.
- Right-click opens port context menu.
- `<Handle type="target" position=Left id={input.name}
  style="top: 50%; {pMarker.style}" class={pMarker.class}>`.
- `{input.name}` label with title showing @require_one_of info.
- If `canAddInputPorts`: delete × button (opacity-0
  group-hover:opacity-100).
- After the list: add-port UI (text input or `+ input` button).

**Outputs column** (line 722-775):
- `space-y-1 text-right flex flex-col items-end min-w-0 flex-1`.
- Mirror layout; label on right, Handle on right.
- Same right-click menu / add-port.

`_raw` does NOT appear in the output list. It's absolutely
positioned in top-right corner (line 1099-1122).

## Field rendering branches (line 846-1062)

Each field in `displayedFields` renders a `<div class="space-y-1">`
with `<label>` + input. Branches by `field.type`:

### `code` (line 851-865)
```svelte
<div class="nodrag nopan" onclick={stopProp}
  onfocusin={add .nowheel}
  onfocusout={remove .nowheel}>
  <CodeEditor
    value={config[key]}
    placeholder={field.placeholder}
    minHeight="120px"
    onchange={(v) => updateConfig(key, v)}
  />
</div>
```

`CodeEditor` is a separate component (CodeMirror-based, python
syntax). Important details:
- wraps in `nodrag nopan` so xyflow doesn't capture clicks.
- adds `nowheel` on focus so scroll-in-editor doesn't pan the
  canvas.
- minHeight 120px.

### `textarea` (line 866-881)
```svelte
<textarea
  id={...}
  class="text-xs bg-muted px-2 py-1.5 rounded border-none outline-none font-mono nodrag nopan box-border block w-full"
  onfocusin={add .nowheel}
  onfocusout={remove .nowheel}
  style="resize: vertical; min-height: 60px; {textareaHeights[key] ? 'height: ...px;' : ''}"
  value={getConfigDisplayValue(key)}     ← debounced via fieldEditor
  onfocus={fieldEditor.focus}
  oninput={fieldEditor.input}
  onblur={fieldEditor.blur}
  use:observeTextareaResize={field.key}   ← ResizeObserver persists height
/>
```

Height is persisted in `config.textareaHeights: Record<string, number>`.
Only saved when height ≥ 60px (line 271). `observeTextareaResize`
is a Svelte action that attaches a ResizeObserver and calls
`handleTextareaResize` with the new height.

### `select` (line 882-893)
`<select>` with `field.options` array. Single value.

### `multiselect` (line 894-914)
Chip grid. Each option is a button; clicked toggles in
`config[key]: string[]`. Selected = `bg-primary text-primary-foreground`.

### `checkbox` (line 915-925)
```svelte
<label class="flex items-center gap-2 cursor-pointer">
  <input type="checkbox" class="w-4 h-4 rounded border-muted-foreground/30"
         checked={config[key] === true}
         onchange={...} />
  <span class="text-xs text-muted-foreground">{field.description || field.label}</span>
</label>
```

### `api_key` (line 926-956)
Toggle between Credits (emerald) and Own key (blue). Sentinel
value `__BYOK__` means "using own key but empty". Shows a password
input below the toggle when BYOK is active.

### `password` (line 957-968)
`<input type="password">` with the field-editor debounce pattern.

### `form_builder` (line 969-1039)
Renders current form fields as rows (fieldType + key + × remove
button). "+ Add field" button opens an inline form:
- fieldType dropdown (from `nodeFormFieldSpecs`)
- key input (sanitized with `.replace(/\s+/g, '_')`)
- options sub-builder (if the spec's `requiredConfig.includes('options')`)
- Cancel / Add buttons

Key validation: can't be empty. On port-name collision with
existing fields: toast error.

### `blob` (line 1040-1047)
`<BlobField fileRef accept id placeholder onUpdate>` — file
upload component (drag-drop, browse, paste URL). Separate
component.

### default / `text` (line 1048-1060)
Plain `<input type="text">` with field-editor debounce.

## Live data rendering (line 779-805)

**Always visible, regardless of expanded state.** `data.liveDataItems`
is populated by the execution pipeline; each item has `type:
'image' | 'text' | 'progress'`, `label`, `data`.

- Image: `<img src={data} class="w-full rounded border
  border-zinc-200 mt-1">`.
- Text: `<div class="w-full text-[10px] font-mono bg-zinc-100
  rounded px-2 py-1.5 pr-7 break-all border border-zinc-200
  select-text cursor-text">`, with a CopyButton overlayed
  top-right.
- Progress: bar, `w-full h-1.5 bg-zinc-200 rounded-full mt-1
  overflow-hidden`, fill `h-full bg-emerald-500 rounded-full`.

## Debug preview (line 1064-1092)

Only when `typeConfig.features.showDebugPreview` is true (Debug
node). Four states:
- Has data: `<pre class="debug-data-container nodrag nopan nowheel
  select-text cursor-text">{debugDataJson}</pre>` + CopyButton.
- Completed: green placeholder "✓ Execution complete".
- Failed: red placeholder "✗ Execution failed: {error}".
- Running: yellow placeholder with spinner, "Processing...".
- Default: placeholder "📥 Waiting for data...".

`debugDataJson` is `JSON.stringify(stripRawKeys(debugData), null, 2)`.
`stripRawKeys` recursively removes `_raw` keys from objects so
the preview shows just the declared output.

CSS (lines around 1150-1210):
```css
.debug-data-container {
  margin: 0;
  background: #f8fafc;
  border: 1px solid #e2e8f0;
  border-radius: 6px;
  padding: 8px;
  min-height: 60px;
  max-height: 400px;
  overflow: auto;
  font-family: ui-monospace, 'SF Mono', Monaco, monospace;
  font-size: 10px;
  line-height: 1.4;
  white-space: pre-wrap;
  word-break: break-word;
  resize: vertical;
  color: #334155;
}
.debug-placeholder { display: flex; flex-direction: column; align-items: center;
  gap: 4px; padding: 16px 8px; border: 1px dashed #e2e8f0; border-radius: 6px;
  color: #94a3b8; font-size: 11px; }
.debug-placeholder.completed { background: #f0fdf4; border-color: #bbf7d0; color: #22c55e; }
.debug-placeholder.running { background: #fffbeb; border-color: #fde68a; color: #f59e0b; }
.debug-placeholder.waiting { background: #f8fafc; border-color: #e2e8f0; }
.debug-spinner {
  width: 14px; height: 14px;
  border: 2px solid #fde68a;
  border-top-color: #f59e0b;
  border-radius: 50%;
  animation: debug-spin 0.8s linear infinite;
}
@keyframes debug-spin { to { transform: rotate(360deg); } }
```

## `computeMinNodeWidth(inputs, outputs)` — collapsed/default width

Shared helper defined in ProjectEditorInner.svelte:811-830. Used
for collapsed regular nodes AND collapsed groups. See
`group-node.md` for the full formula. Port verbatim so node
widths track their port labels.

## Expand / collapse (line 523-556)

```ts
function toggleExpand(e: MouseEvent) {
  if (!hasExpandableContent) return;
  const currentExpanded = (data.config?.expanded as boolean) ?? false;
  if (data.onUpdate) {
    if (currentExpanded) {
      // Collapsing - preserve current width/height so re-expand restores them
      const currentWidth = nodeElement?.offsetWidth;
      const currentHeight = nodeElement?.offsetHeight;
      const existingWidth = config.width;
      const existingHeight = config.height;
      if (currentWidth && currentHeight && currentWidth > 200) {
        data.onUpdate({
          config: { ...config, expanded: false,
            width: existingWidth || currentWidth,
            height: existingHeight || currentHeight,
          }
        });
      } else {
        data.onUpdate({ config: { ...config, expanded: false } });
      }
    } else {
      data.onUpdate({ config: { ...config, expanded: true } });
    }
  }
}
```

Viewport anchoring on toggle is handled by the parent
(ProjectEditorInner), not ProjectNode. See `project-editor.md`
line 679-731.

## Port context menu (line 209-234)

Fires on right-click on port Handle or row. Renders on
document.body via the shared `createPortContextMenu` helper.
Items built by `buildPortMenuItems`:
- `isCustom` = NOT in `typeConfig.defaultInputs/Outputs`.
- `canAddPorts` = `typeConfig.features.canAddInputPorts` (or
  canAddOutputPorts).

v1 hides the delete × button in ProjectNode rows when the port is
a catalog default (because v1 doesn't allow deleting defaults).
The port menu does the same check internally.

## Add port UI (line 696-718, 752-774)

Input/output add buttons visible only when
`canAddInputPorts`/`canAddOutputPorts` is true. Click → text
input appears. Enter commits, Escape cancels. Duplicate name
rejection via toast: `"Input port \"X\" already exists"`. `_raw`
reserved for output side with its own toast.

## Blur-on-deselect (line 239-246)

```ts
$effect(() => {
  if (!selected && nodeElement) {
    const activeElement = document.activeElement;
    if (activeElement && nodeElement.contains(activeElement)) {
      (activeElement as HTMLElement).blur?.();
    }
  }
});
```

Prevents middle-click paste on Linux when deselecting a node with
focused input.

## v2 port plan

### Already done in my earlier attempt (but needs review against spec)
- Status glow classes (wrong class names in my port — used node-running-glow etc. but v1 sets on outer class, not a CSS :global rule via my app.css; my classes match names, should verify).
- Port rendering with `portMarkerStyle`.
- Expand / collapse toggle.
- Label edit on double-click.
- Add-port UI.
- Port context menu on right-click.
- NodeResizer.

### What my earlier port is missing / wrong
- **executions is an array, not single.** My NodeViewData has
  `exec: { status, input, output, error }`, but v1 has
  `executions: NodeExecution[]`. Header shows paginator `‹ N/M ›`
  when length > 1 (in ExecutionInspector). The latest exec drives
  the glow class. See `execution.md`.
- **`textareaHeights` persistence**. Not in my FieldEditor.
- **Debug preview** with the 4 states and spinner. Not ported.
- **Live data (text/image/progress)**. Not ported.
- **Run Location selector.** Not ported.
- **Setup Guide collapsible.** Not ported.
- **`nowheel` class management** on code/textarea focus for
  wheel containment.
- **`infraNodeStatus` badge** in the header. Deferred (playground
  feature).
- **`form_builder` field kind with nested config options
  sub-builder + port-collision detection**. My port is a stub.
- **`_raw` handle position**. My version is close but v1 wraps
  the Handle around the SVG rect (so the Handle IS the square).
  Review my positioning.
- **CSS for debug-data-container, debug-placeholder, debug-spin
  animation**. Not in my app.css.
- **`cursor-text` on label + preserving focus stability**.

### Divergences to make explicit
- CodeEditor: deferred, replaced by textarea until we integrate
  CodeMirror without exploding bundle size.
- BlobField: deferred to Phase B (cloud upload endpoint).
- Infra status badge: playground only.
- Run Location selector: playground only (v2 cloud feature).

## v2 port status

My current Port is partial. A fresh port following this spec is
required. Order:
1. Accept `executions: NodeExecution[]` in NodeViewData.
2. Wire live data rendering.
3. Wire textareaHeights persistence.
4. Wire debug preview 4-state placeholder + spinner.
5. Wire Setup Guide.
6. CSS global rules matching v1.
7. Verify glow class names + :global rules are correct.

All other pieces already exist but need audit against this doc.
