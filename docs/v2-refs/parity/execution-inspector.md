# ExecutionInspector + JsonTree Parity

**v1 source**:
- `dashboard-v1/src/lib/components/project/ExecutionInspector.svelte` (169 lines)
- `dashboard-v1/src/lib/components/project/JsonTree.svelte` (73 lines)

See `execution.md` for the underlying NodeExecution data model +
how Group execs are synthesized from `__in`/`__out` passthroughs.

## Inline controls in the node header (line 43-60)

Rendered INSIDE the node's header via `<ExecutionInspector
{executions} label={data.label || typeConfig.label} />`. Two
pieces:

### Navigator (only when `count > 1`)

```svelte
{@const statusColor = selExec?.status === 'failed' ? 'text-red-500'
  : selExec?.status === 'completed' ? 'text-green-600'
  : selExec?.status === 'running' ? 'text-blue-500'
  : 'text-muted-foreground'}
<div class="inline-flex items-center gap-0.5 ml-1.5 text-[9px] select-none {statusColor}">
  <button disabled={selectedIndex === 0} ...>‹</button>
  <span class="font-mono tabular-nums">{selectedIndex + 1}/{count}</span>
  <button disabled={selectedIndex >= count - 1} ...>›</button>
</div>
```

Updates `selectedIndex`. Every button click:
`e.stopPropagation()` so xyflow doesn't interpret it as a node
drag. Visible color tracks the latest exec's status.

### Inspect button (when `count > 0`)

```svelte
<button class="w-5 h-5 flex items-center justify-center rounded hover:bg-black/5 cursor-pointer transition-colors text-zinc-400 nodrag"
  onclick={(e) => { e.stopPropagation(); open = true; }}
  title="Inspect execution"
>
  <Search class="w-3 h-3" />
</button>
```

Toggles `open`. `.nodrag` class prevents xyflow drag.

## Modal dialog (line 73-168)

v1 uses `$lib/components/ui/dialog` (bits-ui based). `bind:open`,
`sm:max-w-[92vw] max-h-[85vh] overflow-hidden p-0 gap-0` content
class. Styles inside:

### Header (line 75-94)

- Left: status icon (color-coded) + label + navigator (if count > 1).
- Right: `CopyButton` for full text + close ✕.
- `px-4 py-2.5 border-b border-zinc-200 shrink-0`.

### 3-column body (line 96-157)

`grid grid-cols-3 min-h-0 overflow-hidden` with
`style="height: calc(85vh - 80px)"`.

**Column 1: INPUT** (line 97-113)
- Header: `flex items-center justify-between px-3 py-1.5 bg-zinc-50
  border-b border-zinc-200 shrink-0`, label `INPUT` in zinc-400
  uppercase tracking.
- Body: scrollable, `p-2`. If `selected.input` is an object with
  keys: one `<JsonTree data={value} label={key} defaultExpanded>`
  per entry. Else: `<div class="p-1 text-xs text-zinc-400 italic">No input data</div>`.

**Column 2: DETAILS** (line 115-136)
- Error box (red border, bg): `rounded border border-red-200 bg-red-50 p-2.5` with an "Error" heading + `<pre>` of the message.
- Completed: `text-[11px] text-green-600` "Completed successfully".
- Running: `animate-pulse text-[11px] text-blue-600` "Running...".
- Waiting for input: `animate-pulse text-[11px] text-blue-600` "Waiting for input...".
- Skipped: `text-[11px] text-zinc-500` "Skipped (null input on required port)".

**Column 3: OUTPUT** (line 138-157)
- Same shape as input: one JsonTree per top-level key, or fallback
  strings for null / primitive outputs.

Each column header has its own CopyButton that copies that
column's JSON.

### Footer (line 159-166)

```svelte
<div class="flex items-center gap-4 px-4 py-1.5 border-t border-zinc-200 bg-zinc-50 text-[10px] text-zinc-500 shrink-0">
  <span class="font-medium {statusColor}">{selected.status}</span>
  <span class="font-mono">{formatDuration(selected.startedAt, selected.completedAt)}</span>
  {#if selected.costUsd > 0}
    <span class="font-mono">{formatCost(selected.costUsd)}</span>
  {/if}
  <span>{new Date(selected.startedAt).toLocaleString()}</span>
</div>
```

## Duration + cost formatters

**formatDuration(startMs, endMs)** (line 26-32):
- Missing endMs → `"running..."`
- ms < 1000 → `"{ms}ms"`
- ms < 60000 → `"{s}s"` 1 decimal
- else → `"{m}m {s}s"`

**formatCost(usd)** (line 34-39):
- 0 → `"$0"`
- < 0.001 → `"$0.000000"` (6 decimals)
- < 0.01 → `"$0.0000"` (4 decimals)
- else → `"$0.00"` (2 decimals)

## Full-text copy

One button copies everything:

```
--- Input ---
{inputJson or (none)}

--- Details ---
{error or status}

--- Output ---
{outputJson or (none)}

Status: {s} | Duration: {d}[ | Cost: {c}] | {localTimestamp} | {executionId}
```

## Selection reset effect (line 22-23)

```ts
$effect(() => {
  if (count > 0) selectedIndex = count - 1;
});
```

When a new execution is appended, jump to it.

## JsonTree (file: JsonTree.svelte)

Recursive renderer with collapse toggles. I already ported this
correctly; spec is in my group-node commit. Key shape:

```ts
{ data: unknown, label?: string, depth?: number, defaultExpanded?: boolean }
```

- `isObject`, `isArray`, `isExpandable` derived from `data`.
- Preview: `[n]` for arrays, `{n}` for objects.
- Expanded: chevron svg rotates 90°, entries render as
  `<JsonTree>` for nested objects, inline row for primitives.
- Leaf colors:
  - null: `text-zinc-400 italic`
  - string: `text-green-700`
  - number: `text-blue-700`
  - boolean: `text-amber-700`
  - other: `text-zinc-700`
- `formatValue(val)`:
  - null → `'null'`
  - string len > 120 → `'"...(first 120)..."'`
  - else → `JSON.stringify`-like.

## v2 port plan

### v2 port status

Ported. `executions: NodeExecution[]` + selectedIndex paginator in
the node header (‹ N/M ›) + magnifier button. The modal reproduces
the 3-column layout (Input / Details / Output), each column with a
per-column copy button plus a full-text copy in the header. Status-
specific Details text covers running / waiting_for_input / completed
/ skipped / cancelled / failed with matching colors and pulse
animation. Footer shows status + formatted duration + cost (hidden
if 0) + local timestamp, via the shared `utils/status.ts` helpers.

### Divergences
- v1 uses bits-ui `<Dialog.Root>`; v2 uses a plain `fixed inset-0`
  modal. Same visual behavior, no extra dependency.
- v1's `CopyButton` is a small component; v2 inlines a button that
  calls `navigator.clipboard.writeText`.
