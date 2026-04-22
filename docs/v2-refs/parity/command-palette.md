# CommandPalette Parity

**v1 source**: `dashboard-v1/src/lib/components/project/CommandPalette.svelte`
(480 lines).

## What it owns

Two panels side by side, centered at `top: 15%`:

1. **Main panel** (420px): search input at top, scrollable result
   list. Actions section first (when no search), then nodes
   grouped by category.
2. **Preview panel** (256px, `self-start`): shown when a NODE is
   highlighted. Describes the node's icon, label, type, tags,
   inputs, outputs.

## Props

```ts
{
  open: $bindable(false);
  onAddNode: (type: NodeType) => void;
  onAction?: (action: string) => void;
  playground?: boolean;  // hides cloud-only actions when true
}
```

## Actions (line 109-122)

```ts
{ id: 'save', label: 'Save Project', icon: Save, shortcut: 'Ctrl+S' },
{ id: 'run', label: 'Run Project', icon: Play, shortcut: 'Ctrl+Enter' },
{ id: 'export_json', label: 'Export as JSON', icon: Upload },
{ id: 'export_weft', label: 'Export as Weft', icon: Upload },
{ id: 'import', label: 'Import from JSON/Weft', icon: Download },
{ id: 'undo', label: 'Undo', icon: Undo2, shortcut: 'Ctrl+Z' },
{ id: 'redo', label: 'Redo', icon: Redo2, shortcut: 'Ctrl+Shift+Z' },
{ id: 'duplicate', label: 'Duplicate Selected', icon: Copy, shortcut: 'Ctrl+D' },
{ id: 'delete', label: 'Delete Selected', icon: Trash2, shortcut: 'Del' },
{ id: 'selectAll', label: 'Select All Nodes', icon: CheckSquare, shortcut: 'Ctrl+A' },
{ id: 'fitView', label: 'Fit View', icon: Maximize2 },
{ id: 'autoOrganize', label: 'Auto Organize Layout', icon: LayoutDashboard },
```

Playground hides `['save', 'run', 'export_json', 'export_weft', 'import']`.

## Category icons (line 71-79)

```ts
{
  Triggers: { icon: Zap, order: 0 },
  AI: { icon: BrainCircuit, order: 1 },
  Data: { icon: ChartBar, order: 2 },
  Flow: { icon: GitFork, order: 3 },
  Infrastructure: { icon: Server, order: 4 },
  Utility: { icon: Wrench, order: 5 },
  Debug: { icon: Bug, order: 6 },
}
```

Categories derived from `ALL_NODES` filtered by
`!node.features?.hidden`. Sorted by `CATEGORY_CONFIG[name].order`.

## Ranking algorithm (line 127-141)

`scoreNode(config, query)` returns a numeric score, **lower is
better**, `-1` means no match:

```
0: exact label match
1: label starts with query
2: any word in label starts with query
3: label contains query
4: any tag matches
5: description contains query
-1: no match
```

Actions: scored as `label.startsWith ? 1 : label.includes ? 3 : 5`,
plus `+0.5` to rank below nodes at the same tier.

Results sorted by score ascending.

## No-search view

Actions section first (flat list). Then each category with its
icon header. No preview in this view until user arrows / hovers
to highlight a specific item.

## Keyboard (line 224-310)

Palette-local:
- Escape: close.
- ArrowDown / ArrowUp: move `selectedIndex` with scroll-into-view.
- Enter: `selectItem(filteredItems[selectedIndex])`.

Global (capture-phase handler, line 252-310):
- **Ctrl+P**: toggle open (works regardless of focus).
- When palette is closed AND focus is outside an editable element:
  - Ctrl+S → `onAction('save')`
  - Ctrl+Z → `onAction('undo')`
  - Ctrl+Shift+Z → `onAction('redo')`
  - Ctrl+A → `onAction('selectAll')`
  - Ctrl+D → `onAction('duplicate')`
  - Ctrl+Enter → `onAction('run')`
  - Delete (no modifier) → `onAction('delete')`

`isEditableElement` check skips: INPUT, TEXTAREA, contentEditable,
or `.edit-textarea` / `.annotation-node.editing` closest ancestor.

## Favorites / recents (line 49-68)

`localStorage` keys:
- `STORAGE_KEYS.nodeFavorites`: array of NodeType pinned by user
  (not currently rendered anywhere visible).
- `STORAGE_KEYS.nodeRecents`: last 5 selected, updated on every
  selection (`addToRecents`).

Reserved for future UI. Not displayed in v1's default palette.

## Visual styling

### Backdrop (line 316-320)
`fixed inset-0 z-[100] bg-black/50` + onclick closes.

### Main panel (line 329)
`w-[420px] bg-popover border rounded-xl shadow-2xl overflow-hidden`.

### Search row (line 331-341)
`flex items-center border-b px-3`.
Input: `flex-1 py-3 bg-transparent outline-none text-sm` with
placeholder "Search nodes and actions...".
Esc kbd: `text-xs text-muted-foreground bg-muted px-1.5 py-0.5 rounded`.

### Result list (line 344)
`max-h-96 overflow-y-auto p-2`.

### Result item (line 355-366, 371-380, 389-400, 411-419)
```
w-full flex items-center gap-2 px-3 py-2 rounded-lg text-sm text-left transition-colors
{selected? 'bg-accent text-accent-foreground' : 'hover:bg-muted'}
```
- Leading icon (16px).
- Flex-1 label.
- Trailing kbd (actions) or category name (search view).
- `data-selected={itemIndex === selectedIndex}` so
  `scrollSelectedIntoView` can query.

### Preview panel (line 430-477)
`w-64 bg-popover border rounded-xl shadow-2xl p-4 self-start`.
- Header: 24px icon + label + type string.
- Description paragraph.
- Inputs section: `text-xs font-medium text-green-600` label.
  Input chips: `text-xs px-1.5 py-0.5 bg-green-100 text-green-700 rounded`.
- Outputs section: blue variant.
- Tags section: zinc variant.

## v2 port status

Ported. Actions list covers save/run/export_json/export_weft/import/
undo/redo/duplicate/delete/selectAll/fitView/autoOrganize with their
v1 icons and keyboard hints. `data-selected` attribute on the
highlighted row drives scroll-into-view via a $effect. Preview panel
reads `catalog.inputs/outputs/tags` and renders the same
green/blue/zinc chip triplet. `playground` prop hides cloud-only
actions (save/run/export/import).

Global keyboard shortcuts (Ctrl+P/D, Delete) live in Graph.svelte's
onHotkey handler — palette-specific shortcuts (ArrowUp/Down, Enter,
Escape) stay local to the palette. autoOrganize wires to
runAutoLayout; fitView fires a window resize event.

## Deferred

- Favorites/recents UI (never visible in v1 default either).
- Export/Import JSON: not in v2 extension yet.
- Dialog-based import: playground-only.
