# Port System Parity

**v1 sources**:
- `dashboard-v1/src/lib/utils/port-marker.ts` (143 lines)
- `dashboard-v1/src/lib/utils/port-context-menu.ts` (109 lines)

## Port marker state machine

Port marker = the small dot/triangle rendered at a Handle
position. Four states, three shapes.

### `PortMarkerState` (4 values)

| state          | meaning                                       |
|----------------|-----------------------------------------------|
| `full`         | required + not satisfied (fully colored fill) |
| `empty`        | optional (outline only)                       |
| `half`         | in a `@require_one_of` group (half-filled)    |
| `empty-dotted` | satisfied from a config literal (dotted outline) |

### `PortMarkerShape` (3 values)

From `port.laneMode`:
- `null / undefined` → `circle`
- `'Gather'` → `gather` (triangle base on left, point on right: `>`)
- `'Expand'` → `expand` (triangle base on right, point on left: `<`)

### `inputMarkerState(required, inOneOfRequired, isConfigFilled)`

Priority order:
1. `isConfigFilled` → `empty-dotted`  ← overrides everything
2. `required` → `full`
3. `inOneOfRequired` → `half`
4. else → `empty`

Output ports are ALWAYS `full`.

### Rendering

**Circles** use CSS on the Handle:
- `full`:
  - side=input: `background-color: {color}; border-color: {color}`
  - side=output: `background-color: {color}; border-color: white`
- `half`: `background: linear-gradient(to right, {color} 50%, white 50%); border-color: {color}`
- `empty-dotted`: `background-color: white; border-color: {color}; border-style: dotted`
- `empty`: `background-color: white; border-color: {color}`

**Triangles** use inline SVG `background-image` data URLs (because
CSS borders can't stroke slanted edges). 12x12 viewBox, 1px inset,
stroke-width 1, stroke-linejoin round. Points:
- gather: `"1,1 11,6 1,11"`
- expand: `"11,1 1,6 11,11"`

For `half`:
- gather: clip-path rect `x=0 y=0 w=6 h=12` (left half)
- expand: clip-path rect `x=6 y=0 w=6 h=12` (right half)

For `empty-dotted`: stroke-dasharray `"1.5 1.2"`.

### Classes

Base: `!w-3 !h-3`.
Circle adds: `!border !rounded-full`.
Triangle: no extra class (SVG background draws the outline).

Extra class param allows callers to add positioning overrides
(e.g. `!relative !inset-auto !transform-none` for GroupNode's
side ports).

## Port context menu

Right-click on a port. Floating menu on `document.body`
(ATTENTION: NOT inside the xyflow node, because xyflow applies
CSS transforms that skew absolute-positioned menus).

### Items (buildPortMenuItems)

```ts
const items = [];
if (side === 'input') {
  items.push({ label: port.required ? '☐ Make optional' : '☑ Make required',
               onClick: onToggleRequired });
}
items.push({ label: `✎ Type: ${port.portType || 'MustOverride'}`,
             onClick: () => {
               const nt = prompt('Enter port type:', port.portType || '');
               if (nt && nt.trim() && nt.trim() !== port.portType) {
                 onSetType(nt.trim());
               }
             }});
if (isCustom && canAddPorts) {
  items.push({ label: 'Remove port', onClick: onRemove, color: '#ef4444' });
}
```

Rules:
- Required toggle only on inputs.
- Type edit always present.
- Remove only when the port is user-added AND the node type
  supports adding ports on this side.

### Menu DOM

Backdrop:
```css
position: fixed;
inset: 0;
z-index: 9998;
```
Clicks on backdrop close. `contextmenu` event on backdrop closes.

Menu:
```css
position: fixed;
left: {x}px;
top: {y}px;
z-index: 9999;
background: white;
border: 1px solid #e4e4e7;
border-radius: 8px;
box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
padding: 4px 0;
min-width: 180px;
```

Button:
```css
width: 100%;
display: flex;
align-items: center;
gap: 8px;
padding: 6px 12px;
font-size: 12px;
text-align: left;
border: none;
background: none;
cursor: pointer;
color: {item.color || '#18181b'};
```

Hover: `background: #f4f4f5`.

### Cleanup function

`createPortContextMenu` returns `() => void` that removes both
backdrop and menu DOM nodes. Svelte `$effect` consumes this:
```ts
$effect(() => {
  if (!portContextMenu) return;
  return createPortContextMenu(x, y, items, () => { portContextMenu = null; });
});
```

## `_raw` synthetic port

Every non-Group node has an implicit `_raw` output. Rendered by
ProjectNode as a 10x10 SVG square in the TOP-RIGHT corner, NOT in
the output list (line 1099-1122). Wraps a Handle (`id="_raw"`).
Fill is black when connected, white otherwise. Allows a single
edge to carry the full output record.

`_raw` as an input port name is RESERVED (v1 rejects with toast
`"_raw" is a reserved port name`).

Groups do NOT have `_raw`.

## v2 port status

- `port-marker.ts` already ported to
  `extension-vscode/src/webview/utils/port-marker.ts`, verbatim.
- `port-context-menu.ts` already ported, verbatim.
- `_raw` handle in my ProjectNode port: needs review. v1 wraps
  `<Handle>` around the SVG (making the Handle the square
  itself). My version has them as siblings. Should consolidate.
