# Colors Parity

**v1 source**: `dashboard-v1/src/lib/constants/colors.ts` (41 lines).

```ts
export const PORT_TYPE_COLORS: Record<string, string> = {
  String:       '#6b7280',   // neutral gray
  Number:       '#5a9eb8',   // cyan
  Boolean:      '#b05574',   // pink
  Null:         '#a1a1aa',   // zinc-400 muted
  Image:        '#c4a35a',   // warm gold
  Video:        '#8b6fc0',   // rich purple
  Audio:        '#4a9e6f',   // forest green
  Document:     '#9e7c5a',   // warm brown
  List:         '#5a8a8a',   // teal
  Dict:         '#7c6f9f',   // purple
  TypeVar:      '#6366f1',   // indigo
  MustOverride: '#ef4444',   // red (needs attention)
};

const FALLBACK_COLOR = '#52525b';  // dark gray

export function getPortTypeColor(portType: string): string {
  if (!portType) return FALLBACK_COLOR;
  if (PORT_TYPE_COLORS[portType]) return PORT_TYPE_COLORS[portType];
  if (portType === 'Media') return PORT_TYPE_COLORS.Image;
  const parsed = parseWeftType(portType);
  return parsed ? colorForParsed(parsed) : FALLBACK_COLOR;
}
```

`colorForParsed(parsed)`:
```
primitive:      PORT_TYPE_COLORS[value] ?? FALLBACK
list:           PORT_TYPE_COLORS.List
dict:           PORT_TYPE_COLORS.Dict
json_dict:      PORT_TYPE_COLORS.Dict
union:          colorForParsed(first element)
typevar:        PORT_TYPE_COLORS.TypeVar
must_override:  PORT_TYPE_COLORS.MustOverride
```

`Media` is a special alias for `Image | Video | Audio | Document`
(union); this helper resolves it to the Image color directly for
speed.

## Status colors (not in colors.ts, but related; utils/status.ts)

```ts
getStatusStyle('completed')       → bg-emerald-500/10 text-emerald-600 border-emerald-500/20
getStatusStyle('running')         → bg-blue-500/10 text-blue-600 border-blue-500/20
getStatusStyle('waiting_for_input') → bg-purple-500/10 text-purple-600 border-purple-500/20
getStatusStyle('failed')          → bg-red-500/10 text-red-600 border-red-500/20
getStatusStyle('cancelled')       → bg-orange-500/10 text-orange-600 border-orange-500/20
getStatusStyle('pending')         → bg-slate-500/10 text-slate-500 border-slate-500/20
getStatusStyle(*)                 → bg-zinc-100 text-zinc-500 border-zinc-200
```

## Node category colors (not here; lives in each node's
NODE_TYPE_CONFIG entry)

v1 colors nodes per-type (each NODE_TYPE_CONFIG has its own color).
v2 reads this from `catalog.color` in the metadata.json per node.
We already color-coded the stdlib in a previous commit.

## v2 port status

- `colors.ts` + `weft-type.ts` already ported verbatim.
- Status styles live in `utils/status.ts`, ported.
- Node colors in metadata.json for each stdlib node, done.
- Media alias: handled.

No further work needed here.
