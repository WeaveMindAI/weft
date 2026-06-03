// The companion `.layout` file: node positions + sizes + collapse state.
//
// Layout is NOT part of the `.weft` language. It lives in a sibling file so the
// source stays clean, and it's a frontend concern (where boxes sit on a
// canvas), so it stays in the webview rather than going through the Rust
// edit-server. Format, one entry per line:
//   scopedId @layout x y [WxH] [expanded|collapsed]

export interface LayoutEntry {
  x: number;
  y: number;
  w?: number;
  h?: number;
  expanded?: boolean;
}

/** Parse layoutCode into a map of scoped id -> entry. */
export function parseLayoutCode(layoutCode: string): Record<string, LayoutEntry> {
  const map: Record<string, LayoutEntry> = {};
  if (!layoutCode) return map;
  for (const line of layoutCode.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    const match = trimmed.match(/^(.+?)\s+@layout\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)(?:\s+(\d+(?:\.\d+)?)x(\d+(?:\.\d+)?))?(?:\s+(collapsed|expanded))?\s*$/);
    if (!match) continue;
    const [, scopedId, xStr, yStr, wStr, hStr, state] = match;
    const entry: LayoutEntry = { x: parseFloat(xStr), y: parseFloat(yStr) };
    if (wStr && hStr) {
      entry.w = parseFloat(wStr);
      entry.h = parseFloat(hStr);
    }
    if (state === 'expanded') entry.expanded = true;
    if (state === 'collapsed') entry.expanded = false;
    map[scopedId] = entry;
  }
  return map;
}

/** Update or insert a layout entry. Returns the new layoutCode.
 *
 *  `undefined` for `w`/`h`/`expanded` means "leave whatever is already
 *  persisted", NOT "clear it". This matters because position-only updates (a
 *  drag, an ELK reflow moving a NEIGHBOUR node) call this without knowing the
 *  node's size/collapse state; a destructive rewrite would strip the persisted
 *  `expanded` flag and the node would snap to its type default on the next
 *  rebuild (spurious collapse/expand on untouched nodes). To actually clear a
 *  flag, pass `null`. */
export function updateLayoutEntry(
  layoutCode: string,
  scopedId: string,
  x: number,
  y: number,
  w?: number,
  h?: number,
  expanded?: boolean | null,
): string {
  const lines = (layoutCode || '').split('\n');
  const idx = lines.findIndex((l) => {
    const t = l.trim();
    return t.startsWith(scopedId + ' @layout') || t.startsWith(scopedId + '\t@layout');
  });
  // Merge against the existing entry so undefined args preserve prior values.
  const prior = idx >= 0 ? parseLayoutCode(lines[idx])[scopedId] : undefined;
  const mergedW = w !== undefined ? w : prior?.w;
  const mergedH = h !== undefined ? h : prior?.h;
  const mergedExpanded = expanded !== undefined ? expanded : prior?.expanded;
  const newLine = `${scopedId} ${formatLayoutStr(x, y, mergedW, mergedH, mergedExpanded)}`;
  if (idx >= 0) lines[idx] = newLine;
  else lines.push(newLine);
  return lines.filter((l) => l.trim() !== '').join('\n');
}

/** Remove a layout entry. Returns the new layoutCode. */
export function removeLayoutEntry(layoutCode: string, scopedId: string): string {
  if (!layoutCode) return '';
  return layoutCode
    .split('\n')
    .filter((l) => {
      const t = l.trim();
      return !(t.startsWith(scopedId + ' @layout') || t.startsWith(scopedId + '\t@layout'));
    })
    .join('\n');
}

function formatLayoutStr(x: number, y: number, w?: number, h?: number, expanded?: boolean | null): string {
  let s = `@layout ${Math.round(x)} ${Math.round(y)}`;
  if (w !== undefined && h !== undefined) s += ` ${Math.round(w)}x${Math.round(h)}`;
  if (expanded === true) s += ' expanded';
  if (expanded === false) s += ' collapsed';
  return s;
}

/** Re-key a whole subtree's layout entries when its scoped address changes (a
 *  move that reparents `oldKey` to `newKey`, or a group rename). Operates on the
 *  PARSED entry map, not a text regex: the moved node is `oldKey`, its
 *  descendants are `oldKey + '.' + rest`, and each becomes `newKey[/.rest]`.
 *
 *  Parsing to a map first is what makes this exact and non-compounding: a regex
 *  prefix-sweep over raw text could match an already-rewritten line and stack
 *  the prefix again (`A.B.A.B...`). Here every source entry is rewritten exactly
 *  once from its parsed key. Entries outside the subtree are untouched. */
export function renameLayoutSubtree(layoutCode: string, oldKey: string, newKey: string): string {
  if (!layoutCode || oldKey === newKey) return layoutCode;
  const map = parseLayoutCode(layoutCode);
  let code = layoutCode;
  for (const [key, entry] of Object.entries(map)) {
    let nextKey: string | null = null;
    if (key === oldKey) nextKey = newKey;
    else if (key.startsWith(oldKey + '.')) nextKey = newKey + key.slice(oldKey.length);
    if (nextKey === null) continue;
    code = removeLayoutEntry(code, key);
    code = updateLayoutEntry(code, nextKey, entry.x, entry.y, entry.w, entry.h, entry.expanded ?? null);
  }
  return code;
}

// ── Reversible layout edits ──────────────────────────────────────────────
//
// Layout mutations are SEMANTIC ops (not text patches) so they're reversible
// AND storage-agnostic: a `setEntry`/`removeEntry`/`renamePrefix` maps to a
// `.layout` file line today and to a DB row upsert/delete/prefix-update on a
// hosted backend, with the same action model. This is the layout half of the
// editor's undo stack (the source half is a `TextEdit`, because source is a
// text document where byte-exactness matters; layout is per-node view state
// where structure matters). Layout is TS-owned: Rust never sees these ops.

export type LayoutOp =
  | { op: 'setEntry'; id: string; entry: LayoutEntry }
  | { op: 'removeEntry'; id: string };

/** Apply a layout op batch to `layoutCode`, returning the new layoutCode. */
export function applyLayoutOps(layoutCode: string, ops: LayoutOp[]): string {
  let code = layoutCode;
  for (const op of ops) {
    if (op.op === 'setEntry') {
      const e = op.entry;
      code = updateLayoutEntry(code, op.id, e.x, e.y, e.w, e.h, e.expanded ?? null);
    } else {
      code = removeLayoutEntry(code, op.id);
    }
  }
  return code;
}

/** The ops that transform `from` into `to` (a layout diff). This is how the
 *  editor captures a reversible layout action: after a mutation, the UNDO ops
 *  are `diffLayoutOps(after, before)`. One helper for recording (after vs
 *  before) and for replay (applyLayoutOps then diff the result back), so there's
 *  a single source of truth for layout reversibility. A `renamePrefix` shows up
 *  as remove+set of the affected entries, which replays identically. */
export function diffLayoutOps(from: string, to: string): LayoutOp[] {
  const a = parseLayoutCode(from);
  const b = parseLayoutCode(to);
  const ops: LayoutOp[] = [];
  for (const [id, entry] of Object.entries(b)) {
    const prior = a[id];
    if (!prior || !sameEntry(prior, entry)) ops.push({ op: 'setEntry', id, entry });
  }
  for (const id of Object.keys(a)) {
    if (!(id in b)) ops.push({ op: 'removeEntry', id });
  }
  return ops;
}

function sameEntry(a: LayoutEntry, b: LayoutEntry): boolean {
  return a.x === b.x && a.y === b.y && a.w === b.w && a.h === b.h && a.expanded === b.expanded;
}
