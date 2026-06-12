// The companion `.layout` file: node positions + sizes + collapse state.
//
// Layout is NOT part of the `.weft` language. It lives in a sibling file so the
// source stays clean, and it's a frontend concern (where boxes sit on a
// canvas), so it stays in the webview rather than going through the Rust
// edit-server. Format, one entry per line:
//   scopedId @layout x y [WxH] [expanded|collapsed] [configCollapsed]

export interface LayoutEntry {
  x: number;
  y: number;
  w?: number;
  h?: number;
  expanded?: boolean;
  /// Loop-specific: whether the loop's config strip is collapsed
  /// inside the expanded box. Persists across reloads alongside
  /// `expanded`. Ignored for non-loop containers.
  configCollapsed?: boolean;
}

/** Parse layoutCode into a map of scoped id -> entry. */
export function parseLayoutCode(layoutCode: string): Record<string, LayoutEntry> {
  const map: Record<string, LayoutEntry> = {};
  if (!layoutCode) return map;
  for (const line of layoutCode.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    const match = trimmed.match(/^(.+?)\s+@layout\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)(?:\s+(\d+(?:\.\d+)?)x(\d+(?:\.\d+)?))?(?:\s+(collapsed|expanded))?(?:\s+(configCollapsed))?\s*$/);
    if (!match) continue;
    const [, scopedId, xStr, yStr, wStr, hStr, state, configState] = match;
    const entry: LayoutEntry = { x: parseFloat(xStr), y: parseFloat(yStr) };
    if (wStr && hStr) {
      entry.w = parseFloat(wStr);
      entry.h = parseFloat(hStr);
    }
    if (state === 'expanded') entry.expanded = true;
    if (state === 'collapsed') entry.expanded = false;
    if (configState === 'configCollapsed') entry.configCollapsed = true;
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
  configCollapsed?: boolean | null,
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
  const mergedConfigCollapsed = configCollapsed !== undefined ? configCollapsed : prior?.configCollapsed;
  const newLine = `${scopedId} ${formatLayoutStr(x, y, mergedW, mergedH, mergedExpanded, mergedConfigCollapsed)}`;
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

function formatLayoutStr(x: number, y: number, w?: number, h?: number, expanded?: boolean | null, configCollapsed?: boolean | null): string {
  let s = `@layout ${Math.round(x)} ${Math.round(y)}`;
  if (w !== undefined && h !== undefined) s += ` ${Math.round(w)}x${Math.round(h)}`;
  if (expanded === true) s += ' expanded';
  if (expanded === false) s += ' collapsed';
  if (configCollapsed === true) s += ' configCollapsed';
  return s;
}

/** Serialize a layout map back to layoutCode (one line per entry). Entry order
 *  follows insertion order of the map. The inverse of `parseLayoutCode`. */
export function serializeLayoutMap(map: Record<string, LayoutEntry>): string {
  return Object.entries(map)
    .map(([id, e]) => `${id} ${formatLayoutStr(e.x, e.y, e.w, e.h, e.expanded ?? undefined, e.configCollapsed ?? undefined)}`)
    .join('\n');
}

/** Re-key a whole subtree's layout entries when its scoped address changes (a
 *  move that reparents `oldKey` to `newKey`, or a group rename). The moved node
 *  is `oldKey`, its descendants are `oldKey + '.' + rest`, and each becomes
 *  `newKey[.rest]`. Entries outside the subtree are untouched.
 *
 *  This is a PURE map rebuild: parse once, rewrite each source entry's key
 *  EXACTLY ONCE into a fresh map, serialize once. It never mutates the text
 *  incrementally, so it can't (a) re-match an already-rewritten line and stack
 *  the prefix (`A.B.A.B...`), nor (b) let a rewritten key collide with a
 *  not-yet-processed source line and silently drop/overwrite the wrong entry
 *  (the failure mode of the old remove-then-insert-over-mutating-text loop).
 *
 *  Key collisions: if a rewritten key lands on an existing key, the MOVED-SUBTREE
 *  entry wins (it overwrites). That is correct because layout is view-state
 *  subordinate to source: after the rename the source has exactly one decl at the
 *  new id, so at most one layout entry should describe it; the displaced entry is
 *  obsolete. (A rename/move that would manufacture a real duplicate id is refused
 *  by the Rust edit, which rolls the layout back wholesale, so a collision here is
 *  always either a legitimate overwrite or about to be discarded.) Determinism:
 *  unchanged entries are written first, then the re-keyed ones, so the winner is
 *  always the moved entry regardless of original line order. */
export function renameLayoutSubtree(layoutCode: string, oldKey: string, newKey: string): string {
  if (!layoutCode || oldKey === newKey) return layoutCode;
  const map = parseLayoutCode(layoutCode);
  const rebuilt: Record<string, LayoutEntry> = {};
  const reKeyed: Array<[string, LayoutEntry]> = [];
  for (const [key, entry] of Object.entries(map)) {
    if (key === oldKey) reKeyed.push([newKey, entry]);
    else if (key.startsWith(oldKey + '.')) reKeyed.push([newKey + key.slice(oldKey.length), entry]);
    else rebuilt[key] = entry; // outside the subtree: keep as-is
  }
  // Re-keyed entries last so the moved subtree wins any collision with an entry
  // outside it (the displaced entry's view-state is obsolete after the rename).
  for (const [k, entry] of reKeyed) rebuilt[k] = entry;
  return serializeLayoutMap(rebuilt);
}

// ── Container containment floors ─────────────────────────────────────────
//
// A container's drawn box must visually enclose its children, including
// children that are themselves containers (a Loop inside a Group). Nothing
// in the incremental edit path grows a parent when a large child lands
// inside it (only a full ELK pass recomputes sizes), so the renderer floors
// each expanded container's size by its children's extents, recursively
// bottom-up. Pure geometry: positions are parent-relative, sizes come from
// the layout entries (with caller-supplied defaults for unmeasured nodes).

export interface ContainmentItem {
  id: string;
  /// Parent container id (undefined = top level).
  parentId?: string;
  /// True for an EXPANDED container (collapsed containers are leaf chips).
  container: boolean;
  /// Parent-relative position.
  x: number;
  y: number;
  /// Drawn size, when known (layout entry / config). Defaults apply otherwise.
  w?: number;
  h?: number;
}

/** Effective minimum (w, h) per expanded container so every child fits inside
 *  with `margin` to spare on the right/bottom. A container's own saved size
 *  wins when larger. Children at negative coordinates are not compensated
 *  (the box can only grow right/down; a full auto-organize repacks those). */
export function computeContainmentFloors(
  items: ContainmentItem[],
  defaults: { w: number; h: number },
  margin: { right: number; bottom: number },
): Map<string, { w: number; h: number }> {
  const childrenOf = new Map<string, ContainmentItem[]>();
  for (const item of items) {
    if (!item.parentId) continue;
    const list = childrenOf.get(item.parentId);
    if (list) list.push(item);
    else childrenOf.set(item.parentId, [item]);
  }
  const floors = new Map<string, { w: number; h: number }>();
  const visiting = new Set<string>(); // cycle guard: malformed parent chains stay finite
  const effectiveSize = (item: ContainmentItem): { w: number; h: number } => {
    if (item.container) {
      const cached = floors.get(item.id);
      if (cached) return cached;
      if (visiting.has(item.id)) return { w: item.w ?? defaults.w, h: item.h ?? defaults.h };
      visiting.add(item.id);
      let w = item.w ?? defaults.w;
      let h = item.h ?? defaults.h;
      for (const child of childrenOf.get(item.id) ?? []) {
        const cs = effectiveSize(child);
        w = Math.max(w, child.x + cs.w + margin.right);
        h = Math.max(h, child.y + cs.h + margin.bottom);
      }
      visiting.delete(item.id);
      const size = { w, h };
      floors.set(item.id, size);
      return size;
    }
    return { w: item.w ?? defaults.w, h: item.h ?? defaults.h };
  };
  for (const item of items) {
    if (item.container) effectiveSize(item);
  }
  return floors;
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
      code = updateLayoutEntry(code, op.id, e.x, e.y, e.w, e.h, e.expanded ?? null, e.configCollapsed ?? null);
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
  return a.x === b.x && a.y === b.y && a.w === b.w && a.h === b.h
    && a.expanded === b.expanded && a.configCollapsed === b.configCollapsed;
}
