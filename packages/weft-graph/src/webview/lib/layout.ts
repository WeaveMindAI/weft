// The companion `.layout` file: node positions + sizes + collapse state.
//
// Layout is NOT part of the `.weft` language. It lives in a sibling file so the
// source stays clean, and it's a frontend concern (where boxes sit on a
// canvas), so it stays in the webview rather than going through the Rust
// edit-server. Format, one entry per line:
//   scopedId @layout x y [WxH] [expanded|collapsed] [configCollapsed]

import { bareRecord } from './types';

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

// ── Per-project view mode ────────────────────────────────────────────────
//
// The graph can render in two view modes: the full builder view (every port,
// config, body) or a simplified view (each node a square with one in/one out
// dot, edges collapsed per node-pair). This is a PER-PROJECT preference, not
// per-node, so it lives as a single header line in the `.layout` file rather
// than in the node map:
//   @view simplified
// Absence of the line means builder view (the default). `parseLayoutCode`
// already skips any line that isn't a `@layout` entry, so the header is inert
// to the node-position parser; only `parseViewMode`/`setViewMode` read it.

export type ViewMode = 'builder' | 'simplified';

/** Read the per-project view mode from layoutCode (default 'builder'). */
export function parseViewMode(layoutCode: string): ViewMode {
  if (!layoutCode) return 'builder';
  for (const line of layoutCode.split('\n')) {
    if (line.trim() === '@view simplified') return 'simplified';
  }
  return 'builder';
}

/** Return layoutCode with the view-mode header set to `mode`. Removes the
 *  header for 'builder' (the default) so a builder-view project carries no
 *  marker, and writes a single `@view simplified` line otherwise. The header
 *  is kept as the first line for legibility. */
export function setViewMode(layoutCode: string, mode: ViewMode): string {
  const lines = (layoutCode || '').split('\n').filter((l) => l.trim() !== '@view simplified' && l.trim() !== '');
  if (mode === 'simplified') lines.unshift('@view simplified');
  return lines.join('\n');
}

/** Parse layoutCode into a map of scoped id -> entry. */
// Each view stores its node positions under its own verb on its own lines, so
// the builder layout and the simplified layout coexist in ONE file without
// clobbering each other (a node is a wide box in builder, a small square in
// simplified, so they need independent positions). Builder uses `@layout` (the
// original format, untouched); simplified uses `@slayout`. Every layout helper
// takes a `verb` so the SAME machinery serves both views (DRY): pass
// `LAYOUT_VERB` for builder, `SIMPLIFIED_LAYOUT_VERB` for simplified.
export const LAYOUT_VERB = '@layout';
export const SIMPLIFIED_LAYOUT_VERB = '@slayout';
export type LayoutVerb = typeof LAYOUT_VERB | typeof SIMPLIFIED_LAYOUT_VERB;

/** The one entry-line pattern for a verb: parsing and rewriting share it, so
 *  a line is "a layout entry" by exactly one definition. */
function entryRe(verb: LayoutVerb): RegExp {
  return new RegExp(`^(.+?)\\s+${verb}\\s+(-?\\d+(?:\\.\\d+)?)\\s+(-?\\d+(?:\\.\\d+)?)(?:\\s+(\\d+(?:\\.\\d+)?)x(\\d+(?:\\.\\d+)?))?(?:\\s+(collapsed|expanded))?(?:\\s+(configCollapsed))?\\s*$`);
}

function matchToEntry(match: RegExpMatchArray): [string, LayoutEntry] {
  const [, scopedId, xStr, yStr, wStr, hStr, state, configState] = match;
  const entry: LayoutEntry = { x: parseFloat(xStr), y: parseFloat(yStr) };
  if (wStr && hStr) {
    entry.w = parseFloat(wStr);
    entry.h = parseFloat(hStr);
  }
  if (state === 'expanded') entry.expanded = true;
  if (state === 'collapsed') entry.expanded = false;
  if (configState === 'configCollapsed') entry.configCollapsed = true;
  return [scopedId, entry];
}

export function parseLayoutCode(layoutCode: string, verb: LayoutVerb = LAYOUT_VERB): Record<string, LayoutEntry> {
  // Keyed by node id (user-chosen), so no inherited entries: see
  // `bareRecord`.
  const map: Record<string, LayoutEntry> = bareRecord();
  if (!layoutCode) return map;
  const re = entryRe(verb);
  for (const line of layoutCode.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    const match = trimmed.match(re);
    if (!match) continue;
    const [scopedId, entry] = matchToEntry(match);
    map[scopedId] = entry;
  }
  return map;
}

/** Rewrite ONE verb's entry block through a map mutation: parse every entry
 *  line of `verb` into a map (last-wins, the same rule `parseLayoutCode`
 *  reads by), run `mutate`, and serialize the map back where the block
 *  lived. Every non-matching line (the other verb's entries, the `@view`
 *  header, anything unrecognized) passes through untouched and in order.
 *  This is THE layout writer: it collapses duplicate keys to one line by
 *  construction, so a writer and the reader can never disagree about which
 *  line a key means. */
function rewriteVerbEntries(
  layoutCode: string,
  verb: LayoutVerb,
  mutate: (map: Record<string, LayoutEntry>) => void,
): string {
  const re = entryRe(verb);
  const map: Record<string, LayoutEntry> = {};
  const passthrough: Array<{ idx: number; line: string }> = [];
  let firstIdx = -1;
  (layoutCode || '').split('\n').forEach((line, idx) => {
    const trimmed = line.trim();
    if (!trimmed) return;
    const match = trimmed.match(re);
    if (match) {
      if (firstIdx < 0) firstIdx = idx;
      const [scopedId, entry] = matchToEntry(match);
      map[scopedId] = entry;
    } else {
      passthrough.push({ idx, line: trimmed });
    }
  });
  mutate(map);
  const block = serializeLayoutMap(map, verb).split('\n').filter((l) => l !== '');
  const out: string[] = [];
  let inserted = false;
  for (const p of passthrough) {
    if (!inserted && firstIdx >= 0 && p.idx > firstIdx) {
      out.push(...block);
      inserted = true;
    }
    out.push(p.line);
  }
  if (!inserted) out.push(...block);
  return out.join('\n');
}

/** Update or insert a layout entry. Returns the new layoutCode.
 *
 *  `undefined` for `w`/`h`/`expanded`/`configCollapsed` means "leave whatever
 *  is already persisted", NOT "clear it". This matters because position-only
 *  updates (a drag, an ELK reflow moving a NEIGHBOUR node) call this without
 *  knowing the node's size/collapse state; a destructive rewrite would strip
 *  the persisted `expanded` flag and the node would snap to its type default
 *  on the next rebuild (spurious collapse/expand on untouched nodes). To
 *  actually clear a field, pass `null`: all four optional fields share the
 *  same three-state rule, so a diffed op that DROPS a size can round-trip
 *  (undo of a resize must be able to restore "no explicit size"). */
export function updateLayoutEntry(
  layoutCode: string,
  scopedId: string,
  x: number,
  y: number,
  w?: number | null,
  h?: number | null,
  expanded?: boolean | null,
  configCollapsed?: boolean | null,
  verb: LayoutVerb = LAYOUT_VERB,
): string {
  return rewriteVerbEntries(layoutCode, verb, (map) => {
    const prior = map[scopedId];
    const entry: LayoutEntry = { x, y };
    const mergedW = w === undefined ? prior?.w : w ?? undefined;
    const mergedH = h === undefined ? prior?.h : h ?? undefined;
    // A size is one fact, not two: the format has no way to write width
    // without height, so a half-set size would be dropped SILENTLY at
    // serialization. Refuse it here instead.
    if ((mergedW === undefined) !== (mergedH === undefined)) {
      throw new Error(
        `layout entry '${scopedId}': width and height must be set or cleared together (got w=${mergedW}, h=${mergedH})`,
      );
    }
    const mergedExpanded = expanded === undefined ? prior?.expanded : expanded ?? undefined;
    const mergedConfigCollapsed = configCollapsed === undefined ? prior?.configCollapsed : configCollapsed ?? undefined;
    if (mergedW !== undefined) entry.w = mergedW;
    if (mergedH !== undefined) entry.h = mergedH;
    if (mergedExpanded !== undefined) entry.expanded = mergedExpanded;
    if (mergedConfigCollapsed !== undefined) entry.configCollapsed = mergedConfigCollapsed;
    map[scopedId] = entry;
  });
}

/** Remove a layout entry (one view's verb). Returns the new layoutCode. */
export function removeLayoutEntry(layoutCode: string, scopedId: string, verb: LayoutVerb = LAYOUT_VERB): string {
  if (!layoutCode) return '';
  return rewriteVerbEntries(layoutCode, verb, (map) => {
    delete map[scopedId];
  });
}

/** Remove a node's layout entry from EVERY view's block. A node that left the
 *  project (delete, ungroup consuming its key) is gone in both views; a
 *  caller deleting from only the view it happens to render would strand the
 *  other view's line forever. */
export function removeLayoutEntryEveryView(layoutCode: string, scopedId: string): string {
  return removeLayoutEntry(removeLayoutEntry(layoutCode, scopedId, LAYOUT_VERB), scopedId, SIMPLIFIED_LAYOUT_VERB);
}

function formatLayoutStr(x: number, y: number, w?: number, h?: number, expanded?: boolean | null, configCollapsed?: boolean | null, verb: LayoutVerb = LAYOUT_VERB): string {
  let s = `${verb} ${Math.round(x)} ${Math.round(y)}`;
  if (w !== undefined && h !== undefined) s += ` ${Math.round(w)}x${Math.round(h)}`;
  if (expanded === true) s += ' expanded';
  if (expanded === false) s += ' collapsed';
  if (configCollapsed === true) s += ' configCollapsed';
  return s;
}

/** Serialize a layout map back to layoutCode lines under `verb` (one line per
 *  entry). The inverse of `parseLayoutCode` for that verb. */
export function serializeLayoutMap(map: Record<string, LayoutEntry>, verb: LayoutVerb = LAYOUT_VERB): string {
  return Object.entries(map)
    .map(([id, e]) => `${id} ${formatLayoutStr(e.x, e.y, e.w, e.h, e.expanded ?? undefined, e.configCollapsed ?? undefined, verb)}`)
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
  // A move/rename re-keys the node in BOTH views' position blocks. Each verb's
  // block is rewritten through the one layout writer, so the `@view` header
  // and any line the parser does not recognize pass through untouched (a
  // rename must never be the operation that silently deletes them).
  const reKey = (map: Record<string, LayoutEntry>): void => {
    const reKeyed: Array<[string, LayoutEntry]> = [];
    for (const [key, entry] of Object.entries(map)) {
      if (key === oldKey) reKeyed.push([newKey, entry]);
      else if (key.startsWith(oldKey + '.')) reKeyed.push([newKey + key.slice(oldKey.length), entry]);
      else continue;
      delete map[key];
    }
    // Re-keyed entries written last so the moved subtree wins any collision
    // with an entry outside it (the displaced entry's view-state is obsolete).
    for (const [k, entry] of reKeyed) map[k] = entry;
  };
  const builder = rewriteVerbEntries(layoutCode, LAYOUT_VERB, reKey);
  return rewriteVerbEntries(builder, SIMPLIFIED_LAYOUT_VERB, reKey);
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
// `.layout` file line today and equally to a DB row upsert/delete/prefix-update
// in another backing store, with the same action model. This is the layout half of the
// editor's undo stack (the source half is a `TextEdit`, because source is a
// text document where byte-exactness matters; layout is per-node view state
// where structure matters). Layout is TS-owned: Rust never sees these ops.

export type LayoutOp =
  // `verb` says which view's position block the op targets (absent = builder,
  // for back-compat with existing call sites). A drag in simplified view emits
  // ops with verb '@slayout'; a builder drag omits it.
  | { op: 'setEntry'; id: string; entry: LayoutEntry; verb?: LayoutVerb }
  | { op: 'removeEntry'; id: string; verb?: LayoutVerb }
  // The per-project view mode is part of the layout state, so it travels as a
  // layout op through the SAME pipeline as node moves (apply/diff/undo/persist).
  // Without this it could never round-trip the engine's op-based layout fold.
  | { op: 'setView'; mode: ViewMode };

/** Apply a layout op batch to `layoutCode`, returning the new layoutCode. */
export function applyLayoutOps(layoutCode: string, ops: LayoutOp[]): string {
  let code = layoutCode;
  for (const op of ops) {
    if (op.op === 'setEntry') {
      // An op's entry is the COMPLETE description of the key: every absent
      // field is a clear (`?? null`), never a preserve, so a diff that
      // dropped a size round-trips (undo of a resize restores "no explicit
      // size" instead of resurrecting the resized one).
      const e = op.entry;
      code = updateLayoutEntry(code, op.id, e.x, e.y, e.w ?? null, e.h ?? null, e.expanded ?? null, e.configCollapsed ?? null, op.verb ?? LAYOUT_VERB);
    } else if (op.op === 'removeEntry') {
      code = removeLayoutEntry(code, op.id, op.verb ?? LAYOUT_VERB);
    } else {
      code = setViewMode(code, op.mode);
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
  const ops: LayoutOp[] = [];
  // View-mode change is a layout op too, so a toggle round-trips the engine's
  // fold and undo stack identically to a node move.
  const fromView = parseViewMode(from);
  const toView = parseViewMode(to);
  if (fromView !== toView) ops.push({ op: 'setView', mode: toView });
  // Diff each view's position block independently so a gesture in either view
  // (or a rename touching both) yields the right verb-tagged ops. Builder ops
  // omit `verb` (default) to keep existing op streams byte-identical.
  for (const verb of [LAYOUT_VERB, SIMPLIFIED_LAYOUT_VERB] as const) {
    const a = parseLayoutCode(from, verb);
    const b = parseLayoutCode(to, verb);
    const tag = verb === LAYOUT_VERB ? {} : { verb };
    for (const [id, entry] of Object.entries(b)) {
      const prior = a[id];
      if (!prior || !sameEntry(prior, entry)) ops.push({ op: 'setEntry', id, entry, ...tag });
    }
    for (const id of Object.keys(a)) {
      if (!(id in b)) ops.push({ op: 'removeEntry', id, ...tag });
    }
  }
  return ops;
}

function sameEntry(a: LayoutEntry, b: LayoutEntry): boolean {
  return a.x === b.x && a.y === b.y && a.w === b.w && a.h === b.h
    && a.expanded === b.expanded && a.configCollapsed === b.configCollapsed;
}
