import { describe, it, expect } from 'vitest';
import {
	parseLayoutCode,
	updateLayoutEntry,
	serializeLayoutMap,
	renameLayoutSubtree,
	applyLayoutOps,
	diffLayoutOps,
	computeContainmentFloors,
	parseViewMode,
	setViewMode,
	removeLayoutEntry,
	SIMPLIFIED_LAYOUT_VERB,
	type ContainmentItem,
} from './layout';

describe('view mode header', () => {
	it('defaults to builder when no header present', () => {
		expect(parseViewMode('')).toBe('builder');
		expect(parseViewMode('a @layout 10 20')).toBe('builder');
	});

	it('reads the simplified header regardless of position', () => {
		expect(parseViewMode('@view simplified')).toBe('simplified');
		expect(parseViewMode('a @layout 10 20\n@view simplified')).toBe('simplified');
	});

	it('setViewMode adds/removes the header without touching node entries', () => {
		const code = 'a @layout 10 20\nb @layout 0 0';
		const simplified = setViewMode(code, 'simplified');
		expect(parseViewMode(simplified)).toBe('simplified');
		expect(parseLayoutCode(simplified).a).toEqual({ x: 10, y: 20 });
		expect(parseLayoutCode(simplified).b).toEqual({ x: 0, y: 0 });
		const builder = setViewMode(simplified, 'builder');
		expect(parseViewMode(builder)).toBe('builder');
		expect(builder.includes('@view')).toBe(false);
		expect(parseLayoutCode(builder).a).toEqual({ x: 10, y: 20 });
	});

	it('setViewMode is idempotent (no duplicate headers)', () => {
		const once = setViewMode('a @layout 1 2', 'simplified');
		const twice = setViewMode(once, 'simplified');
		expect(twice.split('\n').filter((l) => l.trim() === '@view simplified').length).toBe(1);
	});

	it('view mode round-trips through the layout-op pipeline (apply + diff)', () => {
		const before = 'a @layout 10 20';
		// A toggle produces a setView op via diff, which applies to set the header.
		const ops = diffLayoutOps(before, setViewMode(before, 'simplified'));
		expect(ops).toContainEqual({ op: 'setView', mode: 'simplified' });
		const after = applyLayoutOps(before, ops);
		expect(parseViewMode(after)).toBe('simplified');
		expect(parseLayoutCode(after).a).toEqual({ x: 10, y: 20 });
		// The inverse op flips it back (undo).
		const undo = diffLayoutOps(after, before);
		expect(undo).toContainEqual({ op: 'setView', mode: 'builder' });
		expect(parseViewMode(applyLayoutOps(after, undo))).toBe('builder');
	});

	it('node ops preserve the view-mode header', () => {
		const code = setViewMode('a @layout 0 0', 'simplified');
		const moved = applyLayoutOps(code, [{ op: 'setEntry', id: 'a', entry: { x: 5, y: 5 } }]);
		expect(parseViewMode(moved)).toBe('simplified');
		const removed = applyLayoutOps(code, [{ op: 'removeEntry', id: 'a' }]);
		expect(parseViewMode(removed)).toBe('simplified');
	});

	it('renameLayoutSubtree preserves the view-mode header (move must not drop simplified)', () => {
		const code = setViewMode('G @layout 0 0 400x300 expanded\nG.a @layout 5 5', 'simplified');
		const renamed = renameLayoutSubtree(code, 'G', 'R');
		expect(parseViewMode(renamed)).toBe('simplified');
		expect(parseLayoutCode(renamed)['R.a']).toEqual({ x: 5, y: 5 });
	});
});

describe('simplified-view position block (@slayout)', () => {
	it('round-trips a @slayout line under the simplified verb', () => {
		const map = parseLayoutCode('a @slayout 12 34', SIMPLIFIED_LAYOUT_VERB);
		expect(map.a).toEqual({ x: 12, y: 34 });
	});

	it('a @slayout line is INVISIBLE to the builder parse and vice versa', () => {
		// The two verbs must never cross-match, or one view would read the other's
		// positions (a node is a wide box in builder, a small square in simplified).
		expect(parseLayoutCode('a @slayout 1 2', '@layout')).toEqual({});
		expect(parseLayoutCode('a @layout 1 2', SIMPLIFIED_LAYOUT_VERB)).toEqual({});
	});

	it('both blocks coexist in one file without clobbering', () => {
		const code = 'a @layout 1 2\na @slayout 9 9';
		expect(parseLayoutCode(code, '@layout').a).toEqual({ x: 1, y: 2 });
		expect(parseLayoutCode(code, SIMPLIFIED_LAYOUT_VERB).a).toEqual({ x: 9, y: 9 });
	});

	it('diffLayoutOps tags a simplified move with the verb and emits no builder op', () => {
		const ops = diffLayoutOps('a @slayout 1 2', 'a @slayout 5 5');
		expect(ops).toContainEqual({ op: 'setEntry', id: 'a', entry: { x: 5, y: 5 }, verb: SIMPLIFIED_LAYOUT_VERB });
		expect(ops.some(o => o.op === 'setEntry' && !('verb' in o))).toBe(false);
	});

	it('applyLayoutOps routes a simplified setEntry to the @slayout line, leaving @layout untouched', () => {
		const code = 'a @layout 1 2\na @slayout 9 9';
		const next = applyLayoutOps(code, [{ op: 'setEntry', id: 'a', entry: { x: 7, y: 7 }, verb: SIMPLIFIED_LAYOUT_VERB }]);
		expect(parseLayoutCode(next, '@layout').a).toEqual({ x: 1, y: 2 }); // builder untouched
		expect(parseLayoutCode(next, SIMPLIFIED_LAYOUT_VERB).a).toEqual({ x: 7, y: 7 });
	});

	it('removeLayoutEntry on one verb leaves the other view\'s line intact', () => {
		const code = 'a @layout 1 2\na @slayout 9 9';
		const next = removeLayoutEntry(code, 'a', SIMPLIFIED_LAYOUT_VERB);
		expect(parseLayoutCode(next, '@layout').a).toEqual({ x: 1, y: 2 });
		expect(parseLayoutCode(next, SIMPLIFIED_LAYOUT_VERB).a).toBeUndefined();
	});

	it('updateLayoutEntry under @slayout preserves size/collapse on a position-only move', () => {
		// A simplified drag writes only x/y; the node's saved WxH + collapsed state
		// must survive (the merge reads the prior entry under the SAME verb).
		const code = 'G @slayout 0 0 400x300 collapsed configCollapsed';
		const moved = updateLayoutEntry(code, 'G', 5, 5, undefined, undefined, undefined, undefined, SIMPLIFIED_LAYOUT_VERB);
		expect(parseLayoutCode(moved, SIMPLIFIED_LAYOUT_VERB).G).toEqual({ x: 5, y: 5, w: 400, h: 300, expanded: false, configCollapsed: true });
	});

	it('diffLayoutOps emits one op per verb when a node moves in BOTH blocks at once', () => {
		const from = 'a @layout 1 1\na @slayout 2 2';
		const to = 'a @layout 9 9\na @slayout 8 8';
		const ops = diffLayoutOps(from, to);
		expect(ops).toContainEqual({ op: 'setEntry', id: 'a', entry: { x: 9, y: 9 } }); // builder, untagged
		expect(ops).toContainEqual({ op: 'setEntry', id: 'a', entry: { x: 8, y: 8 }, verb: SIMPLIFIED_LAYOUT_VERB });
		// applying round-trips both blocks independently
		const after = applyLayoutOps(from, ops);
		expect(parseLayoutCode(after, '@layout').a).toEqual({ x: 9, y: 9 });
		expect(parseLayoutCode(after, SIMPLIFIED_LAYOUT_VERB).a).toEqual({ x: 8, y: 8 });
	});

	it('renameLayoutSubtree re-keys BOTH blocks and drops neither, header preserved', () => {
		const code = setViewMode('G @layout 0 0 400x300 expanded\nG @slayout 1 1\nG.a @layout 5 5\nG.a @slayout 6 6', 'simplified');
		const renamed = renameLayoutSubtree(code, 'G', 'R');
		expect(parseViewMode(renamed)).toBe('simplified');
		expect(parseLayoutCode(renamed, '@layout')['R.a']).toEqual({ x: 5, y: 5 });
		expect(parseLayoutCode(renamed, SIMPLIFIED_LAYOUT_VERB)['R.a']).toEqual({ x: 6, y: 6 });
		expect(parseLayoutCode(renamed, SIMPLIFIED_LAYOUT_VERB)['R']).toEqual({ x: 1, y: 1 });
		// old keys gone in both blocks
		expect(parseLayoutCode(renamed, '@layout')['G.a']).toBeUndefined();
		expect(parseLayoutCode(renamed, SIMPLIFIED_LAYOUT_VERB)['G.a']).toBeUndefined();
	});
});

describe('layout round-trips', () => {
	it('parse <-> serialize keeps every field, including configCollapsed', () => {
		const code = 'a @layout 10 20\nG @layout 0 0 400x300 expanded\nL @layout 5 5 600x500 collapsed configCollapsed';
		const map = parseLayoutCode(code);
		expect(map.L).toEqual({ x: 5, y: 5, w: 600, h: 500, expanded: false, configCollapsed: true });
		expect(parseLayoutCode(serializeLayoutMap(map))).toEqual(map);
	});

	it('renameLayoutSubtree re-keys the subtree without losing configCollapsed', () => {
		const code = 'G @layout 0 0 400x300 expanded\nG.L @layout 5 5 600x500 expanded configCollapsed';
		const renamed = parseLayoutCode(renameLayoutSubtree(code, 'G', 'R'));
		expect(renamed['R.L']).toEqual({ x: 5, y: 5, w: 600, h: 500, expanded: true, configCollapsed: true });
	});

	it('diff + apply round-trips, configCollapsed changes included', () => {
		const before = 'L @layout 5 5 600x500 expanded';
		const after = updateLayoutEntry(before, 'L', 5, 5, 600, 500, true, true);
		const undo = diffLayoutOps(after, before);
		expect(undo.length).toBeGreaterThan(0);
		expect(parseLayoutCode(applyLayoutOps(after, undo))).toEqual(parseLayoutCode(before));
	});
});

describe('computeContainmentFloors', () => {
	const defaults = { w: 280, h: 120 };
	const margin = { right: 40, bottom: 40 };

	it('floors a parent by a container child that overflows it (the loop-in-group bug)', () => {
		const items: ContainmentItem[] = [
			{ id: 'G', container: true, x: 0, y: 0, w: 500, h: 350 },
			{ id: 'G.L', parentId: 'G', container: true, x: 60, y: 80, w: 600, h: 500 },
			{ id: 'G.L.py', parentId: 'G.L', container: false, x: 60, y: 100, w: 280, h: 150 },
		];
		const floors = computeContainmentFloors(items, defaults, margin);
		// The loop keeps its own (already sufficient) size.
		expect(floors.get('G.L')).toEqual({ w: 600, h: 500 });
		// The group grows to contain the loop: 60 + 600 + 40, 80 + 500 + 40.
		expect(floors.get('G')).toEqual({ w: 700, h: 620 });
	});

	it('recursive: a deep child grows every ancestor', () => {
		const items: ContainmentItem[] = [
			{ id: 'A', container: true, x: 0, y: 0, w: 400, h: 300 },
			{ id: 'A.B', parentId: 'A', container: true, x: 50, y: 50, w: 400, h: 300 },
			{ id: 'A.B.c', parentId: 'A.B', container: false, x: 500, y: 400, w: 200, h: 100 },
		];
		const floors = computeContainmentFloors(items, defaults, margin);
		expect(floors.get('A.B')).toEqual({ w: 740, h: 540 });
		expect(floors.get('A')).toEqual({ w: 830, h: 630 });
	});

	it('a parent already large enough keeps its saved size', () => {
		const items: ContainmentItem[] = [
			{ id: 'G', container: true, x: 0, y: 0, w: 1000, h: 800 },
			{ id: 'G.n', parentId: 'G', container: false, x: 100, y: 100, w: 200, h: 100 },
		];
		expect(computeContainmentFloors(items, defaults, margin).get('G')).toEqual({ w: 1000, h: 800 });
	});

	it('unknown child sizes fall back to defaults; empty containers keep their size', () => {
		const items: ContainmentItem[] = [
			{ id: 'G', container: true, x: 0, y: 0, w: 300, h: 100 },
			{ id: 'G.n', parentId: 'G', container: false, x: 0, y: 0 },
			{ id: 'E', container: true, x: 0, y: 0, w: 500, h: 350 },
		];
		const floors = computeContainmentFloors(items, defaults, margin);
		expect(floors.get('G')).toEqual({ w: 320, h: 160 });
		expect(floors.get('E')).toEqual({ w: 500, h: 350 });
	});

	it('a malformed parent cycle terminates with the nodes own sizes', () => {
		const items: ContainmentItem[] = [
			{ id: 'A', parentId: 'B', container: true, x: 0, y: 0, w: 400, h: 300 },
			{ id: 'B', parentId: 'A', container: true, x: 0, y: 0, w: 400, h: 300 },
		];
		const floors = computeContainmentFloors(items, defaults, margin);
		expect(floors.get('A')!.w).toBeGreaterThanOrEqual(400);
		expect(floors.get('B')!.w).toBeGreaterThanOrEqual(400);
	});
});
