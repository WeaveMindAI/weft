import { describe, it, expect } from 'vitest';
import {
	parseLayoutCode,
	updateLayoutEntry,
	serializeLayoutMap,
	renameLayoutSubtree,
	applyLayoutOps,
	diffLayoutOps,
	computeContainmentFloors,
	type ContainmentItem,
} from './layout';

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
