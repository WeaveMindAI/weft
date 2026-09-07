import { describe, expect, it } from 'vitest';
import { classifyUpdate } from './update-classify';
import type { NodeDataUpdates } from '../types';

// The six gestures the node-update handler routes on. Each sender
// spreads the node's FULL config, so the classifier has to separate
// "expanded changed" from "expanded merely present".

const live = { value: 'x', expanded: true, width: 320 };

function classify(updates: NodeDataUpdates, liveConfig: Record<string, unknown> = live) {
	return classifyUpdate('n', updates, liveConfig, false);
}

describe('classifyUpdate', () => {
	it('a pure collapse/expand click is a toggle with no source ops', () => {
		const c = classify({ config: { ...live, expanded: false } });
		expect(c.isExpandToggle).toBe(true);
		expect(c.configOps).toEqual([]);
	});

	it('a connection pick bundled with expanded is NOT a toggle (one recordEdit, no reflow)', () => {
		// A fresh unconnected access node: no `expanded` in config yet,
		// the pick sends the handle + expanded: true as one gesture.
		const c = classify(
			{ config: { account: { id: 'g-1' }, expanded: true } },
			{},
		);
		expect(c.isExpandToggle).toBe(false);
		expect(c.configOps).toHaveLength(1);
		expect(c.configOps[0]).toMatchObject({ op: 'setConfig', node: 'n', key: 'account' });
	});

	it('a resize spread (expanded unchanged) is not a toggle', () => {
		const c = classify({ config: { ...live, width: 400 }, resized: true });
		expect(c.isExpandToggle).toBe(false);
		expect(c.configOps).toEqual([]); // width is a layout key, never a source op
	});

	it('typing in a config field is not a toggle and emits only the changed key', () => {
		const c = classify({ config: { ...live, value: 'xy' } });
		expect(c.isExpandToggle).toBe(false);
		expect(c.configOps).toHaveLength(1);
		expect(c.configOps[0]).toMatchObject({ op: 'setConfig', key: 'value' });
	});

	it('a label rename riding an expanded change is not a toggle', () => {
		const c = classify({ label: 'renamed', config: { ...live, expanded: false } });
		expect(c.isExpandToggle).toBe(false);
	});

	it('a port change riding an expanded change is not a toggle', () => {
		const c = classify({ inputs: [], config: { ...live, expanded: false } });
		expect(c.isExpandToggle).toBe(false);
	});

	it('loop config routes to the loop op family', () => {
		const c = classifyUpdate('l', { config: { parallel: 4 } }, {}, true);
		expect(c.configOps[0]).toMatchObject({ op: 'setLoopConfig', loopId: 'l', key: 'parallel' });
	});
});
