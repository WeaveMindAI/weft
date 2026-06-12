import { describe, it, expect } from 'vitest';
import { diffConfigOps } from './config-diff';

describe('diffConfigOps', () => {
	it('a full-config spread with nothing changed emits ZERO ops (the toggle regression)', () => {
		// An expand/collapse toggle spreads the whole config; every key is
		// unchanged, so no source op may leak out of a pure layout gesture.
		const config = { from: 0, to: 10, step: 1 };
		expect(diffConfigOps('range_1', { ...config, expanded: true }, config, false)).toEqual([]);
	});

	it('emits ops only for the keys that changed', () => {
		const current = { from: 0, to: 10, step: 1 };
		const ops = diffConfigOps('range_1', { ...current, to: 20 }, current, false);
		expect(ops).toEqual([{ op: 'setConfig', node: 'range_1', key: 'to', value: '20' }]);
	});

	it('layout/view keys never become source ops', () => {
		const ops = diffConfigOps('n', { width: 300, height: 200, expanded: false, configCollapsed: true, parentId: 'G', textareaHeights: {} }, {}, false);
		expect(ops).toEqual([]);
	});

	it('unsetting a previously-set key emits a remove; an always-unset key emits nothing', () => {
		expect(diffConfigOps('n', { text: null }, { text: 'old' }, false))
			.toEqual([{ op: 'removeConfig', node: 'n', key: 'text' }]);
		expect(diffConfigOps('n', { text: null }, {}, false)).toEqual([]);
	});

	it('object values compare by canonical token, not identity', () => {
		const current = { schema: { fields: [{ key: 'a' }] } };
		// A fresh-but-equal object (recreated per render) is no change.
		expect(diffConfigOps('n', { schema: { fields: [{ key: 'a' }] } }, current, false)).toEqual([]);
		// A genuinely different object is.
		const ops = diffConfigOps('n', { schema: { fields: [{ key: 'b' }] } }, current, false);
		expect(ops).toHaveLength(1);
		expect(ops[0].op).toBe('setConfig');
	});

	it('routes Loop containers to the loop op family', () => {
		expect(diffConfigOps('MyLoop', { parallel: true }, { parallel: false }, true))
			.toEqual([{ op: 'setLoopConfig', loopId: 'MyLoop', key: 'parallel', value: 'true' }]);
		expect(diffConfigOps('MyLoop', { max_iters: null }, { max_iters: 5 }, true))
			.toEqual([{ op: 'removeLoopConfig', loopId: 'MyLoop', key: 'max_iters' }]);
	});
});
