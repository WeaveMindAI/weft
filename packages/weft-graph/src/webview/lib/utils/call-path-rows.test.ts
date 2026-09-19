import { describe, it, expect } from 'vitest';
import { rowsByCallPath } from './call-path-rows';
import type { Frame } from '../../../protocol';

const row = (id: string, frames: Frame[]) => ({ id, frames });

const rows = {
	'src': [row('src', [])],
	'door__in': [row('door-in', [])],
	'@lib:door__in': [row('body-in', [{ site: 'door' }])],
	'@lib:door.strip': [
		row('strip-a', [{ site: 'door' }]),
		row('strip-b', [{ site: 'door' }, { index: 2 }]),
		row('strip-via-back', [{ site: 'back' }]),
	],
	'@lib:door.inner__in': [row('inner-site-in', [{ site: 'door' }])],
	'@lib:deep.n': [row('deep-n', [{ site: 'door' }, { site: '@lib:door.inner' }])],
	'loop.n': [row('loop-n', [{ index: 0 }])],
};

describe('rowsByCallPath', () => {
	it('at the top level: rows with no call frame are here, each site collects everything under it', () => {
		const { here, inside } = rowsByCallPath(rows, []);
		expect(Object.keys(here).sort()).toEqual(['door__in', 'loop.n', 'src']);
		expect(here['loop.n'].map((r) => r.id)).toEqual(['loop-n']);
		expect(Object.keys(inside).sort()).toEqual(['back', 'door']);
		expect(inside['door'].map((r) => r.id).sort()).toEqual(
			['body-in', 'deep-n', 'inner-site-in', 'strip-a', 'strip-b'],
		);
		expect(inside['back'].map((r) => r.id)).toEqual(['strip-via-back']);
	});

	it('inside a call: only that call is here, a nested site collects its own body', () => {
		const { here, inside } = rowsByCallPath(rows, ['door']);
		expect(Object.keys(here).sort()).toEqual(['@lib:door.inner__in', '@lib:door.strip', '@lib:door__in']);
		// A loop iteration inside the call is still this call.
		expect(here['@lib:door.strip'].map((r) => r.id)).toEqual(['strip-a', 'strip-b']);
		expect(Object.keys(inside)).toEqual(['@lib:door.inner']);
		expect(inside['@lib:door.inner'].map((r) => r.id)).toEqual(['deep-n']);
	});

	it('two calls deep: the rows of that call only, nothing below', () => {
		const { here, inside } = rowsByCallPath(rows, ['door', '@lib:door.inner']);
		expect(Object.keys(here)).toEqual(['@lib:deep.n']);
		expect(inside).toEqual({});
	});

	it('the same file through another site is neither here nor inside', () => {
		const { here, inside } = rowsByCallPath(rows, ['back']);
		expect(here['@lib:door.strip'].map((r) => r.id)).toEqual(['strip-via-back']);
		expect(inside).toEqual({});
	});

	it('a node named like a prototype member is keyed safely', () => {
		const { here } = rowsByCallPath({ constructor: [row('c', [])] }, []);
		expect(here['constructor'].map((r) => r.id)).toEqual(['c']);
	});
});
