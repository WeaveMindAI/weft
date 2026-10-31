import { describe, expect, it } from 'vitest';
import type { ValueRef } from '../../../protocol';
import { partOf, readOnTheSpot, refKey } from './value-ref';

const row: ValueRef = { from: 'row', run: 'r', seq: 3, index: 0 };

describe('value references', () => {
	it('take a part of a part as one part, and nothing for an empty path', () => {
		expect(partOf(row, [])).toBe(row);
		expect(partOf(partOf(row, ['profile']), ['wpm', 1])).toEqual({ from: 'part', of: row, path: ['profile', 'wpm', 1] });
	});

	it('read a value the reference holds on the spot, and leave the record to read the rest', () => {
		const written: ValueRef = { from: 'written', value: { a: [1, 2] } };
		expect(readOnTheSpot(written)).toEqual({ value: { a: [1, 2] } });
		expect(readOnTheSpot(partOf(written, ['a', 1]))).toEqual({ value: 2 });
		expect(readOnTheSpot(partOf(written, ['nope']))).toBeUndefined();
		expect(readOnTheSpot({ from: 'list', items: [written, { from: 'written', value: null }] })).toEqual({ value: [{ a: [1, 2] }, null] });
		expect(readOnTheSpot({ from: 'list', items: [written, row] })).toBeUndefined();
		expect(readOnTheSpot(row)).toBeUndefined();
	});

	it('know two references to one value written the same way as one', () => {
		expect(refKey({ from: 'row', run: 'r', seq: 3, index: 0 })).toBe(refKey(row));
		expect(refKey(partOf(row, ['x']))).not.toBe(refKey(row));
	});
});
