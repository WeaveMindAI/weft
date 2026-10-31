import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { KeptValue, ValueRef, ValuesAnswer, ValuesRequest } from '../../../protocol';
import { ASK_AGAIN_AFTER_MS, RunValues, portsAsShown, shownAnswer, type ShownValue } from './run-values.svelte';

const row = (seq: number): ValueRef => ({ from: 'row', run: 'r', seq, index: 0 });

/** A run's record answering each trip with the next of `answers`, a string
 *  being a trip that fails with that error. */
function record(answers: Array<KeptValue[] | string>) {
	const trips: ValuesRequest[] = [];
	const read = async (_run: string, request: ValuesRequest): Promise<ValuesAnswer> => {
		trips.push(request);
		const answer = answers[trips.length - 1];
		if (answer === undefined) throw new Error('no answer left in the fake');
		if (typeof answer === 'string') throw new Error(answer);
		return { values: answer };
	};
	return { trips, values: new RunValues(read) };
}

beforeEach(() => { vi.useFakeTimers(); });
afterEach(() => { vi.useRealTimers(); });

describe('the values a run\'s events name', () => {
	it('show each answer by what it says: a value, gone, or broken', () => {
		expect(shownAnswer({ value: 3 })).toEqual({ state: 'read', value: 3 });
		expect(shownAnswer({ gone: 'the run was erased' })).toEqual({ state: 'gone', why: 'the run was erased' });
		expect(shownAnswer({ broken: 'row 3 has no part `x`' })).toEqual({ state: 'broken', why: 'row 3 has no part `x`' });
	});

	it('show a gone port as null with why, and a broken or failed one as an error with no value', () => {
		const states = new Map<number, ShownValue>([
			[1, { state: 'read', value: 'a' }],
			[2, { state: 'gone', why: 'erased' }],
			[3, { state: 'broken', why: 'disagree' }],
			[4, { state: 'failed', why: 'offline' }],
			[5, { state: 'reading' }],
		]);
		const shown = portsAsShown(
			{ a: row(1), b: row(2), c: row(3), d: row(4), e: row(5) },
			(at) => states.get((at as { seq: number }).seq)!,
		);
		expect(shown.values).toEqual({ a: 'a', b: null });
		expect(shown.gone).toEqual({ b: 'erased' });
		expect(shown.errors).toEqual({ c: 'disagree', d: 'could not be read: offline (asking again)' });
		expect(shown.reading).toBe(true);
	});

	it('read what is asked together in one trip', async () => {
		const { trips, values } = record([[{ value: 1 }, { broken: 'no such part' }]]);
		values.follow('r');
		expect(values.shown(row(1))).toEqual({ state: 'reading' });
		expect(values.shown(row(2))).toEqual({ state: 'reading' });
		await vi.advanceTimersByTimeAsync(0);
		expect(trips).toEqual([{ refs: [row(1), row(2)] }]);
		expect(values.shown(row(1))).toEqual({ state: 'read', value: 1 });
		expect(values.shown(row(2))).toEqual({ state: 'broken', why: 'no such part' });
	});

	it('keep a failed read only until it is asked again, and ask again while it is shown', async () => {
		const { trips, values } = record(['the host is unreachable', [{ value: 7 }]]);
		values.follow('r');
		values.shown(row(1));
		await vi.advanceTimersByTimeAsync(0);
		expect(values.shown(row(1))).toEqual({ state: 'failed', why: 'the host is unreachable' });
		await vi.advanceTimersByTimeAsync(ASK_AGAIN_AFTER_MS);
		// Still shown: showing it again asks for it again.
		expect(values.shown(row(1))).toEqual({ state: 'reading' });
		await vi.advanceTimersByTimeAsync(0);
		expect(trips).toHaveLength(2);
		expect(values.shown(row(1))).toEqual({ state: 'read', value: 7 });
	});

	it('ask nothing again for a failed read that is no longer shown', async () => {
		const { trips, values } = record(['the host is unreachable']);
		values.follow('r');
		values.shown(row(1));
		await vi.advanceTimersByTimeAsync(0);
		values.release([row(1)]);
		await vi.advanceTimersByTimeAsync(ASK_AGAIN_AFTER_MS * 3);
		expect(trips).toHaveLength(1);
	});

	it('show an answer with the wrong number of values as failed, never as a value', async () => {
		const { values } = record([[{ value: 1 }]]);
		values.follow('r');
		values.shown(row(1));
		values.shown(row(2));
		await vi.advanceTimersByTimeAsync(0);
		expect(values.shown(row(1))).toEqual({ state: 'failed', why: 'the install answered 1 values for the 2 asked' });
	});
});
