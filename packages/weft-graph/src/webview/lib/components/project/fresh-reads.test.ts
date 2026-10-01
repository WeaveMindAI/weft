import { describe, expect, it } from 'vitest';
import { FreshReads, type ReadOutcome } from './fresh-reads';

function deferred<T>() {
	let resolve!: (v: T) => void;
	const promise = new Promise<T>((r) => (resolve = r));
	return { promise, resolve };
}

function rig() {
	const pending: { resolve: (v: number) => void }[] = [];
	const settled: ReadOutcome<number>[] = [];
	const reads = new FreshReads<number>(
		() => {
			const d = deferred<number>();
			pending.push(d);
			return d.promise;
		},
		(o) => settled.push(o),
	);
	return { reads, pending, settled };
}

describe('FreshReads', () => {
	it('shares a read under way when no write landed since it started', async () => {
		const { reads, pending, settled } = rig();
		const a = reads.load();
		const b = reads.load();
		expect(pending).toHaveLength(1);
		pending[0].resolve(1);
		await Promise.all([a, b]);
		expect(settled).toEqual([{ ok: true, value: 1 }]);
	});

	it('starts a fresh read after a write and drops the older answer', async () => {
		const { reads, pending, settled } = rig();
		const before = reads.load();
		reads.wrote();
		const after = reads.load();
		expect(pending).toHaveLength(2);
		pending[1].resolve(2);
		await after;
		pending[0].resolve(1);
		await before;
		expect(settled).toEqual([{ ok: true, value: 2 }]);
	});

	it('a superseded read resolves only once the newer one settled', async () => {
		const { reads, pending, settled } = rig();
		const before = reads.load();
		reads.wrote();
		void reads.load();
		pending[0].resolve(1);
		let done = false;
		void before.then(() => (done = true));
		await Promise.resolve();
		await Promise.resolve();
		expect(done).toBe(false);
		pending[1].resolve(2);
		await before;
		expect(settled).toEqual([{ ok: true, value: 2 }]);
	});
});
