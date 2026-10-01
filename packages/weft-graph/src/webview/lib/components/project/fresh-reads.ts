// One shared read of a value that writes change on the server. Callers
// asking while a read is under way share it, unless a write landed since
// that read started: its answer may predate the write, so a fresh read
// starts, and the older one's answer is dropped when it arrives.

export type ReadOutcome<T> = { ok: true; value: T } | { ok: false; error: unknown };

export class FreshReads<T> {
	private writes = 0;
	private started = 0;
	private inFlight: { writes: number; promise: Promise<void> } | null = null;

	constructor(
		private readonly fetch: () => Promise<T>,
		private readonly settle: (outcome: ReadOutcome<T>) => void,
	) {}

	/// A write has landed: any read from now on must start after it.
	wrote(): void {
		this.writes++;
	}

	/// Read (again), or join the read under way when no write landed since
	/// it started. Resolves once the newest read has settled.
	load(): Promise<void> {
		if (this.inFlight && this.inFlight.writes === this.writes) return this.inFlight.promise;
		const id = ++this.started;
		const promise = this.fetch()
			.then(
				(value): ReadOutcome<T> => ({ ok: true, value }),
				(error: unknown): ReadOutcome<T> => ({ ok: false, error }),
			)
			.then((outcome) => {
				// A newer read owns the answer: wait for it instead.
				if (id !== this.started) return this.inFlight?.promise;
				this.inFlight = null;
				this.settle(outcome);
			});
		this.inFlight = { writes: this.writes, promise };
		return promise;
	}
}
