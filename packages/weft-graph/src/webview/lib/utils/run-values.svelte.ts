// The values the shown run's events name, read back as the graph shows
// them. An event carries where the run's record keeps a value
// (`ValueRef`), never the value: the inspector and a debug preview ask
// for the values they show, the ones asked together are read in one trip
// (`readValues`), and only a bounded number stays read.

import type { KeptValue, ValueRef, ValuesAnswer, ValuesRequest } from '../../../protocol';
import { readValues } from '../../host';
import { readOnTheSpot, refKey } from './value-ref';

/** A value as the graph shows it: being read; read; gone from the run's
 *  record (the run was erased); broken (the record and the reference
 *  disagree: a bug); or failed (it could not be read just now, and is
 *  asked again while it is shown). */
export type ShownValue =
	| { state: 'reading' }
	| { state: 'read'; value: unknown }
	| { state: 'gone'; why: string }
	| { state: 'broken'; why: string }
	| { state: 'failed'; why: string };

/** Each port's value of a payload as the graph shows it
 *  (`RunValues.shownPorts`). */
export interface ShownPorts {
	/** The ports read, a gone port's as null (it still shows, with why
	 *  in `gone`). A broken or failed port has none: it shows as an error. */
	values: Record<string, unknown>;
	/** Whether any port is still being read. */
	reading: boolean;
	/** Why each gone port is. */
	gone: Record<string, string>;
	/** Why each broken or failed port shows no value. */
	errors: Record<string, string>;
}

/** How a value one answer of the dispatcher carries shows. */
export function shownAnswer(answer: KeptValue): ShownValue {
	if ('value' in answer) return { state: 'read', value: answer.value };
	if ('gone' in answer) return { state: 'gone', why: answer.gone };
	return { state: 'broken', why: answer.broken };
}

/** The ports of `ports` as `shown` shows each one. */
export function portsAsShown(ports: Record<string, ValueRef> | undefined, shown: (at: ValueRef) => ShownValue): ShownPorts {
	const values: Record<string, unknown> = Object.create(null);
	const gone: Record<string, string> = Object.create(null);
	const errors: Record<string, string> = Object.create(null);
	let reading = false;
	for (const [port, at] of Object.entries(ports ?? {})) {
		const value = shown(at);
		switch (value.state) {
			case 'read':
				values[port] = value.value;
				break;
			case 'gone':
				values[port] = null;
				gone[port] = value.why;
				break;
			case 'broken':
				errors[port] = value.why;
				break;
			case 'failed':
				errors[port] = `could not be read: ${value.why} (asking again)`;
				break;
			case 'reading':
				reading = true;
				break;
		}
	}
	return { values, reading, gone, errors };
}

/** How many values stay read at once: what a few open inspectors and the
 *  debug previews on a canvas show. The one read longest ago goes first,
 *  and is read again if it is shown again. */
const KEPT = 512;

/** How long a value that could not be read waits before it is asked
 *  again, if it is still shown then. */
export const ASK_AGAIN_AFTER_MS = 3000;

export class RunValues {
	/** The run the values are read from. */
	#run: string | undefined;
	/** The values read or being read, by `refKey`, oldest first. */
	#values = new Map<string, ShownValue>();
	/** What a read waiting for its turn asks. */
	#asked = new Map<string, ValueRef>();
	/** Bumped whenever a value lands, so what shows one shows it again. */
	#landed = $state(0);

	/** How a trip to the run's record reads what was asked. */
	readonly #read: (run: string, request: ValuesRequest) => Promise<ValuesAnswer>;

	constructor(read: (run: string, request: ValuesRequest) => Promise<ValuesAnswer>) {
		this.#read = read;
	}

	/** Read from run `run` from now on, forgetting what was read. */
	follow(run: string | undefined): void {
		this.#run = run;
		this.#values.clear();
		this.#asked.clear();
		this.#landed++;
	}

	/** The value `at` names, as it stands: read on the spot when the
	 *  reference holds it, else read from the run's record (asked once,
	 *  together with whatever else is asked meanwhile). */
	shown(at: ValueRef): ShownValue {
		void this.#landed;
		const spot = readOnTheSpot(at);
		if (spot) return { state: 'read', value: spot.value };
		const key = refKey(at);
		const known = this.#values.get(key);
		if (known) {
			// Read again recently: the last to go.
			this.#values.delete(key);
			this.#values.set(key, known);
			return known;
		}
		if (this.#run === undefined) return { state: 'gone', why: 'no run is shown' };
		this.#values.set(key, { state: 'reading' });
		if (this.#asked.size === 0) queueMicrotask(() => void this.#readAsked());
		this.#asked.set(key, at);
		return { state: 'reading' };
	}

	/** Each port's value of `ports` ([`portsAsShown`]). */
	shownPorts(ports: Record<string, ValueRef> | undefined): ShownPorts {
		return portsAsShown(ports, (at) => this.shown(at));
	}

	/** Forget the values of `refs`: what showed them closed. */
	release(refs: Iterable<ValueRef>): void {
		for (const at of refs) this.#values.delete(refKey(at));
	}

	async #readAsked(): Promise<void> {
		const run = this.#run;
		const asked = [...this.#asked.entries()];
		this.#asked.clear();
		if (run === undefined || asked.length === 0) return;
		let landed: ShownValue[];
		try {
			const answer = await this.#read(run, { refs: asked.map(([, at]) => at) });
			if (answer.values.length !== asked.length) {
				throw new Error(`the install answered ${answer.values.length} values for the ${asked.length} asked`);
			}
			landed = answer.values.map(shownAnswer);
		} catch (err) {
			const why = err instanceof Error ? err.message : String(err);
			landed = asked.map(() => ({ state: 'failed', why }));
		}
		// Another run was followed meanwhile: these are not its values.
		if (run !== this.#run) return;
		asked.forEach(([key], i) => {
			if (!this.#values.has(key)) return;
			const value = landed[i];
			this.#values.set(key, value);
			if (value.state === 'failed') setTimeout(() => this.#askAgain(run, key, value), ASK_AGAIN_AFTER_MS);
		});
		while (this.#values.size > KEPT) {
			const oldest = this.#values.keys().next().value;
			if (oldest === undefined) break;
			this.#values.delete(oldest);
		}
		this.#landed++;
	}

	/** Forget the failure `failed` of `key`, if it still stands, so what
	 *  still shows it asks for it again. */
	#askAgain(run: string, key: string, failed: ShownValue): void {
		if (run !== this.#run || this.#values.get(key) !== failed) return;
		this.#values.delete(key);
		this.#landed++;
	}
}

/** The shown run's values, one store for the whole graph. */
export const runValues = new RunValues(readValues);
