// What the graph does with a reference to a value (`ValueRef`): read it
// on the spot when it holds the value itself, take a part of it, and know
// which value it names. The values a run's events name come by reference;
// the ones kept in its record are read back through the host
// (`run-values.svelte.ts`).

import type { ValueRef } from '../../../protocol';

/** The reference to what `path` (a wire's keys) reads off the value at
 *  `at`; `at` itself when `path` is empty. A part of a part is one part. */
export function partOf(at: ValueRef, path: Array<string | number>): ValueRef {
	if (path.length === 0) return at;
	if (at.from === 'part') return { from: 'part', of: at.of, path: [...at.path, ...path] };
	return { from: 'part', of: at, path };
}

/** The value `at` names when it is in the reference itself (written out,
 *  or made of values that are); undefined when the run's record keeps it. */
export function readOnTheSpot(at: ValueRef): { value: unknown } | undefined {
	switch (at.from) {
		case 'written':
			return { value: at.value };
		case 'part': {
			const whole = readOnTheSpot(at.of);
			if (!whole) return undefined;
			let value = whole.value;
			for (const step of at.path) {
				if (value === null || typeof value !== 'object') return undefined;
				value = (value as Record<string | number, unknown>)[step];
			}
			return value === undefined ? undefined : { value };
		}
		case 'list': {
			const items: unknown[] = [];
			for (const item of at.items) {
				const read = readOnTheSpot(item);
				if (!read) return undefined;
				items.push(read.value);
			}
			return { value: items };
		}
		case 'row':
		case 'given':
			return undefined;
	}
}

/** The one string a reference is known by: two references to the same
 *  value written the same way share it. */
export function refKey(at: ValueRef): string {
	return JSON.stringify(at);
}
