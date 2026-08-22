import { describe, expect, it } from 'vitest';
import { bareRecord, ownValue } from './index';
import { parseLayoutCode } from '../layout';

describe('bareRecord', () => {
	// Every by-name table in the editor is keyed by something the USER
	// named: a node, a port, a field, a file path. A plain JS object
	// answers `constructor`/`toString`/`valueOf` from its prototype, so
	// such a name reads back a piece of JS machinery instead of
	// "absent", and the editor believes it has data it does not have.
	const PROTOTYPE_KEYS = ['constructor', 'toString', 'valueOf', 'hasOwnProperty', 'isPrototypeOf'];

	it('answers absent for every prototype key, where a plain object does not', () => {
		const table = bareRecord<number>();
		for (const k of PROTOTYPE_KEYS) {
			expect(table[k], k).toBeUndefined();
			// The failure this prevents, spelled out:
			expect(({} as Record<string, unknown>)[k], k).toBeDefined();
		}
	});

	it('keeps entries, and stays bare when rebuilt from a previous table', () => {
		const first = bareRecord<number>({ a: 1 });
		const next = bareRecord(first, { b: 2 });
		expect(next.a).toBe(1);
		expect(next.b).toBe(2);
		expect(next['toString']).toBeUndefined();
	});

	it('lets an accumulator create-or-append under a prototype name', () => {
		// The shape that used to CRASH: `(map[k] ??= []).push(x)` read
		// Object's own constructor (truthy, so no array was created) and
		// threw on `.push`, taking the whole overlay down.
		const map = bareRecord<string[]>();
		(map['constructor'] ??= []).push('bus-1');
		expect(map['constructor']).toEqual(['bus-1']);
	});

	it('the parsed layout table is bare', () => {
		const parsed = parseLayoutCode('@layout constructor x=1 y=2 w=3 h=4');
		expect(ownValue(parsed, 'toString')).toBeUndefined();
		expect(parsed['toString']).toBeUndefined();
	});
});
