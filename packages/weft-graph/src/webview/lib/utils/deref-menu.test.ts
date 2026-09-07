import { describe, it, expect } from 'vitest';
import { derefKeys, derefType } from './deref-menu';

describe('derefKeys', () => {
	const profile = '{ profile: { wpm: Number, name?: String }, id: String }';

	it('lists the record fields at the top level', () => {
		expect(derefKeys(profile, [])).toEqual([
			{ name: 'profile', optional: false, type: '{wpm: Number, name?: String}' },
			{ name: 'id', optional: false, type: 'String' },
		]);
	});

	it('walks the path one level at a time and keeps the `?`', () => {
		expect(derefKeys(profile, ['profile'])).toEqual([
			{ name: 'wpm', optional: false, type: 'Number' },
			{ name: 'name', optional: true, type: 'String' },
		]);
	});

	it('has nothing to offer below a scalar, a JsonDict, or a missing key', () => {
		expect(derefKeys(profile, ['id'])).toBeNull();
		expect(derefKeys('JsonDict', [])).toBeNull();
		expect(derefKeys(profile, ['nope'])).toBeNull();
	});

	it('peels a named type', () => {
		expect(derefKeys('Profile={ wpm: Number }', [])).toEqual([{ name: 'wpm', optional: false, type: 'Number' }]);
	});
});

describe('derefType', () => {
	const profile = '{ profile: { wpm: Number, name?: String }, id: String }';

	it('is the type a dotted wire carries, in the wire spelling', () => {
		expect(derefType(profile, [])).toEqual({ kind: 'type', type: '{profile: {wpm: Number, name?: String}, id: String}' });
		expect(derefType(profile, ['profile', 'wpm'])).toEqual({ kind: 'type', type: 'Number' });
		expect(derefType('Profile={ wpm: Number }', ['wpm'])).toEqual({ kind: 'type', type: 'Number' });
	});

	it('is null off the type', () => {
		expect(derefType(profile, ['id', 'nope'])).toEqual({ kind: 'absent' });
		expect(derefType('not a type', [])).toEqual({ kind: 'unparsed' });
	});
});
