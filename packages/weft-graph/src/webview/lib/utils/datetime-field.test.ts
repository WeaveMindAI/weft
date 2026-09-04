import { describe, expect, it } from 'vitest';
import { fromPickerLocal, toPickerLocal } from './datetime-field';

describe('datetime field', () => {
	it('shows a stored moment on this clock and writes it back with this offset', () => {
		// Whatever zone the test machine is in, the round trip lands on
		// the same instant, and the written form carries an offset the
		// runtime's ISO-8601 reader accepts.
		for (const stored of ['2026-09-03T09:00:00Z', '2026-09-03T11:00:00+02:00', '2026-09-03T11:00:00.250-04:00']) {
			const shown = toPickerLocal(stored);
			expect(shown).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$/);
			const written = fromPickerLocal(shown);
			expect(written).toMatch(/(Z|[+-]\d{2}:\d{2})$/);
			expect(Math.floor(new Date(written).getTime() / 1000)).toBe(Math.floor(new Date(stored).getTime() / 1000));
		}
		expect(fromPickerLocal('2026-09-03T11:00')).toMatch(/^2026-09-03T11:00:00/);
	});

	it('shows nothing for a value that is not a zoned moment', () => {
		expect(toPickerLocal(undefined)).toBe('');
		expect(toPickerLocal('tomorrow at nine')).toBe('');
		expect(toPickerLocal('')).toBe('');
		// A bare date is what the runtime refuses; shown as a moment it
		// would read as the evening before, west of Greenwich.
		expect(toPickerLocal('2026-09-03')).toBe('');
		expect(toPickerLocal('2026-09-03T09:00:00')).toBe('');
		// The runtime wants seconds; showing this as good would hide a
		// value the run refuses.
		expect(toPickerLocal('2026-09-03T09:00Z')).toBe('');
		expect(toPickerLocal('2026-09-03T24:00:00Z')).toBe('');
		expect(toPickerLocal('2026-09-03T09:00:00+24:00')).toBe('');
	});

	it('refuses to write anything but a picker reading', () => {
		expect(() => fromPickerLocal('9/3/2026')).toThrow(/picker reading/);
	});
});
