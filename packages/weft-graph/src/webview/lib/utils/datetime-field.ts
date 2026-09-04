/// The two readings of a `datetime` field: the source holds one
/// ISO-8601 moment with a zone (`2026-09-03T11:00:00+02:00`), the
/// browser's picker only speaks local wall-clock time with no zone
/// (`2026-09-03T11:00:00`). These two convert between them on this
/// machine's clock, and nothing else in the editor reads a date.

/// The shape the picker writes and the runtime's ISO-8601 reader
/// accepts: a date, a `T`, a real time to the second (fractions
/// allowed), and a zone (`Z` or a real offset). Only that is shown, so
/// the field never presents as good a value the run would refuse: a
/// time with no seconds, a `24:00` (which `Date` reads as the next
/// midnight), a bare date (which `Date` reads as UTC midnight and
/// shows as the evening before in the Americas).
const ZONED_MOMENT = /^\d{4}-\d{2}-\d{2}T([01]\d|2[0-3]):[0-5]\d:[0-5]\d(\.\d+)?(Z|[+-]([01]\d|2[0-3]):[0-5]\d)$/;

/// What the picker emits: local wall-clock time, no zone.
const PICKER_LOCAL = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?$/;

/// Two-digit zero padding, the picker's fixed width.
function two(n: number): string {
	return String(n).padStart(2, '0');
}

function localWallClock(d: Date): string {
	return `${d.getFullYear()}-${two(d.getMonth() + 1)}-${two(d.getDate())}T${two(d.getHours())}:${two(d.getMinutes())}:${two(d.getSeconds())}`;
}

/// What the picker shows for a stored value: the moment on this
/// machine's clock, to the second. Anything that is not a zoned
/// ISO-8601 moment (unset, prose, a bare date) shows as empty; the
/// source keeps it until a pick replaces it.
export function toPickerLocal(stored: unknown): string {
	if (typeof stored !== 'string' || !ZONED_MOMENT.test(stored.trim())) return '';
	const d = new Date(stored.trim());
	if (Number.isNaN(d.getTime())) return '';
	return localWallClock(d);
}

/// What the source stores for a picked local time: the same wall-clock
/// reading with this machine's offset spelled out, so the file says
/// which zone the author was in. The offset is the one in force AT
/// that moment (summer or winter), not today's. Only a picker's own
/// reading is accepted; anything else is a caller bug, said loudly.
export function fromPickerLocal(local: string): string {
	if (!PICKER_LOCAL.test(local)) {
		throw new Error(`not a picker reading (YYYY-MM-DDTHH:mm[:ss]): ${JSON.stringify(local)}`);
	}
	const d = new Date(local);
	const offsetMinutes = -d.getTimezoneOffset();
	const sign = offsetMinutes >= 0 ? '+' : '-';
	const abs = Math.abs(offsetMinutes);
	const offset = abs === 0 ? 'Z' : `${sign}${two(Math.floor(abs / 60))}:${two(abs % 60)}`;
	return `${localWallClock(d)}${offset}`;
}
