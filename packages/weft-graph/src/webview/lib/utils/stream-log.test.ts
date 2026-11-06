import { describe, expect, it } from 'vitest';
import { busClosedLine, busLeftLine } from './stream-log';

describe('busLeftLine', () => {
	it('a member that finished just left', () => {
		expect(busLeftLine('host', { kind: 'finished' })).toEqual({ text: '* host left', failed: false });
	});

	it('a member that failed says so and why, as an error', () => {
		expect(busLeftLine('host', { kind: 'failed', error: 'the model went away' })).toEqual({
			text: '* host left (failed: the model went away)',
			failed: true,
		});
	});
});

describe('busClosedLine', () => {
	it('a finished bus reads as a quiet close', () => {
		expect(busClosedLine({ kind: 'finished' })).toEqual({ text: '* the bus closed here', failed: false });
	});

	it('a failed bus says it failed and why, as an error', () => {
		expect(busClosedLine({ kind: 'failed', error: 'producer crashed' })).toEqual({
			text: '* the bus failed here: producer crashed',
			failed: true,
		});
	});
});
