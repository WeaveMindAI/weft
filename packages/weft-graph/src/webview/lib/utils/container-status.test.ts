import { describe, it, expect } from 'vitest';
import { containerStatus } from './container-status';

const rows = (...statuses: string[]) => statuses.map((status) => ({ status }) as { status: any });

describe('containerStatus', () => {
	it('a gated container reads skipped, not completed, whatever its members say', () => {
		expect(containerStatus('skipped', rows('skipped', 'skipped', 'skipped'))).toBe('skipped');
		expect(containerStatus('skipped', rows('skipped'))).toBe('skipped');
	});

	it('a member still running keeps the container running', () => {
		expect(containerStatus('completed', rows('completed', 'running'))).toBe('running');
	});

	it('a member parked on a human holds the container: waiting, not running', () => {
		expect(containerStatus('completed', rows('completed', 'waiting'))).toBe('waiting');
		// Unless something else inside is still actually running.
		expect(containerStatus('completed', rows('waiting', 'running'))).toBe('running');
	});

	it('a member paused with its run holds the container paused, unless another waits on its own', () => {
		expect(containerStatus('completed', rows('completed', 'paused'))).toBe('paused');
		expect(containerStatus('completed', rows('paused', 'waiting'))).toBe('waiting');
	});

	it('a failed member fails the container', () => {
		expect(containerStatus('completed', rows('completed', 'failed'))).toBe('failed');
	});

	it('every row terminal means completed', () => {
		expect(containerStatus('completed', rows('completed', 'skipped', 'cancelled'))).toBe('completed');
	});

	it('falls back to the launch\'s own state while nothing is terminal yet', () => {
		expect(containerStatus('completed', [])).toBe('completed');
	});
});
