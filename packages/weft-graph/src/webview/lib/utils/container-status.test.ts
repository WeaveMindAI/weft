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
		expect(containerStatus('completed', rows('waiting_for_input'))).toBe('running');
	});

	it('a failed member fails the container', () => {
		expect(containerStatus('completed', rows('completed', 'failed'))).toBe('failed');
	});

	it('every row terminal means completed', () => {
		expect(containerStatus('completed', rows('completed', 'skipped', 'cancelled'))).toBe('completed');
	});

	it('falls back to the In boundary while nothing is terminal yet', () => {
		expect(containerStatus('completed', [])).toBe('completed');
	});
});
